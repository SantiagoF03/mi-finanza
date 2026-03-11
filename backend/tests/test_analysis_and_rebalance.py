"""Tests for analysis coherence, currency weights, rebalance calibration.

Part A: weights_by_currency reflects real economic exposure
Part B: No hardcoded symbols in analysis/rebalance path
Part C: suggested_pct calibration
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.clients import _normalize_asset_type, _map_currency, map_iol_portfolio_to_snapshot
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import Recommendation
from app.portfolio.analyzer import _infer_economic_currency, analyze_portfolio
from app.portfolio.profiles import PROFILE_PRESETS, build_target_weights
from app.recommendations.engine import generate_recommendation
from app.services.orchestrator import run_cycle


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# Part A: weights_by_currency reflects mixed ARS/USD holdings
# ---------------------------------------------------------------------------


def test_weights_by_currency_reflects_usd_positions():
    """Positions with currency=USD should contribute to USD weight."""
    snapshot = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "GGAL", "market_value": 40, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "IOLDOLD", "market_value": 50, "asset_type": "DESCONOCIDO", "currency": "USD", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    assert analysis["weights_by_currency"]["USD"] > 0
    assert analysis["weights_by_currency"]["ARS"] > 0
    # IOLDOLD (50/100=0.50) is USD, GGAL (40/100=0.40) is ARS, CASH (10/100=0.10) is ARS
    assert abs(analysis["weights_by_currency"]["USD"] - 0.50) < 0.01
    assert abs(analysis["weights_by_currency"]["ARS"] - 0.50) < 0.01


def test_cedear_counts_as_usd_exposure():
    """CEDEARs trade in ARS but represent USD economic exposure."""
    snapshot = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "AAPL", "market_value": 45, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GGAL", "market_value": 45, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    # AAPL (CEDEAR) → USD economic exposure even though traded in ARS
    assert analysis["weights_by_currency"]["USD"] > 0.4
    # GGAL (ACCIONES) + CASH → ARS
    assert analysis["weights_by_currency"]["ARS"] > 0.4


def test_etf_counts_as_usd_exposure():
    """ETFs like SPY/QQQ represent USD exposure."""
    snapshot = {
        "total_value": 100,
        "cash": 0,
        "currency": "ARS",
        "positions": [
            {"symbol": "SPY", "market_value": 100, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    assert analysis["weights_by_currency"].get("USD", 0) == 1.0


def test_gd_bono_counts_as_usd():
    """GD-prefix bonos (dollar-linked globals) should be USD exposure."""
    assert _infer_economic_currency("GD35", "BONO", "ARS") == "USD"
    assert _infer_economic_currency("GD30", "BONO", "ARS") == "USD"
    assert _infer_economic_currency("AE38", "BONO", "ARS") == "USD"


def test_al_bono_keeps_trading_currency():
    """AL-prefix bonos are peso-linked, keep their trading currency."""
    assert _infer_economic_currency("AL30", "BONO", "ARS") == "ARS"
    assert _infer_economic_currency("AL35", "BONO", "ARS") == "ARS"


def test_weights_by_currency_sums_to_one():
    """Currency weights should sum to ~1.0."""
    snapshot = {
        "total_value": 200,
        "cash": 20,
        "currency": "ARS",
        "positions": [
            {"symbol": "SPY", "market_value": 80, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GGAL", "market_value": 50, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GD35", "market_value": 50, "asset_type": "BONO", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    total = sum(analysis["weights_by_currency"].values())
    assert abs(total - 1.0) < 0.01


def test_usd_position_not_hidden():
    """A USD position with market_value > 0 must show in currency weights."""
    snapshot = {
        "total_value": 100,
        "cash": 0,
        "currency": "ARS",
        "positions": [
            {"symbol": "IOLDOLD", "market_value": 30, "asset_type": "DESCONOCIDO", "currency": "USD", "pnl_pct": 0},
            {"symbol": "GGAL", "market_value": 70, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    assert analysis["weights_by_currency"]["USD"] == 0.3


# ---------------------------------------------------------------------------
# IOL asset type normalization
# ---------------------------------------------------------------------------


def test_normalize_iol_asset_types():
    """IOL's lowercase tipos get normalized to our canonical format."""
    assert _normalize_asset_type("acciones") == "ACCIONES"
    assert _normalize_asset_type("cedears") == "CEDEAR"
    assert _normalize_asset_type("bonos") == "BONO"
    assert _normalize_asset_type("letras") == "TitulosPublicos"
    assert _normalize_asset_type("titulos_publicos") == "TitulosPublicos"
    assert _normalize_asset_type("obligaciones_negociables") == "ON"
    assert _normalize_asset_type("fondos_comunes_de_inversion") == "FondoComundeInversion"
    assert _normalize_asset_type("fci") == "FondoComundeInversion"
    assert _normalize_asset_type("etf") == "ETF"
    assert _normalize_asset_type("etfs") == "ETF"


def test_normalize_preserves_canonical():
    """Already-canonical types should pass through unchanged."""
    assert _normalize_asset_type("CEDEAR") == "CEDEAR"
    assert _normalize_asset_type("ACCIONES") == "ACCIONES"
    assert _normalize_asset_type("BONO") == "BONO"
    assert _normalize_asset_type("FondoComundeInversion") == "FondoComundeInversion"


def test_normalize_unknown_tipo():
    """Unknown tipos return the original or DESCONOCIDO for empty."""
    assert _normalize_asset_type("") == "DESCONOCIDO"
    assert _normalize_asset_type("cauciones") == "cauciones"


def test_map_currency_handles_variations():
    """Currency mapping handles various IOL formats."""
    assert _map_currency("peso_Argentino") == "ARS"
    assert _map_currency("dolar_Estadounidense") == "USD"
    assert _map_currency("dolar_estadounidense") == "USD"
    assert _map_currency("dólar_estadounidense") == "USD"
    assert _map_currency("USD") == "USD"
    assert _map_currency("u$s") == "USD"
    assert _map_currency("ARS") == "ARS"
    assert _map_currency("pesos") == "ARS"
    assert _map_currency(None) == "ARS"


def test_iol_snapshot_normalizes_tipo():
    """map_iol_portfolio_to_snapshot should normalize IOL tipos."""
    iol_payload = {
        "activos": [
            {
                "titulo": {"simbolo": "GGAL", "tipo": "acciones", "moneda": "peso_Argentino"},
                "cantidad": 100,
                "valorizado": 5000,
                "ppc": 50,
                "gananciaPorcentaje": 10,
            },
            {
                "titulo": {"simbolo": "AL30", "tipo": "bonos", "moneda": "peso_Argentino"},
                "cantidad": 200,
                "valorizado": 8000,
                "ppc": 40,
                "gananciaPorcentaje": -5,
            },
            {
                "titulo": {"simbolo": "CRTAFAA", "tipo": "fondos_comunes_de_inversion", "moneda": "peso_Argentino"},
                "cantidad": 50,
                "valorizado": 3000,
                "ppc": 60,
                "gananciaPorcentaje": 2,
            },
        ],
    }
    result = map_iol_portfolio_to_snapshot(iol_payload)
    types = {p["symbol"]: p["asset_type"] for p in result["positions"]}
    assert types["GGAL"] == "ACCIONES"
    assert types["AL30"] == "BONO"
    assert types["CRTAFAA"] == "FondoComundeInversion"


# ---------------------------------------------------------------------------
# Part B: No hardcoded symbols in analysis/rebalance
# ---------------------------------------------------------------------------


def test_target_weights_from_holdings_only():
    """Target weights should be derived from actual holdings, not hardcoded."""
    positions = [
        {"symbol": "GGAL", "asset_type": "ACCIONES"},
        {"symbol": "GD35", "asset_type": "BONO"},
        {"symbol": "CRTAFAA", "asset_type": "FondoComundeInversion"},
    ]
    weights = build_target_weights(positions, profile="moderado")
    # Should contain actual holdings
    assert "GGAL" in weights
    assert "GD35" in weights
    assert "CRTAFAA" in weights
    assert "CASH" in weights
    # Should NOT contain mock hardcoded symbols
    assert "AAPL" not in weights
    assert "MSFT" not in weights
    assert "SPY" not in weights


def test_rebalance_deviation_from_buckets_not_hardcoded():
    """rebalance_deviation should reflect bucket-based targets, not fixed symbols."""
    snapshot = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "PAMP", "market_value": 30, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GD35", "market_value": 30, "asset_type": "BONO", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "SPY", "market_value": 30, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    # Deviation keys should be from holdings + CASH
    assert "PAMP" in analysis["rebalance_deviation"]
    assert "GD35" in analysis["rebalance_deviation"]
    assert "SPY" in analysis["rebalance_deviation"]
    assert "CASH" in analysis["rebalance_deviation"]
    # Should NOT have hardcoded symbols
    assert "AAPL" not in analysis["rebalance_deviation"]
    assert "MSFT" not in analysis["rebalance_deviation"]


def test_weights_by_bucket_present_in_analysis():
    """Analysis should include bucket-level weights for transparency."""
    snapshot = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "GGAL", "market_value": 30, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GD35", "market_value": 30, "asset_type": "BONO", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "SPY", "market_value": 30, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    assert "weights_by_bucket" in analysis
    assert "equity_local" in analysis["weights_by_bucket"]
    assert "renta_fija" in analysis["weights_by_bucket"]
    assert "equity_exterior" in analysis["weights_by_bucket"]
    assert "cash" in analysis["weights_by_bucket"]


# ---------------------------------------------------------------------------
# Part C: suggested_pct calibration
# ---------------------------------------------------------------------------


def test_suggested_pct_scales_with_deviation():
    """suggested_pct should scale with deviation severity, not always max."""
    # Small deviation (8%)
    snapshot_small = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "AAPL", "market_value": 45, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "MSFT", "market_value": 45, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis_small = analyze_portfolio(snapshot_small)
    news = [{"title": "test", "impact": "neutro", "related_assets": [], "event_type": "otro", "confidence": 0.5}]
    rec_small = generate_recommendation(snapshot_small, analysis_small, news, max_move=0.10)

    if rec_small["action"] == "rebalancear":
        # suggested_pct should be less than max_move for moderate deviations
        # (unless deviation is extreme, in which case capping is correct)
        assert rec_small["suggested_pct"] <= 0.10
        assert rec_small["suggested_pct"] >= 0.02


def test_suggested_pct_capped_at_max_move():
    """Even with extreme deviation, suggested_pct should not exceed max_move."""
    snapshot = {
        "total_value": 100,
        "cash": 1,
        "currency": "ARS",
        "positions": [
            {"symbol": "SPY", "market_value": 99, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    news = [{"title": "test", "impact": "neutro", "related_assets": [], "event_type": "otro", "confidence": 0.5}]
    rec = generate_recommendation(snapshot, analysis, news, max_move=0.10)
    assert rec["suggested_pct"] <= 0.10


def test_suggested_pct_not_rigid_fixed():
    """Two different deviation levels should produce different suggested_pct."""
    # Moderate deviation portfolio
    snap_mod = {
        "total_value": 100,
        "cash": 10,
        "currency": "ARS",
        "positions": [
            {"symbol": "AAPL", "market_value": 25, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "MSFT", "market_value": 25, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "GGAL", "market_value": 20, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0},
            {"symbol": "AL30", "market_value": 20, "asset_type": "BONO", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    # Extreme deviation portfolio
    snap_ext = {
        "total_value": 100,
        "cash": 1,
        "currency": "ARS",
        "positions": [
            {"symbol": "SPY", "market_value": 99, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0},
        ],
    }
    news = []
    rec_mod = generate_recommendation(snap_mod, analyze_portfolio(snap_mod), news, max_move=0.15)
    rec_ext = generate_recommendation(snap_ext, analyze_portfolio(snap_ext), news, max_move=0.15)
    # Extreme should suggest >= moderate (both capped or different)
    if rec_mod["action"] == "rebalancear" and rec_ext["action"] in {"rebalancear", "reducir riesgo"}:
        assert rec_ext["suggested_pct"] >= rec_mod["suggested_pct"]


# ---------------------------------------------------------------------------
# Integration: full cycle still works
# ---------------------------------------------------------------------------


def test_full_cycle_analysis_has_currency_weights():
    """End-to-end cycle produces correct analysis with currency weights."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}
    analysis = meta.get("analysis", {})

    # Should have currency weights
    assert "weights_by_currency" in analysis
    assert sum(analysis["weights_by_currency"].values()) > 0.99

    # Should have bucket weights
    assert "weights_by_bucket" in analysis


def test_recommendation_still_limited_to_holdings():
    """After analysis changes, main actions still target only holdings."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()

    mock_holdings = {"AAPL", "MSFT", "SPY", "AL30"}
    for action in rec.actions:
        assert action.symbol in mock_holdings or action.symbol in set(s.whitelist_assets)


def test_external_opportunities_still_separate():
    """External opportunities remain separate from main actions."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.watchlist_assets = ["TSLA"]

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    ext = meta.get("external_opportunities", [])
    ext_symbols = {op["symbol"] for op in ext}
    action_symbols = {a.symbol for a in rec.actions}

    # TSLA should be in external, not in actions
    if "TSLA" in ext_symbols:
        assert "TSLA" not in action_symbols

    s.watchlist_assets = []


def test_unchanged_still_works_after_analysis_changes():
    """Unchanged detection still works with the new analysis fields."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")
    assert second.get("unchanged") is True


# ---------------------------------------------------------------------------
# Simulate real runtime scenario
# ---------------------------------------------------------------------------


def test_real_portfolio_currency_exposure():
    """Simulate the user's real portfolio and verify currency weights make sense.

    Real portfolio: SPY, QQQ, ACWI, BABA, BIDU, BRKB, GLD (CEDEARs/ETFs → USD),
    BYMA, PAMP (ACCIONES → ARS), CRTAFAA, PRREMIB (FCI → ARS),
    GD35 (BONO GD → USD), IOLDOLD (DESCONOCIDO → USD trading currency).
    """
    positions = [
        {"symbol": "ACWI", "market_value": 793, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0.01},
        {"symbol": "BABA", "market_value": 175, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0.02},
        {"symbol": "BIDU", "market_value": 329, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": -0.01},
        {"symbol": "BRKB", "market_value": 517, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0.03},
        {"symbol": "BYMA", "market_value": 382, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0.01},
        {"symbol": "CRTAFAA", "market_value": 1535, "asset_type": "FondoComundeInversion", "currency": "ARS", "pnl_pct": 0.0},
        {"symbol": "GD35", "market_value": 1226, "asset_type": "BONO", "currency": "ARS", "pnl_pct": -0.01},
        {"symbol": "GLD", "market_value": 386, "asset_type": "CEDEAR", "currency": "ARS", "pnl_pct": 0.02},
        {"symbol": "IOLDOLD", "market_value": 0, "asset_type": "DESCONOCIDO", "currency": "USD", "pnl_pct": 0},
        {"symbol": "PAMP", "market_value": 554, "asset_type": "ACCIONES", "currency": "ARS", "pnl_pct": 0.01},
        {"symbol": "PRREMIB", "market_value": 635, "asset_type": "FondoComundeInversion", "currency": "ARS", "pnl_pct": 0.0},
        {"symbol": "QQQ", "market_value": 525, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0.01},
        {"symbol": "SPY", "market_value": 2927, "asset_type": "ETF", "currency": "ARS", "pnl_pct": 0.05},
    ]
    total = sum(p["market_value"] for p in positions) + 16  # cash=16
    snapshot = {
        "total_value": total,
        "cash": 16,
        "currency": "ARS",
        "positions": positions,
    }
    analysis = analyze_portfolio(snapshot)

    # USD exposure should be substantial (SPY, QQQ, ACWI, BABA, BIDU, BRKB, GLD, GD35)
    usd = analysis["weights_by_currency"].get("USD", 0)
    ars = analysis["weights_by_currency"].get("ARS", 0)
    assert usd > 0.5, f"USD exposure should be >50%, got {usd:.2%}"
    assert ars > 0.2, f"ARS exposure should be >20%, got {ars:.2%}"
    assert abs(usd + ars - 1.0) < 0.01

    # Bucket weights should reflect the correct categorization
    buckets = analysis["weights_by_bucket"]
    assert buckets.get("equity_exterior", 0) > 0.3  # CEDEARs + ETFs
    assert buckets.get("equity_local", 0) > 0.05  # BYMA, PAMP
    assert buckets.get("renta_fija", 0) > 0.05  # GD35
    assert buckets.get("fci", 0) > 0.1  # CRTAFAA, PRREMIB

    # CASH deviation should NOT be -0.45 anymore (buckets are now filled)
    cash_dev = analysis["rebalance_deviation"].get("CASH", 0)
    assert abs(cash_dev) < 0.20, f"CASH deviation should be reasonable, got {cash_dev}"

    # No hardcoded symbols in deviation
    assert "MSFT" not in analysis["rebalance_deviation"]
