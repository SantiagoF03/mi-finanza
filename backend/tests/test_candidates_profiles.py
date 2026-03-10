"""Tests for candidate sourcing (Part A), asset_type filtering (Part B),
and dynamic target weights (Part C)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.market.candidates import generate_external_candidates
from app.models.models import Recommendation
from app.portfolio.analyzer import analyze_portfolio
from app.portfolio.profiles import PROFILE_PRESETS, build_target_weights, get_bucket
from app.recommendations.universe import VALID_ASSET_TYPES, build_allowed_assets, is_valid_asset_type
from app.services.orchestrator import run_cycle


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# Part A: Candidate sourcing
# ---------------------------------------------------------------------------


def test_watchlist_generates_candidates_without_news():
    """Watchlist symbols appear as external candidates even without news."""
    allowed = {
        "holdings": {"AAPL"},
        "whitelist": {"AAPL"},
        "watchlist": {"TSLA", "NVDA"},
        "universe": set(),
        "main_allowed": {"AAPL"},
        "external_allowed": {"TSLA", "NVDA"},
    }
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    news_ops = []  # No news at all

    candidates = generate_external_candidates(news_ops, allowed, positions)
    symbols = {c["symbol"] for c in candidates}
    assert "TSLA" in symbols
    assert "NVDA" in symbols
    # Both should be from watchlist source
    for c in candidates:
        assert "watchlist" in c["source_types"]


def test_universe_generates_candidates_without_news():
    """Universe symbols appear as external candidates even without news."""
    allowed = {
        "holdings": {"AAPL"},
        "whitelist": {"AAPL"},
        "watchlist": set(),
        "universe": {"MELI", "GLOB"},
        "main_allowed": {"AAPL"},
        "external_allowed": {"MELI", "GLOB"},
    }
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]

    candidates = generate_external_candidates([], allowed, positions)
    symbols = {c["symbol"] for c in candidates}
    assert "MELI" in symbols
    assert "GLOB" in symbols
    for c in candidates:
        assert "universe" in c["source_types"]


def test_news_plus_watchlist_increases_priority():
    """A symbol in both news and watchlist gets higher priority than watchlist-only."""
    allowed = {
        "holdings": {"AAPL"},
        "whitelist": {"AAPL"},
        "watchlist": {"TSLA", "NVDA"},
        "universe": set(),
        "main_allowed": {"AAPL"},
        "external_allowed": {"TSLA", "NVDA"},
    }
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    news_ops = [
        {"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.7, "event_type": "earnings", "impact": "positivo"},
    ]

    candidates = generate_external_candidates(news_ops, allowed, positions)
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")
    nvda = next(c for c in candidates if c["symbol"] == "NVDA")

    assert tsla["priority_score"] > nvda["priority_score"]
    assert "news" in tsla["source_types"]
    assert "watchlist" in tsla["source_types"]
    assert "news" not in nvda["source_types"]


def test_candidate_has_all_required_fields():
    """Each candidate must have the complete field set."""
    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": {"TSLA"},
        "universe": set(),
        "main_allowed": set(),
        "external_allowed": {"TSLA"},
    }
    candidates = generate_external_candidates([], allowed, [])
    assert len(candidates) == 1
    c = candidates[0]
    required_fields = {
        "symbol", "source_types", "tracking_status", "actionable_external",
        "actionable_reason", "priority_score", "asset_type", "asset_type_valid",
        "asset_type_status", "reason", "confidence", "event_type", "impact",
    }
    assert required_fields.issubset(set(c.keys()))


def test_held_symbol_excluded_from_external_candidates():
    """A symbol already in holdings should not appear as external candidate."""
    allowed = {
        "holdings": {"TSLA"},
        "watchlist": {"TSLA"},
        "universe": set(),
        "main_allowed": {"TSLA"},
        "external_allowed": {"TSLA"},
    }
    positions = [{"symbol": "TSLA", "asset_type": "CEDEAR"}]

    candidates = generate_external_candidates([], allowed, positions)
    assert len(candidates) == 0


# ---------------------------------------------------------------------------
# Part B: Asset type filtering
# ---------------------------------------------------------------------------


def test_invalid_asset_type_does_not_break_cycle():
    """An external candidate with unknown asset_type is marked but doesn't crash."""
    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": {"CRYPTO_TOKEN"},
        "universe": set(),
        "main_allowed": set(),
        "external_allowed": {"CRYPTO_TOKEN"},
    }
    # No position info, so asset_type will be DESCONOCIDO
    candidates = generate_external_candidates([], allowed, [])
    assert len(candidates) == 1
    c = candidates[0]
    assert c["asset_type"] == "DESCONOCIDO"
    assert c["asset_type_status"] == "unknown"  # NOT unsupported
    # Unknown is still actionable because it's in watchlist
    assert c["actionable_external"] is True
    assert c["tracking_status"] == "watchlist"


def test_valid_asset_types_set():
    """VALID_ASSET_TYPES contains all expected types and not DESCONOCIDO."""
    assert "CEDEAR" in VALID_ASSET_TYPES
    assert "ACCIONES" in VALID_ASSET_TYPES
    assert "TitulosPublicos" in VALID_ASSET_TYPES
    assert "FondoComundeInversion" in VALID_ASSET_TYPES
    assert "ETF" in VALID_ASSET_TYPES
    assert "BONO" in VALID_ASSET_TYPES
    assert "ON" in VALID_ASSET_TYPES
    assert "DESCONOCIDO" not in VALID_ASSET_TYPES


def test_is_valid_asset_type_runtime():
    assert is_valid_asset_type("CEDEAR") is True
    assert is_valid_asset_type("ETF") is True
    assert is_valid_asset_type("DESCONOCIDO") is False
    assert is_valid_asset_type("CRYPTO") is False
    assert is_valid_asset_type("") is False


# ---------------------------------------------------------------------------
# Part C: Dynamic target weights
# ---------------------------------------------------------------------------


def test_target_weights_not_hardcoded_aapl_msft():
    """Target weights should be derived from holdings, not hardcoded symbols."""
    positions = [
        {"symbol": "GGAL", "market_value": 50, "asset_type": "ACCIONES", "pnl_pct": 0.01},
        {"symbol": "YPFD", "market_value": 30, "asset_type": "ACCIONES", "pnl_pct": 0.02},
        {"symbol": "AL30", "market_value": 20, "asset_type": "BONO", "pnl_pct": -0.01},
    ]
    weights = build_target_weights(positions, profile="moderado")
    # Should have GGAL, YPFD, AL30, CASH — NOT AAPL/MSFT/SPY
    assert "GGAL" in weights
    assert "YPFD" in weights
    assert "AL30" in weights
    assert "CASH" in weights
    assert "AAPL" not in weights
    assert "MSFT" not in weights
    assert "SPY" not in weights


def test_profile_presets_sum_to_one():
    """Each profile preset bucket weights should sum to ~1.0."""
    for profile_name, preset in PROFILE_PRESETS.items():
        total = sum(preset.values())
        assert abs(total - 1.0) < 0.01, f"{profile_name} sums to {total}"


def test_build_target_weights_distributes_by_bucket():
    """Symbols in the same bucket share that bucket's target weight."""
    positions = [
        {"symbol": "AAPL", "asset_type": "CEDEAR"},
        {"symbol": "MSFT", "asset_type": "CEDEAR"},
        {"symbol": "AL30", "asset_type": "BONO"},
    ]
    weights = build_target_weights(positions, profile="moderado")
    moderado = PROFILE_PRESETS["moderado"]

    # AAPL and MSFT are both equity_exterior, should split that bucket
    assert abs(weights["AAPL"] - moderado["equity_exterior"] / 2) < 0.001
    assert abs(weights["MSFT"] - moderado["equity_exterior"] / 2) < 0.001
    # AL30 is the only renta_fija, gets full bucket weight
    assert abs(weights["AL30"] - moderado["renta_fija"]) < 0.001
    # CASH includes preset cash + unallocated buckets (equity_local, fci, otros)
    expected_cash = moderado["cash"] + moderado["equity_local"] + moderado["fci"] + moderado["otros"]
    assert abs(weights["CASH"] - expected_cash) < 0.001


def test_analyzer_uses_dynamic_weights():
    """analyze_portfolio should use dynamic weights, not AAPL/MSFT/SPY hardcoded."""
    snapshot = {
        "total_value": 100,
        "cash": 20,
        "currency": "ARS",
        "positions": [
            {"symbol": "GGAL", "market_value": 50, "asset_type": "ACCIONES", "pnl_pct": 0.01, "currency": "ARS"},
            {"symbol": "AL30", "market_value": 30, "asset_type": "BONO", "pnl_pct": -0.01, "currency": "ARS"},
        ],
    }
    analysis = analyze_portfolio(snapshot)
    # rebalance_deviation keys should be from actual holdings, not hardcoded
    assert "GGAL" in analysis["rebalance_deviation"]
    assert "AL30" in analysis["rebalance_deviation"]
    assert "CASH" in analysis["rebalance_deviation"]
    # Should NOT have AAPL/MSFT/SPY unless they're in the whitelist targets
    # (they won't be because positions don't include them)


def test_get_bucket_mapping():
    assert get_bucket("CEDEAR") == "equity_exterior"
    assert get_bucket("ETF") == "equity_exterior"
    assert get_bucket("BONO") == "renta_fija"
    assert get_bucket("ON") == "renta_fija"
    assert get_bucket("TitulosPublicos") == "renta_fija"
    assert get_bucket("ACCIONES") == "equity_local"
    assert get_bucket("FondoComundeInversion") == "fci"
    assert get_bucket("UNKNOWN_TYPE") == "otros"


def test_agresivo_profile_more_equity():
    """Agresivo profile should allocate more to equity than conservador."""
    assert PROFILE_PRESETS["agresivo"]["equity_exterior"] > PROFILE_PRESETS["conservador"]["equity_exterior"]
    assert PROFILE_PRESETS["conservador"]["renta_fija"] > PROFILE_PRESETS["agresivo"]["renta_fija"]
    assert PROFILE_PRESETS["conservador"]["cash"] > PROFILE_PRESETS["agresivo"]["cash"]


# ---------------------------------------------------------------------------
# Integration: full cycle
# ---------------------------------------------------------------------------


def test_full_cycle_with_candidate_sourcing():
    """End-to-end cycle uses candidate sourcing and dynamic weights."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.watchlist_assets = ["TSLA", "NVDA"]
    s.market_universe_assets = ["MELI"]

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    # External opportunities should include watchlist/universe candidates
    ext_symbols = {op["symbol"] for op in meta.get("external_opportunities", [])}
    assert "TSLA" in ext_symbols or "NVDA" in ext_symbols or "MELI" in ext_symbols

    # Each opportunity should have the new fields
    for op in meta.get("external_opportunities", []):
        assert "source_types" in op
        assert "actionable_external" in op
        assert "priority_score" in op

    # Recommendation should still be limited to holdings
    for action in rec.actions:
        mock_holdings = {"AAPL", "MSFT", "SPY", "AL30"}
        # Actions should only target held assets (or whitelist)
        assert action.symbol in mock_holdings or action.symbol in set(s.whitelist_assets)

    # Reset
    s.watchlist_assets = []
    s.market_universe_assets = []


def test_unchanged_still_works_after_changes():
    """Unchanged detection still works with the new candidate sourcing."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.watchlist_assets = ["TSLA"]

    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")
    assert second.get("unchanged") is True

    # Reset
    s.watchlist_assets = []


def test_main_recommendation_limited_to_holdings():
    """Even with watchlist/universe, main actions only target holdings."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.watchlist_assets = ["TSLA", "NVDA", "GOOGL"]
    s.market_universe_assets = ["MELI", "GLOB"]

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()

    # No action should target a watchlist/universe symbol
    watchlist_universe = {"TSLA", "NVDA", "GOOGL", "MELI", "GLOB"}
    for action in rec.actions:
        assert action.symbol not in watchlist_universe

    # Reset
    s.watchlist_assets = []
    s.market_universe_assets = []
