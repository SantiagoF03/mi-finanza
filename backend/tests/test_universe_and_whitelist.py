"""Tests for dynamic whitelist, universe, and allowed assets (Parts A & B)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import Recommendation
from app.recommendations.engine import generate_recommendation
from app.recommendations.universe import build_allowed_assets, classify_opportunity_status, is_valid_asset_type
from app.rules.engine import enforce_rules
from app.services.orchestrator import run_cycle


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# Part A: Dynamic whitelist / auto-permitted holdings
# ---------------------------------------------------------------------------


def test_holding_not_in_whitelist_still_permitted():
    """A real holding symbol NOT in WHITELIST_ASSETS should still be allowed in main actions."""
    rec = {
        "action": "reducir riesgo",
        "suggested_pct": 0.05,
        "confidence": 0.8,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [{"symbol": "GGAL", "target_change_pct": -0.05, "reason": "Sobreconcentración"}],
    }
    # GGAL is NOT in the default whitelist
    whitelist = ["AAPL", "MSFT", "SPY"]
    holdings = {"GGAL", "YPFD"}

    out = enforce_rules(rec, whitelist=whitelist, max_move=0.1, holdings=holdings)
    # GGAL should NOT be blocked — it's a real holding
    assert out["status"] == "pending"
    assert len(out["actions"]) == 1
    assert out["actions"][0]["symbol"] == "GGAL"


def test_holding_not_in_whitelist_blocked_without_holdings_param():
    """Without holdings param (backward compat), whitelist is the only gate."""
    rec = {
        "action": "reducir riesgo",
        "suggested_pct": 0.05,
        "confidence": 0.8,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [{"symbol": "GGAL", "target_change_pct": -0.05, "reason": "Sobreconcentración"}],
    }
    whitelist = ["AAPL", "MSFT"]

    out = enforce_rules(rec, whitelist=whitelist, max_move=0.1)
    assert out["status"] == "blocked"
    assert "whitelist" in out["blocked_reason"]


def test_whitelist_manual_override_still_works():
    """WHITELIST_ASSETS should still allow symbols not in holdings."""
    rec = {
        "action": "aumentar posición",
        "suggested_pct": 0.04,
        "confidence": 0.7,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [{"symbol": "QQQ", "target_change_pct": 0.04, "reason": "Override manual"}],
    }
    whitelist = ["QQQ"]
    holdings = {"GGAL"}  # QQQ not in holdings

    out = enforce_rules(rec, whitelist=whitelist, max_move=0.1, holdings=holdings)
    assert out["status"] == "pending"
    assert out["actions"][0]["symbol"] == "QQQ"


def test_external_opportunity_for_non_held_asset():
    """Non-held assets from news should appear as external_opportunities."""
    snapshot = {
        "total_value": 100,
        "cash": 20,
        "currency": "USD",
        "positions": [{"symbol": "GGAL", "market_value": 80, "pnl_pct": 0.01}],
    }
    analysis = {"alerts": [], "weights_by_asset": {"GGAL": 0.8}, "rebalance_deviation": {"GGAL": 0.0}}
    news = [{"impact": "positivo", "related_assets": ["TSLA"], "event_type": "earnings", "confidence": 0.7, "title": "TSLA sube"}]

    rec = generate_recommendation(snapshot, analysis, news, max_move=0.1)
    assert any(op["symbol"] == "TSLA" for op in rec["external_opportunities"])
    # Should NOT be in main actions
    assert all(a["symbol"] != "TSLA" for a in rec["actions"])


def test_external_opportunity_not_promoted_to_main_action():
    """An external opportunity should never become a main action unless it's a holding."""
    rec = {
        "action": "aumentar posición",
        "suggested_pct": 0.04,
        "confidence": 0.7,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [{"symbol": "TSLA", "target_change_pct": 0.04, "reason": "Opportunity"}],
    }
    whitelist = ["AAPL"]
    holdings = {"GGAL"}  # TSLA not in holdings or whitelist

    out = enforce_rules(rec, whitelist=whitelist, max_move=0.1, holdings=holdings)
    assert out["status"] == "blocked"
    assert "whitelist" in out["blocked_reason"]


# ---------------------------------------------------------------------------
# Part B: Market universe / watchlist / tracking status
# ---------------------------------------------------------------------------


def test_build_allowed_assets_includes_holdings():
    positions = [{"symbol": "GGAL"}, {"symbol": "YPFD"}]
    result = build_allowed_assets(positions)
    assert "GGAL" in result["holdings"]
    assert "YPFD" in result["holdings"]
    assert "GGAL" in result["main_allowed"]
    assert "YPFD" in result["main_allowed"]


def test_build_allowed_assets_merges_whitelist():
    s = get_settings()
    positions = [{"symbol": "GGAL"}]
    result = build_allowed_assets(positions)
    # Holdings + whitelist merged
    assert "GGAL" in result["main_allowed"]
    for sym in s.whitelist_assets:
        assert sym in result["main_allowed"]


def test_watchlist_assets_tracked():
    s = get_settings()
    original = s.watchlist_assets
    s.watchlist_assets = ["TSLA", "NVDA"]

    positions = [{"symbol": "GGAL"}]
    result = build_allowed_assets(positions)
    assert "TSLA" in result["watchlist"]
    assert "TSLA" in result["external_allowed"]
    assert "TSLA" not in result["main_allowed"]  # watchlist != main allowed

    s.watchlist_assets = original


def test_classify_opportunity_status():
    allowed = {
        "holdings": {"AAPL", "MSFT"},
        "watchlist": {"TSLA"},
        "universe": {"NVDA", "GOOGL"},
    }
    assert classify_opportunity_status("AAPL", allowed) == "in_holdings"
    assert classify_opportunity_status("TSLA", allowed) == "watchlist"
    assert classify_opportunity_status("NVDA", allowed) == "in_universe"
    assert classify_opportunity_status("RANDOM", allowed) == "untracked"


def test_valid_asset_types():
    assert is_valid_asset_type("CEDEAR")
    assert is_valid_asset_type("ACCIONES")
    assert is_valid_asset_type("TitulosPublicos")
    assert is_valid_asset_type("FondoComundeInversion")
    assert is_valid_asset_type("ETF")
    assert is_valid_asset_type("BONO")
    assert not is_valid_asset_type("CRYPTO")
    assert not is_valid_asset_type("")


def test_full_cycle_with_holdings_auto_permitted():
    """End-to-end: run_cycle with mock broker, holdings are auto-permitted even if not in whitelist."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    # allowed_assets should be in metadata
    assert "allowed_assets" in meta
    assert "holdings" in meta["allowed_assets"]
    assert "main_allowed" in meta["allowed_assets"]

    # Mock broker has AAPL, MSFT, SPY, AL30 — all should be in holdings
    holdings = meta["allowed_assets"]["holdings"]
    assert "AAPL" in holdings
    assert "MSFT" in holdings
    assert "SPY" in holdings
    assert "AL30" in holdings


def test_external_opportunity_gets_tracking_status_in_cycle():
    """External opportunities in metadata should have tracking_status."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    for op in meta.get("external_opportunities", []):
        assert "tracking_status" in op
