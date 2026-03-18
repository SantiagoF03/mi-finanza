"""Tests for cluster-aware scoring, market confirmation, and signal classification.

Covers:
1. score_news_item boosts multi-source clusters
2. score_news_item penalizes weak single-source signals
3. score_news_item boosts holdings-relevant items
4. compute_market_confirmation confirms/contradicts correctly
5. classify_signal categorizes into 4 signal classes
6. score_and_classify_news returns ranked enriched items
7. generate_recommendation filters weak signals via signal_score
8. generate_recommendation boosts confidence on market-confirmed positive
9. External opportunities include signal_class and signal_score
10. Scoring summary appears in recommendation metadata
11. Legacy (non-enriched) news still works
"""

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.recommendations.scoring import (
    classify_signal,
    compute_market_confirmation,
    score_and_classify_news,
    score_news_item,
)
from app.recommendations.engine import generate_recommendation


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def _mock_snapshot(positions=None):
    pos = positions or [
        {"symbol": "AAPL", "asset_type": "CEDEAR", "market_value": 38000, "pnl_pct": 0.11, "quantity": 20, "avg_price": 180},
        {"symbol": "SPY", "asset_type": "ETF", "market_value": 17000, "pnl_pct": -0.05, "quantity": 15, "avg_price": 510},
    ]
    total = sum(p["market_value"] for p in pos) + 12000
    return {"total_value": total, "cash": 12000, "currency": "USD", "positions": pos}


def _mock_analysis():
    return {
        "alerts": [],
        "weights_by_asset": {"AAPL": 0.45, "SPY": 0.25},
        "rebalance_deviation": {"AAPL": 0.05, "SPY": -0.02},
        "equity_weight": 0.70,
    }


def _mock_allowed():
    return {
        "holdings": {"AAPL", "SPY"},
        "whitelist": {"AAPL", "SPY", "QQQ", "AL30"},
        "watchlist": {"MELI", "GLOB"},
        "universe": {"MELI", "GLOB", "GGAL", "YPF"},
        "main_allowed": {"AAPL", "SPY", "QQQ", "AL30"},
        "catalog_dynamic": {"GGAL", "YPF"},
    }


# ---------------------------------------------------------------------------
# 1. score_news_item — multi-source boost
# ---------------------------------------------------------------------------


def test_multi_source_cluster_scores_higher():
    """A cluster with 3 sources should score higher than one with 1 source."""
    single = {"pre_score": 0.5, "source_count": 1, "item_count": 1, "related_assets": []}
    multi = {"pre_score": 0.5, "source_count": 3, "item_count": 5, "related_assets": []}

    s1 = score_news_item(single, set(), set())
    s2 = score_news_item(multi, set(), set())

    assert s2 > s1
    assert s2 - s1 >= 0.2  # significant boost


# ---------------------------------------------------------------------------
# 2. score_news_item — penalizes weak signals
# ---------------------------------------------------------------------------


def test_weak_single_source_penalized():
    """Low-score single-source item should be penalized."""
    weak = {"pre_score": 0.2, "source_count": 1, "item_count": 1, "related_assets": []}
    score = score_news_item(weak, set(), set())
    assert score < 0.2  # penalized below base


# ---------------------------------------------------------------------------
# 3. score_news_item — holdings relevance boost
# ---------------------------------------------------------------------------


def test_holdings_relevance_boosts_score():
    """Items mentioning held symbols should score higher."""
    base = {"pre_score": 0.5, "related_assets": ["AAPL"], "affects_holdings": True}
    no_hold = {"pre_score": 0.5, "related_assets": ["GGAL"], "affects_holdings": False}

    s1 = score_news_item(base, {"AAPL"}, {"AAPL", "GGAL"})
    s2 = score_news_item(no_hold, {"AAPL"}, {"AAPL", "GGAL"})

    assert s1 > s2


# ---------------------------------------------------------------------------
# 4. compute_market_confirmation
# ---------------------------------------------------------------------------


def test_negative_event_negative_pnl_confirmed():
    """Negative event + negative pnl on related holding = confirmed."""
    item = {"impact": "negativo", "related_assets": ["SPY"]}
    positions = [{"symbol": "SPY", "pnl_pct": -0.08}]

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "confirmed"


def test_negative_event_positive_pnl_contradicted():
    """Negative event + positive pnl = contradicted."""
    item = {"impact": "negativo", "related_assets": ["AAPL"]}
    positions = [{"symbol": "AAPL", "pnl_pct": 0.10}]

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "contradicted"


def test_positive_event_positive_pnl_confirmed():
    """Positive event + positive pnl = confirmed."""
    item = {"impact": "positivo", "related_assets": ["AAPL"]}
    positions = [{"symbol": "AAPL", "pnl_pct": 0.12}]

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "confirmed"


def test_no_overlap_unconfirmed():
    """No matching holdings = unconfirmed."""
    item = {"impact": "negativo", "related_assets": ["MELI"]}
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "unconfirmed"


def test_small_pnl_unconfirmed():
    """PnL below threshold = unconfirmed even if directions match."""
    item = {"impact": "negativo", "related_assets": ["AAPL"]}
    positions = [{"symbol": "AAPL", "pnl_pct": -0.01}]  # below 3% threshold

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "unconfirmed"


# ---------------------------------------------------------------------------
# 5. classify_signal — 4 classes
# ---------------------------------------------------------------------------


def test_classify_holding_risk():
    item = {"impact": "negativo", "related_assets": ["AAPL"]}
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, set(), set())
    assert cls == "holding_risk"


def test_classify_holding_opportunity():
    item = {"impact": "positivo", "related_assets": ["AAPL"]}
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, set(), set())
    assert cls == "holding_opportunity"


def test_classify_external_opportunity():
    item = {"impact": "positivo", "related_assets": ["MELI"]}
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, {"MELI"}, set())
    assert cls == "external_opportunity"


def test_classify_observed_candidate():
    item = {"impact": "neutro", "related_assets": ["RANDOM"]}
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, set(), set())
    assert cls == "observed_candidate"


def test_affects_holdings_flag_used():
    """Items with affects_holdings=True should classify as holding even without symbol match."""
    item = {"impact": "negativo", "related_assets": [], "affects_holdings": True}
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, set(), set())
    assert cls == "holding_risk"


# ---------------------------------------------------------------------------
# 6. score_and_classify_news — integrated ranking
# ---------------------------------------------------------------------------


def test_score_and_classify_ranks_risks_first():
    """holding_risk items should rank before external_opportunity items."""
    news = [
        {"title": "External opp", "impact": "positivo", "related_assets": ["MELI"],
         "pre_score": 0.8, "event_type": "upgrade"},
        {"title": "Holding risk", "impact": "negativo", "related_assets": ["AAPL"],
         "pre_score": 0.6, "event_type": "downgrade"},
    ]
    positions = [{"symbol": "AAPL", "pnl_pct": -0.05}]
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)

    assert len(result) == 2
    assert result[0]["signal_class"] == "holding_risk"
    assert result[1]["signal_class"] == "external_opportunity"


def test_score_and_classify_enriches_all_fields():
    """Each item should get signal_score, signal_class, and market_confirmation."""
    news = [{"title": "Test", "impact": "neutro", "related_assets": ["AAPL"],
             "pre_score": 0.5, "event_type": "macro"}]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.02}]

    result = score_and_classify_news(news, positions, _mock_allowed())

    assert len(result) == 1
    item = result[0]
    assert "signal_score" in item
    assert "signal_class" in item
    assert "market_confirmation" in item
    assert isinstance(item["signal_score"], float)
    assert item["signal_class"] in ("holding_risk", "holding_opportunity",
                                     "external_opportunity", "observed_candidate")


# ---------------------------------------------------------------------------
# 7. generate_recommendation — filters weak signals
# ---------------------------------------------------------------------------


def test_engine_filters_weak_negative_signals():
    """Negative hits with very low signal_score should be filtered."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    # Weak negative signal — should be filtered
    news = [{"impact": "negativo", "related_assets": ["AAPL"],
             "signal_score": 0.1, "pre_score": 0.1, "event_type": "rumor"}]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    # Should NOT trigger risk reduction from this weak signal
    assert rec["action"] == "mantener"


def test_engine_strong_positive_triggers_action():
    """Strong positive signal on holding should trigger action."""
    snapshot = _mock_snapshot()
    analysis = {"alerts": [], "weights_by_asset": {"AAPL": 0.35}, "rebalance_deviation": {},
                "equity_weight": 0.60}
    news = [{"impact": "positivo", "related_assets": ["AAPL"],
             "signal_score": 0.7, "pre_score": 0.7, "event_type": "earnings_beat",
             "title": "AAPL beats expectations", "source_count": 3}]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    assert rec["action"] == "aumentar posición"


# ---------------------------------------------------------------------------
# 8. Market confirmation boosts confidence
# ---------------------------------------------------------------------------


def test_market_confirmed_positive_boosts_confidence():
    """Positive event with market confirmation should boost confidence."""
    snapshot = _mock_snapshot()
    analysis = {"alerts": [], "weights_by_asset": {"AAPL": 0.35}, "rebalance_deviation": {},
                "equity_weight": 0.60}
    news_confirmed = [{
        "impact": "positivo", "related_assets": ["AAPL"],
        "signal_score": 0.7, "pre_score": 0.7, "event_type": "upgrade",
        "title": "AAPL upgraded", "source_count": 2,
        "market_confirmation": {"status": "confirmed", "detail": "PnL +11%", "avg_pnl_pct": 0.11},
    }]
    news_plain = [{
        "impact": "positivo", "related_assets": ["AAPL"],
        "signal_score": 0.7, "pre_score": 0.7, "event_type": "upgrade",
        "title": "AAPL upgraded", "source_count": 2,
    }]

    rec_conf = generate_recommendation(snapshot, analysis, news_confirmed, 0.10)
    rec_plain = generate_recommendation(snapshot, analysis, news_plain, 0.10)

    assert rec_conf["confidence"] > rec_plain["confidence"]


# ---------------------------------------------------------------------------
# 9. External opportunities include signal_class
# ---------------------------------------------------------------------------


def test_external_opportunities_have_signal_class():
    """External opportunities should include signal_class and signal_score."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [{
        "impact": "positivo", "related_assets": ["MELI"],
        "signal_score": 0.6, "pre_score": 0.6, "event_type": "upgrade",
        "title": "MELI grows", "signal_class": "external_opportunity",
    }]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext = rec["external_opportunities"]
    assert len(ext) >= 1
    meli_op = [o for o in ext if o["symbol"] == "MELI"][0]
    assert "signal_class" in meli_op
    assert "signal_score" in meli_op


# ---------------------------------------------------------------------------
# 10. Scoring summary in orchestrator metadata
# ---------------------------------------------------------------------------


def test_orchestrator_includes_scoring_summary():
    """run_cycle metadata should include scoring_summary."""
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db = make_db()
    settings = get_settings()
    original_cooldown = settings.trigger_cooldown_seconds
    settings.trigger_cooldown_seconds = 0

    try:
        result = run_cycle(db, source="test")
    finally:
        settings.trigger_cooldown_seconds = original_cooldown

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    assert "scoring_summary" in meta
    ss = meta["scoring_summary"]
    assert "total_signals" in ss
    assert "by_class" in ss
    assert "by_confirmation" in ss
    assert "top_signals" in ss


# ---------------------------------------------------------------------------
# 11. Legacy non-enriched news still works
# ---------------------------------------------------------------------------


def test_legacy_news_without_signal_fields():
    """News without signal_score/signal_class should still work (legacy compat)."""
    snapshot = _mock_snapshot()
    analysis = {"alerts": [], "weights_by_asset": {"AAPL": 0.35}, "rebalance_deviation": {},
                "equity_weight": 0.60}
    # Legacy format — no signal_score, signal_class, market_confirmation
    news = [{"impact": "positivo", "related_assets": ["AAPL"],
             "pre_score": 0.7, "event_type": "earnings_beat",
             "title": "Good earnings"}]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    # Should still trigger positive action using pre_score as fallback
    assert rec["action"] == "aumentar posición"
    assert rec["confidence"] >= 0.5
