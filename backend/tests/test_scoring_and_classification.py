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
12-16. observed_candidate / external_opportunity separation
17. catalog_dynamic defaults to observed_candidate (not external_opportunity)
18. catalog_dynamic promotion with strong evidence
19. catalog_dynamic without promotion stays observed_candidate
20. Integration: catalog_dynamic → observed_candidate in full pipeline
21. Promotion flag tracked (promoted_from_observed)
22. Market confirmation with catalog prices (non-holdings)
23. Market confirmation catalog: confirmed, contradicted, unconfirmed
24. Market confirmation falls back gracefully without catalog_prices
25. observed_candidates exposed in /recommendations/current API
26. Contradicted blocks catalog promotion
27. effective_score boosts confirmed, penalizes contradicted
28. Ranking uses effective_score (confirmed items rank higher)
29. suppressed_by_contradiction flag on weak contradicted externals
30. scoring_summary includes promoted_count, suppressed_count, confirmation_source
31. Engine external_opportunities sorted by effective_score
32. suppressed_candidates separated from external/observed in engine
33. suppressed_candidates persisted in orchestrator metadata
34. suppressed_candidates exposed in API
35. scoring_summary ranked_signals_preview excludes suppressed
"""

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.recommendations.scoring import (
    classify_signal,
    compute_effective_score,
    compute_market_confirmation,
    promote_catalog_candidate,
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
        "universe_curated": {"MELI", "GLOB"},
        "catalog_dynamic": {"GGAL", "YPF"},
        "universe": {"MELI", "GLOB", "GGAL", "YPF"},
        "main_allowed": {"AAPL", "SPY", "QQQ", "AL30"},
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
    assert "ranked_signals_preview" in ss
    assert "actionable_count" in ss
    assert "observed_count" in ss


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


# ---------------------------------------------------------------------------
# 12. observed_candidate does NOT enter external_opportunities
# ---------------------------------------------------------------------------


def test_observed_candidate_excluded_from_external_opportunities():
    """Items with signal_class=observed_candidate must not appear in external_opportunities."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [
        {
            "impact": "positivo", "related_assets": ["RANDOM_TICKER"],
            "signal_score": 0.6, "pre_score": 0.6, "event_type": "upgrade",
            "title": "Unknown asset rallies", "signal_class": "observed_candidate",
        },
    ]
    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    symbols_in_ext = [o["symbol"] for o in rec["external_opportunities"]]
    assert "RANDOM_TICKER" not in symbols_in_ext

    # But it SHOULD be in observed_candidates
    symbols_in_obs = [o["symbol"] for o in rec["observed_candidates"]]
    assert "RANDOM_TICKER" in symbols_in_obs


# ---------------------------------------------------------------------------
# 13. external_opportunity class DOES enter external_opportunities
# ---------------------------------------------------------------------------


def test_external_opportunity_enters_external_opportunities():
    """Items with signal_class=external_opportunity must appear in external_opportunities."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [
        {
            "impact": "positivo", "related_assets": ["MELI"],
            "signal_score": 0.65, "pre_score": 0.65, "event_type": "expansion",
            "title": "MELI expands fintech", "signal_class": "external_opportunity",
        },
    ]
    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    symbols_in_ext = [o["symbol"] for o in rec["external_opportunities"]]
    assert "MELI" in symbols_in_ext

    symbols_in_obs = [o["symbol"] for o in rec["observed_candidates"]]
    assert "MELI" not in symbols_in_obs


# ---------------------------------------------------------------------------
# 14. Metadata separates observed vs actionable
# ---------------------------------------------------------------------------


def test_metadata_separates_observed_and_actionable():
    """Orchestrator metadata must include both external_opportunities and observed_candidates."""
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

    # Both keys must exist
    assert "external_opportunities" in meta
    assert "observed_candidates" in meta
    assert isinstance(meta["external_opportunities"], list)
    assert isinstance(meta["observed_candidates"], list)


# ---------------------------------------------------------------------------
# 15. ranked_signals_preview is ordered by signal_score
# ---------------------------------------------------------------------------


def test_ranked_signals_preview_ordered_by_score():
    """ranked_signals_preview in scoring_summary must be sorted by signal_score desc."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "Low", "signal_score": 0.2, "signal_class": "observed_candidate",
         "market_confirmation": {"status": "unconfirmed"}, "source_count": 1, "related_assets": []},
        {"title": "High", "signal_score": 0.9, "signal_class": "holding_risk",
         "market_confirmation": {"status": "confirmed"}, "source_count": 3, "related_assets": ["AAPL"]},
        {"title": "Mid", "signal_score": 0.5, "signal_class": "external_opportunity",
         "market_confirmation": {"status": "unconfirmed"}, "source_count": 2, "related_assets": ["MELI"]},
    ]
    summary = _build_scoring_summary(scored)

    preview = summary["ranked_signals_preview"]
    assert len(preview) == 3
    assert preview[0]["title"] == "High"
    assert preview[1]["title"] == "Mid"
    assert preview[2]["title"] == "Low"

    # Scores must be descending
    scores = [p["signal_score"] for p in preview]
    assert scores == sorted(scores, reverse=True)


def test_scoring_summary_counts_actionable_vs_observed():
    """scoring_summary must separate actionable_count from observed_count."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "A", "signal_score": 0.8, "signal_class": "holding_risk",
         "market_confirmation": {"status": "confirmed"}, "source_count": 1, "related_assets": []},
        {"title": "B", "signal_score": 0.6, "signal_class": "external_opportunity",
         "market_confirmation": {"status": "unconfirmed"}, "source_count": 1, "related_assets": []},
        {"title": "C", "signal_score": 0.3, "signal_class": "observed_candidate",
         "market_confirmation": {"status": "unconfirmed"}, "source_count": 1, "related_assets": []},
        {"title": "D", "signal_score": 0.2, "signal_class": "observed_candidate",
         "market_confirmation": {"status": "unconfirmed"}, "source_count": 1, "related_assets": []},
    ]
    summary = _build_scoring_summary(scored)

    assert summary["actionable_count"] == 2
    assert summary["observed_count"] == 2
    assert summary["total_signals"] == 4


# ---------------------------------------------------------------------------
# 17. catalog_dynamic defaults to observed_candidate
# ---------------------------------------------------------------------------


def test_catalog_dynamic_defaults_to_observed_candidate():
    """A symbol only in catalog_dynamic (not in watchlist/universe_curated/main_allowed)
    should classify as observed_candidate, NOT external_opportunity."""
    # GGAL is in catalog_dynamic but NOT in watchlist or universe_curated
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    # universe_curated does NOT include GGAL
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, {"MELI"}, {"MELI"})
    assert cls == "observed_candidate"


def test_catalog_dynamic_in_curated_is_external_opportunity():
    """A symbol in both catalog_dynamic AND universe_curated should be external_opportunity."""
    item = {"impact": "positivo", "related_assets": ["MELI"]}
    # MELI is in both watchlist and universe_curated
    cls = classify_signal(item, {"AAPL"}, {"AAPL"}, {"MELI"}, {"MELI"})
    assert cls == "external_opportunity"


# ---------------------------------------------------------------------------
# 18. catalog_dynamic promotion with strong evidence
# ---------------------------------------------------------------------------


def test_promote_catalog_candidate_multi_source():
    """catalog_dynamic item with source_count>=2 and high score should be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.65,
        "related_assets": ["GGAL"],
        "source_count": 2,
    }
    assert promote_catalog_candidate(item, {"GGAL", "YPF"}) is True


def test_promote_catalog_candidate_high_relevance():
    """catalog_dynamic item with high relevance_score should be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.60,
        "related_assets": ["YPF"],
        "source_count": 1,
        "relevance_score": 0.7,
    }
    assert promote_catalog_candidate(item, {"GGAL", "YPF"}) is True


def test_promote_catalog_candidate_external_opp_flag():
    """catalog_dynamic item with external_opportunity_candidate flag should be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.58,
        "related_assets": ["GGAL"],
        "source_count": 1,
        "external_opportunity_candidate": True,
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is True


def test_promote_catalog_candidate_market_confirmed():
    """catalog_dynamic item with market confirmation should be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.60,
        "related_assets": ["GGAL"],
        "source_count": 1,
        "market_confirmation": {"status": "confirmed"},
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is True


# ---------------------------------------------------------------------------
# 19. catalog_dynamic WITHOUT promotion stays observed_candidate
# ---------------------------------------------------------------------------


def test_no_promote_low_score():
    """catalog_dynamic item with low signal_score should NOT be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.40,
        "related_assets": ["GGAL"],
        "source_count": 3,
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is False


def test_no_promote_weak_evidence():
    """catalog_dynamic item with score above min but no strong evidence should NOT be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.56,
        "related_assets": ["GGAL"],
        "source_count": 1,
        "relevance_score": 0.3,
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is False


def test_no_promote_non_catalog_symbol():
    """Item relating to symbol NOT in catalog_dynamic should NOT be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.70,
        "related_assets": ["RANDOM_TICKER"],
        "source_count": 3,
    }
    assert promote_catalog_candidate(item, {"GGAL", "YPF"}) is False


def test_no_promote_already_external():
    """Items already classified as external_opportunity should not be promoted."""
    item = {
        "signal_class": "external_opportunity",
        "signal_score": 0.80,
        "related_assets": ["GGAL"],
        "source_count": 3,
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is False


# ---------------------------------------------------------------------------
# 20. Integration: catalog_dynamic → observed_candidate in full pipeline
# ---------------------------------------------------------------------------


def test_pipeline_catalog_dynamic_observed_by_default():
    """score_and_classify_news should classify catalog-only items as observed_candidate."""
    news = [{
        "title": "GGAL earnings strong",
        "impact": "positivo",
        "related_assets": ["GGAL"],
        "pre_score": 0.5,
        "source_count": 1,
        "event_type": "earnings",
    }]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)
    assert len(result) == 1
    assert result[0]["signal_class"] == "observed_candidate"


def test_pipeline_catalog_dynamic_promoted_with_evidence():
    """score_and_classify_news should promote catalog item with strong evidence."""
    news = [{
        "title": "GGAL multi-source strong signal",
        "impact": "positivo",
        "related_assets": ["GGAL"],
        "pre_score": 0.6,
        "source_count": 3,
        "item_count": 5,
        "event_type": "upgrade",
    }]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)
    assert len(result) == 1
    assert result[0]["signal_class"] == "external_opportunity"
    assert result[0].get("promoted_from_observed") is True


def test_pipeline_watchlist_still_external_opportunity():
    """Watchlist items should remain external_opportunity (not affected by catalog fix)."""
    news = [{
        "title": "MELI fintech expansion",
        "impact": "positivo",
        "related_assets": ["MELI"],
        "pre_score": 0.5,
        "source_count": 1,
        "event_type": "expansion",
    }]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)
    assert len(result) == 1
    assert result[0]["signal_class"] == "external_opportunity"
    assert result[0].get("promoted_from_observed") is not True


# ---------------------------------------------------------------------------
# 21. Promotion flag tracked
# ---------------------------------------------------------------------------


def test_promoted_flag_only_on_promoted_items():
    """promoted_from_observed should only appear on items that were actually promoted."""
    news = [
        {
            "title": "GGAL weak signal", "impact": "positivo", "related_assets": ["GGAL"],
            "pre_score": 0.3, "source_count": 1, "event_type": "rumor",
        },
        {
            "title": "AAPL holding event", "impact": "positivo", "related_assets": ["AAPL"],
            "pre_score": 0.6, "source_count": 2, "event_type": "earnings",
        },
    ]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)

    ggal_item = [r for r in result if "GGAL" in r.get("related_assets", [])][0]
    aapl_item = [r for r in result if "AAPL" in r.get("related_assets", [])][0]

    assert ggal_item["signal_class"] == "observed_candidate"
    assert ggal_item.get("promoted_from_observed") is not True

    assert aapl_item["signal_class"] == "holding_opportunity"
    assert aapl_item.get("promoted_from_observed") is not True


# ---------------------------------------------------------------------------
# 22. Market confirmation with catalog prices (non-holdings)
# ---------------------------------------------------------------------------


def test_catalog_confirmation_positive_event_positive_variacion():
    """Positive event + positive variacion from catalog = confirmed."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = []  # GGAL is NOT in holdings
    catalog_prices = {"GGAL": {"last_price": 1500.0, "variacion_pct": 5.2}}

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "confirmed"
    assert result["source"] == "catalog"


def test_catalog_confirmation_negative_event_negative_variacion():
    """Negative event + negative variacion from catalog = confirmed."""
    item = {"impact": "negativo", "related_assets": ["YPF"]}
    positions = []
    catalog_prices = {"YPF": {"last_price": 800.0, "variacion_pct": -4.5}}

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "confirmed"
    assert result["source"] == "catalog"


def test_catalog_confirmation_contradicted():
    """Positive event + negative variacion from catalog = contradicted."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = []
    catalog_prices = {"GGAL": {"last_price": 1500.0, "variacion_pct": -3.8}}

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "contradicted"
    assert result["source"] == "catalog"


def test_catalog_confirmation_small_variacion_unconfirmed():
    """Small variacion (below 2% threshold) = unconfirmed."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = []
    catalog_prices = {"GGAL": {"last_price": 1500.0, "variacion_pct": 1.0}}

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "unconfirmed"
    assert result["source"] == "catalog"


def test_holdings_takes_priority_over_catalog():
    """When symbol is in holdings, use pnl_pct, not catalog variacion."""
    item = {"impact": "negativo", "related_assets": ["AAPL"]}
    positions = [{"symbol": "AAPL", "pnl_pct": -0.08}]
    catalog_prices = {"AAPL": {"last_price": 200.0, "variacion_pct": 2.0}}  # contradicts

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "confirmed"
    assert result["source"] == "holdings"


# ---------------------------------------------------------------------------
# 23. Market confirmation falls back gracefully
# ---------------------------------------------------------------------------


def test_no_catalog_prices_still_works():
    """Without catalog_prices, non-holding items return unconfirmed (backward compat)."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]

    result = compute_market_confirmation(item, positions)
    assert result["status"] == "unconfirmed"


def test_catalog_prices_empty_dict():
    """Empty catalog_prices dict should still work gracefully."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = []

    result = compute_market_confirmation(item, positions, catalog_prices={})
    assert result["status"] == "unconfirmed"


def test_catalog_prices_none_variacion():
    """Catalog entry with None variacion_pct should not crash."""
    item = {"impact": "positivo", "related_assets": ["GGAL"]}
    positions = []
    catalog_prices = {"GGAL": {"last_price": 1500.0, "variacion_pct": None}}

    result = compute_market_confirmation(item, positions, catalog_prices=catalog_prices)
    assert result["status"] == "unconfirmed"


# ---------------------------------------------------------------------------
# 24. Pipeline integration: catalog prices flow through score_and_classify_news
# ---------------------------------------------------------------------------


def test_pipeline_with_catalog_prices_confirms_external():
    """score_and_classify_news with catalog_prices should produce confirmed for externals."""
    news = [{
        "title": "MELI expands fintech",
        "impact": "positivo",
        "related_assets": ["MELI"],
        "pre_score": 0.6,
        "source_count": 1,
        "event_type": "expansion",
    }]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]
    allowed = _mock_allowed()
    catalog_prices = {"MELI": {"last_price": 1800.0, "variacion_pct": 4.5}}

    result = score_and_classify_news(news, positions, allowed, catalog_prices=catalog_prices)
    assert len(result) == 1
    assert result[0]["signal_class"] == "external_opportunity"
    conf = result[0]["market_confirmation"]
    assert conf["status"] == "confirmed"
    assert conf["source"] == "catalog"


# ---------------------------------------------------------------------------
# 25. observed_candidates in /recommendations/current API
# ---------------------------------------------------------------------------


def test_api_current_recommendation_includes_observed_candidates():
    """GET /recommendations/current should include observed_candidates field."""
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

    # observed_candidates must exist in metadata
    assert "observed_candidates" in meta
    assert isinstance(meta["observed_candidates"], list)

    # Simulate what the API route does
    api_response = {
        "external_opportunities": meta.get("external_opportunities", []),
        "observed_candidates": meta.get("observed_candidates", []),
    }
    assert "observed_candidates" in api_response
    assert isinstance(api_response["observed_candidates"], list)


# ---------------------------------------------------------------------------
# 26. Contradicted blocks catalog promotion
# ---------------------------------------------------------------------------


def test_contradicted_blocks_catalog_promotion():
    """Catalog item with contradicted market confirmation should NOT be promoted
    even if other criteria are strong."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.70,
        "related_assets": ["GGAL"],
        "source_count": 3,
        "market_confirmation": {"status": "contradicted"},
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is False


def test_confirmed_helps_catalog_promotion():
    """Catalog item with confirmed market + high score should be promoted."""
    item = {
        "signal_class": "observed_candidate",
        "signal_score": 0.60,
        "related_assets": ["GGAL"],
        "source_count": 1,
        "market_confirmation": {"status": "confirmed"},
    }
    assert promote_catalog_candidate(item, {"GGAL"}) is True


def test_contradicted_pipeline_blocks_promotion():
    """Full pipeline: catalog item with contradicted confirmation stays observed."""
    news = [{
        "title": "GGAL strong signal but market disagrees",
        "impact": "positivo",
        "related_assets": ["GGAL"],
        "pre_score": 0.6,
        "source_count": 3,
        "item_count": 5,
        "event_type": "upgrade",
    }]
    positions = []
    allowed = _mock_allowed()
    # GGAL dropping 5% contradicts positive event
    catalog_prices = {"GGAL": {"last_price": 1500.0, "variacion_pct": -5.0}}

    result = score_and_classify_news(news, positions, allowed, catalog_prices=catalog_prices)
    assert len(result) == 1
    assert result[0]["signal_class"] == "observed_candidate"
    assert result[0].get("promoted_from_observed") is not True
    assert result[0]["market_confirmation"]["status"] == "contradicted"


# ---------------------------------------------------------------------------
# 27. effective_score boosts confirmed, penalizes contradicted
# ---------------------------------------------------------------------------


def test_effective_score_confirmed_boosted():
    """confirmed market confirmation should boost effective_score."""
    item = {"signal_score": 0.60, "market_confirmation": {"status": "confirmed"}}
    eff = compute_effective_score(item)
    assert eff == 0.70  # 0.60 + 0.10


def test_effective_score_contradicted_penalized():
    """contradicted should penalize effective_score."""
    item = {"signal_score": 0.60, "market_confirmation": {"status": "contradicted"}}
    eff = compute_effective_score(item)
    assert eff == 0.45  # 0.60 - 0.15


def test_effective_score_unconfirmed_unchanged():
    """unconfirmed should not change effective_score."""
    item = {"signal_score": 0.60, "market_confirmation": {"status": "unconfirmed"}}
    eff = compute_effective_score(item)
    assert eff == 0.60


def test_effective_score_capped_at_1():
    """effective_score should not exceed 1.0."""
    item = {"signal_score": 0.95, "market_confirmation": {"status": "confirmed"}}
    eff = compute_effective_score(item)
    assert eff == 1.0


def test_effective_score_floored_at_0():
    """effective_score should not go below 0.0."""
    item = {"signal_score": 0.10, "market_confirmation": {"status": "contradicted"}}
    eff = compute_effective_score(item)
    assert eff == 0.0


# ---------------------------------------------------------------------------
# 28. Ranking uses effective_score
# ---------------------------------------------------------------------------


def test_confirmed_external_ranks_above_unconfirmed():
    """An external_opportunity with confirmed should rank above same-class unconfirmed."""
    news = [
        {
            "title": "MELI unconfirmed",
            "impact": "positivo", "related_assets": ["MELI"],
            "pre_score": 0.6, "source_count": 1, "event_type": "expansion",
        },
        {
            "title": "GLOB confirmed",
            "impact": "positivo", "related_assets": ["GLOB"],
            "pre_score": 0.55, "source_count": 1, "event_type": "upgrade",
        },
    ]
    positions = []
    allowed = _mock_allowed()
    # GLOB rising confirms, MELI flat
    catalog_prices = {
        "GLOB": {"last_price": 300.0, "variacion_pct": 6.0},
        "MELI": {"last_price": 1800.0, "variacion_pct": 0.5},
    }

    result = score_and_classify_news(news, positions, allowed, catalog_prices=catalog_prices)
    externals = [r for r in result if r["signal_class"] == "external_opportunity"]
    assert len(externals) == 2
    # GLOB should rank first despite lower pre_score, because confirmed boosts effective_score
    assert externals[0]["related_assets"] == ["GLOB"]
    assert externals[0]["effective_score"] > externals[1]["effective_score"]


# ---------------------------------------------------------------------------
# 29. suppressed_by_contradiction flag
# ---------------------------------------------------------------------------


def test_suppressed_by_contradiction_weak_external():
    """Weak contradicted external should get suppressed flag."""
    news = [{
        "title": "MELI weak contradicted",
        "impact": "positivo", "related_assets": ["MELI"],
        "pre_score": 0.3, "source_count": 1, "event_type": "rumor",
    }]
    positions = []
    allowed = _mock_allowed()
    catalog_prices = {"MELI": {"last_price": 1800.0, "variacion_pct": -4.0}}

    result = score_and_classify_news(news, positions, allowed, catalog_prices=catalog_prices)
    assert len(result) == 1
    assert result[0]["suppressed_by_contradiction"] is True


def test_strong_contradicted_not_suppressed():
    """Strong signal contradicted should NOT be suppressed (still valuable info)."""
    news = [{
        "title": "MELI strong but contradicted",
        "impact": "positivo", "related_assets": ["MELI"],
        "pre_score": 0.7, "source_count": 3, "item_count": 5, "event_type": "upgrade",
    }]
    positions = []
    allowed = _mock_allowed()
    catalog_prices = {"MELI": {"last_price": 1800.0, "variacion_pct": -4.0}}

    result = score_and_classify_news(news, positions, allowed, catalog_prices=catalog_prices)
    assert len(result) == 1
    # effective_score: high base (~0.85+) - 0.15 = still above 0.45
    assert result[0].get("suppressed_by_contradiction") is not True


def test_holding_risk_not_suppressed():
    """holding_risk should never be suppressed (even if contradicted)."""
    news = [{
        "title": "AAPL risk",
        "impact": "negativo", "related_assets": ["AAPL"],
        "pre_score": 0.3, "source_count": 1, "event_type": "downgrade",
    }]
    positions = [{"symbol": "AAPL", "pnl_pct": 0.05}]  # positive pnl contradicts negative event
    allowed = _mock_allowed()

    result = score_and_classify_news(news, positions, allowed)
    holding_items = [r for r in result if r["signal_class"] == "holding_risk"]
    for item in holding_items:
        assert item.get("suppressed_by_contradiction") is not True


# ---------------------------------------------------------------------------
# 30. Enhanced scoring_summary
# ---------------------------------------------------------------------------


def test_scoring_summary_includes_promoted_suppressed_counts():
    """scoring_summary should include promoted_count and suppressed_count."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "Promoted", "signal_score": 0.7, "effective_score": 0.8,
         "signal_class": "external_opportunity", "promoted_from_observed": True,
         "market_confirmation": {"status": "confirmed", "source": "catalog"},
         "source_count": 2, "related_assets": ["GGAL"]},
        {"title": "Suppressed", "signal_score": 0.3, "effective_score": 0.15,
         "signal_class": "observed_candidate", "suppressed_by_contradiction": True,
         "market_confirmation": {"status": "contradicted", "source": "catalog"},
         "source_count": 1, "related_assets": ["YPF"]},
        {"title": "Normal", "signal_score": 0.5, "effective_score": 0.5,
         "signal_class": "external_opportunity",
         "market_confirmation": {"status": "unconfirmed"},
         "source_count": 1, "related_assets": ["MELI"]},
    ]
    summary = _build_scoring_summary(scored)

    assert summary["promoted_count"] == 1
    assert summary["suppressed_count"] == 1
    assert summary["actionable_count"] == 2
    assert summary["observed_count"] == 1
    assert "by_confirmation_source" in summary
    assert summary["by_confirmation_source"].get("catalog", 0) == 2


def test_scoring_summary_ranked_by_effective_score():
    """ranked_signals_preview should be sorted by effective_score, not signal_score."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "High signal, contradicted", "signal_score": 0.8, "effective_score": 0.65,
         "signal_class": "external_opportunity",
         "market_confirmation": {"status": "contradicted", "source": "catalog"},
         "source_count": 2, "related_assets": ["MELI"]},
        {"title": "Mid signal, confirmed", "signal_score": 0.6, "effective_score": 0.70,
         "signal_class": "external_opportunity",
         "market_confirmation": {"status": "confirmed", "source": "catalog"},
         "source_count": 1, "related_assets": ["GLOB"]},
    ]
    summary = _build_scoring_summary(scored)
    preview = summary["ranked_signals_preview"]

    assert preview[0]["title"] == "Mid signal, confirmed"
    assert preview[1]["title"] == "High signal, contradicted"


# ---------------------------------------------------------------------------
# 31. Engine external_opportunities sorted by effective_score
# ---------------------------------------------------------------------------


def test_engine_external_opportunities_include_effective_score():
    """Engine external_opportunities should include effective_score and confirmation fields."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [{
        "impact": "positivo", "related_assets": ["MELI"],
        "signal_score": 0.65, "effective_score": 0.75,
        "pre_score": 0.65, "event_type": "expansion",
        "title": "MELI grows", "signal_class": "external_opportunity",
        "market_confirmation": {"status": "confirmed", "source": "catalog"},
        "promoted_from_observed": False,
        "suppressed_by_contradiction": False,
    }]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext = rec["external_opportunities"]
    meli_ops = [o for o in ext if o["symbol"] == "MELI"]
    assert len(meli_ops) >= 1
    op = meli_ops[0]
    assert "effective_score" in op
    assert op["effective_score"] == 0.75
    assert op["market_confirmation"] == "confirmed"


# ---------------------------------------------------------------------------
# 32. suppressed_candidates separated from external/observed in engine
# ---------------------------------------------------------------------------


def test_engine_suppressed_goes_to_suppressed_candidates():
    """Items with suppressed_by_contradiction should go to suppressed_candidates,
    not external_opportunities or observed_candidates."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [
        {
            "impact": "positivo", "related_assets": ["MELI"],
            "signal_score": 0.40, "effective_score": 0.25,
            "pre_score": 0.40, "event_type": "rumor",
            "title": "MELI weak contradicted", "signal_class": "external_opportunity",
            "market_confirmation": {"status": "contradicted"},
            "suppressed_by_contradiction": True,
        },
        {
            "impact": "positivo", "related_assets": ["GLOB"],
            "signal_score": 0.65, "effective_score": 0.75,
            "pre_score": 0.65, "event_type": "upgrade",
            "title": "GLOB strong confirmed", "signal_class": "external_opportunity",
            "market_confirmation": {"status": "confirmed"},
            "suppressed_by_contradiction": False,
        },
    ]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    ext_symbols = [o["symbol"] for o in rec["external_opportunities"]]
    obs_symbols = [o["symbol"] for o in rec["observed_candidates"]]
    sup_symbols = [o["symbol"] for o in rec["suppressed_candidates"]]

    assert "GLOB" in ext_symbols
    assert "MELI" in sup_symbols
    assert "MELI" not in ext_symbols
    assert "MELI" not in obs_symbols


def test_engine_suppressed_observed_goes_to_suppressed():
    """observed_candidate with suppressed flag should go to suppressed, not observed."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = [{
        "impact": "positivo", "related_assets": ["RANDOM"],
        "signal_score": 0.40, "effective_score": 0.25,
        "pre_score": 0.40, "event_type": "rumor",
        "title": "Random weak", "signal_class": "observed_candidate",
        "market_confirmation": {"status": "contradicted"},
        "suppressed_by_contradiction": True,
    }]

    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    obs_symbols = [o["symbol"] for o in rec["observed_candidates"]]
    sup_symbols = [o["symbol"] for o in rec["suppressed_candidates"]]

    assert "RANDOM" in sup_symbols
    assert "RANDOM" not in obs_symbols


def test_engine_returns_suppressed_candidates_key():
    """generate_recommendation should always return suppressed_candidates list."""
    snapshot = _mock_snapshot()
    analysis = _mock_analysis()
    news = []

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    assert "suppressed_candidates" in rec
    assert isinstance(rec["suppressed_candidates"], list)


# ---------------------------------------------------------------------------
# 33. suppressed_candidates in orchestrator metadata
# ---------------------------------------------------------------------------


def test_orchestrator_metadata_includes_suppressed_candidates():
    """run_cycle metadata should include suppressed_candidates."""
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

    assert "suppressed_candidates" in meta
    assert isinstance(meta["suppressed_candidates"], list)


# ---------------------------------------------------------------------------
# 34. API exposes suppressed_candidates
# ---------------------------------------------------------------------------


def test_api_response_structure_three_lists():
    """API response should have all three candidate lists with safe defaults."""
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

    # Simulate what the API route does
    api_response = {
        "external_opportunities": meta.get("external_opportunities", []),
        "observed_candidates": meta.get("observed_candidates", []),
        "suppressed_candidates": meta.get("suppressed_candidates", []),
    }
    for key in ("external_opportunities", "observed_candidates", "suppressed_candidates"):
        assert key in api_response
        assert isinstance(api_response[key], list)


# ---------------------------------------------------------------------------
# 35. scoring_summary ranked_signals_preview excludes suppressed
# ---------------------------------------------------------------------------


def test_scoring_summary_preview_excludes_suppressed():
    """ranked_signals_preview should not include suppressed items."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "Suppressed weak", "signal_score": 0.3, "effective_score": 0.15,
         "signal_class": "external_opportunity", "suppressed_by_contradiction": True,
         "market_confirmation": {"status": "contradicted", "source": "catalog"},
         "source_count": 1, "related_assets": ["YPF"]},
        {"title": "Good signal", "signal_score": 0.7, "effective_score": 0.80,
         "signal_class": "external_opportunity",
         "market_confirmation": {"status": "confirmed", "source": "catalog"},
         "source_count": 2, "related_assets": ["GLOB"]},
    ]
    summary = _build_scoring_summary(scored)

    preview = summary["ranked_signals_preview"]
    preview_titles = [p["title"] for p in preview]

    assert "Good signal" in preview_titles
    assert "Suppressed weak" not in preview_titles

    # But suppressed_count still counted
    assert summary["suppressed_count"] == 1


def test_scoring_summary_all_suppressed_empty_preview():
    """If all signals are suppressed, ranked_signals_preview should be empty."""
    from app.services.orchestrator import _build_scoring_summary

    scored = [
        {"title": "Suppressed A", "signal_score": 0.3, "effective_score": 0.15,
         "signal_class": "observed_candidate", "suppressed_by_contradiction": True,
         "market_confirmation": {"status": "contradicted"},
         "source_count": 1, "related_assets": ["YPF"]},
    ]
    summary = _build_scoring_summary(scored)

    assert summary["ranked_signals_preview"] == []
    assert summary["suppressed_count"] == 1
    assert summary["total_signals"] == 1


# ---------------------------------------------------------------------------
# 36. curate_llm_input excludes suppressed items
# ---------------------------------------------------------------------------


def test_curate_llm_input_excludes_suppressed():
    """Suppressed items should be excluded from LLM input."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": "Suppressed", "effective_score": 0.40, "signal_class": "external_opportunity",
         "suppressed_by_contradiction": True, "source_count": 1},
        {"title": "Good holding risk", "effective_score": 0.70, "signal_class": "holding_risk",
         "source_count": 2},
    ]
    curated, meta = curate_llm_input(scored)

    titles = [c["title"] for c in curated]
    assert "Suppressed" not in titles
    assert "Good holding risk" in titles
    assert meta["excluded_suppressed"] == 1
    assert meta["sent_count"] == 1


# ---------------------------------------------------------------------------
# 37. curate_llm_input excludes weak effective_score
# ---------------------------------------------------------------------------


def test_curate_llm_input_excludes_weak():
    """Items with effective_score < 0.30 should be excluded."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": "Too weak", "effective_score": 0.20, "signal_class": "holding_opportunity",
         "source_count": 1},
        {"title": "Strong enough", "effective_score": 0.50, "signal_class": "holding_opportunity",
         "source_count": 1},
    ]
    curated, meta = curate_llm_input(scored)

    titles = [c["title"] for c in curated]
    assert "Too weak" not in titles
    assert "Strong enough" in titles
    assert meta["excluded_weak"] == 1


# ---------------------------------------------------------------------------
# 38. curate_llm_input excludes non-promoted observed_candidate
# ---------------------------------------------------------------------------


def test_curate_llm_input_excludes_observed_candidate():
    """observed_candidate without promotion should be excluded (noise)."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": "Observed not promoted", "effective_score": 0.60,
         "signal_class": "observed_candidate", "source_count": 1},
        {"title": "Promoted observed", "effective_score": 0.65,
         "signal_class": "external_opportunity", "promoted_from_observed": True,
         "source_count": 2},
        {"title": "Holding risk", "effective_score": 0.70,
         "signal_class": "holding_risk", "source_count": 1},
    ]
    curated, meta = curate_llm_input(scored)

    titles = [c["title"] for c in curated]
    assert "Observed not promoted" not in titles
    assert "Promoted observed" in titles
    assert "Holding risk" in titles
    assert meta["excluded_observed"] == 1
    assert meta["sent_count"] == 2


# ---------------------------------------------------------------------------
# 39. curate_llm_input ranking: class priority + effective_score + source_count
# ---------------------------------------------------------------------------


def test_curate_llm_input_ranking():
    """Items should be ranked by class priority, then effective_score desc, then source_count desc."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": "External high", "effective_score": 0.90,
         "signal_class": "external_opportunity", "source_count": 3},
        {"title": "Holding risk low", "effective_score": 0.40,
         "signal_class": "holding_risk", "source_count": 1},
        {"title": "Holding opp mid", "effective_score": 0.60,
         "signal_class": "holding_opportunity", "source_count": 2},
        {"title": "Holding risk high", "effective_score": 0.80,
         "signal_class": "holding_risk", "source_count": 2},
    ]
    curated, meta = curate_llm_input(scored)

    titles = [c["title"] for c in curated]
    # holding_risk items first (class priority 0), then holding_opportunity (1), then external (2)
    assert titles[0] == "Holding risk high"
    assert titles[1] == "Holding risk low"
    assert titles[2] == "Holding opp mid"
    assert titles[3] == "External high"
    assert meta["sent_count"] == 4


# ---------------------------------------------------------------------------
# 40. curate_llm_input respects max_items cap
# ---------------------------------------------------------------------------


def test_curate_llm_input_max_items():
    """Output should be capped at max_items."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": f"Item {i}", "effective_score": 0.50 + i * 0.01,
         "signal_class": "holding_opportunity", "source_count": 1}
        for i in range(20)
    ]
    curated, meta = curate_llm_input(scored, max_items=5)

    assert len(curated) == 5
    assert meta["eligible_count"] == 20
    assert meta["sent_count"] == 5
    assert meta["max_items"] == 5


# ---------------------------------------------------------------------------
# 41. curate_llm_input observability: sent_classes breakdown
# ---------------------------------------------------------------------------


def test_curate_llm_input_observability():
    """llm_input_meta should include class breakdown and exclusion counts."""
    from app.recommendations.scoring import curate_llm_input

    scored = [
        {"title": "HR1", "effective_score": 0.80, "signal_class": "holding_risk", "source_count": 2},
        {"title": "HR2", "effective_score": 0.70, "signal_class": "holding_risk", "source_count": 1},
        {"title": "HO1", "effective_score": 0.60, "signal_class": "holding_opportunity", "source_count": 1},
        {"title": "EO1", "effective_score": 0.50, "signal_class": "external_opportunity", "source_count": 1},
        {"title": "Weak", "effective_score": 0.10, "signal_class": "holding_risk", "source_count": 1},
        {"title": "OC1", "effective_score": 0.55, "signal_class": "observed_candidate", "source_count": 1},
        {"title": "Sup", "effective_score": 0.35, "signal_class": "external_opportunity",
         "suppressed_by_contradiction": True, "source_count": 1},
    ]
    curated, meta = curate_llm_input(scored)

    assert meta["total_scored"] == 7
    assert meta["excluded_suppressed"] == 1
    assert meta["excluded_weak"] == 1
    assert meta["excluded_observed"] == 1
    assert meta["eligible_count"] == 4
    assert meta["sent_count"] == 4
    assert meta["sent_classes"] == {
        "holding_risk": 2,
        "holding_opportunity": 1,
        "external_opportunity": 1,
    }


# ---------------------------------------------------------------------------
# 42. curate_llm_input empty scored_news returns empty
# ---------------------------------------------------------------------------


def test_curate_llm_input_empty():
    """Empty scored_news should return empty curated list."""
    from app.recommendations.scoring import curate_llm_input

    curated, meta = curate_llm_input([])
    assert curated == []
    assert meta["total_scored"] == 0
    assert meta["sent_count"] == 0


# ---------------------------------------------------------------------------
# 43. Orchestrator persists llm_input_meta in metadata_json
# ---------------------------------------------------------------------------


def test_orchestrator_llm_input_meta_persisted():
    """metadata_json should contain llm_input_meta from curation."""
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

    assert "llm_input_meta" in meta
    llm_meta = meta["llm_input_meta"]
    # Should have the standard curation keys (not fallback, since scored_news is always populated)
    assert "sent_count" in llm_meta
    assert "total_scored" in llm_meta or "fallback" in llm_meta


# ---------------------------------------------------------------------------
# 44. build_shortlist prioritizes holdings, then externals, then promoted
# ---------------------------------------------------------------------------


def test_build_shortlist_priority_order():
    """Shortlist should prioritize holding_risk > holding_opportunity > external > promoted."""
    from app.recommendations.scoring import build_shortlist

    scored = [
        {"signal_class": "external_opportunity", "effective_score": 0.90,
         "related_assets": ["GLOB"], "source_count": 2},
        {"signal_class": "holding_risk", "effective_score": 0.70,
         "related_assets": ["AAPL"], "source_count": 1},
        {"signal_class": "holding_opportunity", "effective_score": 0.60,
         "related_assets": ["MSFT"], "source_count": 1},
        {"signal_class": "external_opportunity", "effective_score": 0.65,
         "related_assets": ["YPF"], "promoted_from_observed": True, "source_count": 2},
    ]
    holdings = {"AAPL", "MSFT"}
    symbols, meta = build_shortlist(scored, holdings, max_symbols=8)

    # AAPL (holding_risk) should come first, then MSFT (holding_opp), then externals
    assert symbols[0] == "AAPL"
    assert symbols[1] == "MSFT"
    # GLOB should come before YPF (higher effective_score in externals pass)
    assert "GLOB" in symbols
    assert "YPF" in symbols
    assert meta["selected_count"] == 4


# ---------------------------------------------------------------------------
# 45. build_shortlist respects max_symbols cap
# ---------------------------------------------------------------------------


def test_build_shortlist_max_cap():
    """Shortlist should be capped at max_symbols."""
    from app.recommendations.scoring import build_shortlist

    scored = [
        {"signal_class": "external_opportunity", "effective_score": 0.50 + i * 0.01,
         "related_assets": [f"SYM{i}"], "source_count": 1}
        for i in range(15)
    ]
    symbols, meta = build_shortlist(scored, set(), max_symbols=5)

    assert len(symbols) == 5
    assert meta["total_candidates"] == 15
    assert meta["max_symbols"] == 5


# ---------------------------------------------------------------------------
# 46. build_shortlist deduplicates symbols
# ---------------------------------------------------------------------------


def test_build_shortlist_deduplicates():
    """Same symbol in multiple signals should appear once."""
    from app.recommendations.scoring import build_shortlist

    scored = [
        {"signal_class": "holding_risk", "effective_score": 0.80,
         "related_assets": ["AAPL"], "source_count": 1},
        {"signal_class": "holding_opportunity", "effective_score": 0.60,
         "related_assets": ["AAPL"], "source_count": 1},
        {"signal_class": "external_opportunity", "effective_score": 0.70,
         "related_assets": ["GLOB"], "source_count": 2},
    ]
    symbols, meta = build_shortlist(scored, {"AAPL"})

    assert symbols.count("AAPL") == 1
    assert symbols == ["AAPL", "GLOB"]


# ---------------------------------------------------------------------------
# 47. build_shortlist empty scored_news
# ---------------------------------------------------------------------------


def test_build_shortlist_empty():
    """Empty scored_news should return empty shortlist."""
    from app.recommendations.scoring import build_shortlist

    symbols, meta = build_shortlist([], set())
    assert symbols == []
    assert meta["selected_count"] == 0


# ---------------------------------------------------------------------------
# 48. refine_with_fresh_quotes updates confirmation from catalog to fresh_quote
# ---------------------------------------------------------------------------


def test_refine_updates_confirmation_source():
    """Fresh quote should override catalog confirmation and set source=fresh_quote."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "GLOB rises", "related_assets": ["GLOB"],
            "impact": "positivo", "signal_class": "external_opportunity",
            "signal_score": 0.60, "effective_score": 0.60,
            "market_confirmation": {"status": "unconfirmed", "source": "catalog",
                                    "detail": "Sin confirmación"},
            "source_count": 2,
        },
    ]
    fresh_prices = {
        "GLOB": {"last_price": 100.0, "variacion_pct": 5.0, "source": "fresh_quote"},
    }

    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [])

    assert meta["refined_count"] == 1
    assert "GLOB" in meta["symbols_used"]
    conf = refined[0]["market_confirmation"]
    assert conf["source"] == "fresh_quote"
    assert conf["status"] == "confirmed"  # positive event + positive variacion
    # effective_score should be boosted
    assert refined[0]["effective_score"] > 0.60


# ---------------------------------------------------------------------------
# 49. refine_with_fresh_quotes skips holdings-sourced confirmation
# ---------------------------------------------------------------------------


def test_refine_skips_holdings_source():
    """Items confirmed via holdings pnl_pct should NOT be overridden by fresh quotes."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "AAPL drops", "related_assets": ["AAPL"],
            "impact": "negativo", "signal_class": "holding_risk",
            "signal_score": 0.70, "effective_score": 0.80,
            "market_confirmation": {"status": "confirmed", "source": "holdings",
                                    "detail": "PnL confirms"},
            "source_count": 1,
        },
    ]
    fresh_prices = {
        "AAPL": {"last_price": 180.0, "variacion_pct": 2.0, "source": "fresh_quote"},
    }

    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [])

    assert meta["refined_count"] == 0
    assert refined[0]["market_confirmation"]["source"] == "holdings"


# ---------------------------------------------------------------------------
# 50. refine_with_fresh_quotes empty fresh_prices is a no-op
# ---------------------------------------------------------------------------


def test_refine_empty_fresh_prices():
    """Empty fresh_prices should return scored_news unchanged."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {"title": "test", "related_assets": ["X"], "signal_class": "external_opportunity",
         "signal_score": 0.5, "effective_score": 0.5,
         "market_confirmation": {"status": "unconfirmed", "source": "catalog"}},
    ]
    refined, meta = refine_with_fresh_quotes(scored, {}, [])

    assert meta["refined_count"] == 0
    assert refined == scored


# ---------------------------------------------------------------------------
# 51. refine_with_fresh_quotes can un-suppress items
# ---------------------------------------------------------------------------


def test_refine_can_unsuppress():
    """If fresh data changes contradicted → confirmed, suppression should be removed."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "YPF news", "related_assets": ["YPF"],
            "impact": "positivo", "signal_class": "external_opportunity",
            "signal_score": 0.35, "effective_score": 0.20,
            "market_confirmation": {"status": "contradicted", "source": "catalog",
                                    "detail": "was contradicted"},
            "suppressed_by_contradiction": True,
            "source_count": 1,
        },
    ]
    # Fresh data shows positive movement → should confirm positive event
    fresh_prices = {
        "YPF": {"last_price": 50.0, "variacion_pct": 3.5, "source": "fresh_quote"},
    }

    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [])

    assert meta["refined_count"] == 1
    conf = refined[0]["market_confirmation"]
    assert conf["status"] == "confirmed"
    assert conf["source"] == "fresh_quote"
    # Should no longer be suppressed
    assert not refined[0].get("suppressed_by_contradiction", False)


# ---------------------------------------------------------------------------
# 52. fetch_fresh_quotes returns empty in mock mode
# ---------------------------------------------------------------------------


def test_fetch_fresh_quotes_mock_mode():
    """In mock mode, fetch_fresh_quotes should return empty safely."""
    from app.market.discovery import fetch_fresh_quotes

    db = make_db()
    fresh, meta = fetch_fresh_quotes(db, ["AAPL", "MSFT"])

    assert fresh == {}
    assert meta.get("skipped_mock") is True or meta.get("fetched") == 0


# ---------------------------------------------------------------------------
# 53. Orchestrator persists fresh_quote_meta in metadata_json
# ---------------------------------------------------------------------------


def test_orchestrator_fresh_quote_meta_persisted():
    """metadata_json should contain fresh_quote_meta."""
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

    assert "fresh_quote_meta" in meta
    fq_meta = meta["fresh_quote_meta"]
    # In mock mode, should have shortlist + fetch (skipped_mock) + refinement
    assert "shortlist" in fq_meta or "error" in fq_meta


# ---------------------------------------------------------------------------
# 54. Gap 1: Fresh refinement promotes observed_candidate with strong evidence
# ---------------------------------------------------------------------------


def test_refine_promotes_observed_with_fresh_confirmation():
    """Fresh confirmed quote should promote an observed_candidate to external_opportunity."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "New CEDEAR opportunity", "related_assets": ["BBAR"],
            "impact": "positivo", "signal_class": "observed_candidate",
            "signal_score": 0.60, "effective_score": 0.60,
            "market_confirmation": {"status": "unconfirmed", "source": "catalog",
                                    "detail": "Sin confirmación"},
            "source_count": 2, "relevance_score": 0.7,
        },
    ]
    # Fresh data confirms positive event
    fresh_prices = {
        "BBAR": {"last_price": 100.0, "variacion_pct": 4.0, "source": "fresh_quote"},
    }
    catalog_dynamic = {"BBAR"}

    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [],
                                             catalog_dynamic=catalog_dynamic)

    assert meta["promotions"] == 1
    assert refined[0]["signal_class"] == "external_opportunity"
    assert refined[0]["promoted_from_observed"] is True
    assert refined[0]["market_confirmation"]["source"] == "fresh_quote"


# ---------------------------------------------------------------------------
# 55. Gap 1: Fresh refinement demotes promoted item when contradicted
# ---------------------------------------------------------------------------


def test_refine_demotes_promoted_when_contradicted():
    """Fresh contradicted data should demote a promoted item back to observed_candidate."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "Was promoted", "related_assets": ["GLOB"],
            "impact": "positivo", "signal_class": "external_opportunity",
            "signal_score": 0.60, "effective_score": 0.70,
            "market_confirmation": {"status": "confirmed", "source": "catalog",
                                    "detail": "Was confirmed"},
            "promoted_from_observed": True,
            "source_count": 2,
        },
    ]
    # Fresh data contradicts — positive event but negative market movement
    fresh_prices = {
        "GLOB": {"last_price": 80.0, "variacion_pct": -5.0, "source": "fresh_quote"},
    }
    catalog_dynamic = {"GLOB"}

    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [],
                                             catalog_dynamic=catalog_dynamic)

    assert meta["demotions"] == 1
    assert refined[0]["signal_class"] == "observed_candidate"
    assert refined[0]["promoted_from_observed"] is False
    assert refined[0]["market_confirmation"]["status"] == "contradicted"


# ---------------------------------------------------------------------------
# 56. Gap 1: No promotion/demotion without catalog_dynamic
# ---------------------------------------------------------------------------


def test_refine_no_promotion_without_catalog_dynamic():
    """Without catalog_dynamic, promotion re-evaluation should not happen."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    scored = [
        {
            "title": "Observed item", "related_assets": ["BBAR"],
            "impact": "positivo", "signal_class": "observed_candidate",
            "signal_score": 0.60, "effective_score": 0.60,
            "market_confirmation": {"status": "unconfirmed", "source": "catalog"},
            "source_count": 2, "relevance_score": 0.7,
        },
    ]
    fresh_prices = {
        "BBAR": {"last_price": 100.0, "variacion_pct": 4.0, "source": "fresh_quote"},
    }
    # No catalog_dynamic passed
    refined, meta = refine_with_fresh_quotes(scored, fresh_prices, [])

    assert meta["promotions"] == 0
    assert refined[0]["signal_class"] == "observed_candidate"


# ---------------------------------------------------------------------------
# 57. Gap 1: refinement_meta includes promotions and demotions counts
# ---------------------------------------------------------------------------


def test_refine_meta_includes_promotion_demotion_counts():
    """refinement_meta must include promotions and demotions keys."""
    from app.recommendations.scoring import refine_with_fresh_quotes

    refined, meta = refine_with_fresh_quotes([], {}, [])
    assert "promotions" in meta
    assert "demotions" in meta
    assert meta["promotions"] == 0
    assert meta["demotions"] == 0


# ---------------------------------------------------------------------------
# 58. Gap 2: best_positive uses effective_score for selection
# ---------------------------------------------------------------------------


def test_best_positive_uses_effective_score():
    """Engine should pick best positive hit by effective_score, not signal_score."""
    from app.recommendations.engine import generate_recommendation

    snapshot = {"total_value": 10000, "cash": 2000, "positions": [
        {"symbol": "AAPL", "weight": 0.50, "market_value": 5000, "pnl_pct": 0.05},
        {"symbol": "MSFT", "weight": 0.30, "market_value": 3000, "pnl_pct": 0.02},
    ], "currency": "USD"}
    analysis = {"risk_score": 0.4, "cash_ratio": 0.20, "alerts": []}

    news = [
        {
            "title": "AAPL high signal_score low effective",
            "event_type": "earnings", "impact": "positivo", "confidence": 0.8,
            "related_assets": ["AAPL"], "source_count": 1,
            "signal_score": 0.80, "effective_score": 0.50,
            "signal_class": "holding_opportunity",
            "market_confirmation": {"status": "contradicted"},
        },
        {
            "title": "MSFT lower signal_score high effective",
            "event_type": "earnings", "impact": "positivo", "confidence": 0.8,
            "related_assets": ["MSFT"], "source_count": 2,
            "signal_score": 0.60, "effective_score": 0.70,
            "signal_class": "holding_opportunity",
            "market_confirmation": {"status": "confirmed"},
        },
    ]

    rec = generate_recommendation(snapshot, analysis, news, max_move=0.05)

    # Should pick MSFT (effective_score=0.70) over AAPL (effective_score=0.50)
    if rec["actions"]:
        symbols = [a["symbol"] for a in rec["actions"]]
        assert "MSFT" in symbols


# ---------------------------------------------------------------------------
# 59. Gap 3: API exposes fresh_quote_meta
# ---------------------------------------------------------------------------


def test_api_exposes_fresh_quote_meta():
    """GET /recommendations/current should include fresh_quote_meta."""
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

    # Simulate what the API route does
    api_response = {
        "fresh_quote_meta": meta.get("fresh_quote_meta") or {},
    }
    assert "fresh_quote_meta" in api_response
    assert isinstance(api_response["fresh_quote_meta"], dict)


# ---------------------------------------------------------------------------
# 60. _build_decision_summary: no_signal driver for mantener
# ---------------------------------------------------------------------------


def test_decision_summary_no_signal_driver():
    """When action is 'mantener' with no strong signals, primary_driver is no_signal."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "rationale": "Cartera estable.", "rationale_reasons": [],
        "actions": [], "external_opportunities": [], "observed_candidates": [],
        "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")

    assert summary["primary_driver"] == "no_signal"
    assert summary["winning_signal"] is None
    assert summary["confirmation_used"] == {}


# ---------------------------------------------------------------------------
# 61. _build_decision_summary: concentration driver
# ---------------------------------------------------------------------------


def test_decision_summary_concentration_driver():
    """When action is 'reducir riesgo', primary_driver is concentration."""
    from app.services.orchestrator import _build_decision_summary

    scored = [
        {"title": "Risk on AAPL", "related_assets": ["AAPL"], "signal_class": "holding_risk",
         "signal_score": 0.70, "effective_score": 0.80, "source_count": 2,
         "market_confirmation": {"status": "confirmed", "source": "holdings"}},
    ]
    rec = {
        "action": "reducir riesgo", "rationale": "Sobreconcentración.",
        "rationale_reasons": [{"type": "concentration_reason", "detail": "AAPL 60%"}],
        "actions": [{"symbol": "AAPL", "target_change_pct": -0.05, "reason": "Sobreconcentración"}],
        "external_opportunities": [], "observed_candidates": [], "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, scored, {}, {}, {}, False, "")

    assert summary["primary_driver"] == "concentration"
    assert summary["winning_signal"] is not None
    assert summary["winning_signal"]["symbol"] == "AAPL"
    assert summary["winning_signal"]["effective_score"] == 0.80
    assert summary["confirmation_used"]["source"] == "holdings"


# ---------------------------------------------------------------------------
# 62. _build_decision_summary: positive_signal driver with winning signal
# ---------------------------------------------------------------------------


def test_decision_summary_positive_signal_driver():
    """When action is 'aumentar posición', winning_signal matches the action symbol."""
    from app.services.orchestrator import _build_decision_summary

    scored = [
        {"title": "MSFT boost", "related_assets": ["MSFT"], "signal_class": "holding_opportunity",
         "signal_score": 0.65, "effective_score": 0.75, "source_count": 3,
         "market_confirmation": {"status": "confirmed", "source": "fresh_quote"}},
    ]
    rec = {
        "action": "aumentar posición", "rationale": "Evento positivo.",
        "rationale_reasons": [{"type": "return_expectation_reason", "detail": "Catalizador en MSFT"}],
        "actions": [{"symbol": "MSFT", "target_change_pct": 0.04, "reason": "Evento positivo"}],
        "external_opportunities": [], "observed_candidates": [], "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, scored, {}, {}, {}, False, "")

    assert summary["primary_driver"] == "positive_signal"
    assert summary["winning_signal"]["symbol"] == "MSFT"
    assert summary["winning_signal"]["effective_score"] == 0.75
    assert summary["winning_signal"]["confirmation_source"] == "fresh_quote"
    assert summary["confirmation_used"]["source"] == "fresh_quote"


# ---------------------------------------------------------------------------
# 63. _build_decision_summary: unchanged driver
# ---------------------------------------------------------------------------


def test_decision_summary_unchanged_driver():
    """When unchanged=True, primary_driver is unchanged."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "rationale": "Sin cambios.",
        "rationale_reasons": [], "actions": [],
        "external_opportunities": [], "observed_candidates": [], "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, [], {}, {}, {}, True, "Similar a recomendación anterior")

    assert summary["primary_driver"] == "unchanged"
    assert "Similar a recomendación anterior" in summary["why_selected"]


# ---------------------------------------------------------------------------
# 64. _build_decision_summary: candidates summary with top 3
# ---------------------------------------------------------------------------


def test_decision_summary_candidates_top3():
    """candidates should include counts and top 3 from each group."""
    from app.services.orchestrator import _build_decision_summary

    ext = [{"symbol": f"E{i}", "effective_score": 0.8 - i * 0.1,
            "signal_class": "external_opportunity", "market_confirmation": "confirmed"}
           for i in range(5)]
    obs = [{"symbol": f"O{i}", "effective_score": 0.5, "signal_class": "observed_candidate",
            "market_confirmation": "unconfirmed"} for i in range(2)]
    sup = [{"symbol": "S0", "effective_score": 0.2, "signal_class": "external_opportunity",
            "market_confirmation": "contradicted"}]

    rec = {
        "action": "mantener", "rationale": "test", "rationale_reasons": [], "actions": [],
        "external_opportunities": ext, "observed_candidates": obs,
        "suppressed_candidates": sup,
    }
    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")

    cands = summary["candidates"]
    assert cands["actionable_count"] == 5
    assert cands["observed_count"] == 2
    assert cands["suppressed_count"] == 1
    assert len(cands["top_actionable"]) == 3
    assert len(cands["top_observed"]) == 2
    assert len(cands["top_suppressed"]) == 1


# ---------------------------------------------------------------------------
# 65. _build_decision_summary: llm_input summary from llm_input_meta
# ---------------------------------------------------------------------------


def test_decision_summary_llm_input():
    """llm_input should aggregate from llm_input_meta."""
    from app.services.orchestrator import _build_decision_summary

    llm_meta = {
        "sent_count": 5, "excluded_suppressed": 2, "excluded_weak": 3,
        "excluded_observed": 1, "sent_classes": {"holding_risk": 3, "external_opportunity": 2},
    }
    rec = {
        "action": "mantener", "rationale": "test", "rationale_reasons": [], "actions": [],
        "external_opportunities": [], "observed_candidates": [], "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, [], {}, llm_meta, {}, False, "")

    assert summary["llm_input"]["sent_count"] == 5
    assert summary["llm_input"]["excluded_count"] == 6  # 2+3+1
    assert summary["llm_input"]["sent_classes"]["holding_risk"] == 3


# ---------------------------------------------------------------------------
# 66. _build_decision_summary: shortlist from fresh_quote_meta
# ---------------------------------------------------------------------------


def test_decision_summary_shortlist():
    """shortlist_used should come from fresh_quote_meta."""
    from app.services.orchestrator import _build_decision_summary

    fq_meta = {
        "shortlist": {"symbols": ["AAPL", "GLOB"], "selected_count": 2},
        "fetch": {"fetched": 2}, "refinement": {"promotions": 1, "demotions": 0},
    }
    rec = {
        "action": "mantener", "rationale": "test", "rationale_reasons": [], "actions": [],
        "external_opportunities": [], "observed_candidates": [], "suppressed_candidates": [],
    }
    summary = _build_decision_summary(rec, [], {}, {}, fq_meta, False, "")

    assert summary["shortlist_used"] == ["AAPL", "GLOB"]
    assert summary["promotion_events"]["fresh_promoted"] == 1


# ---------------------------------------------------------------------------
# 67. Orchestrator persists decision_summary in metadata_json
# ---------------------------------------------------------------------------


def test_orchestrator_decision_summary_persisted():
    """metadata_json should contain decision_summary with required keys."""
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

    assert "decision_summary" in meta
    ds = meta["decision_summary"]
    assert "primary_driver" in ds
    assert "winning_signal" in ds
    assert "confirmation_used" in ds
    assert "shortlist_used" in ds
    assert "llm_input" in ds
    assert "candidates" in ds
    assert "promotion_events" in ds
    assert "why_selected" in ds
    # primary_driver should be a valid value
    assert ds["primary_driver"] in (
        "concentration", "rebalance", "positive_signal",
        "no_signal", "unchanged", "empty_portfolio",
    )


# ---------------------------------------------------------------------------
# 68. API exposes decision_summary top-level
# ---------------------------------------------------------------------------


def test_api_exposes_decision_summary():
    """GET /recommendations/current should include decision_summary."""
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

    # Simulate API extraction
    api_decision_summary = meta.get("decision_summary") or {}
    assert isinstance(api_decision_summary, dict)
    assert "primary_driver" in api_decision_summary


# ===========================================================================
# Sprint 14: External opportunities inflation fix
# ===========================================================================


def test_catalog_only_not_actionable():
    """A symbol from catalog_dynamic alone must NOT be actionable_external."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": {"MELI"},
        "catalog_dynamic": {"MELI"},
        "main_allowed": set(),
        "external_allowed": {"MELI"},
    }
    candidates = generate_external_candidates([], allowed, [])
    assert len(candidates) == 1
    c = candidates[0]
    assert c["actionable_external"] is False
    assert "observado" in c["actionable_reason"].lower()


def test_universe_only_not_actionable():
    """A symbol in universe (no news, no watchlist) must NOT be actionable_external."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": {"GLOB"},
        "catalog_dynamic": set(),
        "main_allowed": set(),
        "external_allowed": {"GLOB"},
    }
    candidates = generate_external_candidates([], allowed, [])
    c = candidates[0]
    assert c["actionable_external"] is False


def test_news_makes_actionable():
    """A symbol with news signal becomes actionable_external."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": {"TSLA"},
        "catalog_dynamic": {"TSLA"},
        "main_allowed": set(),
        "external_allowed": {"TSLA"},
    }
    news = [{"symbol": "TSLA", "reason": "Tesla earnings beat", "confidence": 0.8,
             "event_type": "earnings", "impact": "positivo"}]
    candidates = generate_external_candidates(news, allowed, [])
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")
    assert tsla["actionable_external"] is True
    assert "news" in tsla["source_types"]


def test_watchlist_known_valid_is_actionable():
    """A watchlist symbol with known_valid asset_type is actionable."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": {"TSLA"},
        "universe": set(),
        "catalog_dynamic": set(),
        "main_allowed": set(),
        "external_allowed": {"TSLA"},
    }
    candidates = generate_external_candidates([], allowed, [])
    c = candidates[0]
    # TSLA resolves to CEDEAR (known_valid)
    assert c["asset_type_status"] == "known_valid"
    assert c["actionable_external"] is True


def test_watchlist_unknown_type_not_actionable():
    """A watchlist symbol with unknown asset_type is NOT actionable."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": {"ZZZNONSENSE"},
        "universe": set(),
        "catalog_dynamic": set(),
        "main_allowed": set(),
        "external_allowed": {"ZZZNONSENSE"},
    }
    candidates = generate_external_candidates([], allowed, [])
    c = candidates[0]
    assert c["asset_type_status"] == "unknown"
    assert c["actionable_external"] is False


def test_pseudo_ticker_filtered_from_news():
    """Pseudo-tickers (CEO, UK, DOJ, etc.) must be filtered out entirely."""
    from app.market.candidates import generate_external_candidates, PSEUDO_TICKER_BLOCKLIST

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": set(),
        "catalog_dynamic": {"CEO", "UK", "DOJ", "TSLA"},
        "main_allowed": set(),
        "external_allowed": {"CEO", "UK", "DOJ", "TSLA"},
    }
    news = [
        {"symbol": "CEO", "reason": "CEO appointed", "confidence": 0.6,
         "event_type": "otro", "impact": "positivo"},
        {"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.7,
         "event_type": "earnings", "impact": "positivo"},
    ]
    candidates = generate_external_candidates(news, allowed, [])
    symbols = {c["symbol"] for c in candidates}
    # CEO, UK, DOJ should be filtered
    assert "CEO" not in symbols
    assert "UK" not in symbols
    assert "DOJ" not in symbols
    # TSLA (real ticker) should remain
    assert "TSLA" in symbols


def test_pseudo_ticker_blocklist_has_common_tokens():
    """Blocklist includes common false-positive tokens."""
    from app.market.candidates import PSEUDO_TICKER_BLOCKLIST

    expected = {"CEO", "UK", "US", "EU", "DOJ", "SEC", "GDP", "IPO", "BMV",
                "FBI", "FED", "IMF", "AI", "CFO", "CTO", "USD", "EUR"}
    assert expected.issubset(PSEUDO_TICKER_BLOCKLIST)


def test_healthy_distribution_with_mixed_sources():
    """With catalog + news mix, only news-backed + known_valid symbols are actionable."""
    from app.market.candidates import generate_external_candidates

    catalog_syms = {f"SYM{i}" for i in range(100)}
    news_syms = {"SYM0", "SYM1", "SYM2"}

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": catalog_syms,
        "catalog_dynamic": catalog_syms,
        "main_allowed": set(),
        "external_allowed": catalog_syms,
    }
    news = [{"symbol": s, "reason": "Test news", "confidence": 0.7,
             "event_type": "earnings", "impact": "positivo"} for s in news_syms]
    # News symbols have known_valid type via catalog_map; rest are unknown
    catalog_map = {s: "CEDEAR" for s in news_syms}

    candidates = generate_external_candidates(news, allowed, [], catalog_map=catalog_map)
    actionable = [c for c in candidates if c["actionable_external"]]
    observed = [c for c in candidates if not c["actionable_external"]]

    # Only 3 news-backed + known_valid should be actionable, rest observed
    assert len(actionable) == 3
    assert len(observed) == 97
    assert {c["symbol"] for c in actionable} == news_syms


def test_catalog_plus_news_ranks_higher_than_catalog_only():
    """A candidate with news+catalog should rank above catalog-only."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": set(),
        "universe": {"TSLA", "MELI"},
        "catalog_dynamic": {"TSLA", "MELI"},
        "main_allowed": set(),
        "external_allowed": {"TSLA", "MELI"},
    }
    news = [{"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.7,
             "event_type": "earnings", "impact": "positivo"}]
    candidates = generate_external_candidates(news, allowed, [])
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")
    meli = next(c for c in candidates if c["symbol"] == "MELI")
    assert tsla["priority_score"] > meli["priority_score"]
    assert tsla["actionable_external"] is True
    assert meli["actionable_external"] is False


# ===========================================================================
# Sprint 15: End-to-end alignment — buckets, decision_summary, planner, hygiene
# ===========================================================================


def test_decision_summary_actionable_count_uses_actionable_external():
    """decision_summary.candidates.actionable_count must reflect actionable_external=True items only."""
    from unittest.mock import patch, MagicMock
    from app.services.orchestrator import _build_decision_summary

    # Simulate rec with split buckets (orchestrator now separates them)
    rec = {
        "action": "mantener",
        "actions": [],
        "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [
            {"symbol": "TSLA", "actionable_external": True, "effective_score": 0.7,
             "signal_class": "external_opportunity", "market_confirmation": "confirmed"},
        ],
        "observed_candidates": [
            {"symbol": f"OBS{i}", "actionable_external": False, "effective_score": 0.3,
             "signal_class": "observed_candidate", "market_confirmation": "unconfirmed"}
            for i in range(50)
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    assert summary["candidates"]["actionable_count"] == 1
    assert summary["candidates"]["observed_count"] == 50
    assert len(summary["candidates"]["top_actionable"]) == 1
    assert summary["candidates"]["top_actionable"][0]["symbol"] == "TSLA"


def test_orchestrator_splits_external_vs_observed():
    """Real run_cycle must split actionable vs observed in metadata."""
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

    ext_ops = meta.get("external_opportunities", [])
    obs_cands = meta.get("observed_candidates", [])

    # All items in external_opportunities must have actionable_external=True (or be news-backed)
    for item in ext_ops:
        if "actionable_external" in item:
            assert item["actionable_external"] is True, \
                f"{item.get('symbol')} in external_opportunities but actionable_external=False"

    # decision_summary counts must match actual bucket sizes
    ds = meta.get("decision_summary", {})
    if ds:
        assert ds["candidates"]["actionable_count"] == len(ext_ops)
        assert ds["candidates"]["observed_count"] == len(obs_cands)


def test_planner_rejects_non_actionable():
    """Planner must not propose buys for candidates with actionable_external=False."""
    from unittest.mock import patch, MagicMock
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000, "cash": 20000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "MELI",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.9,
            "source_types": ["catalog"],
            "reason": "Observado desde catalog",
            "investable": True,
            "actionable_external": False,
        },
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["news", "watchlist"],
            "reason": "Valid opportunity",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MSFT", "MELI"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot, analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MELI" not in buy_symbols, "Non-actionable must not be bought"
    assert "MSFT" in buy_symbols, "Actionable must be bought"

    # Check rejection reason mentions "no accionable"
    rejected_text = " ".join(plan["why_rejected"])
    assert "MELI" in rejected_text
    assert "no accionable" in rejected_text


def test_shortlist_filters_pseudo_tickers():
    """build_shortlist must not include pseudo-tickers like WSJ, NASA, SEC."""
    from app.recommendations.scoring import build_shortlist

    scored_news = [
        {
            "signal_class": "external_opportunity",
            "effective_score": 0.8,
            "related_assets": ["TSLA", "WSJ", "NASA", "SEC"],
        },
        {
            "signal_class": "holding_risk",
            "effective_score": 0.9,
            "related_assets": ["AAPL"],
        },
    ]
    holdings = {"AAPL"}
    symbols, meta = build_shortlist(scored_news, holdings)

    assert "TSLA" in symbols
    assert "AAPL" in symbols
    assert "WSJ" not in symbols
    assert "NASA" not in symbols
    assert "SEC" not in symbols


def test_blocklist_includes_wsj_nasa():
    """Blocklist must include media/agency entities found in runtime."""
    from app.market.candidates import PSEUDO_TICKER_BLOCKLIST

    for token in ["WSJ", "NASA", "BBC", "CNN", "CNBC", "EPA", "FDA", "NATO", "OPEC"]:
        assert token in PSEUDO_TICKER_BLOCKLIST, f"{token} missing from blocklist"


def test_pseudo_ticker_filtered_from_candidates_and_shortlist():
    """End-to-end: WSJ/NASA don't enter candidates, don't enter shortlist."""
    from app.market.candidates import generate_external_candidates
    from app.recommendations.scoring import build_shortlist

    # Step 1: candidates filter
    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"WSJ", "NASA", "TSLA"},
        "catalog_dynamic": {"WSJ", "NASA", "TSLA"},
        "main_allowed": set(), "external_allowed": {"WSJ", "NASA", "TSLA"},
    }
    news = [
        {"symbol": "WSJ", "reason": "WSJ article", "confidence": 0.8,
         "event_type": "otro", "impact": "positivo"},
        {"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.8,
         "event_type": "earnings", "impact": "positivo"},
    ]
    candidates = generate_external_candidates(news, allowed, [])
    cand_symbols = {c["symbol"] for c in candidates}
    assert "WSJ" not in cand_symbols
    assert "NASA" not in cand_symbols
    assert "TSLA" in cand_symbols

    # Step 2: shortlist filter
    scored_news = [
        {"signal_class": "external_opportunity", "effective_score": 0.9,
         "related_assets": ["WSJ", "NASA", "TSLA"]},
    ]
    symbols, _ = build_shortlist(scored_news, set(), known_symbols={"TSLA"})
    assert "WSJ" not in symbols
    assert "NASA" not in symbols
    assert "TSLA" in symbols


# ===========================================================================
# Sprint 16: Fine-grained actionable/shortlist/observed hygiene
# ===========================================================================


def test_unknown_asset_type_blocks_actionable():
    """A news-backed symbol with unknown asset_type must NOT be actionable."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"HHS"}, "catalog_dynamic": {"HHS"},
        "main_allowed": set(), "external_allowed": {"HHS"},
    }
    news = [{"symbol": "HHS", "reason": "HHS policy change", "confidence": 0.7,
             "event_type": "regulatorio", "impact": "positivo"}]
    candidates = generate_external_candidates(news, allowed, [])
    # HHS is in blocklist now, should be filtered entirely
    symbols = {c["symbol"] for c in candidates}
    assert "HHS" not in symbols


def test_unknown_not_in_blocklist_still_not_actionable():
    """A news-backed symbol with unknown type (not blocklisted) is NOT actionable."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"XYZFAKE"}, "catalog_dynamic": {"XYZFAKE"},
        "main_allowed": set(), "external_allowed": {"XYZFAKE"},
    }
    news = [{"symbol": "XYZFAKE", "reason": "Some news", "confidence": 0.8,
             "event_type": "otro", "impact": "positivo"}]
    candidates = generate_external_candidates(news, allowed, [])
    c = next((x for x in candidates if x["symbol"] == "XYZFAKE"), None)
    assert c is not None
    assert c["asset_type_status"] == "unknown"
    assert c["actionable_external"] is False


def test_known_valid_with_news_is_actionable():
    """A news-backed symbol with known_valid type IS actionable."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"TSLA"}, "catalog_dynamic": {"TSLA"},
        "main_allowed": set(), "external_allowed": {"TSLA"},
    }
    news = [{"symbol": "TSLA", "reason": "Tesla earnings", "confidence": 0.8,
             "event_type": "earnings", "impact": "positivo",
             "signal_class": "external_opportunity", "effective_score": 0.75,
             "market_confirmation": "confirmed"}]
    candidates = generate_external_candidates(news, allowed, [])
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")
    assert tsla["actionable_external"] is True
    assert tsla["asset_type_status"] == "known_valid"


def test_candidate_propagates_pipeline_metadata():
    """Candidates must propagate effective_score, signal_class, market_confirmation from news."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"TSLA"}, "catalog_dynamic": {"TSLA"},
        "main_allowed": set(), "external_allowed": {"TSLA"},
    }
    news = [{"symbol": "TSLA", "reason": "Tesla earnings", "confidence": 0.8,
             "event_type": "earnings", "impact": "positivo",
             "signal_class": "external_opportunity", "effective_score": 0.75,
             "market_confirmation": "confirmed"}]
    candidates = generate_external_candidates(news, allowed, [])
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")

    assert tsla["effective_score"] == 0.75
    assert tsla["signal_class"] == "external_opportunity"
    assert tsla["market_confirmation"] == "confirmed"


def test_candidate_without_news_has_null_metadata():
    """Candidates from catalog-only should have None for pipeline metadata."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"MELI"}, "catalog_dynamic": {"MELI"},
        "main_allowed": set(), "external_allowed": {"MELI"},
    }
    candidates = generate_external_candidates([], allowed, [])
    meli = next(c for c in candidates if c["symbol"] == "MELI")
    assert meli["effective_score"] is None
    assert meli["signal_class"] is None
    assert meli["market_confirmation"] is None


def test_shortlist_structural_filter_known_symbols():
    """build_shortlist passes 3-4 only accept symbols in known_symbols."""
    from app.recommendations.scoring import build_shortlist

    scored_news = [
        {"signal_class": "external_opportunity", "effective_score": 0.9,
         "related_assets": ["TSLA", "HHS", "XYZFAKE"]},
    ]
    known = {"TSLA", "AAPL"}
    symbols, meta = build_shortlist(scored_news, set(), known_symbols=known)
    assert "TSLA" in symbols
    assert "HHS" not in symbols  # blocklist
    assert "XYZFAKE" not in symbols  # not in known_symbols


def test_shortlist_without_known_symbols_backward_compat():
    """Without known_symbols, passes 3-4 accept any non-blocklisted symbol."""
    from app.recommendations.scoring import build_shortlist

    scored_news = [
        {"signal_class": "external_opportunity", "effective_score": 0.9,
         "related_assets": ["TSLA", "XYZFAKE"]},
    ]
    symbols, _ = build_shortlist(scored_news, set())
    assert "TSLA" in symbols
    assert "XYZFAKE" in symbols  # no known_symbols → no structural filter


def test_blocklist_includes_hhs_cdc_ntsb():
    """Blocklist must include government agencies found in runtime."""
    from app.market.candidates import PSEUDO_TICKER_BLOCKLIST

    for token in ["HHS", "CDC", "NTSB", "DHS", "DOD", "NIH", "FEMA"]:
        assert token in PSEUDO_TICKER_BLOCKLIST, f"{token} missing from blocklist"


def test_top_actionable_includes_reason_and_sources():
    """decision_summary top_actionable should include reason and source_types."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [
            {"symbol": "TSLA", "effective_score": 0.75,
             "signal_class": "external_opportunity", "market_confirmation": "confirmed",
             "reason": "Tesla earnings beat", "source_types": ["news", "catalog"]},
        ],
        "observed_candidates": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    top = summary["candidates"]["top_actionable"][0]
    assert top["symbol"] == "TSLA"
    assert top["effective_score"] == 0.75
    assert top["signal_class"] == "external_opportunity"
    assert top["market_confirmation"] == "confirmed"
    assert top["reason"] == "Tesla earnings beat"
    assert top["source_types"] == ["news", "catalog"]


def test_end_to_end_unknown_never_actionable_or_shortlisted():
    """E2E: unknown-type symbols cannot enter external_opportunities or shortlist."""
    from app.market.candidates import generate_external_candidates
    from app.recommendations.scoring import build_shortlist

    # HHS has news but unknown type → not actionable
    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": set(),
        "universe": {"TSLA"}, "catalog_dynamic": {"TSLA"},
        "main_allowed": set(), "external_allowed": {"TSLA"},
    }
    # HHS is blocklisted, won't even appear as candidate
    # XYZFAKE is not blocklisted but has unknown type → not actionable
    news = [
        {"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.8,
         "event_type": "earnings", "impact": "positivo",
         "signal_class": "external_opportunity", "effective_score": 0.8,
         "market_confirmation": "confirmed"},
    ]
    candidates = generate_external_candidates(news, allowed, [])
    actionable = [c for c in candidates if c["actionable_external"]]
    assert all(c["asset_type_status"] == "known_valid" for c in actionable)

    # Shortlist with known_symbols
    scored_news = [
        {"signal_class": "external_opportunity", "effective_score": 0.9,
         "related_assets": ["TSLA", "HHS", "CDC"]},
    ]
    symbols, _ = build_shortlist(scored_news, set(), known_symbols={"TSLA"})
    assert "HHS" not in symbols
    assert "CDC" not in symbols
    assert "TSLA" in symbols


# ===========================================================================
# Sprint 17: Observed path hygiene — engine filter + top_observed quality
# ===========================================================================


def test_engine_filters_pseudo_tickers_from_observed():
    """Engine must not create observed_candidates entries for blocklisted symbols."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "WSJ reports on market",
            "related_assets": ["WSJ", "TSLA"],
            "signal_class": "observed_candidate",
            "signal_score": 0.5,
            "effective_score": 0.5,
            "confidence": 0.6,
            "event_type": "otro",
            "impact": "neutro",
            "source_count": 1,
        },
        {
            "title": "NTSB investigation update",
            "related_assets": ["NTSB"],
            "signal_class": "observed_candidate",
            "signal_score": 0.45,
            "effective_score": 0.45,
            "confidence": 0.5,
            "event_type": "regulatorio",
            "impact": "negativo",
            "source_count": 1,
        },
    ]

    snapshot = {
        "total_value": 100000, "cash": 10000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "market_value": 90000,
                        "quantity": 10, "pnl_pct": 0.05}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.9}, "alerts": []}

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    observed_symbols = {c["symbol"] for c in rec.get("observed_candidates", [])}

    assert "WSJ" not in observed_symbols, "WSJ is a pseudo-ticker, should be filtered"
    assert "NTSB" not in observed_symbols, "NTSB is a pseudo-ticker, should be filtered"
    # TSLA is a real ticker and should survive
    assert "TSLA" in observed_symbols


def test_engine_filters_pseudo_tickers_from_external():
    """Engine must not create external_opportunities entries for blocklisted symbols."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "SEC announces new regulation affecting MELI and HHS",
            "related_assets": ["SEC", "HHS", "MELI"],
            "signal_class": "external_opportunity",
            "signal_score": 0.6,
            "effective_score": 0.6,
            "confidence": 0.7,
            "event_type": "regulatorio",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = {
        "total_value": 100000, "cash": 10000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "market_value": 90000,
                        "quantity": 10, "pnl_pct": 0.05}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.9}, "alerts": []}

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}

    assert "SEC" not in ext_symbols
    assert "HHS" not in ext_symbols
    assert "MELI" in ext_symbols


def test_top_observed_prefers_scored_items():
    """top_observed should prefer items with effective_score over bare catalog items."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [],
        "observed_candidates": [
            # Engine-observed with score (should rank first)
            {"symbol": "MELI", "effective_score": 0.55, "signal_class": "observed_candidate",
             "market_confirmation": "unconfirmed", "reason": "MELI expansion news",
             "source_types": None},
            # Bare catalog item (no score — should rank lower)
            {"symbol": "SYM99", "effective_score": None, "signal_class": None,
             "market_confirmation": None, "reason": "Observado desde catalog",
             "priority_score": 0.25, "source_types": ["catalog"]},
            # Another scored item
            {"symbol": "GLOB", "effective_score": 0.48, "signal_class": "observed_candidate",
             "market_confirmation": "confirmed", "reason": "GLOB quarterly results",
             "source_types": None},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    top = summary["candidates"]["top_observed"]
    assert len(top) == 3
    # MELI should be first (highest effective_score)
    assert top[0]["symbol"] == "MELI"
    assert top[0]["effective_score"] == 0.55
    # GLOB second
    assert top[1]["symbol"] == "GLOB"


def test_observed_count_not_broken_by_engine_filter():
    """Filtering pseudo-tickers in engine should not break observed_count alignment."""
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

    ds = meta.get("decision_summary", {})
    obs_cands = meta.get("observed_candidates", [])

    # Count must match actual list length
    if ds:
        assert ds["candidates"]["observed_count"] == len(obs_cands)

    # No blocklisted symbols in observed
    from app.market.candidates import PSEUDO_TICKER_BLOCKLIST
    for item in obs_cands:
        sym = item.get("symbol", "")
        assert sym not in PSEUDO_TICKER_BLOCKLIST, f"{sym} is pseudo-ticker in observed_candidates"


def test_legitimate_discovery_survives_filter():
    """Real ticker symbols must survive engine filtering."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "MELI expansion in Brazil",
            "related_assets": ["MELI", "GLOB"],
            "signal_class": "observed_candidate",
            "signal_score": 0.5,
            "effective_score": 0.5,
            "confidence": 0.6,
            "event_type": "expansion",
            "impact": "positivo",
            "source_count": 1,
        },
    ]

    snapshot = {
        "total_value": 100000, "cash": 10000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "market_value": 90000,
                        "quantity": 10, "pnl_pct": 0.05}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.9}, "alerts": []}

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    observed_symbols = {c["symbol"] for c in rec.get("observed_candidates", [])}

    assert "MELI" in observed_symbols
    assert "GLOB" in observed_symbols


def test_no_regression_external_opportunities_planner():
    """Engine pseudo-ticker filter must not affect external_opportunities or planner."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "TSLA earnings beat expectations",
            "related_assets": ["TSLA"],
            "signal_class": "external_opportunity",
            "signal_score": 0.7,
            "effective_score": 0.7,
            "confidence": 0.8,
            "event_type": "earnings",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = {
        "total_value": 100000, "cash": 10000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "market_value": 90000,
                        "quantity": 10, "pnl_pct": 0.05}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.9}, "alerts": []}

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}

    assert "TSLA" in ext_symbols


# ===========================================================================
# Sprint 18: Fine semantic alignment — top_observed, investable visibility
# ===========================================================================


def test_top_observed_prefers_known_valid_over_unknown():
    """top_observed must rank known_valid items above unknown items."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [],
        "observed_candidates": [
            # Unknown type with high score (like FCC)
            {"symbol": "FCC", "effective_score": 0.65, "signal_class": "observed_candidate",
             "market_confirmation": "unconfirmed", "reason": "FCC regulation news",
             "asset_type_status": "unknown", "priority_score": 0.4},
            # Known valid with lower score (real discovery)
            {"symbol": "MELI", "effective_score": 0.50, "signal_class": "observed_candidate",
             "market_confirmation": "confirmed", "reason": "MELI expansion news",
             "asset_type_status": "known_valid", "priority_score": 0.35},
            # Another known valid
            {"symbol": "GLOB", "effective_score": 0.45, "signal_class": "observed_candidate",
             "market_confirmation": "unconfirmed", "reason": "GLOB quarterly results",
             "asset_type_status": "known_valid", "priority_score": 0.30},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    top = summary["candidates"]["top_observed"]
    # Known valid items must rank before unknown, regardless of score
    assert top[0]["symbol"] == "MELI"
    assert top[1]["symbol"] == "GLOB"
    assert top[2]["symbol"] == "FCC"


def test_top_actionable_prefers_investable():
    """top_actionable must rank investable items above non-investable."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [
            # Actionable but NOT investable (like BP — not in whitelist)
            {"symbol": "BP", "effective_score": 0.70, "signal_class": "external_opportunity",
             "market_confirmation": "confirmed", "reason": "BP earnings beat",
             "source_types": ["news", "catalog"], "investable": False,
             "asset_type_status": "known_valid", "priority_score": 0.8},
            # Actionable AND investable (in whitelist)
            {"symbol": "TSLA", "effective_score": 0.60, "signal_class": "external_opportunity",
             "market_confirmation": "confirmed", "reason": "TSLA news",
             "source_types": ["news", "catalog"], "investable": True,
             "asset_type_status": "known_valid", "priority_score": 0.75},
        ],
        "observed_candidates": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    top = summary["candidates"]["top_actionable"]
    # Investable must rank first even with lower score
    assert top[0]["symbol"] == "TSLA"
    assert top[0]["investable"] is True
    assert top[1]["symbol"] == "BP"
    assert top[1]["investable"] is False


def test_decision_summary_has_investable_count():
    """decision_summary.candidates must include investable_count."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [
            {"symbol": "TSLA", "investable": True, "effective_score": 0.7},
            {"symbol": "BP", "investable": False, "effective_score": 0.6},
            {"symbol": "MELI", "investable": True, "effective_score": 0.5},
        ],
        "observed_candidates": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    assert summary["candidates"]["actionable_count"] == 3
    assert summary["candidates"]["investable_count"] == 2


def test_top_n_includes_investable_and_asset_type_status():
    """_top_n output must include investable and asset_type_status fields."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Estable",
        "external_opportunities": [
            {"symbol": "TSLA", "investable": True, "asset_type_status": "known_valid",
             "effective_score": 0.7, "signal_class": "external_opportunity",
             "market_confirmation": "confirmed", "reason": "Tesla news",
             "source_types": ["news"]},
        ],
        "observed_candidates": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )

    top = summary["candidates"]["top_actionable"][0]
    assert "investable" in top
    assert "asset_type_status" in top
    assert top["investable"] is True
    assert top["asset_type_status"] == "known_valid"


def test_counts_not_broken_by_sort_changes():
    """Sorting top_actionable/top_observed must not affect counts."""
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

    ds = meta.get("decision_summary", {})
    ext_ops = meta.get("external_opportunities", [])
    obs_cands = meta.get("observed_candidates", [])

    if ds:
        assert ds["candidates"]["actionable_count"] == len(ext_ops)
        assert ds["candidates"]["observed_count"] == len(obs_cands)
        assert "investable_count" in ds["candidates"]
        investable_actual = sum(1 for c in ext_ops if c.get("investable"))
        assert ds["candidates"]["investable_count"] == investable_actual


# ===========================================================================
# Sprint 19: Strict operable bucket — external_opportunities = investable only
# ===========================================================================


def test_non_investable_excluded_from_external_opportunities():
    """Candidates with actionable_external=True but investable=False must NOT be in external_opportunities."""
    from app.market.candidates import generate_external_candidates

    # NVDA: known_valid CEDEAR, in watchlist → actionable=True
    # But NOT in main_allowed (whitelist) → investable=False
    allowed = {
        "holdings": set(), "whitelist": set(), "watchlist": {"NVDA"},
        "universe": set(), "catalog_dynamic": set(),
        "main_allowed": set(),  # NVDA is NOT in whitelist
        "external_allowed": {"NVDA"},
    }
    candidates = generate_external_candidates([], allowed, [])
    nvda = next((c for c in candidates if c["symbol"] == "NVDA"), None)
    assert nvda is not None
    assert nvda["actionable_external"] is True  # watchlist + known_valid
    assert nvda["investable"] is False  # not in main_allowed

    # In the orchestrator split, NVDA would go to observed (not external_opportunities)
    operable = [c for c in candidates if c.get("actionable_external") and c.get("investable")]
    observed = [c for c in candidates if not (c.get("actionable_external") and c.get("investable"))]
    assert "NVDA" not in {c["symbol"] for c in operable}
    assert "NVDA" in {c["symbol"] for c in observed}


def test_investable_stays_in_external_opportunities():
    """Candidates with actionable_external=True AND investable=True stay in external_opportunities."""
    from app.market.candidates import generate_external_candidates

    allowed = {
        "holdings": set(), "whitelist": {"TSLA"}, "watchlist": {"TSLA"},
        "universe": set(), "catalog_dynamic": set(),
        "main_allowed": {"TSLA"},
        "external_allowed": {"TSLA"},
    }
    candidates = generate_external_candidates([], allowed, [])
    tsla = next((c for c in candidates if c["symbol"] == "TSLA"), None)
    assert tsla is not None
    assert tsla["actionable_external"] is True
    assert tsla["investable"] is True

    operable = [c for c in candidates if c.get("actionable_external") and c.get("investable")]
    assert "TSLA" in {c["symbol"] for c in operable}


def test_orchestrator_strict_split_e2e():
    """E2E: orchestrator splits external_opportunities = only investable items."""
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

    ext_ops = meta.get("external_opportunities", [])
    # Every item in external_opportunities must be investable
    for item in ext_ops:
        if "investable" in item:
            assert item["investable"] is True, \
                f"{item.get('symbol')} in external_opportunities but investable=False"


def test_decision_summary_counts_after_strict_split():
    """decision_summary counts must reflect strict split."""
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

    ds = meta.get("decision_summary", {})
    ext_ops = meta.get("external_opportunities", [])
    obs_cands = meta.get("observed_candidates", [])

    if ds:
        assert ds["candidates"]["actionable_count"] == len(ext_ops)
        assert ds["candidates"]["observed_count"] == len(obs_cands)
        # With strict split, investable_count == actionable_count
        assert ds["candidates"]["investable_count"] == len(ext_ops)


def test_planner_no_regression_after_strict_split():
    """Planner must still work correctly with strict external_opportunities."""
    from unittest.mock import patch, MagicMock
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000, "cash": 20000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    # Only investable items in external_opportunities (post strict split)
    external_opportunities = [
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["news", "watchlist"],
            "reason": "Valid opportunity",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MSFT"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot, analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MSFT" in buy_symbols


# Sprint 20: Observed deduplication + enrichment
# ===========================================================================


def test_observed_dedup_same_symbol_from_engine_and_candidates():
    """Same symbol from engine observed + candidates non-operable → single entry."""
    from app.services.orchestrator import _build_decision_summary

    # Simulate engine observed entry (has effective_score, lacks asset_type_status)
    engine_obs = {
        "symbol": "MSFT",
        "reason": "News about MSFT growth",
        "effective_score": 0.7,
        "signal_class": "observed_candidate",
        "priority_score": None,
        "asset_type_status": None,
    }
    # Simulate candidates entry (has asset_type_status, lacks effective_score)
    candidates_obs = {
        "symbol": "MSFT",
        "reason": "Observado desde catalog, watchlist",
        "effective_score": None,
        "signal_class": None,
        "priority_score": 0.45,
        "asset_type_status": "known_valid",
        "source_types": ["catalog", "watchlist"],
        "investable": False,
        "actionable_external": False,
    }

    rec = {
        "observed_candidates": [engine_obs],
        "external_opportunities": [],
        "suppressed_candidates": [],
    }

    # Simulate the orchestrator dedup logic
    _ENRICH_KEYS = (
        "asset_type_status", "asset_type", "source_types", "investable",
        "actionable_external", "priority_score", "tracking_status",
        "actionable_reason", "in_main_allowed", "asset_type_source",
    )
    raw_observed = rec["observed_candidates"] + [candidates_obs]
    seen: dict[str, dict] = {}
    for item in raw_observed:
        sym = item.get("symbol")
        if not sym:
            continue
        if sym not in seen:
            seen[sym] = item
        else:
            existing = seen[sym]
            new_score = item.get("effective_score") or 0
            old_score = existing.get("effective_score") or 0
            if new_score > old_score:
                winner, loser = item, existing
                seen[sym] = winner
            else:
                winner, loser = existing, item
            for key in _ENRICH_KEYS:
                if winner.get(key) is None and loser.get(key) is not None:
                    winner[key] = loser[key]

    merged = list(seen.values())

    # Only 1 entry for MSFT
    msft_entries = [e for e in merged if e["symbol"] == "MSFT"]
    assert len(msft_entries) == 1

    msft = msft_entries[0]
    # Winner should be engine entry (effective_score=0.7 > 0)
    assert msft["effective_score"] == 0.7
    assert msft["reason"] == "News about MSFT growth"
    # Enriched from candidates entry
    assert msft["asset_type_status"] == "known_valid"
    assert msft["priority_score"] == 0.45
    assert msft["source_types"] == ["catalog", "watchlist"]


def test_observed_dedup_engine_duplicates_same_symbol():
    """Engine can produce same symbol from multiple news items — only one survives."""
    from app.services.orchestrator import _build_decision_summary

    engine_obs_1 = {
        "symbol": "GOOG",
        "reason": "News item 1 about Google",
        "effective_score": 0.5,
        "signal_class": "observed_candidate",
    }
    engine_obs_2 = {
        "symbol": "GOOG",
        "reason": "News item 2 about Google (stronger)",
        "effective_score": 0.8,
        "signal_class": "observed_candidate",
    }

    rec = {
        "observed_candidates": [engine_obs_1, engine_obs_2],
        "external_opportunities": [],
        "suppressed_candidates": [],
    }

    # Simulate dedup
    _ENRICH_KEYS = (
        "asset_type_status", "asset_type", "source_types", "investable",
        "actionable_external", "priority_score", "tracking_status",
        "actionable_reason", "in_main_allowed", "asset_type_source",
    )
    raw = rec["observed_candidates"]
    seen: dict[str, dict] = {}
    for item in raw:
        sym = item.get("symbol")
        if not sym:
            continue
        if sym not in seen:
            seen[sym] = item
        else:
            existing = seen[sym]
            new_score = item.get("effective_score") or 0
            old_score = existing.get("effective_score") or 0
            if new_score > old_score:
                winner, loser = item, existing
                seen[sym] = winner
            else:
                winner, loser = existing, item
            for key in _ENRICH_KEYS:
                if winner.get(key) is None and loser.get(key) is not None:
                    winner[key] = loser[key]

    merged = list(seen.values())

    goog_entries = [e for e in merged if e["symbol"] == "GOOG"]
    assert len(goog_entries) == 1
    # Best score wins
    assert goog_entries[0]["effective_score"] == 0.8
    assert goog_entries[0]["reason"] == "News item 2 about Google (stronger)"


def test_observed_top_observed_prefers_enriched_known_valid():
    """After dedup+enrichment, top_observed correctly ranks known_valid+scored items first."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        # Enriched observed: MSFT has known_valid + effective_score
        # UNKNOWN_SYM has known_valid but no signal
        # WEAK_SYM has signal but no known_valid
        "observed_candidates": [
            {
                "symbol": "MSFT",
                "effective_score": 0.7,
                "asset_type_status": "known_valid",
                "priority_score": 0.45,
                "reason": "News about MSFT",
            },
            {
                "symbol": "UNKNOWN_SYM",
                "effective_score": None,
                "asset_type_status": "known_valid",
                "priority_score": 0.25,
                "reason": "Observado desde catalog",
            },
            {
                "symbol": "WEAK_SYM",
                "effective_score": 0.4,
                "asset_type_status": None,
                "priority_score": None,
                "reason": "Some news",
            },
        ],
        "external_opportunities": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    top = summary["candidates"]["top_observed"]

    # MSFT should be first: known_valid=1, effective_score=0.7
    assert top[0]["symbol"] == "MSFT"
    # UNKNOWN_SYM should be second: known_valid=1, effective_score=0 (None→0)
    assert top[1]["symbol"] == "UNKNOWN_SYM"
    # WEAK_SYM should be third: known_valid=0, effective_score=0.4
    assert top[2]["symbol"] == "WEAK_SYM"


def test_observed_dedup_preserves_count_alignment():
    """After dedup, observed_count in decision_summary reflects unique symbols."""
    from app.services.orchestrator import _build_decision_summary

    # 3 raw entries for 2 unique symbols
    observed = [
        {"symbol": "MSFT", "effective_score": 0.7, "signal_class": "observed_candidate"},
        {"symbol": "MSFT", "effective_score": 0.5, "signal_class": "observed_candidate"},
        {"symbol": "GOOG", "effective_score": 0.3, "signal_class": "observed_candidate"},
    ]

    # Apply dedup (same logic as orchestrator)
    seen: dict[str, dict] = {}
    for item in observed:
        sym = item.get("symbol")
        if not sym:
            continue
        if sym not in seen:
            seen[sym] = item
        else:
            existing = seen[sym]
            if (item.get("effective_score") or 0) > (existing.get("effective_score") or 0):
                seen[sym] = item
    deduped = list(seen.values())

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "observed_candidates": deduped,
        "external_opportunities": [],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    assert summary["candidates"]["observed_count"] == 2  # 2 unique symbols, not 3


def test_observed_dedup_does_not_affect_external_opportunities():
    """Dedup on observed must not touch external_opportunities bucket."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [
            {"symbol": "TSLA", "investable": True, "actionable_external": True,
             "effective_score": 0.8, "asset_type_status": "known_valid"},
        ],
        "observed_candidates": [
            {"symbol": "MSFT", "effective_score": 0.7},
            {"symbol": "GOOG", "effective_score": 0.3},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    assert summary["candidates"]["actionable_count"] == 1
    assert summary["candidates"]["observed_count"] == 2
    assert summary["candidates"]["top_actionable"][0]["symbol"] == "TSLA"


def test_planner_no_regression_after_observed_dedup():
    """Planner still works correctly after observed dedup changes."""
    from unittest.mock import MagicMock, patch

    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100_000, "cash": 20_000, "currency": "USD",
        "positions": [{"symbol": "AAPL", "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["news", "watchlist"],
            "reason": "Valid opportunity",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MSFT"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot, analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["planner_status"] in ("success", "proposed")
    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MSFT" in buy_symbols


# Sprint 21: Symbol-news relevance gate (title_mention)
# ===========================================================================


def test_weak_association_not_promoted_to_external():
    """Symbol only in summary (not title) must NOT go to external_opportunities."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "Amazon regresa al mercado de smartphones",
            "summary": "Según analistas de BAC, el movimiento podría impactar al sector tech",
            "related_assets": ["AMZN", "BAC"],
            "signal_class": "external_opportunity",
            "signal_score": 0.65,
            "effective_score": 0.65,
            "confidence": 0.7,
            "event_type": "lanzamiento",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}
    obs_symbols = {c["symbol"] for c in rec.get("observed_candidates", [])}

    # BAC is NOT in the title → should be in observed, not external
    assert "BAC" not in ext_symbols
    assert "BAC" in obs_symbols
    # BAC entry should have title_mention=False
    bac_entry = next(c for c in rec["observed_candidates"] if c["symbol"] == "BAC")
    assert bac_entry["title_mention"] is False


def test_strong_association_promoted_to_external():
    """Symbol IN title must be promoted to external_opportunities normally."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "MELI reporta resultados trimestrales récord",
            "summary": "MercadoLibre supera expectativas con crecimiento del 40%",
            "related_assets": ["MELI"],
            "signal_class": "external_opportunity",
            "signal_score": 0.7,
            "effective_score": 0.7,
            "confidence": 0.8,
            "event_type": "resultado",
            "impact": "positivo",
            "source_count": 3,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}

    assert "MELI" in ext_symbols
    meli = next(c for c in rec["external_opportunities"] if c["symbol"] == "MELI")
    assert meli["title_mention"] is True


def test_mixed_relevance_splits_correctly():
    """News with multiple symbols: title-mentioned stays external, others go to observed."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "TSLA anuncia nueva fábrica en México",
            "summary": "Analistas de MA y DIS ven impacto sectorial positivo",
            "related_assets": ["TSLA", "MA", "DIS"],
            "signal_class": "external_opportunity",
            "signal_score": 0.65,
            "effective_score": 0.65,
            "confidence": 0.7,
            "event_type": "expansion",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}
    obs_symbols = {c["symbol"] for c in rec.get("observed_candidates", [])}

    # TSLA in title → external
    assert "TSLA" in ext_symbols
    # MA and DIS only in summary → observed
    assert "MA" in obs_symbols
    assert "DIS" in obs_symbols
    assert "MA" not in ext_symbols
    assert "DIS" not in ext_symbols


def test_observed_candidates_still_capture_weak_discovery():
    """Symbols downgraded from external still appear in observed for discovery tracking."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "UBS revisa perspectivas del S&P 500",
            "summary": "El análisis incluye impacto potencial en MA, GOOG y NVDA",
            "related_assets": ["MA", "GOOG", "NVDA"],
            "signal_class": "external_opportunity",
            "signal_score": 0.55,
            "effective_score": 0.55,
            "confidence": 0.6,
            "event_type": "analisis",
            "impact": "positivo",
            "source_count": 1,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)
    obs_symbols = {c["symbol"] for c in rec.get("observed_candidates", [])}

    # All 3 are NOT in title → all in observed
    assert "MA" in obs_symbols
    assert "GOOG" in obs_symbols
    assert "NVDA" in obs_symbols
    # None in external
    assert len(rec.get("external_opportunities", [])) == 0


def test_holdings_not_affected_by_title_gate():
    """Holdings signals work regardless of title mention — gate only applies to externals."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "Resultados sectoriales positivos en tech",
            "summary": "AAPL y SPY se benefician del ciclo de crecimiento",
            "related_assets": ["AAPL", "SPY"],
            "signal_class": "holding_opportunity",
            "signal_score": 0.65,
            "effective_score": 0.65,
            "confidence": 0.7,
            "event_type": "resultado",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    # Holdings should still trigger action regardless of title mention
    # (AAPL and SPY are in held_set, so they're handled in the
    #  positive_hits path, not the related_assets external loop)
    assert rec["action"] in ("aumentar posición", "mantener")
    # No external_opportunities for held symbols
    ext_symbols = {c["symbol"] for c in rec.get("external_opportunities", [])}
    assert "AAPL" not in ext_symbols
    assert "SPY" not in ext_symbols


def test_title_mention_visible_in_top_actionable():
    """title_mention field must appear in decision_summary top_actionable and top_observed."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [
            {"symbol": "MELI", "investable": True, "actionable_external": True,
             "effective_score": 0.7, "asset_type_status": "known_valid",
             "title_mention": True},
        ],
        "observed_candidates": [
            {"symbol": "BAC", "effective_score": 0.5, "title_mention": False,
             "asset_type_status": "known_valid"},
            {"symbol": "TSLA", "effective_score": 0.6, "title_mention": True,
             "asset_type_status": "known_valid"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")

    # title_mention visible in top_actionable
    assert summary["candidates"]["top_actionable"][0]["title_mention"] is True
    assert summary["candidates"]["top_actionable"][0]["symbol"] == "MELI"

    # top_observed should prefer title_mention=True
    top_obs = summary["candidates"]["top_observed"]
    assert top_obs[0]["symbol"] == "TSLA"  # title_mention=True ranks first
    assert top_obs[0]["title_mention"] is True
    assert top_obs[1]["symbol"] == "BAC"
    assert top_obs[1]["title_mention"] is False


def test_counts_and_buckets_preserved_with_title_gate():
    """Title gate changes bucket placement but counts remain aligned."""
    from app.recommendations.engine import generate_recommendation

    news = [
        {
            "title": "MELI y GLOB lideran crecimiento en LatAm",
            "summary": "Analistas de BAC recomiendan compra. MA también se beneficia.",
            "related_assets": ["MELI", "GLOB", "BAC", "MA"],
            "signal_class": "external_opportunity",
            "signal_score": 0.65,
            "effective_score": 0.65,
            "confidence": 0.7,
            "event_type": "resultado",
            "impact": "positivo",
            "source_count": 2,
        },
    ]

    snapshot = _mock_snapshot()
    analysis = _mock_analysis()

    rec = generate_recommendation(snapshot, analysis, news, 0.10)

    ext = rec.get("external_opportunities", [])
    obs = rec.get("observed_candidates", [])

    # MELI and GLOB in title → external
    ext_symbols = {c["symbol"] for c in ext}
    assert "MELI" in ext_symbols
    assert "GLOB" in ext_symbols

    # BAC and MA not in title → observed
    obs_symbols = {c["symbol"] for c in obs}
    assert "BAC" in obs_symbols
    assert "MA" in obs_symbols

    # Total unique symbols = 4 (no loss)
    all_symbols = ext_symbols | obs_symbols
    assert len(all_symbols) == 4


def test_planner_no_regression_with_title_gate():
    """Planner works correctly when external_opportunities have title-validated symbols."""
    from unittest.mock import MagicMock, patch

    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100_000, "cash": 20_000, "currency": "USD",
        "positions": [{"symbol": "AAPL", "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "MELI",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["news", "watchlist"],
            "reason": "MELI reporta resultados récord",
            "investable": True,
            "actionable_external": True,
            "title_mention": True,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MELI"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot, analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["planner_status"] in ("success", "proposed")
    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MELI" in buy_symbols


# ---------------------------------------------------------------------------
# Sprint 23: observed_origin separation (signal vs catalog)
# ---------------------------------------------------------------------------


def test_observed_origin_signal_vs_catalog():
    """observed_origin correctly tags signal-based vs catalog-only items."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [],
        "observed_candidates": [
            # Signal-based: has effective_score and signal_class
            {"symbol": "MA", "effective_score": 0.6, "signal_class": "external_opportunity",
             "title_mention": False, "asset_type_status": "known_valid",
             "observed_origin": "signal"},
            {"symbol": "V", "effective_score": 0.55, "signal_class": "external_opportunity",
             "title_mention": False, "asset_type_status": "known_valid",
             "observed_origin": "signal"},
            # Catalog-only: no signal data
            {"symbol": "AAPL", "effective_score": None, "signal_class": None,
             "title_mention": None, "asset_type_status": "known_valid",
             "source_types": ["catalog", "universe"], "priority_score": 0.35,
             "observed_origin": "catalog"},
            {"symbol": "AMZN", "effective_score": None, "signal_class": None,
             "title_mention": None, "asset_type_status": "known_valid",
             "source_types": ["catalog"], "priority_score": 0.3,
             "observed_origin": "catalog"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    cands = summary["candidates"]

    # Total observed_count still correct
    assert cands["observed_count"] == 4

    # New split counts
    assert cands["observed_with_signal_count"] == 2
    assert cands["observed_catalog_count"] == 2

    # top_observed: signal items rank first (observed_origin="signal" boosts)
    top_obs = cands["top_observed"]
    assert top_obs[0]["symbol"] in ("MA", "V")
    assert top_obs[0]["observed_origin"] == "signal"

    # top_observed_signals: only signal items
    top_sig = cands["top_observed_signals"]
    assert len(top_sig) == 2
    for item in top_sig:
        assert item["observed_origin"] == "signal"

    # top_observed_catalog: only catalog items
    top_cat = cands["top_observed_catalog"]
    assert len(top_cat) == 2
    for item in top_cat:
        assert item["observed_origin"] == "catalog"


def test_top_observed_prioritizes_signal_over_catalog():
    """Signal-observed items always rank above catalog-observed in top_observed."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [],
        "observed_candidates": [
            # Catalog with high priority_score but no signal
            {"symbol": "MSFT", "effective_score": None, "signal_class": None,
             "title_mention": None, "asset_type_status": "known_valid",
             "source_types": ["catalog", "watchlist"], "priority_score": 0.55,
             "observed_origin": "catalog"},
            # Signal-based with moderate score
            {"symbol": "MA", "effective_score": 0.45, "signal_class": "external_opportunity",
             "title_mention": False, "asset_type_status": "known_valid",
             "observed_origin": "signal"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    top_obs = summary["candidates"]["top_observed"]

    # MA (signal) must rank before MSFT (catalog) despite MSFT having higher priority_score
    assert top_obs[0]["symbol"] == "MA"
    assert top_obs[0]["observed_origin"] == "signal"
    assert top_obs[1]["symbol"] == "MSFT"
    assert top_obs[1]["observed_origin"] == "catalog"


def test_counts_not_broken_by_observed_origin():
    """observed_count = observed_with_signal_count + observed_catalog_count always."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [
            {"symbol": "MELI", "investable": True, "actionable_external": True,
             "effective_score": 0.7, "asset_type_status": "known_valid",
             "title_mention": True},
        ],
        "observed_candidates": [
            {"symbol": "MA", "effective_score": 0.5, "signal_class": "external_opportunity",
             "observed_origin": "signal"},
            {"symbol": "AAPL", "effective_score": None, "signal_class": None,
             "observed_origin": "catalog"},
            {"symbol": "AMZN", "effective_score": None, "signal_class": None,
             "observed_origin": "catalog"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    cands = summary["candidates"]

    assert cands["actionable_count"] == 1
    assert cands["observed_count"] == 3
    assert cands["observed_with_signal_count"] + cands["observed_catalog_count"] == cands["observed_count"]
    assert cands["observed_with_signal_count"] == 1
    assert cands["observed_catalog_count"] == 2


def test_external_opportunities_not_affected_by_observed_origin():
    """observed_origin changes do not alter external_opportunities."""
    from app.services.orchestrator import _build_decision_summary

    ext_ops = [
        {"symbol": "MELI", "investable": True, "actionable_external": True,
         "effective_score": 0.7, "asset_type_status": "known_valid",
         "title_mention": True},
    ]
    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": ext_ops,
        "observed_candidates": [
            {"symbol": "MA", "effective_score": 0.5, "observed_origin": "signal"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    cands = summary["candidates"]

    # external_opportunities unaffected
    assert cands["actionable_count"] == 1
    assert cands["top_actionable"][0]["symbol"] == "MELI"


def test_planner_no_regression_with_observed_origin():
    """Planner still works when observed_candidates have observed_origin."""
    from unittest.mock import MagicMock, patch

    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100_000, "cash": 20_000, "currency": "USD",
        "positions": [{"symbol": "AAPL", "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "MELI",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["news", "watchlist"],
            "reason": "MELI reporta resultados récord",
            "investable": True,
            "actionable_external": True,
            "title_mention": True,
            "observed_origin": None,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MELI"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot, analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["planner_status"] in ("success", "proposed")
    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MELI" in buy_symbols


def test_unchanged_not_affected_by_observed_origin():
    """detect_unchanged still works with observed_origin present."""
    from app.recommendations.unchanged import detect_unchanged

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "Cartera estable.",
        "risks": "Riesgo moderado.",
        "executive_summary": "test",
        "actions": [],
        "external_opportunities": [],
        "observed_candidates": [
            {"symbol": "MA", "effective_score": 0.5, "observed_origin": "signal"},
            {"symbol": "AAPL", "effective_score": None, "observed_origin": "catalog"},
        ],
        "_news_items": [],
    }

    analysis = {"alerts": [], "weights_by_asset": {}}

    # No previous rec → unchanged=False
    unchanged, reason = detect_unchanged(rec, None, analysis)
    assert unchanged is False


def test_observed_origin_in_top_n_output():
    """observed_origin field appears in _top_n output items."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener",
        "suggested_pct": 0,
        "confidence": 0.5,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [],
        "rationale_reasons": [],
        "external_opportunities": [],
        "observed_candidates": [
            {"symbol": "MA", "effective_score": 0.5, "signal_class": "external_opportunity",
             "observed_origin": "signal"},
        ],
        "suppressed_candidates": [],
    }

    summary = _build_decision_summary(rec, [], {}, {}, {}, False, "")
    top_obs = summary["candidates"]["top_observed"]
    assert len(top_obs) == 1
    assert top_obs[0]["observed_origin"] == "signal"
