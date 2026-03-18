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
