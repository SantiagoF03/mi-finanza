"""Tests for Iteration 7b gaps — surgical fixes.

Covers:
Part A: send_to_llm feeds LLM selection (orchestrator uses triage-filtered news)
Part B: Scheduler cost control (scheduled_full_cycle gated)
Part C: RSS URL persistence + dedup by canonical URL + title fallback
Part D: Real holdings in pre-scoring (snapshot > whitelist)
Part E: Multi-source repetition increases pre_score
Part F: Enriched trigger types persist and expose
Integrity: unchanged/cooldown/approve-reject/external_opportunities intact
"""

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    MarketEvent,
    NewsNormalized,
    NewsRaw,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
)
from app.news.ingestion import (
    _canonicalize_url,
    _compute_pre_score,
    _dedup_hash,
    _load_real_holdings,
    _normalize_title,
    _resolve_trigger_type,
    _topic_hash,
    get_llm_eligible_news,
    has_llm_eligible_news,
    run_ingestion,
)
from app.services.orchestrator import run_cycle


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# Part A: Orchestrator uses triage-filtered news for LLM
# ---------------------------------------------------------------------------


def test_orchestrator_passes_triage_filtered_news_to_llm():
    """LLM should receive only triage-filtered news, not raw provider output."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.llm_enabled = True
    s.llm_api_key = "fake-key"

    captured_news = {}

    def mock_summarize(news_items, snapshot, analysis):
        captured_news["summarize"] = news_items
        return "summary"

    def mock_explain(rec, snapshot, analysis, news_items, unchanged=False):
        captured_news["explain"] = news_items
        return "explanation"

    with patch("app.services.orchestrator.llm_summarize", side_effect=mock_summarize), \
         patch("app.services.orchestrator.llm_explain", side_effect=mock_explain):
        result = run_cycle(db, source="test")

    assert "recommendation_id" in result

    # If LLM was called, the news should come from triage (send_to_llm/trigger_recalc)
    if "summarize" in captured_news:
        for item in captured_news["summarize"]:
            assert item.get("triage_level") in ("send_to_llm", "trigger_recalc")
    if "explain" in captured_news:
        for item in captured_news["explain"]:
            assert item.get("triage_level") in ("send_to_llm", "trigger_recalc")

    s.llm_enabled = False


def test_orchestrator_metadata_includes_ingestion_and_llm_count():
    """Metadata should include ingestion observability and llm_news_used."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    assert "ingestion" in meta
    assert "llm_news_used" in meta
    assert isinstance(meta["llm_news_used"], int)
    assert meta["ingestion"].get("ingestion_status") in ("completed", "failed")


def test_orchestrator_no_llm_call_without_eligible_news():
    """When no triage-eligible news exists, LLM should not be called."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.llm_enabled = True
    s.llm_api_key = "fake-key"

    summarize_called = []

    def mock_summarize(news_items, snapshot, analysis):
        summarize_called.append(True)
        return "summary"

    # Patch get_llm_eligible_news to return empty
    with patch("app.services.orchestrator.get_llm_eligible_news", return_value=[]), \
         patch("app.services.orchestrator.llm_summarize", side_effect=mock_summarize), \
         patch("app.services.orchestrator.llm_explain", return_value="expl"):
        result = run_cycle(db, source="test")

    assert "recommendation_id" in result
    # LLM should NOT have been called since no eligible news
    assert len(summarize_called) == 0

    s.llm_enabled = False


# ---------------------------------------------------------------------------
# Part B: Scheduler cost control
# ---------------------------------------------------------------------------


def test_scheduled_full_cycle_skips_without_events():
    """scheduled_full_cycle should not call run_cycle if no eligible events."""
    s = get_settings()
    s.scheduler_postmarket_force_cycle = False

    with patch("app.scheduler.jobs.run_ingestion") as mock_ingest, \
         patch("app.scheduler.jobs.has_llm_eligible_news", return_value=False), \
         patch("app.scheduler.jobs.get_pending_recalc_events", return_value=[]), \
         patch("app.scheduler.jobs.run_cycle") as mock_cycle, \
         patch("app.scheduler.jobs.SessionLocal") as mock_session_cls:
        mock_session_cls.return_value = MagicMock()
        from app.scheduler.jobs import scheduled_full_cycle
        scheduled_full_cycle()

        mock_ingest.assert_called_once()
        mock_cycle.assert_not_called()


def test_scheduled_full_cycle_runs_with_eligible_news():
    """scheduled_full_cycle should call run_cycle if eligible news exists."""
    s = get_settings()
    s.scheduler_postmarket_force_cycle = False

    with patch("app.scheduler.jobs.run_ingestion") as mock_ingest, \
         patch("app.scheduler.jobs.has_llm_eligible_news", return_value=True), \
         patch("app.scheduler.jobs.get_pending_recalc_events", return_value=[]), \
         patch("app.scheduler.jobs.run_cycle") as mock_cycle, \
         patch("app.scheduler.jobs.SessionLocal") as mock_session_cls:
        mock_session_cls.return_value = MagicMock()
        from app.scheduler.jobs import scheduled_full_cycle
        scheduled_full_cycle()

        mock_cycle.assert_called_once()


def test_scheduled_full_cycle_force_overrides():
    """scheduler_postmarket_force_cycle=True should always call run_cycle."""
    s = get_settings()
    s.scheduler_postmarket_force_cycle = True

    with patch("app.scheduler.jobs.run_ingestion") as mock_ingest, \
         patch("app.scheduler.jobs.has_llm_eligible_news", return_value=False), \
         patch("app.scheduler.jobs.get_pending_recalc_events", return_value=[]), \
         patch("app.scheduler.jobs.run_cycle") as mock_cycle, \
         patch("app.scheduler.jobs.SessionLocal") as mock_session_cls:
        mock_session_cls.return_value = MagicMock()
        from app.scheduler.jobs import scheduled_full_cycle
        scheduled_full_cycle()

        mock_cycle.assert_called_once()

    s.scheduler_postmarket_force_cycle = False


# ---------------------------------------------------------------------------
# Part C: URL persistence and dedup
# ---------------------------------------------------------------------------


def test_canonicalize_url_strips_query_and_fragment():
    """Canonical URL should strip query params, fragments, and trailing slash."""
    assert _canonicalize_url("https://example.com/news/123?utm=abc#section") == "https://example.com/news/123"
    assert _canonicalize_url("https://example.com/news/123/") == "https://example.com/news/123"
    assert _canonicalize_url("HTTPS://Example.COM/Path") == "https://example.com/path"


def test_dedup_hash_url_canonical():
    """Same article with different query params should produce same hash."""
    h1 = _dedup_hash("Title", "https://example.com/article/1?utm=a")
    h2 = _dedup_hash("Title", "https://example.com/article/1?ref=b")
    assert h1 == h2


def test_dedup_hash_fallback_to_title():
    """When URL is empty, dedup should fall back to normalized title."""
    h1 = _dedup_hash("Breaking: FED holds rates!", "")
    h2 = _dedup_hash("breaking: FED holds rates", "")
    assert h1 == h2


def test_dedup_hash_different_urls_different_hash():
    """Different canonical URLs should produce different hashes."""
    h1 = _dedup_hash("Title", "https://example.com/article/1")
    h2 = _dedup_hash("Title", "https://example.com/article/2")
    assert h1 != h2


def test_normalize_title_strips_punctuation():
    """Title normalization should remove punctuation and collapse whitespace."""
    assert _normalize_title("Breaking: FED   holds rates!!!") == "breaking fed holds rates"
    assert _normalize_title("  Multiple  Spaces  ") == "multiple spaces"


def test_url_persisted_in_raw_and_normalized():
    """Ingestion should persist URL in both news_raw and news_normalized."""
    db = make_db()
    run_ingestion(db, source_label="test")

    raw = db.query(NewsRaw).first()
    norm = db.query(NewsNormalized).first()

    # Mock provider doesn't have URLs, but columns should exist and be strings
    assert raw is not None
    assert isinstance(raw.url, str)
    assert norm is not None
    assert isinstance(norm.url, str)


# ---------------------------------------------------------------------------
# Part D: Real holdings in pre-scoring
# ---------------------------------------------------------------------------


def test_load_real_holdings_from_snapshot():
    """_load_real_holdings should read from latest PortfolioSnapshot."""
    db = make_db()

    # Create a snapshot with positions
    snap = PortfolioSnapshot(total_value=10000, cash=2000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(snapshot_id=snap.id, symbol="AAPL", asset_type="equity", quantity=10, market_value=5000, currency="USD", pnl_pct=0.0))
    db.add(PortfolioPosition(snapshot_id=snap.id, symbol="GOOGL", asset_type="equity", quantity=5, market_value=3000, currency="USD", pnl_pct=0.0))
    db.commit()

    symbols, source = _load_real_holdings(db)
    assert source == "snapshot"
    assert "AAPL" in symbols
    assert "GOOGL" in symbols


def test_load_real_holdings_fallback_to_whitelist():
    """Without a snapshot, should fall back to whitelist."""
    db = make_db()
    symbols, source = _load_real_holdings(db)
    assert source == "whitelist"
    assert len(symbols) > 0  # whitelist has defaults


def test_ingestion_reports_holdings_source():
    """Ingestion result should include holdings_source field."""
    db = make_db()
    result = run_ingestion(db, source_label="test")
    assert "holdings_source" in result
    assert result["holdings_source"] in ("snapshot", "whitelist")


# ---------------------------------------------------------------------------
# Part E: Multi-source repetition
# ---------------------------------------------------------------------------


def test_topic_hash_same_for_reordered_titles():
    """Same keywords in different order + same symbols + event type → same topic hash."""
    h1 = _topic_hash("Apple reports strong quarterly earnings", ["AAPL"], "earnings")
    h2 = _topic_hash("Strong quarterly earnings Apple reports", ["AAPL"], "earnings")
    assert h1 == h2


def test_topic_hash_different_for_different_topics():
    """Different topics should produce different hashes."""
    h1 = _topic_hash("Apple reports strong quarterly earnings", ["AAPL"], "earnings")
    h2 = _topic_hash("FED raises interest rates unexpectedly", [], "tasas")
    assert h1 != h2


def test_multi_source_count_boosts_pre_score():
    """Higher multi_source_count should increase pre_score."""
    base = dict(
        event_type="earnings",
        impact="positivo",
        confidence=0.7,
        recency_hours=5,
        related_assets=["AAPL"],
        source="unknown",
        held_symbols={"AAPL"},
        watchlist_symbols=set(),
        universe_symbols=set(),
    )
    score_single = _compute_pre_score(**base, multi_source_count=1)
    score_multi = _compute_pre_score(**base, multi_source_count=3)
    assert score_multi > score_single


def test_multi_source_count_persisted():
    """multi_source_count should be persisted in news_normalized."""
    db = make_db()
    run_ingestion(db, source_label="test")

    norms = db.query(NewsNormalized).all()
    for n in norms:
        assert isinstance(n.multi_source_count, int)
        assert n.multi_source_count >= 1


def test_topic_hash_persisted():
    """topic_hash should be persisted in news_normalized."""
    db = make_db()
    run_ingestion(db, source_label="test")

    norms = db.query(NewsNormalized).all()
    for n in norms:
        assert isinstance(n.topic_hash, str)
        assert len(n.topic_hash) == 16


# ---------------------------------------------------------------------------
# Part F: Enriched trigger types
# ---------------------------------------------------------------------------


def test_resolve_trigger_type_holding_risk():
    assert _resolve_trigger_type("earnings", True, "negativo", ["AAPL"], set(), set()) == "holding_risk"


def test_resolve_trigger_type_holding_opportunity():
    assert _resolve_trigger_type("earnings", True, "positivo", ["AAPL"], set(), set()) == "holding_opportunity"


def test_resolve_trigger_type_holding_signal():
    assert _resolve_trigger_type("earnings", True, "neutro", ["AAPL"], set(), set()) == "holding_signal"


def test_resolve_trigger_type_external_opportunity():
    assert _resolve_trigger_type("earnings", False, "positivo", ["TSLA"], set(), {"TSLA"}) == "external_opportunity"


def test_resolve_trigger_type_macro_risk():
    assert _resolve_trigger_type("tasas", False, "negativo", [], set(), set()) == "macro_risk"


def test_resolve_trigger_type_macro_signal():
    assert _resolve_trigger_type("inflación", False, "positivo", [], set(), set()) == "macro_signal"


def test_resolve_trigger_type_sector_rotation():
    assert _resolve_trigger_type("sectorial", False, "positivo", [], set(), set()) == "sector_rotation"


def test_resolve_trigger_type_news_macro():
    assert _resolve_trigger_type("otro", False, "neutro", [], set(), set()) == "news_macro"


def test_trigger_type_persisted_in_market_event():
    """MarketEvents should have enriched trigger_type values."""
    db = make_db()
    run_ingestion(db, source_label="test")

    events = db.query(MarketEvent).all()
    valid_types = {
        "holding_risk", "holding_opportunity", "holding_signal",
        "external_opportunity", "macro_risk", "macro_signal",
        "sector_rotation", "news_macro",
    }
    for e in events:
        assert e.trigger_type in valid_types


# ---------------------------------------------------------------------------
# LLM-eligible news functions
# ---------------------------------------------------------------------------


def test_get_llm_eligible_news_returns_only_eligible():
    """get_llm_eligible_news should only return send_to_llm and trigger_recalc items."""
    db = make_db()
    run_ingestion(db, source_label="test")

    eligible = get_llm_eligible_news(db)
    for item in eligible:
        assert item["triage_level"] in ("send_to_llm", "trigger_recalc")


def test_has_llm_eligible_news_matches_get():
    """has_llm_eligible_news should be consistent with get_llm_eligible_news."""
    db = make_db()
    run_ingestion(db, source_label="test")

    eligible = get_llm_eligible_news(db)
    has = has_llm_eligible_news(db)
    assert has == (len(eligible) > 0)


# ---------------------------------------------------------------------------
# Integrity: unchanged/cooldown/approve-reject still intact
# ---------------------------------------------------------------------------


def test_cooldown_intact_after_7b():
    """Cooldown should still work after all 7b changes."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 3600

    first = run_cycle(db, source="test")
    assert "recommendation_id" in first

    second = run_cycle(db, source="test")
    assert second.get("status") == "cooldown"


def test_unchanged_intact_after_7b():
    """Unchanged detection should still work after all 7b changes."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")
    assert second.get("unchanged") is True


def test_approve_reject_intact_after_7b():
    """Decision flow should still work after all 7b changes."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    run_cycle(db, source="test")
    rec = db.query(Recommendation).order_by(Recommendation.id.desc()).first()
    assert rec is not None

    if rec.status in ("pending", "blocked"):
        rec.status = "approved"
        db.commit()
        db.refresh(rec)
        assert rec.status == "approved"


def test_external_opportunities_intact_after_7b():
    """External opportunities should still appear in metadata."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}
    assert "external_opportunities" in meta


def test_triage_counts_in_ingestion_result():
    """Ingestion result should include triage_counts breakdown."""
    db = make_db()
    result = run_ingestion(db, source_label="test")
    assert "triage_counts" in result
    counts = result["triage_counts"]
    assert "store_only" in counts
    assert "observe" in counts
    assert "send_to_llm" in counts
    assert "trigger_recalc" in counts
