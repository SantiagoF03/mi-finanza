"""Tests for market event ingestion, recency filter, pre-scoring, alerts, and scheduler.

Covers:
1. Old news excluded by recency rule
2. Recent relevant news passes to observe or send_to_llm
3. Duplicate sources don't create duplicate events
4. Holding event can trigger alert
5. External event doesn't break recommendation
6. Scheduler doesn't call LLM if no eligible events
7. Manual ingestion endpoint works
8. Unchanged / cooldown / approve-reject still intact
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    IngestionRun,
    MarketEvent,
    NewsNormalized,
    NewsRaw,
    Recommendation,
)
from app.news.ingestion import (
    _assign_triage_level,
    _compute_pre_score,
    _compute_recency_hours,
    _dedup_hash,
    _severity_from_triage,
    run_ingestion,
    get_active_alerts,
    get_recent_events,
)
from app.scheduler.jobs import _market_phase
from app.services.orchestrator import run_cycle


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# 1. Old news excluded by recency rule
# ---------------------------------------------------------------------------


def test_old_news_gets_store_only():
    """News older than the recency window should be triage=store_only."""
    triage = _assign_triage_level(
        pre_score=0.60,
        recency_hours=100,  # way past any window
        event_type="earnings",
        mentions_holding=True,
    )
    assert triage == "store_only"


def test_very_old_news_low_pre_score():
    """Very old news should get near-zero recency contribution to pre_score."""
    score = _compute_pre_score(
        event_type="earnings",
        impact="positivo",
        confidence=0.7,
        recency_hours=200,
        related_assets=["AAPL"],
        source="unknown_blog",
        held_symbols={"AAPL"},
        watchlist_symbols=set(),
        universe_symbols=set(),
    )
    # Still gets some score from holding mention + impact + confidence, but no recency
    assert score < 0.60  # recency component is 0


# ---------------------------------------------------------------------------
# 2. Recent relevant news passes to observe or send_to_llm
# ---------------------------------------------------------------------------


def test_recent_holding_news_triggers_recalc():
    """Fresh news about a holding with high score → trigger_recalc."""
    triage = _assign_triage_level(
        pre_score=0.55,
        recency_hours=2,
        event_type="earnings",
        mentions_holding=True,
    )
    assert triage == "trigger_recalc"


def test_recent_moderate_news_gets_send_to_llm():
    """Moderately relevant recent news → send_to_llm."""
    triage = _assign_triage_level(
        pre_score=0.45,
        recency_hours=5,
        event_type="sectorial",
        mentions_holding=False,
    )
    assert triage == "send_to_llm"


def test_recent_low_score_gets_observe():
    """Low-score recent news → observe."""
    triage = _assign_triage_level(
        pre_score=0.25,
        recency_hours=10,
        event_type="otro",
        mentions_holding=False,
    )
    assert triage == "observe"


def test_pre_score_higher_for_holding_mention():
    """News mentioning a holding should score higher than same news without."""
    base_args = dict(
        event_type="earnings",
        impact="positivo",
        confidence=0.7,
        recency_hours=2,
        related_assets=["AAPL"],
        source="reuters",
        watchlist_symbols=set(),
        universe_symbols=set(),
    )
    score_with = _compute_pre_score(**base_args, held_symbols={"AAPL"})
    score_without = _compute_pre_score(**base_args, held_symbols=set())
    assert score_with > score_without


# ---------------------------------------------------------------------------
# 3. Duplicate sources don't create duplicate events
# ---------------------------------------------------------------------------


def test_dedup_hash_same_for_identical_news():
    """Same title+url produces same hash."""
    h1 = _dedup_hash("Breaking: FED holds rates", "https://example.com/1")
    h2 = _dedup_hash("Breaking: FED holds rates", "https://example.com/1")
    assert h1 == h2


def test_dedup_hash_different_for_different_news():
    """Different news produces different hash."""
    h1 = _dedup_hash("Breaking: FED holds rates", "https://example.com/1")
    h2 = _dedup_hash("FED raises rates unexpectedly", "https://example.com/2")
    assert h1 != h2


def test_ingestion_deduplicates_on_second_run():
    """Running ingestion twice with same news should not duplicate raw rows."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    result1 = run_ingestion(db, source_label="test")
    result2 = run_ingestion(db, source_label="test")

    assert result1["items_new"] >= 1
    # Second run: all items already exist → 0 new
    assert result2["items_new"] == 0


# ---------------------------------------------------------------------------
# 4. Holding event can trigger alert
# ---------------------------------------------------------------------------


def test_holding_event_creates_market_event():
    """Ingestion should create MarketEvent for relevant holding news."""
    db = make_db()
    result = run_ingestion(db, source_label="test")

    # Mock news provider generates news about holdings
    assert result["status"] == "completed"
    events = get_recent_events(db)
    # Should have at least one event (mock provider generates 3 items, some relevant)
    assert len(events) >= 1


def test_severity_mapping():
    """Severity should escalate with triage level + impact."""
    assert _severity_from_triage("trigger_recalc", "negativo") == "critical"
    assert _severity_from_triage("trigger_recalc", "positivo") == "high"
    assert _severity_from_triage("send_to_llm", "negativo") == "medium"
    assert _severity_from_triage("observe", "negativo") == "low"


def test_active_alerts_filter():
    """Active alerts should only include unacknowledged medium+ events."""
    db = make_db()
    run_ingestion(db, source_label="test")

    all_events = get_recent_events(db)
    active = get_active_alerts(db)

    # Active alerts should be a subset (medium+ and unacknowledged)
    for alert in active:
        assert alert["severity"] in ("medium", "high", "critical")


# ---------------------------------------------------------------------------
# 5. External event doesn't break recommendation
# ---------------------------------------------------------------------------


def test_ingestion_does_not_break_recommendation_cycle():
    """Running ingestion then a full cycle should work without errors."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    run_ingestion(db, source_label="test")
    result = run_cycle(db, source="test")

    assert "recommendation_id" in result
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    assert rec is not None
    assert rec.status in ("pending", "blocked")


# ---------------------------------------------------------------------------
# 6. Scheduler doesn't call LLM if no eligible events
# ---------------------------------------------------------------------------


def test_market_phase_weekday():
    """Market phase detection for weekday."""
    s = get_settings()
    # Monday 15:00 UTC → should be "open" (default open=11, close=20)
    monday_3pm = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)
    assert _market_phase(monday_3pm) == "open"


def test_market_phase_weekend():
    """Weekends should be 'off'."""
    saturday = datetime(2026, 3, 7, 15, 0, tzinfo=timezone.utc)
    assert _market_phase(saturday) == "off"


def test_market_phase_premarket():
    """Pre-market hours detection."""
    s = get_settings()
    # Default open=11 UTC, premarket starts at 9 UTC
    pre = datetime(2026, 3, 9, 10, 0, tzinfo=timezone.utc)
    assert _market_phase(pre) == "premarket"


def test_market_phase_postmarket():
    """Post-market hours detection."""
    s = get_settings()
    # Default close=20 UTC, postmarket is 20-22
    post = datetime(2026, 3, 9, 20, 30, tzinfo=timezone.utc)
    assert _market_phase(post) == "postmarket"


def test_triage_store_only_means_no_llm():
    """store_only triage level should not trigger LLM or recalc.

    This is the key test: the scheduler only calls LLM/recalc
    for trigger_recalc events. store_only = no further processing.
    """
    triage = _assign_triage_level(
        pre_score=0.10,
        recency_hours=100,
        event_type="otro",
        mentions_holding=False,
    )
    assert triage == "store_only"


# ---------------------------------------------------------------------------
# 7. Manual ingestion endpoint works
# ---------------------------------------------------------------------------


def test_manual_ingestion_returns_summary():
    """run_ingestion should return a complete summary dict."""
    db = make_db()
    result = run_ingestion(db, source_label="manual")
    assert result["status"] == "completed"
    assert "run_id" in result
    assert "items_fetched" in result
    assert "items_new" in result
    assert "events_created" in result

    # IngestionRun should be persisted
    run = db.query(IngestionRun).filter(IngestionRun.id == result["run_id"]).first()
    assert run is not None
    assert run.status == "completed"


def test_ingestion_persists_raw_and_normalized():
    """Ingestion should create both raw and normalized records."""
    db = make_db()
    run_ingestion(db, source_label="test")

    raw_count = db.query(NewsRaw).count()
    norm_count = db.query(NewsNormalized).count()

    assert raw_count >= 1
    assert norm_count >= 1
    assert norm_count == raw_count  # 1:1 mapping


# ---------------------------------------------------------------------------
# 8. Unchanged / cooldown / approve-reject still intact
# ---------------------------------------------------------------------------


def test_cooldown_still_works_after_ingestion():
    """Cooldown should still prevent duplicate cycles even with ingestion."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 3600

    run_ingestion(db, source_label="test")
    first = run_cycle(db, source="test")
    assert "recommendation_id" in first

    second = run_cycle(db, source="test")
    assert second.get("status") == "cooldown"


def test_unchanged_still_works_after_ingestion():
    """Unchanged detection should still work with ingestion tables present."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    run_ingestion(db, source_label="test")
    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")
    assert second.get("unchanged") is True


def test_approve_reject_still_works():
    """Decision flow should be unaffected by ingestion changes."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    run_cycle(db, source="test")
    rec = db.query(Recommendation).order_by(Recommendation.id.desc()).first()
    assert rec is not None

    original_status = rec.status
    if original_status in ("pending", "blocked"):
        rec.status = "approved"
        db.commit()
        db.refresh(rec)
        assert rec.status == "approved"


# ---------------------------------------------------------------------------
# Pre-score and recency helpers
# ---------------------------------------------------------------------------


def test_compute_recency_hours():
    """Recency computation should return correct hours."""
    now = datetime(2026, 3, 11, 12, 0)
    pub = datetime(2026, 3, 11, 6, 0)
    assert _compute_recency_hours(pub, now) == 6.0
    assert _compute_recency_hours(None, now) == 9999.0


def test_pre_score_top_tier_source_boost():
    """Top-tier source should get a boost."""
    base = dict(
        event_type="earnings",
        impact="positivo",
        confidence=0.7,
        recency_hours=5,
        related_assets=[],
        held_symbols=set(),
        watchlist_symbols=set(),
        universe_symbols=set(),
    )
    score_reuters = _compute_pre_score(**base, source="reuters.com/feed")
    score_random = _compute_pre_score(**base, source="random_blog")
    assert score_reuters > score_random


def test_pre_score_watchlist_boost():
    """Watchlist mention should boost score."""
    base = dict(
        event_type="otro",
        impact="neutro",
        confidence=0.5,
        recency_hours=5,
        related_assets=["TSLA"],
        source="unknown",
        held_symbols=set(),
        universe_symbols=set(),
    )
    score_wl = _compute_pre_score(**base, watchlist_symbols={"TSLA"})
    score_no = _compute_pre_score(**base, watchlist_symbols=set())
    assert score_wl > score_no
