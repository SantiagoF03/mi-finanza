"""Intelligent scheduler with market hours awareness (Part D).

Strategy:
- Pre-market: ingestion runs at configured minutes before open (default: 60, 15)
- Market open: ingestion every N minutes (default: 30); full cycle only if events warrant it
- Post-market: one full cycle at close, one light ingestion after
- Off-hours: nothing (or ingestion-only if configured)

The scheduler NEVER calls the LLM unless ingestion found trigger_recalc events.
"""

from __future__ import annotations

from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.news.ingestion import get_pending_recalc_events, has_llm_eligible_news, run_ingestion
from app.services.orchestrator import run_cycle

scheduler = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1})


def _market_phase(now_utc: datetime | None = None) -> str:
    """Determine current market phase: premarket, open, postmarket, off."""
    settings = get_settings()
    now = now_utc or datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()

    if weekday >= 5:
        return "off"

    open_h = settings.scheduler_market_open_hour
    close_h = settings.scheduler_market_close_hour
    premarket_start = open_h - 2

    if premarket_start <= hour < open_h:
        return "premarket"
    if open_h <= hour < close_h:
        return "open"
    if close_h <= hour < close_h + 2:
        return "postmarket"
    return "off"


def scheduled_ingestion():
    """Lightweight job: ingest news, create events, trigger recalc only if needed."""
    db = SessionLocal()
    try:
        run_ingestion(db, source_label="scheduler")

        # Only run full cycle if there are trigger_recalc events pending
        pending = get_pending_recalc_events(db)
        if pending:
            cycle_result = run_cycle(db, source="scheduler_event")
            for evt in pending:
                evt.triggered_recalc = True
                if cycle_result.get("recommendation_id"):
                    evt.recalc_recommendation_id = cycle_result["recommendation_id"]
            db.commit()

            _notify_events(db, pending)
    finally:
        db.close()


def scheduled_full_cycle():
    """Full analysis cycle (used at market close).

    Gated: only runs run_cycle if there are LLM-eligible news or
    pending trigger_recalc events, unless scheduler_postmarket_force_cycle is True.
    """
    settings = get_settings()
    db = SessionLocal()
    try:
        run_ingestion(db, source_label="scheduler_close")

        should_run = (
            settings.scheduler_postmarket_force_cycle
            or has_llm_eligible_news(db)
            or bool(get_pending_recalc_events(db))
        )
        if should_run:
            run_cycle(db, source="scheduler")
    finally:
        db.close()


def _notify_events(db, events):
    """Best-effort notification dispatch."""
    try:
        from app.notifications.dispatcher import dispatch_alerts
        dispatch_alerts(db, events)
    except Exception:
        pass


def start_scheduler() -> None:
    settings = get_settings()
    if not settings.scheduler_enabled or scheduler.running:
        return

    open_h = settings.scheduler_market_open_hour
    close_h = settings.scheduler_market_close_hour

    # Pre-market ingestion runs
    for mins_before in settings.scheduler_premarket_minutes:
        total_mins = open_h * 60 - mins_before
        pre_hour = total_mins // 60
        pre_minute = total_mins % 60
        scheduler.add_job(
            scheduled_ingestion,
            "cron",
            hour=pre_hour,
            minute=pre_minute,
            day_of_week="mon-fri",
            id=f"premarket_{mins_before}",
            replace_existing=True,
            misfire_grace_time=120,
        )

    # During market hours: ingestion every N minutes
    interval = settings.scheduler_open_interval_minutes
    scheduler.add_job(
        scheduled_ingestion,
        "cron",
        hour=f"{open_h}-{close_h - 1}",
        minute=f"*/{interval}",
        day_of_week="mon-fri",
        id="market_hours_ingestion",
        replace_existing=True,
        misfire_grace_time=120,
    )

    # Post-market: full cycle at close
    scheduler.add_job(
        scheduled_full_cycle,
        "cron",
        hour=close_h,
        minute=5,
        day_of_week="mon-fri",
        id="postmarket_close",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Post-market: light ingestion 1h after close
    post_h = close_h + 1 if close_h < 23 else 23
    scheduler.add_job(
        scheduled_ingestion,
        "cron",
        hour=post_h,
        minute=0,
        day_of_week="mon-fri",
        id="postmarket_light",
        replace_existing=True,
        misfire_grace_time=300,
    )

    scheduler.start()
