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
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.news.ingestion import get_pending_recalc_events, has_llm_eligible_news, run_ingestion
from app.services.analysis_gate import get_lease_state
from app.services.orchestrator import run_cycle

# IMPORTANT: this module must never import or call anything from the
# execution/approval layer. Static tests (substring + AST) enforce that no
# sending, approving or order-creating symbol appears here.

scheduler = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1})

_scheduler_state: dict = {
    "last_run_at": None,
    "last_status": None,
    "last_source": None,
    "last_skip_code": None,
    "blocking_recommendation_id": None,
    "blocking_execution_id": None,
    "deferred_events_count": 0,
    "total_runs": 0,
    "total_errors": 0,
    "total_skips": 0,
}


def scheduler_timezone() -> ZoneInfo:
    """Explicit scheduler timezone. Validated at settings load."""
    return ZoneInfo(get_settings().scheduler_timezone)


def scheduler_now() -> datetime:
    """Timezone-aware 'now' in the configured zone (never naive, never host TZ)."""
    return datetime.now(scheduler_timezone())


def _record_cycle_outcome(result: dict | None, source: str) -> None:
    """Record a run_cycle outcome, distinguishing a real skip from an error."""
    if not isinstance(result, dict):
        return
    if result.get("skipped"):
        _scheduler_state["last_skip_code"] = result.get("code")
        _scheduler_state["blocking_recommendation_id"] = result.get("blocking_recommendation_id")
        _scheduler_state["blocking_execution_id"] = result.get("blocking_execution_id")
        _scheduler_state["deferred_events_count"] = result.get("deferred_events_count") or 0
        _scheduler_state["total_skips"] += 1
    else:
        _scheduler_state["last_skip_code"] = None
        _scheduler_state["blocking_recommendation_id"] = None
        _scheduler_state["blocking_execution_id"] = None
        _scheduler_state["deferred_events_count"] = 0


def get_scheduler_state() -> dict:
    """Auditable scheduler state. Contains no secrets and no credentials."""
    settings = get_settings()
    next_jobs = []
    if scheduler.running:
        for job in scheduler.get_jobs():
            next_run = job.next_run_time
            next_jobs.append({
                "id": job.id,
                "next_run_time": next_run.isoformat() if next_run else None,
            })

    tz_name = settings.scheduler_timezone
    open_h = settings.scheduler_market_open_hour
    close_h = settings.scheduler_market_close_hour

    lease = {"lease_held": None, "lease_owner": None, "lease_expires_at": None}
    try:
        db = SessionLocal()
        try:
            lease = get_lease_state(db)
        finally:
            db.close()
    except Exception:
        pass

    return {
        **_scheduler_state,
        "enabled_config": bool(settings.scheduler_enabled),
        "running": scheduler.running,
        "phase": _market_phase(),
        "timezone": tz_name,
        "scheduler_now": scheduler_now().isoformat(),
        # Configured vs interpreted: the hours are plain integers; the
        # timezone is what turns them into real instants.
        "configured_open_time": f"{open_h:02d}:00",
        "configured_close_time": f"{close_h:02d}:00",
        "interpreted_open_time": f"{open_h:02d}:00 {tz_name}",
        "interpreted_close_time": f"{close_h:02d}:00 {tz_name}",
        **lease,
        "jobs": next_jobs,
    }


def _market_phase(now_utc: datetime | None = None) -> str:
    """Current market phase in the CONFIGURED timezone.

    Hours are interpreted in SCHEDULER_TIMEZONE (default UTC, which preserves
    the previous behavior exactly). An explicitly passed datetime is converted
    into that zone, so the result never depends on the host TZ.
    """
    settings = get_settings()
    tz = scheduler_timezone()
    if now_utc is None:
        now = datetime.now(tz)
    else:
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now = now_utc.astimezone(tz)
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

        pending = get_pending_recalc_events(db)
        if pending:
            cycle_result = run_cycle(db, source="scheduler_event")
            _record_cycle_outcome(cycle_result, "scheduler_event")
            # Deferred events: only mark them consumed when a recommendation
            # was actually persisted. A skipped cycle leaves them pending so
            # a future cycle picks them up — never lost, never duplicated.
            if cycle_result.get("recommendation_id"):
                for evt in pending:
                    evt.triggered_recalc = True
                    evt.recalc_recommendation_id = cycle_result["recommendation_id"]
            db.commit()

            _notify_events(db, pending)
            _notify_recommendation_change(db, cycle_result)
        _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _scheduler_state["last_status"] = "ok"
        _scheduler_state["last_source"] = "ingestion"
        _scheduler_state["total_runs"] += 1
    except Exception as exc:
        _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _scheduler_state["last_status"] = f"error: {exc}"
        _scheduler_state["last_source"] = "ingestion"
        _scheduler_state["total_errors"] += 1
    finally:
        db.close()


def scheduled_full_cycle():
    """Full analysis cycle (used at market close).

    Gated: only runs run_cycle if there are LLM-eligible news or
    pending trigger_recalc events, unless scheduler_postmarket_force_cycle is True.

    After cycle completes, triggers recommendation-level push notification
    if new actionable items were detected.
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
            cycle_result = run_cycle(db, source="scheduler")
            _record_cycle_outcome(cycle_result, "scheduler")
            _notify_recommendation_change(db, cycle_result)
        _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _scheduler_state["last_status"] = "ok"
        _scheduler_state["last_source"] = "full_cycle"
        _scheduler_state["total_runs"] += 1
    except Exception as exc:
        _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
        _scheduler_state["last_status"] = f"error: {exc}"
        _scheduler_state["last_source"] = "full_cycle"
        _scheduler_state["total_errors"] += 1
    finally:
        db.close()


def _notify_events(db, events):
    """Best-effort notification dispatch for market events."""
    try:
        from app.notifications.dispatcher import dispatch_alerts
        dispatch_alerts(db, events)
    except Exception:
        pass


def _notify_recommendation_change(db, cycle_result: dict):
    """Best-effort recommendation-level push notification.

    Semi-automatic mode V1: a newly created recommendation triggers ONE
    "needs your review" notification (deduplicated per recommendation). A
    SKIPPED cycle notifies nothing — no recommendation_id means the dispatch
    is a no-op, so a persistent block never produces per-cycle spam.

    Safety: notifications NEVER execute orders.
    """
    if not isinstance(cycle_result, dict) or cycle_result.get("skipped"):
        return
    try:
        from app.notifications.dispatcher import (
            dispatch_recommendation_alerts,
            notify_new_recommendation_pending_review,
        )
        dispatch_recommendation_alerts(db, cycle_result)
        notify_new_recommendation_pending_review(db, cycle_result)
    except Exception:
        pass


def start_scheduler() -> None:
    """Register jobs. With SCHEDULER_ENABLED=false nothing is scheduled, no
    lease is taken and `running` stays false — but /scheduler/status still
    reports the interpreted configuration."""
    settings = get_settings()
    if not settings.scheduler_enabled or scheduler.running:
        return

    # Explicit timezone: APScheduler would otherwise fall back to the host TZ.
    scheduler.configure(timezone=scheduler_timezone())

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
