"""Intelligent scheduler with market hours awareness (Part D).

Strategy:
- Pre-market: ingestion runs at configured minutes before open (default: 60, 15)
- Market open: ingestion every N minutes (default: 30); full cycle only if events warrant it
- Post-market: one full cycle at close, one light ingestion after
- Off-hours: nothing (or ingestion-only if configured)

The scheduler NEVER calls the LLM unless ingestion found trigger_recalc events.
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1})

_scheduler_state: dict = {
    "last_run_at": None,
    "last_status": None,
    "last_source": None,
    "last_job": None,
    "last_error": None,
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


# consume_recalc_events was REMOVED.
#
# It marked events in a SECOND commit after run_cycle had already committed
# the recommendation, leaving a durable window where a crash produced a
# recommendation whose events were still pending — and those events could
# later generate a duplicate. The association now happens inside
# run_cycle's SINGLE atomic commit (Recommendation + RecommendationAction +
# MarketEvent together), so the jobs must NOT commit events at all.


def record_cycle_outcome(result: dict | None, source: str, job: str | None = None) -> None:
    """THE single place where a cycle outcome is recorded.

    Used by scheduled_ingestion, scheduled_full_cycle AND manual analysis, so
    automatic and manual runs report consistently.

    source: the REAL cycle source (scheduler_event | scheduler | manual).
    job:    the job that ran it (ingestion | full_cycle | manual), when known.

    On success it also clears last_error and any stale blocking fields, so the
    status never mixes data from different runs. This module records state —
    it can never execute anything.
    """
    _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
    _scheduler_state["last_source"] = source
    if job is not None:
        _scheduler_state["last_job"] = job

    if result is None:
        # The job ran but no cycle was needed (no pending events / not gated).
        _scheduler_state["last_status"] = "no_cycle_needed"
        _scheduler_state["last_error"] = None
        _clear_blocking_state()
        return
    if not isinstance(result, dict):
        return

    if result.get("skipped"):
        _scheduler_state["last_status"] = "skipped"
        _scheduler_state["last_error"] = None
        _scheduler_state["last_skip_code"] = result.get("code")
        _scheduler_state["blocking_recommendation_id"] = result.get("blocking_recommendation_id")
        _scheduler_state["blocking_execution_id"] = result.get("blocking_execution_id")
        _scheduler_state["deferred_events_count"] = result.get("deferred_events_count") or 0
        _scheduler_state["total_skips"] += 1
    else:
        _scheduler_state["last_status"] = "created"
        _scheduler_state["last_error"] = None
        _clear_blocking_state()


def record_cycle_error(exc: Exception, source: str, job: str) -> None:
    """Record a job/analysis failure with its real source and job."""
    _scheduler_state["last_run_at"] = datetime.now(timezone.utc).isoformat()
    _scheduler_state["last_status"] = "error"
    _scheduler_state["last_source"] = source
    _scheduler_state["last_job"] = job
    _scheduler_state["last_error"] = str(exc)[:300]
    _scheduler_state["total_errors"] += 1


# Backwards-compatible alias used across the codebase and tests.
_record_cycle_outcome = record_cycle_outcome


def _clear_blocking_state() -> None:
    """Drop blocking info so a resolved block never lingers in the status."""
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

    from app.market.calendar import resolve_market_schedule

    tz_name = settings.scheduler_timezone
    schedule = resolve_market_schedule(settings)
    open_label = schedule["open_time"].strftime("%H:%M") if schedule["open_time"] else None
    close_label = schedule["close_time"].strftime("%H:%M") if schedule["close_time"] else None

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
        # Configured vs interpreted: the times carry minutes; the timezone is
        # what turns them into real instants.
        "configured_open_time": open_label,
        "configured_close_time": close_label,
        "interpreted_open_time": f"{open_label} {tz_name}" if open_label else None,
        "interpreted_close_time": f"{close_label} {tz_name}" if close_label else None,
        "schedule_source": {
            "open": schedule["open_source"],
            "close": schedule["close_source"],
        },
        "deprecated_schedule_settings_in_use": schedule["deprecated_settings_in_use"],
        "market_schedule_configured": schedule["configured"],
        "market_holidays_configured": len(schedule["holidays"]),
        **lease,
        "jobs": next_jobs,
    }


def _session_minutes(settings) -> tuple[int, int] | None:
    """Configured open/close as minutes past midnight, or None if unusable.

    Resolved through app.market.calendar so HH:MM settings (10:30) win over
    the deprecated whole-hour ones, and an inconsistent schedule fails
    closed instead of being guessed at.
    """
    from app.market.calendar import resolve_market_schedule

    schedule = resolve_market_schedule(settings)
    if schedule["errors"] or not schedule["open_time"] or not schedule["close_time"]:
        return None
    return (
        schedule["open_time"].hour * 60 + schedule["open_time"].minute,
        schedule["close_time"].hour * 60 + schedule["close_time"].minute,
    )


def _market_phase(now_utc: datetime | None = None) -> str:
    """Current market phase in the CONFIGURED timezone.

    Times are interpreted in SCHEDULER_TIMEZONE and now carry MINUTES, so a
    10:30 open is represented exactly instead of being rounded to 10:00 or
    11:00. An explicitly passed datetime is converted into that zone, so the
    result never depends on the host TZ.
    """
    settings = get_settings()
    tz = scheduler_timezone()
    if now_utc is None:
        now = datetime.now(tz)
    else:
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now = now_utc.astimezone(tz)
    weekday = now.weekday()

    if weekday >= 5:
        return "off"

    session = _session_minutes(settings)
    if session is None:
        # Unknown schedule: never claim the market is open.
        return "off"
    open_m, close_m = session
    current = now.hour * 60 + now.minute
    premarket_start = open_m - 120

    if premarket_start <= current < open_m:
        return "premarket"
    if open_m <= current < close_m:
        return "open"
    if close_m <= current < close_m + 120:
        return "postmarket"
    return "off"


def scheduled_ingestion():
    """Lightweight job: ingest news, create events, trigger recalc only if needed."""
    db = None
    try:
        db = SessionLocal()
        # Transport-only retry: a review notification that never reached the
        # user is retried here, because later cycles are skipped by that very
        # recommendation and would never call the notifier again. Bounded by
        # cooldown + max attempts. Analysis and orders are NEVER retried.
        _retry_review_notifications(db)

        run_ingestion(db, source_label="scheduler")

        pending = get_pending_recalc_events(db)
        if pending:
            cycle_result = run_cycle(db, source="scheduler_event")
            _record_cycle_outcome(cycle_result, "scheduler_event", job="ingestion")
            # Events are associated atomically inside run_cycle — no second
            # commit here.
            _notify_events(db, pending)
            _notify_recommendation_change(db, cycle_result)
        else:
            _record_cycle_outcome(None, "scheduler_event", job="ingestion")
        # last_status / last_source are set by _record_cycle_outcome with the
        # REAL cycle outcome and source; the job only counts the run here.
        _scheduler_state["total_runs"] += 1
    except Exception as exc:
        record_cycle_error(exc, "scheduler_event", "ingestion")
    finally:
        if db is not None:
            db.close()


def scheduled_full_cycle():
    """Full analysis cycle (used at market close).

    Gated: only runs run_cycle if there are LLM-eligible news or
    pending trigger_recalc events, unless scheduler_postmarket_force_cycle is True.

    After cycle completes, triggers recommendation-level push notification
    if new actionable items were detected.
    """
    settings = get_settings()
    db = None
    try:
        db = SessionLocal()
        # Transport-only retry: a review notification that never reached the
        # user is retried here, because later cycles are skipped by that very
        # recommendation and would never call the notifier again. Bounded by
        # cooldown + max attempts. Analysis and orders are NEVER retried.
        _retry_review_notifications(db)

        run_ingestion(db, source_label="scheduler_close")

        should_run = (
            settings.scheduler_postmarket_force_cycle
            or has_llm_eligible_news(db)
            or bool(get_pending_recalc_events(db))
        )
        if should_run:
            cycle_result = run_cycle(db, source="scheduler")
            _record_cycle_outcome(cycle_result, "scheduler", job="full_cycle")
            # Event association happens inside run_cycle's atomic commit.
            _notify_recommendation_change(db, cycle_result)
        else:
            _record_cycle_outcome(None, "scheduler", job="full_cycle")
        # last_status / last_source are set by _record_cycle_outcome with the
        # REAL cycle outcome and source; the job only counts the run here.
        _scheduler_state["total_runs"] += 1
    except Exception as exc:
        record_cycle_error(exc, "scheduler", "full_cycle")
    finally:
        if db is not None:
            db.close()


def _retry_review_notifications(db):
    """Best-effort transport retry. Never analysis, never execution."""
    try:
        from app.notifications.dispatcher import retry_pending_review_notifications
        return retry_pending_review_notifications(db)
    except Exception as exc:
        logger.warning("review notification retry failed: %s", exc)
        return {}


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
        # SINGLE MAIN PUSH policy: the human-review notice is THE push for a
        # newly created recommendation. dispatch_recommendation_alerts still
        # runs to persist its audit trail, but with push suppressed so the
        # same recommendation never produces two notifications.
        dispatch_recommendation_alerts(db, cycle_result, suppress_push=True)
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

    from app.market.calendar import premarket_times

    session = _session_minutes(settings)
    if session is None:
        # An unresolvable schedule schedules NOTHING. Registering jobs against
        # a guessed session would ingest at the wrong times and, worse, make
        # /scheduler/status look healthy while it is misconfigured.
        logger.error(
            "Scheduler not started: market schedule is unresolvable. Set "
            "SCHEDULER_MARKET_OPEN_TIME / SCHEDULER_MARKET_CLOSE_TIME (HH:MM)."
        )
        return
    open_m, close_m = session
    open_h, open_min = divmod(open_m, 60)
    close_h, close_min = divmod(close_m, 60)

    # Pre-market ingestion, relative to the real open (e.g. 60 min before
    # 10:30 → 09:30, 15 min before → 10:15).
    for (pre_hour, pre_minute), mins_before in zip(
        premarket_times(settings, settings.scheduler_premarket_minutes),
        settings.scheduler_premarket_minutes,
    ):
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

    # During the session: ingestion every N minutes, from the open minute.
    # The offset makes the slots land ON the session grid (10:30, 11:00, …)
    # instead of the wall-clock hour grid, and the last slot stays strictly
    # before the close.
    interval = settings.scheduler_open_interval_minutes
    offset = open_min % interval if interval > 0 else 0
    minute_expr = f"{offset}-59/{interval}" if offset else f"*/{interval}"
    scheduler.add_job(
        scheduled_ingestion,
        "cron",
        hour=f"{open_h}-{close_h}" if close_min else f"{open_h}-{max(close_h - 1, open_h)}",
        minute=minute_expr,
        day_of_week="mon-fri",
        id="market_hours_ingestion",
        replace_existing=True,
        misfire_grace_time=120,
    )

    # Last in-session check, shortly BEFORE the close (never after it).
    last_check = max(close_m - 5, open_m)
    scheduler.add_job(
        scheduled_ingestion,
        "cron",
        hour=last_check // 60,
        minute=last_check % 60,
        day_of_week="mon-fri",
        id="pre_close_check",
        replace_existing=True,
        misfire_grace_time=120,
    )

    # Post-market: full cycle just after the close.
    post_close = min(close_m + 5, 23 * 60 + 59)
    scheduler.add_job(
        scheduled_full_cycle,
        "cron",
        hour=post_close // 60,
        minute=post_close % 60,
        day_of_week="mon-fri",
        id="postmarket_close",
        replace_existing=True,
        misfire_grace_time=300,
    )

    # Post-market: light ingestion 1h after close
    post_light = min(close_m + 60, 23 * 60 + 59)
    scheduler.add_job(
        scheduled_ingestion,
        "cron",
        hour=post_light // 60,
        minute=post_light % 60,
        day_of_week="mon-fri",
        id="postmarket_light",
        replace_existing=True,
        misfire_grace_time=300,
    )

    scheduler.start()
