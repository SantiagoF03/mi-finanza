"""Trading calendar with MINUTE precision + scheduler wiring.

The previous configuration could only express whole hours, so the real BYMA
open (10:30) was unrepresentable. These tests pin:

- 10:30 exactly, in America/Argentina/Buenos_Aires;
- premarket runs relative to 10:30, not to a rounded hour;
- a last in-session check strictly BEFORE 17:00, and the post-close cycle
  after it;
- weekends, configured holidays and unknown/inconsistent schedules failing
  closed;
- the scheduler still being structurally incapable of executing anything.

Every assertion passes an explicit `now`, so nothing depends on when the
suite happens to run. The suite-wide "open session" default is opted out of
with @pytest.mark.real_market_calendar.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import pytest

from app.core.config import Settings, get_settings
from app.market.calendar import (
    MARKET_CLOSED,
    MARKET_SCHEDULE_UNKNOWN,
    market_session_state,
    parse_hhmm,
    parse_holidays,
    premarket_times,
    resolve_market_schedule,
)

pytestmark = pytest.mark.real_market_calendar

BACKEND_DIR = Path(__file__).resolve().parent.parent
ART = ZoneInfo("America/Argentina/Buenos_Aires")


@contextmanager
def exec_settings(**overrides):
    s = get_settings()
    saved = {k: getattr(s, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(s, k, v)
        yield s
    finally:
        for k, v in saved.items():
            setattr(s, k, v)


def _byma(**extra):
    base = dict(
        scheduler_market_open_time="10:30",
        scheduler_market_close_time="17:00",
        scheduler_timezone="America/Argentina/Buenos_Aires",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _art(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=ART)


# ───────────────────────────────────────────────────────────────────
# 1 — HH:MM parsing
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("raw,expected", [
    ("10:30", (10, 30)),
    ("00:00", (0, 0)),
    ("23:59", (23, 59)),
    ("9:05", (9, 5)),
])
def test_parse_hhmm_accepts_real_times(raw, expected):
    parsed = parse_hhmm(raw)
    assert (parsed.hour, parsed.minute) == expected


@pytest.mark.parametrize("raw", ["", None, "10", "10:5", "24:00", "10:60", "10:30:00", "abc", "-1:00"])
def test_parse_hhmm_rejects_everything_else(raw):
    """A misread schedule silently shifts when orders may be sent."""
    assert parse_hhmm(raw) is None


def test_invalid_market_time_fails_at_startup():
    with pytest.raises(ValueError):
        Settings(scheduler_market_open_time="25:00")


# ───────────────────────────────────────────────────────────────────
# 2 — 10:30 exactly
# ───────────────────────────────────────────────────────────────────


def test_market_opens_at_1030_not_1000_and_not_1100():
    """The whole point: 10:30 is representable and exact."""
    with exec_settings(**_byma()):
        # Tuesday 2026-08-04
        assert market_session_state(get_settings(), now=_art(2026, 8, 4, 10, 29))["open"] is False
        assert market_session_state(get_settings(), now=_art(2026, 8, 4, 10, 30))["open"] is True
        assert market_session_state(get_settings(), now=_art(2026, 8, 4, 10, 31))["open"] is True


def test_market_closes_at_1700_exactly():
    with exec_settings(**_byma()):
        assert market_session_state(get_settings(), now=_art(2026, 8, 4, 16, 59))["open"] is True
        state = market_session_state(get_settings(), now=_art(2026, 8, 4, 17, 0))
        assert state["open"] is False
        assert state["code"] == MARKET_CLOSED
        assert state["reason"] == "outside_session"


def test_session_state_is_timezone_aware_not_host_local():
    """13:30 UTC is 10:30 ART — open. The host TZ never decides."""
    with exec_settings(**_byma()):
        utc_at_open = datetime(2026, 8, 4, 13, 30, tzinfo=timezone.utc)
        state = market_session_state(get_settings(), now=utc_at_open)
        assert state["open"] is True
        assert state["timezone"] == "America/Argentina/Buenos_Aires"


# ───────────────────────────────────────────────────────────────────
# 3 — fail closed
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("day", [
    _art(2026, 8, 8, 12, 0),   # Saturday
    _art(2026, 8, 9, 12, 0),   # Sunday
])
def test_weekend_is_closed(day):
    with exec_settings(**_byma()):
        state = market_session_state(get_settings(), now=day)
        assert state["open"] is False
        assert state["code"] == MARKET_CLOSED
        assert state["reason"] == "weekend"


def test_configured_holiday_is_closed_even_mid_session():
    with exec_settings(**_byma(market_holidays=["2026-08-04"])):
        state = market_session_state(get_settings(), now=_art(2026, 8, 4, 12, 0))
        assert state["open"] is False
        assert state["code"] == MARKET_CLOSED
        assert state["reason"] == "holiday"


def test_unparseable_holiday_list_fails_closed_not_open():
    """Half a holiday calendar would silently declare a closed day open."""
    holidays, valid = parse_holidays(["2026-08-04", "not-a-date"])
    assert (holidays, valid) == (set(), False)
    with exec_settings(**_byma(market_holidays=["2026-08-04", "not-a-date"])):
        state = market_session_state(get_settings(), now=_art(2026, 8, 4, 12, 0))
        assert state["open"] is False
        assert state["code"] == MARKET_SCHEDULE_UNKNOWN


@pytest.mark.parametrize("overrides", [
    dict(scheduler_market_open_time="", scheduler_market_close_time="",
         scheduler_market_open_hour=0, scheduler_market_close_hour=0),
    dict(scheduler_market_open_time="17:00", scheduler_market_close_time="10:30"),
    dict(scheduler_market_open_time="12:00", scheduler_market_close_time="12:00"),
])
def test_inconsistent_schedule_is_unknown_not_open(overrides):
    with exec_settings(**_byma(**overrides)):
        state = market_session_state(get_settings(), now=_art(2026, 8, 4, 12, 0))
        assert state["open"] is False
        assert state["code"] == MARKET_SCHEDULE_UNKNOWN


# ───────────────────────────────────────────────────────────────────
# 4 — deprecated whole-hour settings
# ───────────────────────────────────────────────────────────────────


def test_deprecated_hour_settings_still_work_and_are_flagged():
    """An existing deployment keeps working while it migrates."""
    with exec_settings(scheduler_market_open_time="", scheduler_market_close_time="",
                       scheduler_market_open_hour=11, scheduler_market_close_hour=20,
                       scheduler_timezone="UTC", market_holidays=[]):
        schedule = resolve_market_schedule(get_settings())
        assert (schedule["open_time"].hour, schedule["open_time"].minute) == (11, 0)
        assert schedule["deprecated_settings_in_use"] is True
        assert "deprecated" in schedule["open_source"]


def test_hhmm_settings_take_precedence_over_deprecated_hours():
    with exec_settings(**_byma(scheduler_market_open_hour=11, scheduler_market_close_hour=20)):
        schedule = resolve_market_schedule(get_settings())
        assert (schedule["open_time"].hour, schedule["open_time"].minute) == (10, 30)
        assert schedule["deprecated_settings_in_use"] is False


# ───────────────────────────────────────────────────────────────────
# 5 — premarket relative to the real open
# ───────────────────────────────────────────────────────────────────


def test_premarket_times_are_relative_to_1030():
    with exec_settings(**_byma()):
        assert premarket_times(get_settings(), [60, 15]) == [(9, 30), (10, 15)]


def test_premarket_run_that_would_wrap_to_previous_day_is_dropped():
    """The old whole-hour arithmetic wrapped around to 23:xx."""
    with exec_settings(**_byma(scheduler_market_open_time="00:30")):
        assert premarket_times(get_settings(), [60, 15]) == [(0, 15)]


# ───────────────────────────────────────────────────────────────────
# 6 — scheduler cron generation
# ───────────────────────────────────────────────────────────────────


@contextmanager
def _started_scheduler(**overrides):
    from app.scheduler import jobs

    with exec_settings(scheduler_enabled=True, scheduler_open_interval_minutes=30,
                       scheduler_premarket_minutes=[60, 15], **_byma(**overrides)):
        with mock.patch.object(jobs.scheduler, "start"), \
             mock.patch.object(type(jobs.scheduler), "running",
                               new_callable=mock.PropertyMock, return_value=False):
            jobs.scheduler.remove_all_jobs()
            jobs.start_scheduler()
            try:
                yield {j.id: j for j in jobs.scheduler.get_jobs()}
            finally:
                jobs.scheduler.remove_all_jobs()


def _cron_field(job, name):
    return str(next(f for f in job.trigger.fields if f.name == name))


def test_scheduler_registers_premarket_ingestion_relative_to_1030():
    with _started_scheduler() as jobs_by_id:
        assert _cron_field(jobs_by_id["premarket_60"], "hour") == "9"
        assert _cron_field(jobs_by_id["premarket_60"], "minute") == "30"
        assert _cron_field(jobs_by_id["premarket_15"], "hour") == "10"
        assert _cron_field(jobs_by_id["premarket_15"], "minute") == "15"


def test_in_session_ingestion_lands_on_the_1030_grid():
    """Slots follow the session grid, not a fixed hour grid.

    For a 10:30 open on a 30-minute interval the grid is :00/:30, which
    already contains 10:30 — so the expression stays `*/30`. The extra 10:00
    slot falls inside the premarket window, where ingestion is wanted anyway.
    """
    with _started_scheduler() as jobs_by_id:
        job = jobs_by_id["market_hours_ingestion"]
        assert _cron_field(job, "minute") == "*/30"
        assert _cron_field(job, "hour") == "10-16"


def test_in_session_grid_is_offset_for_a_non_aligned_open():
    """A 10:20 open on a 30-minute interval must fire at :20 and :50 —
    never on the untouched :00/:30 hour grid."""
    with _started_scheduler(scheduler_market_open_time="10:20") as jobs_by_id:
        job = jobs_by_id["market_hours_ingestion"]
        assert _cron_field(job, "minute") == "20-59/30"


def test_last_check_is_before_close_and_full_cycle_after():
    with _started_scheduler() as jobs_by_id:
        pre_close = jobs_by_id["pre_close_check"]
        assert (_cron_field(pre_close, "hour"), _cron_field(pre_close, "minute")) == ("16", "55")

        post = jobs_by_id["postmarket_close"]
        assert (_cron_field(post, "hour"), _cron_field(post, "minute")) == ("17", "5")


def test_scheduler_uses_the_configured_timezone():
    with _started_scheduler() as jobs_by_id:
        assert str(jobs_by_id["postmarket_close"].trigger.timezone) == (
            "America/Argentina/Buenos_Aires"
        )


def test_unresolvable_schedule_registers_no_jobs_at_all():
    """A guessed session would ingest at the wrong times AND look healthy."""
    with _started_scheduler(scheduler_market_open_time="17:00",
                            scheduler_market_close_time="10:30") as jobs_by_id:
        assert jobs_by_id == {}


def test_market_phase_honours_minutes():
    from app.scheduler.jobs import _market_phase

    with exec_settings(**_byma()):
        assert _market_phase(_art(2026, 8, 4, 10, 29).astimezone(timezone.utc)) == "premarket"
        assert _market_phase(_art(2026, 8, 4, 10, 30).astimezone(timezone.utc)) == "open"
        assert _market_phase(_art(2026, 8, 4, 16, 59).astimezone(timezone.utc)) == "open"
        assert _market_phase(_art(2026, 8, 4, 17, 0).astimezone(timezone.utc)) == "postmarket"


def test_scheduler_status_reports_minutes_and_schedule_source():
    from app.scheduler.jobs import get_scheduler_state

    with exec_settings(**_byma()):
        state = get_scheduler_state()
    assert state["configured_open_time"] == "10:30"
    assert state["configured_close_time"] == "17:00"
    assert state["interpreted_open_time"] == "10:30 America/Argentina/Buenos_Aires"
    assert state["deprecated_schedule_settings_in_use"] is False
    assert state["market_schedule_configured"] is True


# ───────────────────────────────────────────────────────────────────
# 7 — the scheduler still cannot execute anything
# ───────────────────────────────────────────────────────────────────


_FORBIDDEN_EXECUTION_SYMBOLS = {
    "approve_and_execute", "place_order", "submit_order_request",
    "reconcile_execution", "OrderExecution", "_get_execution_broker",
    "_submit_prepared_order", "prepare_validated_execution_batch",
    # New capabilities must be just as unreachable from automatic modules.
    "consume_daily_budget", "evaluate_buy_cash", "get_live_cash",
    "refresh_execution_catalog", "upsert_instrument",
    "fci_execution_blocked", "_preflight_one_order",
}


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/services/orchestrator.py",
    "app/services/analysis_gate.py",
    "app/notifications/dispatcher.py",
    "app/llm/explainer.py",
    "app/news/ingestion.py",
    "app/market/calendar.py",
])
def test_automatic_modules_cannot_execute_or_subscribe(module_path):
    """AST guard, extended: no automatic module may reference anything that
    approves, sends, sizes cash for, or subscribes/redeems anything."""
    forbidden = set(_FORBIDDEN_EXECUTION_SYMBOLS)
    if module_path.endswith("analysis_gate.py"):
        # The gate must READ OrderExecution to detect blocking executions.
        forbidden.discard("OrderExecution")

    tree = ast.parse((BACKEND_DIR / module_path).read_text())
    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            referenced.update(a.name for a in node.names)

    offending = referenced & forbidden
    assert not offending, f"{module_path} references {sorted(offending)}"


def test_calendar_module_touches_no_database_and_no_broker():
    """The calendar answers a question; it never reads or mutates state."""
    tree = ast.parse((BACKEND_DIR / "app" / "market" / "calendar.py").read_text())

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
        elif isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
    assert not any(
        mod.startswith(("sqlalchemy", "httpx", "app.models", "app.broker", "app.services"))
        for mod in imported
    ), f"calendar must stay pure, imports: {sorted(imported)}"

    referenced = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not (referenced & {"commit", "flush", "delete", "execute", "place_order"})
