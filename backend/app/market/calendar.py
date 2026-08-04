"""Trading calendar for BYMA securities — minute precision, fail closed.

The previous configuration could only express whole hours, so the real BYMA
open (10:30 ART) was unrepresentable. This module parses HH:MM times, keeps
backward compatibility with the deprecated hour-only settings, and answers
one question honestly: may an order be sent right now?

Fail closed everywhere:
- an unparseable or inconsistent schedule is `market_schedule_unknown`,
  never "assume it is open";
- weekends and configured holidays are `market_closed`;
- a closing auction / special session we do not model is not guessed.

Fund cutoffs are deliberately NOT handled here: a fund's cutoff is per fund
and belongs to its catalog entry, not to the securities calendar.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")

MARKET_CLOSED = "market_closed"
MARKET_SCHEDULE_UNKNOWN = "market_schedule_unknown"


def parse_hhmm(value) -> time | None:
    """Parse 'HH:MM' into a time. Returns None for anything else.

    Deliberately strict: '9', '9:5', '25:00' and '' are all rejected rather
    than coerced, because a misread schedule silently shifts when orders may
    be sent.
    """
    text = str(value or "").strip()
    match = _TIME_RE.match(text)
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return time(hour=hour, minute=minute)


def parse_holidays(value) -> tuple[set[date], bool]:
    """Parse a holiday list (CSV string or list of 'YYYY-MM-DD').

    Returns (holidays, valid). An unparseable entry makes the whole list
    invalid — a half-read holiday calendar is worse than none, because it
    would silently declare a closed day open.
    """
    if value is None:
        return set(), True
    items = value.split(",") if isinstance(value, str) else value
    if not isinstance(items, (list, tuple)):
        return set(), False

    holidays: set[date] = set()
    for raw in items:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            holidays.add(date.fromisoformat(text))
        except (ValueError, TypeError):
            return set(), False
    return holidays, True


def resolve_market_schedule(settings) -> dict:
    """Resolve the effective securities schedule from settings.

    Precedence: the explicit HH:MM settings win. The deprecated hour-only
    settings are honoured only when the HH:MM ones are absent, so an existing
    deployment keeps working while it migrates.
    """
    open_time = parse_hhmm(getattr(settings, "scheduler_market_open_time", ""))
    close_time = parse_hhmm(getattr(settings, "scheduler_market_close_time", ""))

    open_source = "scheduler_market_open_time"
    close_source = "scheduler_market_close_time"
    deprecated_used = False

    if open_time is None:
        hour = getattr(settings, "scheduler_market_open_hour", None)
        if isinstance(hour, int) and 0 <= hour <= 23:
            open_time = time(hour=hour, minute=0)
            open_source = "scheduler_market_open_hour (deprecated)"
            deprecated_used = True
    if close_time is None:
        hour = getattr(settings, "scheduler_market_close_hour", None)
        if isinstance(hour, int) and 0 <= hour <= 23:
            close_time = time(hour=hour, minute=0)
            close_source = "scheduler_market_close_hour (deprecated)"
            deprecated_used = True

    tz_name = str(getattr(settings, "scheduler_timezone", "") or "").strip()
    tz = None
    tz_error = None
    try:
        tz = ZoneInfo(tz_name) if tz_name else None
    except (ZoneInfoNotFoundError, ValueError, KeyError, ModuleNotFoundError):
        tz_error = MARKET_SCHEDULE_UNKNOWN

    holidays, holidays_valid = parse_holidays(getattr(settings, "market_holidays", None))

    errors: list[str] = []
    if open_time is None or close_time is None or tz is None or tz_error:
        errors.append(MARKET_SCHEDULE_UNKNOWN)
    elif open_time >= close_time:
        # A close at or before the open cannot describe a session.
        errors.append(MARKET_SCHEDULE_UNKNOWN)
    if not holidays_valid:
        errors.append(MARKET_SCHEDULE_UNKNOWN)

    return {
        "open_time": open_time,
        "close_time": close_time,
        "timezone": tz_name,
        "tzinfo": tz,
        "holidays": holidays if holidays_valid else set(),
        "holidays_valid": holidays_valid,
        "open_source": open_source,
        "close_source": close_source,
        "deprecated_settings_in_use": deprecated_used,
        "errors": errors,
        "configured": not errors,
    }


def market_session_state(settings, *, now: datetime | None = None) -> dict:
    """Is the securities market open for order submission right now?

    Returns {open, code, ...}. `code` is None only when open is True.
    """
    schedule = resolve_market_schedule(settings)
    detail = {
        "timezone": schedule["timezone"],
        "open_time": schedule["open_time"].strftime("%H:%M") if schedule["open_time"] else None,
        "close_time": schedule["close_time"].strftime("%H:%M") if schedule["close_time"] else None,
        "deprecated_settings_in_use": schedule["deprecated_settings_in_use"],
    }

    if schedule["errors"]:
        return {**detail, "open": False, "code": MARKET_SCHEDULE_UNKNOWN, "local_time": None}

    tz = schedule["tzinfo"]
    current = now.astimezone(tz) if now is not None else datetime.now(tz)
    detail["local_time"] = current.isoformat()
    detail["local_date"] = current.date().isoformat()

    if current.weekday() >= 5:
        return {**detail, "open": False, "code": MARKET_CLOSED, "reason": "weekend"}
    if current.date() in schedule["holidays"]:
        return {**detail, "open": False, "code": MARKET_CLOSED, "reason": "holiday"}

    local_time = current.time()
    if not (schedule["open_time"] <= local_time < schedule["close_time"]):
        return {**detail, "open": False, "code": MARKET_CLOSED, "reason": "outside_session"}

    return {**detail, "open": True, "code": None}


def session_ingestion_slots(settings, interval_minutes: int) -> list[tuple[int, int]]:
    """EXACT in-session slots, starting AT the open and stopping before close.

    For a 10:30–17:00 session on a 30-minute interval this is exactly
    10:30, 11:00, … 16:30 — and notably NOT 10:00.

    A naive cron of `minute=*/30, hour=10-16` also fires at 10:00, half an
    hour before the market opens, and that slot then reports itself as
    "market hours". Enumerating the slots removes the whole class of
    off-by-one-grid bugs instead of patching the expression.
    """
    schedule = resolve_market_schedule(settings)
    if schedule["errors"] or not schedule["open_time"] or not schedule["close_time"]:
        return []
    try:
        interval = int(interval_minutes)
    except (TypeError, ValueError):
        return []
    if interval <= 0:
        return []

    open_m = schedule["open_time"].hour * 60 + schedule["open_time"].minute
    close_m = schedule["close_time"].hour * 60 + schedule["close_time"].minute

    slots: list[tuple[int, int]] = []
    current = open_m
    while current < close_m:
        slots.append((current // 60, current % 60))
        current += interval
    return slots


def group_slots_by_minute(slots: list[tuple[int, int]]) -> dict[int, list[int]]:
    """Group exact slots into {minute: [hours]} for compact cron triggers.

    Two cron jobs (minute=30 hour=10-16, minute=0 hour=11-16) express the
    13 real slots of a 10:30–17:00/30min session precisely, with no extra
    firing.
    """
    grouped: dict[int, list[int]] = {}
    for hour, minute in slots:
        grouped.setdefault(minute, []).append(hour)
    return {minute: sorted(set(hours)) for minute, hours in grouped.items()}


def premarket_times(settings, minutes_before: list[int]) -> list[tuple[int, int]]:
    """(hour, minute) pairs N minutes before the open, on the same day.

    A premarket run that would fall on the previous day is dropped instead of
    wrapping around to 23:xx, which is what the old whole-hour arithmetic did
    for an early open.
    """
    schedule = resolve_market_schedule(settings)
    if schedule["errors"] or schedule["open_time"] is None:
        return []

    open_minutes = schedule["open_time"].hour * 60 + schedule["open_time"].minute
    result: list[tuple[int, int]] = []
    for offset in minutes_before or []:
        try:
            delta = int(offset)
        except (TypeError, ValueError):
            continue
        total = open_minutes - delta
        if total < 0:
            continue
        pair = (total // 60, total % 60)
        if pair not in result:
            result.append(pair)
    return result
