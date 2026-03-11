"""Notification dispatcher (Part E).

Sends alerts via Telegram (or email, extensible).
Only dispatches when:
- notification_enabled is True
- event severity >= notification_min_severity
- cooldown since last notification has elapsed
"""

from __future__ import annotations

from datetime import datetime, timedelta

import httpx

from app.core.config import get_settings

SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

_last_notification_at: datetime | None = None


def _severity_passes(event_severity: str, min_severity: str) -> bool:
    return SEVERITY_ORDER.get(event_severity, 0) >= SEVERITY_ORDER.get(min_severity, 1)


def _format_alert_message(events: list) -> str:
    """Build a clear, concise Telegram message."""
    lines = ["Mi Finanza - Alerta de mercado\n"]
    for evt in events[:5]:
        severity = getattr(evt, "severity", "?")
        message = getattr(evt, "message", "?")
        symbols = ", ".join(getattr(evt, "affected_symbols", []) or [])
        lines.append(f"[{severity.upper()}] {message}")
        if symbols:
            lines.append(f"  Activos: {symbols}")
        lines.append("")
    if len(events) > 5:
        lines.append(f"... y {len(events) - 5} más")
    return "\n".join(lines)


def _send_telegram(message: str, bot_token: str, chat_id: str) -> bool:
    """Send message via Telegram Bot API. Returns True on success."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = httpx.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def dispatch_alerts(db, events: list) -> dict:
    """Dispatch notifications for qualifying events.

    Returns summary of what was sent.
    """
    global _last_notification_at

    settings = get_settings()

    if not settings.notification_enabled:
        return {"sent": False, "reason": "disabled"}

    # Cooldown check
    now = datetime.utcnow()
    if _last_notification_at:
        elapsed = (now - _last_notification_at).total_seconds()
        if elapsed < settings.notification_cooldown_seconds:
            return {"sent": False, "reason": "cooldown"}

    # Filter by severity
    qualifying = [e for e in events if _severity_passes(getattr(e, "severity", "low"), settings.notification_min_severity)]
    if not qualifying:
        return {"sent": False, "reason": "no_qualifying_events"}

    message = _format_alert_message(qualifying)

    sent = False
    if settings.notification_channel == "telegram":
        if settings.telegram_bot_token and settings.telegram_chat_id:
            sent = _send_telegram(message, settings.telegram_bot_token, settings.telegram_chat_id)

    if sent:
        _last_notification_at = now

    return {"sent": sent, "channel": settings.notification_channel, "events_count": len(qualifying)}
