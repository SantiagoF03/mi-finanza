"""Notification dispatcher (Part E — fortified).

Sends alerts via Telegram (or email, extensible).
Only dispatches when:
- notification_enabled is True
- event severity >= notification_min_severity
- cooldown since last notification has elapsed
- market is in a relevant phase (configurable)

Argentina market hours (BYMA) as primary clock; US sensitivity for CEDEARs/ETFs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from app.core.config import get_settings

SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

# CEDEARs and US ETFs that should trigger US-hours sensitivity
_US_SENSITIVE_TYPES = {"CEDEAR", "ETF"}

_last_notification_at: datetime | None = None


def _severity_passes(event_severity: str, min_severity: str) -> bool:
    return SEVERITY_ORDER.get(event_severity, 0) >= SEVERITY_ORDER.get(min_severity, 1)


def _argentina_market_phase(now_utc: datetime | None = None) -> str:
    """Argentina/BYMA market phase (primary clock).

    BYMA hours: 11:00–20:00 UTC (08:00–17:00 ART).
    Returns: premarket, open, postmarket, off.
    """
    settings = get_settings()
    now = now_utc or datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()

    if weekday >= 5:
        return "off"

    open_h = settings.scheduler_market_open_hour  # 11 UTC
    close_h = settings.scheduler_market_close_hour  # 20 UTC

    if (open_h - 2) <= hour < open_h:
        return "premarket"
    if open_h <= hour < close_h:
        return "open"
    if close_h <= hour < close_h + 2:
        return "postmarket"
    return "off"


def _us_market_phase(now_utc: datetime | None = None) -> str:
    """US market phase (secondary clock for CEDEARs/ETFs).

    NYSE/NASDAQ: 14:30–21:00 UTC (09:30–16:00 ET).
    """
    now = now_utc or datetime.now(timezone.utc)
    hour = now.hour
    minute = now.minute
    weekday = now.weekday()

    if weekday >= 5:
        return "off"

    # Pre-market: 13:00–14:30 UTC
    if hour == 13 or (hour == 14 and minute < 30):
        return "premarket"
    # Open: 14:30–21:00 UTC
    if (hour == 14 and minute >= 30) or (15 <= hour < 21):
        return "open"
    # Post-market: 21:00–22:00 UTC
    if hour == 21:
        return "postmarket"
    return "off"


def _affects_us_assets(event) -> bool:
    """Check if event affects US-sensitive assets (CEDEARs, ETFs)."""
    symbols = getattr(event, "affected_symbols", []) or []
    # Simple heuristic: if any affected symbol is typically US-traded
    _KNOWN_US = {"AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NVDA",
                 "SPY", "QQQ", "VOO", "VTI", "IVV", "ACWI", "BND", "AGG",
                 "DIA", "IWM", "EEM", "ARKK", "MELI", "GLOB", "BABA", "TSM",
                 "KO", "PEP", "WMT", "JNJ", "V", "MA", "JPM", "BAC"}
    return bool(set(symbols) & _KNOWN_US)


def _action_hint(event) -> str:
    """Generate an actionable hint for the alert message."""
    trigger = getattr(event, "trigger_type", "")
    severity = getattr(event, "severity", "low")
    symbols = ", ".join(getattr(event, "affected_symbols", []) or [])

    if trigger == "holding_risk":
        return f"Revisar posición en {symbols}. Considerar reducir exposición."
    if trigger == "holding_opportunity":
        return f"Oportunidad en {symbols}. Esperar próxima recomendación."
    if trigger in ("macro_risk", "macro_signal"):
        return "Evento macro. Recomendación se recalculará automáticamente."
    if trigger == "external_opportunity":
        return f"Nueva oportunidad externa en {symbols}. Solo informativo."
    if trigger == "sector_rotation":
        return "Rotación sectorial detectada. Revisar composición."
    if severity == "critical":
        return "Evento crítico. Revisar cartera en la app."
    return "Revisar detalle en la app."


def _format_alert_message(events: list, ar_phase: str, us_phase: str) -> str:
    """Build a clear, actionable Telegram message."""
    lines = ["Mi Finanza - Alerta de mercado\n"]

    # Market status context
    phase_map = {"premarket": "Pre-apertura", "open": "Mercado abierto",
                 "postmarket": "Post-cierre", "off": "Mercado cerrado"}
    lines.append(f"BYMA: {phase_map.get(ar_phase, ar_phase)} | USA: {phase_map.get(us_phase, us_phase)}\n")

    for evt in events[:5]:
        severity = getattr(evt, "severity", "?")
        message = getattr(evt, "message", "?")
        symbols = ", ".join(getattr(evt, "affected_symbols", []) or [])
        trigger = getattr(evt, "trigger_type", "")

        severity_emoji = {"critical": "!!", "high": "!", "medium": "-", "low": "."}
        prefix = severity_emoji.get(severity, "")

        lines.append(f"[{severity.upper()}]{prefix} {message}")
        if symbols:
            lines.append(f"  Activos: {symbols}")
        if trigger:
            lines.append(f"  Tipo: {trigger}")

        hint = _action_hint(evt)
        lines.append(f"  >> {hint}")
        lines.append("")

    if len(events) > 5:
        lines.append(f"... y {len(events) - 5} alertas más")

    return "\n".join(lines)


def _send_telegram(message: str, bot_token: str, chat_id: str) -> bool:
    """Send message via Telegram Bot API. Returns True on success."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = httpx.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def dispatch_execution_notification(order_execution) -> dict:
    """Send notification about an execution state change."""
    settings = get_settings()
    if not settings.notification_enabled:
        return {"sent": False, "reason": "disabled"}

    status = getattr(order_execution, "status", "unknown")
    symbol = getattr(order_execution, "symbol", "?")
    side = getattr(order_execution, "side", "?")

    status_msg = {
        "execution_requested": f"Orden {side} de {symbol} solicitada al broker.",
        "execution_sent": f"Orden {side} de {symbol} enviada a IOL.",
        "executed": f"Orden {side} de {symbol} ejecutada exitosamente.",
        "partially_executed": f"Orden {side} de {symbol} ejecutada parcialmente.",
        "rejected_by_broker": f"Orden {side} de {symbol} rechazada por el broker.",
        "failed": f"Orden {side} de {symbol} falló.",
    }
    msg = f"Mi Finanza - Ejecución\n\n{status_msg.get(status, f'{symbol}: {status}')}"

    sent = False
    if settings.notification_channel == "telegram" and settings.telegram_bot_token and settings.telegram_chat_id:
        sent = _send_telegram(msg, settings.telegram_bot_token, settings.telegram_chat_id)

    return {"sent": sent, "channel": settings.notification_channel, "status": status}


def dispatch_alerts(db, events: list) -> dict:
    """Dispatch notifications for qualifying events.

    Returns summary of what was sent.
    """
    global _last_notification_at

    settings = get_settings()

    if not settings.notification_enabled:
        return {"sent": False, "reason": "disabled"}

    now = datetime.now(timezone.utc)
    ar_phase = _argentina_market_phase(now)
    us_phase = _us_market_phase(now)

    # Cooldown check — shorter during market hours
    if _last_notification_at:
        elapsed = (now - _last_notification_at.replace(tzinfo=timezone.utc) if _last_notification_at.tzinfo is None else now - _last_notification_at).total_seconds()
        effective_cooldown = settings.notification_cooldown_seconds
        if ar_phase == "open":
            effective_cooldown = max(60, effective_cooldown // 2)  # halve cooldown during market hours
        if elapsed < effective_cooldown:
            return {"sent": False, "reason": "cooldown", "remaining_seconds": int(effective_cooldown - elapsed)}

    # Filter by severity
    qualifying = [e for e in events if _severity_passes(getattr(e, "severity", "low"), settings.notification_min_severity)]
    if not qualifying:
        return {"sent": False, "reason": "no_qualifying_events"}

    # During off-hours, only send critical alerts (unless they affect US assets during US hours)
    if ar_phase == "off":
        critical_or_us = []
        for e in qualifying:
            sev = getattr(e, "severity", "low")
            if sev == "critical":
                critical_or_us.append(e)
            elif _affects_us_assets(e) and us_phase in ("premarket", "open", "postmarket"):
                critical_or_us.append(e)
        qualifying = critical_or_us
        if not qualifying:
            return {"sent": False, "reason": "off_hours_no_critical"}

    message = _format_alert_message(qualifying, ar_phase, us_phase)

    sent = False
    if settings.notification_channel == "telegram":
        if settings.telegram_bot_token and settings.telegram_chat_id:
            sent = _send_telegram(message, settings.telegram_bot_token, settings.telegram_chat_id)

    if sent:
        _last_notification_at = now

    return {
        "sent": sent,
        "channel": settings.notification_channel,
        "events_count": len(qualifying),
        "market_phase_ar": ar_phase,
        "market_phase_us": us_phase,
    }
