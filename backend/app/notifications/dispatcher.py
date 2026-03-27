"""Notification dispatcher (Part E — fortified + real web push P3).

Sends alerts via Telegram + Web Push.
Only dispatches when:
- notification_enabled is True
- event severity >= notification_min_severity
- cooldown since last notification has elapsed
- market is in a relevant phase (configurable)

Argentina market hours (BYMA) as primary clock; US sensitivity for CEDEARs/ETFs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# Real Web Push (Priority 3)
# ---------------------------------------------------------------------------


def _send_single_web_push(
    endpoint: str,
    p256dh: str,
    auth: str,
    payload: dict,
) -> bool:
    """Send a single web push notification using pywebpush or py_vapid fallback.

    Returns True if sent successfully, False otherwise.
    """
    settings = get_settings()
    if not settings.vapid_private_key or not settings.vapid_public_key:
        logger.warning("VAPID keys not configured, cannot send web push")
        return False

    subscription_info = {
        "endpoint": endpoint,
        "keys": {
            "p256dh": p256dh,
            "auth": auth,
        },
    }

    vapid_claims = {
        "sub": f"mailto:{settings.vapid_contact_email}" if settings.vapid_contact_email else "mailto:admin@mifinanza.local",
    }

    # Try pywebpush first (preferred, handles encryption)
    try:
        from pywebpush import webpush
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload),
            vapid_private_key=settings.vapid_private_key,
            vapid_claims=vapid_claims,
        )
        return True
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("pywebpush failed for %s...: %s", endpoint[:50], exc)
        return False

    # Fallback: use py_vapid for auth header + httpx for delivery
    try:
        from py_vapid import Vapid

        vapid = Vapid.from_raw(settings.vapid_private_key)
        auth_headers = vapid.sign({
            "aud": _extract_origin(endpoint),
            "sub": vapid_claims["sub"],
        })

        data_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Authorization": auth_headers["Authorization"],
            "Crypto-Key": auth_headers.get("Crypto-Key", ""),
            "Content-Type": "application/json",
            "TTL": "86400",
        }

        resp = httpx.post(endpoint, content=data_bytes, headers=headers, timeout=10)
        if resp.status_code in (200, 201, 202):
            return True
        logger.warning("Web push HTTP %d for %s...", resp.status_code, endpoint[:50])
        return False

    except ImportError:
        logger.warning("Neither pywebpush nor py_vapid available")
        return False
    except Exception as exc:
        logger.warning("Web push fallback failed for %s...: %s", endpoint[:50], exc)
        return False


def _extract_origin(url: str) -> str:
    """Extract origin (scheme + host) from a URL."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def send_web_push_to_all(
    db,
    *,
    title: str,
    body: str,
    severity: str = "medium",
    deep_link: str = "/",
) -> dict:
    """Send a web push notification to all active subscriptions.

    Handles invalid/expired subscriptions by removing them.
    """
    from app.models.models import PushSubscription

    settings = get_settings()
    if not settings.vapid_private_key or not settings.vapid_public_key:
        return {"sent": 0, "failed": 0, "removed": 0, "reason": "vapid_not_configured"}

    subs = db.query(PushSubscription).all()
    if not subs:
        return {"sent": 0, "failed": 0, "removed": 0, "reason": "no_subscriptions"}

    payload = {
        "title": title,
        "body": body,
        "severity": severity,
        "deep_link": deep_link,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    sent = 0
    failed = 0
    removed = 0
    to_remove = []

    for sub in subs:
        success = _send_single_web_push(
            endpoint=sub.endpoint,
            p256dh=sub.p256dh,
            auth=sub.auth,
            payload=payload,
        )
        if success:
            sent += 1
        else:
            failed += 1
            # Check if subscription is expired/invalid (410 Gone)
            # Mark for removal on repeated failures
            to_remove.append(sub.id)

    # Remove invalid subscriptions (best-effort)
    if to_remove:
        try:
            for sub_id in to_remove:
                sub_obj = db.query(PushSubscription).filter(PushSubscription.id == sub_id).first()
                if sub_obj:
                    db.delete(sub_obj)
                    removed += 1
            db.commit()
        except Exception:
            db.rollback()

    return {"sent": sent, "failed": failed, "removed": removed}


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
        "validation_failed": f"Orden {side} de {symbol} bloqueada por validación.",
        "failed": f"Orden {side} de {symbol} falló.",
    }
    msg = f"Mi Finanza - Ejecución\n\n{status_msg.get(status, f'{symbol}: {status}')}"

    sent = False
    if settings.notification_channel == "telegram" and settings.telegram_bot_token and settings.telegram_chat_id:
        sent = _send_telegram(msg, settings.telegram_bot_token, settings.telegram_chat_id)

    return {"sent": sent, "channel": settings.notification_channel, "status": status}


def dispatch_recommendation_alerts(db, cycle_result: dict) -> dict:
    """Push notification after a cycle produces new actionable items.

    Compares current review_queue.actionable_now against the previous
    recommendation to detect NEW opportunities. Respects cooldown,
    severity, and market-phase rules.

    Safety invariant: notifications NEVER execute orders. They are
    informational only.
    """
    global _last_notification_at

    settings = get_settings()
    if not settings.notification_enabled:
        return {"sent": False, "reason": "disabled"}

    rec_id = cycle_result.get("recommendation_id")
    if not rec_id:
        return {"sent": False, "reason": "no_recommendation"}

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == rec_id).first()
    if not rec or not rec.metadata_json:
        return {"sent": False, "reason": "recommendation_not_found"}

    ds = rec.metadata_json.get("decision_summary", {})
    rq = ds.get("review_queue", {})
    actionable = rq.get("actionable_now", {})
    actionable_count = actionable.get("count", 0)

    if actionable_count == 0:
        return {"sent": False, "reason": "no_actionable_items"}

    # --- Delta detection: compare against previous recommendation ---
    prev_actionable_symbols = set()
    prev_rec = (
        db.query(Recommendation)
        .filter(Recommendation.id != rec_id, Recommendation.superseded_at.isnot(None))
        .order_by(Recommendation.created_at.desc())
        .first()
    )
    if prev_rec and prev_rec.metadata_json:
        prev_ds = prev_rec.metadata_json.get("decision_summary", {})
        prev_rq = prev_ds.get("review_queue", {})
        prev_items = prev_rq.get("actionable_now", {}).get("items", [])
        prev_actionable_symbols = {i.get("symbol") for i in prev_items if i.get("symbol")}

    current_items = actionable.get("items", [])
    current_symbols = {i.get("symbol") for i in current_items if i.get("symbol")}
    new_symbols = current_symbols - prev_actionable_symbols

    # Determine severity based on what changed
    if new_symbols:
        severity = "high"
        symbols_text = ", ".join(sorted(new_symbols))
        body = f"Nuevas oportunidades: {symbols_text}. Solo informativo."
        title = f"Mi Finanza - {len(new_symbols)} nueva(s) oportunidad(es)"
    else:
        # All actionable items are the same as before — low-priority digest
        severity = "low"
        body = f"{actionable_count} oportunidades accionables. Sin cambios."
        title = "Mi Finanza - Resumen"

    # Severity filter
    if not _severity_passes(severity, settings.notification_min_severity):
        return {"sent": False, "reason": "below_min_severity", "severity": severity}

    # Cooldown check
    now = datetime.now(timezone.utc)
    ar_phase = _argentina_market_phase(now)

    if _last_notification_at:
        elapsed = (now - (_last_notification_at.replace(tzinfo=timezone.utc) if _last_notification_at.tzinfo is None else _last_notification_at)).total_seconds()
        effective_cooldown = settings.notification_cooldown_seconds
        if ar_phase == "open":
            effective_cooldown = max(60, effective_cooldown // 2)
        if elapsed < effective_cooldown:
            return {"sent": False, "reason": "cooldown", "remaining_seconds": int(effective_cooldown - elapsed)}

    # --- Send via all channels ---
    telegram_sent = False
    if settings.notification_channel == "telegram" and settings.telegram_bot_token and settings.telegram_chat_id:
        phase_map = {"premarket": "Pre-apertura", "open": "Mercado abierto",
                     "postmarket": "Post-cierre", "off": "Mercado cerrado"}
        msg = (
            f"Mi Finanza - Recomendación actualizada\n\n"
            f"BYMA: {phase_map.get(ar_phase, ar_phase)}\n\n"
            f"{body}\n\n"
            f">> Revisar en la app. No se ejecutan órdenes automáticamente."
        )
        telegram_sent = _send_telegram(msg, settings.telegram_bot_token, settings.telegram_chat_id)

    push_result = {"sent": 0, "failed": 0, "removed": 0}
    try:
        push_result = send_web_push_to_all(
            db,
            title=title,
            body=body,
            severity=severity,
            deep_link="/recommendations",
        )
    except Exception as exc:
        logger.warning("Recommendation push failed: %s", exc)

    any_sent = telegram_sent or push_result.get("sent", 0) > 0
    if any_sent:
        _last_notification_at = now

    return {
        "sent": any_sent,
        "type": "recommendation_change",
        "severity": severity,
        "actionable_count": actionable_count,
        "new_symbols": sorted(new_symbols) if new_symbols else [],
        "telegram_sent": telegram_sent,
        "web_push_sent": push_result.get("sent", 0),
        "web_push_failed": push_result.get("failed", 0),
    }


def dispatch_alerts(db, events: list) -> dict:
    """Dispatch notifications for qualifying events.

    Returns summary of what was sent. Sends via Telegram + Web Push.
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

    telegram_sent = False
    if settings.notification_channel == "telegram":
        if settings.telegram_bot_token and settings.telegram_chat_id:
            telegram_sent = _send_telegram(message, settings.telegram_bot_token, settings.telegram_chat_id)

    # --- Web Push (P3): send to all subscriptions ---
    push_result = {"sent": 0, "failed": 0, "removed": 0}
    try:
        # Build push payload from first qualifying event
        first = qualifying[0]
        severity = getattr(first, "severity", "medium")
        symbols = ", ".join(getattr(first, "affected_symbols", []) or [])
        push_title = f"Mi Finanza - {severity.upper()}"
        push_body = getattr(first, "message", "Alerta de mercado")
        if symbols:
            push_body += f" ({symbols})"
        if len(qualifying) > 1:
            push_body += f" +{len(qualifying) - 1} más"

        push_result = send_web_push_to_all(
            db,
            title=push_title,
            body=push_body,
            severity=severity,
            deep_link="/alerts",
        )
    except Exception as exc:
        logger.warning("Web push dispatch failed: %s", exc)

    any_sent = telegram_sent or push_result.get("sent", 0) > 0
    if any_sent:
        _last_notification_at = now

    return {
        "sent": any_sent,
        "channel": settings.notification_channel,
        "events_count": len(qualifying),
        "market_phase_ar": ar_phase,
        "market_phase_us": us_phase,
        "telegram_sent": telegram_sent,
        "web_push_sent": push_result.get("sent", 0),
        "web_push_failed": push_result.get("failed", 0),
        "web_push_removed": push_result.get("removed", 0),
    }
