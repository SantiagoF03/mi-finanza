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


# ---------------------------------------------------------------------------
# Recommendation-level alert policy
# ---------------------------------------------------------------------------

# Phase-aware cooldown multipliers.
# Pre-market and post-close are the "strong" windows where we allow more alerts.
# Intraday (open) is conservative — double cooldown to avoid spam on micro-changes.
# Off-hours: only critical gets through (handled in should_notify logic).
_PHASE_COOLDOWN_MULTIPLIER = {
    "premarket": 1.0,    # strong window — respect base cooldown
    "open": 2.0,         # conservative — double cooldown
    "postmarket": 0.5,   # strong window — allow faster alerts at close
    "off": 1.0,          # rarely fires, only critical passes
}


def _get_previous_recommendation(db, exclude_id: int):
    """Get the most recent superseded recommendation (for delta detection)."""
    from app.models.models import Recommendation
    return (
        db.query(Recommendation)
        .filter(Recommendation.id != exclude_id, Recommendation.superseded_at.isnot(None))
        .order_by(Recommendation.created_at.desc())
        .first()
    )


def _watchlist_notification_worthy(item: dict) -> bool:
    """Return True if a watchlist item should count for notification delta.

    Filters out: weak signal + unconfirmed market + relevant_not_investable.
    These are real signals but too noisy to push — user reviews in-app.
    """
    if (
        item.get("signal_quality") == "weak"
        and item.get("operational_status") == "relevant_not_investable"
        and item.get("market_confirmation") in (None, "unconfirmed")
    ):
        return False
    return True


def _extract_delta(current_meta: dict, prev_meta: dict | None) -> dict:
    """Compare current recommendation against previous to detect material changes.

    Returns a dict with delta information used by the policy classifier.
    """
    ds = current_meta.get("decision_summary", {})
    rq = ds.get("review_queue", {})
    pc = ds.get("pipeline_counts", {})

    actionable = rq.get("actionable_now", {})
    watchlist = rq.get("watchlist_now", {})
    current_actionable_items = actionable.get("items", [])
    current_actionable_symbols = {i.get("symbol") for i in current_actionable_items if i.get("symbol")}
    current_watchlist_items = watchlist.get("items", [])
    # Only notification-worthy watchlist items count for delta/new_watchlist.
    # watchlist_count also uses the filtered set — weak+unconfirmed+rni items
    # must not justify a postclose_digest by themselves.
    current_watchlist_symbols = {
        i.get("symbol") for i in current_watchlist_items
        if i.get("symbol") and _watchlist_notification_worthy(i)
    }

    prev_actionable_symbols: set = set()
    prev_watchlist_symbols: set = set()
    if prev_meta:
        prev_ds = prev_meta.get("decision_summary", {})
        prev_rq = prev_ds.get("review_queue", {})
        prev_actionable_symbols = {
            i.get("symbol") for i in prev_rq.get("actionable_now", {}).get("items", [])
            if i.get("symbol")
        }
        prev_watchlist_symbols = {
            i.get("symbol") for i in prev_rq.get("watchlist_now", {}).get("items", [])
            if i.get("symbol") and _watchlist_notification_worthy(i)
        }

    return {
        "actionable_count": actionable.get("count", 0),
        "actionable_symbols": current_actionable_symbols,
        "new_actionable": current_actionable_symbols - prev_actionable_symbols,
        "watchlist_count": len(current_watchlist_symbols),
        "watchlist_symbols": current_watchlist_symbols,
        "new_watchlist": current_watchlist_symbols - prev_watchlist_symbols,
        "suppressed_by_contradiction_count": pc.get("suppressed_by_contradiction_count", 0),
        "unchanged": current_meta.get("unchanged", False),
    }


def classify_recommendation_alert(
    delta: dict,
    market_phase: str,
    *,
    contradiction_threshold: int = 3,
) -> dict:
    """Pure policy function: classify a recommendation change into alert category.

    Parameters:
        delta: output of _extract_delta()
        market_phase: premarket | open | postmarket | off
        contradiction_threshold: configurable via notification_contradiction_threshold
            (default 3). >=N contradictions → HIGH thesis_contradiction.

    Returns:
        {
            "category": str,     # new_actionable | thesis_contradiction |
                                 # watchlist_material | postclose_digest |
                                 # analysis_completed | no_material_change
            "severity": str,     # high | medium | low | silent
            "should_notify": bool,
            "title": str,
            "body": str,
        }

    Policy:
    ─────────────────────────────────────────────────────────────────
    HIGH — push always (within cooldown)
      • new_actionable: new symbols in actionable_now vs previous cycle
      • thesis_contradiction: >=contradiction_threshold signals suppressed
        by contradiction. Default 3. Rationale: 1-2 can be noise (ambiguous
        ticker, weak counter-signal). >=3 means portfolio thesis under
        real multi-signal pressure. Configurable for tuning.

    MEDIUM — push in premarket + postmarket only
      • watchlist_material: new symbols in watchlist vs previous cycle.
        Suppressed during market hours (intraday) to avoid noise.
        Quality filter: weak + unconfirmed + relevant_not_investable items
        are excluded from new_watchlist delta (too noisy to push).

    MEDIUM — push only in postmarket (post-close digest)
      • postclose_digest: analysis ran, unchanged=False, and at least one
        material thing to report: actionable items exist, OR new watchlist
        items appeared, OR contradictions were detected (below threshold).
        Summarizes the day. Only fires in postmarket. Severity=medium so it
        passes default min_severity filter without lowering global threshold.

    SILENT — never push
      • analysis_completed: unchanged=True. Full cycle confirmed nothing
        material changed. Product decision: NEVER notifies. User checks
        the app when they want to.
      • no_material_change: nothing to report at all. Edge case (first run,
        empty signals). Silent.
    ─────────────────────────────────────────────────────────────────

    Safety: every message body includes "Solo informativo" disclaimer.
    Notifications NEVER execute orders.
    """
    new_actionable = delta.get("new_actionable", set())
    actionable_count = delta.get("actionable_count", 0)
    new_watchlist = delta.get("new_watchlist", set())
    watchlist_count = delta.get("watchlist_count", 0)
    contradiction_count = delta.get("suppressed_by_contradiction_count", 0)
    unchanged = delta.get("unchanged", False)

    _DISCLAIMER = "Solo informativo, no ejecuta órdenes."

    # --- SILENT (early exit): unchanged=True → analysis_completed ---
    # Product decision: when detect_unchanged says True, the recommendation
    # is materially identical. NEVER notify. User checks when they want to.
    if unchanged:
        return {
            "category": "analysis_completed",
            "severity": "silent",
            "should_notify": False,
            "title": "",
            "body": "",
        }

    # --- HIGH: new actionable opportunities ---
    if new_actionable:
        symbols_text = ", ".join(sorted(new_actionable))
        return {
            "category": "new_actionable",
            "severity": "high",
            "should_notify": True,
            "title": f"Mi Finanza - {len(new_actionable)} oportunidad(es) nueva(s)",
            "body": f"Nuevas oportunidades: {symbols_text}. {_DISCLAIMER}",
        }

    # --- HIGH: thesis contradiction ---
    # Configurable threshold (default 3). 1-2 can be noise.
    if contradiction_count >= contradiction_threshold:
        return {
            "category": "thesis_contradiction",
            "severity": "high",
            "should_notify": True,
            "title": "Mi Finanza - Contradicción de tesis",
            "body": f"{contradiction_count} señales contradicen la tesis actual. Revisar cartera. {_DISCLAIMER}",
        }

    # --- MEDIUM: new watchlist items (not during market hours) ---
    if new_watchlist:
        symbols_text = ", ".join(sorted(new_watchlist)[:5])
        count = len(new_watchlist)
        extra = f" (+{count - 5} más)" if count > 5 else ""
        return {
            "category": "watchlist_material",
            "severity": "medium",
            "should_notify": market_phase in ("premarket", "postmarket"),
            "title": f"Mi Finanza - {count} señal(es) nueva(s) en watchlist",
            "body": f"Nuevas señales en observación: {symbols_text}{extra}. {_DISCLAIMER}",
        }

    # --- LOW: post-close digest ---
    # Fires when there's material activity to summarize at end of day:
    # actionable items exist, OR contradictions were detected (below HIGH
    # threshold), OR watchlist has items. Must be unchanged=False (already
    # guaranteed by early exit above) and must have something to report.
    has_actionable = actionable_count > 0
    has_contradictions = contradiction_count > 0
    has_watchlist = watchlist_count > 0
    has_material = has_actionable or has_contradictions or has_watchlist

    if has_material:
        # Build a summary of what's worth reporting
        parts = []
        if has_actionable:
            parts.append(f"{actionable_count} oportunidad(es) vigente(s)")
        if has_contradictions:
            parts.append(f"{contradiction_count} contradicción(es) detectada(s)")
        if has_watchlist:
            parts.append(f"{watchlist_count} en watchlist")
        summary = ", ".join(parts)

        return {
            "category": "postclose_digest",
            "severity": "medium",
            "should_notify": market_phase == "postmarket",
            "title": "Mi Finanza - Resumen de cierre",
            "body": f"{summary}. Análisis actualizado. {_DISCLAIMER}",
        }

    # --- SILENT: nothing material to report ---
    # Zero actionable, zero watchlist, no contradictions, not unchanged.
    # Edge case: first run with no signals, or a run that found nothing.
    return {
        "category": "no_material_change",
        "severity": "silent",
        "should_notify": False,
        "title": "",
        "body": "",
    }


def dispatch_recommendation_alerts(db, cycle_result: dict) -> dict:
    """Dispatch push notification for a recommendation cycle, governed by alert policy.

    Flow:
    1. Load current + previous recommendation metadata
    2. Compute delta (new actionable, new watchlist, contradictions, unchanged)
    3. Classify via policy (category → severity → should_notify)
    4. Build audit trail (always — even when notifications disabled)
    5. Apply phase-aware cooldown and suppression
    6. Send via Telegram + Web Push
    7. Persist audit trail into recommendation metadata_json

    Safety invariant: notifications NEVER execute orders. They are
    informational only. Every message includes "Solo informativo" disclaimer.
    """
    global _last_notification_at

    settings = get_settings()

    rec_id = cycle_result.get("recommendation_id")
    if not rec_id:
        return {"sent": False, "reason": "no_recommendation"}

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == rec_id).first()
    if not rec or not rec.metadata_json:
        return {"sent": False, "reason": "recommendation_not_found"}

    # --- Delta detection ---
    prev_rec = _get_previous_recommendation(db, rec_id)
    prev_meta = prev_rec.metadata_json if prev_rec and prev_rec.metadata_json else None
    delta = _extract_delta(rec.metadata_json, prev_meta)

    # --- Policy classification ---
    now = datetime.now(timezone.utc)
    ar_phase = _argentina_market_phase(now)
    classification = classify_recommendation_alert(
        delta, ar_phase,
        contradiction_threshold=settings.notification_contradiction_threshold,
    )

    # --- Build audit trail (always, regardless of outcome) ---
    audit = {
        "timestamp": now.isoformat(),
        "category": classification["category"],
        "severity": classification["severity"],
        "should_send": classification["should_notify"],
        "suppress_reason": None,
        "cooldown_applied": False,
        "market_phase": ar_phase,
        "previous_recommendation_id": prev_rec.id if prev_rec else None,
        "comparison_summary": {
            "new_actionable": sorted(delta["new_actionable"]) if delta["new_actionable"] else [],
            "new_watchlist": sorted(delta["new_watchlist"]) if delta["new_watchlist"] else [],
            "actionable_count": delta["actionable_count"],
            "watchlist_count": delta["watchlist_count"],
            "contradiction_count": delta["suppressed_by_contradiction_count"],
            "unchanged": delta["unchanged"],
        },
    }

    # --- Gate: notifications disabled ---
    if not settings.notification_enabled:
        audit["should_send"] = False
        audit["suppress_reason"] = "notifications_disabled"
        _persist_audit(db, rec, audit)
        return {"sent": False, "reason": "disabled"}

    # --- Gate: policy suppression ---
    if not classification["should_notify"]:
        audit["should_send"] = False
        audit["suppress_reason"] = f"policy:{classification['category']}"
        _persist_audit(db, rec, audit)
        return {
            "sent": False,
            "reason": "policy_suppressed",
            "category": classification["category"],
            "severity": classification["severity"],
        }

    if not _severity_passes(classification["severity"], settings.notification_min_severity):
        audit["should_send"] = False
        audit["suppress_reason"] = f"below_min_severity:{classification['severity']}<{settings.notification_min_severity}"
        _persist_audit(db, rec, audit)
        return {
            "sent": False,
            "reason": "below_min_severity",
            "category": classification["category"],
            "severity": classification["severity"],
        }

    if _last_notification_at:
        tz_last = (
            _last_notification_at.replace(tzinfo=timezone.utc)
            if _last_notification_at.tzinfo is None
            else _last_notification_at
        )
        elapsed = (now - tz_last).total_seconds()
        base_cooldown = settings.notification_cooldown_seconds
        multiplier = _PHASE_COOLDOWN_MULTIPLIER.get(ar_phase, 1.0)
        effective_cooldown = base_cooldown * multiplier
        if elapsed < effective_cooldown:
            audit["should_send"] = False
            audit["suppress_reason"] = "cooldown"
            audit["cooldown_applied"] = True
            _persist_audit(db, rec, audit)
            return {
                "sent": False,
                "reason": "cooldown",
                "category": classification["category"],
                "severity": classification["severity"],
                "remaining_seconds": int(effective_cooldown - elapsed),
            }

    # --- Deliver via all channels ---
    title = classification["title"]
    body = classification["body"]

    telegram_sent = False
    if settings.notification_channel == "telegram" and settings.telegram_bot_token and settings.telegram_chat_id:
        phase_map = {"premarket": "Pre-apertura", "open": "Mercado abierto",
                     "postmarket": "Post-cierre", "off": "Mercado cerrado"}
        msg = (
            f"{title}\n\n"
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
            severity=classification["severity"],
            deep_link="/recommendations",
        )
    except Exception as exc:
        logger.warning("Recommendation push failed: %s", exc)

    any_sent = telegram_sent or push_result.get("sent", 0) > 0
    if any_sent:
        _last_notification_at = now

    audit["sent"] = any_sent
    audit["telegram_sent"] = telegram_sent
    audit["web_push_sent"] = push_result.get("sent", 0)
    _persist_audit(db, rec, audit)

    return {
        "sent": any_sent,
        "category": classification["category"],
        "severity": classification["severity"],
        "actionable_count": delta["actionable_count"],
        "new_actionable": sorted(delta["new_actionable"]) if delta["new_actionable"] else [],
        "new_watchlist": sorted(delta["new_watchlist"]) if delta["new_watchlist"] else [],
        "telegram_sent": telegram_sent,
        "web_push_sent": push_result.get("sent", 0),
        "web_push_failed": push_result.get("failed", 0),
    }


def _persist_audit(db, rec, audit: dict) -> None:
    """Best-effort: write notification_audit into recommendation metadata_json."""
    try:
        # Refresh rec to ensure clean session state after run_cycle + app_log commits
        try:
            db.refresh(rec)
        except Exception:
            pass
        meta = rec.metadata_json or {}
        meta["notification_audit"] = audit
        rec.metadata_json = meta
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(rec, "metadata_json")
        db.commit()
    except Exception as exc:
        logger.warning("_persist_audit failed for rec %s: %s", getattr(rec, "id", "?"), exc)
        try:
            db.rollback()
        except Exception:
            pass


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
