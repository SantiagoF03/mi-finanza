"""Explicit, human-initiated cancellation of a submitted order.

Contract: DELETE /api/v2/operaciones/{numeroOperacion}

INVARIANTS — the whole point of this module:
- cancellation is NEVER automatic, NEVER scheduled, NEVER LLM-triggered;
- exactly ONE DELETE per human decision, never a retry;
- an ambiguous outcome (timeout, 5xx) becomes `cancellation_unknown` and
  requires manual reconciliation — re-sending a DELETE we cannot prove failed
  risks cancelling a *different*, later order;
- it is gated by its own flag (ORDER_CANCELLATION_ENABLED), independent of
  the send flags: being allowed to buy says nothing about being allowed to
  cancel;
- `confirm_cancelled` in the reconciliation flow remains what it always was —
  a record that a human cancelled in IOL's own panel. It is NOT this, and
  this does not replace it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets as _secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.models import OrderExecution
from app.services.logs import app_log

logger = logging.getLogger(__name__)

# Order states from which a cancellation request is meaningful: the order
# reached (or may have reached) the broker and may still be live.
CANCELLABLE_STATUSES = {"execution_sent", "manual_reconciliation_required", "submitting"}

# Terminal cancellation outcomes.
STATUS_CANCELLED = "cancelled_at_broker"
STATUS_CANCELLATION_REJECTED = "cancellation_rejected"
STATUS_CANCELLATION_UNKNOWN = "cancellation_unknown"


def cancellation_phrase(execution_id: int) -> str:
    return f"CANCELAR EJECUCION {execution_id}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _err(message: str, code: str, status_code: int) -> dict:
    return {"error": message, "code": code, "status_code": status_code}


def _canonical_cancellation_payload(
    order_exec: OrderExecution, broker_status: dict, generated_at: str, expires_at: str
) -> dict:
    """Deterministic payload for the cancellation preview signature."""
    return {
        "execution_id": order_exec.id,
        "recommendation_id": order_exec.recommendation_id,
        "symbol": order_exec.symbol,
        "side": order_exec.side,
        "quantity_sent": order_exec.quantity_sent,
        "broker_order_id": order_exec.broker_order_id or "",
        "status": order_exec.status,
        "generated_at": generated_at,
        "expires_at": expires_at,
        # The FRESH broker state is signed too: approving a cancellation
        # against a stale view of the order is exactly what we must prevent.
        "broker_status": broker_status.get("status"),
    }


def _sign(payload: dict, secret: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def build_cancellation_preview(
    db: Session,
    execution_id: int,
    *,
    execution_key: str | None,
    generated_at: datetime | None = None,
) -> dict:
    """READ-ONLY cancellation preview, signed.

    Reads the CURRENT state of the operation from IOL (a read-only call) so
    the human decides against reality, not against our last stored guess. It
    never sends a DELETE.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Cancelación bloqueada: credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.", "invalid_execution_key", 403)
    if not settings.execution_preview_secret:
        return _err("Cancelación bloqueada: firma de preview no configurada.",
                    "preview_signing_not_configured", 423)

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    blocking: list[str] = []
    if not bool(getattr(settings, "order_cancellation_enabled", False)):
        blocking.append("cancellation_disabled")
    if order_exec.status not in CANCELLABLE_STATUSES:
        blocking.append("not_cancellable_status")
    if not (order_exec.broker_order_id or "").strip():
        blocking.append("broker_order_id_missing")

    # Fresh broker state, read-only. A failure here is a blocking reason, not
    # an exception: cancelling blind is worse than not cancelling.
    broker_status: dict = {}
    if not blocking:
        from app.services.execution import _get_execution_broker

        try:
            broker = _get_execution_broker()
            broker_status = broker.get_order_status(order_exec.broker_order_id) or {}
        except Exception as exc:
            logger.warning("cancellation preview status read failed: %s", exc)
            blocking.append("broker_status_unavailable")
        else:
            if broker_status.get("status") in (None, "", "error"):
                blocking.append("broker_status_unavailable")

    gen_dt = generated_at or _utcnow()
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    expires_dt = gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds)
    generated_at_iso, expires_at_iso = gen_dt.isoformat(), expires_dt.isoformat()

    preview_hash = ""
    if not blocking:
        preview_hash = _sign(
            _canonical_cancellation_payload(
                order_exec, broker_status, generated_at_iso, expires_at_iso
            ),
            settings.execution_preview_secret,
        )

    return {
        "execution_id": execution_id,
        "recommendation_id": order_exec.recommendation_id,
        "symbol": order_exec.symbol,
        "side": order_exec.side,
        "quantity_sent": order_exec.quantity_sent,
        "broker_order_id": order_exec.broker_order_id or "",
        "current_status": order_exec.status,
        "broker_status": broker_status.get("status"),
        "endpoint": f"/api/v2/operaciones/{order_exec.broker_order_id}",
        "method": "DELETE",
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "preview_hash": preview_hash,
        "confirmation_phrase": cancellation_phrase(execution_id),
        "cancellation_enabled": bool(getattr(settings, "order_cancellation_enabled", False)),
        "would_cancel": False,
        "dry_run": True,
        "can_submit_cancellation": not blocking and bool(preview_hash),
        "blocking_reasons": blocking,
        "message": "Preview de cancelación. No se envió ningún DELETE.",
    }


def cancel_execution(
    db: Session,
    execution_id: int,
    *,
    execution_key: str | None,
    preview_hash: str | None,
    preview_generated_at: str | None,
    confirmation_text: str | None,
    note: str = "",
) -> dict:
    """Send EXACTLY ONE DELETE, after full human authorization.

    Ordering matters: every check runs before the request, the order is
    claimed atomically so a concurrent duplicate loses, and the outcome is
    persisted immediately. There is no retry anywhere in this function.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Cancelación bloqueada: credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.", "invalid_execution_key", 403)
    if not bool(getattr(settings, "order_cancellation_enabled", False)):
        return _err(
            "Cancelación deshabilitada (ORDER_CANCELLATION_ENABLED=false). "
            "No se envió ningún DELETE.",
            "cancellation_disabled", 423,
        )
    if not settings.execution_preview_secret:
        return _err("Cancelación bloqueada: firma de preview no configurada.",
                    "preview_signing_not_configured", 423)

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    expected = cancellation_phrase(execution_id)
    if not confirmation_text or confirmation_text.strip() != expected:
        return _err(f"Confirmación incorrecta. Frase requerida exacta: '{expected}'.",
                    "confirmation_mismatch", 422)

    if not preview_hash or not preview_generated_at:
        return _err("Se requiere preview_hash y preview_generated_at.",
                    "preview_required", 409)
    try:
        gen_dt = datetime.fromisoformat(preview_generated_at)
    except (ValueError, TypeError):
        return _err("preview_generated_at inválido.", "preview_invalid", 409)
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    if _utcnow() > gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds):
        return _err("El preview de cancelación venció. Generá uno nuevo.",
                    "preview_expired", 409)

    rebuilt = build_cancellation_preview(
        db, execution_id, execution_key=execution_key, generated_at=gen_dt
    )
    if "error" in rebuilt:
        return rebuilt
    if rebuilt.get("blocking_reasons"):
        code = rebuilt["blocking_reasons"][0]
        return _err(f"Cancelación bloqueada: {code}.", code, 409)
    server_hash = rebuilt.get("preview_hash") or ""
    if not server_hash or not _secrets.compare_digest(server_hash, str(preview_hash)):
        return _err(
            "El preview de cancelación no coincide con el estado actual "
            "(la orden cambió en el broker, o el hash fue alterado).",
            "preview_mismatch", 409,
        )

    # Atomic claim: a concurrent or repeated request loses and gets 409, so
    # exactly one DELETE can ever be issued for this decision.
    previous_status = order_exec.status
    claimed = (
        db.query(OrderExecution)
        .filter(
            OrderExecution.id == execution_id,
            OrderExecution.status == previous_status,
            OrderExecution.status.in_(sorted(CANCELLABLE_STATUSES)),
        )
        .update({"status": "cancelling"}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return _err("La ejecución ya está siendo cancelada o cambió de estado.",
                    "cancellation_conflict", 409)
    db.commit()
    db.refresh(order_exec)

    broker_order_id = order_exec.broker_order_id
    from app.services.execution import _get_execution_broker

    # SINGLE DELETE. No retry, no loop, no fallback request.
    try:
        broker = _get_execution_broker()
        result = broker.cancel_order(broker_order_id)
    except Exception as exc:
        result = {
            "outcome": "cancellation_unknown",
            "endpoint_used": f"/api/v2/operaciones/{broker_order_id}",
            "raw_response": {},
            "error": f"Unexpected cancellation failure: {str(exc)[:200]}",
        }

    now = _utcnow()
    outcome = result.get("outcome")
    if outcome == "cancelled":
        new_status = STATUS_CANCELLED
        message = "Cancelación aceptada por el broker."
    elif outcome == "rejected":
        # Definitive: IOL evaluated the DELETE and refused it. The order is
        # whatever it was — most often already executed.
        new_status = STATUS_CANCELLATION_REJECTED
        message = "El broker rechazó la cancelación. La orden sigue su curso."
    else:
        # The DELETE may or may not have been applied. Re-sending could
        # cancel a DIFFERENT, later order, so it is never retried.
        new_status = STATUS_CANCELLATION_UNKNOWN
        message = (
            "Resultado de cancelación incierto. Requiere conciliación manual. "
            "NO se reintenta automáticamente."
        )

    order_exec.status = new_status
    order_exec.completed_at = now if outcome != "cancellation_unknown" else None
    if outcome != "cancelled":
        order_exec.error_message = (result.get("error") or message)[:500]

    br = dict(order_exec.broker_response or {})
    audit = list(br.get("cancellation_audit") or [])
    audit.append({
        "timestamp": now.isoformat(),
        "action": "cancel_order",
        "method": "DELETE",
        "endpoint": result.get("endpoint_used"),
        "broker_order_id": broker_order_id,
        "previous_status": previous_status,
        "new_status": new_status,
        "outcome": outcome,
        "error": (result.get("error") or "")[:300],
        "note": (note or "").strip()[:300],
        "attempts": 1,
        "source": "manual_user",
    })
    br["cancellation_audit"] = audit
    br["cancellation_result"] = result.get("raw_response", {})
    order_exec.broker_response = br

    app_log(db, "Cancelación de orden enviada al broker", context={
        "execution_id": execution_id,
        "broker_order_id": broker_order_id,
        "outcome": outcome,
        "new_status": new_status,
        "source": "manual_user",
    })
    db.commit()

    return {
        "execution_id": execution_id,
        "broker_order_id": broker_order_id,
        "previous_status": previous_status,
        "new_status": new_status,
        "outcome": outcome,
        "requires_reconciliation": outcome == "cancellation_unknown",
        "delete_requests_sent": 1,
        "message": message,
        "cancellation_audit": audit,
    }
