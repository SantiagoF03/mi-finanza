"""FCI — Fondos Comunes de Inversión, as a SEPARATE execution family.

IOL's official API documents an FCI contract distinct from the securities
one:

    GET  /api/v2/Titulos/FCI                  fund catalog
    GET  /api/v2/Titulos/FCI/{simbolo}        fund detail
    POST /api/v2/operar/suscripcion/fci       subscribe
    POST /api/v2/operar/rescate/fci           redeem

A fund is NOT a security and must never borrow the securities path:

- it is subscribed/redeemed by AMOUNT (or cuotapartes), not by quantity at a
  limit price;
- there is no order book, no bid/ask, no tick, no quantity step;
- the valuation that decides the outcome does not exist at submission time,
  so "submitted" is never "executed";
- confirmation is asynchronous and arrives after the fund's own cutoff.

Consequently FundOperation — never OrderExecution — models it.

CONTRACT-FIELD VERIFICATION
---------------------------
The endpoint PATHS above are documented. The exact request FIELD NAMES are
not reproduced here from memory, and this module deliberately refuses to
invent them: `FCI_REQUEST_CONTRACT_VERIFIED` is False until an operator
records the verified field mapping in FCI_REQUEST_FIELDS. Until then the
submission path fails closed with `fci_request_contract_unverified`, while
everything that does NOT depend on those names — catalog, validation,
preview, HMAC, approval, state machine, at-most-once — is fully implemented
and tested.

That is the honest split: an unverified wire format must never be guessed
into a real subscription, but it also must not stop the rest of the flow
from being built and reviewed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets as _secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.broker.execution_class import CLASS_FCI, FAMILY_FUND
from app.core.config import get_settings
from app.models.models import FundInstrument, FundOperation, FundOperationDecision
from app.services.logs import app_log

logger = logging.getLogger(__name__)

# --- Official endpoints -----------------------------------------------------
FCI_CATALOG_ENDPOINT = "/api/v2/Titulos/FCI"
FCI_DETAIL_ENDPOINT = "/api/v2/Titulos/FCI/{simbolo}"
FCI_SUBSCRIBE_ENDPOINT = "/api/v2/operar/suscripcion/fci"
FCI_REDEEM_ENDPOINT = "/api/v2/operar/rescate/fci"

OPERATION_SUBSCRIBE = "subscribe"
OPERATION_REDEEM = "redeem"

# --- Request field mapping (UNVERIFIED) -------------------------------------
# Populate ONLY from the official documentation, then flip the flag. Guessing
# these produces a malformed real subscription, which is worse than not
# sending one.
FCI_REQUEST_CONTRACT_VERIFIED = False
FCI_REQUEST_FIELDS: dict = {}
FCI_CONTRACT_UNVERIFIED_CODE = "fci_request_contract_unverified"

# `soloValidar` is IOL's documented pre-check switch: the same endpoint,
# asked to validate WITHOUT creating the operation.
SOLO_VALIDAR_FIELD = "soloValidar"

# --- State machine ----------------------------------------------------------
STATE_PREPARED = "prepared"
STATE_VALIDATION_REQUESTED = "validation_requested"
STATE_VALIDATED = "validated"
STATE_APPROVAL_REQUESTED = "approval_requested"
STATE_SUBMITTING = "submitting"
STATE_SUBMITTED = "submitted"
STATE_PENDING_CONFIRMATION = "pending_confirmation"
STATE_CONFIRMED = "confirmed"
STATE_REJECTED = "rejected"
STATE_CANCELLED = "cancelled"
STATE_SUBMISSION_UNKNOWN = "submission_unknown"
STATE_RECONCILIATION_REQUIRED = "reconciliation_required"

FUND_OPERATION_STATES = (
    STATE_PREPARED, STATE_VALIDATION_REQUESTED, STATE_VALIDATED,
    STATE_APPROVAL_REQUESTED, STATE_SUBMITTING, STATE_SUBMITTED,
    STATE_PENDING_CONFIRMATION, STATE_CONFIRMED, STATE_REJECTED,
    STATE_CANCELLED, STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED,
)

# States from which a fund operation must NEVER be re-submitted.
NO_RESUBMIT_STATES = {
    STATE_SUBMITTING, STATE_SUBMITTED, STATE_PENDING_CONFIRMATION,
    STATE_CONFIRMED, STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED,
}


def fund_confirmation_phrase(operation_id: int) -> str:
    return f"EJECUTAR OPERACION FCI {operation_id}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _err(message: str, code: str, status_code: int) -> dict:
    return {"error": message, "code": code, "status_code": status_code}


def get_fci_capability() -> dict:
    """Read-only capability report. Contains no secrets."""
    settings = get_settings()
    return {
        "execution_class": CLASS_FCI,
        "execution_family": FAMILY_FUND,
        "endpoints": {
            "catalog": FCI_CATALOG_ENDPOINT,
            "detail": FCI_DETAIL_ENDPOINT,
            "subscribe": FCI_SUBSCRIBE_ENDPOINT,
            "redeem": FCI_REDEEM_ENDPOINT,
        },
        "contract_documented": True,
        "request_contract_verified": FCI_REQUEST_CONTRACT_VERIFIED,
        "subscription_flag_enabled": bool(getattr(settings, "fci_subscription_enabled", False)),
        "redemption_flag_enabled": bool(getattr(settings, "fci_redemption_enabled", False)),
        # Ready only when the flag AND the verified field mapping are both in
        # place. A flag alone can never make an unverified wire format safe.
        "ready_for_real_subscription": (
            FCI_REQUEST_CONTRACT_VERIFIED
            and bool(getattr(settings, "fci_subscription_enabled", False))
        ),
        "ready_for_real_redemption": (
            FCI_REQUEST_CONTRACT_VERIFIED
            and bool(getattr(settings, "fci_redemption_enabled", False))
        ),
        "analysis_supported": True,
        "recommendation_supported": True,
        "preview_supported": True,
        "uses_order_execution": False,
        "supports_limit_price": False,
        "documentation": "docs/IOL_FCI_CAPABILITY.md",
    }


# ---------------------------------------------------------------------------
# Fund catalog (read-only)
# ---------------------------------------------------------------------------


def get_fund(db: Session, symbol) -> FundInstrument | None:
    key = str(symbol or "").strip().upper()
    if not key:
        return None
    return db.query(FundInstrument).filter(FundInstrument.symbol == key).first()


def refresh_fund_catalog(db: Session, broker, *, settings=None) -> dict:
    """Populate FundInstrument from IOL's official FCI catalog (read-only).

    Cutoff, settlement delay and minimum amount are read PER FUND. There is
    deliberately no global default: assuming 15:00 for every fund would
    silently mis-time operations for funds that close earlier.
    """
    from app.broker.instrument_catalog import PROV_IOL_FCI, _utcnow_naive

    try:
        resp = broker._authorized_get(FCI_CATALOG_ENDPOINT)
        payload = resp.json()
    except Exception as exc:
        return {"error": f"No se pudo leer el catálogo de FCI: {str(exc)[:200]}",
                "code": "fci_catalog_unavailable", "status_code": 502}

    items = payload if isinstance(payload, list) else (payload or {}).get("titulos") or []
    now = _utcnow_naive()
    created = updated = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("simbolo") or "").strip().upper()
        if not symbol:
            skipped += 1
            continue

        from app.broker.clients import _map_currency

        fund = get_fund(db, symbol)
        if fund is None:
            fund = FundInstrument(symbol=symbol)
            db.add(fund)
            created += 1
        else:
            updated += 1

        fund.name = str(item.get("descripcion") or item.get("nombre") or "")[:200]
        fund.manager = str(item.get("administradora") or "")[:200]
        fund.currency = _map_currency(item.get("moneda"))
        fund.raw_detail = {k: v for k, v in item.items() if not isinstance(v, (dict, list))}
        fund.source = "iol_fci_catalog"
        fund.verified_at = now
        fund.active = True
        # Identity is verified by IOL's own catalog. Operational parameters
        # (cutoff, minimum, delay) are only verified if the payload carries
        # them — otherwise they stay NULL and the fund cannot operate.
        provenance = {"symbol": PROV_IOL_FCI, "currency": PROV_IOL_FCI,
                      "manager": PROV_IOL_FCI}
        cutoff = item.get("horarioCorte") or item.get("cutoff")
        if cutoff:
            fund.cutoff_local_time = str(cutoff)[:5]
            provenance["cutoff_local_time"] = PROV_IOL_FCI
        minimum = item.get("montoMinimo") or item.get("inversionMinima")
        if minimum is not None:
            try:
                fund.minimum_amount = float(minimum)
                provenance["minimum_amount"] = PROV_IOL_FCI
            except (TypeError, ValueError):
                pass
        delay = item.get("plazoLiquidacion") or item.get("plazo")
        if isinstance(delay, int):
            fund.settlement_delay_days = delay
            provenance["settlement_delay_days"] = PROV_IOL_FCI
        fund.field_provenance = provenance
        # A fund is only operable once cutoff and minimum are known.
        complete = bool(fund.cutoff_local_time) and fund.minimum_amount is not None
        fund.verification_status = "verified" if complete else "candidate"
        fund.subscription_supported = complete
        fund.redemption_supported = complete

    db.commit()
    return {"created": created, "updated": updated, "skipped": skipped,
            "total": db.query(FundInstrument).count()}


# ---------------------------------------------------------------------------
# Preview (signed, informational, would_execute=False)
# ---------------------------------------------------------------------------


def _canonical_fund_payload(operation: FundOperation, fund: FundInstrument,
                            generated_at: str, expires_at: str) -> dict:
    return {
        "fund_operation_id": operation.id,
        "fund_symbol": operation.fund_symbol,
        "manager": fund.manager,
        "operation": operation.operation,
        "currency": operation.currency,
        "amount": operation.amount,
        "quotaparts": operation.quotaparts,
        "cutoff_local_time": operation.cutoff_local_time,
        "settlement_delay_days": operation.settlement_delay_days,
        "minimum_amount": fund.minimum_amount,
        "validation_result": operation.validation_result or {},
        "generated_at": generated_at,
        "expires_at": expires_at,
    }


def _sign(payload: dict, secret: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def build_fund_operation_preview(
    db: Session,
    operation_id: int,
    *,
    generated_at: datetime | None = None,
) -> dict:
    """Signed preview for a fund operation. READ-ONLY, never submits.

    Deliberately carries NO limit price, NO bid/ask and NO quantity step: a
    fund has none of those, and showing one would misrepresent the operation.
    """
    settings = get_settings()

    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}
    fund = get_fund(db, operation.fund_symbol)
    if not fund:
        return {"error": "Fund not found in catalog", "code": "fund_not_in_catalog",
                "status_code": 409}

    blocking: list[str] = []
    if not FCI_REQUEST_CONTRACT_VERIFIED:
        blocking.append(FCI_CONTRACT_UNVERIFIED_CODE)
    if operation.operation == OPERATION_SUBSCRIBE and not getattr(
        settings, "fci_subscription_enabled", False
    ):
        blocking.append("fci_subscription_disabled")
    if operation.operation == OPERATION_REDEEM and not getattr(
        settings, "fci_redemption_enabled", False
    ):
        blocking.append("fci_redemption_disabled")
    if not settings.execution_admin_key:
        blocking.append("execution_admin_key_not_configured")
    if not settings.execution_preview_secret:
        blocking.append("preview_signing_not_configured")
    if fund.verification_status != "verified":
        blocking.append("fund_not_verified")
    if not fund.active:
        blocking.append("fund_inactive")
    if operation.status in NO_RESUBMIT_STATES:
        blocking.append("fund_operation_already_submitted")
    if operation.operation == OPERATION_SUBSCRIBE:
        if fund.minimum_amount is not None and (operation.amount or 0) < fund.minimum_amount:
            blocking.append("fund_minimum_amount_not_met")
    cutoff_state = evaluate_cutoff(fund, settings=settings)
    if cutoff_state["code"]:
        blocking.append(cutoff_state["code"])

    gen_dt = generated_at or _utcnow()
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    expires_dt = gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds)
    generated_at_iso, expires_at_iso = gen_dt.isoformat(), expires_dt.isoformat()

    preview_hash = ""
    if settings.execution_preview_secret:
        preview_hash = _sign(
            _canonical_fund_payload(operation, fund, generated_at_iso, expires_at_iso),
            settings.execution_preview_secret,
        )

    return {
        "fund_operation_id": operation.id,
        "execution_family": FAMILY_FUND,
        "execution_class": CLASS_FCI,
        "fund_symbol": operation.fund_symbol,
        "fund_name": fund.name,
        "manager": fund.manager,
        "operation": operation.operation,
        "currency": operation.currency,
        "amount": operation.amount,
        "quotaparts": operation.quotaparts,
        "minimum_amount": fund.minimum_amount,
        "cutoff_local_time": operation.cutoff_local_time,
        "cutoff_state": cutoff_state,
        "settlement_delay_days": operation.settlement_delay_days,
        "validation_result": operation.validation_result or {},
        "status": operation.status,
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "preview_hash": preview_hash,
        "confirmation_phrase": fund_confirmation_phrase(operation.id),
        # A fund has none of these, by construction.
        "limit_price": None,
        "best_bid": None,
        "best_ask": None,
        "quantity_step": None,
        "would_execute": False,
        "dry_run": True,
        "immediate": False,
        "can_submit": not blocking and bool(preview_hash),
        "blocking_reasons": blocking,
        "endpoint": (
            FCI_SUBSCRIBE_ENDPOINT if operation.operation == OPERATION_SUBSCRIBE
            else FCI_REDEEM_ENDPOINT
        ),
        "message": "Preview de operación FCI. No se envió ninguna operación.",
    }


def evaluate_cutoff(fund: FundInstrument, *, settings, now: datetime | None = None) -> dict:
    """Is the fund's OWN cutoff still open?

    Per fund, never a shared constant: two funds legitimately close at
    different times, and applying one fund's cutoff to another mis-times the
    operation by a whole settlement day.
    """
    from app.market.calendar import parse_hhmm, resolve_market_schedule

    if not fund.cutoff_local_time:
        return {"code": "fund_cutoff_unknown", "cutoff": None, "open": False}
    cutoff = parse_hhmm(fund.cutoff_local_time)
    if cutoff is None:
        return {"code": "fund_cutoff_unknown", "cutoff": fund.cutoff_local_time, "open": False}

    schedule = resolve_market_schedule(settings)
    tz = schedule.get("tzinfo")
    if tz is None:
        return {"code": "market_schedule_unknown", "cutoff": fund.cutoff_local_time,
                "open": False}
    current = (now.astimezone(tz) if now is not None else datetime.now(tz))
    if current.weekday() >= 5 or current.date() in schedule.get("holidays", set()):
        return {"code": "fund_cutoff_non_business_day",
                "cutoff": fund.cutoff_local_time, "open": False,
                "local_time": current.isoformat()}
    if current.time() >= cutoff:
        return {"code": "fund_cutoff_passed", "cutoff": fund.cutoff_local_time,
                "open": False, "local_time": current.isoformat()}
    return {"code": None, "cutoff": fund.cutoff_local_time, "open": True,
            "local_time": current.isoformat()}


# ---------------------------------------------------------------------------
# Request building (fails closed while the field mapping is unverified)
# ---------------------------------------------------------------------------


def build_fund_request(operation: FundOperation, *, solo_validar: bool) -> tuple[dict | None, str | None]:
    """Build the official FCI request body.

    Returns (request, error_code). While FCI_REQUEST_CONTRACT_VERIFIED is
    False this ALWAYS fails: the endpoint paths are documented, the field
    names are not recorded here, and inventing them would produce a malformed
    real subscription.
    """
    if not FCI_REQUEST_CONTRACT_VERIFIED or not FCI_REQUEST_FIELDS:
        return None, FCI_CONTRACT_UNVERIFIED_CODE
    if operation.operation not in (OPERATION_SUBSCRIBE, OPERATION_REDEEM):
        return None, "invalid_fund_operation"

    endpoint = (
        FCI_SUBSCRIBE_ENDPOINT if operation.operation == OPERATION_SUBSCRIBE
        else FCI_REDEEM_ENDPOINT
    )
    mapping = FCI_REQUEST_FIELDS.get(operation.operation) or {}
    if not mapping:
        return None, FCI_CONTRACT_UNVERIFIED_CODE

    body = {}
    for local_name, wire_name in mapping.items():
        value = getattr(operation, local_name, None)
        if value is not None:
            body[wire_name] = value
    body[SOLO_VALIDAR_FIELD] = bool(solo_validar)
    return {"endpoint": endpoint, "body": body, "solo_validar": bool(solo_validar)}, None


def validate_fund_operation(db: Session, operation_id: int, broker) -> dict:
    """Pre-check via the official `soloValidar=true` mechanism.

    A validation is NEVER an execution: it must not set broker_operation_id,
    must not move the operation into a submitted state, and must not consume
    any at-most-once budget.
    """
    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}
    if operation.status in NO_RESUBMIT_STATES:
        return _err("La operación ya fue enviada; no se revalida.",
                    "fund_operation_already_submitted", 409)

    request, error = build_fund_request(operation, solo_validar=True)
    if error:
        operation.blocked_reason = error
        db.commit()
        return _err(
            "El mapeo de campos del contrato FCI no está verificado: no se envía "
            "ninguna validación con nombres inventados.",
            error, 501,
        )

    operation.status = STATE_VALIDATION_REQUESTED
    db.commit()

    try:
        result = broker.submit_fund_request(request)
    except Exception as exc:
        operation.status = STATE_PREPARED
        operation.error_message = f"Validación falló: {str(exc)[:200]}"
        db.commit()
        return _err(operation.error_message, "fund_validation_failed", 502)

    operation.validation_result = result.get("raw_response", {}) or {}
    operation.status = (
        STATE_VALIDATED if result.get("outcome") == "validated" else STATE_PREPARED
    )
    # A soloValidar call NEVER produces an operation id.
    operation.broker_operation_id = ""
    db.commit()

    return {
        "fund_operation_id": operation.id,
        "status": operation.status,
        "solo_validar": True,
        "is_execution": False,
        "validation_result": operation.validation_result,
    }


def submit_fund_operation(
    db: Session,
    operation_id: int,
    *,
    execution_key: str | None,
    preview_hash: str | None,
    preview_generated_at: str | None,
    confirmation_text: str | None,
    note: str = "",
) -> dict:
    """Submit ONE subscription or redemption, at most once.

    Every gate must pass before the single POST: capability flag, verified
    field mapping, admin credential, signed non-expired preview, exact
    phrase, verified fund, live balance or holding, open cutoff. An ambiguous
    outcome becomes `submission_unknown` and is NEVER re-sent — a fund
    operation is not idempotent and a duplicate subscription is real money.
    """
    settings = get_settings()

    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}

    if not FCI_REQUEST_CONTRACT_VERIFIED:
        return _err(
            "El contrato de campos de FCI no está verificado. Los endpoints son "
            "oficiales, pero los nombres de campo no se inventan.",
            FCI_CONTRACT_UNVERIFIED_CODE, 501,
        )
    if operation.status in NO_RESUBMIT_STATES:
        return _err("La operación ya fue enviada. No se reenvía automáticamente.",
                    "fund_operation_already_submitted", 409)

    enabled = (
        settings.fci_subscription_enabled if operation.operation == OPERATION_SUBSCRIBE
        else settings.fci_redemption_enabled
    )
    if not enabled:
        return _err("Operación de FCI deshabilitada por configuración.",
                    f"fci_{operation.operation}_disabled", 423)
    if not settings.execution_admin_key:
        return _err("Credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.", "invalid_execution_key", 403)

    expected = fund_confirmation_phrase(operation.id)
    if not confirmation_text or confirmation_text.strip() != expected:
        return _err(f"Confirmación incorrecta. Frase requerida exacta: '{expected}'.",
                    "confirmation_mismatch", 422)

    if not preview_hash or not preview_generated_at:
        return _err("Se requiere preview_hash y preview_generated_at.", "preview_required", 409)
    try:
        gen_dt = datetime.fromisoformat(preview_generated_at)
    except (ValueError, TypeError):
        return _err("preview_generated_at inválido.", "preview_invalid", 409)
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    if _utcnow() > gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds):
        return _err("El preview venció.", "preview_expired", 409)

    rebuilt = build_fund_operation_preview(db, operation.id, generated_at=gen_dt)
    if "error" in rebuilt:
        return rebuilt
    if rebuilt.get("blocking_reasons"):
        code = rebuilt["blocking_reasons"][0]
        return _err(f"Operación bloqueada: {code}.", code, 409)
    if not _secrets.compare_digest(rebuilt.get("preview_hash") or "", str(preview_hash)):
        return _err("El preview no coincide con el estado actual.", "preview_mismatch", 409)

    request, error = build_fund_request(operation, solo_validar=False)
    if error:
        return _err("No se pudo construir la request de FCI.", error, 501)

    # Atomic claim → exactly one submission.
    claimed = (
        db.query(FundOperation)
        .filter(FundOperation.id == operation.id,
                FundOperation.status.notin_(sorted(NO_RESUBMIT_STATES)))
        .update({"status": STATE_SUBMITTING}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return _err("La operación ya fue reclamada por otra solicitud.",
                    "fund_operation_already_submitted", 409)
    db.add(FundOperationDecision(
        fund_operation_id=operation.id, decision="approved",
        note=note, preview_hash=str(preview_hash),
    ))
    db.commit()
    db.refresh(operation)

    from app.services.execution import _get_execution_broker

    try:
        broker = _get_execution_broker()
        result = broker.submit_fund_request(request)
    except Exception as exc:
        result = {"outcome": "submission_unknown", "operation_id": "",
                  "raw_response": {}, "error": f"Unexpected failure: {str(exc)[:200]}"}

    outcome = result.get("outcome")
    operation.broker_response = result.get("raw_response", {}) or {}
    if outcome == "submitted":
        operation.broker_operation_id = str(result.get("operation_id") or "")
        operation.submitted_at = _utcnow()
        # NOT "confirmed": a fund confirms later, at a valuation that did not
        # exist when we submitted.
        operation.status = STATE_PENDING_CONFIRMATION
    elif outcome == "rejected":
        operation.status = STATE_REJECTED
        operation.error_message = (result.get("error") or "Rejected")[:500]
    else:
        operation.status = STATE_SUBMISSION_UNKNOWN
        operation.error_message = (
            f"Resultado incierto: {result.get('error', 'unknown')[:300]}. "
            "Requiere conciliación manual. NO se reintenta."
        )
    db.commit()

    app_log(db, "Operación FCI enviada", context={
        "fund_operation_id": operation.id,
        "operation": operation.operation,
        "fund_symbol": operation.fund_symbol,
        "status": operation.status,
    })

    return {
        "fund_operation_id": operation.id,
        "operation": operation.operation,
        "status": operation.status,
        "broker_operation_id": operation.broker_operation_id,
        "requires_reconciliation": operation.status == STATE_SUBMISSION_UNKNOWN,
        "immediate": False,
        "submissions_sent": 1,
        "message": (
            "Operación enviada. La confirmación del fondo es posterior al cutoff."
            if operation.status == STATE_PENDING_CONFIRMATION
            else operation.error_message
        ),
    }
