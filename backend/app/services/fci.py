"""FCI — Fondos Comunes de Inversión, as a SEPARATE execution family.

IOL's official API documents an FCI contract distinct from the securities
one:

    GET  /api/v2/Titulos/FCI                  fund catalog
    GET  /api/v2/Titulos/FCI/{simbolo}        fund detail
    POST /api/v2/operar/suscripcion/fci       subscribe
    POST /api/v2/operar/rescate/fci           redeem

A fund is NOT a security and must never borrow the securities path:

- it is subscribed AND redeemed by AMOUNT (`Monto`), not by quantity at a
  limit price;
- there is no order book, no bid/ask, no tick, no quantity step;
- the valuation that decides the outcome does not exist at submission time,
  so "submitted" is never "executed";
- confirmation is asynchronous and arrives after the fund's own cutoff.

Consequently FundOperation — never OrderExecution — models it.

REQUEST CONTRACT
----------------
Both operar endpoints take exactly three form fields, sent as
application/x-www-form-urlencoded:

    Simbolo      the fund symbol
    Monto        the amount, deterministically serialized
    soloValidar  "true" to validate WITHOUT creating the operation

`quotaparts` is never sent: the documented contract expresses a redemption by
Monto too, and there is no official field for cuotapartes.

AUTHORISATION, in order
-----------------------
1. ORDER_EXECUTION_ENABLED — the global lock, above everything;
2. FCI_SUBSCRIPTION_ENABLED / FCI_REDEMPTION_ENABLED — per capability;
3. a FRESH `soloValidar` result whose hash matches this exact request;
4. a signed, unexpired preview;
5. the exact confirmation phrase + the execution credential;
6. a LIVE preflight (balance or holding, cutoff, minimum, daily cap);
7. exactly one POST, never retried.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets as _secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from app.broker.execution_class import CLASS_FCI, FAMILY_FUND
from app.broker.numeric import positive_decimal
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

# --- Request field mapping (VERIFIED against the official "Mi Cuenta" docs) --
#
# Both endpoints take exactly three form fields:
#     Simbolo, Monto, soloValidar
#
# Note that RESCATE is also expressed by Monto — NOT by cantidad/cuotapartes.
# The model can carry `quotaparts` for display, but it is never sent through
# this contract, because no official request field exists for it.
FCI_REQUEST_CONTRACT_VERIFIED = True
FCI_CONTENT_TYPE = "application/x-www-form-urlencoded"

FIELD_SIMBOLO = "Simbolo"
FIELD_MONTO = "Monto"
SOLO_VALIDAR_FIELD = "soloValidar"

# local attribute -> exact wire field name. Immutable.
FCI_REQUEST_FIELDS: dict = {
    OPERATION_SUBSCRIBE: {"fund_symbol": FIELD_SIMBOLO, "amount": FIELD_MONTO},
    OPERATION_REDEEM: {"fund_symbol": FIELD_SIMBOLO, "amount": FIELD_MONTO},
}

FCI_CONTRACT_UNVERIFIED_CODE = "fci_request_contract_unverified"

# --- Fund catalog field names ------------------------------------------------
# These are OBSERVED names, not documented ones. Reading them makes a value
# visible; it does not make it trustworthy enough to time or size real money.
CUTOFF_FIELD_CANDIDATES = ("horarioCorte", "cutoff")
MINIMUM_FIELD_CANDIDATES = ("montoMinimo", "inversionMinima")
SETTLEMENT_DELAY_FIELD_CANDIDATES = ("plazoLiquidacion", "plazo")

# Provenance for a value we read from an observed (undocumented) field. It is
# deliberately NOT a verifying provenance.
PROV_IOL_FCI_OBSERVED = "iol_fci_catalog_observed"

# Provenances that make a fund's OPERATIONAL parameters trustworthy.
# The observed-field provenance is absent on purpose.
FUND_VERIFYING_PROVENANCES = frozenset({
    "iol_fci_catalog", "admin_verified_override",
})


def _first_observed(payload: dict, names: tuple[str, ...]):
    """First present, non-empty value among candidate field names."""
    for name in names:
        if name in payload:
            value = payload.get(name)
            if value not in (None, ""):
                return value
    return None

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


def validation_payload_hash(operation: FundOperation) -> str:
    """Hash of exactly WHAT a validation was about.

    A `soloValidar` result is evidence about one specific request. Change the
    amount, the fund, the side or the currency and that evidence no longer
    describes what we are about to send — so the hash must change too.
    """
    canonical = json.dumps(
        {
            "fund_symbol": str(operation.fund_symbol or "").strip().upper(),
            "operation": str(operation.operation or ""),
            "currency": str(operation.currency or "").strip().upper(),
            # The serialized amount, so 145.5 and 145.50 hash identically —
            # they produce the same request.
            "amount": serialize_monto(operation.amount) or "",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validation_state(operation: FundOperation, *, settings, now: datetime | None = None) -> dict:
    """Is this operation backed by a FRESH, MATCHING validation?

    Returns {"valid": bool, "code": str|None, ...}. Fail-closed: anything we
    cannot positively confirm is not a valid validation.
    """
    current = now or _utcnow()
    ttl = int(getattr(settings, "fci_validation_ttl_seconds", 0) or 0)
    detail = {
        "status": operation.status,
        "validated_at": operation.validated_at.isoformat() if operation.validated_at else None,
        "ttl_seconds": ttl,
    }

    if ttl <= 0:
        return {**detail, "valid": False, "code": "fci_validation_ttl_not_configured"}
    if operation.status != STATE_VALIDATED:
        return {**detail, "valid": False, "code": "fci_validation_required"}
    if operation.validated_at is None:
        return {**detail, "valid": False, "code": "fci_validation_required"}

    validated_at = operation.validated_at
    if validated_at.tzinfo is None:
        validated_at = validated_at.replace(tzinfo=timezone.utc)
    age = (current - validated_at).total_seconds()
    detail["age_seconds"] = age
    if age > ttl:
        return {**detail, "valid": False, "code": "fci_validation_expired"}

    expected = validation_payload_hash(operation)
    if not operation.validated_payload_hash:
        return {**detail, "valid": False, "code": "fci_validation_required"}
    if not _secrets.compare_digest(operation.validated_payload_hash, expected):
        # The amount or the fund changed after validating.
        return {**detail, "valid": False, "code": "fci_validation_stale_payload"}

    return {**detail, "valid": True, "code": None}


def fci_execution_locked(settings) -> bool:
    """The GLOBAL lock applies to funds exactly as it does to securities.

    A per-capability flag is a second key, never a replacement for the master
    one: with ORDER_EXECUTION_ENABLED=false nothing reaches IOL, funds
    included.
    """
    return not bool(getattr(settings, "order_execution_enabled", False))


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
        "request_fields": [FIELD_SIMBOLO, FIELD_MONTO, SOLO_VALIDAR_FIELD],
        "content_type": FCI_CONTENT_TYPE,
        "subscription_flag_enabled": bool(getattr(settings, "fci_subscription_enabled", False)),
        "redemption_flag_enabled": bool(getattr(settings, "fci_redemption_enabled", False)),
        # The GLOBAL lock is reported here too: a per-capability flag is a
        # second key, never a replacement for the master one.
        "execution_locked": fci_execution_locked(settings),
        "validation_ttl_seconds": getattr(settings, "fci_validation_ttl_seconds", 0),
        "limits_configured": bool(
            getattr(settings, "fci_max_operation_amount", 0) > 0
            and getattr(settings, "fci_max_daily_amount", 0) > 0
        ),
        # Ready only when the global lock is OPEN, the capability flag is on,
        # the field mapping is verified and the limits are configured.
        "ready_for_real_subscription": (
            FCI_REQUEST_CONTRACT_VERIFIED
            and not fci_execution_locked(settings)
            and bool(getattr(settings, "fci_subscription_enabled", False))
            and getattr(settings, "fci_max_operation_amount", 0) > 0
            and getattr(settings, "fci_max_daily_amount", 0) > 0
        ),
        "ready_for_real_redemption": (
            FCI_REQUEST_CONTRACT_VERIFIED
            and not fci_execution_locked(settings)
            and bool(getattr(settings, "fci_redemption_enabled", False))
            and getattr(settings, "fci_max_operation_amount", 0) > 0
            and getattr(settings, "fci_max_daily_amount", 0) > 0
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
    from app.broker.instrument_catalog import (
        PROV_ADMIN_OVERRIDE,
        PROV_IOL_FCI,
        _utcnow_naive,
    )

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
        # Identity comes from IOL's own FCI catalog — that is real evidence.
        provenance = {"symbol": PROV_IOL_FCI, "currency": PROV_IOL_FCI,
                      "manager": PROV_IOL_FCI}

        # OPERATIONAL parameters are a different matter. The listing may or
        # may not carry them, and the field names below are OBSERVED, not
        # documented. So we distinguish three things:
        #   present    — a key with that name exists in the payload
        #   recognized — we know how to read it
        #   verified   — a trustworthy source asserts it
        # A merely-observed field makes the value visible; it does NOT make
        # the fund operable. Only PROV_ADMIN_OVERRIDE (a human confirming
        # against the documentation) promotes these to verified.
        observed: dict = {}
        cutoff = _first_observed(item, CUTOFF_FIELD_CANDIDATES)
        if cutoff is not None:
            fund.cutoff_local_time = str(cutoff)[:5]
            observed["cutoff_local_time"] = cutoff
        minimum = _first_observed(item, MINIMUM_FIELD_CANDIDATES)
        if minimum is not None:
            try:
                fund.minimum_amount = float(minimum)
                observed["minimum_amount"] = minimum
            except (TypeError, ValueError):
                pass
        delay = _first_observed(item, SETTLEMENT_DELAY_FIELD_CANDIDATES)
        if isinstance(delay, int):
            fund.settlement_delay_days = delay
            observed["settlement_delay_days"] = delay

        # Preserve any field a human already verified: an automatic refresh
        # must not silently demote an administrative confirmation.
        previous = dict(fund.field_provenance or {})
        for field in ("cutoff_local_time", "minimum_amount", "settlement_delay_days"):
            if previous.get(field) == PROV_ADMIN_OVERRIDE:
                provenance[field] = PROV_ADMIN_OVERRIDE
            elif field in observed:
                provenance[field] = PROV_IOL_FCI_OBSERVED
        fund.field_provenance = provenance
        fund.raw_detail = {**(fund.raw_detail or {}), "_observed_operational": observed}

        # A fund is operable only when cutoff AND minimum are VERIFIED — not
        # merely present. Having read a plausible-looking `horarioCorte` from
        # an undocumented field is not evidence enough to time real money.
        verified_fields = {
            field for field, source in provenance.items()
            if source in FUND_VERIFYING_PROVENANCES
        }
        operable = {"cutoff_local_time", "minimum_amount"} <= verified_fields
        fund.verification_status = "verified" if operable else "candidate"
        # Capabilities are NOT inferred from cutoff+minimum being present.
        # They stay off until a per-operation validation or an administrative
        # confirmation says otherwise.
        fund.subscription_supported = operable and bool(fund.subscription_supported)
        fund.redemption_supported = operable and bool(fund.redemption_supported)

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
    # The GLOBAL lock first: a capability flag never overrides it.
    if fci_execution_locked(settings):
        blocking.append("execution_locked")
    if operation.operation == OPERATION_SUBSCRIBE and not getattr(
        settings, "fci_subscription_enabled", False
    ):
        blocking.append("fci_subscription_disabled")
    if operation.operation == OPERATION_REDEEM and not getattr(
        settings, "fci_redemption_enabled", False
    ):
        blocking.append("fci_redemption_disabled")
    # A validation must exist, be fresh, and describe THIS request.
    validation = validation_state(operation, settings=settings)
    if not validation["valid"]:
        blocking.append(validation["code"])
    # Per-operation cap, per currency. 0 = not configured = blocked.
    amount = positive_decimal(operation.amount)
    max_operation = positive_decimal(getattr(settings, "fci_max_operation_amount", 0))
    if amount is None:
        blocking.append("invalid_fund_amount")
    elif max_operation is None:
        blocking.append("fci_limits_not_configured")
    elif amount > max_operation:
        blocking.append("fci_operation_limit_exceeded")
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
        "validation_state": validation,
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


def serialize_monto(value) -> str | None:
    """Deterministic decimal serialization for `Monto`.

    Never `str(float)`: binary floats render as 145.54000000000002 or 1e+05
    depending on the value, and an amount is money. Decimal + positional
    formatting gives the same string for the same amount, every time — which
    also means the signed preview hash stays stable.
    """
    dec = positive_decimal(value)
    if dec is None:
        return None
    normalized = dec.normalize()
    # normalize() may yield exponent notation for round numbers (1E+5).
    if normalized == normalized.to_integral_value():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


def build_fund_request(operation: FundOperation, *, solo_validar: bool) -> tuple[dict | None, str | None]:
    """Build the official FCI request, form-urlencoded.

    Contract (both endpoints): `Simbolo`, `Monto`, `soloValidar`.

    `quotaparts` is deliberately never sent: the documented "Mi Cuenta"
    contract expresses a redemption by Monto too, and there is no official
    field for cuotapartes. Sending an undocumented field is how a real
    redemption gets silently misinterpreted.
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

    symbol = str(operation.fund_symbol or "").strip().upper()
    if not symbol:
        return None, "invalid_fund_symbol"

    monto = serialize_monto(operation.amount)
    if monto is None:
        # The contract has no way to express "redeem all my cuotapartes";
        # without a positive Monto there is no request to build.
        return None, "invalid_fund_amount"

    form_data = {
        mapping["fund_symbol"]: symbol,
        mapping["amount"]: monto,
        # Lowercase JSON-style booleans, as form values.
        SOLO_VALIDAR_FIELD: "true" if solo_validar else "false",
    }

    return {
        "endpoint": endpoint,
        "content_type": FCI_CONTENT_TYPE,
        "form_data": form_data,
        "solo_validar": bool(solo_validar),
    }, None


def validate_fund_operation(db: Session, operation_id: int, broker) -> dict:
    """Pre-check via the official `soloValidar=true` mechanism.

    A validation is NEVER an execution: it must not set broker_operation_id,
    must not move the operation into a submitted state, and must not consume
    any at-most-once budget.
    """
    settings = get_settings()

    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}
    if operation.status in NO_RESUBMIT_STATES:
        return _err("La operación ya fue enviada; no se revalida.",
                    "fund_operation_already_submitted", 409)

    # The GLOBAL lock gates validation too. `soloValidar` does not create an
    # operation, but it is still an authenticated call to the broker made in
    # preparation for executing — with execution off, we do not make it.
    if fci_execution_locked(settings):
        return _err(
            "Validación bloqueada: ejecución deshabilitada "
            "(ORDER_EXECUTION_ENABLED=false).",
            "execution_locked", 423,
        )

    request, error = build_fund_request(operation, solo_validar=True)
    if error:
        operation.blocked_reason = error
        db.commit()
        return _err(f"No se pudo construir la request de validación: {error}.",
                    error, 422)

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
    validated = result.get("outcome") == "validated"
    operation.status = STATE_VALIDATED if validated else STATE_PREPARED
    if validated:
        operation.validated_at = _utcnow().replace(tzinfo=None)
        # Bind the validation to EXACTLY what was validated.
        operation.validated_payload_hash = validation_payload_hash(operation)
    else:
        operation.validated_at = None
        operation.validated_payload_hash = ""
        operation.error_message = (result.get("error") or "")[:500]
    # A soloValidar call NEVER produces an operation id, whatever came back.
    operation.broker_operation_id = ""
    db.commit()

    return {
        "fund_operation_id": operation.id,
        "status": operation.status,
        "solo_validar": True,
        "is_execution": False,
        "validated": validated,
        "validated_at": operation.validated_at.isoformat() if operation.validated_at else None,
        "validation_ttl_seconds": settings.fci_validation_ttl_seconds,
        "validation_result": operation.validation_result,
        "http_requests_sent": result.get("http_requests_sent", 1),
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
            "El contrato de campos de FCI no está verificado.",
            FCI_CONTRACT_UNVERIFIED_CODE, 501,
        )
    if operation.status in NO_RESUBMIT_STATES:
        # Covers submission_unknown: a human repeating the request against an
        # ambiguous outcome must NOT produce a second subscription.
        return _err("La operación ya fue enviada. No se reenvía automáticamente.",
                    "fund_operation_already_submitted", 409)

    # GLOBAL lock before anything else.
    if fci_execution_locked(settings):
        return _err(
            "Ejecución bloqueada: ORDER_EXECUTION_ENABLED=false. "
            "No se envió ninguna operación de FCI.",
            "execution_locked", 423,
        )

    enabled = (
        settings.fci_subscription_enabled if operation.operation == OPERATION_SUBSCRIBE
        else settings.fci_redemption_enabled
    )
    if not enabled:
        code = ("fci_subscription_disabled"
                if operation.operation == OPERATION_SUBSCRIBE
                else "fci_redemption_disabled")
        return _err("Operación de FCI deshabilitada por configuración.", code, 423)

    # A FRESH validation describing THIS request is mandatory.
    validation = validation_state(operation, settings=settings)
    if not validation["valid"]:
        return _err(
            f"La operación no tiene una validación vigente: {validation['code']}.",
            validation["code"], 409,
        )
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
        return _err("No se pudo construir la request de FCI.", error, 422)

    # --- LIVE preflight, immediately before the POST ---
    from app.services.execution import _get_execution_broker

    try:
        broker = _get_execution_broker()
    except Exception as exc:
        return _err(f"Broker no disponible: {str(exc)[:200]}", "broker_unavailable", 502)

    preflight_error, preflight_audit = fund_live_preflight(
        db, operation, broker=broker, settings=settings
    )
    if preflight_error:
        operation.blocked_reason = preflight_error
        db.commit()
        return _err(f"Preflight de FCI bloqueado: {preflight_error}.",
                    preflight_error, 409)

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

    try:
        result = broker.submit_fund_request(request)
    except Exception as exc:
        result = {"outcome": "submission_unknown", "operation_id": "",
                  "raw_response": {}, "http_requests_sent": 1,
                  "error": f"Unexpected failure: {str(exc)[:200]}"}

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


# ---------------------------------------------------------------------------
# Live preflight — read the world immediately before the POST
# ---------------------------------------------------------------------------


def pending_fund_amount(db: Session, *, currency: str, exclude_id: int | None = None) -> Decimal:
    """Amount tied up by fund operations we cannot yet call resolved.

    A submitted-but-unconfirmed subscription has already committed money even
    though the balance may not show it, and an ambiguous one may have too.
    Counted per CURRENCY — pesos and dollars are separate.
    """
    rows = (
        db.query(FundOperation)
        .filter(FundOperation.status.in_(
            [STATE_SUBMITTING, STATE_SUBMITTED, STATE_PENDING_CONFIRMATION,
             STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED]
        ))
        .all()
    )
    total = Decimal("0")
    target = (currency or "").strip().upper()
    for row in rows:
        if exclude_id is not None and row.id == exclude_id:
            continue
        if str(row.currency or "").strip().upper() != target:
            continue
        amount = positive_decimal(row.amount)
        if amount is not None:
            total += amount
    return total


def fund_live_preflight(
    db: Session, operation: FundOperation, *, broker, settings, now: datetime | None = None
) -> tuple[str | None, dict]:
    """Everything that must be true at the LAST possible moment.

    Deliberately re-reads the world instead of trusting the preview: the
    preview is what a human approved, not a reservation of cash or holdings.

    Subscription: live balance in the fund's OWN currency, minus pending fund
    operations, minus the reserve, plus the cost buffer, above the minimum.
    Redemption: live portfolio position in that exact fund, enough to cover
    the amount, minus already-pending redemptions.
    Both: the fund's own cutoff must still be open, and the daily cap must fit.
    """
    audit: dict = {"operation": operation.operation, "currency": operation.currency}
    fund = get_fund(db, operation.fund_symbol)
    if fund is None:
        return "fund_not_in_catalog", audit

    currency = str(operation.currency or "").strip().upper()
    amount = positive_decimal(operation.amount)
    if not currency or amount is None:
        return "invalid_fund_amount", audit
    audit["amount"] = float(amount)

    # --- Cutoff, re-checked now (it may have passed since the preview) ---
    cutoff = evaluate_cutoff(fund, settings=settings, now=now)
    audit["cutoff"] = cutoff
    if cutoff["code"]:
        return cutoff["code"], audit

    # --- Minimum, re-checked now ---
    minimum = positive_decimal(fund.minimum_amount)
    if operation.operation == OPERATION_SUBSCRIBE:
        if minimum is None:
            return "fund_minimum_unknown", audit
        if amount < minimum:
            return "fund_minimum_amount_not_met", audit

    # --- Daily cap, per currency ---
    from app.services.execution_limits import reserve_daily_budget, trade_date_for

    trade_date = trade_date_for(settings)
    max_daily = getattr(settings, "fci_max_daily_amount", 0)
    reserve_error, reserve_audit = reserve_daily_budget(
        db,
        trade_date=trade_date,
        execution_class=f"{CLASS_FCI}:{operation.operation}",
        currency=currency,
        notional=amount,
        max_daily_notional=max_daily,
    )
    audit["daily_budget"] = reserve_audit
    if reserve_error:
        return reserve_error, audit

    pending = pending_fund_amount(db, currency=currency, exclude_id=operation.id)
    audit["pending_fund_amount"] = float(pending)

    if operation.operation == OPERATION_SUBSCRIBE:
        try:
            live_cash = broker.get_live_cash(currency)
        except Exception:
            return "live_cash_unavailable", audit
        from app.services.execution_limits import evaluate_buy_cash

        cash_error, cash_audit = evaluate_buy_cash(
            live_cash=live_cash,
            required_notional=amount,
            currency=currency,
            fee_buffer_pct=getattr(settings, "fci_fee_buffer_pct", 0.0) or 0.0,
            min_cash_reserve=getattr(settings, "fci_min_cash_reserve", 0.0) or 0.0,
            pending_notional=pending,
        )
        audit["live_cash_check"] = cash_audit
        if cash_error:
            return cash_error, audit
        return None, {**audit, "passed": True}

    # --- Redemption: the live holding must cover it ---
    try:
        live = broker.get_portfolio_snapshot() or {}
    except Exception:
        return "live_fund_position_unavailable", audit

    symbol = str(operation.fund_symbol or "").strip().upper()
    match = None
    for position in live.get("positions") or []:
        if str(position.get("symbol") or "").strip().upper() == symbol:
            match = position
            break
    if match is None:
        return "live_fund_position_missing", audit

    if str(match.get("currency") or "").strip().upper() != currency:
        return "instrument_currency_mismatch", audit

    # The redeemable figure is a VALUE, because the contract redeems by Monto.
    redeemable = positive_decimal(match.get("market_value"))
    if redeemable is None:
        # Without a trustworthy valuation we cannot tell whether the amount
        # exceeds the holding. Guessing here would over-redeem.
        return "live_fund_position_unavailable", audit

    audit["redeemable_value"] = float(redeemable)
    if amount + pending > redeemable:
        return "live_fund_position_insufficient", audit

    return None, {**audit, "passed": True}


# ---------------------------------------------------------------------------
# Operation lifecycle — creation and manual reconciliation
# ---------------------------------------------------------------------------

# Terminal outcomes a human may record. Deliberately no "resend" and no
# "retry": once a submission is ambiguous, the only way forward is to state
# what actually happened at IOL.
RECONCILIATION_TARGETS = {
    "confirmed": STATE_CONFIRMED,
    "rejected": STATE_REJECTED,
    "cancelled": STATE_CANCELLED,
    "reconciliation_required": STATE_RECONCILIATION_REQUIRED,
}


def create_fund_operation(
    db: Session,
    *,
    fund_symbol: str,
    operation: str,
    amount,
    note: str = "",
) -> dict:
    """Create a FundOperation in `prepared`. Never contacts IOL."""
    if operation not in (OPERATION_SUBSCRIBE, OPERATION_REDEEM):
        return _err(f"Operación inválida: '{operation}'. Usá subscribe o redeem.",
                    "invalid_fund_operation", 422)

    fund = get_fund(db, fund_symbol)
    if fund is None:
        # Only symbols the FCI catalog knows. A fund nobody catalogued is not
        # something to send money to.
        return _err("El fondo no está en el catálogo de FCI.",
                    "fund_not_in_catalog", 404)

    # The documented contract expresses BOTH sides by Monto, so a positive
    # amount is always required — there is no cuotapartes request to build.
    monto = serialize_monto(amount)
    if monto is None:
        return _err(
            "Se requiere un monto positivo. El contrato de Mi Cuenta expresa "
            "suscripción y rescate por Monto; no acepta cuotapartes.",
            "invalid_fund_amount", 422,
        )

    record = FundOperation(
        fund_symbol=fund.symbol,
        operation=operation,
        currency=fund.currency,
        amount=float(Decimal(monto)),
        quotaparts=None,
        status=STATE_PREPARED,
        cutoff_local_time=fund.cutoff_local_time,
        settlement_delay_days=fund.settlement_delay_days,
        blocked_reason="",
        error_message=note[:500] if note else "",
    )
    db.add(record)
    db.commit()

    app_log(db, "Operación FCI preparada", context={
        "fund_operation_id": record.id,
        "fund_symbol": record.fund_symbol,
        "operation": record.operation,
    })
    db.commit()

    return {
        "fund_operation_id": record.id,
        "fund_symbol": record.fund_symbol,
        "operation": record.operation,
        "currency": record.currency,
        "amount": record.amount,
        "status": record.status,
        "next_step": "validate",
        "message": (
            "Operación preparada. Requiere validación (soloValidar=true) antes "
            "de poder aprobarse."
        ),
    }


def get_fund_operation(db: Session, operation_id: int) -> dict:
    """Read-only state + audit for one operation. No secrets."""
    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}

    settings = get_settings()
    decisions = (
        db.query(FundOperationDecision)
        .filter(FundOperationDecision.fund_operation_id == operation_id)
        .order_by(FundOperationDecision.id)
        .all()
    )
    return {
        "fund_operation_id": operation.id,
        "fund_symbol": operation.fund_symbol,
        "operation": operation.operation,
        "currency": operation.currency,
        "amount": operation.amount,
        "status": operation.status,
        "broker_operation_id": operation.broker_operation_id or "",
        "cutoff_local_time": operation.cutoff_local_time,
        "settlement_delay_days": operation.settlement_delay_days,
        "validated_at": operation.validated_at.isoformat() if operation.validated_at else None,
        "validation_state": validation_state(operation, settings=settings),
        "validation_result": operation.validation_result or {},
        "blocked_reason": operation.blocked_reason or "",
        "error_message": operation.error_message or "",
        "submitted_at": operation.submitted_at.isoformat() if operation.submitted_at else None,
        "confirmed_at": operation.confirmed_at.isoformat() if operation.confirmed_at else None,
        "requires_reconciliation": operation.status in (
            STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED
        ),
        "immediate": False,
        "decisions": [
            {
                "decision": d.decision,
                "note": d.note or "",
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in decisions
        ],
    }


def reconcile_fund_operation(
    db: Session,
    operation_id: int,
    *,
    execution_key: str | None,
    outcome: str,
    note: str = "",
    broker_operation_id: str | None = None,
) -> dict:
    """Record what actually happened at IOL. NEVER re-sends anything.

    This is the only exit from `submission_unknown`: a human checks the IOL
    panel and states the truth. There is deliberately no path back to a
    submittable state — a fund operation is not idempotent.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Conciliación bloqueada: credencial no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.",
                    "invalid_execution_key", 403)
    if outcome not in RECONCILIATION_TARGETS:
        return _err(
            f"Resultado inválido: '{outcome}'. Válidos: "
            f"{', '.join(sorted(RECONCILIATION_TARGETS))}.",
            "invalid_reconciliation_outcome", 422,
        )
    clean_note = (note or "").strip()
    if not clean_note:
        return _err("Se requiere una nota con la evidencia de IOL.",
                    "note_required", 422)

    operation = db.query(FundOperation).filter(FundOperation.id == operation_id).first()
    if not operation:
        return {"error": "Fund operation not found", "status_code": 404}

    previous = operation.status
    if previous in (STATE_CONFIRMED, STATE_REJECTED, STATE_CANCELLED):
        return _err(f"La operación ya está en un estado terminal ('{previous}').",
                    "not_reconcilable", 409)

    target = RECONCILIATION_TARGETS[outcome]
    claimed = (
        db.query(FundOperation)
        .filter(FundOperation.id == operation_id, FundOperation.status == previous)
        .update({"status": target}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return _err("La operación fue conciliada por otra solicitud.",
                    "reconciliation_conflict", 409)
    db.refresh(operation)

    if broker_operation_id and not operation.broker_operation_id:
        operation.broker_operation_id = str(broker_operation_id).strip()[:100]
    if target == STATE_CONFIRMED:
        operation.confirmed_at = _utcnow().replace(tzinfo=None)
    operation.error_message = clean_note[:500]

    db.add(FundOperationDecision(
        fund_operation_id=operation.id,
        decision=f"reconciled:{outcome}",
        note=clean_note,
    ))
    app_log(db, "Conciliación manual de operación FCI", context={
        "fund_operation_id": operation.id,
        "previous_status": previous,
        "new_status": target,
        "outcome": outcome,
        "source": "manual_user",
    })
    db.commit()

    return {
        "fund_operation_id": operation.id,
        "previous_status": previous,
        "new_status": target,
        "outcome": outcome,
        "resent": False,
        "message": "Conciliación registrada. No se reenvió ninguna operación.",
    }
