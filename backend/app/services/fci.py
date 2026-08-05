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
import re
import secrets as _secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from app.broker.execution_class import CLASS_FCI, FAMILY_FUND
from app.broker.numeric import positive_decimal
from app.core.config import get_settings
from app.models.models import (
    FundBudgetReservation,
    FundInstrument,
    FundInstrumentVerification,
    FundOperation,
    FundOperationDecision,
)
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

# States that TIE UP capacity: money (or holdings) is committed even though
# nothing is settled yet. `submission_unknown` counts precisely because we
# cannot prove it did not reach IOL.
CAPACITY_RESERVING_STATES = (
    STATE_SUBMITTING, STATE_SUBMITTED, STATE_PENDING_CONFIRMATION,
    STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED,
)

# Terminal states. `confirmed` belongs here too: once the fund confirms, the
# real balance and the real holding reflect it, so counting it again would
# subtract the same money twice.
CAPACITY_FREE_STATES = (
    STATE_PREPARED, STATE_VALIDATION_REQUESTED, STATE_VALIDATED,
    STATE_APPROVAL_REQUESTED, STATE_CONFIRMED, STATE_REJECTED, STATE_CANCELLED,
)

# --- Daily ledger keys ------------------------------------------------------
# Subscribing and redeeming are opposite flows and must never share a budget:
# a day's worth of redemptions cannot license a subscription. They are also
# separate from every securities class.
LEDGER_CLASS_SUBSCRIBE = "FCI_SUBSCRIBE"
LEDGER_CLASS_REDEEM = "FCI_REDEEM"

# Keys written by earlier versions. Kept readable as audit, never counted:
# adding them to the current key would charge the same money twice.
LEGACY_LEDGER_CLASSES = {
    OPERATION_SUBSCRIBE: f"{CLASS_FCI}:{OPERATION_SUBSCRIBE}",
    OPERATION_REDEEM: f"{CLASS_FCI}:{OPERATION_REDEEM}",
}


def fci_ledger_class(operation: str) -> str:
    """Explicit ledger key for one FCI side."""
    if operation == OPERATION_SUBSCRIBE:
        return LEDGER_CLASS_SUBSCRIBE
    if operation == OPERATION_REDEEM:
        return LEDGER_CLASS_REDEEM
    raise ValueError(f"unknown fund operation: {operation!r}")


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


def fund_verification_hash(fund: FundInstrument, operation: str) -> str:
    """Hash of everything about the FUND that an authorisation depends on.

    A validation is evidence about a request made against a fund in a
    particular verified state. Demote the fund, withdraw a capability, change
    the cutoff or the minimum, and that evidence no longer describes what we
    are about to send — even though the request bytes are identical.

    Includes the identity hash so a frozen-and-re-verified fund never inherits
    an approval given for what it used to be.
    """
    canonical = json.dumps(
        {
            "symbol": str(fund.symbol or "").strip().upper(),
            "identity_hash": fund.identity_hash or "",
            "verification_status": str(fund.verification_status or ""),
            "currency": str(fund.currency or "").strip().upper(),
            "cutoff_local_time": fund.cutoff_local_time or "",
            "minimum_amount": serialize_monto(fund.minimum_amount) or "",
            "settlement_delay_days": fund.settlement_delay_days,
            "active": bool(fund.active),
            # Only the capability THIS operation needs: turning redemption off
            # must not invalidate a pending subscription's validation.
            "capability": (
                bool(fund.subscription_supported) if operation == OPERATION_SUBSCRIBE
                else bool(fund.redemption_supported)
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fund_verification_changed(
    operation: FundOperation, fund: FundInstrument | None
) -> bool:
    """Did the fund's verified state move since this operation was validated?"""
    if fund is None:
        return True
    recorded = getattr(operation, "validated_fund_hash", "") or ""
    if not recorded:
        # Validated before this field existed, or never validated. Either way
        # we cannot claim the fund state was checked.
        return True
    return not _secrets.compare_digest(
        recorded, fund_verification_hash(fund, operation.operation)
    )


def validation_state(operation: FundOperation, *, settings, now: datetime | None = None,
                     fund: FundInstrument | None = None) -> dict:
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

    # The fund's own verified state is part of what was validated. Checked
    # only when the caller supplies the fund, so callers that genuinely have
    # no session (pure TTL checks) keep working.
    if fund is not None and fund_verification_changed(operation, fund):
        return {**detail, "valid": False, "code": "fund_verification_changed"}

    return {**detail, "valid": True, "code": None}


def fci_execution_locked(settings) -> bool:
    """The GLOBAL lock applies to funds exactly as it does to securities.

    A per-capability flag is a second key, never a replacement for the master
    one: with ORDER_EXECUTION_ENABLED=false nothing reaches IOL, funds
    included.
    """
    return not bool(getattr(settings, "order_execution_enabled", False))


def fund_capability_blocker(operation: str, settings) -> str | None:
    """The per-capability flag for ONE side, or None if it is on.

    Subscription and redemption are independent capabilities: being allowed to
    put money into a fund says nothing about being allowed to take it out.
    """
    if operation == OPERATION_SUBSCRIBE:
        if not bool(getattr(settings, "fci_subscription_enabled", False)):
            return "fci_subscription_disabled"
        return None
    if operation == OPERATION_REDEEM:
        if not bool(getattr(settings, "fci_redemption_enabled", False)):
            return "fci_redemption_disabled"
        return None
    return "invalid_fund_operation"


def _resolve_broker(broker_or_factory):
    """Accept either a broker or a zero-arg factory that builds one.

    Discriminated by the METHOD a broker must have, not by `callable()`: a
    test double is callable too, so a callable-check would call the double and
    use whatever it returned. What actually distinguishes them is that a
    broker can submit a fund request and a factory cannot.
    """
    if broker_or_factory is None:
        return None
    if hasattr(broker_or_factory, "submit_fund_request"):
        return broker_or_factory
    if callable(broker_or_factory):
        return broker_or_factory()
    return broker_or_factory


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _err(message: str, code: str, status_code: int) -> dict:
    return {"error": message, "code": code, "status_code": status_code}


def get_fci_capability() -> dict:
    """Read-only capability report. Contains no secrets."""
    settings = get_settings()
    locked = fci_execution_locked(settings)
    subscription_flag = bool(getattr(settings, "fci_subscription_enabled", False))
    redemption_flag = bool(getattr(settings, "fci_redemption_enabled", False))
    limits_configured = bool(
        getattr(settings, "fci_max_operation_amount", 0) > 0
        and getattr(settings, "fci_max_daily_amount", 0) > 0
    )
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
        "subscription_flag_enabled": subscription_flag,
        "redemption_flag_enabled": redemption_flag,
        # The GLOBAL lock is reported here too: a per-capability flag is a
        # second key, never a replacement for the master one.
        "execution_locked": locked,
        "global_execution_open": not locked,
        # Validating and submitting are gated by the SAME two keys, reported
        # separately so an operator can see which one is shut.
        "can_validate_subscription": not locked and subscription_flag,
        "can_validate_redemption": not locked and redemption_flag,
        "can_submit_subscription": (
            not locked and subscription_flag
            and FCI_REQUEST_CONTRACT_VERIFIED and limits_configured
        ),
        "can_submit_redemption": (
            not locked and redemption_flag
            and FCI_REQUEST_CONTRACT_VERIFIED and limits_configured
        ),
        "ledger_classes": {
            OPERATION_SUBSCRIBE: LEDGER_CLASS_SUBSCRIBE,
            OPERATION_REDEEM: LEDGER_CLASS_REDEEM,
        },
        "validation_ttl_seconds": getattr(settings, "fci_validation_ttl_seconds", 0),
        "limits_configured": limits_configured,
        # Ready only when the global lock is OPEN, the capability flag is on,
        # the field mapping is verified and the limits are configured.
        "ready_for_real_subscription": (
            FCI_REQUEST_CONTRACT_VERIFIED and not locked
            and subscription_flag and limits_configured
        ),
        "ready_for_real_redemption": (
            FCI_REQUEST_CONTRACT_VERIFIED and not locked
            and redemption_flag and limits_configured
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


FUND_STATUS_CANDIDATE = "candidate"
FUND_STATUS_VERIFIED = "verified"
FUND_STATUS_IDENTITY_CHANGED = "identity_changed"
FUND_STATUS_REJECTED = "rejected"

# What makes a fund the SAME fund. Cutoff, minimum and settlement delay are
# operational parameters, not identity: they can be corrected without the
# instrument becoming a different one.
FUND_IDENTITY_FIELDS = ("symbol", "currency", "manager")


def fund_identity_hash(fund: FundInstrument) -> str:
    """Hash of what makes this fund THIS fund."""
    canonical = json.dumps(
        {field: str(getattr(fund, field, "") or "").strip().upper()
         for field in FUND_IDENTITY_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def fund_operability_blockers(fund: FundInstrument, operation: str) -> list[str]:
    """Stable codes explaining why a fund may not be operated. Empty = it may.

    Order matters: the first code is what gets reported, so it must be the
    most fundamental reason.
    """
    reasons: list[str] = []
    if not bool(fund.active):
        reasons.append("fund_inactive")
    status = str(fund.verification_status or FUND_STATUS_CANDIDATE)
    if status == FUND_STATUS_IDENTITY_CHANGED:
        reasons.append("fund_identity_changed")
    elif status == FUND_STATUS_REJECTED:
        reasons.append("fund_rejected")
    elif status != FUND_STATUS_VERIFIED:
        reasons.append("fund_not_verified")

    provenance = dict(fund.field_provenance or {})

    def _verified(field: str) -> bool:
        return provenance.get(field) in FUND_VERIFYING_PROVENANCES

    if not fund.cutoff_local_time or not _verified("cutoff_local_time"):
        reasons.append("fund_cutoff_unverified")
    if not positive_decimal(fund.minimum_amount) or not _verified("minimum_amount"):
        reasons.append("fund_minimum_unverified")
    if not str(fund.currency or "").strip() or not _verified("currency"):
        reasons.append("fund_currency_unverified")

    if operation == OPERATION_SUBSCRIBE and not bool(fund.subscription_supported):
        reasons.append("fund_subscription_unsupported")
    if operation == OPERATION_REDEEM and not bool(fund.redemption_supported):
        reasons.append("fund_redemption_unsupported")
    return reasons


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
    created = updated = skipped = frozen = preserved = 0

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

        # --- Identity first. A refresh may correct operational parameters of
        #     the SAME fund, but if the fund itself became something else, no
        #     prior administrative verification applies to it.
        previous_identity = {
            field: getattr(fund, field, None) for field in FUND_IDENTITY_FIELDS
        }
        previous_hash = fund.identity_hash or ""
        was_verified = fund.verification_status == FUND_STATUS_VERIFIED

        fund.name = str(item.get("descripcion") or item.get("nombre") or "")[:200]
        fund.manager = str(item.get("administradora") or "")[:200]
        fund.currency = _map_currency(item.get("moneda"))
        fund.raw_detail = {k: v for k, v in item.items() if not isinstance(v, (dict, list))}
        fund.source = "iol_fci_catalog"
        fund.verified_at = now
        fund.active = True

        new_hash = fund_identity_hash(fund)
        identity_changed = bool(previous_hash) and previous_hash != new_hash
        fund.identity_hash = new_hash
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

        previous = dict(fund.field_provenance or {})

        if identity_changed:
            # FREEZE. The administrative override described a different fund,
            # so it does not carry over — inheriting it is how an approval for
            # fund A ends up authorising money into fund B.
            fund.previous_identity = previous_identity
            fund.pending_identity = {
                field: getattr(fund, field, None) for field in FUND_IDENTITY_FIELDS
            }
            fund.verification_status = FUND_STATUS_IDENTITY_CHANGED
            fund.subscription_supported = False
            fund.redemption_supported = False
            for field in observed:
                provenance[field] = PROV_IOL_FCI_OBSERVED
            fund.field_provenance = provenance
            fund.raw_detail = {**(fund.raw_detail or {}),
                               "_observed_operational": observed}
            db.add(FundInstrumentVerification(
                fund_symbol=fund.symbol,
                action="identity_changed",
                previous_status=FUND_STATUS_VERIFIED if was_verified
                                else str(fund.verification_status or ""),
                new_status=FUND_STATUS_IDENTITY_CHANGED,
                currency=str(fund.currency or ""),
                cutoff_local_time=fund.cutoff_local_time,
                minimum_amount=fund.minimum_amount,
                settlement_delay_days=fund.settlement_delay_days,
                source="iol_fci_catalog",
                note="La identidad del fondo cambió durante un refresh automático.",
                identity_hash=new_hash,
            ))
            frozen += 1
            continue

        # Same fund: preserve any field a human already verified. An automatic
        # refresh must not silently demote an administrative confirmation.
        for field in ("cutoff_local_time", "minimum_amount", "settlement_delay_days",
                      "currency"):
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
        if fund.verification_status == FUND_STATUS_REJECTED:
            # A human said no. A refresh is not an appeal.
            fund.subscription_supported = False
            fund.redemption_supported = False
            continue
        fund.verification_status = (
            FUND_STATUS_VERIFIED if operable else FUND_STATUS_CANDIDATE
        )
        # Capabilities are NOT inferred from cutoff+minimum being present.
        # They stay off until an administrative confirmation says otherwise.
        fund.subscription_supported = operable and bool(fund.subscription_supported)
        fund.redemption_supported = operable and bool(fund.redemption_supported)
        if operable and was_verified:
            preserved += 1

    db.commit()
    return {"created": created, "updated": updated, "skipped": skipped,
            "identity_frozen": frozen, "verifications_preserved": preserved,
            "total": db.query(FundInstrument).count()}


# ---------------------------------------------------------------------------
# Administrative verification of a fund
#
# The catalog can READ a fund; only a human can VOUCH for it. These functions
# never contact IOL, never touch a flag and never create an operation — they
# record an assertion and change one row's verification state.
# ---------------------------------------------------------------------------

_CUTOFF_PATTERN = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _fund_verification_hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")
    ).hexdigest()


def verify_fund_instrument(
    db: Session,
    symbol: str,
    *,
    execution_key: str | None,
    cutoff_local_time: str | None,
    minimum_amount,
    settlement_delay_days,
    currency: str | None,
    subscription_supported,
    redemption_supported,
    source: str = "",
    note: str = "",
) -> dict:
    """Promote a fund to `verified` on a human's explicit assertion.

    Every parameter must be stated. Nothing is inherited from the observed
    catalog values: the whole point is that a human read the documentation and
    is now asserting these numbers, so silently reusing an unverified reading
    would defeat the exercise.

    Never calls IOL. Never enables a flag. Never creates a FundOperation.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.",
                    "invalid_execution_key", 403)

    if not str(note or "").strip():
        return _err("Se requiere una nota que explique la verificación.",
                    "verification_note_required", 422)

    fund = get_fund(db, symbol)
    if fund is None:
        return _err("El fondo no está en el catálogo de FCI.",
                    "fund_not_in_catalog", 404)

    cutoff = str(cutoff_local_time or "").strip()
    if not _CUTOFF_PATTERN.match(cutoff):
        return _err("El cutoff debe tener formato HH:MM (24h).",
                    "invalid_cutoff_format", 422)

    minimum = positive_decimal(minimum_amount)
    if minimum is None:
        return _err("El monto mínimo debe ser mayor que cero.",
                    "invalid_minimum_amount", 422)

    try:
        delay = int(settlement_delay_days)
    except (TypeError, ValueError):
        return _err("El plazo de liquidación debe ser un entero de días.",
                    "invalid_settlement_delay", 422)
    if delay < 0:
        return _err("El plazo de liquidación no puede ser negativo.",
                    "invalid_settlement_delay", 422)

    stated_currency = str(currency or "").strip().upper()
    if not stated_currency:
        return _err("Se requiere confirmar la moneda del fondo.",
                    "fund_currency_required", 422)
    if stated_currency != str(fund.currency or "").strip().upper():
        # The operator is describing a different fund from the one catalogued.
        return _err(
            f"La moneda declarada ({stated_currency}) no coincide con la del "
            f"catálogo ({fund.currency}).",
            "fund_currency_mismatch", 409,
        )

    # Both capabilities must be stated explicitly. Defaulting either one to
    # True would hand out a capability nobody asked for.
    if subscription_supported is None or redemption_supported is None:
        return _err(
            "Se requiere indicar explícitamente subscription_supported y "
            "redemption_supported.",
            "fund_capabilities_required", 422,
        )
    subscribe_ok = bool(subscription_supported)
    redeem_ok = bool(redemption_supported)

    if fund.verification_status == FUND_STATUS_IDENTITY_CHANGED:
        # Verifying a frozen fund would rubber-stamp an identity change that
        # nobody looked at. Resolve the change first.
        return _err(
            "El fondo está congelado por un cambio de identidad. "
            "Revisá previous_identity y pending_identity antes de verificar.",
            "fund_identity_changed", 409,
        )

    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE, _utcnow_naive

    previous_status = str(fund.verification_status or FUND_STATUS_CANDIDATE)
    fund.cutoff_local_time = cutoff
    fund.minimum_amount = float(minimum)
    fund.settlement_delay_days = delay
    fund.subscription_supported = subscribe_ok
    fund.redemption_supported = redeem_ok
    fund.verification_status = FUND_STATUS_VERIFIED
    fund.verified_at = _utcnow_naive()
    fund.identity_hash = fund_identity_hash(fund)
    fund.field_provenance = {
        **(fund.field_provenance or {}),
        "cutoff_local_time": PROV_ADMIN_OVERRIDE,
        "minimum_amount": PROV_ADMIN_OVERRIDE,
        "settlement_delay_days": PROV_ADMIN_OVERRIDE,
        "currency": PROV_ADMIN_OVERRIDE,
    }

    data_hash = _fund_verification_hash({
        "symbol": fund.symbol,
        "currency": stated_currency,
        "cutoff_local_time": cutoff,
        "minimum_amount": float(minimum),
        "settlement_delay_days": delay,
        "subscription_supported": subscribe_ok,
        "redemption_supported": redeem_ok,
    })
    record = FundInstrumentVerification(
        fund_symbol=fund.symbol,
        action="verify",
        previous_status=previous_status,
        new_status=FUND_STATUS_VERIFIED,
        currency=stated_currency,
        cutoff_local_time=cutoff,
        minimum_amount=float(minimum),
        settlement_delay_days=delay,
        subscription_supported=subscribe_ok,
        redemption_supported=redeem_ok,
        source=str(source or "")[:100],
        note=str(note).strip()[:2000],
        data_hash=data_hash,
        identity_hash=fund.identity_hash,
    )
    db.add(record)
    db.commit()

    app_log(db, "Fondo FCI verificado administrativamente", context={
        "fund_symbol": fund.symbol,
        "previous_status": previous_status,
        "new_status": fund.verification_status,
        "verification_id": record.id,
    })
    db.commit()

    return {
        "fund_symbol": fund.symbol,
        "previous_status": previous_status,
        "verification_status": fund.verification_status,
        "cutoff_local_time": fund.cutoff_local_time,
        "minimum_amount": fund.minimum_amount,
        "settlement_delay_days": fund.settlement_delay_days,
        "subscription_supported": fund.subscription_supported,
        "redemption_supported": fund.redemption_supported,
        "verification_id": record.id,
        "data_hash": data_hash,
        # Said explicitly because it is the most likely misreading: verifying
        # a fund does not turn any capability on.
        "flags_unchanged": True,
        "message": (
            "Fondo verificado. Esto NO habilita la ejecución: siguen haciendo "
            "falta ORDER_EXECUTION_ENABLED y el flag de la capacidad."
        ),
    }


def _set_fund_status(
    db: Session,
    symbol: str,
    *,
    execution_key: str | None,
    note: str,
    action: str,
    new_status: str,
) -> dict:
    """Shared body of reject and demote. Both block operation."""
    settings = get_settings()
    if not settings.execution_admin_key:
        return _err("Credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.",
                    "invalid_execution_key", 403)
    if not str(note or "").strip():
        return _err("Se requiere una nota que explique la decisión.",
                    "verification_note_required", 422)

    fund = get_fund(db, symbol)
    if fund is None:
        return _err("El fondo no está en el catálogo de FCI.",
                    "fund_not_in_catalog", 404)

    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE

    previous_status = str(fund.verification_status or FUND_STATUS_CANDIDATE)
    fund.verification_status = new_status
    # Both capabilities go off. Neither a rejection nor a demotion leaves a
    # fund half-operable.
    fund.subscription_supported = False
    fund.redemption_supported = False
    # Withdraw the administrative provenance: the assertion is being taken
    # back, so the fields must stop counting as verified.
    fund.field_provenance = {
        field: source for field, source in (fund.field_provenance or {}).items()
        if source != PROV_ADMIN_OVERRIDE
    }

    record = FundInstrumentVerification(
        fund_symbol=fund.symbol,
        action=action,
        previous_status=previous_status,
        new_status=new_status,
        currency=str(fund.currency or ""),
        cutoff_local_time=fund.cutoff_local_time,
        minimum_amount=fund.minimum_amount,
        settlement_delay_days=fund.settlement_delay_days,
        note=str(note).strip()[:2000],
        identity_hash=fund.identity_hash or "",
    )
    db.add(record)
    db.commit()

    app_log(db, f"Fondo FCI {action}", context={
        "fund_symbol": fund.symbol,
        "previous_status": previous_status,
        "new_status": new_status,
        "verification_id": record.id,
    })
    db.commit()

    return {
        "fund_symbol": fund.symbol,
        "previous_status": previous_status,
        "verification_status": fund.verification_status,
        "subscription_supported": False,
        "redemption_supported": False,
        "verification_id": record.id,
        "message": "El fondo ya no puede operar.",
    }


def reject_fund_instrument(db: Session, symbol: str, *, execution_key, note: str) -> dict:
    """A human refuses this fund. A later automatic refresh is not an appeal."""
    return _set_fund_status(
        db, symbol, execution_key=execution_key, note=note,
        action="reject", new_status=FUND_STATUS_REJECTED,
    )


def demote_fund_instrument(db: Session, symbol: str, *, execution_key, note: str) -> dict:
    """Withdraw a verification without refusing the fund outright.

    Refuses to touch a fund frozen by an identity change. Demoting one would
    move it to `candidate` and quietly discard the pending identity — the
    change would be *dismissed* rather than *decided*, and the next
    verification would be granted without anyone having looked at what
    actually changed. That decision has its own endpoint.
    """
    fund = get_fund(db, symbol)
    if fund is not None and fund.verification_status == FUND_STATUS_IDENTITY_CHANGED:
        return _err(
            "El fondo está congelado por un cambio de identidad. Resolvelo con "
            "POST /api/funds/catalog/{symbol}/identity-decision antes de degradarlo.",
            "fund_identity_decision_required", 409,
        )
    return _set_fund_status(
        db, symbol, execution_key=execution_key, note=note,
        action="demote", new_status=FUND_STATUS_CANDIDATE,
    )


def pending_identity_hash(fund: FundInstrument) -> str:
    """Hash of the identity a frozen fund is waiting for a decision on.

    The operator quotes it back when deciding, so a decision made while
    looking at one change cannot be applied to a different one that landed in
    between.
    """
    payload = fund.pending_identity or {}
    canonical = json.dumps(
        {field: str(payload.get(field) or "").strip().upper()
         for field in FUND_IDENTITY_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def decide_fund_identity_change(
    db: Session,
    symbol: str,
    *,
    execution_key: str | None,
    decision: str,
    expected_pending_identity_hash: str | None,
    note: str,
) -> dict:
    """Accept or reject the identity change that froze this fund.

    Accepting does NOT re-verify: it moves the fund to `candidate` with both
    capabilities off, so the operational parameters must be asserted again.
    The previous verification described a different fund; carrying it over is
    exactly the failure this freeze exists to prevent.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.",
                    "invalid_execution_key", 403)
    if decision not in ("accept", "reject"):
        return _err(f"Decisión inválida: '{decision}'. Usá accept o reject.",
                    "invalid_identity_decision", 422)
    if not str(note or "").strip():
        return _err("Se requiere una nota que explique la decisión.",
                    "verification_note_required", 422)

    fund = get_fund(db, symbol)
    if fund is None:
        return _err("El fondo no está en el catálogo de FCI.",
                    "fund_not_in_catalog", 404)
    if fund.verification_status != FUND_STATUS_IDENTITY_CHANGED:
        return _err("El fondo no tiene un cambio de identidad pendiente.",
                    "no_pending_identity_change", 409)

    current_hash = pending_identity_hash(fund)
    if not expected_pending_identity_hash or not _secrets.compare_digest(
        str(expected_pending_identity_hash), current_hash
    ):
        # The operator is deciding about an identity that is no longer the one
        # on the table.
        return _err(
            "El hash de identidad pendiente no coincide. Volvé a leer el fondo "
            "antes de decidir.",
            "pending_identity_hash_mismatch", 409,
        )

    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE

    previous_status = FUND_STATUS_IDENTITY_CHANGED
    if decision == "accept":
        fund.verification_status = FUND_STATUS_CANDIDATE
        fund.previous_identity = None
        fund.pending_identity = None
        fund.identity_hash = fund_identity_hash(fund)
        new_status = FUND_STATUS_CANDIDATE
    else:
        fund.verification_status = FUND_STATUS_REJECTED
        fund.active = False
        new_status = FUND_STATUS_REJECTED

    # Either way both capabilities stay off and the administrative provenance
    # is withdrawn: whatever was verified was verified about something else.
    fund.subscription_supported = False
    fund.redemption_supported = False
    fund.field_provenance = {
        field: source for field, source in (fund.field_provenance or {}).items()
        if source != PROV_ADMIN_OVERRIDE
    }

    record = FundInstrumentVerification(
        fund_symbol=fund.symbol,
        action=f"identity_{decision}",
        previous_status=previous_status,
        new_status=new_status,
        currency=str(fund.currency or ""),
        cutoff_local_time=fund.cutoff_local_time,
        minimum_amount=fund.minimum_amount,
        settlement_delay_days=fund.settlement_delay_days,
        note=str(note).strip()[:2000],
        data_hash=current_hash,
        identity_hash=fund.identity_hash or "",
    )
    db.add(record)
    db.commit()

    app_log(db, f"Cambio de identidad de FCI {decision}", context={
        "fund_symbol": fund.symbol,
        "decision": decision,
        "new_status": new_status,
        "verification_id": record.id,
    })
    db.commit()

    return {
        "fund_symbol": fund.symbol,
        "decision": decision,
        "previous_status": previous_status,
        "verification_status": fund.verification_status,
        "subscription_supported": False,
        "redemption_supported": False,
        "verification_id": record.id,
        "requires_reverification": decision == "accept",
        "message": (
            "Identidad aceptada. El fondo vuelve a candidate y requiere una "
            "verificación nueva: lo verificado antes describía otro fondo."
            if decision == "accept"
            else "Identidad rechazada. El fondo queda inactivo y no puede operar."
        ),
    }


def list_fund_verifications(db: Session, symbol: str) -> list[dict]:
    """Append-only audit trail for one fund, newest first. No secrets."""
    rows = (
        db.query(FundInstrumentVerification)
        .filter(FundInstrumentVerification.fund_symbol == str(symbol or "").strip().upper())
        .order_by(FundInstrumentVerification.id.desc())
        .all()
    )
    return [
        {
            "id": row.id,
            "action": row.action,
            "previous_status": row.previous_status,
            "new_status": row.new_status,
            "currency": row.currency,
            "cutoff_local_time": row.cutoff_local_time,
            "minimum_amount": row.minimum_amount,
            "settlement_delay_days": row.settlement_delay_days,
            "subscription_supported": row.subscription_supported,
            "redemption_supported": row.redemption_supported,
            "source": row.source,
            "note": row.note,
            "data_hash": row.data_hash,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]


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
    # A validation must exist, be fresh, and describe THIS request AND the
    # fund's verified state at the time it was made.
    validation = validation_state(operation, settings=settings, fund=fund)
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
    # The SAME operability rules as create/validate/submit. Duplicating a
    # subset here is how a preview ends up approving what submit refuses.
    for code in fund_operability_blockers(fund, operation.operation):
        if code not in blocking:
            blocking.append(code)
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


def validate_fund_operation(db: Session, operation_id: int, broker_factory) -> dict:
    """Pre-check via the official `soloValidar=true` mechanism.

    `broker_factory` is a CALLABLE, not a broker. The route used to build the
    broker before calling this function, so a request refused by the global
    lock still authenticated against IOL on its way to being refused — the
    lock stopped the operation but not the connection.

    Now the broker is created only after every gate has passed, which makes
    "blocked" mean zero broker instantiations and zero HTTP calls. A plain
    broker object is still accepted for callers that already have one.

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
            "Validación bloqueada: execution_locked "
            "(ORDER_EXECUTION_ENABLED=false).",
            "execution_locked", 423,
        )

    # And so does the per-capability flag. Validating a redemption we are not
    # allowed to send is still an authenticated call about an operation that
    # cannot happen: with the capability off there is nothing to pre-check.
    # Refused BEFORE the broker is obtained, so no request of any kind exists,
    # and BEFORE any write, so validated_at and the payload hash stay as they
    # were.
    capability_error = fund_capability_blocker(operation.operation, settings)
    if capability_error:
        return _err(
            f"Operación de FCI deshabilitada por configuración: {capability_error}.",
            capability_error, 423,
        )

    # The FUND itself must still be operable. A fund that was demoted,
    # rejected or frozen after this operation was prepared must not be
    # pre-checked as if the old verification still held.
    fund = get_fund(db, operation.fund_symbol)
    if fund is None:
        return _err("El fondo no está en el catálogo de FCI.", "fund_not_in_catalog", 404)
    fund_blockers = fund_operability_blockers(fund, operation.operation)
    if fund_blockers:
        operation.blocked_reason = fund_blockers[0]
        db.commit()
        return _err(f"El fondo no está habilitado para operar: {fund_blockers[0]}.",
                    fund_blockers[0], 409)

    request, error = build_fund_request(operation, solo_validar=True)
    if error:
        operation.blocked_reason = error
        db.commit()
        return _err(f"No se pudo construir la request de validación: {error}.",
                    error, 422)

    # --- Every gate has passed. ONLY NOW is a broker created. ---
    try:
        broker = _resolve_broker(broker_factory)
    except Exception as exc:
        return _err(f"Broker no disponible: {str(exc)[:200]}", "broker_unavailable", 502)
    if broker is None:
        return _err("Broker no disponible.", "broker_unavailable", 502)

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
        # Bind the validation to EXACTLY what was validated — including the
        # fund's verification state, so a later demotion or identity change
        # invalidates it instead of riding on an approval that no longer holds.
        operation.validated_fund_hash = fund_verification_hash(fund, operation.operation)
        operation.validated_payload_hash = validation_payload_hash(operation)
    else:
        operation.validated_at = None
        operation.validated_payload_hash = ""
        operation.validated_fund_hash = ""
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

    capability_error = fund_capability_blocker(operation.operation, settings)
    if capability_error:
        return _err(
            f"Operación de FCI deshabilitada por configuración: {capability_error}.",
            capability_error, 423,
        )

    # A FRESH validation describing THIS request — and the fund's verified
    # state when it was made — is mandatory.
    fund = get_fund(db, operation.fund_symbol)
    if fund is None:
        return _err("El fondo no está en el catálogo de FCI.", "fund_not_in_catalog", 404)
    validation = validation_state(operation, settings=settings, fund=fund)
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

    # READ-ONLY. Consumes no budget, so an operation refused here for having
    # no cash or no holding costs the day's allowance nothing.
    preflight_error, preflight_audit = fund_live_preflight(
        db, operation, broker=broker, settings=settings
    )
    if preflight_error:
        operation.blocked_reason = preflight_error
        db.commit()
        return _err(f"Preflight de FCI bloqueado: {preflight_error}.",
                    preflight_error, 409)

    # --- LAST look at the fund, immediately before the claim. Everything
    #     above took time; a demotion or an identity change that landed in
    #     between must stop the send, not ride on a preview that predates it.
    db.refresh(fund)
    last_blockers = fund_operability_blockers(fund, operation.operation)
    if last_blockers:
        operation.blocked_reason = last_blockers[0]
        db.commit()
        return _err(f"El fondo dejó de estar habilitado: {last_blockers[0]}.",
                    last_blockers[0], 409)
    if fund_verification_changed(operation, fund):
        operation.blocked_reason = "fund_verification_changed"
        db.commit()
        return _err(
            "La verificación del fondo cambió después de validar. "
            "Se requiere una validación nueva.",
            "fund_verification_changed", 409,
        )

    # --- Claim + reserve, together or not at all. Only now does anything
    #     become irreversible.
    claim_error, claim_audit = claim_and_reserve_fund_operation(
        db, operation, settings=settings
    )
    if claim_error:
        db.refresh(operation)
        if claim_error == "fund_operation_already_submitted":
            return _err("La operación ya fue reclamada por otra solicitud.",
                        claim_error, 409)
        operation.blocked_reason = claim_error
        db.commit()
        return _err(f"Reserva de presupuesto bloqueada: {claim_error}.",
                    claim_error, 409)

    db.add(FundOperationDecision(
        fund_operation_id=operation.id, decision="approved",
        note=note, preview_hash=str(preview_hash),
    ))
    db.commit()
    db.refresh(operation)

    # The lifecycle dict lets us tell "never sent" from "may have been sent"
    # even if the client raises: `request_started` is stamped by the client
    # immediately before the POST begins.
    lifecycle: dict = {}
    try:
        result = broker.submit_fund_request(request, lifecycle=lifecycle)
    except Exception as exc:
        # The client is supposed to convert failures into outcomes, so
        # reaching here means the client itself broke. Which side of the point
        # of no return it broke on is NOT a guess: it is recorded.
        logger.exception("submit_fund_request raised for fund operation %s", operation.id)
        started = bool(lifecycle.get("request_started"))
        result = {
            "outcome": "submission_unknown" if started else "local_error",
            "operation_id": "",
            "raw_response": {},
            "http_requests_sent": 1 if started else 0,
            "request_started": started,
            "error": f"{type(exc).__name__}: {str(exc)[:200]}",
        }

    outcome = result.get("outcome")
    requests_sent = int(result.get("http_requests_sent") or 0)

    # --- Nothing left the process: undo the reservation and let the operator
    #     try again. This is the ONLY path that releases budget automatically,
    #     and it is safe precisely because no request exists to reconcile.
    if requests_sent == 0 and outcome in ("local_error", "rejected"):
        released = release_fund_budget(
            db, operation, settings=settings,
            reason=f"fund_submission_never_sent:{outcome}",
        )
        # Back to where the human left it. The claim already moved the row to
        # `submitting`, so restore the validated status FIRST and then ask
        # whether that validation is still alive — asking while the row says
        # `submitting` would always answer no, and would silently demand a
        # re-validation that the TTL had not actually expired.
        operation.status = STATE_VALIDATED
        if not validation_state(operation, settings=settings)["valid"]:
            operation.status = STATE_PREPARED
        operation.broker_response = result.get("raw_response", {}) or {}
        operation.error_message = (result.get("error") or "")[:500]
        operation.blocked_reason = str(outcome)
        db.commit()
        app_log(db, "Envío de FCI abortado antes de salir", context={
            "fund_operation_id": operation.id,
            "outcome": outcome,
            "budget_released": released.get("released"),
            "status": operation.status,
        })
        db.commit()
        return {
            "fund_operation_id": operation.id,
            "operation": operation.operation,
            "status": operation.status,
            "broker_operation_id": "",
            "outcome": outcome,
            "http_requests_sent": 0,
            "requires_reconciliation": False,
            "budget_released": released,
            "message": (
                "No se envió ninguna request. El presupuesto reservado se liberó "
                "y la operación puede reintentarse."
            ),
        }

    # --- A request DID start. The budget stays reserved from here on: an
    #     operation that may be live at IOL must not free capacity for a
    #     second one.
    operation.broker_response = result.get("raw_response", {}) or {}
    if outcome == "submitted":
        operation.broker_operation_id = str(result.get("operation_id") or "")
        operation.submitted_at = _utcnow()
        # NOT "confirmed": a fund confirms later, at a valuation that did not
        # exist when we submitted.
        operation.status = STATE_PENDING_CONFIRMATION
    elif outcome == "rejected":
        # Refused by IOL AFTER the request arrived. Terminal, but the budget
        # is not returned automatically — releasing it is an administrative
        # act, because "IOL says no" and "IOL received nothing" are different
        # facts and only the second one is provable here.
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
        "request_started": bool(result.get("request_started", True)),
        "response_received": bool(result.get("response_received", False)),
    })

    return {
        "fund_operation_id": operation.id,
        "operation": operation.operation,
        "status": operation.status,
        "broker_operation_id": operation.broker_operation_id,
        "outcome": outcome,
        "http_requests_sent": requests_sent,
        "request_started": bool(result.get("request_started", True)),
        "response_received": bool(result.get("response_received", False)),
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


def _pending_fund_total(
    db: Session,
    *,
    operation: str,
    currency: str,
    symbol: str | None = None,
    exclude_id: int | None = None,
) -> Decimal:
    """Amount tied up by unresolved fund operations of ONE side."""
    rows = (
        db.query(FundOperation)
        .filter(FundOperation.operation == operation)
        .filter(FundOperation.status.in_(list(CAPACITY_RESERVING_STATES)))
        .all()
    )
    total = Decimal("0")
    target_currency = (currency or "").strip().upper()
    target_symbol = (symbol or "").strip().upper() or None
    for row in rows:
        if exclude_id is not None and row.id == exclude_id:
            continue
        if str(row.currency or "").strip().upper() != target_currency:
            continue
        if target_symbol and str(row.fund_symbol or "").strip().upper() != target_symbol:
            continue
        amount = positive_decimal(row.amount)
        if amount is not None:
            total += amount
    return total


def pending_fund_subscriptions(
    db: Session, *, currency: str, exclude_id: int | None = None
) -> Decimal:
    """Cash already committed by unresolved SUBSCRIPTIONS, per currency.

    Affects the spendable balance and nothing else. A pending subscription is
    money on its way out; it does not make any holding smaller, so it must
    never be subtracted from a redeemable position.

    Not grouped by symbol: every subscription in a currency competes for the
    same cash, whichever fund it goes to.
    """
    return _pending_fund_total(
        db, operation=OPERATION_SUBSCRIBE, currency=currency, exclude_id=exclude_id
    )


def pending_fund_redemptions(
    db: Session, *, symbol: str, currency: str, exclude_id: int | None = None
) -> Decimal:
    """Holding already committed by unresolved REDEMPTIONS of ONE fund.

    Grouped by SYMBOL as well as currency: redeeming from fund A says nothing
    about how much of fund B is left. And a pending redemption is money on its
    way in — it never reduces the cash available to subscribe.
    """
    return _pending_fund_total(
        db,
        operation=OPERATION_REDEEM,
        currency=currency,
        symbol=symbol,
        exclude_id=exclude_id,
    )


def fund_live_preflight(
    db: Session, operation: FundOperation, *, broker, settings, now: datetime | None = None
) -> tuple[str | None, dict]:
    """Everything that must be true at the LAST possible moment. READ-ONLY.

    Deliberately re-reads the world instead of trusting the preview: the
    preview is what a human approved, not a reservation of cash or holdings.

    Subscription: live balance in the fund's OWN currency, minus pending
    subscriptions, minus the reserve, plus the cost buffer, above the minimum.
    Redemption: live position in that exact fund, minus pending redemptions of
    that same fund.
    Both: the fund's own cutoff must still be open.

    This function does NOT touch the daily ledger. It used to reserve budget
    before checking the live balance, so an operation refused for having no
    money still burned that day's allowance — a preflight that fails must cost
    nothing. Reserving is `claim_and_reserve_fund_operation`, called only once
    every check here has passed.
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

    # --- Per-operation cap. A read-only comparison; nothing is consumed. ---
    max_operation = positive_decimal(getattr(settings, "fci_max_operation_amount", 0))
    if max_operation is None:
        return "fci_limits_not_configured", audit
    audit["max_operation_amount"] = float(max_operation)
    if amount > max_operation:
        return "fci_operation_limit_exceeded", audit

    if operation.operation == OPERATION_SUBSCRIBE:
        # Only OTHER subscriptions compete for this cash. A pending redemption
        # is money coming IN and must not shrink the spendable balance.
        pending = pending_fund_subscriptions(
            db, currency=currency, exclude_id=operation.id
        )
        audit["pending_fund_subscriptions"] = float(pending)

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

    # --- Redemption: the live holding of THIS fund must cover it ---
    pending = pending_fund_redemptions(
        db, symbol=operation.fund_symbol, currency=currency, exclude_id=operation.id
    )
    audit["pending_fund_redemptions"] = float(pending)

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


def claim_and_reserve_fund_operation(
    db: Session, operation: FundOperation, *, settings
) -> tuple[str | None, dict]:
    """Claim the operation AND reserve its daily budget, together or not at all.

    This is the point of no return's doorway, and the two writes are coupled on
    purpose:

    - claim without reserve → an operation stuck in `submitting` that never
      gets sent and can never be retried;
    - reserve without claim → a day's allowance burned by an operation someone
      else is already submitting.

    Both happen inside ONE uncommitted transaction, so a failure of either
    rolls the other back. Concurrency is settled by the claim's conditional
    UPDATE (only one caller can move the row out of a resubmittable state) and
    by the ledger's own conditional UPDATE (the limit lives in the WHERE
    clause, so the arithmetic happens in the database).

    Returns (error_code, audit). None means: claimed, reserved, committed.
    """
    from app.services.execution_limits import reserve_daily_budget, trade_date_for

    amount = positive_decimal(operation.amount)
    currency = str(operation.currency or "").strip().upper()
    trade_date = trade_date_for(settings)
    ledger_class = fci_ledger_class(operation.operation)
    audit = {
        "trade_date": trade_date,
        "execution_class": ledger_class,
        "currency": currency,
    }
    if amount is None:
        return "invalid_fund_amount", audit

    claimed = (
        db.query(FundOperation)
        .filter(FundOperation.id == operation.id,
                FundOperation.status.notin_(sorted(NO_RESUBMIT_STATES)))
        .update({"status": STATE_SUBMITTING}, synchronize_session=False)
    )
    if claimed != 1:
        # Someone else got there first. Nothing was reserved: the ledger write
        # is below this line, not above it.
        db.rollback()
        return "fund_operation_already_submitted", {**audit, "claimed": False}
    audit["claimed"] = True

    reserve_error, reserve_audit = reserve_daily_budget(
        db,
        trade_date=trade_date,
        execution_class=ledger_class,
        currency=currency,
        notional=amount,
        max_daily_notional=getattr(settings, "fci_max_daily_amount", 0),
    )
    audit["daily_budget"] = reserve_audit
    if reserve_error:
        # Rolls back the claim too, so the operation does NOT stay in
        # `submitting` for a submission that will never happen.
        db.rollback()
        return reserve_error, {**audit, "reserved": False}

    # Persist WHICH row was incremented, in the same transaction. Recomputing
    # the key at release time is how a release after midnight decrements the
    # wrong day.
    db.add(FundBudgetReservation(
        fund_operation_id=operation.id,
        trade_date=trade_date,
        ledger_class=ledger_class,
        currency=currency,
        reserved_notional=float(amount),
        status=RESERVATION_RESERVED,
    ))
    audit["reserved"] = True
    return None, audit


RESERVATION_RESERVED = "reserved"
RESERVATION_RELEASED = "released"

# States that prove the operation reached — or may have reached — the broker.
# Budget for any of these is NEVER released automatically: giving it back
# would let a second operation spend allowance the first one may already have
# committed at IOL.
NON_RELEASABLE_STATES = frozenset({
    STATE_SUBMITTED, STATE_PENDING_CONFIRMATION, STATE_CONFIRMED,
    STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED,
})


def release_fund_budget(db: Session, operation: FundOperation, *, settings, reason: str) -> dict:
    """Give back budget for a submission that provably never left.

    Idempotent and self-identifying:

    - the row to decrement comes from the stored reservation, not from
      `trade_date_for(now)` — a release after midnight must not shrink today's
      allowance for money spent yesterday;
    - a second call returns `budget_already_released` and touches nothing.
      `submitted_notional` is spending authority, and decrementing it twice
      hands out allowance nobody gave back;
    - an operation that reached the broker is refused outright.

    `settings` is accepted for signature stability; the stored reservation is
    what decides, precisely so that current configuration cannot change where
    a past reservation is returned to.
    """
    from app.services.execution_limits import release_daily_budget

    reservation = (
        db.query(FundBudgetReservation)
        .filter(FundBudgetReservation.fund_operation_id == operation.id)
        .first()
    )
    if reservation is None:
        return {"released": False, "reason": "no_reservation_found"}
    if reservation.status == RESERVATION_RELEASED:
        return {
            "released": False,
            "reason": "budget_already_released",
            "released_at": reservation.released_at.isoformat()
            if reservation.released_at else None,
            "original_release_reason": reservation.release_reason,
        }
    if operation.status in NON_RELEASABLE_STATES:
        return {"released": False, "reason": "operation_reached_broker",
                "status": operation.status}

    result = release_daily_budget(
        db,
        trade_date=reservation.trade_date,
        execution_class=reservation.ledger_class,
        currency=reservation.currency,
        notional=Decimal(str(reservation.reserved_notional)),
        reason=reason,
    )
    reservation.status = RESERVATION_RELEASED
    reservation.released_at = _utcnow().replace(tzinfo=None)
    reservation.release_reason = str(reason)[:200]
    return {
        **result,
        "trade_date": reservation.trade_date,
        "execution_class": reservation.ledger_class,
        "reservation_id": reservation.id,
    }


# ---------------------------------------------------------------------------
# Operation lifecycle — creation and manual reconciliation
# ---------------------------------------------------------------------------

# Terminal outcomes a human may record. Deliberately no "resend" and no
# "retry": once a submission is ambiguous, the only way forward is to state
# what actually happened at IOL.
# Only an operation that reached — or may have reached — IOL can be
# reconciled. `submitting` is included because the claim succeeded: the POST
# may have been in flight when the process died.
RECONCILABLE_STATES = frozenset({
    STATE_SUBMITTING, STATE_SUBMITTED, STATE_PENDING_CONFIRMATION,
    STATE_SUBMISSION_UNKNOWN, STATE_RECONCILIATION_REQUIRED,
})

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

    # Being IN the catalog is not the same as being fit to operate. Reading a
    # fund read-only makes it visible; only an administrative verification
    # makes its cutoff, its minimum and its currency trustworthy enough to
    # time and size real money. An approvable operation on a `candidate` is
    # exactly the thing that looks safe and is not.
    blocked = fund_operability_blockers(fund, operation)
    if blocked:
        return _err(
            f"El fondo no está habilitado para operar: {blocked[0]}. "
            "Verificá el fondo con POST /api/funds/catalog/{symbol}/verify.",
            blocked[0], 409,
        )

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
    if previous not in RECONCILABLE_STATES:
        # Reconciling is DECLARING what IOL did. An operation that never left
        # this process has nothing at IOL to declare, so marking it
        # `confirmed` would invent a subscription that does not exist — and
        # the fabricated record would then reserve capacity and read as real
        # money in the portfolio.
        return _err(
            f"La operación está en '{previous}' y nunca llegó al broker. "
            "No hay nada que conciliar.",
            "fund_operation_never_submitted", 409,
        )

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
