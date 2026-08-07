"""Execution instrument catalog — the sole authority on tradeable identity.

A symbol appearing in a recommendation, in the analysis universe or even in
the live portfolio is NOT sufficient to trade it. It must have a catalog
entry that is complete, active and fresh. Anything else fails closed:

    instrument_catalog_missing      no entry at all
    instrument_catalog_incomplete   entry exists but lacks identity/tick/step
    instrument_catalog_stale        entry exists but its verification expired
    instrument_class_unsupported    entry maps to no execution class

The catalog is fed by READ-ONLY broker data (portfolio positions and the
discovery catalog). It is never fed by LLM output: a generated string must
never be able to declare a currency, a market or a price tick.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.broker.execution_class import (
    FAMILY_FUND,
    FAMILY_SECURITIES,
    execution_family_of,
    resolve_execution_class,
)
from app.models.models import ExecutionInstrument

# Fields that make up the instrument's *identity*. A change in any of them
# means the broker is describing a different thing under the same symbol, so
# the entry must be re-verified rather than silently reused.
#
# `settlement` is deliberately ABSENT. It is a POLICY choice — which plazo we
# choose to trade — not something the broker asserts about the instrument.
# Including it meant that switching the class policy from t1 to t0 flagged
# every instrument as `identity_changed`, freezing the whole catalog over a
# configuration change. Settlement is still carried in the policy, the
# preview, the preflight and the order request, and still validated against
# the class policy — it just does not define WHAT the instrument is.
IDENTITY_FIELDS = (
    "broker_symbol",
    "market",
    "asset_type",
    "instrument_type",
    "currency",
    "country",
)

# Bumped whenever IDENTITY_FIELDS changes. A hash computed under an older
# version is not comparable with a current one, so a version change must not
# masquerade as the broker changing an instrument's identity.
IDENTITY_HASH_VERSION = 2

# --- Verification lifecycle -------------------------------------------------
STATUS_CANDIDATE = "candidate"
STATUS_VERIFIED = "verified"
STATUS_IDENTITY_CHANGED = "identity_changed"
STATUS_REJECTED = "rejected"
STATUS_STALE = "stale"

VERIFICATION_STATUSES = (
    STATUS_CANDIDATE, STATUS_VERIFIED, STATUS_IDENTITY_CHANGED,
    STATUS_REJECTED, STATUS_STALE,
)

# --- Provenance -------------------------------------------------------------
# WHERE a field's value came from. This is the difference between "IOL told
# us this instrument trades in pesos" and "a class policy assumed pesos".
PROV_IOL_PORTFOLIO = "iol_portfolio"        # the account's own holdings
PROV_IOL_TITLE_DETAIL = "iol_title_detail"  # GET /api/v2/{mercado}/Titulos/{simbolo}
PROV_IOL_QUOTE = "iol_quote"                # .../Cotizacion responded
PROV_IOL_FCI = "iol_fci_catalog"            # GET /api/v2/Titulos/FCI
PROV_ADMIN_OVERRIDE = "admin_verified_override"
PROV_CLASS_DEFAULT = "class_policy_default"  # a GUESS, never a verification

# Provenances that count as actual verification of a field's value. A class
# default is deliberately absent: a policy may supply a LIMIT, but it has not
# looked at the instrument and must never claim it did.
VERIFYING_PROVENANCES = frozenset({
    PROV_IOL_PORTFOLIO,
    PROV_IOL_TITLE_DETAIL,
    PROV_IOL_QUOTE,
    PROV_IOL_FCI,
    PROV_ADMIN_OVERRIDE,
})

# Fields that must each carry a verifying provenance before a SECURITY may
# be executed. price_tick and quantity_step are here on purpose: IOL rejects
# an order whose decimals do not match the real minimum alteration, so a
# guessed tick is an order we know may be refused.
#
# `settlement` is deliberately NOT here. It is a POLICY choice — which plazo
# we choose to trade — not a property of the instrument that anyone could
# verify. Its provenance is still recorded, and the scope evaluator still
# checks it against the class policy and the global order policy; it simply
# cannot be "verified" the way a currency or a tick can.
REQUIRED_SECURITIES_PROVENANCE = (
    "broker_symbol", "asset_type", "instrument_type", "currency",
    "market", "price_tick", "quantity_step",
    "buy_supported", "sell_supported", "quote_supported",
)

REQUIRED_FUND_PROVENANCE = (
    "broker_symbol", "asset_type", "instrument_type", "currency",
)


def normalize_symbol(symbol) -> str:
    return str(symbol or "").strip().upper()


def _utcnow_naive() -> datetime:
    """Naive UTC — every DB timestamp in this project is naive UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def compute_identity_hash(identity: dict) -> str:
    """Stable hash over the identity fields only.

    The version is part of the hashed payload so two hashes computed under
    different field sets can never accidentally collide, and so a stored hash
    can be recognised as belonging to an older definition.
    """
    canonical = json.dumps(
        {
            "_v": IDENTITY_HASH_VERSION,
            **{field: str(identity.get(field) or "") for field in IDENTITY_FIELDS},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _identity_hash_version_of(entry: ExecutionInstrument) -> int:
    """Which identity definition an entry's stored hash was built with.

    Rows written before versioning carry no marker; they are version 1 (the
    definition that still included `settlement`).
    """
    stored = entry.raw_identity or {}
    if isinstance(stored, dict) and "_identity_hash_version" in stored:
        try:
            return int(stored["_identity_hash_version"])
        except (TypeError, ValueError):
            return 1
    return 1


def _identity_differs(stored: dict, proposed: dict) -> bool:
    """Compare identities by FIELD, independent of any hash version.

    Used when the stored hash predates the current definition: the fields
    themselves are the durable truth, the hash is only an index over them.
    """
    for field in IDENTITY_FIELDS:
        if str(stored.get(field) or "") != str(proposed.get(field) or ""):
            return True
    return False


def _as_positive_float(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num != num or num in (float("inf"), float("-inf")) or num <= 0:
        return None
    return num


def get_instrument(db: Session, symbol) -> ExecutionInstrument | None:
    key = normalize_symbol(symbol)
    if not key:
        return None
    return (
        db.query(ExecutionInstrument)
        .filter(ExecutionInstrument.broker_symbol == key)
        .first()
    )


def verification_status_of(entry: ExecutionInstrument) -> str:
    """Current verification status, treating NULL/empty as `candidate`.

    Rows written before verification existed (PR #138) have no status. They
    must read as `candidate` — NOT verified — because nobody ever checked
    them. That is the fail-closed direction.
    """
    raw = (getattr(entry, "verification_status", "") or "").strip()
    return raw if raw in VERIFICATION_STATUSES else STATUS_CANDIDATE


def required_provenance_fields(entry: ExecutionInstrument) -> tuple[str, ...]:
    if entry.execution_family == FAMILY_FUND:
        return REQUIRED_FUND_PROVENANCE
    return REQUIRED_SECURITIES_PROVENANCE


# Numeric fields a human can verify administratively, and which an automatic
# refresh can only ever supply from a class default. Their value and their
# provenance are preserved together — see `_apply_refreshed_field`.
ADMIN_PRESERVABLE_NUMERIC_FIELDS = ("quantity_step", "price_tick", "minimum_quantity")


def _admin_value_wins(current_origin, incoming_origin) -> bool:
    """Does the stored administrative value survive this incoming source?

    Only against a class default. That is the deliberate narrow rule:

    - `class_policy_default` never looked at the instrument, so it must not
      overwrite a value a human checked against real documentation;
    - a genuinely authoritative source (`iol_title_detail`, `iol_portfolio`,
      `iol_quote`, `iol_fci_catalog`) DOES overwrite it. If IOL now states a
      different minimum lot, the broker is telling us something the human's
      earlier reading no longer matches, and silently keeping the old number
      would be preferring a stale human note over the venue itself;
    - a later `admin_verified_override` overwrites it too — that is how an
      operator corrects their own earlier value.

    So the preserved case is exactly one: admin verified, refresh could only
    guess.
    """
    return current_origin == PROV_ADMIN_OVERRIDE and incoming_origin == PROV_CLASS_DEFAULT


def _apply_refreshed_field(entry: ExecutionInstrument, field: str, incoming,
                           provenance: dict) -> None:
    """Write a numeric field, unless a stored admin value outranks the source.

    Kept next to the provenance merge on purpose: these two decisions have to
    agree, and they previously did not.
    """
    current_origin = (entry.field_provenance or {}).get(field)
    incoming_origin = provenance.get(field)
    if _admin_value_wins(current_origin, incoming_origin):
        # Keep what the human verified — value AND label.
        return
    setattr(entry, field, _as_positive_float(incoming))


def has_usable_price_tick(entry: ExecutionInstrument) -> bool:
    """Is there SOME trustworthy way to price this instrument's decimals?

    Either a fixed tick, or a verified dynamic rule that actually covers this
    instrument. A rule stored on the row is not enough on its own: it must
    carry a verifying provenance AND apply to this family/class/market/
    currency. A class default that merely mentioned a rule can never satisfy
    this, which is the point — a default asserts nothing about the instrument.
    """
    if _as_positive_float(entry.price_tick) is not None:
        return True
    return verified_price_tick_rule(entry) is not None


def verified_price_tick_rule(entry: ExecutionInstrument) -> str | None:
    """The dynamic rule this instrument may be priced with, or None.

    Three conditions, all required: the rule is named, its provenance
    verifies, and its audited scope covers this instrument. Dropping any one
    of them is how a rule checked for argentine equities ends up pricing a
    bond.
    """
    from app.broker.price_rules import KNOWN_PRICE_TICK_RULES, price_tick_rule_applies

    rule = (getattr(entry, "price_tick_rule", None) or "").strip()
    if not rule or rule not in KNOWN_PRICE_TICK_RULES:
        return None
    provenance = entry.field_provenance or {}
    if provenance.get("price_tick_rule") not in VERIFYING_PROVENANCES:
        return None
    if not price_tick_rule_applies(
        rule,
        execution_family=entry.execution_family,
        execution_class=entry.execution_class,
        market=entry.market,
        currency=entry.currency,
    ):
        return None
    return rule


def unverified_fields(entry: ExecutionInstrument) -> list[str]:
    """Operative fields lacking a verifying provenance.

    A field whose provenance is `class_policy_default` counts as unverified:
    the policy supplied a number without ever looking at the instrument.
    """
    provenance = entry.field_provenance or {}
    missing = []
    for field in required_provenance_fields(entry):
        if field == "price_tick":
            # Satisfied by EITHER a verified fixed tick or a verified dynamic
            # rule — the requirement is "we can price the decimals", not "a
            # constant exists".
            if (provenance.get("price_tick") in VERIFYING_PROVENANCES
                    and _as_positive_float(entry.price_tick) is not None):
                continue
            if verified_price_tick_rule(entry) is not None:
                continue
            missing.append(field)
            continue

        source = provenance.get(field)
        if source not in VERIFYING_PROVENANCES:
            missing.append(field)
            continue

        # A provenance label certifies a VALUE. If the value is gone, the
        # label certifies nothing — and a row reading
        # `quantity_step=None, provenance=admin_verified_override, verified`
        # is exactly the incoherent state that let an unusable instrument
        # claim it was ready to trade.
        if (field in ADMIN_PRESERVABLE_NUMERIC_FIELDS
                and _as_positive_float(getattr(entry, field, None)) is None):
            missing.append(field)
    return sorted(missing)


def recompute_verification_status(entry: ExecutionInstrument) -> str:
    """Derive the status a freshly-refreshed entry deserves.

    Never promotes out of identity_changed or rejected: those are human
    decisions and an automatic refresh must not overwrite them.
    """
    current = verification_status_of(entry)
    if current in (STATUS_IDENTITY_CHANGED, STATUS_REJECTED):
        return current
    return STATUS_CANDIDATE if unverified_fields(entry) else STATUS_VERIFIED


def catalog_entry_status(
    entry: ExecutionInstrument | None,
    *,
    now: datetime | None = None,
    max_age_seconds: float | None = None,
) -> tuple[str | None, dict]:
    """Usability verdict for ONE catalog entry.

    Returns (blocking_code, details). blocking_code is None only when the
    entry is present, complete, active and fresh.
    """
    if entry is None:
        return "instrument_catalog_missing", {}

    current = now or _utcnow_naive()
    if current.tzinfo is not None:
        current = current.astimezone(timezone.utc).replace(tzinfo=None)

    provenance = entry.field_provenance or {}
    status = verification_status_of(entry)
    details = {
        "broker_symbol": entry.broker_symbol,
        "execution_class": entry.execution_class or None,
        "execution_family": entry.execution_family or None,
        "verified_at": entry.verified_at.isoformat() if entry.verified_at else None,
        "stale_after": entry.stale_after.isoformat() if entry.stale_after else None,
        "active": bool(entry.active),
        "verification_status": status,
        "field_provenance": dict(provenance),
    }

    if not entry.active:
        return "instrument_inactive", details

    if not entry.execution_class or not entry.execution_family:
        return "instrument_class_unsupported", details

    # An identity change freezes the instrument until a human rules on it.
    # Checked BEFORE completeness so the reason reported is the real one.
    if status == STATUS_IDENTITY_CHANGED:
        details["pending_identity"] = entry.pending_identity or {}
        details["previous_identity"] = entry.previous_identity or {}
        return "instrument_identity_changed", details
    if status == STATUS_REJECTED:
        return "instrument_rejected", details

    missing = [f for f in ("market", "asset_type", "instrument_type", "currency")
               if not (getattr(entry, f, "") or "").strip()]
    if entry.execution_family == FAMILY_SECURITIES:
        # A security without a settlement term, tick or step cannot be priced
        # or sized safely — IOL rejects incompatible decimals outright.
        if not (entry.settlement or "").strip():
            missing.append("settlement")
        # A tick may come from a fixed value OR from a verified dynamic rule.
        # Requiring a single fixed number would block every bCBA equity, whose
        # minimum alteration genuinely depends on the price — and would invite
        # storing one band's tick as if it were constant, which is the unsafe
        # answer this branch exists to avoid.
        if not has_usable_price_tick(entry):
            missing.append("price_tick")
        if _as_positive_float(entry.quantity_step) is None:
            missing.append("quantity_step")
    elif entry.execution_family == FAMILY_FUND:
        if _as_positive_float(entry.fund_minimum_amount) is None:
            missing.append("fund_minimum_amount")
        if not (entry.fund_cutoff_local_time or "").strip():
            missing.append("fund_cutoff_local_time")
    if missing:
        details["missing_fields"] = sorted(set(missing))
        return "instrument_catalog_incomplete", details

    if entry.verified_at is None:
        details["missing_fields"] = ["verified_at"]
        return "instrument_catalog_incomplete", details

    # Every operative field must carry a VERIFYING provenance. A value that
    # came from a class default is present but unverified — it makes the
    # entry a candidate, never an executable instrument.
    unverified = unverified_fields(entry)
    if unverified:
        details["unverified_fields"] = unverified
        return "instrument_not_verified", details

    if status != STATUS_VERIFIED:
        return "instrument_not_verified", details

    # Freshness: the entry's own stale_after, plus the class-level maximum
    # age. The stricter of the two wins — a long-lived entry can never
    # outlive the policy's catalog_max_age.
    if entry.stale_after is not None and current > entry.stale_after:
        return "instrument_catalog_stale", details
    if max_age_seconds is not None:
        age = (current - entry.verified_at).total_seconds()
        details["age_seconds"] = age
        if age > max_age_seconds:
            return "instrument_catalog_stale", details

    return None, details


def instrument_identity(entry: ExecutionInstrument) -> dict:
    """The signed identity fragment for an order preview."""
    return {
        "symbol": entry.broker_symbol,
        "display_symbol": entry.display_symbol or entry.broker_symbol,
        "asset_type": entry.asset_type,
        "instrument_type": entry.instrument_type,
        "currency": entry.currency,
        "market": entry.market,
        "settlement": entry.settlement,
        "execution_class": entry.execution_class,
        "execution_family": entry.execution_family,
        "raw_identity_hash": entry.raw_identity_hash,
    }


def upsert_instrument(
    db: Session,
    *,
    broker_symbol,
    asset_type,
    instrument_type,
    currency,
    market,
    settlement,
    source: str,
    display_symbol=None,
    description: str = "",
    country: str = "argentina",
    quantity_step=None,
    price_tick=None,
    minimum_quantity=None,
    buy_supported: bool = False,
    sell_supported: bool = False,
    quote_supported: bool = False,
    cancellation_supported: bool = False,
    fund_minimum_amount=None,
    fund_cutoff_local_time=None,
    settlement_delay_days=None,
    verified_at: datetime | None = None,
    max_age_seconds: float | None = None,
    active: bool = True,
    provenance: dict | None = None,
) -> tuple[ExecutionInstrument, bool]:
    """Create or refresh ONE catalog entry.

    `provenance` maps each field to WHERE its value came from. It is what
    separates a verified instrument from a plausible guess: a field whose
    provenance is `class_policy_default` keeps the entry a `candidate`, and a
    candidate can never authorise an order.

    Returns (entry, identity_changed). On an identity change the entry is NOT
    overwritten and NOT re-verified — it is frozen as `identity_changed` for
    a human to accept or reject.
    """
    key = normalize_symbol(broker_symbol)
    execution_class = resolve_execution_class(asset_type)
    execution_family = execution_family_of(execution_class) or ""

    identity = {
        "broker_symbol": key,
        "market": str(market or "").strip(),
        "asset_type": str(asset_type or "").strip(),
        "instrument_type": str(instrument_type or "").strip(),
        "currency": str(currency or "").strip(),
        "country": str(country or "").strip(),
        # Recorded for audit and for the preflight, but NOT part of identity:
        # switching the class policy from t1 to t0 is a configuration change,
        # not the broker describing a different instrument.
        "settlement": str(settlement or "").strip(),
        "_identity_hash_version": IDENTITY_HASH_VERSION,
    }
    identity_hash = compute_identity_hash(identity)

    now = verified_at or _utcnow_naive()
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc).replace(tzinfo=None)
    stale_after = now + timedelta(seconds=max_age_seconds) if max_age_seconds else None

    entry = get_instrument(db, key)
    identity_changed = False
    if entry is None:
        entry = ExecutionInstrument(broker_symbol=key)
        entry.field_provenance = {}
        entry.verification_audit = []
        db.add(entry)
    else:
        # An entry that has never been given an identity hash (a fresh row,
        # or a PR #138 row) is not "changed" — it is simply being populated.
        stored_hash = entry.raw_identity_hash or ""
        if not stored_hash:
            identity_changed = False
        elif _identity_hash_version_of(entry) != IDENTITY_HASH_VERSION:
            # The hash was computed under a DIFFERENT field set (it used to
            # include settlement). Comparing across versions would mark every
            # instrument as changed for a reason that has nothing to do with
            # the broker. Re-derive the comparison from the stored identity
            # fields themselves, which are version-independent.
            identity_changed = _identity_differs(entry.raw_identity or {}, identity)
        else:
            identity_changed = stored_hash != identity_hash

    if identity_changed:
        # DO NOT overwrite and re-verify. The broker is now describing a
        # different thing under the same symbol — different currency, market,
        # settlement or type. Silently absorbing that is how an order ends up
        # priced in the wrong currency. Freeze it and let a human rule.
        entry.previous_identity = dict(entry.raw_identity or {})
        entry.pending_identity = dict(identity)
        entry.verification_status = STATUS_IDENTITY_CHANGED
        entry.identity_changed_at = now
        audit = list(entry.verification_audit or [])
        audit.append({
            "timestamp": now.isoformat(),
            "event": "identity_change_detected",
            "previous": dict(entry.raw_identity or {}),
            "proposed": dict(identity),
            "source": str(source or "")[:50],
        })
        entry.verification_audit = audit
        # verified_at deliberately NOT touched: nothing was verified here.
        db.flush()
        return entry, True

    entry.display_symbol = normalize_symbol(display_symbol) or key
    entry.description = str(description or "")[:200]
    entry.country = identity["country"]
    entry.market = identity["market"]
    entry.settlement = identity["settlement"]
    entry.asset_type = identity["asset_type"]
    entry.instrument_type = identity["instrument_type"]
    entry.currency = identity["currency"]
    entry.execution_class = execution_class or ""
    entry.execution_family = execution_family
    # A value and its provenance are ONE unit. Writing the value here and
    # merging provenance afterwards is what produced the bug this fixes: the
    # refresh overwrote an admin-verified `quantity_step` with the class
    # default's None, the later merge kept the `admin_verified_override`
    # LABEL, and the row ended up claiming a verified value it no longer had.
    for field, incoming in (
        ("quantity_step", quantity_step),
        ("price_tick", price_tick),
        ("minimum_quantity", minimum_quantity),
    ):
        _apply_refreshed_field(entry, field, incoming, provenance or {})
    entry.active = bool(active)
    entry.buy_supported = bool(buy_supported)
    entry.sell_supported = bool(sell_supported)
    entry.quote_supported = bool(quote_supported)
    entry.cancellation_supported = bool(cancellation_supported)
    entry.fund_minimum_amount = _as_positive_float(fund_minimum_amount)
    entry.fund_cutoff_local_time = (str(fund_cutoff_local_time).strip()
                                    if fund_cutoff_local_time else None)
    entry.settlement_delay_days = (int(settlement_delay_days)
                                   if isinstance(settlement_delay_days, int) else None)
    entry.source = str(source or "")[:50]
    entry.stale_after = stale_after
    entry.raw_identity_hash = identity_hash
    entry.raw_identity = identity

    # Merge provenance: an ADMIN-verified field is never demoted by a later
    # automatic refresh that could only offer a class default for it.
    #
    # The VALUES for the numeric fields above were already merged under the
    # same rule by `_apply_refreshed_field`, so the two can no longer diverge.
    # Every other field's value is written unconditionally and its provenance
    # follows the same rule here.
    merged = dict(entry.field_provenance or {})
    for field, origin in (provenance or {}).items():
        if _admin_value_wins(merged.get(field), origin):
            continue
        merged[field] = origin
    entry.field_provenance = merged

    entry.verification_status = recompute_verification_status(entry)
    # verified_at means "this is when the VERIFIED values were established".
    # A candidate has nothing verified, so it must not carry a fresh stamp
    # that would later read as a recent verification.
    if entry.verification_status == STATUS_VERIFIED:
        entry.verified_at = now
    elif entry.verified_at is None:
        entry.verified_at = now
    db.flush()
    return entry, identity_changed


def refresh_catalog_from_positions(
    db: Session,
    positions: list[dict],
    *,
    settings,
    source: str = "iol_portfolio",
    verified_at: datetime | None = None,
) -> dict:
    """Feed the catalog from READ-ONLY live portfolio positions.

    The portfolio is the most trustworthy identity source available without
    an order: it is the broker itself stating asset type, instrument type and
    currency for a symbol the account actually holds.

    Tick and step cannot be derived from a position, so they come from the
    class policy defaults (or a per-symbol override). Without them the entry
    is still written, but it stays `instrument_catalog_incomplete` and cannot
    trade — visible instead of invisible.
    """
    from app.broker.execution_class import load_class_policies

    class_policies, policy_errors = load_class_policies(settings)
    ticks = _symbol_tick_overrides(settings)

    created = 0
    updated = 0
    skipped: list[dict] = []
    identity_changes: list[str] = []

    for position in positions or []:
        if not isinstance(position, dict):
            continue
        symbol = normalize_symbol(position.get("symbol"))
        if not symbol:
            continue
        asset_type = position.get("asset_type") or position.get("instrument_type")
        execution_class = resolve_execution_class(asset_type)
        if not execution_class:
            skipped.append({"symbol": symbol, "reason": "instrument_class_unsupported",
                            "asset_type": str(asset_type or "")})
            continue

        policy = class_policies.get(execution_class)
        if not policy:
            skipped.append({"symbol": symbol, "reason": "class_policy_not_configured",
                            "execution_class": execution_class})
            continue

        family = execution_family_of(execution_class)
        settlement = policy["settlements"][0] if policy.get("settlements") else ""
        market = policy["markets"][0] if policy.get("markets") else ""
        override = ticks.get(symbol) or {}

        # PROVENANCE, honestly recorded.
        #
        # The portfolio genuinely establishes identity: it is IOL stating
        # what the account holds. It establishes NOTHING about market,
        # settlement, tick, step or capabilities — those come from the class
        # policy, i.e. they are assumptions until verified elsewhere.
        provenance = {
            "broker_symbol": PROV_IOL_PORTFOLIO,
            "asset_type": PROV_IOL_PORTFOLIO,
            "instrument_type": PROV_IOL_PORTFOLIO,
            "currency": PROV_IOL_PORTFOLIO,
            "market": PROV_CLASS_DEFAULT,
            "settlement": PROV_CLASS_DEFAULT,
            "price_tick": (
                PROV_ADMIN_OVERRIDE if "price_tick" in override else PROV_CLASS_DEFAULT
            ),
            "quantity_step": (
                PROV_ADMIN_OVERRIDE if "quantity_step" in override else PROV_CLASS_DEFAULT
            ),
            "minimum_quantity": (
                PROV_ADMIN_OVERRIDE if "minimum_quantity" in override else PROV_CLASS_DEFAULT
            ),
            # Capabilities are NOT inferred from the execution family. Holding
            # something proves nothing about being able to buy, sell or quote
            # it — that needs evidence, gathered by resolve_instrument().
            "buy_supported": PROV_CLASS_DEFAULT,
            "sell_supported": PROV_CLASS_DEFAULT,
            "quote_supported": PROV_CLASS_DEFAULT,
        }

        existed = get_instrument(db, symbol) is not None
        _, identity_changed = upsert_instrument(
            db,
            broker_symbol=symbol,
            asset_type=asset_type,
            instrument_type=position.get("instrument_type") or asset_type,
            currency=position.get("currency"),
            market=market,
            settlement=settlement,
            source=source,
            description=str(position.get("description") or ""),
            quantity_step=override.get("quantity_step", policy.get("default_quantity_step")),
            price_tick=override.get("price_tick", policy.get("default_price_tick")),
            minimum_quantity=override.get("minimum_quantity",
                                          policy.get("default_quantity_step")),
            # Holding an instrument is not evidence that it can be bought,
            # sold or quoted. These stay false until resolve_instrument()
            # actually observes a quote and a title detail.
            buy_supported=False,
            sell_supported=False,
            quote_supported=False,
            cancellation_supported=False,
            verified_at=verified_at,
            max_age_seconds=policy.get("catalog_max_age_seconds"),
            provenance=provenance,
        )
        if identity_changed:
            identity_changes.append(symbol)
        if existed:
            updated += 1
        else:
            created += 1

    db.commit()
    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "identity_changed": sorted(identity_changes),
        "class_policy_errors": policy_errors,
        "source": source,
    }


def _symbol_tick_overrides(settings) -> dict[str, dict]:
    """Per-symbol tick/step, for instruments whose minimum alteration differs
    from the class default. Purely numeric — never identity."""
    raw = getattr(settings, "execution_instrument_ticks", None) or {}
    if not isinstance(raw, dict):
        return {}
    result: dict[str, dict] = {}
    for symbol, spec in raw.items():
        key = normalize_symbol(symbol)
        if not key or not isinstance(spec, dict):
            continue
        allowed = {"price_tick", "quantity_step", "minimum_quantity"}
        if set(spec.keys()) - allowed:
            continue
        cleaned = {k: v for k, v in spec.items() if _as_positive_float(v) is not None}
        if cleaned:
            result[key] = cleaned
    return result


def list_catalog(db: Session) -> list[ExecutionInstrument]:
    return (
        db.query(ExecutionInstrument)
        .order_by(ExecutionInstrument.broker_symbol)
        .all()
    )


def catalog_to_dict(entry: ExecutionInstrument) -> dict:
    """Non-sensitive projection of a catalog entry."""
    return {
        "broker_symbol": entry.broker_symbol,
        "display_symbol": entry.display_symbol or entry.broker_symbol,
        "description": entry.description or "",
        "country": entry.country or "",
        "market": entry.market or "",
        "settlement": entry.settlement or "",
        "asset_type": entry.asset_type or "",
        "instrument_type": entry.instrument_type or "",
        "execution_family": entry.execution_family or "",
        "execution_class": entry.execution_class or "",
        "currency": entry.currency or "",
        "quantity_step": entry.quantity_step,
        "price_tick": entry.price_tick,
        "minimum_quantity": entry.minimum_quantity,
        "active": bool(entry.active),
        "buy_supported": bool(entry.buy_supported),
        "sell_supported": bool(entry.sell_supported),
        "quote_supported": bool(entry.quote_supported),
        "cancellation_supported": bool(entry.cancellation_supported),
        "fund_minimum_amount": entry.fund_minimum_amount,
        "fund_cutoff_local_time": entry.fund_cutoff_local_time,
        "settlement_delay_days": entry.settlement_delay_days,
        "source": entry.source or "",
        "verified_at": entry.verified_at.isoformat() if entry.verified_at else None,
        "stale_after": entry.stale_after.isoformat() if entry.stale_after else None,
        "raw_identity_hash": entry.raw_identity_hash or "",
    }


# ---------------------------------------------------------------------------
# Identity-change decisions — explicit, human, auditable.
# ---------------------------------------------------------------------------


def accept_identity_change(
    entry: ExecutionInstrument, *, note: str = "", now: datetime | None = None
) -> dict:
    """A human accepts the broker's new identity for this symbol.

    The proposed identity becomes the entry's identity, but the entry drops
    back to `candidate`, NOT `verified`: accepting that IOL now calls this a
    dollar-denominated CEDEAR is not the same as having re-verified its tick,
    step and capabilities under that new identity.
    """
    stamp = now or _utcnow_naive()
    proposed = dict(entry.pending_identity or {})
    if not proposed:
        return {"changed": False, "reason": "no_pending_identity"}

    entry.market = proposed.get("market", entry.market)
    entry.settlement = proposed.get("settlement", entry.settlement)
    entry.asset_type = proposed.get("asset_type", entry.asset_type)
    entry.instrument_type = proposed.get("instrument_type", entry.instrument_type)
    entry.currency = proposed.get("currency", entry.currency)
    entry.country = proposed.get("country", entry.country)

    execution_class = resolve_execution_class(entry.asset_type)
    entry.execution_class = execution_class or ""
    entry.execution_family = execution_family_of(execution_class) or ""

    entry.raw_identity = proposed
    entry.raw_identity_hash = compute_identity_hash(proposed)
    entry.previous_identity = None
    entry.pending_identity = None
    entry.identity_changed_at = None

    # The new identity invalidates every capability and numeric field that
    # was verified under the OLD one.
    entry.buy_supported = False
    entry.sell_supported = False
    entry.quote_supported = False
    provenance = dict(entry.field_provenance or {})
    for field in ("price_tick", "quantity_step", "minimum_quantity",
                  "buy_supported", "sell_supported", "quote_supported"):
        provenance[field] = PROV_CLASS_DEFAULT
    entry.field_provenance = provenance
    entry.verification_status = STATUS_CANDIDATE

    audit = list(entry.verification_audit or [])
    audit.append({
        "timestamp": stamp.isoformat(),
        "event": "identity_change_accepted",
        "accepted": proposed,
        "note": note[:300],
        "resulting_status": STATUS_CANDIDATE,
        "source": "manual_user",
    })
    entry.verification_audit = audit
    return {"changed": True, "verification_status": STATUS_CANDIDATE}


def reject_identity_change(
    entry: ExecutionInstrument, *, note: str = "", now: datetime | None = None
) -> dict:
    """A human refuses the broker's new identity. The instrument is parked.

    Deliberately NOT "keep the old identity and carry on": the broker is
    telling us something we do not accept, so trading it on our old
    assumption would be the worst of both. It becomes `rejected` and is
    unusable until someone re-resolves it.
    """
    stamp = now or _utcnow_naive()
    audit = list(entry.verification_audit or [])
    audit.append({
        "timestamp": stamp.isoformat(),
        "event": "identity_change_rejected",
        "refused": dict(entry.pending_identity or {}),
        "note": note[:300],
        "source": "manual_user",
    })
    entry.verification_audit = audit
    entry.verification_status = STATUS_REJECTED
    entry.pending_identity = None
    entry.identity_changed_at = None
    return {"changed": True, "verification_status": STATUS_REJECTED}


def verify_fields(
    entry: ExecutionInstrument,
    *,
    fields: dict,
    provenance_source: str,
    note: str = "",
    now: datetime | None = None,
) -> dict:
    """Administratively verify specific numeric/capability fields.

    This is the ONLY way a price_tick or quantity_step becomes verified
    without official IOL metadata: a named human states the value and it is
    recorded with `admin_verified_override` provenance plus an audit entry.
    A class default can never reach this state on its own.
    """
    from app.broker.price_rules import KNOWN_PRICE_TICK_RULES, price_tick_rule_applies

    stamp = now or _utcnow_naive()
    applied = {}
    for field, value in (fields or {}).items():
        if field == "price_tick_rule":
            # Only rules this build actually implements. Arbitrary text would
            # persist a rule nothing can evaluate, and the instrument would
            # read as verified while no code could price it.
            rule = str(value or "").strip()
            if rule not in KNOWN_PRICE_TICK_RULES:
                return {"changed": False, "reason": "unknown_price_tick_rule"}
            # And it must cover THIS instrument. A rule audited for argentine
            # equities in pesos on bCBA does not become correct for a bond
            # because someone typed its name.
            if not price_tick_rule_applies(
                rule,
                execution_family=entry.execution_family,
                execution_class=entry.execution_class,
                market=entry.market,
                currency=entry.currency,
            ):
                return {"changed": False, "reason": "price_tick_rule_not_applicable"}
            entry.price_tick_rule = rule
            entry.price_tick_rule_verified_at = stamp
            applied[field] = rule
        elif field in ("price_tick", "quantity_step", "minimum_quantity"):
            parsed = _as_positive_float(value)
            if parsed is None:
                return {"changed": False, "reason": f"invalid_{field}"}
            setattr(entry, field, parsed)
            applied[field] = parsed
        elif field in ("buy_supported", "sell_supported", "quote_supported",
                       "cancellation_supported"):
            if not isinstance(value, bool):
                return {"changed": False, "reason": f"invalid_{field}"}
            setattr(entry, field, value)
            applied[field] = value
        else:
            return {"changed": False, "reason": "unsupported_field"}

    provenance = dict(entry.field_provenance or {})
    for field in applied:
        provenance[field] = provenance_source
    entry.field_provenance = provenance

    audit = list(entry.verification_audit or [])
    audit.append({
        "timestamp": stamp.isoformat(),
        "event": "fields_verified",
        "fields": applied,
        "provenance": provenance_source,
        "note": note[:300],
        "source": "manual_user",
    })
    entry.verification_audit = audit
    entry.verification_status = recompute_verification_status(entry)
    if entry.verification_status == STATUS_VERIFIED:
        entry.verified_at = stamp
    return {
        "changed": True,
        "applied": applied,
        "verification_status": entry.verification_status,
        "unverified_fields": unverified_fields(entry),
    }
