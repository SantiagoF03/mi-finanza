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
IDENTITY_FIELDS = (
    "broker_symbol",
    "market",
    "settlement",
    "asset_type",
    "instrument_type",
    "currency",
    "country",
)


def normalize_symbol(symbol) -> str:
    return str(symbol or "").strip().upper()


def _utcnow_naive() -> datetime:
    """Naive UTC — every DB timestamp in this project is naive UTC."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def compute_identity_hash(identity: dict) -> str:
    """Stable hash over the identity fields only."""
    canonical = json.dumps(
        {field: str(identity.get(field) or "") for field in IDENTITY_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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

    details = {
        "broker_symbol": entry.broker_symbol,
        "execution_class": entry.execution_class or None,
        "execution_family": entry.execution_family or None,
        "verified_at": entry.verified_at.isoformat() if entry.verified_at else None,
        "stale_after": entry.stale_after.isoformat() if entry.stale_after else None,
        "active": bool(entry.active),
    }

    if not entry.active:
        return "instrument_inactive", details

    if not entry.execution_class or not entry.execution_family:
        return "instrument_class_unsupported", details

    missing = [f for f in ("market", "asset_type", "instrument_type", "currency")
               if not (getattr(entry, f, "") or "").strip()]
    if entry.execution_family == FAMILY_SECURITIES:
        # A security without a settlement term, tick or step cannot be priced
        # or sized safely — IOL rejects incompatible decimals outright.
        if not (entry.settlement or "").strip():
            missing.append("settlement")
        if _as_positive_float(entry.price_tick) is None:
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
) -> tuple[ExecutionInstrument, bool]:
    """Create or refresh ONE catalog entry.

    Returns (entry, identity_changed). When the raw identity hash changes the
    entry is NOT silently updated in place as if nothing happened: the caller
    gets identity_changed=True so it can require re-verification.
    """
    key = normalize_symbol(broker_symbol)
    execution_class = resolve_execution_class(asset_type)
    execution_family = execution_family_of(execution_class) or ""

    identity = {
        "broker_symbol": key,
        "market": str(market or "").strip(),
        "settlement": str(settlement or "").strip(),
        "asset_type": str(asset_type or "").strip(),
        "instrument_type": str(instrument_type or "").strip(),
        "currency": str(currency or "").strip(),
        "country": str(country or "").strip(),
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
        db.add(entry)
    else:
        identity_changed = entry.raw_identity_hash != identity_hash

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
    entry.quantity_step = _as_positive_float(quantity_step)
    entry.price_tick = _as_positive_float(price_tick)
    entry.minimum_quantity = _as_positive_float(minimum_quantity)
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
    entry.verified_at = now
    entry.stale_after = stale_after
    entry.raw_identity_hash = identity_hash
    entry.raw_identity = identity
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
            # Read-only discovery can establish identity and quoting, never
            # cancellation: no cancel contract has been verified for this app.
            buy_supported=family == FAMILY_SECURITIES,
            sell_supported=family == FAMILY_SECURITIES,
            quote_supported=family == FAMILY_SECURITIES,
            cancellation_supported=False,
            verified_at=verified_at,
            max_age_seconds=policy.get("catalog_max_age_seconds"),
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
