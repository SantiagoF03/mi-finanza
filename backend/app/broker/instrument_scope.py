"""Execution scope & instrument policies — explicit allowlist, fail closed.

A symbol appearing in a recommendation is NOT sufficient to trade it. Every
tradeable instrument needs an explicit policy declaring its identity
(asset/instrument type, currency), its venue (market, settlement) and its
hard per-symbol limits (quantity step, max quantity, max notional).

An empty policy map blocks sandbox and real execution entirely.
"""

from __future__ import annotations

import math

from app.broker.order_request import KNOWN_IOL_MARKETS, KNOWN_IOL_SETTLEMENTS

REQUIRED_POLICY_FIELDS = (
    "asset_type",
    "instrument_type",
    "currency",
    "market",
    "settlement",
    "quantity_step",
    # IOL's "alteración mínima": the limit price must be an exact multiple of
    # it. Required — without a known tick we cannot guarantee a price IOL
    # will accept, and it rejects incompatible decimals.
    "price_tick",
    "max_quantity",
    "max_notional",
)

_NUMERIC_FIELDS = ("quantity_step", "price_tick", "max_quantity", "max_notional")


def normalize_symbol(symbol) -> str:
    return str(symbol or "").strip().upper()


def _positive_finite(value) -> float | None:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num) or num <= 0:
        return None
    return num


def validate_instrument_policy(symbol, policy) -> tuple[dict | None, str | None]:
    """Validate ONE instrument policy.

    Returns (normalized_policy, error_code). Unknown fields are rejected so
    a typo can never silently change execution semantics.
    """
    sym = normalize_symbol(symbol)
    if not sym:
        return None, "instrument_policy_invalid"
    if not isinstance(policy, dict):
        return None, "instrument_policy_invalid"

    unknown = set(policy.keys()) - set(REQUIRED_POLICY_FIELDS)
    if unknown:
        return None, "instrument_policy_invalid"
    missing = [f for f in REQUIRED_POLICY_FIELDS if f not in policy]
    if missing:
        return None, "instrument_policy_invalid"

    asset_type = str(policy.get("asset_type") or "").strip()
    instrument_type = str(policy.get("instrument_type") or "").strip()
    currency = str(policy.get("currency") or "").strip()
    market = str(policy.get("market") or "").strip()
    settlement = str(policy.get("settlement") or "").strip()
    if not asset_type or not instrument_type or not currency:
        return None, "instrument_policy_invalid"
    if market not in KNOWN_IOL_MARKETS:
        return None, "instrument_policy_invalid"
    if settlement not in KNOWN_IOL_SETTLEMENTS:
        return None, "instrument_policy_invalid"

    numbers = {}
    for field in _NUMERIC_FIELDS:
        num = _positive_finite(policy.get(field))
        if num is None:
            return None, "instrument_policy_invalid"
        numbers[field] = num

    return {
        "symbol": sym,
        "asset_type": asset_type,
        "instrument_type": instrument_type,
        "currency": currency,
        "market": market,
        "settlement": settlement,
        **numbers,
    }, None


def load_instrument_policies(settings) -> tuple[dict[str, dict], list[str]]:
    """Parse and validate the whole policy map.

    Returns (policies_by_symbol, errors). Any invalid entry yields
    instrument_policy_invalid — the map is never partially trusted.
    """
    raw = settings.execution_instrument_policies or {}
    if not isinstance(raw, dict):
        return {}, ["instrument_policy_invalid"]
    if not raw:
        return {}, []

    policies: dict[str, dict] = {}
    errors: list[str] = []
    for symbol, policy in raw.items():
        normalized, error = validate_instrument_policy(symbol, policy)
        if error:
            if error not in errors:
                errors.append(error)
            continue
        policies[normalized["symbol"]] = normalized
    return policies, errors


def evaluate_order_scope(
    *,
    order: dict,
    position,
    policies: dict[str, dict],
    settings,
) -> tuple[dict, list[str]]:
    """Evaluate scope for ONE preview order.

    Returns (scope_info, blocking_codes). scope_info carries the signed
    instrument identity + per-symbol execution scope (empty dicts when the
    policy is unusable).
    """
    codes: list[str] = []
    symbol = normalize_symbol(order.get("symbol"))
    side = order.get("side")
    quantity = order.get("quantity_planned") or 0
    notional = order.get("estimated_notional") or 0

    if not policies:
        return {"instrument_identity": {}, "execution_scope": {}}, ["execution_scope_not_configured"]

    policy = policies.get(symbol)
    if not policy:
        return {"instrument_identity": {}, "execution_scope": {}}, ["instrument_policy_missing"]

    # Sell-only phase: buys are blocked outright (never converted, never ignored).
    if side == "buy" and settings.execution_sell_only:
        codes.append("buy_execution_disabled")

    # Global IOL policy and the per-symbol policy must agree exactly.
    if policy["market"] != (settings.iol_order_market or ""):
        codes.append("instrument_market_mismatch")
    if policy["settlement"] != (settings.iol_order_settlement or ""):
        codes.append("instrument_settlement_mismatch")

    # Identity must match the snapshot position (sells always need one).
    if side == "sell":
        if position is None:
            codes.append("live_position_missing")
        else:
            if normalize_symbol(getattr(position, "symbol", "")) != symbol:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "asset_type", "") or "") != policy["asset_type"]:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "instrument_type", "") or "") != policy["instrument_type"]:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "currency", "") or "") != policy["currency"]:
                codes.append("instrument_currency_mismatch")

    # Per-symbol hard limits.
    step = policy["quantity_step"]
    if quantity <= 0 or abs((quantity / step) - round(quantity / step)) > 1e-9:
        codes.append("quantity_step_mismatch")
    if quantity > policy["max_quantity"]:
        codes.append("symbol_quantity_limit_exceeded")
    if notional > policy["max_notional"]:
        codes.append("symbol_notional_limit_exceeded")

    scope_info = {
        "instrument_identity": {
            "symbol": symbol,
            "asset_type": policy["asset_type"],
            "instrument_type": policy["instrument_type"],
            "currency": policy["currency"],
            "market": policy["market"],
            "settlement": policy["settlement"],
        },
        "execution_scope": {
            "sell_only": bool(settings.execution_sell_only),
            "quantity_step": policy["quantity_step"],
            "price_tick": policy["price_tick"],
            "max_quantity": policy["max_quantity"],
            "max_notional": policy["max_notional"],
        },
    }
    # Dedup preserving order
    seen = set()
    deduped = [c for c in codes if not (c in seen or seen.add(c))]
    return scope_info, deduped
