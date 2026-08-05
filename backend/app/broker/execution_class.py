"""Execution classes, class policies, per-symbol overrides and denylist.

Replaces "one hand-written policy per symbol" with:

1. a policy per CLASS (ACCIONES, CEDEARS, FCI) — the scalable part;
2. per-symbol OVERRIDES that may only tighten, never loosen;
3. an explicit DENYLIST that always wins;
4. the execution catalog as the sole source of instrument identity.

Nothing here reads the network and nothing here can send an order. A class
with no configured policy is not "unlimited" — it is unusable.
"""

from __future__ import annotations

import math

from app.broker.order_request import (
    KNOWN_IOL_MARKETS,
    KNOWN_IOL_SETTLEMENTS,
    SUPPORTED_ORDER_TYPES,
)

# --- Execution classes ------------------------------------------------------
CLASS_ACCIONES = "ACCIONES"
CLASS_CEDEARS = "CEDEARS"
CLASS_FCI = "FCI"

KNOWN_EXECUTION_CLASSES = (CLASS_ACCIONES, CLASS_CEDEARS, CLASS_FCI)

# --- Execution families -----------------------------------------------------
# The family decides WHICH contract applies. A fund is never priced, never
# has a limit price and never has a book, so it can never travel through the
# securities path.
FAMILY_SECURITIES = "securities"
FAMILY_FUND = "fund"

CLASS_FAMILY = {
    CLASS_ACCIONES: FAMILY_SECURITIES,
    CLASS_CEDEARS: FAMILY_SECURITIES,
    CLASS_FCI: FAMILY_FUND,
}

# Map the broker's normalized asset_type (app.broker.clients._normalize_asset_type)
# onto an execution class. Anything not listed here has NO execution class and
# is therefore not tradeable by this application — bonds, ONs, letras and ETFs
# are deliberately absent: they were never audited for execution.
ASSET_TYPE_TO_CLASS = {
    "ACCIONES": CLASS_ACCIONES,
    "CEDEAR": CLASS_CEDEARS,
    "FondoComundeInversion": CLASS_FCI,
}


def resolve_execution_class(asset_type) -> str | None:
    """Execution class for a normalized asset type, or None if out of scope."""
    return ASSET_TYPE_TO_CLASS.get(str(asset_type or "").strip())


def execution_family_of(execution_class) -> str | None:
    return CLASS_FAMILY.get(str(execution_class or "").strip())


# --- Class policy schema ----------------------------------------------------
# Every field is REQUIRED. A partially specified class policy is invalid, not
# "defaulted": a missing limit must never be read as an absent limit.
_BOOL_FIELDS = ("buy_enabled", "sell_enabled")
_LIST_FIELDS = ("currencies", "markets", "settlements")
_POSITIVE_FIELDS = (
    "max_order_notional",
    "max_daily_notional",
    "max_quantity",
    "max_portfolio_pct",
    "max_quote_age_seconds",
    "max_price_deviation_pct",
    "catalog_max_age_seconds",
    "validity_minutes",
)
# Zero is meaningful for these (no reserve / no buffer is a real choice), so
# they are validated as >= 0 instead of > 0.
_NON_NEGATIVE_FIELDS = ("min_cash_reserve", "fee_buffer_pct")
_STRING_FIELDS = ("order_type",)

# Class-wide fallbacks for tick and step. NULL is a legitimate value and means
# "this class asserts nothing about tick/step" — which is the honest answer,
# because a class-wide tick is a claim about every one of its instruments.
#
# A null here is not a loophole: the fallback is recorded with the
# `class_policy_default` provenance, which does NOT verify, so an instrument
# whose tick comes from here stays unverified and cannot trade. The verified
# catalog remains the authority; these only ever fill a gap that still blocks.
_OPTIONAL_POSITIVE_FIELDS = ("default_quantity_step", "default_price_tick")

REQUIRED_CLASS_POLICY_FIELDS = (
    _BOOL_FIELDS + _LIST_FIELDS + _POSITIVE_FIELDS + _OPTIONAL_POSITIVE_FIELDS
    + _NON_NEGATIVE_FIELDS + _STRING_FIELDS
)

# Overrides may address any of these. Booleans may only be turned OFF and
# numbers may only be lowered — see apply_override().
OVERRIDABLE_FIELDS = (
    _BOOL_FIELDS
    + (
        "max_order_notional",
        "max_daily_notional",
        "max_quantity",
        "max_portfolio_pct",
        "max_quote_age_seconds",
        "max_price_deviation_pct",
        "catalog_max_age_seconds",
        "validity_minutes",
    )
    + ("min_cash_reserve", "fee_buffer_pct")
)

# min_cash_reserve and fee_buffer_pct are protective: tightening them means
# raising them, not lowering them.
_TIGHTEN_BY_RAISING = {"min_cash_reserve", "fee_buffer_pct"}


def _positive_finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num) or num <= 0:
        return None
    return num


def _non_negative_finite(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(num) or num < 0:
        return None
    return num


def _clean_upper_list(value) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    items = []
    for raw in value:
        text = str(raw or "").strip()
        if not text:
            return None
        items.append(text)
    return items or None


def validate_class_policy(class_name, policy) -> tuple[dict | None, str | None]:
    """Validate ONE class policy. Unknown fields are rejected so a typo can
    never silently change execution semantics."""
    name = str(class_name or "").strip().upper()
    if name not in KNOWN_EXECUTION_CLASSES:
        return None, "unknown_execution_class"
    if not isinstance(policy, dict):
        return None, "class_policy_invalid"

    unknown = set(policy.keys()) - set(REQUIRED_CLASS_POLICY_FIELDS)
    if unknown:
        return None, "class_policy_invalid"
    missing = [f for f in REQUIRED_CLASS_POLICY_FIELDS if f not in policy]
    if missing:
        return None, "class_policy_incomplete"

    resolved: dict = {"execution_class": name, "execution_family": CLASS_FAMILY[name]}

    for field in _BOOL_FIELDS:
        value = policy.get(field)
        if not isinstance(value, bool):
            return None, "class_policy_invalid"
        resolved[field] = value

    for field in _LIST_FIELDS:
        items = _clean_upper_list(policy.get(field))
        if items is None:
            return None, "class_policy_invalid"
        resolved[field] = items

    for field in _POSITIVE_FIELDS:
        num = _positive_finite(policy.get(field))
        if num is None:
            return None, "class_policy_invalid"
        resolved[field] = num

    for field in _NON_NEGATIVE_FIELDS:
        num = _non_negative_finite(policy.get(field))
        if num is None:
            return None, "class_policy_invalid"
        resolved[field] = num

    for field in _OPTIONAL_POSITIVE_FIELDS:
        raw_value = policy.get(field)
        if raw_value is None:
            # Explicitly "not asserted". Distinct from a bad value: a wrong
            # number would be worse than no number, so 0 and negatives are
            # still rejected.
            resolved[field] = None
            continue
        num = _positive_finite(raw_value)
        if num is None:
            return None, "class_policy_invalid"
        resolved[field] = num

    order_type = str(policy.get("order_type") or "").strip()
    if CLASS_FAMILY[name] == FAMILY_SECURITIES and order_type not in SUPPORTED_ORDER_TYPES:
        return None, "class_policy_invalid"
    resolved["order_type"] = order_type

    # Venue values must be ones this project actually knows. We never invent
    # a market or settlement term IOL has not documented for us.
    if CLASS_FAMILY[name] == FAMILY_SECURITIES:
        if any(m not in KNOWN_IOL_MARKETS for m in resolved["markets"]):
            return None, "class_policy_invalid"
        if any(s not in KNOWN_IOL_SETTLEMENTS for s in resolved["settlements"]):
            return None, "class_policy_invalid"

    # A percentage limit above 1.0 is almost certainly a unit mistake
    # (2 meaning "2%"), and it would authorise twice the portfolio.
    for field in ("max_portfolio_pct", "max_price_deviation_pct", "fee_buffer_pct"):
        if resolved[field] > 1:
            return None, "class_policy_invalid"

    return resolved, None


def load_class_policies(settings) -> tuple[dict[str, dict], list[str]]:
    """Parse and validate the whole class-policy map.

    Returns (policies_by_class, error_codes). One invalid entry never yields a
    partially trusted map: the offending class is dropped and reported.
    """
    raw = getattr(settings, "execution_class_policies", None) or {}
    if not isinstance(raw, dict):
        return {}, ["class_policy_invalid"]
    if not raw:
        return {}, []

    policies: dict[str, dict] = {}
    errors: list[str] = []
    for class_name, policy in raw.items():
        resolved, error = validate_class_policy(class_name, policy)
        if error:
            if error not in errors:
                errors.append(error)
            continue
        policies[resolved["execution_class"]] = resolved
    return policies, errors


def load_instrument_overrides(settings) -> tuple[dict[str, dict], list[str]]:
    """Per-symbol overrides. Values are only validated for shape here; the
    tighten-only rule is enforced in apply_override()."""
    raw = getattr(settings, "execution_instrument_overrides", None) or {}
    if not isinstance(raw, dict):
        return {}, ["instrument_override_invalid"]
    if not raw:
        return {}, []

    overrides: dict[str, dict] = {}
    errors: list[str] = []
    for symbol, override in raw.items():
        key = str(symbol or "").strip().upper()
        if not key or not isinstance(override, dict):
            if "instrument_override_invalid" not in errors:
                errors.append("instrument_override_invalid")
            continue
        unknown = set(override.keys()) - set(OVERRIDABLE_FIELDS)
        if unknown:
            if "instrument_override_invalid" not in errors:
                errors.append("instrument_override_invalid")
            continue
        overrides[key] = dict(override)
    return overrides, errors


def load_denylist(settings) -> set[str]:
    raw = getattr(settings, "execution_denylist", None) or []
    if isinstance(raw, str):
        raw = [part for part in raw.split(",")]
    return {str(item or "").strip().upper() for item in raw if str(item or "").strip()}


def apply_override(policy: dict, override: dict | None, *, allow_increase: bool) -> tuple[dict, list[str]]:
    """Apply a per-symbol override to a class policy.

    Tighten-only by default:
    - a boolean may only go True → False (never False → True);
    - a limit may only be LOWERED (or raised, for the protective
      min_cash_reserve / fee_buffer_pct);
    - anything that would loosen the class policy is rejected with
      `override_increases_limit` and the class value is kept.

    `allow_increase` (EXECUTION_ALLOW_OVERRIDE_LIMIT_INCREASE, default false)
    is the explicit administrative escape hatch demanded by the spec: without
    it, no override can ever widen a global limit.
    """
    resolved = dict(policy)
    codes: list[str] = []
    if not override:
        return resolved, codes

    for field, raw_value in override.items():
        if field not in OVERRIDABLE_FIELDS:
            codes.append("instrument_override_invalid")
            continue

        if field in _BOOL_FIELDS:
            if not isinstance(raw_value, bool):
                codes.append("instrument_override_invalid")
                continue
            if raw_value and not resolved[field] and not allow_increase:
                # Re-enabling something the class disabled is a widening.
                codes.append("override_increases_limit")
                continue
            resolved[field] = raw_value if allow_increase else (resolved[field] and raw_value)
            continue

        if field in _TIGHTEN_BY_RAISING:
            num = _non_negative_finite(raw_value)
            if num is None:
                codes.append("instrument_override_invalid")
                continue
            if num < resolved[field] and not allow_increase:
                codes.append("override_increases_limit")
                continue
            resolved[field] = num if allow_increase else max(resolved[field], num)
            continue

        num = _positive_finite(raw_value)
        if num is None:
            codes.append("instrument_override_invalid")
            continue
        if num > resolved[field] and not allow_increase:
            codes.append("override_increases_limit")
            continue
        resolved[field] = num if allow_increase else min(resolved[field], num)

    seen: set[str] = set()
    deduped = [c for c in codes if not (c in seen or seen.add(c))]
    return resolved, deduped


def resolve_effective_policy(
    *,
    symbol: str,
    execution_class: str | None,
    class_policies: dict[str, dict],
    overrides: dict[str, dict],
    denylist: set[str],
    allow_increase: bool,
) -> tuple[dict | None, list[str]]:
    """THE single resolver: class policy + override + denylist for one symbol.

    Returns (effective_policy, blocking_codes). A None policy always comes
    with at least one blocking code — there is no silent permissive default.
    """
    key = str(symbol or "").strip().upper()
    if key and key in denylist:
        return None, ["instrument_denylisted"]
    if not execution_class:
        return None, ["instrument_class_unsupported"]
    policy = class_policies.get(execution_class)
    if not policy:
        return None, ["class_policy_not_configured"]

    effective, codes = apply_override(policy, overrides.get(key), allow_increase=allow_increase)
    return effective, codes
