"""Unified order authorization: catalog + class policy + legacy allowlist.

THE single place that answers "may this order exist at all?", combining:

  1. the denylist            — always wins, nothing overrides it;
  2. the capability flags    — SECURITIES_BUY_ENABLED / SECURITIES_SELL_ENABLED;
  3. the execution catalog   — instrument identity, tick, step, freshness;
  4. the class policy        — per-class limits, plus tighten-only overrides;
  5. the legacy per-symbol   — EXECUTION_INSTRUMENT_POLICIES, still honoured
     policy map                so the current production configuration keeps
                               working while it migrates.

Resolution order for a symbol: a legacy per-symbol policy, when present, is
authoritative for that symbol (it is the stricter, hand-audited form). Every
other symbol goes through catalog + class policy. A symbol covered by
neither is not tradeable — there is no wildcard and no permissive default.
"""

from __future__ import annotations

from app.broker.execution_class import (
    FAMILY_FUND,
    FAMILY_SECURITIES,
    execution_family_of,
    load_class_policies,
    load_denylist,
    load_instrument_overrides,
    resolve_effective_policy,
    resolve_execution_class,
)
from app.broker.instrument_catalog import (
    catalog_entry_status,
    get_instrument,
    instrument_identity,
    normalize_symbol,
)
from app.broker.instrument_scope import load_instrument_policies

# Source labels, surfaced in the preview so a reviewer can see WHICH rule set
# authorised an order.
SOURCE_LEGACY = "legacy_instrument_policy"
SOURCE_CLASS = "class_policy"


def securities_buy_enabled(settings) -> bool:
    """Buys require the dedicated flag AND the absence of the legacy
    sell-only switch.

    EXECUTION_SELL_ONLY is deprecated but NOT removed: while it is true it
    keeps forcing buys off, so an existing deployment cannot start buying
    because a new flag appeared. Migration is: set SECURITIES_BUY_ENABLED
    deliberately, then set EXECUTION_SELL_ONLY=false.
    """
    if getattr(settings, "execution_sell_only", True):
        return False
    return bool(getattr(settings, "securities_buy_enabled", False))


def securities_sell_enabled(settings) -> bool:
    """The strict, new-contract sell capability. Default false."""
    return bool(getattr(settings, "securities_sell_enabled", False))


def legacy_sell_bridge_active(settings) -> bool:
    """DEPRECATED compatibility bridge for the pre-existing sell contract.

    Before this change, sells were authorised by EXECUTION_SELL_ONLY=true
    plus an explicit per-symbol EXECUTION_INSTRUMENT_POLICIES allowlist. That
    path is audited and in production, so introducing SECURITIES_SELL_ENABLED
    must not silently disable it — that would be a regression dressed up as
    hardening.

    The bridge is deliberately narrow: it only authorises SELLS, it only
    applies to symbols covered by the legacy per-symbol allowlist (which is
    still enforced order by order), and it never applies to the class-policy
    path. Migration is to set SECURITIES_SELL_ENABLED=true and then drop
    EXECUTION_SELL_ONLY.
    """
    return bool(getattr(settings, "execution_sell_only", False))


def sell_capability_enabled(settings) -> bool:
    """Is ANY sell path currently authorised (strict flag or legacy bridge)?"""
    return securities_sell_enabled(settings) or legacy_sell_bridge_active(settings)


def _quantity_matches_step(quantity, step) -> bool:
    if step is None or step <= 0:
        return False
    if quantity is None or quantity <= 0:
        return False
    ratio = quantity / step
    return abs(ratio - round(ratio)) <= 1e-9


def load_authorization_context(settings) -> dict:
    """Everything needed to authorise orders, loaded once per preview/batch."""
    class_policies, class_errors = load_class_policies(settings)
    overrides, override_errors = load_instrument_overrides(settings)
    legacy_policies, legacy_errors = load_instrument_policies(settings)
    return {
        "class_policies": class_policies,
        "overrides": overrides,
        "denylist": load_denylist(settings),
        "legacy_policies": legacy_policies,
        "allow_increase": bool(
            getattr(settings, "execution_allow_override_limit_increase", False)
        ),
        "errors": list(class_errors) + list(override_errors) + list(legacy_errors),
    }


def _legacy_scope(*, order, position, policy, settings) -> tuple[dict, list[str]]:
    """Per-symbol legacy policy — unchanged semantics, plus the new flags."""
    from app.broker.instrument_scope import evaluate_order_scope

    scope_info, codes = evaluate_order_scope(
        order=order, position=position, policies={policy["symbol"]: policy}, settings=settings
    )
    side = order.get("side")
    codes = list(codes)
    # Legacy symbols keep their original authorisation path (see
    # legacy_sell_bridge_active) so the audited production config still works.
    if side == "sell" and not sell_capability_enabled(settings):
        if "sell_execution_disabled" not in codes:
            codes.append("sell_execution_disabled")
    if side == "buy" and not securities_buy_enabled(settings):
        if "buy_execution_disabled" not in codes:
            codes.append("buy_execution_disabled")
    scope_info = dict(scope_info)
    scope_info["execution_scope"] = {
        **(scope_info.get("execution_scope") or {}),
        "policy_source": SOURCE_LEGACY,
        "execution_family": FAMILY_SECURITIES,
        "legacy_sell_bridge": legacy_sell_bridge_active(settings),
    }
    return scope_info, codes


def evaluate_order_authorization(
    db,
    *,
    order: dict,
    position,
    settings,
    context: dict | None = None,
    now=None,
) -> tuple[dict, list[str]]:
    """Authorise ONE preview order. Returns (scope_info, blocking_codes).

    Pure with respect to the broker: it reads the catalog and the policies,
    never the network, and never sends anything.
    """
    ctx = context or load_authorization_context(settings)
    symbol = normalize_symbol(order.get("symbol"))
    side = order.get("side")
    quantity = order.get("quantity_planned") or 0
    notional = order.get("estimated_notional") or 0

    empty = {"instrument_identity": {}, "execution_scope": {}}

    if symbol and symbol in ctx["denylist"]:
        return empty, ["instrument_denylisted"]

    # --- Legacy per-symbol policy wins for the symbols it covers ---
    legacy_policy = ctx["legacy_policies"].get(symbol)
    if legacy_policy:
        return _legacy_scope(
            order=order, position=position, policy=legacy_policy, settings=settings
        )

    # --- Class-policy path: identity comes from the execution catalog ---
    if not ctx["class_policies"]:
        # Neither a legacy policy for this symbol nor any class policy at all.
        if ctx["legacy_policies"]:
            return empty, ["instrument_policy_missing"]
        return empty, ["execution_scope_not_configured"]

    entry = get_instrument(db, symbol)
    execution_class = entry.execution_class if entry else None
    policy, policy_codes = resolve_effective_policy(
        symbol=symbol,
        execution_class=execution_class or resolve_execution_class(
            getattr(position, "asset_type", None)
        ),
        class_policies=ctx["class_policies"],
        overrides=ctx["overrides"],
        denylist=ctx["denylist"],
        allow_increase=ctx["allow_increase"],
    )
    if policy is None:
        # Report the catalog gap when that is the real cause, so the operator
        # fixes the catalog instead of guessing at the policy.
        if entry is None:
            return empty, ["instrument_catalog_missing"]
        return empty, policy_codes

    codes: list[str] = list(policy_codes)

    catalog_code, catalog_details = catalog_entry_status(
        entry, now=now, max_age_seconds=policy.get("catalog_max_age_seconds")
    )
    if catalog_code:
        return {**empty, "catalog": catalog_details}, codes + [catalog_code]

    family = entry.execution_family or execution_family_of(entry.execution_class)

    # A fund can never travel through the securities order path. This is the
    # structural guarantee, independent of any flag.
    if family == FAMILY_FUND:
        # A fund is not "unsupported" — it has its own official contract
        # (POST /api/v2/operar/suscripcion|rescate/fci) and its own family.
        # It simply must never travel the SECURITIES path, which would give
        # it a limit price and an OrderExecution row it has no business
        # having. Route it to the fund flow instead.
        return (
            {
                "instrument_identity": instrument_identity(entry),
                "execution_scope": {
                    "policy_source": SOURCE_CLASS,
                    "execution_family": FAMILY_FUND,
                    "execution_class": entry.execution_class,
                },
                "catalog": catalog_details,
            },
            codes + ["fund_requires_fci_contract"],
        )
    if family != FAMILY_SECURITIES:
        return empty, codes + ["instrument_class_unsupported"]

    # --- Capability flags (independent, both default false) ---
    if side == "buy":
        if not securities_buy_enabled(settings):
            codes.append("buy_execution_disabled")
        if not policy.get("buy_enabled"):
            codes.append("class_buy_disabled")
        if not entry.buy_supported:
            codes.append("instrument_buy_unsupported")
    elif side == "sell":
        if not securities_sell_enabled(settings):
            codes.append("sell_execution_disabled")
        if not policy.get("sell_enabled"):
            codes.append("class_sell_disabled")
        if not entry.sell_supported:
            codes.append("instrument_sell_unsupported")
    else:
        codes.append("invalid_broker_side")

    # --- Venue must be inside the class policy AND match the global one ---
    if entry.market not in policy.get("markets", []):
        codes.append("instrument_market_mismatch")
    if entry.settlement not in policy.get("settlements", []):
        codes.append("instrument_settlement_mismatch")
    if entry.currency not in policy.get("currencies", []):
        codes.append("instrument_currency_mismatch")
    if entry.market != (getattr(settings, "iol_order_market", "") or ""):
        codes.append("instrument_market_mismatch")
    if entry.settlement != (getattr(settings, "iol_order_settlement", "") or ""):
        codes.append("instrument_settlement_mismatch")

    # --- Identity must match the snapshot position for a sell ---
    if side == "sell":
        if position is None:
            codes.append("live_position_missing")
        else:
            if normalize_symbol(getattr(position, "symbol", "")) != symbol:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "asset_type", "") or "") != entry.asset_type:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "instrument_type", "") or "") != entry.instrument_type:
                codes.append("instrument_identity_mismatch")
            if (getattr(position, "currency", "") or "") != entry.currency:
                codes.append("instrument_currency_mismatch")

    # --- Sizing ---
    if not _quantity_matches_step(quantity, entry.quantity_step):
        codes.append("quantity_step_mismatch")
    if entry.minimum_quantity is not None and quantity < entry.minimum_quantity:
        codes.append("minimum_quantity_not_met")
    if quantity > policy["max_quantity"]:
        codes.append("symbol_quantity_limit_exceeded")
    if notional > policy["max_order_notional"]:
        codes.append("symbol_notional_limit_exceeded")

    scope_info = {
        "instrument_identity": instrument_identity(entry),
        "execution_scope": {
            "policy_source": SOURCE_CLASS,
            "execution_family": FAMILY_SECURITIES,
            "execution_class": entry.execution_class,
            "buy_enabled": bool(policy.get("buy_enabled")) and securities_buy_enabled(settings),
            "sell_enabled": bool(policy.get("sell_enabled")) and securities_sell_enabled(settings),
            "quantity_step": entry.quantity_step,
            "price_tick": entry.price_tick,
            "minimum_quantity": entry.minimum_quantity,
            "max_quantity": policy["max_quantity"],
            "max_notional": policy["max_order_notional"],
            "max_daily_notional": policy["max_daily_notional"],
            "max_portfolio_pct": policy["max_portfolio_pct"],
            "min_cash_reserve": policy["min_cash_reserve"],
            "fee_buffer_pct": policy["fee_buffer_pct"],
            "max_quote_age_seconds": policy["max_quote_age_seconds"],
            "max_price_deviation_pct": policy["max_price_deviation_pct"],
            "order_type": policy["order_type"],
            "validity_minutes": policy["validity_minutes"],
            "catalog_max_age_seconds": policy["catalog_max_age_seconds"],
        },
        "catalog": catalog_details,
    }

    seen: set[str] = set()
    deduped = [c for c in codes if not (c in seen or seen.add(c))]
    return scope_info, deduped


def effective_policy_for(db, symbol, settings, context: dict | None = None) -> dict | None:
    """Effective (class + override) policy for a symbol, or None.

    Used by the preflight to read per-class quote/limit parameters without
    re-deriving them, and by the readiness report.
    """
    ctx = context or load_authorization_context(settings)
    key = normalize_symbol(symbol)
    legacy = ctx["legacy_policies"].get(key)
    if legacy:
        return {
            "policy_source": SOURCE_LEGACY,
            "execution_family": FAMILY_SECURITIES,
            "execution_class": None,
            "price_tick": legacy["price_tick"],
            "quantity_step": legacy["quantity_step"],
            "max_quantity": legacy["max_quantity"],
            "max_order_notional": legacy["max_notional"],
            # Legacy policies predate per-class parameters: fall back to the
            # global settings that governed them, never to a permissive value.
            "max_daily_notional": None,
            "max_portfolio_pct": getattr(settings, "execution_max_portfolio_pct", 0.0),
            "min_cash_reserve": 0.0,
            "fee_buffer_pct": 0.0,
            "max_quote_age_seconds": getattr(settings, "execution_max_quote_age_seconds", 0),
            "max_price_deviation_pct": getattr(settings, "execution_max_price_deviation_pct", 0.0),
            "order_type": getattr(settings, "iol_order_type", ""),
            "validity_minutes": getattr(settings, "iol_order_validity_minutes", 0),
        }

    entry = get_instrument(db, key)
    if entry is None:
        return None
    policy, _ = resolve_effective_policy(
        symbol=key,
        execution_class=entry.execution_class,
        class_policies=ctx["class_policies"],
        overrides=ctx["overrides"],
        denylist=ctx["denylist"],
        allow_increase=ctx["allow_increase"],
    )
    if policy is None:
        return None
    return {
        **policy,
        "policy_source": SOURCE_CLASS,
        "price_tick": entry.price_tick,
        "quantity_step": entry.quantity_step,
    }
