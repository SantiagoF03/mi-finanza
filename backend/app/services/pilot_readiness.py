"""Technical readiness for controlled execution pilots. READ-ONLY.

Two questions that look like one and are not:

- **technically ready** — is this instrument, on this side, correctly
  described, priced and sized? A fact about the world.
- **activation ready** — are we ALLOWED to send? A fact about configuration.

They are reported separately because collapsing them makes the system
unpreparable: with every lock shut — which is the correct resting state — a
combined verdict says "not ready", and an operator cannot tell a missing tick
from a closed padlock. You prepare with the locks shut and open them last.

`live=false` (the default) answers everything that needs no broker: catalog,
identity, class, policy, limits. `live=true` additionally probes ONE side of
ONE symbol for a specific quantity — best ask + live balance for a buy, best
bid + live position for a sell — and it costs a real broker call, so it is
gated behind the execution credential and capped at 10 symbols.

Nothing here sends, approves, enables a flag or writes to the catalog.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.orm import Session

from app.broker.execution_class import (
    CLASS_ACCIONES,
    CLASS_CEDEARS,
    FAMILY_SECURITIES,
    load_class_policies,
    load_denylist,
    load_instrument_overrides,
    resolve_effective_policy,
)
from app.broker.instrument_catalog import (
    catalog_entry_status,
    get_instrument,
    list_catalog,
    normalize_symbol,
)
from app.broker.numeric import non_negative_decimal, positive_decimal
from app.core.config import get_settings

logger = logging.getLogger(__name__)

SECURITIES_CLASSES = (CLASS_ACCIONES, CLASS_CEDEARS)

# A live probe costs a real broker call per symbol per side. Ten is a limit,
# not a target: the pilot flow needs exactly one.
MAX_LIVE_SYMBOLS = 10

# Codes that describe CONFIGURATION, not the instrument. They never make
# something technically unready — they make it un-sendable, which is a
# different sentence.
ACTIVATION_CODES = frozenset({
    "execution_locked",
    "buy_execution_disabled",
    "sell_execution_disabled",
    "execution_sell_only_blocks_buy",
    "execution_pilot_creation_disabled",
})


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _err(message: str, code: str, status_code: int) -> dict:
    return {"error": message, "code": code, "status_code": status_code}


# ---------------------------------------------------------------------------
# Quote probing
# ---------------------------------------------------------------------------


def _quote_probe(broker, symbol: str, side: str, market: str, settlement: str) -> dict:
    """Read ONE side of the book. Never raises, never orders.

    Returns evidence, not a verdict. `available` means a usable price for THIS
    side exists right now; `source` says which side answered, because a
    provider replying with the other side is not evidence for the one we asked
    about.

    This is liquidity, not capability: an empty book at 3pm does not mean the
    instrument cannot be traded. The two are reported apart
    (`quote_available_now` vs `capability_verified`).
    """
    probe = {
        "requested_side": side,
        "expected_source": "bid" if side == "sell" else "ask",
        "available": False,
        "price": None,
        "source": None,
        "age_seconds": None,
        "retrieved_at": None,
        "error": None,
        "probed": broker is not None,
    }
    if broker is None:
        probe["error"] = "quote_not_probed"
        return probe
    try:
        quote = broker.get_execution_quote(symbol, side, market, settlement) or {}
    except Exception as exc:  # a probe must never break the report
        probe["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        return probe

    probe["source"] = quote.get("source")
    probe["price"] = quote.get("price")
    retrieved_at = quote.get("retrieved_at")
    probe["retrieved_at"] = retrieved_at
    if retrieved_at:
        try:
            stamp = datetime.fromisoformat(str(retrieved_at))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            probe["age_seconds"] = (_utcnow() - stamp).total_seconds()
        except (TypeError, ValueError):
            probe["age_seconds"] = None

    if not quote.get("available"):
        probe["error"] = "quote_unavailable"
        return probe
    if quote.get("source") != probe["expected_source"]:
        probe["error"] = "quote_wrong_side"
        return probe
    price = quote.get("price")
    if price is None or isinstance(price, bool):
        probe["error"] = "quote_unavailable"
        return probe
    try:
        if float(price) <= 0:
            probe["error"] = "quote_unavailable"
            return probe
    except (TypeError, ValueError):
        probe["error"] = "quote_unavailable"
        return probe

    probe["available"] = True
    return probe


# ---------------------------------------------------------------------------
# Per-instrument verification, split by what it actually proves
# ---------------------------------------------------------------------------


def instrument_verification(entry) -> dict:
    """Identity, buy capability and sell capability — three separate verdicts.

    An instrument with a bid but no ask is perfectly sellable. Requiring
    buy_supported AND sell_supported AND quote_supported before anything works
    means one missing side blocks both, which is not what the data says.
    """
    from app.broker.instrument_catalog import VERIFYING_PROVENANCES

    if entry is None:
        return {
            "identity_verified": False, "buy_verified": False, "sell_verified": False,
            "identity_missing": ["instrument_catalog_missing"],
            "buy_missing": ["instrument_catalog_missing"],
            "sell_missing": ["instrument_catalog_missing"],
        }

    provenance = entry.field_provenance or {}

    def verified(field: str) -> bool:
        return provenance.get(field) in VERIFYING_PROVENANCES

    # Shared identity and mechanics: what any order of any side needs.
    identity_fields = ("broker_symbol", "asset_type", "instrument_type", "currency",
                       "market", "price_tick", "quantity_step")
    identity_missing = [f for f in identity_fields if not verified(f)]

    buy_missing = list(identity_missing)
    if not verified("buy_supported"):
        buy_missing.append("buy_supported")
    if not bool(entry.buy_supported):
        buy_missing.append("buy_unsupported")

    sell_missing = list(identity_missing)
    if not verified("sell_supported"):
        sell_missing.append("sell_supported")
    if not bool(entry.sell_supported):
        sell_missing.append("sell_unsupported")

    return {
        "identity_verified": not identity_missing,
        "buy_verified": not buy_missing,
        "sell_verified": not sell_missing,
        "identity_missing": sorted(set(identity_missing)),
        "buy_missing": sorted(set(buy_missing)),
        "sell_missing": sorted(set(sell_missing)),
    }


def _technical_blockers(
    *, side: str, entry, catalog_code: str | None, policy: dict | None,
    policy_codes: list[str], probe: dict, verification: dict, denylisted: bool,
    live: bool,
) -> list[str]:
    """Facts about the instrument that stop this side. No flags here."""
    reasons: list[str] = []
    if denylisted:
        reasons.append("instrument_denylisted")

    # A catalog code about the OTHER side's capability must not block this one.
    if catalog_code and catalog_code != "instrument_not_verified":
        reasons.append(catalog_code)

    if entry is not None and entry.execution_family != FAMILY_SECURITIES:
        reasons.append("fund_requires_fci_contract")
    if entry is not None and entry.execution_class not in SECURITIES_CLASSES:
        reasons.append("instrument_class_unsupported")

    if not verification["identity_verified"]:
        reasons.append("instrument_identity_not_verified")
    if side == "buy" and not verification["buy_verified"]:
        reasons.append("instrument_buy_not_verified")
    if side == "sell" and not verification["sell_verified"]:
        reasons.append("instrument_sell_not_verified")

    if policy is None:
        reasons.append("class_policy_not_configured")
    else:
        reasons.extend(policy_codes)
        if side == "buy" and not policy.get("buy_enabled"):
            reasons.append("class_buy_disabled")
        if side == "sell" and not policy.get("sell_enabled"):
            reasons.append("class_sell_disabled")

    # Liquidity is only a blocker when we actually looked. Without a probe we
    # do not know, and "we did not look" must not read as "there is no book".
    if live:
        if not probe.get("available"):
            reasons.append(
                "quote_wrong_side" if probe.get("error") == "quote_wrong_side"
                else "quote_unavailable"
            )
        elif policy and probe.get("age_seconds") is not None:
            max_age = policy.get("max_quote_age_seconds")
            if max_age and probe["age_seconds"] > float(max_age):
                reasons.append("quote_stale")
    else:
        reasons.append("live_check_not_performed")

    return _dedupe(reasons)


def _activation_blockers(*, side: str, settings, include_pilot_flag: bool = False) -> list[str]:
    """Configuration that stops the send. Never a fact about the instrument."""
    from app.broker.execution_scope import (
        securities_buy_enabled,
        sell_capability_enabled,
    )

    reasons: list[str] = []
    if not bool(getattr(settings, "order_execution_enabled", False)):
        reasons.append("execution_locked")
    if side == "buy":
        if bool(getattr(settings, "execution_sell_only", False)):
            reasons.append("execution_sell_only_blocks_buy")
        if not securities_buy_enabled(settings):
            reasons.append("buy_execution_disabled")
    if side == "sell" and not sell_capability_enabled(settings):
        reasons.append("sell_execution_disabled")
    if include_pilot_flag and not bool(
        getattr(settings, "execution_pilot_creation_enabled", False)
    ):
        reasons.append("execution_pilot_creation_disabled")
    return _dedupe(reasons)


def _dedupe(reasons: list[str]) -> list[str]:
    """Preserve order; the first code is the headline reason."""
    seen: set[str] = set()
    ordered: list[str] = []
    for code in reasons:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


# ---------------------------------------------------------------------------
# Live checks for an EXACT quantity
# ---------------------------------------------------------------------------


def _live_buy_check(db: Session, broker, *, entry, policy, quantity, probe, settings) -> dict:
    """Can we pay for exactly this many, right now, in the right currency?

    Never consults snapshot.cash: a snapshot is a photograph, and money that
    was there an hour ago does not authorise an order now.
    """
    from app.services.execution_limits import (
        evaluate_buy_cash,
        pending_buy_notional,
        trade_date_for,
    )

    result = {"performed": False, "code": None, "detail": {}, "exact_notional": None}
    ask = positive_decimal(probe.get("price"))
    qty = positive_decimal(quantity)
    if ask is None or qty is None:
        result["code"] = "quote_unavailable"
        return result

    notional = (qty * ask).quantize(Decimal("0.01"))
    result["exact_notional"] = float(notional)
    currency = str(entry.currency or "").strip().upper()

    try:
        live_cash = broker.get_live_cash(currency)
    except Exception as exc:
        result["performed"] = True
        result["code"] = "live_cash_unavailable"
        result["detail"] = {"error": f"{type(exc).__name__}: {str(exc)[:120]}"}
        return result

    pending = pending_buy_notional(
        db, currency=currency, trade_date=trade_date_for(settings), settings=settings
    )
    code, detail = evaluate_buy_cash(
        live_cash=live_cash,
        required_notional=notional,
        currency=currency,
        fee_buffer_pct=(policy or {}).get("fee_buffer_pct", 0.0),
        min_cash_reserve=(policy or {}).get("min_cash_reserve", 0.0),
        pending_notional=pending,
    )
    result.update({"performed": True, "code": code, "detail": detail})
    return result


def _live_sell_check(broker, *, entry, quantity, probe) -> dict:
    """Do we actually hold this many, right now? No short selling."""
    result = {"performed": False, "code": None, "detail": {}, "exact_notional": None}
    bid = positive_decimal(probe.get("price"))
    qty = positive_decimal(quantity)
    if bid is None or qty is None:
        result["code"] = "quote_unavailable"
        return result
    result["exact_notional"] = float((qty * bid).quantize(Decimal("0.01")))

    try:
        live = broker.get_portfolio_snapshot() or {}
    except Exception as exc:
        result["performed"] = True
        result["code"] = "live_position_unavailable"
        result["detail"] = {"error": f"{type(exc).__name__}: {str(exc)[:120]}"}
        return result

    result["performed"] = True
    symbol = normalize_symbol(entry.broker_symbol)
    match = None
    for position in live.get("positions") or []:
        if normalize_symbol(position.get("symbol")) == symbol:
            match = position
            break
    if match is None:
        result["code"] = "live_position_missing"
        return result

    held = non_negative_decimal(match.get("quantity"))
    if held is None:
        result["code"] = "live_position_unavailable"
        return result
    committed = non_negative_decimal(match.get("committed")) or Decimal("0")
    available = held - committed
    result["detail"] = {
        "held_quantity": float(held),
        "committed_quantity": float(committed),
        "available_quantity": float(available),
        "requested_quantity": float(qty),
        "currency": str(match.get("currency") or "").strip().upper(),
    }
    if str(match.get("currency") or "").strip().upper() != str(entry.currency or "").strip().upper():
        result["code"] = "instrument_currency_mismatch"
        return result
    if available < qty:
        # Selling more than we hold is short selling, which this system does
        # not do — not "not yet", but not at all.
        result["code"] = "live_position_insufficient"
        return result
    return result


def _price_tick_report(entry, probe: dict) -> dict:
    """Tick reporting, with the effective value ONLY when a price exists.

    Without a live quote there is no price, and without a price a dynamic rule
    has no answer. Reporting a made-up `effective_price_tick` in that case
    would show an operator a number the order path never agreed to.
    """
    from app.broker.instrument_catalog import (
        VERIFYING_PROVENANCES,
        verified_price_tick_rule,
    )
    from app.broker.price_rules import PriceTickRuleError, resolve_price_band_for_rule

    report = {
        "fixed_price_tick": entry.price_tick if entry is not None else None,
        "fixed_price_tick_verified": False,
        "dynamic_price_tick_rule_verified": False,
        "price_tick_rule": None,
        "price_tick_band": None,
        "effective_price_tick": None,
        "reference_price_used_for_tick": None,
        "price_tick_mode": None,
        "effective_price_tick_requires_live_quote": False,
    }
    if entry is None:
        return report

    provenance = entry.field_provenance or {}
    fixed_verified = (
        entry.price_tick is not None
        and provenance.get("price_tick") in VERIFYING_PROVENANCES
    )
    report["fixed_price_tick_verified"] = bool(fixed_verified)

    rule = verified_price_tick_rule(entry)
    if rule is None:
        if fixed_verified:
            report["price_tick_mode"] = "fixed"
            report["effective_price_tick"] = str(entry.price_tick)
        return report

    report.update({
        "dynamic_price_tick_rule_verified": True,
        "price_tick_rule": rule,
        "price_tick_mode": "dynamic",
        "effective_price_tick_requires_live_quote": True,
    })
    price = probe.get("price") if probe.get("available") else None
    if price is None:
        return report
    try:
        band = resolve_price_band_for_rule(rule, price)
    except PriceTickRuleError:
        return report
    report.update({
        "price_tick_band": band["band"],
        "effective_price_tick": format(band["tick"], "f"),
        "reference_price_used_for_tick": format(
            positive_decimal(price) or Decimal("0"), "f"
        ),
    })
    return report


def _quantity_blockers(*, entry, policy, quantity, exact_notional) -> list[str]:
    """Does this exact quantity fit the instrument and the class limits?"""
    reasons: list[str] = []
    qty = positive_decimal(quantity)
    if qty is None:
        return ["invalid_quantity"]

    step = entry.quantity_step if entry is not None else None
    if step is None:
        reasons.append("quantity_step_unverified")
    else:
        step_f = float(step)
        if step_f <= 0 or abs((float(qty) / step_f) - round(float(qty) / step_f)) > 1e-9:
            reasons.append("quantity_step_mismatch")

    minimum = entry.minimum_quantity if entry is not None else None
    if minimum is not None and float(qty) < float(minimum):
        reasons.append("minimum_quantity_not_met")

    if policy:
        max_quantity = policy.get("max_quantity")
        if max_quantity and float(qty) > float(max_quantity):
            reasons.append("symbol_quantity_limit_exceeded")
        max_notional = policy.get("max_order_notional")
        if max_notional and exact_notional and float(exact_notional) > float(max_notional):
            reasons.append("order_limit_exceeded")
    return reasons


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


def evaluate_pilot_readiness(
    db: Session,
    *,
    symbols: list[str] | None = None,
    side: str | None = None,
    quantity=None,
    live: bool = False,
    broker=None,
    settings=None,
) -> dict:
    """Readiness per symbol. `live=True` probes the requested side only.

    In live mode the caller must name the symbols, the side and the quantity:
    a readiness answer for "some quantity" of "some side" is not an answer to
    the question a pilot actually asks.
    """
    settings = settings or get_settings()
    class_policies, class_errors = load_class_policies(settings)
    overrides, override_errors = load_instrument_overrides(settings)
    denylist = load_denylist(settings)
    allow_increase = bool(getattr(settings, "execution_allow_override_limit_increase", False))

    requested = [normalize_symbol(s) for s in (symbols or []) if normalize_symbol(s)]
    sides = ("buy", "sell") if not side else (str(side).strip().lower(),)

    if requested:
        entries = [(sym, get_instrument(db, sym)) for sym in requested]
    else:
        # Without an explicit list we do NOT walk the whole catalog in live
        # mode; that would be one broker call per instrument per side.
        entries = [
            (entry.broker_symbol, entry)
            for entry in list_catalog(db)
            if entry.execution_class in SECURITIES_CLASSES
        ]

    reports: list[dict] = []
    for symbol, entry in entries:
        execution_class = entry.execution_class if entry is not None else None
        policy, policy_codes = resolve_effective_policy(
            symbol=symbol,
            execution_class=execution_class,
            class_policies=class_policies,
            overrides=overrides,
            denylist=denylist,
            allow_increase=allow_increase,
        )
        catalog_code, catalog_details = catalog_entry_status(
            entry,
            max_age_seconds=(class_policies.get(execution_class or "") or {}).get(
                "catalog_max_age_seconds"
            ),
        )
        verification = instrument_verification(entry)

        market = (entry.market if entry is not None else "") or settings.iol_order_market or ""
        settlement = (
            (entry.settlement if entry is not None else "")
            or settings.iol_order_settlement or ""
        )

        side_reports: dict = {}
        for one_side in ("buy", "sell"):
            probed = live and one_side in sides and entry is not None
            probe = (
                _quote_probe(broker, symbol, one_side, market, settlement)
                if probed else _quote_probe(None, symbol, one_side, market, settlement)
            )
            technical = _technical_blockers(
                side=one_side, entry=entry, catalog_code=catalog_code, policy=policy,
                policy_codes=policy_codes, probe=probe, verification=verification,
                denylisted=symbol in denylist, live=probed,
            )
            live_check: dict = {"performed": False}
            exact_notional = None
            if probed and quantity is not None:
                if one_side == "buy":
                    live_check = _live_buy_check(
                        db, broker, entry=entry, policy=policy,
                        quantity=quantity, probe=probe, settings=settings,
                    )
                else:
                    live_check = _live_sell_check(
                        broker, entry=entry, quantity=quantity, probe=probe
                    )
                exact_notional = live_check.get("exact_notional")
                if live_check.get("code"):
                    technical.append(live_check["code"])
                technical.extend(_quantity_blockers(
                    entry=entry, policy=policy, quantity=quantity,
                    exact_notional=exact_notional,
                ))
                technical = _dedupe(technical)

            activation = _activation_blockers(side=one_side, settings=settings)
            side_reports[one_side] = {
                # Technical readiness is about the instrument. It can — and
                # before a pilot SHOULD — be true while every lock is shut.
                "technically_ready": not technical,
                "technical_blocking_reasons": technical,
                "activation_ready": not technical and not activation,
                "activation_blocking_reasons": activation,
                "ready_for_real_execution": not technical and not activation,
                "capability_verified": (
                    verification["buy_verified"] if one_side == "buy"
                    else verification["sell_verified"]
                ),
                "quote_available_now": bool(probe.get("available")),
                "quote": probe,
                "live_check": live_check,
                "exact_notional": exact_notional,
                **_price_tick_report(entry, probe),
            }

        reference_price = (
            side_reports["sell"]["quote"].get("price")
            or side_reports["buy"]["quote"].get("price")
        )
        sizing = _suggested_pilot_limits(entry, policy, reference_price)

        reports.append({
            "symbol": symbol,
            "execution_class": execution_class,
            "execution_family": entry.execution_family if entry is not None else None,
            "currency": entry.currency if entry is not None else None,
            "market": market or None,
            "settlement": settlement or None,
            "catalog_status": catalog_details.get("verification_status"),
            "catalog_blocking_reason": catalog_code,
            "policy_configured": policy is not None,
            "verification": verification,
            "requested_quantity": float(quantity) if quantity is not None else None,
            "live": live,
            "buy": side_reports["buy"],
            "sell": side_reports["sell"],
            **sizing,
            "manual_configuration_required": _manual_configuration_required(
                entry, policy, settings
            ),
        })

    return {
        "generated_at": _utcnow().isoformat(),
        "read_only": True,
        "sends_orders": False,
        "live": live,
        "requested_symbols": requested or None,
        "requested_side": side,
        "requested_quantity": float(quantity) if quantity is not None else None,
        "class_policy_errors": list(class_errors) + list(override_errors),
        "symbols": reports,
        "classes": _class_rollup(reports, class_policies, settings),
        "disclaimer": (
            "Aptitud TÉCNICA únicamente. No es una recomendación de inversión "
            "y los montos sugeridos son topes técnicos, no objetivos."
        ),
    }


def evaluate_symbol_side(
    db: Session, *, symbol: str, side: str, quantity, broker, settings=None
) -> dict:
    """One symbol, one side, one exact quantity, with live checks.

    The pilot creator's entry point: it must never create a pilot for a
    quantity we already know cannot be paid for or is not held.
    """
    report = evaluate_pilot_readiness(
        db, symbols=[symbol], side=side, quantity=quantity,
        live=True, broker=broker, settings=settings,
    )
    entry = next(
        (r for r in report["symbols"] if r["symbol"] == normalize_symbol(symbol)), None
    )
    if entry is None:
        return _err("No se pudo evaluar el símbolo.", "pilot_readiness_unavailable", 409)
    return {"report": report, "symbol_report": entry, "side_report": entry[side]}


def _manual_configuration_required(entry, policy: dict | None, settings) -> list[str]:
    """What a HUMAN still has to set, as opposed to what the app can read."""
    todo: list[str] = []
    if entry is None:
        todo.append("resolve_instrument_read_only")
        return todo
    if entry.price_tick is None:
        todo.append("verify_price_tick")
    if entry.quantity_step is None:
        todo.append("verify_quantity_step")
    if policy is None:
        todo.append(f"configure_class_policy_{entry.execution_class or 'unknown'}")
    if not settings.execution_admin_key:
        todo.append("configure_execution_admin_key")
    if not settings.execution_preview_secret:
        todo.append("configure_preview_secret")
    return todo


def _suggested_pilot_limits(entry, policy: dict | None, probe_price) -> dict:
    """Smallest technically valid pilot size. A CEILING, never a suggestion.

    Quantity is 1 whenever the step allows it; if the instrument trades in
    lots of 100, 1 is not a valid order and we say so instead of rounding into
    something bigger than the operator asked for.
    """
    result = {
        "suggested_pilot_max_quantity": None,
        "suggested_pilot_max_notional": None,
        "minimum_valid_quantity": None,
        "minimum_valid_notional": None,
        "quantity_step": entry.quantity_step if entry is not None else None,
        "price_tick": entry.price_tick if entry is not None else None,
        "notes": [],
    }
    if entry is None:
        result["notes"].append("instrument_not_in_catalog")
        return result

    step = entry.quantity_step
    minimum = entry.minimum_quantity
    if step is None:
        result["notes"].append("quantity_step_unverified")
    else:
        step_f = float(step)
        smallest = step_f
        if minimum is not None and float(minimum) > smallest:
            # Round the minimum UP to a whole number of steps.
            import math

            smallest = math.ceil(float(minimum) / step_f) * step_f
        result["minimum_valid_quantity"] = smallest
        result["suggested_pilot_max_quantity"] = smallest
        if smallest > 1.0:
            result["notes"].append("minimum_order_is_more_than_one_unit")

    quantity = result["minimum_valid_quantity"]
    if quantity is not None and probe_price:
        try:
            minimum_notional = float(quantity) * float(probe_price)
        except (TypeError, ValueError):
            minimum_notional = None
        if minimum_notional is not None:
            result["minimum_valid_notional"] = round(minimum_notional, 2)
            limit = (policy or {}).get("max_order_notional")
            if limit and minimum_notional > float(limit):
                # The smallest legal order is bigger than the pilot limit. The
                # limit is NOT widened to fit: that would be the system
                # authorising more than the operator did.
                result["notes"].append("pilot_limit_below_minimum_lot")
                result["suggested_pilot_max_notional"] = float(limit)
            else:
                result["suggested_pilot_max_notional"] = round(
                    min(minimum_notional, float(limit)) if limit else minimum_notional, 2
                )
    elif quantity is not None:
        result["notes"].append("no_executable_price_to_size_with")
    return result


def _class_rollup(reports: list[dict], class_policies: dict, settings) -> dict:
    """Per-class readiness. A class is NEVER ready because one symbol is."""
    from app.broker.execution_scope import (
        legacy_sell_bridge_active,
        securities_buy_enabled,
        sell_capability_enabled,
    )

    rollup: dict = {}
    for execution_class in SECURITIES_CLASSES:
        members = [r for r in reports if r["execution_class"] == execution_class]
        buy_ok = [r["symbol"] for r in members if r["buy"]["technically_ready"]]
        sell_ok = [r["symbol"] for r in members if r["sell"]["technically_ready"]]
        blocked = [
            r["symbol"] for r in members
            if not r["buy"]["technically_ready"] and not r["sell"]["technically_ready"]
        ]
        policy = class_policies.get(execution_class)
        rollup[execution_class.lower()] = {
            "policy_configured": policy is not None,
            "flag_buy_enabled": securities_buy_enabled(settings),
            "flag_sell_enabled": sell_capability_enabled(settings),
            "technically_ready_buy_symbols": sorted(buy_ok),
            "technically_ready_sell_symbols": sorted(sell_ok),
            # Activation requires the policy AND the flag AND at least one
            # technically ready instrument. Any one missing means not ready.
            "buy_ready": bool(policy) and securities_buy_enabled(settings) and bool(buy_ok),
            "sell_ready": bool(policy) and sell_capability_enabled(settings) and bool(sell_ok),
            "covered_symbols": sorted(set(buy_ok) | set(sell_ok)),
            "buy_ready_symbols": sorted(buy_ok),
            "sell_ready_symbols": sorted(sell_ok),
            "blocked_symbols": sorted(blocked),
            "evaluated_symbols": len(members),
        }
    rollup["legacy_sell_path_ready"] = legacy_sell_bridge_active(settings)
    return rollup


# ---------------------------------------------------------------------------
# Policy template — a draft that the loaders must ACCEPT
# ---------------------------------------------------------------------------

PILOT_MAX_ORDER_NOTIONAL = 500.0
PILOT_MAX_DAILY_NOTIONAL = 1000.0
PILOT_MAX_QUANTITY = 1.0
PILOT_MAX_PORTFOLIO_PCT = 0.02
PILOT_MIN_CASH_RESERVE = 1000.0
PILOT_FEE_BUFFER_PCT = 0.01
PILOT_MAX_QUOTE_AGE_SECONDS = 15
PILOT_MAX_PRICE_DEVIATION_PCT = 0.02
PILOT_CATALOG_MAX_AGE_SECONDS = 86400
PILOT_VALIDITY_MINUTES = 10


def build_pilot_policy_template(
    db: Session, *, symbols: list[str] | None = None, settings=None
) -> dict:
    """A reviewable JSON draft the loaders accept as-is. Writes nothing.

    The previous template produced a config the application itself rejected —
    a draft that cannot be loaded is worse than no draft, because the operator
    discovers it only after pasting it into production. So the shape is now
    exactly what `load_class_policies` and `load_instrument_overrides` accept,
    and a test round-trips it through both.

    Rules, all for the same reason (an invented number in a policy is
    indistinguishable from a verified one once it is loaded):

    - tick and step NEVER go into an override — that schema has no such
      fields, and putting them there is what made the draft unloadable. They
      go into a clearly separate, non-variable section instead;
    - `buy_enabled` / `sell_enabled` are always false in the draft;
    - class limits are never widened to fit a big minimum lot: if the smallest
      legal order exceeds the pilot limit, that is reported, not fixed;
    - overrides only ever TIGHTEN.
    """
    from app.broker.execution_class import (
        KNOWN_IOL_MARKETS,
        KNOWN_IOL_SETTLEMENTS,
        OVERRIDABLE_FIELDS,
    )

    settings = settings or get_settings()
    requested = [normalize_symbol(s) for s in (symbols or []) if normalize_symbol(s)]
    if requested:
        resolved = [(s, get_instrument(db, s)) for s in requested]
        entries = [e for _, e in resolved if e is not None]
        missing = [s for s, e in resolved if e is None]
    else:
        entries = [e for e in list_catalog(db) if e.execution_class in SECURITIES_CLASSES]
        missing = []

    warnings: list[str] = [
        "Plantilla para REVISIÓN humana. No escribe ninguna variable ni toca Railway.",
        "buy_enabled y sell_enabled vienen en false a propósito: habilitarlos es "
        "una decisión aparte, después de revisar los límites.",
        "price_tick y quantity_step NO son variables de entorno. Se cargan con "
        "POST /api/broker/instruments/{symbol}/verify-fields; mirá la sección "
        "INSTRUMENT_FIELD_VERIFICATION_PAYLOADS.",
    ]
    if missing:
        warnings.append(
            "Símbolos sin entrada de catálogo (resolvelos read-only primero): "
            + ", ".join(sorted(missing))
        )

    class_block: dict = {}
    instrument_overrides: dict = {}
    verification_payloads: dict = {}

    for execution_class in SECURITIES_CLASSES:
        members = [e for e in entries if e.execution_class == execution_class]
        if not members:
            continue

        currencies = sorted({(e.currency or "").upper() for e in members if e.currency})
        # Venue values are matched CASE-SENSITIVELY by the loader (IOL's own
        # spelling is `bCBA`), so the template must emit the canonical form.
        # Lower-casing them here is what made the draft fail validation.
        markets = sorted({
            _canonical_venue(e.market, KNOWN_IOL_MARKETS) for e in members if e.market
        })
        settlements = sorted({
            _canonical_venue(e.settlement, KNOWN_IOL_SETTLEMENTS)
            for e in members if e.settlement
        })
        for label, values, known in (("mercado", markets, KNOWN_IOL_MARKETS),
                                     ("plazo", settlements, KNOWN_IOL_SETTLEMENTS)):
            unknown = [v for v in values if v not in known]
            if unknown:
                warnings.append(
                    f"{execution_class}: {label} no reconocido {unknown}. "
                    "La clase queda inválida hasta corregirlo; no se inventa un valor."
                )
        for label, values in (("moneda", currencies), ("mercado", markets),
                              ("plazo", settlements)):
            if not values:
                warnings.append(
                    f"{execution_class}: ningún valor de {label} observado. "
                    "La clase queda incompleta hasta que lo completes a mano."
                )

        # The class max_quantity must be able to express the biggest minimum
        # lot among its members; otherwise every order of that instrument is
        # refused by a limit that was never about it.
        lot_sizes = [
            float(e.quantity_step) for e in members if e.quantity_step is not None
        ]
        max_quantity = max([PILOT_MAX_QUANTITY] + lot_sizes) if lot_sizes else PILOT_MAX_QUANTITY

        class_block[execution_class] = {
            "buy_enabled": False,
            "sell_enabled": False,
            "currencies": currencies,
            "markets": markets,
            "settlements": settlements,
            "max_order_notional": PILOT_MAX_ORDER_NOTIONAL,
            "max_daily_notional": PILOT_MAX_DAILY_NOTIONAL,
            "max_quantity": max_quantity,
            "max_portfolio_pct": PILOT_MAX_PORTFOLIO_PCT,
            "min_cash_reserve": PILOT_MIN_CASH_RESERVE,
            "fee_buffer_pct": PILOT_FEE_BUFFER_PCT,
            "max_quote_age_seconds": PILOT_MAX_QUOTE_AGE_SECONDS,
            "max_price_deviation_pct": PILOT_MAX_PRICE_DEVIATION_PCT,
            "catalog_max_age_seconds": PILOT_CATALOG_MAX_AGE_SECONDS,
            "validity_minutes": PILOT_VALIDITY_MINUTES,
            # Explicitly null: a class-wide tick is an assertion about every
            # member. Null keeps the provenance non-verifying, so an
            # instrument without a real tick stays blocked — which is correct.
            "default_quantity_step": None,
            "default_price_tick": None,
            "order_type": settings.iol_order_type or "precioLimite",
        }

        for entry in members:
            override: dict = {}
            step = entry.quantity_step
            if step is not None and float(step) < max_quantity:
                # Tightening only, and only when it IS tighter.
                override["max_quantity"] = float(step)
            # Every key must be an overridable one; anything else makes the
            # whole map unloadable.
            override = {k: v for k, v in override.items() if k in OVERRIDABLE_FIELDS}
            if override:
                instrument_overrides[entry.broker_symbol] = override

            if entry.price_tick is None or entry.quantity_step is None:
                verification_payloads[entry.broker_symbol] = {
                    "price_tick": entry.price_tick,
                    "quantity_step": entry.quantity_step,
                    "note": "<por qué verificás estos valores y contra qué fuente>",
                }
                warnings.append(
                    f"{entry.broker_symbol}: tick o step sin verificar. "
                    "Verificalos con POST /api/broker/instruments/"
                    f"{entry.broker_symbol}/verify-fields."
                )

            minimum_notional = _minimum_lot_notional(entry)
            if minimum_notional and minimum_notional > PILOT_MAX_ORDER_NOTIONAL:
                warnings.append(
                    f"{entry.broker_symbol}: pilot_limit_below_minimum_lot — el lote "
                    f"mínimo vale ~{minimum_notional:.2f} y el límite del piloto es "
                    f"{PILOT_MAX_ORDER_NOTIONAL:.2f}. Subir el límite es una decisión "
                    "tuya; la plantilla no lo amplía sola."
                )

    return {
        "generated_at": _utcnow().isoformat(),
        "read_only": True,
        "writes_configuration": False,
        "EXECUTION_CLASS_POLICIES": class_block,
        "EXECUTION_INSTRUMENT_OVERRIDES": instrument_overrides,
        "EXECUTION_DENYLIST": sorted(load_denylist(settings)),
        # NOT an environment variable. Payloads for the verify-fields endpoint,
        # kept apart so nobody pastes them into Railway.
        "INSTRUMENT_FIELD_VERIFICATION_PAYLOADS": verification_payloads,
        "warnings": warnings,
        "next_step": (
            "Revisá los valores, completá sólo lo verificado, cargá las tres "
            "primeras claves a mano y usá la cuarta con verify-fields. Este "
            "endpoint no escribe nada."
        ),
    }


def _canonical_venue(value, known: set[str]) -> str:
    """The known spelling of a market or settlement, if we recognise it.

    Matching is case-insensitive, but the OUTPUT is the canonical form,
    because the policy loader compares exactly. An unrecognised value is
    returned untouched rather than coerced into something that looks valid —
    a wrong venue silently accepted is an order routed somewhere else.
    """
    raw = str(value or "").strip()
    for candidate in known:
        if candidate.lower() == raw.lower():
            return candidate
    return raw


def _minimum_lot_notional(entry) -> float | None:
    """Value of the smallest legal order, using the catalog's own price."""
    step = entry.quantity_step
    if step is None:
        return None
    price = None
    raw = entry.raw_identity or {}
    for key in ("last_price", "ultimoPrecio", "price"):
        candidate = positive_decimal(raw.get(key))
        if candidate is not None:
            price = float(candidate)
            break
    if price is None:
        return None
    return float(step) * price


# ---------------------------------------------------------------------------
# next_safe_action — one per capability
# ---------------------------------------------------------------------------


def next_safe_actions(readiness: dict) -> dict:
    """The next step for EACH capability, independently.

    Previously one string covered everything, so a missing FCI limit told an
    operator working on ACCIONES to go configure FCI. The capabilities do not
    depend on each other and their instructions must not either.
    """
    return {
        "acciones": _securities_next_action(readiness, "acciones"),
        "cedears": _securities_next_action(readiness, "cedears"),
        "fci_subscription": _fci_next_action(readiness, "subscription"),
        "fci_redemption": _fci_next_action(readiness, "redemption"),
    }


def _securities_next_action(readiness: dict, class_key: str) -> str:
    if not readiness.get("catalog_available") or readiness.get("catalog_total", 0) == 0:
        return "resolve_instruments"

    block = readiness.get(class_key) or {}
    if not block.get("policy_configured"):
        return f"configure_class_policy_{class_key}"
    if block.get("blocked_symbols"):
        return "verify_instrument_fields"
    if not block.get("covered_symbols"):
        return "resolve_instruments"
    if not readiness.get("order_execution_enabled"):
        # Everything technical is done and the lock is shut — the intended
        # resting state before a pilot, not a problem to fix.
        return f"ready_for_controlled_{class_key}_pilot"
    return "run_controlled_pilot"


def _fci_next_action(readiness: dict, capability: str) -> str:
    fci = readiness.get("fci_capability") or {}
    funds = readiness.get("fci") or {}
    if not fci.get("limits_configured"):
        return "configure_fci_limits"
    if not funds.get("verified_funds"):
        return "verify_fund"
    if not fci.get(f"{capability}_flag_enabled"):
        return f"enable_fci_{capability}_flag"
    if not readiness.get("order_execution_enabled"):
        return "run_sandbox_validation"
    return f"ready_for_controlled_fci_{capability}"


def next_safe_action(readiness: dict) -> str:
    """Single headline action, kept for compatibility.

    Derived from the per-capability map so the two can never disagree:
    securities first, because that is the path being activated.
    """
    actions = next_safe_actions(readiness)
    for key in ("acciones", "cedears"):
        value = actions[key]
        if not value.startswith("ready_for_") and value != "run_controlled_pilot":
            return value
    for key in ("fci_subscription", "fci_redemption"):
        value = actions[key]
        if not value.startswith("ready_for_") and value != "run_sandbox_validation":
            return value
    return actions["acciones"]
