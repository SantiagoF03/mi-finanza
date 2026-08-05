"""Technical readiness for controlled execution pilots. READ-ONLY.

What this module answers: *could* this app send an order for this symbol
without hitting a blocker? It never answers *should* — there is no view here
on whether an instrument is a good investment, and the suggested sizes are
technical ceilings (one lot, the smallest limit that applies), not advice.

Nothing here sends, approves, enables a flag or writes to the catalog. The
only side effect any of it may have is reading a quote, and even that is
optional: without a broker the report still comes back, with the quote-derived
answers reported as unavailable instead of assumed.

The separation matters because "the app is ready" is not one fact. Buying and
selling are separate capabilities, ACCIONES and CEDEARS are separate classes
with separate policies, and one symbol being ready says nothing about the
class being ready.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

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
from app.core.config import get_settings

logger = logging.getLogger(__name__)

SECURITIES_CLASSES = (CLASS_ACCIONES, CLASS_CEDEARS)

# Ordered from "nothing works yet" to "everything technical is done". The
# first unmet step is the next safe action — deterministic, never a judgement
# call, so two operators reading the same state get the same instruction.
NEXT_ACTION_ORDER = (
    "resolve_instruments",
    "verify_instrument_fields",
    "configure_class_policies",
    "configure_fci_limits",
    "verify_fund",
    "run_sandbox_validation",
    "ready_for_controlled_pilot",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _quote_probe(broker, symbol: str, side: str, market: str, settlement: str) -> dict:
    """Read ONE side of the book. Never raises, never orders.

    Returns the evidence, not a verdict: `available` says a usable price for
    THIS side exists, `source` says which side answered. A provider replying
    with the wrong side is not evidence for the side we asked about.
    """
    probe = {
        "requested_side": side,
        "expected_source": "bid" if side == "sell" else "ask",
        "available": False,
        "price": None,
        "source": None,
        "age_seconds": None,
        "error": None,
    }
    if broker is None:
        probe["error"] = "broker_unavailable"
        return probe
    try:
        quote = broker.get_execution_quote(symbol, side, market, settlement) or {}
    except Exception as exc:  # a probe must never break the report
        probe["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
        return probe

    probe["source"] = quote.get("source")
    probe["price"] = quote.get("price")
    retrieved_at = quote.get("retrieved_at")
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


def _side_blockers(
    *,
    side: str,
    entry,
    catalog_code: str | None,
    policy: dict | None,
    policy_codes: list[str],
    probe: dict,
    settings,
    denylisted: bool,
) -> list[str]:
    """Stable codes explaining why one SIDE cannot be piloted yet."""
    from app.broker.execution_scope import (
        securities_buy_enabled,
        sell_capability_enabled,
    )

    reasons: list[str] = []
    if denylisted:
        reasons.append("instrument_denylisted")
    if catalog_code:
        reasons.append(catalog_code)
    if entry is not None and entry.execution_family != FAMILY_SECURITIES:
        reasons.append("fund_requires_fci_contract")
    if entry is not None and entry.execution_class not in SECURITIES_CLASSES:
        reasons.append("instrument_class_unsupported")

    if policy is None:
        reasons.append("class_policy_not_configured")
    else:
        reasons.extend(policy_codes)
        if side == "buy" and not policy.get("buy_enabled"):
            reasons.append("class_buy_disabled")
        if side == "sell" and not policy.get("sell_enabled"):
            reasons.append("class_sell_disabled")

    # Per-side evidence FROM THE CATALOG. Never derived from the other side:
    # being able to sell something says nothing about being able to buy it.
    if entry is not None:
        if side == "buy" and not bool(entry.buy_supported):
            reasons.append("instrument_buy_unsupported")
        if side == "sell" and not bool(entry.sell_supported):
            reasons.append("instrument_sell_unsupported")

    # Per-side evidence FROM THE BOOK, right now.
    if not probe.get("available"):
        reasons.append(
            "quote_wrong_side" if probe.get("error") == "quote_wrong_side"
            else "quote_unavailable"
        )

    # Capability flags. Reported as blockers because they genuinely block —
    # but they are configuration, not a missing fact about the instrument.
    if side == "buy" and not securities_buy_enabled(settings):
        reasons.append("buy_execution_disabled")
    if side == "sell" and not sell_capability_enabled(settings):
        reasons.append("sell_execution_disabled")

    # Deduplicate, preserving order: the first code is the headline reason.
    seen: set[str] = set()
    ordered: list[str] = []
    for code in reasons:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


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
        "quantity_step": entry.quantity_step if entry is not None else None,
        "price_tick": entry.price_tick if entry is not None else None,
        "notes": [],
    }
    if entry is None:
        result["notes"].append("instrument_not_in_catalog")
        return result

    step = entry.quantity_step
    if step is None:
        result["notes"].append("quantity_step_unverified")
    elif float(step) <= 1.0:
        result["suggested_pilot_max_quantity"] = 1
    else:
        # One step IS the smallest valid order here.
        result["suggested_pilot_max_quantity"] = float(step)
        result["notes"].append("minimum_order_is_one_step")

    quantity = result["suggested_pilot_max_quantity"]
    if quantity is not None and probe_price:
        try:
            notional = float(quantity) * float(probe_price)
        except (TypeError, ValueError):
            notional = None
        if notional is not None:
            caps = [notional]
            if policy:
                for key in ("max_order_notional",):
                    limit = policy.get(key)
                    if limit:
                        caps.append(float(limit))
            result["suggested_pilot_max_notional"] = round(min(caps), 2)
            if policy and min(caps) < notional:
                result["notes"].append("capped_by_class_policy")
    elif quantity is not None:
        result["notes"].append("no_executable_price_to_size_with")
    return result


def evaluate_pilot_readiness(
    db: Session,
    *,
    symbols: list[str] | None = None,
    broker=None,
    settings=None,
) -> dict:
    """Technical readiness per symbol and per class. Sends nothing.

    `symbols` restricts the report; without it every catalogued securities
    instrument is evaluated. Symbols that are not in the catalog are reported
    as such rather than skipped — "we have never resolved this" is itself the
    answer an operator needs.
    """
    settings = settings or get_settings()
    class_policies, class_errors = load_class_policies(settings)
    overrides, override_errors = load_instrument_overrides(settings)
    denylist = load_denylist(settings)
    allow_increase = bool(getattr(settings, "execution_allow_override_limit_increase", False))

    requested = [normalize_symbol(s) for s in (symbols or []) if normalize_symbol(s)]
    if requested:
        entries = [(sym, get_instrument(db, sym)) for sym in requested]
    else:
        entries = [
            (entry.broker_symbol, entry)
            for entry in list_catalog(db)
            if entry.execution_class in SECURITIES_CLASSES
        ]

    reports: list[dict] = []
    for symbol, entry in entries:
        execution_class = entry.execution_class if entry is not None else None
        base_policy = class_policies.get(execution_class or "")
        # THE resolver — class policy + override + denylist, same one the
        # order path uses. Duplicating that logic here is how a readiness
        # report ends up disagreeing with the code that actually decides.
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
            max_age_seconds=(base_policy or {}).get("catalog_max_age_seconds"),
        )

        market = (entry.market if entry is not None else "") or settings.iol_order_market or ""
        settlement = (
            (entry.settlement if entry is not None else "")
            or settings.iol_order_settlement
            or ""
        )
        buy_probe = _quote_probe(broker, symbol, "buy", market, settlement)
        sell_probe = _quote_probe(broker, symbol, "sell", market, settlement)

        denylisted = symbol in denylist
        buy_reasons = _side_blockers(
            side="buy", entry=entry, catalog_code=catalog_code, policy=policy,
            policy_codes=policy_codes, probe=buy_probe, settings=settings,
            denylisted=denylisted,
        )
        sell_reasons = _side_blockers(
            side="sell", entry=entry, catalog_code=catalog_code, policy=policy,
            policy_codes=policy_codes, probe=sell_probe, settings=settings,
            denylisted=denylisted,
        )

        # Size off the side that can actually be priced; prefer the sell side
        # because that is the conservative one to pilot first.
        reference_price = sell_probe.get("price") or buy_probe.get("price")
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
            "buy": {
                "technically_ready": not buy_reasons,
                "blocking_reasons": buy_reasons,
                "quote": buy_probe,
            },
            "sell": {
                "technically_ready": not sell_reasons,
                "blocking_reasons": sell_reasons,
                "quote": sell_probe,
            },
            **sizing,
            "manual_configuration_required": _manual_configuration_required(
                entry, policy, settings
            ),
        })

    return {
        "generated_at": _utcnow().isoformat(),
        "read_only": True,
        "sends_orders": False,
        "requested_symbols": requested or None,
        "class_policy_errors": list(class_errors) + list(override_errors),
        "symbols": reports,
        "classes": _class_rollup(reports, class_policies, settings),
        "disclaimer": (
            "Aptitud TÉCNICA únicamente. No es una recomendación de inversión "
            "y los montos sugeridos son topes técnicos, no objetivos."
        ),
    }


def _class_rollup(reports: list[dict], class_policies: dict, settings) -> dict:
    """Per-class readiness. A class is NEVER ready because one symbol is.

    Reported both ways on purpose: `covered_symbols` says which instruments
    could be piloted, `ready` says whether the class as a whole is configured.
    Conflating them is how one working symbol gets read as "we can trade
    ACCIONES now".
    """
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
            # Requires the policy AND the flag AND at least one instrument that
            # actually passes. Any one of the three missing means not ready.
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
# Policy template — a draft for a human to review, never a written config
# ---------------------------------------------------------------------------

# Conservative starting limits. Deliberately small: a pilot exists to prove the
# path works end to end, not to take a position.
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
    """A reviewable JSON draft for the execution env vars. Writes nothing.

    Rules it follows, all for the same reason — an invented number in a policy
    is indistinguishable from a verified one once it is in the config:

    - tick and step come from the catalog or come back `null`;
    - `buy_enabled` and `sell_enabled` are ALWAYS false in the draft;
    - currency/market/settlement are copied from the observed identity, never
      defaulted;
    - ACCIONES and CEDEARS get separate blocks even when their numbers match.
    """
    settings = settings or get_settings()
    requested = [normalize_symbol(s) for s in (symbols or []) if normalize_symbol(s)]
    if requested:
        entries = [e for e in (get_instrument(db, s) for s in requested) if e is not None]
        missing = [s for s in requested if get_instrument(db, s) is None]
    else:
        entries = [e for e in list_catalog(db) if e.execution_class in SECURITIES_CLASSES]
        missing = []

    warnings: list[str] = [
        "Plantilla para REVISIÓN humana. No escribe ninguna variable ni toca Railway.",
        "buy_enabled y sell_enabled vienen en false a propósito: habilitarlos es "
        "una decisión aparte, después de revisar los límites.",
        "Un valor null es un dato NO VERIFICADO. Completarlo a mano equivale a "
        "afirmarlo: si no lo verificaste, dejalo en null y el instrumento "
        "queda bloqueado, que es el resultado correcto.",
    ]
    if missing:
        warnings.append(
            f"Símbolos sin entrada de catálogo (resolvelos read-only primero): "
            f"{', '.join(sorted(missing))}"
        )

    class_block: dict = {}
    instrument_overrides: dict = {}
    for execution_class in SECURITIES_CLASSES:
        members = [e for e in entries if e.execution_class == execution_class]
        if not members:
            continue

        # Identity is copied from what was OBSERVED, per class, deduplicated.
        currencies = sorted({(e.currency or "").upper() for e in members if e.currency})
        markets = sorted({(e.market or "").lower() for e in members if e.market})
        settlements = sorted({(e.settlement or "").lower() for e in members if e.settlement})
        if not currencies:
            warnings.append(f"{execution_class}: ninguna moneda observada; completá a mano.")
        if not markets:
            warnings.append(f"{execution_class}: ningún mercado observado; completá a mano.")
        if not settlements:
            warnings.append(f"{execution_class}: ningún plazo observado; completá a mano.")

        # Tick and step are per INSTRUMENT, so the class block carries no
        # default for them — a class-wide tick is a guess about every member.
        class_block[execution_class] = {
            "buy_enabled": False,
            "sell_enabled": False,
            "currencies": currencies or None,
            "markets": markets or None,
            "settlements": settlements or None,
            "max_order_notional": PILOT_MAX_ORDER_NOTIONAL,
            "max_daily_notional": PILOT_MAX_DAILY_NOTIONAL,
            "max_quantity": PILOT_MAX_QUANTITY,
            "max_portfolio_pct": PILOT_MAX_PORTFOLIO_PCT,
            "min_cash_reserve": PILOT_MIN_CASH_RESERVE,
            "fee_buffer_pct": PILOT_FEE_BUFFER_PCT,
            "max_quote_age_seconds": PILOT_MAX_QUOTE_AGE_SECONDS,
            "max_price_deviation_pct": PILOT_MAX_PRICE_DEVIATION_PCT,
            "catalog_max_age_seconds": PILOT_CATALOG_MAX_AGE_SECONDS,
            "validity_minutes": PILOT_VALIDITY_MINUTES,
            "default_quantity_step": None,
            "default_price_tick": None,
            "order_type": settings.iol_order_type or "precioLimite",
        }

        for entry in members:
            override = {
                # Never larger than the class value: an override may only
                # tighten unless an administrative escape says otherwise.
                "max_quantity": (
                    1.0 if (entry.quantity_step is None or float(entry.quantity_step) <= 1.0)
                    else float(entry.quantity_step)
                ),
                "price_tick": entry.price_tick,
                "quantity_step": entry.quantity_step,
            }
            if entry.price_tick is None or entry.quantity_step is None:
                warnings.append(
                    f"{entry.broker_symbol}: tick o step sin verificar. "
                    "Verificalos con /api/broker/instruments/{symbol}/verify-fields."
                )
            instrument_overrides[entry.broker_symbol] = override

    return {
        "generated_at": _utcnow().isoformat(),
        "read_only": True,
        "writes_configuration": False,
        "EXECUTION_CLASS_POLICIES": class_block,
        "EXECUTION_INSTRUMENT_OVERRIDES": instrument_overrides,
        "EXECUTION_DENYLIST": sorted(load_denylist(settings)),
        "warnings": warnings,
        "next_step": (
            "Revisá los null, completá sólo lo verificado, y recién entonces "
            "cargá las variables a mano. Este endpoint no las escribe."
        ),
    }


def next_safe_action(readiness: dict) -> str:
    """The single next step, derived from blockers. Deterministic.

    Not a suggestion engine: given the same state it always returns the same
    string, and every branch is a fact about configuration, never a judgement.
    """
    if not readiness.get("catalog_available"):
        return "resolve_instruments"
    if readiness.get("catalog_total", 0) == 0:
        return "resolve_instruments"

    blocked = readiness.get("blocked_instruments") or []
    if any(
        "instrument_catalog_incomplete" in (b.get("reasons") or [])
        or "instrument_not_verified" in (b.get("reasons") or [])
        for b in blocked
    ):
        return "verify_instrument_fields"

    if not readiness.get("class_policies_configured"):
        return "configure_class_policies"

    fci = readiness.get("fci_capability") or {}
    if not fci.get("limits_configured"):
        return "configure_fci_limits"
    if not (readiness.get("fci") or {}).get("verified_funds"):
        return "verify_fund"

    if not readiness.get("order_execution_enabled"):
        # Everything technical is done and the lock is still shut — which is
        # the intended resting state, not a problem to fix.
        return "run_sandbox_validation"
    return "ready_for_controlled_pilot"
