"""Instrument capability reporting and execution-catalog refresh.

Answers, for every instrument the user could plausibly want to trade, the
only question that matters operationally: *can this be bought, can it be
sold, and if not, exactly why?*

Strictly read-only with respect to orders. The refresh writes catalog rows
from READ-ONLY broker data (the live portfolio); it never places, cancels or
approves anything.
"""

from __future__ import annotations

import secrets as _secrets

from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.execution_class import (
    FAMILY_FUND,
    FAMILY_SECURITIES,
    load_class_policies,
    resolve_execution_class,
)
from app.broker.execution_scope import (
    evaluate_order_authorization,
    load_authorization_context,
    securities_buy_enabled,
    sell_capability_enabled,
)
from app.broker.instrument_catalog import (
    catalog_entry_status,
    catalog_to_dict,
    get_instrument,
    list_catalog,
    normalize_symbol,
    refresh_catalog_from_positions,
)
from app.core.config import get_settings
from app.models.models import PortfolioSnapshot
from app.services.logs import app_log

# Codes that only reflect the probe's synthetic size, not the instrument.
_SIZING_CODES = {
    "quantity_step_mismatch",
    "minimum_quantity_not_met",
    "symbol_quantity_limit_exceeded",
    "symbol_notional_limit_exceeded",
}


def _probe(db: Session, symbol: str, side: str, position, settings, context) -> list[str]:
    """Authorisation codes for a symbol/side, ignoring size-only verdicts."""
    order = {
        "symbol": symbol,
        "side": side,
        "quantity_planned": 0,
        "estimated_notional": 0,
    }
    _, codes = evaluate_order_authorization(
        db, order=order, position=position, settings=settings, context=context
    )
    return [c for c in codes if c not in _SIZING_CODES]


def _price_tick_report(entry) -> dict:
    """How this instrument's price decimals are decided.

    `effective_price_tick` is deliberately absent here: it depends on a price,
    and this report has no live quote. Inventing one — say from the last
    observed price — would put a number in front of an operator that the
    order path would not necessarily agree with.
    """
    from app.broker.instrument_catalog import VERIFYING_PROVENANCES, verified_price_tick_rule
    from app.broker.price_rules import describe_rule

    if entry is None:
        return {
            "fixed_price_tick_verified": False,
            "dynamic_price_tick_rule_verified": False,
            "price_tick_rule": None,
            "price_tick_mode": None,
        }
    provenance = entry.field_provenance or {}
    fixed_verified = (
        entry.price_tick is not None
        and provenance.get("price_tick") in VERIFYING_PROVENANCES
    )
    rule = verified_price_tick_rule(entry)
    return {
        "fixed_price_tick": entry.price_tick,
        "fixed_price_tick_verified": bool(fixed_verified),
        "dynamic_price_tick_rule_verified": rule is not None,
        "price_tick_rule": rule,
        "price_tick_rule_detail": describe_rule(rule),
        "price_tick_observed": getattr(entry, "price_tick_observed", None),
        "price_tick_mode": "dynamic" if rule else ("fixed" if fixed_verified else None),
        # Said explicitly so nobody reads its absence as zero.
        "effective_price_tick_requires_live_quote": rule is not None,
    }


def build_instrument_capabilities(db: Session) -> dict:
    """Read-only capability matrix. Contains no secrets and no credentials."""
    settings = get_settings()
    context = load_authorization_context(settings)
    class_policies, class_errors = load_class_policies(settings)

    snapshot = (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )
    positions_by_symbol = {}
    if snapshot:
        for position in snapshot.positions:
            positions_by_symbol[normalize_symbol(position.symbol)] = position

    # Union of everything the user could act on: held instruments and every
    # catalog entry. A held instrument missing from the catalog is exactly
    # the gap this endpoint exists to surface.
    catalog_entries = {normalize_symbol(e.broker_symbol): e for e in list_catalog(db)}
    symbols = sorted(set(positions_by_symbol) | set(catalog_entries))

    items = []
    for symbol in symbols:
        position = positions_by_symbol.get(symbol)
        entry = catalog_entries.get(symbol)
        policy = class_policies.get(entry.execution_class) if entry else None
        catalog_code, catalog_details = catalog_entry_status(
            entry, max_age_seconds=(policy or {}).get("catalog_max_age_seconds")
        )

        detected_class = (
            entry.execution_class
            if entry and entry.execution_class
            else resolve_execution_class(getattr(position, "asset_type", None))
        )
        family = entry.execution_family if entry else None
        if not family and detected_class:
            from app.broker.execution_class import execution_family_of

            family = execution_family_of(detected_class)

        buy_codes = _probe(db, symbol, "buy", position, settings, context)
        sell_codes = _probe(db, symbol, "sell", position, settings, context)

        missing = list(catalog_details.get("missing_fields", []))
        if entry is None:
            missing = ["catalog_entry"]

        items.append({
            "symbol": symbol,
            "in_portfolio": position is not None,
            "held_quantity": getattr(position, "quantity", None),
            "held_currency": getattr(position, "currency", None),
            "detected_class": detected_class,
            "execution_family": family,
            "identity": catalog_to_dict(entry) if entry else None,
            "catalog_status": catalog_code or "ok",
            "missing_fields": missing,
            # How this instrument's decimals get priced. The two are mutually
            # exclusive in practice and reported apart so an operator can see
            # WHICH one is missing: a fixed tick and a verified rule are
            # different pieces of work.
            **_price_tick_report(entry),
            "buy_ready": not buy_codes,
            "sell_ready": not sell_codes,
            "buy_blocking_reasons": buy_codes,
            "sell_blocking_reasons": sell_codes,
            # A fund is structurally non-executable through this app.
            "manual_operation_required": family == FAMILY_FUND,
        })

    from app.services.fci import get_fci_capability

    securities = [i for i in items if i["execution_family"] == FAMILY_SECURITIES]
    return {
        "generated_from_snapshot_id": snapshot.id if snapshot else None,
        "capabilities": {
            "securities_buy_enabled": securities_buy_enabled(settings),
            "securities_sell_enabled": sell_capability_enabled(settings),
            "fci": get_fci_capability(),
        },
        "class_policies_configured": sorted(class_policies.keys()),
        "class_policy_errors": class_errors,
        "authorization_errors": context["errors"],
        "totals": {
            "instruments": len(items),
            "in_portfolio": sum(1 for i in items if i["in_portfolio"]),
            "buy_ready": sum(1 for i in items if i["buy_ready"]),
            "sell_ready": sum(1 for i in items if i["sell_ready"]),
            "securities": len(securities),
            "funds": sum(1 for i in items if i["execution_family"] == FAMILY_FUND),
            "blocked": sum(1 for i in items if not i["buy_ready"] and not i["sell_ready"]),
        },
        "instruments": items,
    }


def refresh_execution_catalog(db: Session, *, execution_key: str | None) -> dict:
    """Rebuild catalog identity from the READ-ONLY live portfolio.

    Requires the execution credential: the catalog is what establishes an
    instrument's identity, so writing it is an administrative act even though
    no order can result from it.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return {
            "error": "Refresh bloqueado: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    class_policies, class_errors = load_class_policies(settings)
    if not class_policies:
        return {
            "error": (
                "No hay políticas por clase configuradas: el catálogo no puede "
                "determinar mercado, plazo ni límites."
            ),
            "code": "class_policy_not_configured",
            "status_code": 423,
        }

    # READ-ONLY broker access. Deliberately uses the portfolio snapshot
    # contract, which is the same call the analysis cycle already makes.
    from app.services.orchestrator import _get_broker

    try:
        broker = _get_broker()
        live = broker.get_portfolio_snapshot() or {}
    except Exception as exc:
        return {
            "error": f"No se pudo leer la cartera para refrescar el catálogo: {str(exc)[:200]}",
            "code": "portfolio_read_failed",
            "status_code": 502,
        }

    result = refresh_catalog_from_positions(
        db, live.get("positions") or [], settings=settings
    )
    app_log(db, "Catálogo de ejecución refrescado (read-only)", context={
        "created": result["created"],
        "updated": result["updated"],
        "skipped": len(result["skipped"]),
        "identity_changed": result["identity_changed"],
    })
    db.commit()
    return {**result, "class_policy_errors": class_errors}


# ---------------------------------------------------------------------------
# Administrative verification actions (read-only w.r.t. orders)
# ---------------------------------------------------------------------------


def _require_execution_key(execution_key: str | None) -> dict | None:
    settings = get_settings()
    if not settings.execution_admin_key:
        return {
            "error": "Bloqueado: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }
    return None


def decide_identity_change(
    db: Session, symbol: str, *, execution_key: str | None, decision: str, note: str = ""
) -> dict:
    """Accept or reject a detected identity change. Never sends an order."""
    from app.broker.instrument_catalog import (
        STATUS_IDENTITY_CHANGED,
        accept_identity_change,
        reject_identity_change,
        verification_status_of,
    )

    guard = _require_execution_key(execution_key)
    if guard:
        return guard
    if decision not in ("accept", "reject"):
        return {"error": f"Decisión inválida: '{decision}'.",
                "code": "invalid_identity_decision", "status_code": 422}

    entry = get_instrument(db, symbol)
    if entry is None:
        return {"error": "Instrument not found in execution catalog", "status_code": 404}
    if verification_status_of(entry) != STATUS_IDENTITY_CHANGED:
        return {"error": "El instrumento no tiene un cambio de identidad pendiente.",
                "code": "no_pending_identity_change", "status_code": 409}

    result = (
        accept_identity_change(entry, note=note) if decision == "accept"
        else reject_identity_change(entry, note=note)
    )
    app_log(db, "Decisión sobre cambio de identidad de instrumento", context={
        "symbol": entry.broker_symbol,
        "decision": decision,
        "resulting_status": result.get("verification_status"),
        "source": "manual_user",
    })
    db.commit()
    return {
        "symbol": entry.broker_symbol,
        "decision": decision,
        **result,
        "unverified_fields": _unverified(entry),
        "message": (
            "Identidad aceptada. El instrumento vuelve a 'candidate': lo verificado "
            "bajo la identidad anterior ya no aplica."
            if decision == "accept"
            else "Identidad rechazada. El instrumento queda inoperable hasta re-resolverlo."
        ),
    }


def verify_instrument_fields(
    db: Session, symbol: str, *, execution_key: str | None, fields: dict, note: str = ""
) -> dict:
    """Administratively verify tick/step/capabilities for ONE instrument."""
    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE, verify_fields

    guard = _require_execution_key(execution_key)
    if guard:
        return guard

    entry = get_instrument(db, symbol)
    if entry is None:
        return {"error": "Instrument not found in execution catalog", "status_code": 404}
    if not isinstance(fields, dict) or not fields:
        return {"error": "Se requiere al menos un campo a verificar.",
                "code": "no_fields_provided", "status_code": 422}

    result = verify_fields(
        entry, fields=fields, provenance_source=PROV_ADMIN_OVERRIDE, note=note
    )
    if not result.get("changed"):
        return {"error": f"No se pudo verificar: {result.get('reason')}.",
                "code": result.get("reason", "verification_failed"), "status_code": 422}

    app_log(db, "Verificación administrativa de campos de instrumento", context={
        "symbol": entry.broker_symbol,
        "fields": sorted(result.get("applied", {}).keys()),
        "source": "manual_user",
    })
    db.commit()
    return {"symbol": entry.broker_symbol, **result}


def resolve_instruments(
    db: Session, *, symbols: list[str], execution_key: str | None
) -> dict:
    """Resolve not-held instruments read-only into CANDIDATE entries."""
    from app.broker.instrument_resolver import resolve_missing_instruments

    guard = _require_execution_key(execution_key)
    if guard:
        return guard

    settings = get_settings()
    from app.services.orchestrator import _get_broker

    try:
        broker = _get_broker()
    except Exception as exc:
        return {"error": f"No se pudo instanciar el broker: {str(exc)[:200]}",
                "code": "broker_unavailable", "status_code": 502}

    return resolve_missing_instruments(
        db, symbols=symbols or [], broker=broker, settings=settings
    )


def account_status_diagnostic(*, execution_key: str | None) -> dict:
    """Normalized estadocuenta diagnosis. Never returns the raw payload."""
    from app.broker.account_status import diagnose_account_status

    guard = _require_execution_key(execution_key)
    if guard:
        return guard

    from app.services.orchestrator import _get_broker

    try:
        broker = _get_broker()
        payload = broker.get_account_status_raw()
    except Exception as exc:
        return {"error": f"No se pudo leer el estado de cuenta: {str(exc)[:200]}",
                "code": "account_status_unavailable", "status_code": 502}

    diagnosis = diagnose_account_status(payload)
    # Deliberately NOT echoing the payload: it carries account numbers.
    return {**diagnosis, "raw_payload_returned": False}


def _unverified(entry) -> list[str]:
    from app.broker.instrument_catalog import unverified_fields

    return unverified_fields(entry)
