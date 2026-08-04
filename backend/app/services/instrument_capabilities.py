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
