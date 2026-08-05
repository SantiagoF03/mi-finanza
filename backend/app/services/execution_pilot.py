"""Execution pilot — administrative creation of ONE controlled recommendation.

This module can ONLY create a Recommendation + RecommendationAction for a
single-unit BYMA sell. It is deliberately incapable of sending anything:

- it never instantiates a broker;
- it never fetches a quote;
- it never calls place_order or submit_order_request;
- it never approves, and it never flips any execution lock.

After creation the operator keeps using the existing, unchanged path:
    GET  /api/recommendations/{id}/execution-preview
    POST /api/recommendations/{id}/approve   (still gated by
                                              ORDER_EXECUTION_ENABLED)

Double lock: creation requires EXECUTION_PILOT_CREATION_ENABLED=true AND
ORDER_EXECUTION_ENABLED=false — the pilot may only be *prepared* while real
sending is blocked.
"""

from __future__ import annotations

import secrets as _secrets
from datetime import datetime, timezone

from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.instrument_scope import load_instrument_policies, normalize_symbol
from app.broker.numeric import positive_decimal
from app.core.config import get_settings
from app.services.analysis_gate import (
    acquire_analysis_lease,
    check_recommendation_creation_allowed,
    release_analysis_lease,
)
from app.models.models import (
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.logs import app_log

PILOT_SYMBOL = "BYMA"
PILOT_SIDE = "sell"
PILOT_QUANTITY = 1
PILOT_CONFIRMATION = "CREAR PILOTO BYMA 1"

# Identity the snapshot position must have for the pilot to be created.
PILOT_ASSET_TYPE = "ACCIONES"
PILOT_INSTRUMENT_TYPE = "ACCIONES"
PILOT_CURRENCY = "ARS"


def _err(message: str, code: str, status_code: int) -> dict:
    return {"error": message, "code": code, "status_code": status_code}


def create_execution_pilot_recommendation(
    db: Session,
    *,
    execution_key: str | None,
    symbol: str | None,
    side: str | None,
    quantity,
    confirmation_text: str | None,
    note: str = "",
) -> dict:
    """Create the BYMA pilot recommendation. Never sends an order.

    Every validation runs BEFORE any write. On any failure nothing is
    persisted.
    """
    settings = get_settings()

    # --- Credential gates first (no information leak about DB state) ---
    if not settings.execution_admin_key:
        return _err(
            "Creación de piloto bloqueada: credencial de ejecución no configurada en el servidor.",
            "execution_admin_key_not_configured", 423,
        )
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return _err("Credencial de ejecución inválida o ausente.", "invalid_execution_key", 403)

    # --- Double lock ---
    if not settings.execution_pilot_creation_enabled:
        return _err(
            "Creación de piloto deshabilitada (EXECUTION_PILOT_CREATION_ENABLED=false).",
            "execution_pilot_creation_disabled", 423,
        )
    if settings.order_execution_enabled:
        return _err(
            "El piloto solo puede prepararse mientras el envío real está bloqueado "
            "(ORDER_EXECUTION_ENABLED debe ser false).",
            "order_execution_must_be_disabled", 423,
        )

    # --- Exact confirmation phrase ---
    if not confirmation_text or confirmation_text.strip() != PILOT_CONFIRMATION:
        return _err(
            f"Confirmación incorrecta. Frase requerida exacta: '{PILOT_CONFIRMATION}'.",
            "confirmation_mismatch", 422,
        )

    # --- Strictly literal payload: no implicit values ---
    if normalize_symbol(symbol) != PILOT_SYMBOL:
        return _err(
            f"Solo se admite el símbolo {PILOT_SYMBOL} (recibido: {symbol!r}).",
            "pilot_symbol_not_allowed", 422,
        )
    if (side or "").strip().lower() != PILOT_SIDE:
        return _err(
            f"Solo se admite side={PILOT_SIDE} (recibido: {side!r}).",
            "pilot_side_not_allowed", 422,
        )
    qty = positive_decimal(quantity)
    if qty is None or qty != PILOT_QUANTITY:
        return _err(
            f"Solo se admite quantity={PILOT_QUANTITY} (recibido: {quantity!r}).",
            "pilot_quantity_not_allowed", 422,
        )

    # --- Snapshot identity and holding ---
    snapshot = (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )
    if not snapshot:
        return _err("No hay snapshot de portfolio disponible.", "snapshot_missing", 409)

    position = next(
        (p for p in snapshot.positions if normalize_symbol(p.symbol) == PILOT_SYMBOL), None
    )
    if position is None:
        return _err(
            f"{PILOT_SYMBOL} no está en el último snapshot de portfolio.",
            "pilot_position_missing", 409,
        )
    if (position.asset_type or "") != PILOT_ASSET_TYPE or \
       (position.instrument_type or "") != PILOT_INSTRUMENT_TYPE:
        return _err(
            f"La identidad de {PILOT_SYMBOL} en el snapshot no coincide con la esperada "
            f"({PILOT_ASSET_TYPE}/{PILOT_INSTRUMENT_TYPE}).",
            "pilot_identity_mismatch", 409,
        )
    if (position.currency or "") != PILOT_CURRENCY:
        return _err(
            f"La moneda de {PILOT_SYMBOL} no es {PILOT_CURRENCY}.",
            "pilot_currency_mismatch", 409,
        )
    held = positive_decimal(position.quantity)
    if held is None or held < PILOT_QUANTITY:
        return _err(
            f"La tenencia de {PILOT_SYMBOL} es insuficiente para vender {PILOT_QUANTITY}.",
            "pilot_position_insufficient", 409,
        )

    # --- Instrument policy allowlist ---
    policies, policy_errors = load_instrument_policies(settings)
    if policy_errors:
        return _err("Política de instrumentos inválida.", "instrument_policy_invalid", 423)
    policy = policies.get(PILOT_SYMBOL)
    if policy is None:
        return _err(
            f"{PILOT_SYMBOL} no está autorizado en EXECUTION_INSTRUMENT_POLICIES.",
            "instrument_policy_missing", 423,
        )
    if policy["max_quantity"] < PILOT_QUANTITY:
        return _err(
            f"La política de {PILOT_SYMBOL} permite como máximo {policy['max_quantity']}.",
            "symbol_quantity_limit_exceeded", 422,
        )
    step = policy["quantity_step"]
    if step <= 0 or abs((PILOT_QUANTITY / step) - round(PILOT_QUANTITY / step)) > 1e-9:
        return _err(
            f"La cantidad {PILOT_QUANTITY} no respeta el quantity_step {step}.",
            "quantity_step_mismatch", 422,
        )

    # --- Central mutual exclusion: same lease and same gate as run_cycle ---
    # The pilot is a Recommendation like any other, so it must not be able to
    # create a second open one behind the analysis cycle's back.
    lease_owner, lease_error = acquire_analysis_lease(db)
    if lease_owner is None:
        return _err(
            "Otro ciclo de análisis está en curso. Reintentá en unos segundos.",
            lease_error or "analysis_lease_unavailable", 409,
        )

    try:
        gate = check_recommendation_creation_allowed(db)
        if not gate.allowed:
            return _err(
                (gate.detail or "Hay una decisión pendiente.")
                + " Resolvela explícitamente antes de crear el piloto.",
                gate.code or "open_recommendation_requires_decision", 409,
            )
        return _create_pilot_locked(db, snapshot, position, note, lease_owner)
    finally:
        release_analysis_lease(db, lease_owner)


def _create_pilot_locked(db: Session, snapshot, position, note: str, lease_owner: str) -> dict:
    """Persist the pilot while holding the lease. No broker, no quote, no order."""
    settings = get_settings()
    rec = Recommendation(
        action="rebalancear",
        status="pending",
        suggested_pct=0.0,
        confidence=1.0,
        rationale=(
            f"Piloto administrativo de ejecución controlada: vender {PILOT_QUANTITY} "
            f"{PILOT_SYMBOL}. Creado manualmente, sin envío de orden."
        ),
        risks="Piloto de ejecución real controlado. Requiere aprobación manual explícita.",
        executive_summary=f"[PILOTO] Vender {PILOT_QUANTITY} {PILOT_SYMBOL}",
        metadata_json={
            "execution_pilot": True,
            "pilot_symbol": PILOT_SYMBOL,
            "pilot_side": PILOT_SIDE,
            "pilot_quantity": PILOT_QUANTITY,
            "created_manually": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_id": snapshot.id,
            "note": (note or "").strip(),
        },
    )
    db.add(rec)
    db.flush()

    action = RecommendationAction(
        recommendation_id=rec.id,
        symbol=PILOT_SYMBOL,
        # Informative only: the executed quantity comes from quantity_override.
        target_change_pct=-0.0001,
        reason=f"Piloto controlado: venta explícita de {PILOT_QUANTITY} {PILOT_SYMBOL}.",
        quantity_override=PILOT_QUANTITY,
    )
    db.add(action)
    db.flush()

    # No supersession: the gate above guarantees there was no other open
    # recommendation. A pilot never silently discards a pending human
    # decision — it is blocked instead.
    app_log(db, "Recomendación piloto de ejecución creada manualmente", context={
        "recommendation_id": rec.id,
        "symbol": PILOT_SYMBOL,
        "side": PILOT_SIDE,
        "quantity": PILOT_QUANTITY,
        "snapshot_id": snapshot.id,
    })
    db.commit()

    return {
        "recommendation_id": rec.id,
        "recommendation_action_id": action.id,
        "status": rec.status,
        "execution_pilot": True,
        "symbol": PILOT_SYMBOL,
        "side": PILOT_SIDE,
        "quantity": PILOT_QUANTITY,
        "quantity_override": PILOT_QUANTITY,
        "snapshot_id": snapshot.id,
        "superseded_recommendation_ids": [],
        "order_execution_enabled": settings.order_execution_enabled,
        "message": (
            "Recomendación piloto creada. NO se envió ninguna orden. "
            f"Revisá GET /api/recommendations/{rec.id}/execution-preview y, "
            "solo si decidís ejecutar, usá POST "
            f"/api/recommendations/{rec.id}/approve con el contrato reforzado."
        ),
    }




# _supersede_other_open_recommendations was REMOVED: it flipped
# superseded_at/metadata but left the previous recommendation `pending`,
# which could leave two open recommendations at once. The central gate now
# blocks pilot creation instead of superseding anything.


# ---------------------------------------------------------------------------
# Generic controlled pilots — one per (class, side)
#
# Four independent pilots: ACCIONES buy, ACCIONES sell, CEDEARS buy, CEDEARS
# sell. Independent because proving a sell works proves nothing about a buy:
# a buy needs live cash and an ask, a sell needs a live holding and a bid.
#
# This creates a Recommendation and nothing else. It never quotes, never
# approves, never sends, and never touches an existing recommendation — in
# particular not Recommendation 12 (the executed BYMA sell) or 13 (still
# pending): both are history, and a pilot that recycled either would rewrite
# a record instead of making a new one.
# ---------------------------------------------------------------------------

PILOT_TYPE_BY_SIDE = {"buy": "security_buy", "sell": "security_sell"}


def securities_pilot_phrase(symbol: str, side: str, quantity) -> str:
    """The exact phrase for ONE pilot. Different pilot ⇒ different phrase."""
    qty = positive_decimal(quantity)
    rendered = format(qty.normalize(), "f") if qty is not None else str(quantity)
    return f"CREAR PILOTO {str(side).strip().upper()} {str(symbol).strip().upper()} {rendered}"


def create_securities_pilot_recommendation(
    db: Session,
    *,
    execution_key: str | None,
    symbol: str | None,
    side: str | None,
    quantity,
    confirmation_text: str | None,
    note: str = "",
    broker=None,
) -> dict:
    """Create ONE controlled securities pilot. Never sends an order.

    Everything is validated before any write, so a refusal leaves nothing
    behind. Ordered credential → flag → phrase → payload → technical
    readiness, so a caller without the credential learns nothing about the
    catalog.
    """
    from app.broker.execution_class import CLASS_ACCIONES, CLASS_CEDEARS
    from app.broker.instrument_catalog import get_instrument
    from app.services.pilot_readiness import evaluate_pilot_readiness

    settings = get_settings()

    if not settings.execution_admin_key:
        return _err("Credencial de ejecución no configurada.",
                    "execution_admin_key_not_configured", 423)
    if not execution_key or not _secrets.compare_digest(
        str(execution_key), settings.execution_admin_key
    ):
        return _err("Credencial de ejecución inválida o ausente.",
                    "invalid_execution_key", 403)

    if not settings.execution_pilot_creation_enabled:
        return _err(
            "Creación de piloto deshabilitada (EXECUTION_PILOT_CREATION_ENABLED=false).",
            "execution_pilot_creation_disabled", 423,
        )

    clean_symbol = normalize_symbol(symbol)
    clean_side = str(side or "").strip().lower()
    qty = positive_decimal(quantity)

    if not clean_symbol:
        return _err("Se requiere un símbolo explícito.", "pilot_symbol_required", 422)
    if clean_side not in PILOT_TYPE_BY_SIDE:
        return _err(f"side debe ser buy o sell (recibido: {side!r}).",
                    "pilot_side_not_allowed", 422)
    if qty is None:
        return _err("Se requiere una cantidad explícita y positiva.",
                    "pilot_quantity_required", 422)

    expected_phrase = securities_pilot_phrase(clean_symbol, clean_side, qty)
    if not confirmation_text or confirmation_text.strip() != expected_phrase:
        return _err(
            f"Confirmación incorrecta. Frase requerida exacta: '{expected_phrase}'.",
            "confirmation_mismatch", 422,
        )

    entry = get_instrument(db, clean_symbol)
    if entry is None:
        return _err(
            f"{clean_symbol} no está en el catálogo de ejecución. Resolvelo "
            "read-only antes de crear un piloto.",
            "instrument_catalog_missing", 409,
        )
    if entry.execution_class not in (CLASS_ACCIONES, CLASS_CEDEARS):
        return _err(
            f"{clean_symbol} pertenece a la clase {entry.execution_class or 'desconocida'}, "
            "que no tiene piloto de títulos.",
            "instrument_class_unsupported", 409,
        )

    # --- Technical readiness for THIS side. The same evaluator the readiness
    #     endpoint uses, so a pilot can never be created for a symbol the
    #     report calls blocked.
    readiness = evaluate_pilot_readiness(
        db, symbols=[clean_symbol], broker=broker, settings=settings
    )
    report = next((r for r in readiness["symbols"] if r["symbol"] == clean_symbol), None)
    if report is None:
        return _err("No se pudo evaluar la aptitud técnica del símbolo.",
                    "pilot_readiness_unavailable", 409)
    side_report = report[clean_side]
    if not side_report["technically_ready"]:
        reasons = side_report["blocking_reasons"]
        return _err(
            f"{clean_symbol} no está técnicamente listo para {clean_side}: "
            f"{', '.join(reasons)}.",
            reasons[0] if reasons else "pilot_not_technically_ready", 409,
        )

    # --- Quantity must fit the instrument's own step and the class limit.
    step = entry.quantity_step
    if step is None:
        return _err(
            f"El quantity_step de {clean_symbol} no está verificado.",
            "quantity_step_unverified", 409,
        )
    step_f = float(step)
    if step_f <= 0 or abs((float(qty) / step_f) - round(float(qty) / step_f)) > 1e-9:
        return _err(f"La cantidad {qty} no respeta el quantity_step {step_f}.",
                    "quantity_step_mismatch", 422)

    max_notional = report.get("suggested_pilot_max_notional")
    reference_price = (
        side_report["quote"].get("price") if side_report["quote"].get("available") else None
    )
    if reference_price is not None and max_notional is not None:
        try:
            projected = float(qty) * float(reference_price)
        except (TypeError, ValueError):
            projected = None
        if projected is not None and projected > float(max_notional) + 1e-9:
            return _err(
                f"El monto proyectado ({projected:.2f}) supera el tope técnico del "
                f"piloto ({float(max_notional):.2f}).",
                "pilot_notional_limit_exceeded", 422,
            )

    # --- Same lease and same gate as the analysis cycle: a pilot must not be
    #     able to open a second recommendation behind its back, and it never
    #     supersedes a pending human decision (Recommendation 13 included).
    lease_owner, lease_error = acquire_analysis_lease(db)
    if lease_owner is None:
        return _err("Otro ciclo de análisis está en curso. Reintentá en unos segundos.",
                    lease_error or "analysis_lease_unavailable", 409)
    try:
        gate = check_recommendation_creation_allowed(db)
        if not gate.allowed:
            return _err(
                (gate.detail or "Hay una decisión pendiente.")
                + " Resolvela explícitamente antes de crear el piloto.",
                gate.code or "open_recommendation_requires_decision", 409,
            )
        return _create_securities_pilot_locked(
            db,
            entry=entry,
            symbol=clean_symbol,
            side=clean_side,
            quantity=float(qty),
            note=note,
            phrase=expected_phrase,
        )
    finally:
        release_analysis_lease(db, lease_owner)


def _create_securities_pilot_locked(
    db: Session, *, entry, symbol: str, side: str, quantity: float,
    note: str, phrase: str,
) -> dict:
    """Persist ONE new pilot recommendation. No broker, no quote, no order."""
    settings = get_settings()
    pilot_type = PILOT_TYPE_BY_SIDE[side]
    verb = "Comprar" if side == "buy" else "Vender"

    rec = Recommendation(
        action="rebalancear",
        status="pending",
        suggested_pct=0.0,
        confidence=1.0,
        rationale=(
            f"Piloto administrativo de ejecución controlada: {verb.lower()} "
            f"{quantity} {symbol} ({entry.execution_class}). Creado manualmente, "
            "sin envío de orden."
        ),
        risks=(
            "Piloto de ejecución real controlado. Requiere aprobación manual "
            "explícita y el candado global abierto."
        ),
        executive_summary=f"[PILOTO] {verb} {quantity} {symbol}",
        metadata_json={
            "execution_pilot": True,
            "pilot_type": pilot_type,
            "execution_class": entry.execution_class,
            "symbol": symbol,
            "quantity": quantity,
            "side": side,
            "currency": entry.currency,
            "market": entry.market,
            "settlement": entry.settlement,
            "created_manually": True,
            "confirmation_phrase": phrase,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "note": (note or "").strip(),
        },
    )
    db.add(rec)
    db.flush()

    action = RecommendationAction(
        recommendation_id=rec.id,
        symbol=symbol,
        # Informative only: the executed quantity comes from quantity_override.
        target_change_pct=(0.0001 if side == "buy" else -0.0001),
        reason=f"Piloto controlado: {verb.lower()} explícitamente {quantity} {symbol}.",
        quantity_override=quantity,
    )
    db.add(action)
    db.flush()

    app_log(db, "Recomendación piloto de títulos creada manualmente", context={
        "recommendation_id": rec.id,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "execution_class": entry.execution_class,
        "pilot_type": pilot_type,
    })
    db.commit()

    return {
        "recommendation_id": rec.id,
        "recommendation_action_id": action.id,
        "status": rec.status,
        "execution_pilot": True,
        "pilot_type": pilot_type,
        "execution_class": entry.execution_class,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "approved": False,
        "order_sent": False,
        "order_execution_enabled": settings.order_execution_enabled,
        "message": (
            "Recomendación piloto creada en estado pending. NO se aprobó y NO se "
            f"envió ninguna orden. Revisá GET /api/recommendations/{rec.id}/"
            "execution-preview antes de decidir."
        ),
    }
