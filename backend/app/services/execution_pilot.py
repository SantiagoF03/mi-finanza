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

    # --- Only one pending pilot at a time ---
    existing = _find_pending_pilot(db)
    if existing is not None:
        return _err(
            f"Ya existe una recomendación piloto pendiente (id={existing.id}).",
            "pilot_already_pending", 409,
        )

    # --- Create (no broker, no quote, no order) ---
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

    superseded = _supersede_other_open_recommendations(db, rec.id)

    app_log(db, "Recomendación piloto de ejecución creada manualmente", context={
        "recommendation_id": rec.id,
        "symbol": PILOT_SYMBOL,
        "side": PILOT_SIDE,
        "quantity": PILOT_QUANTITY,
        "snapshot_id": snapshot.id,
        "superseded_recommendation_ids": superseded,
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
        "superseded_recommendation_ids": superseded,
        "order_execution_enabled": settings.order_execution_enabled,
        "message": (
            "Recomendación piloto creada. NO se envió ninguna orden. "
            f"Revisá GET /api/recommendations/{rec.id}/execution-preview y, "
            "solo si decidís ejecutar, usá POST "
            f"/api/recommendations/{rec.id}/approve con el contrato reforzado."
        ),
    }


def _find_pending_pilot(db: Session) -> Recommendation | None:
    open_recs = (
        db.query(Recommendation)
        .filter(Recommendation.status.in_(["pending", "blocked"]))
        .order_by(desc(Recommendation.created_at))
        .all()
    )
    for rec in open_recs:
        meta = rec.metadata_json or {}
        if isinstance(meta, dict) and meta.get("execution_pilot") is True:
            return rec
    return None


def _supersede_other_open_recommendations(db: Session, pilot_id: int) -> list[int]:
    """Mark other open recommendations superseded, keeping full history.

    Nothing is deleted: the previous recommendation keeps its row, status and
    metadata, and records which pilot superseded it.
    """
    superseded = []
    now = datetime.now(timezone.utc)
    others = (
        db.query(Recommendation)
        .filter(
            Recommendation.id != pilot_id,
            Recommendation.status.in_(["pending", "blocked"]),
            Recommendation.superseded_at.is_(None),
        )
        .all()
    )
    for rec in others:
        rec.superseded_at = now
        meta = dict(rec.metadata_json or {})
        meta["superseded_by_execution_pilot"] = pilot_id
        meta["superseded_at"] = now.isoformat()
        rec.metadata_json = meta
        superseded.append(rec.id)
    return superseded
