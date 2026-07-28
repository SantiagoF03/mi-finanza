"""Execution service — approve triggers real IOL execution with safe order planning.

Flow:
1. User approves recommendation via API
2. For each RecommendationAction:
   a. Load latest portfolio snapshot to get real position data
   b. Calculate safe quantity from target_change_pct + position value
   c. Validate quantity > 0 and consistent
   d. Create OrderExecution row with full traceability
   e. If validation passes → send to broker
   f. If validation fails → status=validation_failed, no order sent
3. Notification dispatched on state changes

CRITICAL INVARIANTS:
- Scheduler NEVER calls this module
- LLM NEVER triggers execution
- Only user approve via API triggers execution
- Recommendation must be in pending/blocked state to approve
- FAIL CLOSED: if we can't compute a safe quantity, we don't send the order
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import secrets as _secrets
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
    UserDecision,
)
from app.services.logs import app_log

logger = logging.getLogger(__name__)


def _get_fresh_quote(broker, symbol: str, side: str) -> dict:
    """Attempt to get a fresh tradeable price from the broker.

    Returns dict with:
    - available: bool
    - price: float | None  (best bid for sell, best ask for buy, or last)
    - source: str           (e.g. "bid", "ask", "last", "none")

    In mock mode: returns a synthetic quote so tests proceed.
    In real mode: would query IOL cotizaciones for fresh pricing.
    If no fresh quote is available, returns available=False.
    """
    # MockBrokerClient — always provide a quote so mock flow isn't blocked
    if hasattr(broker, "_mock_orders"):
        return {"available": True, "price": None, "source": "market_order"}

    # Real broker — attempt to get fresh quote from IOL
    try:
        resp = broker._authorized_get(f"/api/v2/Cotizaciones/detalle/bCBA/{symbol}")
        data = resp.json()
        if isinstance(data, dict):
            if side == "sell":
                price = data.get("puntas", {}).get("precioCompra") or data.get("ultimoPrecio")
            else:
                price = data.get("puntas", {}).get("precioVenta") or data.get("ultimoPrecio")
            if price and float(price) > 0:
                source = "bid" if side == "sell" else "ask"
                return {"available": True, "price": float(price), "source": source}
            # Has data but no usable price
            last = data.get("ultimoPrecio")
            if last and float(last) > 0:
                return {"available": True, "price": float(last), "source": "last"}
    except Exception as exc:
        logger.warning("_get_fresh_quote failed for %s: %s", symbol, exc)

    return {"available": False, "price": None, "source": "none"}


def _get_execution_broker():
    settings = get_settings()
    if settings.broker_mode == "mock":
        return MockBrokerClient()
    return IolBrokerClient()


def _get_latest_snapshot(db: Session) -> PortfolioSnapshot | None:
    """Get the most recent portfolio snapshot with positions."""
    return (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )


def _find_position(snapshot: PortfolioSnapshot, symbol: str) -> PortfolioPosition | None:
    """Find a position by symbol in the snapshot."""
    for p in snapshot.positions:
        if p.symbol == symbol:
            return p
    return None


def _plan_order(
    action: RecommendationAction,
    snapshot: PortfolioSnapshot,
) -> dict:
    """Plan a safe order from a recommendation action using real portfolio data.

    Returns a plan dict with:
    - valid: bool
    - side: str
    - quantity_planned: float
    - portfolio_value_used: float
    - position_value_used: float
    - blocked_reason: str (empty if valid)
    - snapshot_price_ref: float | None  (for traceability ONLY, never sent to broker)
    """
    symbol = action.symbol
    target_pct = action.target_change_pct
    side = "sell" if target_pct < 0 else "buy"
    abs_pct = abs(target_pct)

    portfolio_value = snapshot.total_value if snapshot else 0
    position = _find_position(snapshot, symbol) if snapshot else None

    # --- Validation: sell (reduce position) ---
    if side == "sell":
        if not position:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": 0,
                "blocked_reason": f"No position found for {symbol} in latest snapshot. Cannot sell.",
                "snapshot_price_ref": None,
            }

        position_value = position.market_value or 0
        position_qty = position.quantity or 0

        if position_qty <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Position quantity for {symbol} is {position_qty}. Cannot sell zero/negative.",
                "snapshot_price_ref": None,
            }

        # Calculate the amount to sell as % of portfolio value applied to position
        target_value = portfolio_value * abs_pct
        if position_value <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Position market_value for {symbol} is {position_value}. Cannot calculate.",
                "snapshot_price_ref": None,
            }

        # Price per unit from position data
        price_per_unit = position_value / position_qty
        if price_per_unit <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Derived price per unit for {symbol} is {price_per_unit}. Cannot calculate.",
                "snapshot_price_ref": None,
            }

        # Quantity to sell — cannot exceed held quantity
        raw_qty = target_value / price_per_unit
        quantity_planned = min(raw_qty, position_qty)
        # Round down to integer for most IOL instruments
        quantity_planned = math.floor(quantity_planned)

        if quantity_planned <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Calculated sell quantity for {symbol} rounds to 0 (target_value={target_value:.2f}, price={price_per_unit:.2f}).",
                "snapshot_price_ref": price_per_unit,
            }

        return {
            "valid": True,
            "side": side,
            "quantity_planned": quantity_planned,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position_value,
            "blocked_reason": "",
            "snapshot_price_ref": price_per_unit,
        }

    # --- Buy (increase position / new position) ---
    # For MVP: buy is supported but requires cash available
    cash = snapshot.cash if snapshot else 0
    target_value = portfolio_value * abs_pct

    if target_value <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"Target buy value for {symbol} is 0. abs_pct={abs_pct}.",
            "snapshot_price_ref": None,
        }

    # Need a price — from position or fail
    price_per_unit = None
    if position and position.quantity and position.quantity > 0 and position.market_value:
        price_per_unit = position.market_value / position.quantity
    elif position and position.avg_price and position.avg_price > 0:
        price_per_unit = position.avg_price

    if not price_per_unit or price_per_unit <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"No price reference for {symbol}. Cannot calculate buy quantity.",
            "snapshot_price_ref": None,
        }

    # Don't buy more than available cash
    buy_value = min(target_value, cash)
    quantity_planned = math.floor(buy_value / price_per_unit)

    if quantity_planned <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"Buy quantity for {symbol} rounds to 0 (buy_value={buy_value:.2f}, price={price_per_unit:.2f}, cash={cash:.2f}).",
            "snapshot_price_ref": price_per_unit,
        }

    return {
        "valid": True,
        "side": side,
        "quantity_planned": quantity_planned,
        "portfolio_value_used": portfolio_value,
        "position_value_used": position.market_value if position else 0,
        "blocked_reason": "",
        "snapshot_price_ref": price_per_unit,
    }


# ---------------------------------------------------------------------------
# Execution Authorization V1 — canonical preview, HMAC signing, blocking codes
# ---------------------------------------------------------------------------

_APPROVABLE_STATUSES = {"pending", "blocked"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    """Naive UTC now — DB timestamps are naive UTC (datetime.utcnow defaults)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def confirmation_phrase(recommendation_id: int) -> str:
    return f"EJECUTAR RECOMENDACION {recommendation_id}"


def _build_order_previews(actions: list, snapshot: PortfolioSnapshot) -> list[dict]:
    """Deterministic order plans with notional and portfolio percentage.

    This is THE single implementation of the execution plan calculation,
    used by both the preview endpoint and the approve validation.
    """
    orders = []
    for action in actions:
        plan = _plan_order(action, snapshot)
        valid = plan["valid"]
        blocked_reason = plan["blocked_reason"]
        price = plan["snapshot_price_ref"]
        qty = plan["quantity_planned"]
        estimated_notional = 0.0
        portfolio_pct = 0.0

        if valid:
            if not price or price <= 0:
                valid = False
                blocked_reason = f"No valid snapshot price reference for {action.symbol}. Cannot estimate notional."
            else:
                notional = (Decimal(str(qty)) * Decimal(str(price))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                estimated_notional = float(notional)
                pv = plan["portfolio_value_used"]
                if pv and pv > 0:
                    portfolio_pct = float(
                        (notional / Decimal(str(pv))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                    )

        orders.append({
            "recommendation_action_id": action.id,
            "symbol": action.symbol,
            "side": plan["side"],
            "target_change_pct": action.target_change_pct,
            "quantity_planned": qty,
            "snapshot_price_ref": price,
            "estimated_notional": estimated_notional,
            "portfolio_value_used": plan["portfolio_value_used"],
            "position_value_used": plan["position_value_used"],
            "portfolio_pct": portfolio_pct,
            "valid": valid,
            "blocked_reason": blocked_reason,
            "would_execute": False,
        })
    return orders


def _canonical_preview_payload(
    rec: Recommendation,
    snapshot: PortfolioSnapshot,
    orders: list[dict],
    generated_at_iso: str,
    expires_at_iso: str,
    settings,
) -> dict:
    """Canonical, deterministic payload for HMAC signing."""
    return {
        "recommendation_id": rec.id,
        "recommendation_created_at": rec.created_at.isoformat() if rec.created_at else "",
        "snapshot_id": snapshot.id,
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "broker_mode": settings.broker_mode,
        "orders": [
            {
                "recommendation_action_id": o["recommendation_action_id"],
                "symbol": o["symbol"],
                "side": o["side"],
                "target_change_pct": o["target_change_pct"],
                "quantity_planned": o["quantity_planned"],
                "snapshot_price_ref": o["snapshot_price_ref"],
                "estimated_notional": o["estimated_notional"],
                "portfolio_value_used": o["portfolio_value_used"],
                "position_value_used": o["position_value_used"],
                "portfolio_pct": o["portfolio_pct"],
            }
            for o in orders
        ],
        "limits": {
            "max_order_value": settings.execution_max_order_value,
            "max_total_value": settings.execution_max_total_value,
            "max_portfolio_pct": settings.execution_max_portfolio_pct,
        },
    }


def _sign_preview_payload(payload: dict, secret: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def _has_prior_execution(db: Session, recommendation_id: int) -> bool:
    """Server-side double-execution check: existing executions or approved decision."""
    if db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).count() > 0:
        return True
    if db.query(UserDecision).filter(
        UserDecision.recommendation_id == recommendation_id,
        UserDecision.decision == "approved",
    ).count() > 0:
        return True
    return False


def _limits_configured(settings) -> bool:
    return (
        settings.execution_max_order_value > 0
        and settings.execution_max_total_value > 0
        and settings.execution_max_portfolio_pct > 0
    )


def _evaluate_limit_reasons(orders: list[dict], snapshot: PortfolioSnapshot, settings) -> list[str]:
    """Stable blocking codes for limit violations. Fail closed on currency mismatch."""
    reasons = []
    valid_orders = [o for o in orders if o["valid"]]

    if not _limits_configured(settings):
        reasons.append("execution_limits_not_configured")
        return reasons

    # No automatic currency conversion: every position involved must be in
    # the snapshot currency, otherwise notional comparisons are meaningless.
    snapshot_currency = getattr(snapshot, "currency", None)
    if snapshot_currency:
        for o in valid_orders:
            pos = _find_position(snapshot, o["symbol"])
            if pos is not None and pos.currency and pos.currency != snapshot_currency:
                reasons.append("currency_mismatch")
                break

    if any(o["estimated_notional"] > settings.execution_max_order_value for o in valid_orders):
        reasons.append("order_limit_exceeded")

    total = sum(o["estimated_notional"] for o in valid_orders)
    if total > settings.execution_max_total_value:
        reasons.append("total_limit_exceeded")

    if any(o["portfolio_pct"] > settings.execution_max_portfolio_pct for o in valid_orders):
        reasons.append("portfolio_pct_limit_exceeded")

    return reasons


def build_execution_preview(
    db: Session,
    recommendation_id: int,
    *,
    generated_at: datetime | None = None,
) -> dict:
    """Single deterministic builder of the execution authorization preview.

    Used by GET /execution-preview AND by the server-side validation inside
    approve (rebuild + signature match).

    READ-ONLY by contract:
    - no broker client, no place_order, no IOL calls
    - no OrderExecution rows, no db.add, no db.commit
    - no Recommendation.status change, no UserDecision, no notifications

    Always dry_run=True / would_execute=False. can_submit_approval reflects
    whether the reinforced approval flow could be initiated.
    """
    settings = get_settings()

    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == recommendation_id
    ).all()

    snapshot = _get_latest_snapshot(db)
    if not snapshot:
        return {"error": "No portfolio snapshot available for preview", "status_code": 400}

    orders_preview = _build_order_previews(actions, snapshot)

    gen_dt = generated_at or _utcnow()
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    expires_dt = gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds)
    generated_at_iso = gen_dt.isoformat()
    expires_at_iso = expires_dt.isoformat()

    admin_configured = bool(settings.execution_admin_key)
    signing_configured = bool(settings.execution_preview_secret)
    limits_configured = _limits_configured(settings)

    # --- Blocking reasons: stable codes the frontend can rely on ---
    blocking_reasons: list[str] = []
    if settings.broker_mode != "mock" and not settings.order_execution_enabled:
        blocking_reasons.append("execution_locked")
    if not admin_configured:
        blocking_reasons.append("execution_admin_key_not_configured")
    if not signing_configured:
        blocking_reasons.append("preview_signing_not_configured")
    blocking_reasons.extend(_evaluate_limit_reasons(orders_preview, snapshot, settings))
    if rec.status not in _APPROVABLE_STATUSES:
        blocking_reasons.append("recommendation_not_pending")
    if rec.superseded_at is not None:
        blocking_reasons.append("recommendation_superseded")
    if rec.created_at is not None:
        age = _utcnow_naive() - rec.created_at
        if age > timedelta(minutes=settings.execution_max_recommendation_age_minutes):
            blocking_reasons.append("recommendation_stale")
    if not orders_preview or any(not o["valid"] for o in orders_preview):
        blocking_reasons.append("invalid_order")
    if _has_prior_execution(db, recommendation_id):
        blocking_reasons.append("already_executed")

    preview_hash = ""
    if signing_configured:
        payload = _canonical_preview_payload(
            rec, snapshot, orders_preview, generated_at_iso, expires_at_iso, settings
        )
        preview_hash = _sign_preview_payload(payload, settings.execution_preview_secret)

    can_submit_approval = not blocking_reasons and bool(preview_hash)

    total_notional = round(sum(o["estimated_notional"] for o in orders_preview if o["valid"]), 2)

    return {
        "recommendation_id": recommendation_id,
        "recommendation_status": rec.status,
        "recommendation_action": rec.action,
        "recommendation_created_at": rec.created_at.isoformat() if rec.created_at else None,
        "snapshot_id": snapshot.id,
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "broker_mode": settings.broker_mode,
        "order_execution_enabled": settings.order_execution_enabled,
        "execution_admin_auth_configured": admin_configured,
        "execution_limits_configured": limits_configured,
        "dry_run": True,
        "would_execute": False,
        "can_submit_approval": can_submit_approval,
        "blocking_reasons": blocking_reasons,
        "preview_hash": preview_hash,
        "message": "Execution preview only. No order was sent and no state was changed.",
        "actions_count": len(actions),
        "orders_preview": orders_preview,
        "limits": {
            "max_order_value": settings.execution_max_order_value,
            "max_total_value": settings.execution_max_total_value,
            "max_portfolio_pct": settings.execution_max_portfolio_pct,
            "total_estimated_notional": total_notional,
            "configured": limits_configured,
        },
    }


def preview_execution_plan(db: Session, recommendation_id: int) -> dict:
    """Backward-compatible alias for the canonical preview builder."""
    return build_execution_preview(db, recommendation_id)


def _validate_reinforced_authorization(
    db: Session,
    rec: Recommendation,
    settings,
    execution_key: str | None,
    preview_hash: str | None,
    preview_generated_at: str | None,
    confirmation_text: str | None,
) -> dict | None:
    """Execution Authorization V1 checks. Returns an error dict or None if OK.

    Runs BEFORE any state change or broker interaction. The execution
    credential is never persisted, logged, or echoed back.
    """
    # 7. Secondary credential must be configured server-side
    if not settings.execution_admin_key:
        return {
            "error": "Ejecución bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }

    # 8. X-Execution-Key must match (constant-time)
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    # 9. Preview signing must be configured
    if not settings.execution_preview_secret:
        return {
            "error": "Ejecución bloqueada: firma de preview no configurada en el servidor.",
            "code": "preview_signing_not_configured",
            "status_code": 423,
        }

    # 10. Exact confirmation phrase
    expected = confirmation_phrase(rec.id)
    if not confirmation_text or confirmation_text.strip() != expected:
        return {
            "error": f"Confirmación incorrecta. Frase requerida exacta: '{expected}'.",
            "code": "confirmation_mismatch",
            "status_code": 422,
        }

    # 11. Preview must exist and not be expired
    if not preview_hash or not preview_generated_at:
        return {
            "error": "Se requiere preview_hash y preview_generated_at del preview revisado.",
            "code": "preview_required",
            "status_code": 409,
        }
    try:
        gen_dt = datetime.fromisoformat(preview_generated_at)
    except (ValueError, TypeError):
        return {
            "error": "preview_generated_at inválido.",
            "code": "preview_invalid",
            "status_code": 409,
        }
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    now = _utcnow()
    if now > gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds):
        return {
            "error": "El preview venció. Generá y revisá un preview nuevo.",
            "code": "preview_expired",
            "status_code": 409,
        }

    # 12-15. Rebuild the preview server-side and require an exact signature match.
    # Any drift (new snapshot, changed actions/quantities, tampered hash,
    # different limits) produces a different HMAC.
    rebuilt = build_execution_preview(db, rec.id, generated_at=gen_dt)
    if "error" in rebuilt:
        return {**rebuilt, "code": rebuilt.get("code", "preview_rebuild_failed")}
    server_hash = rebuilt.get("preview_hash") or ""
    if not server_hash or not _secrets.compare_digest(server_hash, str(preview_hash)):
        return {
            "error": (
                "El preview no coincide con el estado actual del servidor "
                "(snapshot, acciones u órdenes cambiaron, o el hash fue alterado)."
            ),
            "code": "preview_mismatch",
            "status_code": 409,
        }

    # 16. Every order must be valid
    blocking = rebuilt.get("blocking_reasons", [])
    if "invalid_order" in blocking:
        return {
            "error": "Hay órdenes inválidas en el plan de ejecución.",
            "code": "invalid_order",
            "status_code": 422,
        }

    # 17-19. Limits must be configured and respected
    if "execution_limits_not_configured" in blocking:
        return {
            "error": "Ejecución bloqueada: límites de ejecución no configurados.",
            "code": "execution_limits_not_configured",
            "status_code": 423,
        }
    for code in ("currency_mismatch", "order_limit_exceeded", "total_limit_exceeded", "portfolio_pct_limit_exceeded"):
        if code in blocking:
            return {
                "error": f"Límite de ejecución violado: {code}.",
                "code": code,
                "status_code": 422,
            }

    return None


def approve_and_execute(
    db: Session,
    recommendation_id: int,
    note: str = "",
    *,
    execution_key: str | None = None,
    preview_hash: str | None = None,
    preview_generated_at: str | None = None,
    confirmation_text: str | None = None,
) -> dict:
    """Approve a recommendation and trigger order execution.

    Execution Authorization V1:
    - Real broker: ALWAYS requires the reinforced contract (execution key,
      signed non-expired preview, exact confirmation phrase, limits) on top
      of the ORDER_EXECUTION_ENABLED safety lock.
    - Mock broker: legacy direct approve still works (tests/staging); if the
      reinforced fields are provided, they are fully validated so staging can
      rehearse the real flow without touching IOL.

    All validations run BEFORE any state change or broker interaction.
    Returns dict with execution results or error.
    """
    settings = get_settings()

    # 1. Recommendation exists
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    # 2. Allowed state
    if rec.status not in _APPROVABLE_STATUSES:
        return {"error": f"No se puede aprobar: estado actual es '{rec.status}'", "status_code": 400}

    # 3. Never executed before (server-side double-execution protection)
    if _has_prior_execution(db, recommendation_id):
        return {
            "error": "La recomendación ya tiene ejecuciones o una aprobación previa.",
            "code": "already_executed",
            "status_code": 409,
        }

    # 4. Must be the current recommendation (not superseded)
    if rec.superseded_at is not None:
        return {
            "error": "La recomendación fue reemplazada por una más nueva.",
            "code": "recommendation_superseded",
            "status_code": 409,
        }

    # 5. Not older than the maximum allowed age
    if rec.created_at is not None:
        age = _utcnow_naive() - rec.created_at
        if age > timedelta(minutes=settings.execution_max_recommendation_age_minutes):
            return {
                "error": "La recomendación excede la antigüedad máxima permitida para ejecutar.",
                "code": "recommendation_stale",
                "status_code": 409,
            }

    # 6. SAFETY LOCK — with a real broker, approve must not proceed unless
    # ORDER_EXECUTION_ENABLED=true. Fail closed BEFORE any state change:
    # no status change, no UserDecision, no OrderExecution rows, no broker,
    # no quotes, no notifications.
    if settings.broker_mode != "mock" and not settings.order_execution_enabled:
        return {
            "error": (
                "Safety lock activo: ejecución real deshabilitada "
                "(ORDER_EXECUTION_ENABLED=false). No se aprobó la recomendación "
                "ni se envió ninguna orden."
            ),
            "code": "execution_locked",
            "status_code": 423,
        }

    # 7-19. Reinforced authorization — mandatory for real broker; opt-in for
    # mock (staging rehearsal) when any reinforced field is provided.
    reinforced = settings.broker_mode != "mock" or any(
        v is not None for v in (execution_key, preview_hash, preview_generated_at, confirmation_text)
    )
    if reinforced:
        err = _validate_reinforced_authorization(
            db, rec, settings, execution_key, preview_hash, preview_generated_at, confirmation_text
        )
        if err:
            return err

    # 20. Atomic claim BEFORE any broker call — a concurrent approve for the
    # same recommendation loses the conditional update and gets a 409.
    claimed = (
        db.query(Recommendation)
        .filter(
            Recommendation.id == recommendation_id,
            Recommendation.status.in_(list(_APPROVABLE_STATUSES)),
        )
        .update({"status": "execution_pending"}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return {
            "error": "La recomendación ya fue reclamada o ejecutada por otra solicitud.",
            "code": "already_executed_or_in_progress",
            "status_code": 409,
        }
    db.flush()
    db.refresh(rec)

    # Claim succeeded (status=execution_pending) → record the decision.
    # The execution credential is deliberately NOT persisted or logged.
    decision = UserDecision(recommendation_id=recommendation_id, decision="approved", note=note)
    db.add(decision)
    db.flush()

    app_log(db, "Recomendación aprobada por usuario", context={
        "recommendation_id": recommendation_id,
        "action": rec.action,
    })

    # Load actions
    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == recommendation_id
    ).all()

    if not actions or rec.action == "mantener":
        rec.status = "approved"
        db.commit()
        return {
            "recommendation_id": recommendation_id,
            "status": "approved",
            "executions": [],
            "message": "Aprobada sin órdenes (acción: mantener o sin activos afectados).",
        }

    try:
        return _execute_claimed_orders(db, rec, actions, note)
    except Exception as exc:
        # Partial/unexpected failure: keep traceability, never auto-retry a
        # real order. The recommendation stays out of the approvable states.
        logger.error("Unexpected execution failure for recommendation %s: %s", recommendation_id, exc)
        try:
            rec.status = "execution_failed"
            db.commit()
        except Exception:
            db.rollback()
        return {
            "error": f"Fallo inesperado durante la ejecución: {str(exc)[:300]}",
            "code": "execution_failed",
            "status_code": 500,
        }


def _execute_claimed_orders(db: Session, rec: Recommendation, actions: list, note: str) -> dict:
    """Run the order loop for an already-claimed recommendation."""
    recommendation_id = rec.id

    # Load latest snapshot for order planning
    snapshot = _get_latest_snapshot(db)

    # Create OrderExecution rows with planning and validation
    broker = _get_execution_broker()
    executions = []

    for action in actions:
        # --- ORDER PLANNING ---
        plan = _plan_order(action, snapshot)

        order_exec = OrderExecution(
            recommendation_id=recommendation_id,
            recommendation_action_id=action.id,
            symbol=action.symbol,
            side=plan["side"],
            target_change_pct=action.target_change_pct,
            status="execution_requested",
            portfolio_value_used=plan["portfolio_value_used"],
            position_value_used=plan["position_value_used"],
            quantity_planned=plan["quantity_planned"],
        )
        db.add(order_exec)
        db.flush()

        # --- VALIDATION ---
        if not plan["valid"]:
            order_exec.status = "validation_failed"
            order_exec.validation_status = "failed"
            order_exec.blocked_reason = plan["blocked_reason"]
            order_exec.error_message = plan["blocked_reason"]
            order_exec.completed_at = datetime.now(timezone.utc)

            app_log(db, f"Orden {plan['side']} para {action.symbol} bloqueada por validación", context={
                "order_execution_id": order_exec.id,
                "recommendation_id": recommendation_id,
                "symbol": action.symbol,
                "blocked_reason": plan["blocked_reason"],
            })

            executions.append(_exec_summary(order_exec))
            continue

        # Validation passed
        order_exec.validation_status = "passed"
        order_exec.quantity = plan["quantity_planned"]
        order_exec.quantity_sent = plan["quantity_planned"]

        app_log(db, f"Orden {plan['side']} solicitada para {action.symbol}", context={
            "order_execution_id": order_exec.id,
            "recommendation_id": recommendation_id,
            "symbol": action.symbol,
            "side": plan["side"],
            "target_change_pct": action.target_change_pct,
            "quantity_planned": plan["quantity_planned"],
            "portfolio_value_used": plan["portfolio_value_used"],
            "position_value_used": plan["position_value_used"],
        })

        # --- FRESH QUOTE (never use snapshot-derived price for broker) ---
        quote = _get_fresh_quote(broker, action.symbol, plan["side"])
        if not quote["available"]:
            order_exec.status = "validation_failed"
            order_exec.validation_status = "failed"
            order_exec.blocked_reason = (
                f"No fresh quote available for {action.symbol}. Cannot send order without live pricing."
            )
            order_exec.error_message = order_exec.blocked_reason
            order_exec.completed_at = datetime.now(timezone.utc)

            app_log(db, f"Orden {plan['side']} para {action.symbol} bloqueada: sin cotización fresca", context={
                "order_execution_id": order_exec.id,
                "symbol": action.symbol,
            })
            executions.append(_exec_summary(order_exec))
            continue

        # --- BROKER EXECUTION ---
        # price=quote["price"] → if None, broker sends precioMercado (market order)
        # if quote has a fresh price, broker sends precioLimite with live price
        try:
            result = broker.place_order(
                symbol=action.symbol,
                side=plan["side"],
                quantity=plan["quantity_planned"],
                price=quote["price"],
            )

            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.broker_response = result.get("raw_response", {})
            order_exec.endpoint_used = result.get("endpoint_used", "")
            order_exec.sent_at = datetime.now(timezone.utc)

            if result.get("status") == "sent":
                order_exec.status = "execution_sent"
            elif result.get("status") == "rejected":
                order_exec.status = "rejected_by_broker"
                order_exec.error_message = result.get("error", "Broker rejected order")
                order_exec.completed_at = datetime.now(timezone.utc)
            else:
                order_exec.status = "failed"
                order_exec.error_message = result.get("error", "Unknown error")
                order_exec.completed_at = datetime.now(timezone.utc)

        except Exception as exc:
            order_exec.status = "failed"
            order_exec.error_message = str(exc)[:500]
            order_exec.completed_at = datetime.now(timezone.utc)

        executions.append(_exec_summary(order_exec))

    # Best-effort notification
    try:
        from app.notifications.dispatcher import dispatch_execution_notification
        for action in actions:
            exec_row = db.query(OrderExecution).filter(
                OrderExecution.recommendation_action_id == action.id
            ).first()
            if exec_row:
                dispatch_execution_notification(exec_row, db=db)
    except Exception as exc:
        logger.warning("Post-approval notification dispatch failed: %s", exc)

    rec.status = "approved"
    db.commit()

    return {
        "recommendation_id": recommendation_id,
        "status": "approved",
        "executions": executions,
        "message": f"{len(executions)} órdenes procesadas.",
    }


def _exec_summary(order_exec: OrderExecution) -> dict:
    return {
        "id": order_exec.id,
        "symbol": order_exec.symbol,
        "side": order_exec.side,
        "status": order_exec.status,
        "validation_status": order_exec.validation_status,
        "quantity_planned": order_exec.quantity_planned,
        "quantity_sent": order_exec.quantity_sent,
        "broker_order_id": order_exec.broker_order_id,
        "endpoint_used": order_exec.endpoint_used,
        "error": order_exec.error_message,
        "blocked_reason": order_exec.blocked_reason,
    }


def reject_recommendation(db: Session, recommendation_id: int, note: str = "") -> dict:
    """Reject a recommendation. No execution."""
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    if rec.status not in {"pending", "blocked"}:
        return {"error": f"No se puede rechazar: estado actual es '{rec.status}'", "status_code": 400}

    rec.status = "rejected"
    decision = UserDecision(recommendation_id=recommendation_id, decision="rejected", note=note)
    db.add(decision)
    db.commit()

    app_log(db, "Recomendación rechazada por usuario", context={
        "recommendation_id": recommendation_id,
    })

    return {"recommendation_id": recommendation_id, "status": "rejected"}


def get_executions_for_recommendation(db: Session, recommendation_id: int) -> list[dict]:
    """Get all executions for a given recommendation."""
    execs = db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).order_by(desc(OrderExecution.created_at)).all()
    return [_exec_to_dict(e) for e in execs]


def get_recent_executions(db: Session, limit: int = 20) -> list[dict]:
    """Get recent executions across all recommendations."""
    execs = db.query(OrderExecution).order_by(desc(OrderExecution.created_at)).limit(limit).all()
    return [_exec_to_dict(e) for e in execs]


def get_execution_by_id(db: Session, execution_id: int) -> dict | None:
    """Get a single execution by ID."""
    e = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not e:
        return None
    return _exec_to_dict(e)


def _exec_to_dict(e: OrderExecution) -> dict:
    return {
        "id": e.id,
        "recommendation_id": e.recommendation_id,
        "symbol": e.symbol,
        "side": e.side,
        "target_change_pct": e.target_change_pct,
        "status": e.status,
        "validation_status": e.validation_status,
        "quantity_planned": e.quantity_planned,
        "quantity_sent": e.quantity_sent,
        "portfolio_value_used": e.portfolio_value_used,
        "position_value_used": e.position_value_used,
        "blocked_reason": e.blocked_reason,
        "broker_order_id": e.broker_order_id,
        "endpoint_used": e.endpoint_used,
        "error_message": e.error_message,
        "executed_quantity": e.executed_quantity,
        "executed_price": e.executed_price,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "sent_at": e.sent_at.isoformat() if e.sent_at else None,
        "completed_at": e.completed_at.isoformat() if e.completed_at else None,
    }
