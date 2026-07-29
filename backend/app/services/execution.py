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
) -> tuple[dict | None, dict | None]:
    """Execution Authorization checks.

    Returns (error, validated_preview):
    - (error_dict, None) when any check fails
    - (None, rebuilt_preview) when everything passes — the caller MUST execute
      exactly these validated orders, never a recalculated plan (V2).

    Runs BEFORE any state change or broker interaction. The execution
    credential is never persisted, logged, or echoed back.
    """
    # 7. Secondary credential must be configured server-side
    if not settings.execution_admin_key:
        return {
            "error": "Ejecución bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }, None

    # 8. X-Execution-Key must match (constant-time)
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }, None

    # 9. Preview signing must be configured
    if not settings.execution_preview_secret:
        return {
            "error": "Ejecución bloqueada: firma de preview no configurada en el servidor.",
            "code": "preview_signing_not_configured",
            "status_code": 423,
        }, None

    # 10. Exact confirmation phrase
    expected = confirmation_phrase(rec.id)
    if not confirmation_text or confirmation_text.strip() != expected:
        return {
            "error": f"Confirmación incorrecta. Frase requerida exacta: '{expected}'.",
            "code": "confirmation_mismatch",
            "status_code": 422,
        }, None

    # 11. Preview must exist and not be expired
    if not preview_hash or not preview_generated_at:
        return {
            "error": "Se requiere preview_hash y preview_generated_at del preview revisado.",
            "code": "preview_required",
            "status_code": 409,
        }, None
    try:
        gen_dt = datetime.fromisoformat(preview_generated_at)
    except (ValueError, TypeError):
        return {
            "error": "preview_generated_at inválido.",
            "code": "preview_invalid",
            "status_code": 409,
        }, None
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    now = _utcnow()
    if now > gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds):
        return {
            "error": "El preview venció. Generá y revisá un preview nuevo.",
            "code": "preview_expired",
            "status_code": 409,
        }, None

    # 12-15. Rebuild the preview server-side and require an exact signature match.
    # Any drift (new snapshot, changed actions/quantities, tampered hash,
    # different limits) produces a different HMAC.
    rebuilt = build_execution_preview(db, rec.id, generated_at=gen_dt)
    if "error" in rebuilt:
        return {**rebuilt, "code": rebuilt.get("code", "preview_rebuild_failed")}, None
    server_hash = rebuilt.get("preview_hash") or ""
    if not server_hash or not _secrets.compare_digest(server_hash, str(preview_hash)):
        return {
            "error": (
                "El preview no coincide con el estado actual del servidor "
                "(snapshot, acciones u órdenes cambiaron, o el hash fue alterado)."
            ),
            "code": "preview_mismatch",
            "status_code": 409,
        }, None

    # 16. Every order must be valid
    blocking = rebuilt.get("blocking_reasons", [])
    if "invalid_order" in blocking:
        return {
            "error": "Hay órdenes inválidas en el plan de ejecución.",
            "code": "invalid_order",
            "status_code": 422,
        }, None

    # 17-19. Limits must be configured and respected
    if "execution_limits_not_configured" in blocking:
        return {
            "error": "Ejecución bloqueada: límites de ejecución no configurados.",
            "code": "execution_limits_not_configured",
            "status_code": 423,
        }, None
    for code in ("currency_mismatch", "order_limit_exceeded", "total_limit_exceeded", "portfolio_pct_limit_exceeded"):
        if code in blocking:
            return {
                "error": f"Límite de ejecución violado: {code}.",
                "code": code,
                "status_code": 422,
            }, None

    return None, rebuilt


def _crosscheck_validated_orders(actions: list, validated_orders: list[dict]) -> dict | None:
    """Belt-and-suspenders re-verification of the validated preview against
    the recommendation's actions. The HMAC already guarantees consistency;
    any mismatch here means something is deeply wrong → fail closed."""
    action_by_id = {a.id: a for a in actions}
    if len(validated_orders) != len(actions):
        return {
            "error": "El preview validado no cubre exactamente las acciones de la recomendación.",
            "code": "validated_orders_inconsistent",
            "status_code": 409,
        }
    for o in validated_orders:
        action = action_by_id.get(o.get("recommendation_action_id"))
        expected_side = None
        if action is not None:
            expected_side = "sell" if action.target_change_pct < 0 else "buy"
        if (
            action is None
            or action.symbol != o.get("symbol")
            or expected_side != o.get("side")
            or not o.get("valid")
            or not o.get("quantity_planned")
            or o.get("quantity_planned") <= 0
        ):
            return {
                "error": "Inconsistencia entre el preview validado y las acciones de la recomendación.",
                "code": "validated_orders_inconsistent",
                "status_code": 409,
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

    Execution Authorization V2 (durability):
    - Real broker: ALWAYS requires the reinforced contract (execution key,
      signed non-expired preview, exact confirmation phrase, limits) on top
      of the ORDER_EXECUTION_ENABLED safety lock, and executes EXACTLY the
      validated preview orders — never a recalculated plan.
    - Mock broker: legacy direct approve still works (tests/staging); if the
      reinforced fields are provided, they are fully validated so staging can
      rehearse the real flow without touching IOL.

    Submission semantics: at-most-once automatic submission + manual
    reconciliation on uncertain outcome. An interrupted or dubious order is
    NEVER re-sent automatically.

    All validations run BEFORE any state change or broker interaction.
    Returns dict with execution results or error.
    """
    settings = get_settings()

    # 1. Recommendation exists
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    # 2. Never executed before (server-side double-execution protection).
    # Checked BEFORE the status check so an interrupted execution (rec in
    # execution_pending/manual_reconciliation_required with persisted intents)
    # answers 409, never re-submits.
    if _has_prior_execution(db, recommendation_id):
        return {
            "error": "La recomendación ya tiene ejecuciones o una aprobación previa. No se reenvía automáticamente.",
            "code": "already_executed",
            "status_code": 409,
        }

    # 3. Allowed state
    if rec.status not in _APPROVABLE_STATUSES:
        if rec.status in {"execution_pending", "submitting", "manual_reconciliation_required"}:
            return {
                "error": (
                    f"La recomendación está en estado '{rec.status}': hay una ejecución en curso o "
                    "con resultado incierto. Requiere conciliación manual, no se reenvía."
                ),
                "code": "already_executed_or_in_progress",
                "status_code": 409,
            }
        return {"error": f"No se puede aprobar: estado actual es '{rec.status}'", "status_code": 400}

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
    validated_preview = None
    if reinforced:
        err, validated_preview = _validate_reinforced_authorization(
            db, rec, settings, execution_key, preview_hash, preview_generated_at, confirmation_text
        )
        if err:
            return err

    # Load actions (needed for the cross-check BEFORE claiming)
    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == recommendation_id
    ).all()

    # V2: re-verify the validated orders against the recommendation's actions.
    # Fail closed pre-claim on any inconsistency.
    if validated_preview is not None and actions and rec.action != "mantener":
        err = _crosscheck_validated_orders(actions, validated_preview.get("orders_preview", []))
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

    if not actions or rec.action == "mantener":
        rec.status = "approved"
        db.commit()
        return {
            "recommendation_id": recommendation_id,
            "status": "approved",
            "executions": [],
            "message": "Aprobada sin órdenes (acción: mantener o sin activos afectados).",
        }

    if validated_preview is not None:
        # Durable reinforced path: persist intents, then submit exactly the
        # validated preview orders. Internal errors resolve conservatively.
        return _execute_validated_orders(db, rec, validated_preview, preview_generated_at)

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


# States from which an order must NEVER be automatically re-submitted.
_NO_RESUBMIT_STATUSES = {"submitting", "execution_sent", "manual_reconciliation_required"}


def _execute_validated_orders(
    db: Session,
    rec: Recommendation,
    validated_preview: dict,
    preview_generated_at: str | None,
) -> dict:
    """Durable execution of EXACTLY the validated preview orders (V2).

    Semantics: at-most-once automatic submission + manual reconciliation on
    uncertain outcome (IOL offers no idempotency keys, so exactly-once cannot
    be guaranteed — we guarantee we never auto-retry instead).

    Durability protocol:
    1. Persist ALL OrderExecution intents (with request_audit traceability,
       quantity_sent=None — nothing was sent yet) and COMMIT before
       instantiating the broker or fetching quotes.
    2. Per order: reload → verify still eligible → fetch fresh quote (a
       failure here is DEFINITIVE: place_order was never called → failed)
       → mark 'submitting' + quantity_sent=quantity_planned + COMMIT →
       single place_order call → persist outcome → COMMIT.
    3. A crash between 'submitting' and the outcome leaves the row in
       'submitting': visibly stuck, never auto-resent, approve returns 409.

    quantity_sent semantics: None until the moment we commit 'submitting'
    right before the broker call; it must never suggest a send happened
    while the order is only 'execution_requested'.

    No snapshot re-fetch, no _plan_order re-run: symbol/side/quantity come
    verbatim from the signed preview. The fresh quote decides ONLY the price
    (limit vs market), never symbol/side/quantity.
    """
    recommendation_id = rec.id
    orders = validated_preview.get("orders_preview", [])
    preview_hash = validated_preview.get("preview_hash", "")
    snapshot_id = validated_preview.get("snapshot_id")

    # --- Phase 1: persist intents, then COMMIT (durability point) ---
    intents: list[OrderExecution] = []
    for o in orders:
        request_audit = {
            "snapshot_id": snapshot_id,
            "preview_hash": preview_hash,
            "preview_generated_at": preview_generated_at,
            "estimated_notional": o.get("estimated_notional"),
            "portfolio_pct": o.get("portfolio_pct"),
            "execution_request_id": f"rec{recommendation_id}-act{o['recommendation_action_id']}-{preview_hash[:16]}",
        }
        order_exec = OrderExecution(
            recommendation_id=recommendation_id,
            recommendation_action_id=o["recommendation_action_id"],
            symbol=o["symbol"],
            side=o["side"],
            target_change_pct=o["target_change_pct"],
            status="execution_requested",
            validation_status="passed",
            portfolio_value_used=o["portfolio_value_used"],
            position_value_used=o["position_value_used"],
            quantity_planned=o["quantity_planned"],
            quantity=o["quantity_planned"],
            quantity_sent=None,
            broker_response={"request_audit": request_audit, "broker_result": None, "reconciliation_audit": []},
        )
        db.add(order_exec)
        intents.append(order_exec)

    # Claim + UserDecision + intents survive a process crash from here on.
    db.commit()

    # --- Phase 2: submit each intent at most once ---
    executions = []
    sent_count = 0
    uncertain_count = 0

    try:
        broker = _get_execution_broker()

        for order_exec in intents:
            # Reload the persisted row and verify single-attempt eligibility
            db.refresh(order_exec)
            if order_exec.status != "execution_requested":
                # submitting / sent / manual_reconciliation_required etc.
                # → never auto-resubmit
                executions.append(_exec_summary(order_exec))
                if order_exec.status == "execution_sent":
                    sent_count += 1
                elif order_exec.status in ("submitting", "manual_reconciliation_required"):
                    uncertain_count += 1
                continue

            # Fresh quote BEFORE 'submitting': any problem here is a
            # DEFINITIVE preparation failure — place_order was never called,
            # so this is 'failed', never 'manual_reconciliation_required'.
            quote_error = None
            quote = None
            try:
                quote = _get_fresh_quote(broker, order_exec.symbol, order_exec.side)
            except Exception as exc:
                quote_error = str(exc)[:300]

            if quote_error is not None or not quote.get("available"):
                order_exec.status = "failed"
                order_exec.error_message = (
                    f"Quote preparation failed for {order_exec.symbol} "
                    f"({quote_error or 'no fresh quote available'}). Order NOT sent — place_order was never called."
                )
                order_exec.completed_at = datetime.now(timezone.utc)
                db.commit()
                executions.append(_exec_summary(order_exec))
                continue

            # Point of no return: from here the request may reach the broker.
            # quantity_sent gets its value ONLY now, together with 'submitting'.
            order_exec.status = "submitting"
            order_exec.quantity_sent = order_exec.quantity_planned
            db.commit()

            try:
                result = broker.place_order(
                    symbol=order_exec.symbol,
                    side=order_exec.side,
                    quantity=order_exec.quantity_planned,
                    price=quote["price"],
                )
            except Exception as exc:
                # Uncertain outcome: IOL may or may not have received the
                # order. NEVER assume it is safe to retry.
                order_exec.status = "manual_reconciliation_required"
                order_exec.error_message = (
                    f"Resultado incierto del broker: {str(exc)[:300]}. "
                    "Requiere conciliación manual. NO se reintenta automáticamente."
                )
                order_exec.completed_at = datetime.now(timezone.utc)
                db.commit()
                uncertain_count += 1
                executions.append(_exec_summary(order_exec))
                continue

            # Persist the broker outcome immediately, preserving request_audit
            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.endpoint_used = result.get("endpoint_used", "")
            order_exec.sent_at = datetime.now(timezone.utc)
            order_exec.broker_response = {
                **(order_exec.broker_response or {}),
                "broker_result": result.get("raw_response", result),
            }
            if result.get("status") == "sent":
                order_exec.status = "execution_sent"
                sent_count += 1
            elif result.get("status") == "rejected":
                order_exec.status = "rejected_by_broker"
                order_exec.error_message = result.get("error", "Broker rejected order")
                order_exec.completed_at = datetime.now(timezone.utc)
            else:
                order_exec.status = "failed"
                order_exec.error_message = result.get("error", "Unknown error")
                order_exec.completed_at = datetime.now(timezone.utc)
            db.commit()
            executions.append(_exec_summary(order_exec))

    except Exception as exc:
        # Infrastructure failure mid-loop: conservative resolution. Intents
        # already persisted; nothing gets re-sent automatically.
        logger.error("Infrastructure failure during validated execution of rec %s: %s", recommendation_id, exc)
        uncertain_count += 1

    # --- Phase 3: recommendation outcome ---
    total = len(intents)
    if uncertain_count > 0:
        rec.status = "manual_reconciliation_required"
    elif sent_count == total and total > 0:
        rec.status = "approved"
    elif sent_count == 0:
        rec.status = "execution_failed"
    else:
        rec.status = "execution_partial"
    db.commit()

    # Best-effort notification (notifications NEVER execute orders)
    try:
        from app.notifications.dispatcher import dispatch_execution_notification
        for order_exec in intents:
            dispatch_execution_notification(order_exec, db=db)
    except Exception as exc:
        logger.warning("Post-execution notification dispatch failed: %s", exc)

    return {
        "recommendation_id": recommendation_id,
        "status": rec.status,
        "executions": executions,
        "message": (
            f"{len(executions)} órdenes procesadas: {sent_count} enviadas, "
            f"{uncertain_count} con resultado incierto."
        ),
    }


def _execute_claimed_orders(db: Session, rec: Recommendation, actions: list, note: str) -> dict:
    """Legacy order loop (mock broker without reinforced fields — never IOL)."""
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
    """Get a single execution by ID, with full audit detail and the
    associated recommendation. Never includes credentials or tokens."""
    e = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not e:
        return None
    result = _exec_to_dict(e)
    rec = db.query(Recommendation).filter(Recommendation.id == e.recommendation_id).first()
    result["recommendation"] = {
        "id": rec.id,
        "status": rec.status,
        "action": rec.action,
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
        "superseded_at": rec.superseded_at.isoformat() if rec.superseded_at else None,
    } if rec else None
    return result


def _extract_broker_response_audits(broker_response) -> dict:
    """Safely split broker_response into its audit components.

    Legacy rows may hold a raw broker payload instead of the structured
    {request_audit, broker_result, reconciliation_audit} shape.
    """
    if not isinstance(broker_response, dict):
        return {"request_audit": None, "broker_result": broker_response, "reconciliation_audit": []}
    if "request_audit" in broker_response or "reconciliation_audit" in broker_response or "broker_result" in broker_response:
        return {
            "request_audit": broker_response.get("request_audit"),
            "broker_result": broker_response.get("broker_result"),
            "reconciliation_audit": broker_response.get("reconciliation_audit") or [],
            "broker_status_checks": broker_response.get("broker_status_checks") or [],
        }
    return {"request_audit": None, "broker_result": broker_response, "reconciliation_audit": [], "broker_status_checks": []}


def _exec_to_dict(e: OrderExecution) -> dict:
    audits = _extract_broker_response_audits(e.broker_response)
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
        "request_audit": audits.get("request_audit"),
        "broker_result": audits.get("broker_result"),
        "reconciliation_audit": audits.get("reconciliation_audit"),
        "broker_status_checks": audits.get("broker_status_checks", []),
    }


# ---------------------------------------------------------------------------
# Execution Reconciliation V1 — manual resolution of uncertain orders.
#
# INVARIANTS:
# - reconciliation NEVER creates orders, NEVER calls place_order, NEVER
#   re-executes a recommendation, NEVER returns an order to execution_requested
#   nor a recommendation to pending/blocked;
# - only a human with the execution credential + exact phrase can reconcile;
# - every action is appended to an append-only reconciliation_audit list;
# - it works regardless of ORDER_EXECUTION_ENABLED: the safety lock blocks
#   NEW orders, it must not prevent resolving a past uncertain one.
# ---------------------------------------------------------------------------

_RECONCILABLE_STATUSES = {"submitting", "manual_reconciliation_required"}

_RECONCILIATION_ACTIONS = {
    "confirm_not_sent": {
        "target_status": "not_sent_confirmed",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO NO ENVIADA",
    },
    "confirm_sent": {
        "target_status": "execution_sent",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO ENVIADA",
    },
    "confirm_rejected": {
        "target_status": "rejected_by_broker",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO RECHAZADA",
    },
    "confirm_executed": {
        "target_status": "executed",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO EJECUTADA",
    },
}

_UNCERTAIN_ORDER_STATUSES = {"submitting", "manual_reconciliation_required", "execution_requested"}
_SENT_ORDER_STATUSES = {"execution_sent", "executed"}
_DEFINITIVE_FAIL_STATUSES = {"failed", "rejected_by_broker", "not_sent_confirmed", "validation_failed"}


def reconciliation_phrase(execution_id: int, action: str) -> str:
    return _RECONCILIATION_ACTIONS[action]["phrase"].format(execution_id=execution_id)


def _recompute_recommendation_outcome(db: Session, recommendation_id: int) -> str | None:
    """Recompute the aggregate recommendation status from ALL its orders.

    Never returns the recommendation to pending/blocked (it can never become
    approvable again).
    """
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return None
    orders = db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).all()
    if not orders:
        return rec.status

    statuses = [o.status for o in orders]
    if any(s in _UNCERTAIN_ORDER_STATUSES for s in statuses):
        new_status = "manual_reconciliation_required"
    elif all(s in _SENT_ORDER_STATUSES for s in statuses):
        new_status = "approved"
    elif any(s in _SENT_ORDER_STATUSES for s in statuses):
        new_status = "execution_partial"
    else:
        new_status = "execution_failed"

    rec.status = new_status
    return new_status


def get_reconciliation_queue(db: Session) -> list[dict]:
    """Orders pending manual inspection: any order in submitting or
    manual_reconciliation_required, plus every order of an execution_partial
    recommendation (full picture for the partial batch)."""
    direct = db.query(OrderExecution).filter(
        OrderExecution.status.in_(sorted(_RECONCILABLE_STATUSES))
    ).all()

    partial_rec_ids = [
        r.id for r in db.query(Recommendation).filter(Recommendation.status == "execution_partial").all()
    ]
    partial_orders = (
        db.query(OrderExecution).filter(OrderExecution.recommendation_id.in_(partial_rec_ids)).all()
        if partial_rec_ids else []
    )

    rec_status_cache: dict[int, str] = {}

    def _rec_status(rec_id: int) -> str:
        if rec_id not in rec_status_cache:
            rec = db.query(Recommendation).filter(Recommendation.id == rec_id).first()
            rec_status_cache[rec_id] = rec.status if rec else "unknown"
        return rec_status_cache[rec_id]

    seen: dict[int, OrderExecution] = {}
    for e in list(direct) + list(partial_orders):
        seen[e.id] = e

    items = []
    for e in sorted(seen.values(), key=lambda x: x.id, reverse=True):
        audits = _extract_broker_response_audits(e.broker_response)
        items.append({
            "id": e.id,
            "recommendation_id": e.recommendation_id,
            "recommendation_status": _rec_status(e.recommendation_id),
            "symbol": e.symbol,
            "side": e.side,
            "status": e.status,
            "quantity_planned": e.quantity_planned,
            "quantity_sent": e.quantity_sent,
            "broker_order_id": e.broker_order_id or "",
            "endpoint_used": e.endpoint_used or "",
            "error_message": e.error_message or "",
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "sent_at": e.sent_at.isoformat() if e.sent_at else None,
            "request_audit": audits.get("request_audit"),
            "reconciliation_audit": audits.get("reconciliation_audit"),
        })
    return items


def reconcile_execution(
    db: Session,
    execution_id: int,
    *,
    execution_key: str | None,
    action: str,
    confirmation_text: str | None,
    note: str = "",
    broker_order_id: str | None = None,
    executed_quantity: float | None = None,
    executed_price: float | None = None,
) -> dict:
    """Manually resolve an uncertain order. NEVER sends, retries or creates
    orders — it only records a human decision with full audit trail.

    Credentials are validated first (no information leak about rows), the
    execution key is checked constant-time and never persisted or logged.
    """
    settings = get_settings()

    # Credential gates first
    if not settings.execution_admin_key:
        return {
            "error": "Conciliación bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    if action not in _RECONCILIATION_ACTIONS:
        return {
            "error": f"Acción de conciliación inválida: '{action}'.",
            "code": "invalid_reconciliation_action",
            "status_code": 422,
        }

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    expected_phrase = reconciliation_phrase(execution_id, action)
    if not confirmation_text or confirmation_text.strip() != expected_phrase:
        return {
            "error": f"Confirmación incorrecta. Frase requerida exacta: '{expected_phrase}'.",
            "code": "confirmation_mismatch",
            "status_code": 422,
        }

    previous_status = order_exec.status
    # Eligibility: only uncertain states — with ONE tested exception:
    # execution_sent may be corrected to executed with explicit evidence.
    if action == "confirm_executed":
        allowed_from = _RECONCILABLE_STATUSES | {"execution_sent"}
    else:
        allowed_from = _RECONCILABLE_STATUSES
    if previous_status not in allowed_from:
        return {
            "error": f"El estado '{previous_status}' no es conciliable con la acción '{action}'.",
            "code": "not_reconcilable",
            "status_code": 409,
        }

    # Per-action evidence requirements
    clean_broker_id = (broker_order_id or "").strip()
    clean_note = (note or "").strip()
    if action == "confirm_sent" and not clean_broker_id:
        return {
            "error": "confirm_sent requiere broker_order_id no vacío.",
            "code": "broker_order_id_required",
            "status_code": 422,
        }
    if action == "confirm_rejected" and not clean_note:
        return {
            "error": "confirm_rejected requiere una nota no vacía con el motivo.",
            "code": "note_required",
            "status_code": 422,
        }
    if action == "confirm_executed":
        if not clean_broker_id and not (order_exec.broker_order_id or "").strip():
            return {
                "error": "confirm_executed requiere broker_order_id.",
                "code": "broker_order_id_required",
                "status_code": 422,
            }
        if not executed_quantity or executed_quantity <= 0:
            return {
                "error": "confirm_executed requiere executed_quantity > 0.",
                "code": "executed_quantity_required",
                "status_code": 422,
            }
        if not executed_price or executed_price <= 0:
            return {
                "error": "confirm_executed requiere executed_price > 0.",
                "code": "executed_price_required",
                "status_code": 422,
            }

    new_status = _RECONCILIATION_ACTIONS[action]["target_status"]

    # Atomic conditional transition — a concurrent/repeated reconciliation
    # loses the update and gets a 409; the same change is never applied twice.
    claimed = (
        db.query(OrderExecution)
        .filter(OrderExecution.id == execution_id, OrderExecution.status == previous_status)
        .update({"status": new_status}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return {
            "error": "La ejecución fue conciliada por otra solicitud. Revisá el estado actual.",
            "code": "reconciliation_conflict",
            "status_code": 409,
        }
    db.refresh(order_exec)

    now = _utcnow()

    # Append-only audit entry — never includes credentials of any kind.
    audit_entry = {
        "timestamp": now.isoformat(),
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "note": clean_note,
        "broker_order_id": clean_broker_id or None,
        "executed_quantity": executed_quantity,
        "executed_price": executed_price,
        "source": "manual_user",
    }
    br = dict(order_exec.broker_response or {})
    audit_list = list(br.get("reconciliation_audit") or [])
    audit_list.append(audit_entry)
    br["reconciliation_audit"] = audit_list
    order_exec.broker_response = br

    if action == "confirm_not_sent":
        # Confirmed externally that IOL never received it. quantity_sent goes
        # back to None: nothing was actually sent. NEVER re-sent, never back
        # to execution_requested.
        order_exec.quantity_sent = None
        order_exec.completed_at = now
    elif action == "confirm_sent":
        order_exec.broker_order_id = clean_broker_id
        if not order_exec.sent_at:
            order_exec.sent_at = now
    elif action == "confirm_rejected":
        order_exec.error_message = clean_note
        order_exec.completed_at = now
    elif action == "confirm_executed":
        if clean_broker_id:
            order_exec.broker_order_id = clean_broker_id
        order_exec.executed_quantity = executed_quantity
        order_exec.executed_price = executed_price
        order_exec.completed_at = now

    db.flush()
    recommendation_status = _recompute_recommendation_outcome(db, order_exec.recommendation_id)

    app_log(db, "Conciliación manual de ejecución aplicada", context={
        "execution_id": execution_id,
        "recommendation_id": order_exec.recommendation_id,
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "source": "manual_user",
    })
    db.commit()

    return {
        "execution_id": execution_id,
        "recommendation_id": order_exec.recommendation_id,
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "recommendation_status": recommendation_status,
        "reconciliation_audit": audit_list,
    }


def refresh_broker_status(db: Session, execution_id: int, *, execution_key: str | None) -> dict:
    """READ-ONLY broker status check by broker_order_id.

    Uses the existing get_order_status (GET /api/v2/operaciones/{id} on IOL).
    NEVER calls place_order, never creates or retries orders, and NEVER
    changes the execution status automatically: IOL state labels are not a
    contract we control, so mapping to 'executed' stays a manual decision
    (conservative choice per spec). The raw result is appended to
    broker_response.broker_status_checks for the human to inspect.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return {
            "error": "Consulta bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    if not (order_exec.broker_order_id or "").strip():
        return {
            "error": "La ejecución no tiene broker_order_id: no hay nada que consultar.",
            "code": "broker_order_id_missing",
            "status_code": 422,
        }

    broker = _get_execution_broker()
    try:
        result = broker.get_order_status(order_exec.broker_order_id)
    except Exception as exc:
        return {
            "error": f"Consulta de estado falló: {str(exc)[:300]}",
            "code": "broker_status_query_failed",
            "status_code": 502,
        }

    now = _utcnow()
    br = dict(order_exec.broker_response or {})
    checks = list(br.get("broker_status_checks") or [])
    checks.append({"timestamp": now.isoformat(), "raw": result})
    br["broker_status_checks"] = checks
    order_exec.broker_response = br

    app_log(db, "Consulta read-only de estado de orden al broker", context={
        "execution_id": execution_id,
        "broker_order_id": order_exec.broker_order_id,
        "broker_status": result.get("status"),
    })
    db.commit()

    return {
        "execution_id": execution_id,
        "broker_order_id": order_exec.broker_order_id,
        "broker_status": result.get("status"),
        "status_unchanged": True,
        "current_status": order_exec.status,
        "raw_stored": True,
        "message": (
            "Consulta read-only registrada. El estado de la ejecución NO se cambió "
            "automáticamente: la conciliación sigue siendo manual."
        ),
    }
