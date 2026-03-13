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

import math
from datetime import datetime

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
    - last_price_used: float | None
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
                "last_price_used": None,
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
                "last_price_used": None,
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
                "last_price_used": None,
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
                "last_price_used": None,
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
                "last_price_used": price_per_unit,
            }

        return {
            "valid": True,
            "side": side,
            "quantity_planned": quantity_planned,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position_value,
            "blocked_reason": "",
            "last_price_used": price_per_unit,
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
            "last_price_used": None,
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
            "last_price_used": None,
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
            "last_price_used": price_per_unit,
        }

    return {
        "valid": True,
        "side": side,
        "quantity_planned": quantity_planned,
        "portfolio_value_used": portfolio_value,
        "position_value_used": position.market_value if position else 0,
        "blocked_reason": "",
        "last_price_used": price_per_unit,
    }


def approve_and_execute(db: Session, recommendation_id: int, note: str = "") -> dict:
    """Approve a recommendation and trigger order execution.

    Returns dict with execution results or error.
    """
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    if rec.status not in {"pending", "blocked"}:
        return {"error": f"No se puede aprobar: estado actual es '{rec.status}'", "status_code": 400}

    # Mark as approved
    rec.status = "approved"
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
        db.commit()
        return {
            "recommendation_id": recommendation_id,
            "status": "approved",
            "executions": [],
            "message": "Aprobada sin órdenes (acción: mantener o sin activos afectados).",
        }

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
            order_exec.completed_at = datetime.utcnow()

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

        # --- BROKER EXECUTION ---
        try:
            result = broker.place_order(
                symbol=action.symbol,
                side=plan["side"],
                quantity=plan["quantity_planned"],
                price=plan["last_price_used"],
            )

            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.broker_response = result.get("raw_response", {})
            order_exec.endpoint_used = result.get("endpoint_used", "")
            order_exec.sent_at = datetime.utcnow()

            if result.get("status") == "sent":
                order_exec.status = "execution_sent"
            elif result.get("status") == "rejected":
                order_exec.status = "rejected_by_broker"
                order_exec.error_message = result.get("error", "Broker rejected order")
                order_exec.completed_at = datetime.utcnow()
            else:
                order_exec.status = "failed"
                order_exec.error_message = result.get("error", "Unknown error")
                order_exec.completed_at = datetime.utcnow()

        except Exception as exc:
            order_exec.status = "failed"
            order_exec.error_message = str(exc)[:500]
            order_exec.completed_at = datetime.utcnow()

        executions.append(_exec_summary(order_exec))

    # Best-effort notification
    try:
        from app.notifications.dispatcher import dispatch_execution_notification
        for action in actions:
            exec_row = db.query(OrderExecution).filter(
                OrderExecution.recommendation_action_id == action.id
            ).first()
            if exec_row:
                dispatch_execution_notification(exec_row)
    except Exception:
        pass

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
