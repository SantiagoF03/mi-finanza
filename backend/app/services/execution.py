"""Execution service — approve triggers real IOL execution.

Flow:
1. User approves recommendation via API
2. For each RecommendationAction, create an OrderExecution row (status=execution_requested)
3. Place order via broker client
4. Update OrderExecution status based on broker response
5. Notification dispatched on state changes

CRITICAL INVARIANTS:
- Scheduler NEVER calls this module
- LLM NEVER triggers execution
- Only user approve via API triggers execution
- Recommendation must be in pending/blocked state to approve
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.models.models import OrderExecution, Recommendation, RecommendationAction, UserDecision
from app.services.logs import app_log


def _get_execution_broker():
    settings = get_settings()
    if settings.broker_mode == "mock":
        return MockBrokerClient()
    return IolBrokerClient()


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

    # Create OrderExecution rows and execute
    broker = _get_execution_broker()
    executions = []

    for action in actions:
        side = "sell" if action.target_change_pct < 0 else "buy"
        order_exec = OrderExecution(
            recommendation_id=recommendation_id,
            recommendation_action_id=action.id,
            symbol=action.symbol,
            side=side,
            target_change_pct=action.target_change_pct,
            status="execution_requested",
        )
        db.add(order_exec)
        db.flush()

        app_log(db, f"Orden {side} solicitada para {action.symbol}", context={
            "order_execution_id": order_exec.id,
            "recommendation_id": recommendation_id,
            "symbol": action.symbol,
            "side": side,
            "target_change_pct": action.target_change_pct,
        })

        # Place order via broker
        try:
            result = broker.place_order(
                symbol=action.symbol,
                side=side,
                quantity=0,  # quantity calculated from target_change_pct by broker or manually
                price=None,
            )

            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.broker_response = result.get("raw_response", {})
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

        executions.append({
            "id": order_exec.id,
            "symbol": order_exec.symbol,
            "side": order_exec.side,
            "status": order_exec.status,
            "broker_order_id": order_exec.broker_order_id,
            "error": order_exec.error_message,
        })

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
        "broker_order_id": e.broker_order_id,
        "error_message": e.error_message,
        "executed_quantity": e.executed_quantity,
        "executed_price": e.executed_price,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "sent_at": e.sent_at.isoformat() if e.sent_at else None,
        "completed_at": e.completed_at.isoformat() if e.completed_at else None,
    }
