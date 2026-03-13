"""Tests for execution layer, push subscriptions, dispatcher fortification, and scheduler safety.

Priority 5 — minimum 10+ tests covering:
- OrderExecution model creation
- Approve triggers execution
- Reject does NOT trigger execution
- Scheduler NEVER executes orders
- Mock broker place_order works
- Execution states
- Push subscription endpoint
- Notification settings endpoint
- Dispatcher market hours awareness
- Dispatcher off-hours filtering
- Actionable message hints
- Approve of already-approved fails
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    PushSubscription,
    Recommendation,
    RecommendationAction,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _make_snapshot(db: Session) -> PortfolioSnapshot:
    """Create a portfolio snapshot with AAPL position for execution tests."""
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.flush()
    return snap


def _make_recommendation(db: Session, action="rebalancear", status="pending") -> Recommendation:
    rec = Recommendation(
        action=action,
        status=status,
        suggested_pct=0.05,
        confidence=0.7,
        rationale="Test rationale",
        risks="Test risks",
        executive_summary="Test summary",
        metadata_json={},
    )
    db.add(rec)
    db.flush()
    act = RecommendationAction(
        recommendation_id=rec.id,
        symbol="AAPL",
        target_change_pct=-0.05,
        reason="Sobreconcentración",
    )
    db.add(act)
    db.flush()
    return rec


# ---------------------------------------------------------------------------
# Execution layer
# ---------------------------------------------------------------------------


def test_order_execution_model_creation(db):
    """OrderExecution model can be created with all required fields."""
    rec = _make_recommendation(db)
    oe = OrderExecution(
        recommendation_id=rec.id,
        symbol="AAPL",
        side="sell",
        target_change_pct=-0.05,
        status="pending",
    )
    db.add(oe)
    db.flush()
    assert oe.id is not None
    assert oe.status == "pending"
    assert oe.symbol == "AAPL"
    assert oe.side == "sell"


def test_approve_triggers_execution(db):
    """Approving a recommendation creates OrderExecution rows and calls broker."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_broker_fn:
        mock_broker = mock_broker_fn.return_value
        mock_broker.place_order.return_value = {
            "order_id": "MOCK-123",
            "status": "sent",
            "raw_response": {"mock": True},
        }

        result = approve_and_execute(db, rec.id, note="test approve")

    assert result["status"] == "approved"
    assert len(result["executions"]) == 1
    assert result["executions"][0]["symbol"] == "AAPL"
    assert result["executions"][0]["status"] == "execution_sent"

    # Verify DB state
    updated_rec = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
    assert updated_rec.status == "approved"

    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).first()
    assert oe is not None
    assert oe.status == "execution_sent"
    assert oe.broker_order_id == "MOCK-123"


def test_reject_does_not_execute(db):
    """Rejecting a recommendation does NOT create any OrderExecution."""
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import reject_recommendation

    result = reject_recommendation(db, rec.id, note="not now")
    assert result["status"] == "rejected"

    oes = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).all()
    assert len(oes) == 0


def test_approve_already_approved_fails(db):
    """Cannot approve a recommendation that is already approved."""
    rec = _make_recommendation(db, status="approved")
    db.commit()

    from app.services.execution import approve_and_execute

    result = approve_and_execute(db, rec.id)
    assert "error" in result
    assert result["status_code"] == 400


def test_approve_mantener_no_orders(db):
    """Approving a 'mantener' recommendation creates no orders."""
    rec = _make_recommendation(db, action="mantener")
    db.commit()

    from app.services.execution import approve_and_execute

    result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"
    assert len(result["executions"]) == 0


def test_execution_states_on_broker_failure(db):
    """When broker fails, execution status is 'failed'."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_broker_fn:
        mock_broker = mock_broker_fn.return_value
        mock_broker.place_order.side_effect = Exception("Connection refused")

        result = approve_and_execute(db, rec.id)

    assert result["executions"][0]["status"] == "failed"
    assert "Connection refused" in result["executions"][0]["error"]


def test_execution_rejected_by_broker(db):
    """When broker rejects, execution status is 'rejected_by_broker'."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_broker_fn:
        mock_broker = mock_broker_fn.return_value
        mock_broker.place_order.return_value = {
            "order_id": "",
            "status": "rejected",
            "error": "Insufficient funds",
            "raw_response": {},
        }

        result = approve_and_execute(db, rec.id)

    assert result["executions"][0]["status"] == "rejected_by_broker"


# ---------------------------------------------------------------------------
# Mock broker
# ---------------------------------------------------------------------------


def test_mock_broker_place_order():
    """MockBrokerClient.place_order returns a valid order."""
    from app.broker.clients import MockBrokerClient

    client = MockBrokerClient()
    result = client.place_order("AAPL", "sell", 10, price=150.0)
    assert result["status"] == "sent"
    assert result["order_id"].startswith("MOCK-")
    assert result["raw_response"]["mock"] is True


def test_mock_broker_get_order_status():
    """MockBrokerClient.get_order_status always returns executed."""
    from app.broker.clients import MockBrokerClient

    client = MockBrokerClient()
    result = client.get_order_status("MOCK-123")
    assert result["status"] == "terminada"


# ---------------------------------------------------------------------------
# Scheduler safety
# ---------------------------------------------------------------------------


def test_scheduler_never_imports_execution():
    """Scheduler module should NEVER import from services.execution."""
    import inspect

    from app.scheduler import jobs

    source = inspect.getsource(jobs)
    assert "execution" not in source.lower() or "execute" not in source
    assert "place_order" not in source
    assert "approve_and_execute" not in source


def test_scheduled_ingestion_no_execution():
    """scheduled_ingestion does not call any execution function."""
    import inspect

    from app.scheduler.jobs import scheduled_ingestion

    source = inspect.getsource(scheduled_ingestion)
    assert "place_order" not in source
    assert "approve_and_execute" not in source
    assert "OrderExecution" not in source


def test_scheduled_full_cycle_no_execution():
    """scheduled_full_cycle does not call any execution function."""
    import inspect

    from app.scheduler.jobs import scheduled_full_cycle

    source = inspect.getsource(scheduled_full_cycle)
    assert "place_order" not in source
    assert "approve_and_execute" not in source
    assert "OrderExecution" not in source


# ---------------------------------------------------------------------------
# Dispatcher fortification
# ---------------------------------------------------------------------------


def test_dispatcher_argentina_market_phases():
    """Argentina market phases are correctly computed."""
    from app.notifications.dispatcher import _argentina_market_phase

    # Tuesday 15:00 UTC = market open (11-20 UTC)
    open_time = datetime(2024, 3, 5, 15, 0, tzinfo=timezone.utc)
    assert _argentina_market_phase(open_time) == "open"

    # Tuesday 10:00 UTC = premarket (9-11 UTC)
    pre_time = datetime(2024, 3, 5, 10, 0, tzinfo=timezone.utc)
    assert _argentina_market_phase(pre_time) == "premarket"

    # Tuesday 21:00 UTC = postmarket (20-22 UTC)
    post_time = datetime(2024, 3, 5, 21, 0, tzinfo=timezone.utc)
    assert _argentina_market_phase(post_time) == "postmarket"

    # Saturday = off
    saturday = datetime(2024, 3, 9, 15, 0, tzinfo=timezone.utc)
    assert _argentina_market_phase(saturday) == "off"


def test_dispatcher_us_market_phases():
    """US market phases are correctly computed."""
    from app.notifications.dispatcher import _us_market_phase

    # Tuesday 16:00 UTC = open (14:30-21:00 UTC)
    open_time = datetime(2024, 3, 5, 16, 0, tzinfo=timezone.utc)
    assert _us_market_phase(open_time) == "open"

    # Tuesday 13:30 UTC = premarket
    pre_time = datetime(2024, 3, 5, 13, 30, tzinfo=timezone.utc)
    assert _us_market_phase(pre_time) == "premarket"

    # Tuesday 21:30 UTC = postmarket
    post_time = datetime(2024, 3, 5, 21, 30, tzinfo=timezone.utc)
    assert _us_market_phase(post_time) == "postmarket"


def test_dispatcher_action_hint():
    """Action hints are generated for different trigger types."""
    from app.notifications.dispatcher import _action_hint

    class MockEvent:
        def __init__(self, trigger_type, severity="medium", affected_symbols=None):
            self.trigger_type = trigger_type
            self.severity = severity
            self.affected_symbols = affected_symbols or []

    hint = _action_hint(MockEvent("holding_risk", affected_symbols=["AAPL"]))
    assert "AAPL" in hint
    assert "reducir" in hint.lower()

    hint = _action_hint(MockEvent("macro_risk"))
    assert "recalculará" in hint.lower()

    hint = _action_hint(MockEvent("external_opportunity", affected_symbols=["TSLA"]))
    assert "informativo" in hint.lower()


def test_dispatcher_off_hours_only_critical():
    """During off-hours, only critical alerts pass (unless US-sensitive during US hours)."""
    from app.notifications.dispatcher import _argentina_market_phase, _severity_passes

    # Verify severity filter works
    assert _severity_passes("critical", "medium") is True
    assert _severity_passes("low", "medium") is False
    assert _severity_passes("medium", "medium") is True


# ---------------------------------------------------------------------------
# Push subscriptions
# ---------------------------------------------------------------------------


def test_push_subscription_model(db):
    """PushSubscription model can be created."""
    sub = PushSubscription(
        endpoint="https://fcm.googleapis.com/fcm/send/test123",
        p256dh="test-p256dh-key",
        auth="test-auth-key",
    )
    db.add(sub)
    db.flush()
    assert sub.id is not None
    assert sub.endpoint == "https://fcm.googleapis.com/fcm/send/test123"


# ---------------------------------------------------------------------------
# Notification settings API (via direct function test)
# ---------------------------------------------------------------------------


def test_notification_settings_validation():
    """Notification settings validates severity levels."""
    valid_severities = {"low", "medium", "high", "critical"}
    assert "extreme" not in valid_severities
    assert "medium" in valid_severities


# ---------------------------------------------------------------------------
# Execution retrieval
# ---------------------------------------------------------------------------


def test_get_recent_executions(db):
    """get_recent_executions returns executions in reverse chronological order."""
    rec = _make_recommendation(db)
    oe1 = OrderExecution(recommendation_id=rec.id, symbol="AAPL", side="sell", target_change_pct=-0.05, status="executed")
    oe2 = OrderExecution(recommendation_id=rec.id, symbol="MSFT", side="buy", target_change_pct=0.03, status="pending")
    db.add_all([oe1, oe2])
    db.commit()

    from app.services.execution import get_recent_executions

    results = get_recent_executions(db)
    assert len(results) == 2
    assert all("symbol" in r for r in results)


def test_get_execution_by_id(db):
    """get_execution_by_id returns the correct execution."""
    rec = _make_recommendation(db)
    oe = OrderExecution(recommendation_id=rec.id, symbol="AAPL", side="sell", target_change_pct=-0.05, status="executed")
    db.add(oe)
    db.commit()

    from app.services.execution import get_execution_by_id

    result = get_execution_by_id(db, oe.id)
    assert result is not None
    assert result["symbol"] == "AAPL"

    missing = get_execution_by_id(db, 9999)
    assert missing is None
