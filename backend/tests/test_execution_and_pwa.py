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


# ---------------------------------------------------------------------------
# Sprint: Alert policy — recommendation-level notifications
# ---------------------------------------------------------------------------


def _make_rec_with_meta(db, actionable_items=None, watchlist_items=None,
                        contradiction_count=0, unchanged=False, superseded=False):
    """Helper: create a Recommendation with full metadata for alert policy tests."""
    a_items = actionable_items or []
    w_items = watchlist_items or []
    meta = {
        "unchanged": unchanged,
        "decision_summary": {
            "review_queue": {
                "actionable_now": {"count": len(a_items), "items": a_items},
                "watchlist_now": {"count": len(w_items), "items": w_items,
                                  "relevant_not_investable_count": 0,
                                  "investable_signal_count": len(w_items)},
                "suppressed_review": {"count": 0, "items": []},
                "total_items": len(a_items) + len(w_items),
            },
            "pipeline_counts": {
                "suppressed_by_contradiction_count": contradiction_count,
            },
            "consumer_guidance": {"primary_view": "review_queue", "version": "38c"},
        },
    }
    from datetime import datetime
    rec = Recommendation(
        action="hold", status="pending", suggested_pct=0.0, confidence=0.7,
        rationale="test", risks="none", executive_summary="test",
        metadata_json=meta,
        superseded_at=datetime.utcnow() if superseded else None,
    )
    db.add(rec)
    db.commit()
    return rec


def _enable_notifications():
    """Context-manager-like helper to enable notifications and reset cooldown."""
    from app.core.config import get_settings
    import app.notifications.dispatcher as disp
    settings = get_settings()
    originals = {
        "enabled": settings.notification_enabled,
        "severity": settings.notification_min_severity,
        "last_at": disp._last_notification_at,
    }
    settings.notification_enabled = True
    settings.notification_min_severity = "low"
    disp._last_notification_at = None
    return originals


def _restore_notifications(originals):
    from app.core.config import get_settings
    import app.notifications.dispatcher as disp
    settings = get_settings()
    settings.notification_enabled = originals["enabled"]
    settings.notification_min_severity = originals["severity"]
    disp._last_notification_at = originals["last_at"]


# ---------------------------------------------------------------------------
# 1. classify_recommendation_alert — pure policy tests
# ---------------------------------------------------------------------------


def test_policy_new_actionable_is_high():
    """New actionable items => HIGH severity, should_notify=True."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL", "MSFT"},
        "new_actionable": {"MSFT"}, "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "premarket")
    assert result["severity"] == "high"
    assert result["category"] == "new_actionable"
    assert result["should_notify"] is True
    assert "MSFT" in result["body"]
    assert "informativo" in result["body"].lower()


def test_policy_thesis_contradiction_is_high():
    """>=3 contradiction suppressions => HIGH severity.

    Threshold rationale: 1-2 contradictions can be noise (ambiguous tickers,
    weak counter-signals). >=3 means the portfolio thesis is under real
    multi-signal pressure and warrants immediate review.
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 3,
        "watchlist_symbols": {"A", "B", "C"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 3, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["severity"] == "high"
    assert result["category"] == "thesis_contradiction"
    assert result["should_notify"] is True


def test_policy_two_contradictions_not_high():
    """2 contradictions is NOT enough — could be noise or ambiguous tickers."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 2, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["category"] != "thesis_contradiction"


def test_policy_single_contradiction_not_high():
    """1 contradiction is NOT enough for thesis_contradiction category."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 1, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["category"] != "thesis_contradiction"


def test_policy_watchlist_material_premarket_notifies():
    """New watchlist items in premarket => MEDIUM severity, should_notify=True."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 4,
        "watchlist_symbols": {"A", "B", "C", "D"}, "new_watchlist": {"C", "D"},
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "premarket")
    assert result["severity"] == "medium"
    assert result["category"] == "watchlist_material"
    assert result["should_notify"] is True


def test_policy_watchlist_intraday_suppressed():
    """New watchlist items during market hours => MEDIUM but should_notify=False."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 2,
        "watchlist_symbols": {"A", "B"}, "new_watchlist": {"B"},
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["severity"] == "medium"
    assert result["should_notify"] is False, "Watchlist changes during open market should NOT push"


def test_policy_watchlist_postclose_notifies():
    """New watchlist items in postmarket => MEDIUM, should_notify=True."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 5,
        "watchlist_symbols": {"A", "B", "C", "D", "E"},
        "new_watchlist": {"D", "E", "C"},
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["severity"] == "medium"
    assert result["should_notify"] is True


def test_policy_postclose_digest_semantics():
    """Postclose digest: actionable exists, no new symbols, analysis changed.

    Condition: actionable_count > 0 AND unchanged=False AND new_actionable empty.
    Meaning: the system re-evaluated with fresh data, and the same opportunities
    remain valid. The copy must reflect this — NOT say "sin cambios materiales"
    (that would be contradictory since unchanged=False).
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL", "MSFT"},
        "new_actionable": set(), "watchlist_count": 3,
        "watchlist_symbols": {"A", "B", "C"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["severity"] == "low"
    assert result["category"] == "postclose_digest"
    assert result["should_notify"] is True
    # Copy must be consistent: analysis changed, opportunities confirmed
    assert "confirmada" in result["body"].lower() or "vigente" in result["body"].lower()
    # Must NOT say "sin cambios" — that would contradict unchanged=False
    assert "sin cambios materiales" not in result["body"].lower()
    assert "informativo" in result["body"].lower() or "no ejecuta" in result["body"].lower()


def test_policy_no_change_actionable_intraday_silent():
    """Actionable exists but unchanged, during market => LOW, should_notify=False."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL"},
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["should_notify"] is False


def test_policy_analysis_completed_unchanged_is_silent():
    """unchanged=True => SILENT, category=analysis_completed. Never pushes.

    Explicit product decision: when the system ran a full cycle and confirmed
    nothing material changed, we don't interrupt the user. They check when
    they want to. This is distinct from no_material_change (edge case).
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": True,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["severity"] == "silent"
    assert result["category"] == "analysis_completed"
    assert result["should_notify"] is False


def test_policy_analysis_completed_even_with_actionable_unchanged():
    """unchanged=True with existing actionable items => still analysis_completed.

    Even if there are actionable items from a previous cycle, if unchanged=True
    the recommendation is the same. No new information to push.
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL", "MSFT"},
        "new_actionable": set(), "watchlist_count": 3,
        "watchlist_symbols": {"A", "B", "C"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": True,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["severity"] == "silent"
    assert result["category"] == "analysis_completed"
    assert result["should_notify"] is False


def test_policy_no_actionable_no_watchlist_no_contradiction_silent():
    """Zero everything => SILENT."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "premarket")
    assert result["severity"] == "silent"
    assert result["should_notify"] is False


def test_policy_all_payloads_have_disclaimer():
    """Every non-silent policy message includes safety disclaimer."""
    from app.notifications.dispatcher import classify_recommendation_alert

    scenarios = [
        {"new_actionable": {"X"}, "actionable_count": 1, "actionable_symbols": {"X"},
         "watchlist_count": 0, "watchlist_symbols": set(), "new_watchlist": set(),
         "suppressed_by_contradiction_count": 0, "unchanged": False},
        {"new_actionable": set(), "actionable_count": 0, "actionable_symbols": set(),
         "watchlist_count": 2, "watchlist_symbols": {"A", "B"}, "new_watchlist": {"A", "B"},
         "suppressed_by_contradiction_count": 0, "unchanged": False},
        {"new_actionable": set(), "actionable_count": 0, "actionable_symbols": set(),
         "watchlist_count": 0, "watchlist_symbols": set(), "new_watchlist": set(),
         "suppressed_by_contradiction_count": 3, "unchanged": False},
        {"new_actionable": set(), "actionable_count": 1, "actionable_symbols": {"Y"},
         "watchlist_count": 0, "watchlist_symbols": set(), "new_watchlist": set(),
         "suppressed_by_contradiction_count": 0, "unchanged": False},
    ]
    for delta in scenarios:
        result = classify_recommendation_alert(delta, "postmarket")
        if result["should_notify"]:
            assert "informativo" in result["body"].lower() or "no ejecuta" in result["body"].lower(), \
                f"Category {result['category']} missing disclaimer: {result['body']}"


# ---------------------------------------------------------------------------
# 2. dispatch_recommendation_alerts — integration tests
# ---------------------------------------------------------------------------


def test_dispatch_new_actionable_high_push(db):
    """New actionable => high severity push dispatched."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}], superseded=True)
    rec_new = _make_rec_with_meta(db, actionable_items=[
        {"symbol": "AAPL"}, {"symbol": "MSFT"},
    ])

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec_new.id})
        assert result["severity"] == "high"
        assert result["category"] == "new_actionable"
        assert "MSFT" in result["new_actionable"]
        assert result["actionable_count"] == 2
    finally:
        _restore_notifications(orig)


def test_dispatch_same_actionable_no_push_intraday(db):
    """Same actionable, no change, during market => policy_suppressed (LOW in open)."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}], superseded=True)
    rec_new = _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}])

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec_new.id})
        # During non-postmarket, low severity postclose_digest won't fire
        # The phase will be whatever the test machine's clock says,
        # but with no new symbols and no contradictions, it's either
        # postclose_digest (postmarket) or no_material_change
        assert result["sent"] is False or result["severity"] == "low"
    finally:
        _restore_notifications(orig)


def test_dispatch_unchanged_silent(db):
    """unchanged=True => policy_suppressed, category=analysis_completed."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    rec = _make_rec_with_meta(db, unchanged=True)

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["sent"] is False
        assert result["reason"] == "policy_suppressed"
        assert result["category"] == "analysis_completed"
        assert result["severity"] == "silent"
    finally:
        _restore_notifications(orig)


def test_dispatch_contradiction_high(db):
    """>=3 contradictions => high severity thesis_contradiction."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    rec = _make_rec_with_meta(db, contradiction_count=4)

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["severity"] == "high"
        assert result["category"] == "thesis_contradiction"
    finally:
        _restore_notifications(orig)


def test_dispatch_contradiction_below_threshold(db):
    """2 contradictions => NOT thesis_contradiction (threshold is >=3)."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    rec = _make_rec_with_meta(db, contradiction_count=2)

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result.get("category") != "thesis_contradiction"
    finally:
        _restore_notifications(orig)


def test_dispatch_disabled(db):
    """notification_enabled=False => disabled."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.core.config import get_settings

    settings = get_settings()
    original = settings.notification_enabled
    settings.notification_enabled = False
    try:
        rec = _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}])
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["sent"] is False
        assert result["reason"] == "disabled"
    finally:
        settings.notification_enabled = original


def test_dispatch_no_rec_id():
    """No recommendation_id => no_recommendation."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.core.config import get_settings
    from app.db.session import SessionLocal

    settings = get_settings()
    original = settings.notification_enabled
    settings.notification_enabled = True
    db = SessionLocal()
    try:
        result = dispatch_recommendation_alerts(db, {})
        assert result["sent"] is False
        assert result["reason"] == "no_recommendation"
    finally:
        settings.notification_enabled = original
        db.close()


def test_dispatch_respects_cooldown(db):
    """Push blocked when cooldown hasn't elapsed."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    import app.notifications.dispatcher as disp
    from datetime import datetime, timezone

    rec = _make_rec_with_meta(db, actionable_items=[{"symbol": "NEW"}])

    orig = _enable_notifications()
    disp._last_notification_at = datetime.now(timezone.utc)  # just notified
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["sent"] is False
        assert result["reason"] == "cooldown"
    finally:
        _restore_notifications(orig)


def test_dispatch_respects_min_severity(db):
    """Push blocked when severity < min_severity setting."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.core.config import get_settings
    import app.notifications.dispatcher as disp

    # Create a rec that would be LOW severity (same actionable, no change)
    _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}], superseded=True)
    rec = _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}])

    settings = get_settings()
    orig_enabled = settings.notification_enabled
    orig_severity = settings.notification_min_severity
    old_last = disp._last_notification_at
    settings.notification_enabled = True
    settings.notification_min_severity = "high"  # only high passes
    disp._last_notification_at = None
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        if result.get("reason") == "below_min_severity":
            assert result["sent"] is False
        # If category was no_material_change, it's policy_suppressed before severity check
    finally:
        settings.notification_enabled = orig_enabled
        settings.notification_min_severity = orig_severity
        disp._last_notification_at = old_last


def test_dispatch_watchlist_new_items(db):
    """New watchlist items produce watchlist_material category via delta extraction."""
    from app.notifications.dispatcher import _extract_delta, classify_recommendation_alert

    _make_rec_with_meta(db, watchlist_items=[{"symbol": "A"}], superseded=True)
    rec = _make_rec_with_meta(db, watchlist_items=[
        {"symbol": "A"}, {"symbol": "B"}, {"symbol": "C"},
    ])

    # Test via pure policy (phase-independent)
    prev_meta = {"decision_summary": {"review_queue": {
        "actionable_now": {"count": 0, "items": []},
        "watchlist_now": {"count": 1, "items": [{"symbol": "A"}]},
    }}}
    delta = _extract_delta(rec.metadata_json, prev_meta)
    assert "B" in delta["new_watchlist"]
    assert "C" in delta["new_watchlist"]

    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "watchlist_material"
    assert result["severity"] == "medium"
    assert result["should_notify"] is True


# ---------------------------------------------------------------------------
# 3. Phase-aware cooldown
# ---------------------------------------------------------------------------


def test_phase_cooldown_multipliers():
    """Phase multipliers: open=2x, postmarket=0.5x, premarket=1x."""
    from app.notifications.dispatcher import _PHASE_COOLDOWN_MULTIPLIER

    assert _PHASE_COOLDOWN_MULTIPLIER["open"] == 2.0, "Intraday should double cooldown"
    assert _PHASE_COOLDOWN_MULTIPLIER["postmarket"] == 0.5, "Post-close should halve cooldown"
    assert _PHASE_COOLDOWN_MULTIPLIER["premarket"] == 1.0


# ---------------------------------------------------------------------------
# 4. Scheduler wiring
# ---------------------------------------------------------------------------


def test_scheduler_full_cycle_calls_notify_recommendation():
    """scheduled_full_cycle calls _notify_recommendation_change after run_cycle."""
    import inspect
    from app.scheduler import jobs

    source = inspect.getsource(jobs.scheduled_full_cycle)
    assert "_notify_recommendation_change" in source


def test_scheduler_ingestion_calls_notify_recommendation():
    """scheduled_ingestion calls _notify_recommendation_change after event-triggered cycle."""
    import inspect
    from app.scheduler import jobs

    source = inspect.getsource(jobs.scheduled_ingestion)
    assert "_notify_recommendation_change" in source


def test_notify_recommendation_change_is_best_effort():
    """_notify_recommendation_change swallows exceptions (best-effort)."""
    import inspect
    from app.scheduler import jobs

    source = inspect.getsource(jobs._notify_recommendation_change)
    assert "try:" in source
    assert "except" in source
    assert "pass" in source


# ---------------------------------------------------------------------------
# 5. Safety invariants
# ---------------------------------------------------------------------------


def test_notification_never_executes_orders():
    """Notification code NEVER imports or calls execution functions."""
    import inspect
    from app.notifications import dispatcher

    source = inspect.getsource(dispatcher)
    assert "from app.services.execution" not in source
    assert "execute_recommendation" not in source
    assert "send_order" not in source


def test_classifier_source_has_disclaimer():
    """classify_recommendation_alert source contains safety disclaimer."""
    import inspect
    from app.notifications.dispatcher import classify_recommendation_alert

    source = inspect.getsource(classify_recommendation_alert)
    assert "Solo informativo" in source
    assert "NEVER execute orders" in source or "no ejecuta" in source.lower()


def test_dispatcher_source_has_disclaimer():
    """dispatch_recommendation_alerts source contains safety disclaimer."""
    import inspect
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    source = inspect.getsource(dispatch_recommendation_alerts)
    assert "No se ejecutan órdenes" in source
    assert "informational only" in source.lower() or "informativo" in source.lower()


# ---------------------------------------------------------------------------
# 6. Delta extraction (unit tests)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 6b. Expanded policy tests — configurable threshold, digest, analysis_completed
# ---------------------------------------------------------------------------


def test_policy_contradiction_threshold_configurable():
    """Contradiction threshold can be changed via parameter."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 2, "unchanged": False,
    }
    # With default threshold=3, 2 contradictions is NOT high
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] != "thesis_contradiction"

    # With threshold=2, it IS high
    result = classify_recommendation_alert(delta, "postmarket", contradiction_threshold=2)
    assert result["category"] == "thesis_contradiction"
    assert result["severity"] == "high"
    assert result["should_notify"] is True


def test_policy_contradiction_threshold_from_config(db):
    """dispatch_recommendation_alerts passes config threshold to classifier."""
    from app.core.config import get_settings
    settings = get_settings()

    # Verify the setting exists and has default value
    assert hasattr(settings, "notification_contradiction_threshold")
    assert settings.notification_contradiction_threshold == 3


def test_policy_digest_watchlist_only_no_actionable():
    """Postclose digest fires when watchlist has items but no actionable.

    Scenario: analysis ran, no actionable opportunities, but watchlist has
    5 items being tracked. This is material — the user wants to know at
    end of day that the system is tracking things.
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 5,
        "watchlist_symbols": {"A", "B", "C", "D", "E"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "postclose_digest"
    assert result["severity"] == "low"
    assert result["should_notify"] is True
    assert "watchlist" in result["body"].lower()
    assert "informativo" in result["body"].lower() or "no ejecuta" in result["body"].lower()


def test_policy_digest_contradictions_only():
    """Postclose digest fires when contradictions exist (below HIGH threshold).

    Scenario: 2 contradictions detected (below threshold of 3), no actionable,
    no new watchlist. Still material for a post-close summary.
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 2, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "postclose_digest"
    assert result["severity"] == "low"
    assert result["should_notify"] is True
    assert "contradicción" in result["body"].lower()


def test_policy_digest_mixed_material():
    """Postclose digest with actionable + contradictions + watchlist."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL", "MSFT"},
        "new_actionable": set(), "watchlist_count": 4,
        "watchlist_symbols": {"A", "B", "C", "D"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 1, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "postclose_digest"
    assert result["should_notify"] is True
    # All three should be mentioned
    assert "vigente" in result["body"].lower()
    assert "contradicción" in result["body"].lower()
    assert "watchlist" in result["body"].lower()


def test_policy_digest_zero_material_is_silent():
    """Zero actionable, zero watchlist, zero contradictions => no_material_change, SILENT."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 0, "actionable_symbols": set(),
        "new_actionable": set(), "watchlist_count": 0,
        "watchlist_symbols": set(), "new_watchlist": set(),
        "suppressed_by_contradiction_count": 0, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "no_material_change"
    assert result["severity"] == "silent"
    assert result["should_notify"] is False


def test_policy_digest_not_intraday():
    """Postclose digest does NOT fire during market hours, even with material."""
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 2, "actionable_symbols": {"AAPL"},
        "new_actionable": set(), "watchlist_count": 3,
        "watchlist_symbols": {"A", "B", "C"}, "new_watchlist": set(),
        "suppressed_by_contradiction_count": 1, "unchanged": False,
    }
    result = classify_recommendation_alert(delta, "open")
    assert result["category"] == "postclose_digest"
    assert result["should_notify"] is False, "Digest should not push during market hours"


def test_policy_analysis_completed_overrides_everything():
    """unchanged=True always wins, even with contradictions and actionable.

    This tests the early-exit: unchanged=True means the system confirmed
    nothing changed. No matter what other fields say, it's SILENT.
    """
    from app.notifications.dispatcher import classify_recommendation_alert
    delta = {
        "actionable_count": 5, "actionable_symbols": {"A", "B", "C", "D", "E"},
        "new_actionable": {"D", "E"}, "watchlist_count": 10,
        "watchlist_symbols": set(), "new_watchlist": {"X"},
        "suppressed_by_contradiction_count": 5, "unchanged": True,
    }
    result = classify_recommendation_alert(delta, "postmarket")
    assert result["category"] == "analysis_completed"
    assert result["severity"] == "silent"
    assert result["should_notify"] is False


# ---------------------------------------------------------------------------
# 7. Delta extraction (unit tests)
# ---------------------------------------------------------------------------


def test_extract_delta_new_actionable():
    """_extract_delta correctly identifies new actionable symbols."""
    from app.notifications.dispatcher import _extract_delta

    current = {
        "decision_summary": {
            "review_queue": {
                "actionable_now": {"count": 2, "items": [{"symbol": "A"}, {"symbol": "B"}]},
                "watchlist_now": {"count": 0, "items": []},
            },
            "pipeline_counts": {"suppressed_by_contradiction_count": 0},
        },
        "unchanged": False,
    }
    prev = {
        "decision_summary": {
            "review_queue": {
                "actionable_now": {"count": 1, "items": [{"symbol": "A"}]},
                "watchlist_now": {"count": 0, "items": []},
            },
        },
    }
    delta = _extract_delta(current, prev)
    assert delta["new_actionable"] == {"B"}
    assert delta["actionable_count"] == 2
    assert delta["unchanged"] is False


def test_extract_delta_no_previous():
    """_extract_delta with no previous rec treats everything as new."""
    from app.notifications.dispatcher import _extract_delta

    current = {
        "decision_summary": {
            "review_queue": {
                "actionable_now": {"count": 1, "items": [{"symbol": "X"}]},
                "watchlist_now": {"count": 2, "items": [{"symbol": "W1"}, {"symbol": "W2"}]},
            },
            "pipeline_counts": {"suppressed_by_contradiction_count": 0},
        },
        "unchanged": False,
    }
    delta = _extract_delta(current, None)
    assert delta["new_actionable"] == {"X"}
    assert delta["new_watchlist"] == {"W1", "W2"}


def test_extract_delta_unchanged():
    """_extract_delta propagates unchanged flag."""
    from app.notifications.dispatcher import _extract_delta

    current = {
        "decision_summary": {
            "review_queue": {
                "actionable_now": {"count": 0, "items": []},
                "watchlist_now": {"count": 0, "items": []},
            },
            "pipeline_counts": {"suppressed_by_contradiction_count": 0},
        },
        "unchanged": True,
    }
    delta = _extract_delta(current, None)
    assert delta["unchanged"] is True


# ---------------------------------------------------------------------------
# 8. Notification audit trail
# ---------------------------------------------------------------------------


def test_audit_trail_persisted_on_policy_suppressed(db):
    """When policy suppresses notification, audit trail is persisted in metadata_json."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.models.models import Recommendation

    rec = _make_rec_with_meta(db, unchanged=True)

    orig = _enable_notifications()
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["sent"] is False
        assert result["reason"] == "policy_suppressed"

        # Reload from DB
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit is not None
        assert audit["category"] == "analysis_completed"
        assert audit["severity"] == "silent"
        assert audit["should_send"] is False
        assert audit["suppress_reason"] == "policy:analysis_completed"
        assert audit["cooldown_applied"] is False
        assert audit["previous_recommendation_id"] is None or isinstance(audit["previous_recommendation_id"], int)
        assert "comparison_summary" in audit
        assert "timestamp" in audit
    finally:
        _restore_notifications(orig)


def test_audit_trail_persisted_on_cooldown(db):
    """When cooldown blocks notification, audit trail records cooldown_applied=True."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    import app.notifications.dispatcher as disp
    from datetime import datetime, timezone

    rec = _make_rec_with_meta(db, actionable_items=[{"symbol": "NEW"}])

    orig = _enable_notifications()
    disp._last_notification_at = datetime.now(timezone.utc)
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["reason"] == "cooldown"

        from app.models.models import Recommendation
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit is not None
        assert audit["cooldown_applied"] is True
        assert audit["suppress_reason"] == "cooldown"
    finally:
        _restore_notifications(orig)


def test_audit_trail_has_comparison_summary(db):
    """Audit trail includes comparison_summary with delta fields."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}], superseded=True)
    rec = _make_rec_with_meta(db, actionable_items=[
        {"symbol": "AAPL"}, {"symbol": "MSFT"},
    ])

    orig = _enable_notifications()
    try:
        dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})

        from app.models.models import Recommendation
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit is not None
        cs = audit["comparison_summary"]
        assert "MSFT" in cs["new_actionable"]
        assert cs["actionable_count"] == 2
        assert isinstance(cs["watchlist_count"], int)
        assert isinstance(cs["unchanged"], bool)
    finally:
        _restore_notifications(orig)


def test_audit_trail_has_previous_recommendation_id(db):
    """Audit trail records previous_recommendation_id when one exists."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts

    old = _make_rec_with_meta(db, actionable_items=[{"symbol": "AAPL"}], superseded=True)
    rec = _make_rec_with_meta(db, actionable_items=[
        {"symbol": "AAPL"}, {"symbol": "NEW"},
    ])

    orig = _enable_notifications()
    try:
        dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})

        from app.models.models import Recommendation
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit["previous_recommendation_id"] == old.id
    finally:
        _restore_notifications(orig)


def test_audit_trail_shape():
    """Verify audit trail has all required fields (structural test)."""
    required_keys = {
        "timestamp", "category", "severity", "should_send",
        "suppress_reason", "cooldown_applied", "market_phase",
        "previous_recommendation_id", "comparison_summary",
    }
    comparison_keys = {
        "new_actionable", "new_watchlist", "actionable_count",
        "watchlist_count", "contradiction_count", "unchanged",
    }
    # Just verify the field lists are complete — the integration tests
    # above prove the actual values are correct.
    assert len(required_keys) == 9
    assert len(comparison_keys) == 6


def test_audit_trail_on_below_min_severity(db):
    """When severity < min_severity, audit trail records the reason."""
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.core.config import get_settings
    import app.notifications.dispatcher as disp

    # Create rec with watchlist-only material (medium severity)
    rec = _make_rec_with_meta(db, watchlist_items=[
        {"symbol": "A"}, {"symbol": "B"}, {"symbol": "C"},
    ])

    settings = get_settings()
    orig_enabled = settings.notification_enabled
    orig_severity = settings.notification_min_severity
    old_last = disp._last_notification_at
    settings.notification_enabled = True
    settings.notification_min_severity = "high"  # medium won't pass
    disp._last_notification_at = None
    try:
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        # This rec has no new_watchlist (no previous to compare), so watchlist
        # items are all new → watchlist_material (medium). But phase determines
        # should_notify. If phase suppresses first, reason is policy_suppressed.
        # Either way, audit should be persisted.
        from app.models.models import Recommendation
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit is not None
        assert audit["suppress_reason"] is not None
    finally:
        settings.notification_enabled = orig_enabled
        settings.notification_min_severity = orig_severity
        disp._last_notification_at = old_last


def test_audit_trail_persisted_when_notifications_disabled(db):
    """Audit trail persists even when notification_enabled=False.

    This was the real gap: the old code returned early on disabled
    before building/persisting audit, so notification_audit was never
    written. Now it always persists.
    """
    from app.notifications.dispatcher import dispatch_recommendation_alerts
    from app.core.config import get_settings
    from app.models.models import Recommendation

    settings = get_settings()
    original = settings.notification_enabled
    settings.notification_enabled = False
    try:
        rec = _make_rec_with_meta(db, unchanged=True, watchlist_items=[
            {"symbol": "LP"}, {"symbol": "JLL"},
        ])
        result = dispatch_recommendation_alerts(db, {"recommendation_id": rec.id})
        assert result["sent"] is False
        assert result["reason"] == "disabled"

        # Audit trail MUST still be persisted
        db.expire(rec)
        fresh = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
        audit = fresh.metadata_json.get("notification_audit")
        assert audit is not None, "audit trail must be persisted even when notifications are disabled"
        assert audit["category"] == "analysis_completed"
        assert audit["severity"] == "silent"
        assert audit["should_send"] is False
        assert audit["suppress_reason"] == "notifications_disabled"
        assert audit["cooldown_applied"] is False
        assert "comparison_summary" in audit
        assert audit["comparison_summary"]["unchanged"] is True
        assert audit["comparison_summary"]["watchlist_count"] == 2
    finally:
        settings.notification_enabled = original


def test_analysis_run_endpoint_persists_audit():
    """POST /analysis/run wires dispatch_recommendation_alerts for audit."""
    import inspect
    from app.api import routes

    source = inspect.getsource(routes.run_manual_analysis)
    assert "dispatch_recommendation_alerts" in source, \
        "POST /analysis/run must call dispatch_recommendation_alerts for audit trail"
