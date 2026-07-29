"""Execution Durability V2 — mandatory test coverage.

Semantics under test: at-most-once automatic submission + manual
reconciliation on uncertain outcome. Exactly-once CANNOT be claimed (IOL
offers no idempotency keys); what is guaranteed is that a dubious or
interrupted order is never re-sent automatically.

Covers:
- the reinforced flow executes EXACTLY the validated signed preview (no
  snapshot re-fetch, no _plan_order re-run, quantity verbatim);
- intents (claim + UserDecision + OrderExecution rows with request_audit)
  are committed BEFORE any broker interaction and survive crashes (verified
  with a second session over a file-backed database);
- per-order fail-closed state machine (submitting → outcome, one commit per
  transition, never auto-resubmit from submitting/sent/reconciliation);
- recommendation outcome semantics (approved / execution_failed /
  execution_partial / manual_reconciliation_required);
- /decision no longer offers an alternative executable approval route.

NO real IOL calls: IolBrokerClient is patched to explode for every test in
this module.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
    UserDecision,
)
from app.services.execution import (
    approve_and_execute,
    build_execution_preview,
    confirmation_phrase,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


@pytest.fixture(autouse=True)
def no_iol_ever():
    """Guarantee no test in this module can instantiate the real IOL client."""
    with mock.patch(
        "app.services.execution.IolBrokerClient",
        side_effect=AssertionError("Real IOL client must never be instantiated in tests"),
    ):
        yield


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@pytest.fixture
def file_db(tmp_path):
    """File-backed DB with a session factory, so a SECOND independent session
    can observe what actually got persisted (crash-survival checks)."""
    engine = create_engine(f"sqlite:///{tmp_path}/durability.db")
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session, factory
    session.close()
    engine.dispose()


def _make_snapshot(db: Session, total_value=100000, cash=12000, with_msft=False) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=total_value, cash=cash, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    if with_msft:
        db.add(PortfolioPosition(
            snapshot_id=snap.id, symbol="MSFT", asset_type="CEDEAR",
            instrument_type="CEDEAR", currency="USD", quantity=12,
            market_value=28000, avg_price=340, pnl_pct=0.08,
        ))
    db.flush()
    return snap


def _make_recommendation(db: Session, symbols=("AAPL",), target_pct=-0.05) -> Recommendation:
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for symbol in symbols:
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=symbol,
            target_change_pct=target_pct, reason="test",
        ))
    db.flush()
    return rec


@contextmanager
def exec_settings(**overrides):
    s = get_settings()
    saved = {k: getattr(s, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(s, k, v)
        yield s
    finally:
        for k, v in saved.items():
            setattr(s, k, v)


def _full_auth_settings(**extra):
    base = dict(
        broker_mode="mock",
        order_execution_enabled=False,
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        execution_max_recommendation_age_minutes=60,
        execution_max_order_value=1_000_000.0,
        execution_max_total_value=1_000_000.0,
        execution_max_portfolio_pct=0.50,
    )
    base.update(extra)
    return base


def _reinforced_approve(db, rec, preview, key=TEST_ADMIN_KEY, **overrides):
    kwargs = dict(
        execution_key=key,
        preview_hash=preview["preview_hash"],
        preview_generated_at=preview["generated_at"],
        confirmation_text=confirmation_phrase(rec.id),
    )
    kwargs.update(overrides)
    return approve_and_execute(db, rec.id, **kwargs)


def _sent_broker():
    broker = mock.MagicMock()
    broker.place_order.return_value = {
        "status": "sent", "order_id": "MOCK-OK", "raw_response": {"ok": True}, "endpoint_used": "mock",
    }
    return broker


# ───────────────────────────────────────────────────────────────────
# Parte 1 — ejecutar exactamente el preview validado (tests 1-5)
# ───────────────────────────────────────────────────────────────────


def test_reinforced_flow_does_not_refetch_snapshot_after_validation(db):
    from app.services import execution as exec_mod
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    real_fn = exec_mod._get_latest_snapshot
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_latest_snapshot", side_effect=real_fn) as snap_spy, \
             mock.patch("app.services.execution._get_execution_broker", return_value=_sent_broker()):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    # Exactly one call: the validation rebuild. ZERO extra calls in execution.
    assert snap_spy.call_count == 1


def test_reinforced_flow_does_not_replan_orders_after_validation(db):
    from app.services import execution as exec_mod
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    real_fn = exec_mod._plan_order
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._plan_order", side_effect=real_fn) as plan_spy, \
             mock.patch("app.services.execution._get_execution_broker", return_value=_sent_broker()):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    # One action → one _plan_order call inside the validation rebuild only.
    assert plan_spy.call_count == 1


def test_quantity_sent_is_exactly_preview_quantity(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = _sent_broker()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        expected_qty = preview["orders_preview"][0]["quantity_planned"]
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    _, kwargs = broker.place_order.call_args
    assert kwargs["quantity"] == expected_qty
    assert kwargs["symbol"] == "AAPL"
    assert kwargs["side"] == "sell"


def test_new_snapshot_after_validation_does_not_change_quantity(db):
    """Even if a bigger snapshot appears mid-approve (after the validation
    rebuild), the sent quantity is the signed preview's, verbatim."""
    from app.services import execution as exec_mod
    snap = _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    real_fn = exec_mod._get_latest_snapshot
    calls = {"n": 0}

    def snapshot_sequence(session):
        calls["n"] += 1
        if calls["n"] == 1:
            return real_fn(session)  # validation rebuild sees the original
        inflated = mock.MagicMock()
        inflated.id = snap.id + 999
        inflated.total_value = 200000  # would double the recalculated qty
        inflated.currency = "USD"
        inflated.positions = []
        return inflated

    broker = _sent_broker()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        expected_qty = preview["orders_preview"][0]["quantity_planned"]
        with mock.patch("app.services.execution._get_latest_snapshot", side_effect=snapshot_sequence), \
             mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    _, kwargs = broker.place_order.call_args
    assert kwargs["quantity"] == expected_qty == 2


def test_actions_changed_before_validation_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        action = db.query(RecommendationAction).filter(
            RecommendationAction.recommendation_id == rec.id
        ).first()
        action.target_change_pct = -0.10
        db.commit()
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 409
    assert result["code"] == "preview_mismatch"
    assert db.query(OrderExecution).count() == 0


# ───────────────────────────────────────────────────────────────────
# Parte 2/3 — intents persistidos antes del broker (tests 6-11)
# ───────────────────────────────────────────────────────────────────


def _run_flow_capturing_broker_view(file_db):
    """Run the durable flow; inside place_order, observe DB state from a
    SECOND independent session. Returns (result, captured, place_order_calls)."""
    session, factory = file_db
    _make_snapshot(session)
    rec = _make_recommendation(session)
    session.commit()
    captured = {}

    def place_order(**kwargs):
        other = factory()
        try:
            other_rec = other.get(Recommendation, rec.id)
            rows = other.query(OrderExecution).filter(
                OrderExecution.recommendation_id == rec.id
            ).all()
            captured["rec_status"] = other_rec.status
            captured["intent_count"] = len(rows)
            captured["intent_status"] = rows[0].status if rows else None
            captured["intent_audit"] = (rows[0].broker_response or {}).get("request_audit") if rows else None
            captured["intent_quantity"] = rows[0].quantity_planned if rows else None
        finally:
            other.close()
        return {"status": "sent", "order_id": "CAP-1", "raw_response": {"ok": True}, "endpoint_used": "mock"}

    broker = mock.MagicMock()
    broker.place_order.side_effect = place_order
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(session, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(session, rec, preview)
    return result, captured, broker.place_order.call_count, preview


def test_intents_committed_in_db_before_broker_call(file_db):
    result, captured, calls, _ = _run_flow_capturing_broker_view(file_db)
    assert result["status"] == "approved"
    assert calls == 1
    assert captured["intent_count"] == 1  # visible from another session → committed


def test_recommendation_is_execution_pending_when_broker_called(file_db):
    _, captured, _, _ = _run_flow_capturing_broker_view(file_db)
    assert captured["rec_status"] == "execution_pending"


def test_order_execution_persisted_when_broker_called(file_db):
    _, captured, _, _ = _run_flow_capturing_broker_view(file_db)
    assert captured["intent_count"] == 1
    assert captured["intent_quantity"] == 2


def test_intent_is_submitting_when_broker_called(file_db):
    _, captured, _, _ = _run_flow_capturing_broker_view(file_db)
    assert captured["intent_status"] == "submitting"


def test_intent_preserves_snapshot_hash_and_quantity(file_db):
    result, captured, _, preview = _run_flow_capturing_broker_view(file_db)
    audit = captured["intent_audit"]
    assert audit is not None
    assert audit["snapshot_id"] == preview["snapshot_id"]
    assert audit["preview_hash"] == preview["preview_hash"]
    assert audit["preview_generated_at"] == preview["generated_at"]
    assert audit["estimated_notional"] == preview["orders_preview"][0]["estimated_notional"]
    assert audit["portfolio_pct"] == preview["orders_preview"][0]["portfolio_pct"]
    assert audit["execution_request_id"]
    assert captured["intent_quantity"] == preview["orders_preview"][0]["quantity_planned"]


def test_broker_response_does_not_erase_request_audit(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=_sent_broker()):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.broker_response["request_audit"]["preview_hash"] == preview["preview_hash"]
    assert oe.broker_response["broker_result"] == {"ok": True}


# ───────────────────────────────────────────────────────────────────
# Caída / resultado incierto (tests 12-15)
# ───────────────────────────────────────────────────────────────────


def test_uncertain_outcome_survives_session_and_blocks_second_approve(file_db):
    """Broker raises mid-call (crash/timeout): the intent must survive the
    session that made the broker call, and a NEW session must block resend."""
    session, factory = file_db
    _make_snapshot(session)
    rec = _make_recommendation(session)
    session.commit()
    rec_id = rec.id

    broker = mock.MagicMock()
    broker.place_order.side_effect = RuntimeError("connection lost mid-request")
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(session, rec_id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(session, rec, preview)
    assert result["status"] == "manual_reconciliation_required"

    # Simulate process death: close the session that did the broker call.
    session.rollback()
    session.close()

    fresh = factory()
    try:
        oe = fresh.query(OrderExecution).filter(OrderExecution.recommendation_id == rec_id).one()
        assert oe.status == "manual_reconciliation_required"  # intent survived
        assert oe.broker_response["request_audit"]["preview_hash"] == preview["preview_hash"]

        with exec_settings(**_full_auth_settings()):
            second = approve_and_execute(fresh, rec_id)
        assert second["status_code"] == 409
    finally:
        fresh.close()
    assert broker.place_order.call_count == 1


def test_crash_leaves_submitting_second_approve_409_no_resend(file_db):
    """Hard-crash simulation: process died right after persisting 'submitting'
    (before any outcome). On restart, approve must 409 and never resend."""
    session, factory = file_db
    _make_snapshot(session)
    rec = _make_recommendation(session)
    session.flush()
    action = session.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id
    ).first()
    rec.status = "execution_pending"
    session.add(UserDecision(recommendation_id=rec.id, decision="approved", note=""))
    session.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="submitting", validation_status="passed",
        quantity_planned=2, quantity=2, quantity_sent=2,
        broker_response={"request_audit": {"preview_hash": "x" * 64}, "broker_result": None},
    ))
    session.commit()
    rec_id = rec.id
    session.close()

    fresh = factory()
    try:
        broker = mock.MagicMock()
        with exec_settings(**_full_auth_settings()):
            with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
                result = approve_and_execute(fresh, rec_id)
        assert result["status_code"] == 409
        broker.place_order.assert_not_called()
        oe = fresh.query(OrderExecution).filter(OrderExecution.recommendation_id == rec_id).one()
        assert oe.status == "submitting"  # stuck visibly, awaiting manual reconciliation
    finally:
        fresh.close()


def test_submitting_intent_is_never_resubmitted(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.flush()
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id
    ).first()
    rec.status = "execution_pending"
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="submitting", validation_status="passed",
        quantity_planned=2, quantity=2, quantity_sent=2,
    ))
    db.commit()
    broker = mock.MagicMock()
    with exec_settings(**_full_auth_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 409
    broker.place_order.assert_not_called()


def test_manual_reconciliation_intent_is_never_resubmitted(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.flush()
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id
    ).first()
    rec.status = "manual_reconciliation_required"
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="manual_reconciliation_required", validation_status="passed",
        quantity_planned=2, quantity=2, quantity_sent=2,
    ))
    db.commit()
    broker = mock.MagicMock()
    with exec_settings(**_full_auth_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 409
    broker.place_order.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Parte 4 — resultado de la recomendación (tests 16-19)
# ───────────────────────────────────────────────────────────────────


def test_all_orders_sent_recommendation_approved(db):
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"))
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=_sent_broker()):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "approved"


def test_no_orders_sent_recommendation_execution_failed(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = mock.MagicMock()
    broker.place_order.return_value = {"status": "error", "error": "definitive broker error", "raw_response": {}}
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "execution_failed"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "execution_failed"
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "failed"


def test_some_orders_sent_recommendation_execution_partial(db):
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"))
    db.commit()
    broker = mock.MagicMock()
    broker.place_order.side_effect = [
        {"status": "sent", "order_id": "OK-1", "raw_response": {}, "endpoint_used": "mock"},
        {"status": "rejected", "error": "insufficient balance", "raw_response": {}},
    ]
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "execution_partial"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "execution_partial"
    statuses = {e["status"] for e in result["executions"]}
    assert statuses == {"execution_sent", "rejected_by_broker"}


def test_uncertain_outcome_recommendation_manual_reconciliation(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = mock.MagicMock()
    broker.place_order.side_effect = TimeoutError("read timeout — request may have reached IOL")
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "manual_reconciliation_required"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "manual_reconciliation_required"
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "manual_reconciliation_required"


# ───────────────────────────────────────────────────────────────────
# Parte 5 — ruta alternativa cerrada (tests 20-21)
# ───────────────────────────────────────────────────────────────────


def test_decision_approved_is_gone_and_never_calls_approve():
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    with mock.patch("app.api.routes.approve_and_execute") as approve_spy:
        resp = client.post("/api/recommendations/999999/decision", json={"decision": "approved"})
    assert resp.status_code == 410
    assert "/approve" in resp.json()["detail"]
    approve_spy.assert_not_called()


def test_decision_rejected_still_works():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.db.session import SessionLocal

    real_db = SessionLocal()
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    real_db.add(rec)
    real_db.commit()
    rec_id = rec.id
    try:
        client = TestClient(app)
        resp = client.post(f"/api/recommendations/{rec_id}/decision", json={"decision": "rejected"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"
    finally:
        cleanup = SessionLocal()
        try:
            for model in (UserDecision, Recommendation):
                for row in cleanup.query(model).filter(
                    (model.recommendation_id == rec_id) if model is UserDecision else (model.id == rec_id)
                ).all():
                    cleanup.delete(row)
            cleanup.commit()
        finally:
            cleanup.close()
        real_db.close()


# ───────────────────────────────────────────────────────────────────
# Compatibilidad (tests 22-23)
# ───────────────────────────────────────────────────────────────────


def test_mock_legacy_flow_still_works_without_iol(db):
    """Legacy mock approve (no reinforced fields) keeps working — and the
    autouse fixture guarantees IolBrokerClient is never touched."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", order_execution_enabled=False):
        result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"
    assert len(result["executions"]) == 1


def test_module_patches_iol_client_everywhere():
    """Test 23: the autouse fixture makes any IolBrokerClient instantiation
    blow up, so no test in this module can reach IOL."""
    from app.services import execution as exec_mod
    with pytest.raises(AssertionError):
        exec_mod.IolBrokerClient()
