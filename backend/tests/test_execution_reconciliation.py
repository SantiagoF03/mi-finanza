"""Execution Reconciliation V1 — mandatory test coverage.

Covers:
- quantity_sent semantics fix (None at intent, assigned only with
  'submitting' right before the broker call; quote failures are definitive
  pre-broker failures, never uncertain);
- the manual reconciliation queue and endpoint (X-API-Key + X-Execution-Key
  + exact phrase), append-only audit, per-action evidence requirements;
- reconciliation NEVER sends, retries or creates orders and never returns a
  recommendation to an approvable state;
- aggregate recommendation recompute after each reconciliation;
- read-only broker status refresh that never changes state automatically.

NO real IOL calls: IolBrokerClient is patched to explode for every test.
"""

import json
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
    get_reconciliation_queue,
    reconcile_execution,
    reconciliation_phrase,
    refresh_broker_status,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


@pytest.fixture(autouse=True)
def no_iol_ever():
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


def _make_snapshot(db: Session, with_msft=False) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
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


def _make_recommendation(db: Session, symbols=("AAPL",), status="pending") -> Recommendation:
    rec = Recommendation(
        action="rebalancear", status=status, suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for symbol in symbols:
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=symbol,
            target_change_pct=-0.05, reason="test",
        ))
    db.flush()
    return rec


def _make_uncertain_execution(db: Session, status="manual_reconciliation_required",
                              rec_status="manual_reconciliation_required",
                              symbols=("AAPL",)) -> tuple[Recommendation, OrderExecution]:
    """Recommendation + one persisted uncertain intent (as the durable flow
    would leave them after a crash/uncertain outcome)."""
    _make_snapshot(db)
    rec = _make_recommendation(db, symbols=symbols, status=rec_status)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id
    ).first()
    oe = OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id,
        symbol=action.symbol, side="sell", target_change_pct=-0.05,
        status=status, validation_status="passed",
        quantity_planned=2, quantity=2,
        quantity_sent=2 if status in ("submitting", "manual_reconciliation_required") else None,
        broker_response={
            "request_audit": {"snapshot_id": 1, "preview_hash": "h" * 64,
                              "preview_generated_at": "2026-01-01T00:00:00+00:00",
                              "estimated_notional": 3800.0, "portfolio_pct": 0.038,
                              "execution_request_id": "recX-actY-hash"},
            "broker_result": None,
            "reconciliation_audit": [],
        },
    )
    db.add(oe)
    db.commit()
    return rec, oe


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


def _reinforced_approve(db, rec, preview, key=TEST_ADMIN_KEY):
    return approve_and_execute(
        db, rec.id,
        execution_key=key,
        preview_hash=preview["preview_hash"],
        preview_generated_at=preview["generated_at"],
        confirmation_text=confirmation_phrase(rec.id),
    )


def _reconcile(db, oe, action, key=TEST_ADMIN_KEY, phrase=None, **kwargs):
    return reconcile_execution(
        db, oe.id,
        execution_key=key,
        action=action,
        confirmation_text=phrase if phrase is not None else reconciliation_phrase(oe.id, action),
        **kwargs,
    )


# ───────────────────────────────────────────────────────────────────
# Parte 1 — quantity_sent semantics (tests 1-5)
# ───────────────────────────────────────────────────────────────────


def test_new_intent_has_quantity_sent_none(db):
    """At quote time (pre-submitting) the persisted intent must show
    quantity_sent=None and status execution_requested."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    observed = {}

    def spy_quote(broker, symbol, side):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        observed["status"] = row.status
        observed["quantity_sent"] = row.quantity_sent
        return {"available": True, "price": None, "source": "market_order"}

    broker = mock.MagicMock()
    broker.place_order.return_value = {"status": "sent", "order_id": "OK", "raw_response": {}, "endpoint_used": "m"}
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_fresh_quote", side_effect=spy_quote), \
             mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    assert observed["status"] == "execution_requested"
    assert observed["quantity_sent"] is None


def test_no_quote_leaves_failed_with_quantity_sent_none(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = mock.MagicMock()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_fresh_quote",
                        return_value={"available": False, "price": None, "source": "none"}), \
             mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "execution_failed"
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "failed"
    assert oe.quantity_sent is None
    assert "place_order was never called" in oe.error_message
    broker.place_order.assert_not_called()


def test_quote_exception_is_definitive_failed_not_reconciliation(db):
    """An exception BEFORE the broker call can never be 'uncertain'."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = mock.MagicMock()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_fresh_quote",
                        side_effect=RuntimeError("quote service down")), \
             mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "execution_failed"
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "failed"
    assert oe.status != "manual_reconciliation_required"
    assert oe.quantity_sent is None
    broker.place_order.assert_not_called()


def test_submitting_and_quantity_sent_persisted_before_place_order(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    observed = {}

    def spy_place_order(**kwargs):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        observed["status"] = row.status
        observed["quantity_sent"] = row.quantity_sent
        return {"status": "sent", "order_id": "OK", "raw_response": {}, "endpoint_used": "m"}

    broker = mock.MagicMock()
    broker.place_order.side_effect = spy_place_order
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
    assert result["status"] == "approved"
    assert observed["status"] == "submitting"
    assert observed["quantity_sent"] == 2


def test_quantity_sent_assigned_pre_broker_not_at_intent(db):
    """Combined check: None at quote time, == planned at place_order time."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    timeline = []

    def spy_quote(broker, symbol, side):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        timeline.append(("quote", row.quantity_sent))
        return {"available": True, "price": None, "source": "market_order"}

    def spy_place_order(**kwargs):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        timeline.append(("place_order", row.quantity_sent))
        return {"status": "sent", "order_id": "OK", "raw_response": {}, "endpoint_used": "m"}

    broker = mock.MagicMock()
    broker.place_order.side_effect = spy_place_order
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_fresh_quote", side_effect=spy_quote), \
             mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            _reinforced_approve(db, rec, preview)
    assert timeline == [("quote", None), ("place_order", 2)]


# ───────────────────────────────────────────────────────────────────
# Parte 3/5 — cola y protección de endpoints (tests 6-11)
# ───────────────────────────────────────────────────────────────────


def test_queue_returns_only_reconcilable_states(db):
    _make_snapshot(db, with_msft=True)
    rec_ok = _make_recommendation(db, status="approved")
    action_ok = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec_ok.id).first()
    db.add(OrderExecution(
        recommendation_id=rec_ok.id, recommendation_action_id=action_ok.id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="execution_sent", quantity_planned=2,
    ))
    rec_unc, oe_unc = _make_uncertain_execution(db, status="submitting", rec_status="manual_reconciliation_required")
    db.commit()

    queue = get_reconciliation_queue(db)
    ids = {item["id"] for item in queue}
    assert oe_unc.id in ids
    statuses = {item["status"] for item in queue}
    assert "execution_sent" not in statuses  # approved rec's sent order excluded


def test_queue_includes_orders_of_execution_partial_recommendation(db):
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"), status="execution_partial")
    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).all()
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=actions[0].id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="execution_sent", quantity_planned=2,
    ))
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=actions[1].id,
        symbol="MSFT", side="sell", target_change_pct=-0.05,
        status="rejected_by_broker", quantity_planned=2,
    ))
    db.commit()
    queue = get_reconciliation_queue(db)
    symbols = {item["symbol"] for item in queue if item["recommendation_id"] == rec.id}
    assert symbols == {"AAPL", "MSFT"}
    assert all(item["recommendation_status"] == "execution_partial"
               for item in queue if item["recommendation_id"] == rec.id)


def _endpoint_403(path, method="get"):
    from fastapi.testclient import TestClient
    from app.main import app
    settings = get_settings()
    original = settings.api_key
    try:
        settings.api_key = "test-secret-key-123"
        client = TestClient(app)
        resp = getattr(client, method)(path, **({"json": {}} if method == "post" else {}))
        return resp.status_code
    finally:
        settings.api_key = original


def test_reconciliation_queue_requires_api_key():
    assert _endpoint_403("/api/executions/reconciliation-queue") == 403


def test_executions_recent_requires_api_key():
    assert _endpoint_403("/api/executions/recent") == 403


def test_execution_detail_requires_api_key():
    assert _endpoint_403("/api/executions/1") == 403


def test_reconcile_requires_api_key():
    assert _endpoint_403("/api/executions/1/reconcile", method="post") == 403


def test_reconcile_requires_execution_key(db):
    """Without X-Execution-Key (or wrong), 403 at the service — API key alone
    is not enough."""
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_not_sent", key=None)
    assert result["status_code"] == 403


# ───────────────────────────────────────────────────────────────────
# Parte 6 — acciones de conciliación (tests 12-19, 26)
# ───────────────────────────────────────────────────────────────────


def test_invalid_execution_key_changes_nothing(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_not_sent", key="wrong-key")
    assert result["status_code"] == 403
    db.expire_all()
    assert db.get(OrderExecution, oe.id).status == "manual_reconciliation_required"
    assert db.get(Recommendation, rec.id).status == "manual_reconciliation_required"


def test_wrong_phrase_changes_nothing(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_not_sent", phrase=f"conciliar ejecucion {oe.id} como no enviada")
    assert result["status_code"] == 422
    assert result["code"] == "confirmation_mismatch"
    db.expire_all()
    assert db.get(OrderExecution, oe.id).status == "manual_reconciliation_required"


def test_confirm_not_sent_ends_in_not_sent_confirmed(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_not_sent", note="verificado en IOL: sin operación")
    assert result["new_status"] == "not_sent_confirmed"
    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    assert row.status == "not_sent_confirmed"
    assert row.quantity_sent is None  # confirmed nothing was sent
    assert row.completed_at is not None


def test_confirm_not_sent_never_returns_recommendation_to_pending(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_not_sent", note="no llegó")
    db.expire_all()
    rec_row = db.get(Recommendation, rec.id)
    assert rec_row.status == "execution_failed"  # single order, none sent
    assert rec_row.status not in ("pending", "blocked")
    # And it can never be approved again (server-side double-execution guard)
    with exec_settings(**_full_auth_settings()):
        again = approve_and_execute(db, rec.id)
    assert again["status_code"] == 409


def test_confirm_sent_requires_broker_order_id(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_sent", broker_order_id="  ")
    assert result["status_code"] == 422
    assert result["code"] == "broker_order_id_required"
    db.expire_all()
    assert db.get(OrderExecution, oe.id).status == "manual_reconciliation_required"


def test_confirm_rejected_requires_note(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_rejected", note="   ")
    assert result["status_code"] == 422
    assert result["code"] == "note_required"


def test_confirm_executed_requires_id_quantity_and_price(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        no_id = _reconcile(db, oe, "confirm_executed", executed_quantity=2, executed_price=1900.0)
        assert no_id["status_code"] == 422
        no_qty = _reconcile(db, oe, "confirm_executed", broker_order_id="IOL-1",
                            executed_quantity=0, executed_price=1900.0)
        assert no_qty["status_code"] == 422
        no_price = _reconcile(db, oe, "confirm_executed", broker_order_id="IOL-1",
                              executed_quantity=2, executed_price=-1)
        assert no_price["status_code"] == 422
    db.expire_all()
    assert db.get(OrderExecution, oe.id).status == "manual_reconciliation_required"


def test_confirm_executed_sets_values_and_timestamps(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_executed", broker_order_id="IOL-777",
                            executed_quantity=2, executed_price=1900.0,
                            note="verificado en panel IOL")
    assert result["new_status"] == "executed"
    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    assert row.status == "executed"
    assert row.broker_order_id == "IOL-777"
    assert row.executed_quantity == 2
    assert row.executed_price == 1900.0
    assert row.completed_at is not None


def test_non_reconcilable_states_409(db):
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        # execution_requested is NO LONGER here: it is a durable pre-POST
        # state and is now reconcilable, but only as confirm_not_sent (see
        # test_semiautomatic_scheduler.py).
        for status in ("failed", "rejected_by_broker", "executed", "not_sent_confirmed"):
            engine = create_engine("sqlite:///:memory:")
            Base.metadata.create_all(bind=engine)
            session = sessionmaker(bind=engine)()
            try:
                rec, oe = _make_uncertain_execution(session, status=status)
                result = _reconcile(session, oe, "confirm_not_sent", note="x")
                assert result["status_code"] == 409, f"status {status} should be 409"
                assert result["code"] == "not_reconcilable"
            finally:
                session.close()


def test_execution_sent_can_only_be_corrected_to_executed(db):
    rec, oe = _make_uncertain_execution(db, status="execution_sent", rec_status="approved")
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        blocked = _reconcile(db, oe, "confirm_not_sent", note="x")
        assert blocked["status_code"] == 409
        ok = _reconcile(db, oe, "confirm_executed", broker_order_id="IOL-9",
                        executed_quantity=2, executed_price=1900.0)
    assert ok["new_status"] == "executed"


# ───────────────────────────────────────────────────────────────────
# Auditoría append-only (tests 20-21, 28)
# ───────────────────────────────────────────────────────────────────


def test_reconciliation_appends_audit_entry(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        _reconcile(db, oe, "confirm_not_sent", note="motivo X")
    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    audit = row.broker_response["reconciliation_audit"]
    assert len(audit) == 1
    entry = audit[0]
    assert entry["action"] == "confirm_not_sent"
    assert entry["previous_status"] == "manual_reconciliation_required"
    assert entry["new_status"] == "not_sent_confirmed"
    assert entry["note"] == "motivo X"
    assert entry["source"] == "manual_user"
    assert entry["timestamp"]
    # request_audit intact
    assert row.broker_response["request_audit"]["preview_hash"] == "h" * 64


def test_second_reconciliation_preserves_previous_audit(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        _reconcile(db, oe, "confirm_sent", broker_order_id="IOL-5", note="llegó a IOL")
        _reconcile(db, oe, "confirm_executed", broker_order_id="IOL-5",
                   executed_quantity=2, executed_price=1900.0, note="ejecutada confirmada")
    db.expire_all()
    audit = db.get(OrderExecution, oe.id).broker_response["reconciliation_audit"]
    assert len(audit) == 2
    assert audit[0]["action"] == "confirm_sent"
    assert audit[1]["action"] == "confirm_executed"
    assert audit[0]["new_status"] == "execution_sent"


def test_secrets_never_in_responses_or_audit(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY, api_key="an-api-key-value"):
        result = _reconcile(db, oe, "confirm_not_sent", note="ok")
    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    serialized = json.dumps(result) + json.dumps(row.broker_response)
    assert TEST_ADMIN_KEY not in serialized
    assert "an-api-key-value" not in serialized
    queue_serialized = json.dumps(get_reconciliation_queue(db))
    assert TEST_ADMIN_KEY not in queue_serialized


# ───────────────────────────────────────────────────────────────────
# Invariantes: conciliar nunca envía ni crea (tests 22-24, 27)
# ───────────────────────────────────────────────────────────────────


def test_reconcile_never_calls_place_order_or_broker(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        with mock.patch("app.services.execution._get_execution_broker") as broker_factory:
            _reconcile(db, oe, "confirm_not_sent", note="ok")
    broker_factory.assert_not_called()


def test_reconcile_never_creates_order_executions(db):
    rec, oe = _make_uncertain_execution(db)
    before = db.query(OrderExecution).count()
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        _reconcile(db, oe, "confirm_sent", broker_order_id="IOL-1", note="ok")
    assert db.query(OrderExecution).count() == before


def test_reconcile_never_resubmits_and_never_returns_to_requested(db):
    """No reconciliation outcome may land in execution_requested (resendable)."""
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        for action, kwargs in [
            ("confirm_not_sent", {"note": "x"}),
            ("confirm_sent", {"broker_order_id": "IOL-1"}),
            ("confirm_rejected", {"note": "rechazada"}),
            ("confirm_executed", {"broker_order_id": "IOL-1", "executed_quantity": 2, "executed_price": 1900.0}),
        ]:
            engine = create_engine("sqlite:///:memory:")
            Base.metadata.create_all(bind=engine)
            session = sessionmaker(bind=engine)()
            try:
                rec, oe = _make_uncertain_execution(session)
                with mock.patch("app.services.execution._get_execution_broker") as bf:
                    result = _reconcile(session, oe, action, **kwargs)
                bf.assert_not_called()
                session.expire_all()
                row = session.get(OrderExecution, oe.id)
                assert row.status not in ("execution_requested", "pending")
                assert result["new_status"] == row.status
            finally:
                session.close()


def test_repeated_reconciliation_applies_once(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        first = _reconcile(db, oe, "confirm_not_sent", note="primera")
        second = _reconcile(db, oe, "confirm_not_sent", note="repetida")
    assert first["new_status"] == "not_sent_confirmed"
    assert second["status_code"] == 409
    db.expire_all()
    audit = db.get(OrderExecution, oe.id).broker_response["reconciliation_audit"]
    assert len(audit) == 1  # applied exactly once


# ───────────────────────────────────────────────────────────────────
# Parte 7 — recompute del estado agregado (test 25)
# ───────────────────────────────────────────────────────────────────


def test_reconciliation_updates_aggregate_recommendation_status(db):
    """sent + uncertain → manual_reconciliation_required; after reconciling
    the uncertain one as rejected → execution_partial; if instead confirmed
    executed → approved."""
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"), status="manual_reconciliation_required")
    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).all()
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=actions[0].id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="execution_sent", quantity_planned=2, quantity_sent=2,
        broker_response={"request_audit": {}, "broker_result": {}, "reconciliation_audit": []},
    ))
    uncertain = OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=actions[1].id,
        symbol="MSFT", side="sell", target_change_pct=-0.05,
        status="manual_reconciliation_required", quantity_planned=2, quantity_sent=2,
        broker_response={"request_audit": {}, "broker_result": {}, "reconciliation_audit": []},
    )
    db.add(uncertain)
    db.commit()

    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, uncertain, "confirm_rejected", note="rechazada por IOL")
    assert result["recommendation_status"] == "execution_partial"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "execution_partial"


def test_all_sent_after_reconciliation_yields_approved(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, oe, "confirm_sent", broker_order_id="IOL-2")
    assert result["recommendation_status"] == "approved"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "approved"


# ───────────────────────────────────────────────────────────────────
# Parte 8 — consulta read-only al broker
# ───────────────────────────────────────────────────────────────────


def test_refresh_broker_status_requires_keys_and_order_id(db):
    rec, oe = _make_uncertain_execution(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        bad_key = refresh_broker_status(db, oe.id, execution_key="wrong")
        assert bad_key["status_code"] == 403
        no_id = refresh_broker_status(db, oe.id, execution_key=TEST_ADMIN_KEY)
        assert no_id["status_code"] == 422
        assert no_id["code"] == "broker_order_id_missing"


def test_refresh_broker_status_is_read_only_and_never_changes_state(db):
    rec, oe = _make_uncertain_execution(db)
    oe.broker_order_id = "IOL-42"
    db.commit()
    broker = mock.MagicMock()
    broker.get_order_status.return_value = {"order_id": "IOL-42", "status": "terminada", "raw_response": {"estado": "terminada"}}
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = refresh_broker_status(db, oe.id, execution_key=TEST_ADMIN_KEY)
    assert result["status_unchanged"] is True
    assert result["broker_status"] == "terminada"
    broker.place_order.assert_not_called()
    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    assert row.status == "manual_reconciliation_required"  # unchanged: manual decision
    checks = row.broker_response["broker_status_checks"]
    assert len(checks) == 1
    assert checks[0]["raw"]["status"] == "terminada"
    assert row.broker_response["request_audit"]  # preserved
