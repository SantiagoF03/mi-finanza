"""Real cancellation (DELETE) and the separate FCI contract.

Cancellation:
    DELETE /api/v2/operaciones/{numero} — explicit, human-only, exactly once.
    An ambiguous outcome is never retried: re-sending a DELETE we cannot
    prove failed risks cancelling a DIFFERENT, later order.

FCI:
    POST /api/v2/operar/suscripcion/fci
    POST /api/v2/operar/rescate/fci
    A separate family with its own models and state machine — never an
    OrderExecution, never a limit price.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    FundInstrument,
    FundOperation,
    FundOperationDecision,
    OrderExecution,
    Recommendation,
)
from app.services import fci as fci_service
from app.services.cancellation import (
    CANCELLABLE_STATUSES,
    STATUS_CANCELLATION_REJECTED,
    STATUS_CANCELLATION_UNKNOWN,
    STATUS_CANCELLED,
    build_cancellation_preview,
    cancel_execution,
    cancellation_phrase,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


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


def _cancel_settings(**extra):
    base = dict(
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_cancellation_enabled=True,
        scheduler_market_open_time="10:30",
        scheduler_market_close_time="17:00",
        scheduler_timezone="America/Argentina/Buenos_Aires",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _sent_order(db, status="execution_sent", broker_order_id="183382168"):
    db.add(Recommendation(
        id=1, action="rebalancear", status="execution_pending", suggested_pct=0.1,
        confidence=0.7, rationale="r", risks="k", executive_summary="s",
        metadata_json={},
    ))
    order = OrderExecution(
        recommendation_id=1, symbol="BYMA", side="sell", target_change_pct=-0.1,
        status=status, quantity_planned=1, quantity_sent=1,
        broker_order_id=broker_order_id, broker_response={},
    )
    db.add(order)
    db.commit()
    return order


def _cancel_broker(outcome="cancelled", status="iniciada"):
    broker = mock.MagicMock()
    broker.get_order_status.return_value = {
        "order_id": "183382168", "status": status, "raw_response": {}
    }
    broker.cancel_order.return_value = {
        "outcome": outcome,
        "endpoint_used": "/api/v2/operaciones/183382168",
        "raw_response": {"ok": outcome == "cancelled"},
        "error": "" if outcome == "cancelled" else f"{outcome} outcome",
    }
    return broker


def _approve_cancellation(db, order, broker, **overrides):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        preview = build_cancellation_preview(db, order.id, execution_key=TEST_ADMIN_KEY)
        assert preview.get("can_submit_cancellation") is True, preview.get("blocking_reasons")
        payload = dict(
            execution_key=TEST_ADMIN_KEY,
            preview_hash=preview["preview_hash"],
            preview_generated_at=preview["generated_at"],
            confirmation_text=cancellation_phrase(order.id),
            note="cancelación manual",
        )
        payload.update(overrides)
        return cancel_execution(db, order.id, **payload)


# ───────────────────────────────────────────────────────────────────
# 1 — cancellation sends exactly one DELETE
# ───────────────────────────────────────────────────────────────────


def test_cancellation_sends_exactly_one_delete(db):
    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        result = _approve_cancellation(db, order, broker)

    assert broker.cancel_order.call_count == 1
    assert broker.cancel_order.call_args[0][0] == "183382168"
    assert result["delete_requests_sent"] == 1
    assert result["new_status"] == STATUS_CANCELLED
    db.refresh(order)
    assert order.status == STATUS_CANCELLED


def test_cancellation_preview_reads_fresh_broker_state_and_sends_nothing(db):
    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            preview = build_cancellation_preview(db, order.id, execution_key=TEST_ADMIN_KEY)

    broker.get_order_status.assert_called_once_with("183382168")
    broker.cancel_order.assert_not_called()
    assert preview["would_cancel"] is False
    assert preview["dry_run"] is True
    assert preview["method"] == "DELETE"
    assert preview["endpoint"] == "/api/v2/operaciones/183382168"
    assert preview["broker_status"] == "iniciada"


def test_ambiguous_cancellation_is_never_retried(db):
    """Re-sending a DELETE we cannot prove failed risks cancelling a
    DIFFERENT, later order."""
    order = _sent_order(db)
    broker = _cancel_broker(outcome="cancellation_unknown")
    with exec_settings(**_cancel_settings()):
        result = _approve_cancellation(db, order, broker)

    assert broker.cancel_order.call_count == 1
    assert result["new_status"] == STATUS_CANCELLATION_UNKNOWN
    assert result["requires_reconciliation"] is True
    db.refresh(order)
    assert order.status == STATUS_CANCELLATION_UNKNOWN
    assert order.completed_at is None


def test_rejected_cancellation_is_definitive(db):
    order = _sent_order(db)
    broker = _cancel_broker(outcome="rejected")
    with exec_settings(**_cancel_settings()):
        result = _approve_cancellation(db, order, broker)
    assert result["new_status"] == STATUS_CANCELLATION_REJECTED
    assert result["requires_reconciliation"] is False


def test_a_second_cancellation_attempt_does_not_send_again(db):
    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        _approve_cancellation(db, order, broker)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            second = cancel_execution(
                db, order.id, execution_key=TEST_ADMIN_KEY,
                preview_hash="whatever", preview_generated_at=datetime.now(timezone.utc).isoformat(),
                confirmation_text=cancellation_phrase(order.id),
            )
    assert broker.cancel_order.call_count == 1
    assert "error" in second


# ───────────────────────────────────────────────────────────────────
# 2 — cancellation authorization
# ───────────────────────────────────────────────────────────────────


def test_cancellation_is_blocked_by_its_own_flag(db):
    """Being allowed to send an order says nothing about being allowed to
    cancel one."""
    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings(order_cancellation_enabled=False)):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            preview = build_cancellation_preview(db, order.id, execution_key=TEST_ADMIN_KEY)
            result = cancel_execution(
                db, order.id, execution_key=TEST_ADMIN_KEY, preview_hash="x",
                preview_generated_at=datetime.now(timezone.utc).isoformat(),
                confirmation_text=cancellation_phrase(order.id),
            )
    assert "cancellation_disabled" in preview["blocking_reasons"]
    assert result["code"] == "cancellation_disabled"
    broker.cancel_order.assert_not_called()


def test_cancellation_defaults_to_disabled():
    from app.core.config import Settings

    assert Settings().order_cancellation_enabled is False


@pytest.mark.parametrize("overrides,expected", [
    ({"execution_key": "wrong"}, "invalid_execution_key"),
    ({"confirmation_text": "CANCELAR"}, "confirmation_mismatch"),
    ({"preview_hash": "tampered"}, "preview_mismatch"),
])
def test_cancellation_requires_full_authorization(db, overrides, expected):
    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        result = _approve_cancellation(db, order, broker, **overrides)
    assert result["code"] == expected
    broker.cancel_order.assert_not_called()
    db.refresh(order)
    assert order.status == "execution_sent"


def test_expired_cancellation_preview_is_refused(db):
    order = _sent_order(db)
    broker = _cancel_broker()
    stale = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with exec_settings(**_cancel_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = cancel_execution(
                db, order.id, execution_key=TEST_ADMIN_KEY, preview_hash="x",
                preview_generated_at=stale,
                confirmation_text=cancellation_phrase(order.id),
            )
    assert result["code"] == "preview_expired"
    broker.cancel_order.assert_not_called()


@pytest.mark.parametrize("status", ["executed", "failed", "not_sent_confirmed",
                                    "preflight_cancelled", "execution_ready"])
def test_only_live_orders_are_cancellable(db, status):
    order = _sent_order(db, status=status)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            preview = build_cancellation_preview(db, order.id, execution_key=TEST_ADMIN_KEY)
    assert "not_cancellable_status" in preview["blocking_reasons"]
    assert status not in CANCELLABLE_STATUSES


def test_cancellation_without_broker_order_id_is_blocked(db):
    order = _sent_order(db, broker_order_id="")
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            preview = build_cancellation_preview(db, order.id, execution_key=TEST_ADMIN_KEY)
    assert "broker_order_id_missing" in preview["blocking_reasons"]


def test_confirm_cancelled_remains_manual_reconciliation_only(db):
    """`confirm_cancelled` records that a HUMAN cancelled in IOL's panel. It
    is not, and must not become, a substitute for sending the DELETE."""
    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(db)
    broker = _cancel_broker()
    with exec_settings(**_cancel_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = reconcile_execution(
                db, order.id, execution_key=TEST_ADMIN_KEY, action="confirm_cancelled",
                confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
                note="cancelada a mano en el panel de IOL",
            )
    broker.cancel_order.assert_not_called()
    assert result["new_status"] == "cancelled_by_user"
    assert result["new_status"] != STATUS_CANCELLED


def test_broker_cancel_order_classifies_outcomes():
    """A 4xx is definitive; a 5xx or a network failure is not."""
    import httpx

    from app.broker.clients import IolBrokerClient

    def _client(handler):
        broker = IolBrokerClient(api_base="https://sandbox.iol-test.local",
                                 username="u", password="p")
        broker._client = httpx.Client(transport=httpx.MockTransport(handler), timeout=5)
        broker._access_token = "t"
        broker._expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        return broker

    assert _client(lambda r: httpx.Response(200, json={"ok": True})).cancel_order(
        "1")["outcome"] == "cancelled"
    assert _client(lambda r: httpx.Response(400, json={"e": 1})).cancel_order(
        "1")["outcome"] == "rejected"
    assert _client(lambda r: httpx.Response(503, json={})).cancel_order(
        "1")["outcome"] == "cancellation_unknown"

    def _boom(request):
        raise httpx.ConnectError("boom")

    assert _client(_boom).cancel_order("1")["outcome"] == "cancellation_unknown"


# ───────────────────────────────────────────────────────────────────
# 3 — FCI uses its own contract
# ───────────────────────────────────────────────────────────────────


def _fund(db, symbol="FCIAR", cutoff="15:00", minimum=1000.0, status="verified"):
    fund = FundInstrument(
        symbol=symbol, name="Fondo Test", manager="IOL Asset Management",
        currency="ARS", cutoff_local_time=cutoff, settlement_delay_days=1,
        minimum_amount=minimum, subscription_supported=True, redemption_supported=True,
        active=True, verification_status=status, source="iol_fci_catalog",
        verified_at=datetime.utcnow(),
    )
    db.add(fund)
    db.commit()
    return fund


def _operation(db, *, operation="subscribe", amount=10_000.0, symbol="FCIAR"):
    op = FundOperation(
        fund_symbol=symbol, operation=operation, currency="ARS", amount=amount,
        status=fci_service.STATE_PREPARED, cutoff_local_time="15:00",
        settlement_delay_days=1,
    )
    db.add(op)
    db.commit()
    return op


def test_fci_endpoints_are_the_official_ones():
    capability = fci_service.get_fci_capability()
    assert capability["endpoints"]["subscribe"] == "/api/v2/operar/suscripcion/fci"
    assert capability["endpoints"]["redeem"] == "/api/v2/operar/rescate/fci"
    assert capability["endpoints"]["catalog"] == "/api/v2/Titulos/FCI"
    assert capability["endpoints"]["detail"] == "/api/v2/Titulos/FCI/{simbolo}"
    assert capability["contract_documented"] is True


def test_fci_never_creates_an_order_execution(db):
    _fund(db)
    operation = _operation(db)
    assert db.query(OrderExecution).count() == 0
    assert db.query(FundOperation).count() == 1
    assert operation.status in fci_service.FUND_OPERATION_STATES


@pytest.mark.parametrize("operation", ["subscribe", "redeem"])
def test_fci_preview_has_no_securities_vocabulary(db, operation):
    _fund(db)
    op = _operation(db, operation=operation)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY,
                       execution_preview_secret=TEST_PREVIEW_SECRET,
                       execution_preview_ttl_seconds=300,
                       fci_subscription_enabled=True, fci_redemption_enabled=True,
                       scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        preview = fci_service.build_fund_operation_preview(db, op.id)

    assert preview["execution_family"] == "fund"
    assert preview["operation"] == operation
    assert preview["limit_price"] is None
    assert preview["best_bid"] is None
    assert preview["best_ask"] is None
    assert preview["quantity_step"] is None
    assert preview["would_execute"] is False
    assert preview["immediate"] is False
    assert preview["manager"] == "IOL Asset Management"
    assert preview["minimum_amount"] == 1000.0
    assert preview["settlement_delay_days"] == 1
    assert preview["preview_hash"]


def test_fci_cutoff_is_per_fund_never_hardcoded(db):
    """Applying one fund's cutoff to another mis-times the operation by a
    whole settlement day."""
    early = _fund(db, symbol="EARLY", cutoff="13:00")
    late = _fund(db, symbol="LATE", cutoff="16:30")
    with exec_settings(scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        settings = get_settings()
        at_1400 = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc).replace(
            tzinfo=timezone.utc
        )
        early_state = fci_service.evaluate_cutoff(early, settings=settings, now=at_1400)
        late_state = fci_service.evaluate_cutoff(late, settings=settings, now=at_1400)

    assert early_state["cutoff"] == "13:00"
    assert late_state["cutoff"] == "16:30"
    assert early_state["cutoff"] != late_state["cutoff"]


def test_fci_cutoff_passed_blocks(db):
    fund = _fund(db, cutoff="10:00")
    with exec_settings(scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        # 14:00 UTC = 11:00 ART, past a 10:00 cutoff.
        state = fci_service.evaluate_cutoff(
            fund, settings=get_settings(),
            now=datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc),
        )
    assert state["code"] == "fund_cutoff_passed"
    assert state["open"] is False


def test_fund_without_a_cutoff_cannot_operate(db):
    fund = _fund(db, cutoff=None)
    with exec_settings(scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        state = fci_service.evaluate_cutoff(fund, settings=get_settings())
    assert state["code"] == "fund_cutoff_unknown"


def test_fci_below_minimum_amount_is_blocked(db):
    _fund(db, minimum=50_000.0)
    op = _operation(db, amount=1_000.0)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY,
                       execution_preview_secret=TEST_PREVIEW_SECRET,
                       execution_preview_ttl_seconds=300,
                       fci_subscription_enabled=True,
                       scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        preview = fci_service.build_fund_operation_preview(db, op.id)
    assert "fund_minimum_amount_not_met" in preview["blocking_reasons"]


def test_unverified_fund_is_blocked(db):
    _fund(db, status="candidate")
    op = _operation(db)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY,
                       execution_preview_secret=TEST_PREVIEW_SECRET,
                       execution_preview_ttl_seconds=300,
                       fci_subscription_enabled=True,
                       scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        preview = fci_service.build_fund_operation_preview(db, op.id)
    assert "fund_not_verified" in preview["blocking_reasons"]


@pytest.mark.parametrize("operation,blocking_code", [
    ("subscribe", "fci_subscription_disabled"),
    ("redeem", "fci_redemption_disabled"),
])
def test_fci_flags_gate_each_operation_separately(db, operation, blocking_code):
    _fund(db)
    op = _operation(db, operation=operation)
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY,
                       execution_preview_secret=TEST_PREVIEW_SECRET,
                       execution_preview_ttl_seconds=300,
                       fci_subscription_enabled=False, fci_redemption_enabled=False,
                       scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00", market_holidays=[]):
        preview = fci_service.build_fund_operation_preview(db, op.id)
    assert blocking_code in preview["blocking_reasons"]


def test_fci_flags_default_to_false():
    from app.core.config import Settings

    settings = Settings()
    assert settings.fci_subscription_enabled is False
    assert settings.fci_redemption_enabled is False


def _fci_broker(*, outcome="validated", operation_id="", raise_on_call=False):
    broker = mock.MagicMock()
    if raise_on_call:
        broker.submit_fund_request.side_effect = RuntimeError("network down")
    else:
        broker.submit_fund_request.return_value = {
            "outcome": outcome,
            "operation_id": operation_id,
            "endpoint_used": "/api/v2/operar/suscripcion/fci",
            "raw_response": {"numeroOperacion": operation_id} if operation_id else {},
            "http_requests_sent": 1,
            "error": "" if outcome in ("validated", "submitted") else outcome,
        }
    broker.get_live_cash.return_value = {
        "available": True, "cash": 1_000_000.0, "currency": "ARS",
        "committed": 0.0, "source": "estadocuenta",
    }
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": 1_000_000.0,
        "positions": [{"symbol": "FCIAR", "asset_type": "FondoComundeInversion",
                       "instrument_type": "FondoComundeInversion",
                       "currency": "ARS", "quantity": 100, "market_value": 500_000.0}],
    }
    return broker


def _live_settings(**extra):
    """Settings with the global lock OPEN — used only to prove the inner
    gates work. Production keeps ORDER_EXECUTION_ENABLED=false."""
    base = dict(
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_execution_enabled=True,
        fci_subscription_enabled=True, fci_redemption_enabled=True,
        fci_validation_ttl_seconds=120,
        fci_max_operation_amount=100_000.0, fci_max_daily_amount=500_000.0,
        fci_fee_buffer_pct=0.0, fci_min_cash_reserve=0.0,
        scheduler_timezone="America/Argentina/Buenos_Aires",
        scheduler_market_open_time="10:30", scheduler_market_close_time="17:00",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _validate(db, op, broker, **overrides):
    with exec_settings(**_live_settings(**overrides)):
        return fci_service.validate_fund_operation(db, op.id, broker)


def test_solo_validar_is_never_recorded_as_an_execution(db):
    """A validation must not set an operation id, must not move the operation
    into a submitted state, and must not consume at-most-once budget."""
    _fund(db)
    op = _operation(db)
    broker = _fci_broker(outcome="validated", operation_id="SHOULD-BE-IGNORED")

    result = _validate(db, op, broker)

    assert result["is_execution"] is False
    assert result["solo_validar"] is True
    sent = broker.submit_fund_request.call_args[0][0]
    assert sent["form_data"]["soloValidar"] == "true"
    db.refresh(op)
    # Even though the broker echoed an id, a validation NEVER creates one.
    assert op.broker_operation_id == ""
    assert op.status == fci_service.STATE_VALIDATED
    assert op.status not in fci_service.NO_RESUBMIT_STATES
    assert db.query(OrderExecution).count() == 0


def test_a_prepared_operation_cannot_be_submitted(db):
    _fund(db)
    op = _operation(db)
    broker = _fci_broker()
    with exec_settings(**_live_settings()):
        result = fci_service.submit_fund_operation(
            db, op.id, execution_key=TEST_ADMIN_KEY, preview_hash="x",
            preview_generated_at=datetime.now(timezone.utc).isoformat(),
            confirmation_text=fci_service.fund_confirmation_phrase(op.id),
        )
    assert result["code"] == "fci_validation_required"
    broker.submit_fund_request.assert_not_called()
    db.refresh(op)
    assert op.status == fci_service.STATE_PREPARED


def test_an_expired_validation_cannot_be_submitted(db):
    _fund(db)
    op = _operation(db)
    _validate(db, op, _fci_broker())
    db.refresh(op)
    # Age the validation past its TTL.
    op.validated_at = datetime.utcnow() - timedelta(hours=2)
    db.commit()

    broker = _fci_broker()
    with exec_settings(**_live_settings()):
        state = fci_service.validation_state(op, settings=get_settings())
        result = fci_service.submit_fund_operation(
            db, op.id, execution_key=TEST_ADMIN_KEY, preview_hash="x",
            preview_generated_at=datetime.now(timezone.utc).isoformat(),
            confirmation_text=fci_service.fund_confirmation_phrase(op.id),
        )
    assert state["code"] == "fci_validation_expired"
    assert result["code"] == "fci_validation_expired"
    broker.submit_fund_request.assert_not_called()


def test_changing_the_amount_invalidates_the_validation(db):
    """A soloValidar result is evidence about ONE specific request."""
    _fund(db)
    op = _operation(db, amount=10_000.0)
    _validate(db, op, _fci_broker())
    db.refresh(op)
    with exec_settings(**_live_settings()):
        assert fci_service.validation_state(op, settings=get_settings())["valid"] is True

        op.amount = 90_000.0
        db.commit()
        state = fci_service.validation_state(op, settings=get_settings())
    assert state["valid"] is False
    assert state["code"] == "fci_validation_stale_payload"


def test_changing_the_fund_invalidates_the_validation(db):
    _fund(db)
    _fund(db, symbol="OTHER")
    op = _operation(db)
    _validate(db, op, _fci_broker())
    db.refresh(op)
    with exec_settings(**_live_settings()):
        op.fund_symbol = "OTHER"
        db.commit()
        state = fci_service.validation_state(op, settings=get_settings())
    assert state["code"] == "fci_validation_stale_payload"


def test_a_failed_validation_does_not_mark_the_operation_validated(db):
    _fund(db)
    op = _operation(db)
    _validate(db, op, _fci_broker(outcome="rejected"))
    db.refresh(op)
    assert op.status == fci_service.STATE_PREPARED
    assert op.validated_at is None
    assert op.validated_payload_hash == ""


@pytest.mark.parametrize("operation_kind,endpoint", [
    ("subscribe", "/api/v2/operar/suscripcion/fci"),
    ("redeem", "/api/v2/operar/rescate/fci"),
])
def test_submission_uses_the_documented_form_contract(db, operation_kind, endpoint):
    # A low minimum so the documented example amount (145.54) is admissible.
    _fund(db, minimum=100.0)
    op = _operation(db, operation=operation_kind, amount=145.54)
    broker = _fci_broker()
    _validate(db, op, broker)
    db.refresh(op)

    with exec_settings(**_live_settings()):
        preview = fci_service.build_fund_operation_preview(db, op.id)
        assert preview["can_submit"] is True, preview["blocking_reasons"]
        broker.submit_fund_request.return_value = {
            "outcome": "submitted", "operation_id": "FCI-1",
            "endpoint_used": endpoint,
            "raw_response": {"numeroOperacion": "FCI-1"},
            "http_requests_sent": 1, "error": "",
        }
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            result = fci_service.submit_fund_operation(
                db, op.id, execution_key=TEST_ADMIN_KEY,
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=fci_service.fund_confirmation_phrase(op.id),
            )

    assert result["status"] == fci_service.STATE_PENDING_CONFIRMATION
    sent = broker.submit_fund_request.call_args[0][0]
    assert sent["endpoint"] == endpoint
    assert sent["content_type"] == "application/x-www-form-urlencoded"
    assert sent["form_data"] == {
        "Simbolo": "FCIAR", "Monto": "145.54", "soloValidar": "false",
    }
    assert result["submissions_sent"] == 1
    assert db.query(OrderExecution).count() == 0


def test_a_post_timeout_produces_submission_unknown_and_one_post(db):
    _fund(db)
    op = _operation(db)
    broker = _fci_broker()
    _validate(db, op, broker)
    db.refresh(op)

    with exec_settings(**_live_settings()):
        preview = fci_service.build_fund_operation_preview(db, op.id)
        broker.submit_fund_request.reset_mock()
        broker.submit_fund_request.return_value = {
            "outcome": "submission_unknown", "operation_id": "",
            "endpoint_used": "/api/v2/operar/suscripcion/fci",
            "raw_response": {}, "http_requests_sent": 1, "error": "timeout",
        }
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            result = fci_service.submit_fund_operation(
                db, op.id, execution_key=TEST_ADMIN_KEY,
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=fci_service.fund_confirmation_phrase(op.id),
            )

    assert result["status"] == fci_service.STATE_SUBMISSION_UNKNOWN
    assert result["requires_reconciliation"] is True
    assert broker.submit_fund_request.call_count == 1


def test_a_human_retry_against_submission_unknown_does_not_resend(db):
    """A duplicate subscription is real money. There is no path back."""
    _fund(db)
    op = _operation(db)
    broker = _fci_broker()
    _validate(db, op, broker)
    db.refresh(op)

    with exec_settings(**_live_settings()):
        preview = fci_service.build_fund_operation_preview(db, op.id)
        broker.submit_fund_request.reset_mock()
        broker.submit_fund_request.return_value = {
            "outcome": "submission_unknown", "operation_id": "",
            "endpoint_used": "/api/v2/operar/suscripcion/fci",
            "raw_response": {}, "http_requests_sent": 1, "error": "timeout",
        }
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            fci_service.submit_fund_operation(
                db, op.id, execution_key=TEST_ADMIN_KEY,
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=fci_service.fund_confirmation_phrase(op.id),
            )
            again = fci_service.submit_fund_operation(
                db, op.id, execution_key=TEST_ADMIN_KEY,
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=fci_service.fund_confirmation_phrase(op.id),
            )

    assert again["code"] == "fund_operation_already_submitted"
    assert broker.submit_fund_request.call_count == 1


def test_the_global_lock_blocks_every_fci_path(db):
    """A per-capability flag is a second key, never a replacement for the
    master one."""
    _fund(db)
    op = _operation(db)
    broker = _fci_broker()

    locked = _live_settings(order_execution_enabled=False)
    with exec_settings(**locked):
        validation = fci_service.validate_fund_operation(db, op.id, broker)
        preview = fci_service.build_fund_operation_preview(db, op.id)
        submission = fci_service.submit_fund_operation(
            db, op.id, execution_key=TEST_ADMIN_KEY, preview_hash="x",
            preview_generated_at=datetime.now(timezone.utc).isoformat(),
            confirmation_text=fci_service.fund_confirmation_phrase(op.id),
        )

    assert validation["code"] == "execution_locked"
    assert "execution_locked" in preview["blocking_reasons"]
    assert preview["can_submit"] is False
    assert submission["code"] == "execution_locked"
    broker.submit_fund_request.assert_not_called()
    assert db.query(FundOperationDecision).count() == 0


def test_readiness_requires_the_global_lock_open(db):
    from app.services.execution import get_execution_readiness

    with exec_settings(**_live_settings(order_execution_enabled=False)):
        locked = get_execution_readiness()
    with exec_settings(**_live_settings(order_execution_enabled=True)):
        open_lock = get_execution_readiness()

    assert locked["ready_for_real_fci_subscription"] is False
    assert locked["fci_capability"]["execution_locked"] is True
    assert open_lock["ready_for_real_fci_subscription"] is True
    assert open_lock["fci_capability"]["request_fields"] == [
        "Simbolo", "Monto", "soloValidar"
    ]


def test_fci_state_machine_covers_the_asynchronous_reality():
    """A fund confirms later, at a valuation that did not exist at submission
    time — so "submitted" can never mean "executed"."""
    for state in ("prepared", "validation_requested", "validated",
                  "approval_requested", "submitting", "submitted",
                  "pending_confirmation", "confirmed", "rejected", "cancelled",
                  "submission_unknown", "reconciliation_required"):
        assert state in fci_service.FUND_OPERATION_STATES
    assert "executed" not in fci_service.FUND_OPERATION_STATES
    # Once submitted (or ambiguous) it may never be re-sent.
    assert fci_service.STATE_SUBMISSION_UNKNOWN in fci_service.NO_RESUBMIT_STATES
    assert fci_service.STATE_PENDING_CONFIRMATION in fci_service.NO_RESUBMIT_STATES


def _obsolete_readiness_check():
    from app.services.execution import get_execution_readiness

    with exec_settings(fci_subscription_enabled=True, fci_redemption_enabled=True):
        readiness = get_execution_readiness()
    assert readiness["fci_subscription_enabled"] is True
    # The flag alone can never make an unverified wire format safe.
    assert readiness["fci_request_contract_verified"] is False
    assert readiness["ready_for_real_fci_subscription"] is False
    assert readiness["ready_for_real_fci_redemption"] is False


def test_fund_catalog_refresh_reads_cutoff_per_fund(db):
    broker = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = [
        {"simbolo": "AAA", "descripcion": "Fondo A", "administradora": "IOL",
         "moneda": "peso_Argentino", "horarioCorte": "13:00", "montoMinimo": 500},
        {"simbolo": "BBB", "descripcion": "Fondo B", "administradora": "IOL",
         "moneda": "peso_Argentino", "horarioCorte": "16:00", "montoMinimo": 1000},
        # No cutoff and no minimum → stays a candidate, cannot operate.
        {"simbolo": "CCC", "descripcion": "Fondo C", "moneda": "peso_Argentino"},
    ]
    broker._authorized_get.return_value = response

    result = fci_service.refresh_fund_catalog(db, broker)
    assert result["created"] == 3
    assert fci_service.get_fund(db, "AAA").cutoff_local_time == "13:00"
    assert fci_service.get_fund(db, "BBB").cutoff_local_time == "16:00"
    incomplete = fci_service.get_fund(db, "CCC")
    assert incomplete.verification_status == "candidate"
    assert incomplete.subscription_supported is False


# ───────────────────────────────────────────────────────────────────
# 4 — nothing automatic may cancel, subscribe or redeem
# ───────────────────────────────────────────────────────────────────


_FORBIDDEN = {
    "cancel_execution", "cancel_order", "submit_fund_operation",
    "validate_fund_operation", "build_fund_request", "submit_fund_request",
    "approve_and_execute", "place_order", "submit_order_request",
    "reserve_daily_budget",
}


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/services/orchestrator.py",
    "app/services/analysis_gate.py",
    "app/notifications/dispatcher.py",
    "app/llm/explainer.py",
    "app/news/ingestion.py",
])
def test_automatic_modules_cannot_cancel_subscribe_or_redeem(module_path):
    import ast
    from pathlib import Path

    backend = Path(__file__).resolve().parent.parent
    tree = ast.parse((backend / module_path).read_text())
    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            referenced.update(a.name for a in node.names)
    offending = referenced & _FORBIDDEN
    assert not offending, f"{module_path} references {sorted(offending)}"
