"""The complete FCI API flow, through the real routes.

prepare → validate (soloValidar=true) → preview (signed) → submit (one POST)
        → reconcile (a human states what happened at IOL)

Every step is exercised through FastAPI so the wiring — auth headers, status
codes, the order of the gates — is covered, not just the service functions.

No test here performs network I/O or contacts IOL: the broker is always a
double, and the securities broker factory is patched to explode if the real
client is ever constructed.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base, get_db
from app.main import app
from app.models.models import (
    FundInstrument,
    FundOperation,
    FundOperationDecision,
    OrderExecution,
)
from app.services import fci as fci_service

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
EXEC_HEADERS = {"X-Execution-Key": TEST_ADMIN_KEY}


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/fci.db", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def client(db):
    """A client that does NOT fire the app lifespan.

    Startup starts the global APScheduler; leaving it running leaks into every
    later test that registers jobs (SchedulerAlreadyRunningError). The schema
    these tests need is already created by conftest, so there is nothing the
    startup hook contributes here.
    """
    app.dependency_overrides[get_db] = lambda: db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


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


def _live(**extra):
    """Global lock OPEN — only to prove the inner gates work. Production
    keeps ORDER_EXECUTION_ENABLED=false."""
    base = dict(
        api_key="",
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_execution_enabled=True,
        fci_subscription_enabled=True,
        fci_redemption_enabled=True,
        fci_validation_ttl_seconds=120,
        fci_max_operation_amount=100_000.0,
        fci_max_daily_amount=500_000.0,
        fci_fee_buffer_pct=0.0,
        fci_min_cash_reserve=0.0,
        scheduler_timezone="America/Argentina/Buenos_Aires",
        scheduler_market_open_time="10:30",
        scheduler_market_close_time="17:00",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _fund(db, symbol="FCIAR", cutoff="23:59", minimum=100.0):
    """A fund an operator has administratively verified.

    Cutoff 23:59 so the tests do not depend on when the suite runs; the
    cutoff logic itself has its own dedicated tests.
    """
    fund = FundInstrument(
        symbol=symbol, name="Fondo Test", manager="IOL Asset Management",
        currency="ARS", cutoff_local_time=cutoff, settlement_delay_days=1,
        minimum_amount=minimum, subscription_supported=True,
        redemption_supported=True, active=True, verification_status="verified",
        field_provenance={
            "symbol": "iol_fci_catalog", "currency": "iol_fci_catalog",
            "cutoff_local_time": "admin_verified_override",
            "minimum_amount": "admin_verified_override",
        },
        source="iol_fci_catalog", verified_at=datetime.utcnow(),
    )
    db.add(fund)
    db.commit()
    return fund


def _broker(*, validate="validated", submit="submitted", operation_id="FCI-1"):
    broker = mock.MagicMock()

    def _submit(request, *, lifecycle=None):
        # Mirrors the real client's lifecycle contract: a double that cannot
        # report how far it got would make "never sent" indistinguishable from
        # "may have been sent".
        if isinstance(lifecycle, dict):
            lifecycle.update({"before_send": True, "request_started": True,
                              "response_received": True, "response_parsed": True})
        if request.get("solo_validar"):
            return {"outcome": validate, "operation_id": "",
                    "endpoint_used": request["endpoint"], "raw_response": {},
                    "http_requests_sent": 1, "request_started": True, "error": ""}
        return {"outcome": submit, "operation_id": operation_id if submit == "submitted" else "",
                "endpoint_used": request["endpoint"],
                "raw_response": {"numeroOperacion": operation_id},
                "http_requests_sent": 1, "request_started": True,
                "error": "" if submit == "submitted" else submit}

    broker.submit_fund_request.side_effect = _submit
    broker.get_live_cash.return_value = {
        "available": True, "cash": 1_000_000.0, "currency": "ARS",
        "committed": 0.0, "source": "estadocuenta",
    }
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": 1_000_000.0,
        "positions": [{"symbol": "FCIAR", "asset_type": "FondoComundeInversion",
                       "instrument_type": "FondoComundeInversion",
                       "currency": "ARS", "quantity": 100,
                       "market_value": 800_000.0}],
    }
    return broker


@contextmanager
def _with_broker(broker):
    """Patch BOTH factories the FCI flow can reach, and make the real IOL
    client explode if anything ever tries to construct it."""
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker), \
         mock.patch("app.services.orchestrator._get_broker", return_value=broker), \
         mock.patch("app.broker.clients.IolBrokerClient.__init__",
                    side_effect=AssertionError("real IOL client must never be built")):
        yield


def _prepare(client, *, symbol="FCIAR", operation="subscribe", amount=10_000.0):
    return client.post("/api/funds/operations", json={
        "fund_symbol": symbol, "operation": operation, "amount": amount,
    })


def _full_flow(client, db, broker, *, operation="subscribe", amount=10_000.0):
    """prepare → validate → preview, returning (operation_id, preview)."""
    created = _prepare(client, operation=operation, amount=amount)
    assert created.status_code == 200, created.text
    operation_id = created.json()["fund_operation_id"]

    with _with_broker(broker):
        validated = client.post(
            f"/api/funds/operations/{operation_id}/validate", headers=EXEC_HEADERS
        )
    assert validated.status_code == 200, validated.text

    preview = client.get(f"/api/funds/operations/{operation_id}/preview")
    assert preview.status_code == 200, preview.text
    return operation_id, preview.json()


# ───────────────────────────────────────────────────────────────────
# 1 — preparation
# ───────────────────────────────────────────────────────────────────


def test_prepare_creates_a_prepared_operation_without_touching_iol(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()), _with_broker(broker):
        response = _prepare(client)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == fci_service.STATE_PREPARED
    assert body["next_step"] == "validate"
    broker.submit_fund_request.assert_not_called()
    assert db.query(OrderExecution).count() == 0


def test_prepare_rejects_a_symbol_outside_the_fci_catalog(db, client):
    with exec_settings(**_live()):
        response = _prepare(client, symbol="NOPE")
    assert response.status_code == 404
    assert db.query(FundOperation).count() == 0


@pytest.mark.parametrize("operation", ["buy", "sell", "", "transfer"])
def test_prepare_rejects_a_non_fund_operation_word(db, client, operation):
    _fund(db)
    with exec_settings(**_live()):
        response = _prepare(client, operation=operation)
    assert response.status_code == 422


@pytest.mark.parametrize("amount", [0, -1, None])
def test_prepare_requires_a_positive_amount(db, client, amount):
    """The documented Mi Cuenta contract expresses BOTH sides by Monto, so
    there is no cuotapartes-only request to build."""
    _fund(db)
    with exec_settings(**_live()):
        response = client.post("/api/funds/operations", json={
            "fund_symbol": "FCIAR", "operation": "subscribe", "amount": amount,
        })
    assert response.status_code in (422, 400)


def test_prepare_never_stores_quotaparts_for_execution(db, client):
    _fund(db)
    with exec_settings(**_live()):
        _prepare(client)
    operation = db.query(FundOperation).first()
    assert operation.quotaparts is None


# ───────────────────────────────────────────────────────────────────
# 2 — validation
# ───────────────────────────────────────────────────────────────────


def test_validate_sends_solo_validar_true_once_and_creates_no_decision(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        created = _prepare(client)
        operation_id = created.json()["fund_operation_id"]
        with _with_broker(broker):
            response = client.post(
                f"/api/funds/operations/{operation_id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 200
    body = response.json()
    assert body["is_execution"] is False
    assert body["validated"] is True
    assert broker.submit_fund_request.call_count == 1
    sent = broker.submit_fund_request.call_args[0][0]
    assert sent["form_data"]["soloValidar"] == "true"
    # A validation is NOT an approval.
    assert db.query(FundOperationDecision).count() == 0
    assert db.query(OrderExecution).count() == 0

    operation = db.get(FundOperation, operation_id)
    assert operation.status == fci_service.STATE_VALIDATED
    assert operation.validated_at is not None
    assert operation.validated_payload_hash
    assert operation.broker_operation_id == ""


def test_validate_requires_the_execution_credential(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id = _prepare(client).json()["fund_operation_id"]
        with _with_broker(broker):
            no_key = client.post(f"/api/funds/operations/{operation_id}/validate")
            wrong = client.post(f"/api/funds/operations/{operation_id}/validate",
                                headers={"X-Execution-Key": "wrong"})

    assert no_key.status_code == 403
    assert wrong.status_code == 403
    broker.submit_fund_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# 3 — preview
# ───────────────────────────────────────────────────────────────────


def test_preview_is_signed_read_only_and_never_executes(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        _operation_id, preview = _full_flow(client, db, broker)

    assert preview["would_execute"] is False
    assert preview["dry_run"] is True
    assert preview["immediate"] is False
    assert preview["preview_hash"]
    assert preview["can_submit"] is True, preview["blocking_reasons"]
    # A fund has none of the securities vocabulary.
    assert preview["limit_price"] is None
    assert preview["best_bid"] is None
    assert preview["best_ask"] is None
    assert preview["quantity_step"] is None


def test_a_prepared_operation_cannot_produce_an_approvable_preview(db, client):
    _fund(db)
    with exec_settings(**_live()):
        operation_id = _prepare(client).json()["fund_operation_id"]
        preview = client.get(f"/api/funds/operations/{operation_id}/preview").json()

    assert preview["can_submit"] is False
    assert "fci_validation_required" in preview["blocking_reasons"]


# ───────────────────────────────────────────────────────────────────
# 4 — submission
# ───────────────────────────────────────────────────────────────────


def _submit(client, operation_id, preview, **overrides):
    payload = {
        "preview_hash": preview["preview_hash"],
        "preview_generated_at": preview["generated_at"],
        "confirmation_text": fci_service.fund_confirmation_phrase(operation_id),
        "note": "aprobado por el usuario",
    }
    payload.update(overrides)
    return client.post(
        f"/api/funds/operations/{operation_id}/submit", json=payload, headers=EXEC_HEADERS
    )


@pytest.mark.parametrize("operation,endpoint", [
    ("subscribe", "/api/v2/operar/suscripcion/fci"),
    ("redeem", "/api/v2/operar/rescate/fci"),
])
def test_submit_sends_one_post_with_the_documented_contract(db, client, operation, endpoint):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker, operation=operation)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == fci_service.STATE_PENDING_CONFIRMATION
    assert body["submissions_sent"] == 1
    assert body["immediate"] is False

    assert broker.submit_fund_request.call_count == 1
    sent = broker.submit_fund_request.call_args[0][0]
    assert sent["endpoint"] == endpoint
    assert sent["content_type"] == "application/x-www-form-urlencoded"
    assert set(sent["form_data"]) == {"Simbolo", "Monto", "soloValidar"}
    assert sent["form_data"]["soloValidar"] == "false"
    # A fund NEVER becomes an OrderExecution.
    assert db.query(OrderExecution).count() == 0


def test_submit_requires_the_exact_phrase(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview,
                               confirmation_text="EJECUTAR")

    assert response.status_code == 422
    broker.submit_fund_request.assert_not_called()


def test_submit_requires_a_matching_preview(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview, preview_hash="tampered")

    assert response.status_code == 409
    broker.submit_fund_request.assert_not_called()


def test_an_expired_preview_is_refused(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        stale = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview, preview_generated_at=stale)

    assert response.status_code == 409
    broker.submit_fund_request.assert_not_called()


def test_a_timeout_produces_submission_unknown_with_a_single_post(db, client):
    _fund(db)
    broker = _broker(submit="submission_unknown")
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == fci_service.STATE_SUBMISSION_UNKNOWN
    assert body["requires_reconciliation"] is True
    assert broker.submit_fund_request.call_count == 1


def test_a_human_retry_against_submission_unknown_never_resends(db, client):
    """A duplicate subscription is real money. There is no path back."""
    _fund(db)
    broker = _broker(submit="submission_unknown")
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            _submit(client, operation_id, preview)
            again = _submit(client, operation_id, preview)

    assert again.status_code == 409
    assert broker.submit_fund_request.call_count == 1


# ───────────────────────────────────────────────────────────────────
# 5 — the global lock
# ───────────────────────────────────────────────────────────────────


def test_the_global_lock_blocks_validation_preview_and_submission(db, client):
    """ORDER_EXECUTION_ENABLED=false stops every FCI path — this is the
    PRODUCTION configuration."""
    _fund(db)
    broker = _broker()

    with exec_settings(**_live(order_execution_enabled=False)):
        created = _prepare(client)
        assert created.status_code == 200, "preparing is harmless and stays allowed"
        operation_id = created.json()["fund_operation_id"]

        with _with_broker(broker):
            validate = client.post(
                f"/api/funds/operations/{operation_id}/validate", headers=EXEC_HEADERS
            )
        preview = client.get(f"/api/funds/operations/{operation_id}/preview").json()
        with _with_broker(broker):
            submit = _submit(client, operation_id, {
                "preview_hash": "x",
                "generated_at": datetime.now(timezone.utc).isoformat(),
            })

    assert validate.status_code == 423
    assert "execution_locked" in preview["blocking_reasons"]
    assert preview["can_submit"] is False
    assert submit.status_code == 423
    broker.submit_fund_request.assert_not_called()
    assert db.query(FundOperationDecision).count() == 0


@pytest.mark.parametrize("operation,flag,code", [
    ("subscribe", "fci_subscription_enabled", "fci_subscription_disabled"),
    ("redeem", "fci_redemption_enabled", "fci_redemption_disabled"),
])
def test_each_capability_flag_gates_its_own_operation(db, client, operation, flag, code):
    """The capability flag stops the operation at VALIDATION, not at submit.

    `soloValidar=true` creates nothing, but it is still an authenticated call
    to the broker made in preparation for an operation we are not allowed to
    perform. With the capability off there is nothing to pre-check, so no
    request is made at all — and the operation's validation state is left
    exactly as it was.
    """
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(**{flag: False})):
        created = _prepare(client, operation=operation, amount=10_000.0)
        operation_id = created.json()["fund_operation_id"]
        with _with_broker(broker):
            validated = client.post(
                f"/api/funds/operations/{operation_id}/validate", headers=EXEC_HEADERS
            )

    assert validated.status_code == 423
    assert code in validated.text
    broker.submit_fund_request.assert_not_called()

    record = db.get(FundOperation, operation_id)
    db.refresh(record)
    assert record.status == fci_service.STATE_PREPARED
    assert record.validated_at is None
    assert record.validated_payload_hash == ""
    assert db.query(FundOperationDecision).count() == 0


@pytest.mark.parametrize("operation,flag", [
    ("subscribe", "fci_subscription_enabled"),
    ("redeem", "fci_redemption_enabled"),
])
def test_turning_a_capability_off_after_validating_still_blocks_the_submit(
    db, client, operation, flag
):
    """Validating while allowed does not buy a right to submit later."""
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker, operation=operation)
    broker.submit_fund_request.reset_mock()
    with exec_settings(**_live(**{flag: False})):
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 423
    broker.submit_fund_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# 6 — state, reconciliation
# ───────────────────────────────────────────────────────────────────


def test_the_state_endpoint_exposes_audit_without_secrets(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        operation_id, _preview = _full_flow(client, db, broker)
        response = client.get(f"/api/funds/operations/{operation_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == fci_service.STATE_VALIDATED
    assert body["immediate"] is False
    assert "validation_state" in body
    serialized = response.text
    for secret in (TEST_ADMIN_KEY, TEST_PREVIEW_SECRET):
        assert secret not in serialized


@pytest.mark.parametrize("outcome,expected", [
    ("confirmed", fci_service.STATE_CONFIRMED),
    ("rejected", fci_service.STATE_REJECTED),
    ("cancelled", fci_service.STATE_CANCELLED),
    ("reconciliation_required", fci_service.STATE_RECONCILIATION_REQUIRED),
])
def test_reconciliation_records_the_outcome_and_never_resends(db, client, outcome, expected):
    _fund(db)
    broker = _broker(submit="submission_unknown")
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        with _with_broker(broker):
            _submit(client, operation_id, preview)
        broker.submit_fund_request.reset_mock()

        response = client.post(
            f"/api/funds/operations/{operation_id}/reconcile",
            json={"outcome": outcome, "note": "verificado en el panel de IOL",
                  "broker_operation_id": "IOL-777"},
            headers=EXEC_HEADERS,
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["new_status"] == expected
    assert body["resent"] is False
    broker.submit_fund_request.assert_not_called()


def test_reconciliation_requires_evidence_and_the_credential(db, client):
    _fund(db)
    broker = _broker(submit="submission_unknown")
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        with _with_broker(broker):
            _submit(client, operation_id, preview)

        no_note = client.post(
            f"/api/funds/operations/{operation_id}/reconcile",
            json={"outcome": "confirmed", "note": ""}, headers=EXEC_HEADERS,
        )
        wrong_key = client.post(
            f"/api/funds/operations/{operation_id}/reconcile",
            json={"outcome": "confirmed", "note": "x"},
            headers={"X-Execution-Key": "wrong"},
        )
        bad_outcome = client.post(
            f"/api/funds/operations/{operation_id}/reconcile",
            json={"outcome": "resend", "note": "x"}, headers=EXEC_HEADERS,
        )

    assert no_note.status_code == 422
    assert wrong_key.status_code == 403
    assert bad_outcome.status_code == 422


def test_there_is_no_resend_outcome_at_all():
    assert "resend" not in fci_service.RECONCILIATION_TARGETS
    assert "retry" not in fci_service.RECONCILIATION_TARGETS
    assert "executed" not in fci_service.FUND_OPERATION_STATES


# ───────────────────────────────────────────────────────────────────
# 7 — live preflight
# ───────────────────────────────────────────────────────────────────


def test_a_subscription_beyond_the_live_balance_is_refused(db, client):
    _fund(db)
    broker = _broker()
    broker.get_live_cash.return_value = {
        "available": True, "cash": 10.0, "currency": "ARS",
        "committed": 0.0, "source": "estadocuenta",
    }
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "insufficient_live_cash" in response.text
    broker.submit_fund_request.assert_not_called()


def test_an_unreadable_live_balance_refuses_the_subscription(db, client):
    _fund(db)
    broker = _broker()
    broker.get_live_cash.return_value = {
        "available": False, "cash": None, "currency": "ARS", "source": "estadocuenta",
    }
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "live_cash_unavailable" in response.text
    broker.submit_fund_request.assert_not_called()


def test_a_redemption_beyond_the_live_holding_is_refused(db, client):
    _fund(db)
    broker = _broker()
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000.0, "cash": 0.0,
        "positions": [{"symbol": "FCIAR", "currency": "ARS", "market_value": 500.0}],
    }
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker, operation="redeem")
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "live_fund_position_insufficient" in response.text
    broker.submit_fund_request.assert_not_called()


def test_a_redemption_without_a_valuation_is_refused(db, client):
    """Without a trustworthy value we cannot tell whether the amount exceeds
    the holding, and guessing would over-redeem."""
    _fund(db)
    broker = _broker()
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000.0, "cash": 0.0,
        "positions": [{"symbol": "FCIAR", "currency": "ARS", "market_value": None}],
    }
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker, operation="redeem")
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "live_fund_position_unavailable" in response.text
    broker.submit_fund_request.assert_not_called()


def test_a_redemption_of_a_fund_we_do_not_hold_is_refused(db, client):
    _fund(db)
    broker = _broker()
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 0.0, "cash": 0.0, "positions": [],
    }
    with exec_settings(**_live()):
        operation_id, preview = _full_flow(client, db, broker, operation="redeem")
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "live_fund_position_missing" in response.text


def test_the_per_operation_cap_blocks_an_oversized_amount(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(fci_max_operation_amount=1_000.0)):
        operation_id, preview = _full_flow(client, db, broker, amount=50_000.0)

    assert preview["can_submit"] is False
    assert "fci_operation_limit_exceeded" in preview["blocking_reasons"]


def test_unconfigured_fci_limits_block(db, client):
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(fci_max_operation_amount=0.0)):
        _operation_id, preview = _full_flow(client, db, broker)

    assert preview["can_submit"] is False
    assert "fci_limits_not_configured" in preview["blocking_reasons"]


def test_the_daily_cap_is_per_currency_and_blocks_a_later_operation(db, client):
    from app.services.execution_limits import reserve_daily_budget, trade_date_for

    _fund(db)
    broker = _broker()
    with exec_settings(**_live(fci_max_daily_amount=15_000.0)):
        # Consume most of today's FCI subscription budget in ARS.
        reserve_daily_budget(
            db, trade_date=trade_date_for(get_settings()),
            execution_class=fci_service.LEDGER_CLASS_SUBSCRIBE, currency="ARS",
            notional=__import__("decimal").Decimal("14000"),
            max_daily_notional=15_000.0,
        )
        db.commit()

        operation_id, preview = _full_flow(client, db, broker, amount=10_000.0)
        broker.submit_fund_request.reset_mock()
        with _with_broker(broker):
            response = _submit(client, operation_id, preview)

    assert response.status_code == 409
    assert "daily_limit_exceeded" in response.text
    broker.submit_fund_request.assert_not_called()
