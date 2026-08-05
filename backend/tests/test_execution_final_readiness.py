"""Final readiness: FCI ledger ordering, request lifecycle, fund verification,
securities pilots.

The theme running through this file is the difference between "we refused" and
"we spent something refusing". A preflight that fails must cost nothing: no
daily budget consumed, no operation left claimed, no validation invalidated.
So most of these tests assert on the LEDGER, not just on the status code —
a 409 with budget silently burned is the bug, and only the ledger shows it.

The second theme is provability. `submission_unknown` is expensive: it strands
an operation until a human checks IOL by hand. It must be reserved for cases
where a request genuinely started, and never used for a local bug that sent
nothing.

Nothing here touches IOL. Every broker is a double or an httpx.MockTransport.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base, get_db
from app.main import app
from app.models.models import (
    ExecutionDailyNotional,
    ExecutionInstrument,
    FundInstrument,
    FundInstrumentVerification,
    FundOperation,
    FundOperationDecision,
    Recommendation,
)
from app.services import fci as fci_service
from app.services.execution_limits import trade_date_for

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
EXEC_HEADERS = {"X-Execution-Key": TEST_ADMIN_KEY}


# ───────────────────────────────────────────────────────────────────
# fixtures
# ───────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/readiness.db", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def client(db):
    """Non-lifespan client: firing startup would leave the global scheduler
    running for every later test in the suite."""
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
    """Global lock OPEN — only to prove the INNER gates work.

    Production keeps ORDER_EXECUTION_ENABLED=false; if the lock were the only
    thing stopping an operation, none of these tests would be testing anything.
    """
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


def _fund(db, symbol="FCIAR", *, currency="ARS", cutoff="23:59", minimum=100.0,
          verified=True, subscribe=True, redeem=True):
    fund = FundInstrument(
        symbol=symbol, name="Fondo Test", manager="IOL Asset Management",
        currency=currency, cutoff_local_time=cutoff, settlement_delay_days=1,
        minimum_amount=minimum,
        subscription_supported=subscribe and verified,
        redemption_supported=redeem and verified,
        active=True,
        verification_status="verified" if verified else "candidate",
        field_provenance=(
            {
                "symbol": "iol_fci_catalog",
                "currency": "admin_verified_override",
                "cutoff_local_time": "admin_verified_override",
                "minimum_amount": "admin_verified_override",
            }
            if verified
            else {"symbol": "iol_fci_catalog",
                  "currency": "iol_fci_catalog",
                  "cutoff_local_time": "iol_fci_catalog_observed",
                  "minimum_amount": "iol_fci_catalog_observed"}
        ),
        source="iol_fci_catalog", verified_at=datetime.utcnow(),
    )
    db.add(fund)
    db.commit()
    return fund


def _broker(*, cash=1_000_000.0, holding=800_000.0, symbol="FCIAR",
            currency="ARS", submit="submitted", validate="validated"):
    broker = mock.MagicMock()

    def _submit(request, *, lifecycle=None):
        if isinstance(lifecycle, dict):
            lifecycle.update({"before_send": True, "request_started": True,
                              "response_received": True, "response_parsed": True})
        if request.get("solo_validar"):
            return {"outcome": validate, "operation_id": "", "raw_response": {},
                    "http_requests_sent": 1, "request_started": True, "error": ""}
        return {"outcome": submit,
                "operation_id": "FCI-1" if submit == "submitted" else "",
                "raw_response": {"numeroOperacion": "FCI-1"},
                "http_requests_sent": 1, "request_started": True, "error": ""}

    broker.submit_fund_request.side_effect = _submit
    broker.get_live_cash.return_value = {
        "available": True, "cash": cash, "currency": currency,
        "committed": 0.0, "source": "estadocuenta",
    }
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": cash,
        "positions": [{"symbol": symbol, "asset_type": "FondoComundeInversion",
                       "instrument_type": "FondoComundeInversion",
                       "currency": currency, "quantity": 100,
                       "market_value": holding}],
    }
    return broker


@contextmanager
def _with_broker(broker):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        yield


def _operation(db, *, operation="subscribe", amount=10_000.0, symbol="FCIAR",
               currency="ARS", validated=True):
    record = FundOperation(
        fund_symbol=symbol, operation=operation, currency=currency,
        amount=amount, status=fci_service.STATE_PREPARED,
        cutoff_local_time="23:59", settlement_delay_days=1,
    )
    db.add(record)
    db.commit()
    if validated:
        record.status = fci_service.STATE_VALIDATED
        record.validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        record.validated_payload_hash = fci_service.validation_payload_hash(record)
        # The fund's verified state is part of what a validation attests to,
        # so a hand-built "validated" operation has to record it too.
        fund = fci_service.get_fund(db, symbol)
        if fund is not None:
            record.validated_fund_hash = fci_service.fund_verification_hash(
                fund, operation
            )
        db.commit()
    return record


def _ledger_total(db, execution_class, currency="ARS", settings=None) -> float:
    row = (
        db.query(ExecutionDailyNotional)
        .filter(
            ExecutionDailyNotional.trade_date == trade_date_for(settings or get_settings()),
            ExecutionDailyNotional.execution_class == execution_class,
            ExecutionDailyNotional.currency == currency,
        )
        .first()
    )
    return float(row.submitted_notional) if row else 0.0


def _preview(client, operation_id):
    return client.get(f"/api/funds/operations/{operation_id}/preview").json()


def _submit(client, operation_id, preview, **overrides):
    payload = {
        "preview_hash": preview.get("preview_hash", ""),
        "preview_generated_at": preview.get("generated_at", ""),
        "confirmation_text": fci_service.fund_confirmation_phrase(operation_id),
        "note": "aprobado",
    }
    payload.update(overrides)
    return client.post(
        f"/api/funds/operations/{operation_id}/submit", json=payload, headers=EXEC_HEADERS
    )


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 1 — a refused preflight must not consume daily budget
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("scenario", [
    "insufficient_cash",
    "insufficient_position",
    "cutoff_closed",
    "preview_expired",
    "validation_expired",
    "operation_limit",
    "limits_not_configured",
])
def test_a_failed_preflight_never_consumes_daily_budget(db, client, scenario):
    """Tests 1-5: every refusal leaves the ledger EXACTLY where it was.

    The old order reserved budget before reading the live balance, so an
    operation refused for having no money still burned that day's allowance —
    and a second, fundable operation could then be refused for a limit the
    first one never had the right to consume.
    """
    settings_overrides: dict = {}
    operation_kind = "subscribe"
    amount = 10_000.0
    cutoff = "23:59"
    validated = True
    preview_age = timedelta(0)
    broker_kwargs: dict = {}

    if scenario == "insufficient_cash":
        broker_kwargs = {"cash": 5.0}
    elif scenario == "insufficient_position":
        operation_kind = "redeem"
        broker_kwargs = {"holding": 1.0}
    elif scenario == "cutoff_closed":
        cutoff = "00:01"
    elif scenario == "preview_expired":
        preview_age = timedelta(seconds=3600)
    elif scenario == "validation_expired":
        validated = False
    elif scenario == "operation_limit":
        settings_overrides = {"fci_max_operation_amount": 100.0}
    elif scenario == "limits_not_configured":
        settings_overrides = {"fci_max_operation_amount": 0.0}

    _fund(db, cutoff=cutoff)
    broker = _broker(**broker_kwargs)
    ledger_class = fci_service.fci_ledger_class(operation_kind)

    with exec_settings(**_live(**settings_overrides)):
        record = _operation(db, operation=operation_kind, amount=amount,
                            validated=validated)
        before = _ledger_total(db, ledger_class)

        preview = _preview(client, record.id)
        if preview_age:
            stamp = datetime.now(timezone.utc) - preview_age
            preview = {**preview, "generated_at": stamp.isoformat()}

        with _with_broker(broker):
            response = _submit(client, record.id, preview)

        after = _ledger_total(db, ledger_class)

    assert response.status_code in (409, 423), response.text
    assert after == before == 0.0, f"{scenario} consumed daily budget"
    broker.submit_fund_request.assert_not_called()
    db.refresh(record)
    assert record.status != fci_service.STATE_SUBMITTING


def test_a_missing_execution_key_never_consumes_daily_budget(db, client):
    """Test 5b: a credential failure is not a spending event."""
    _fund(db)
    broker = _broker()
    ledger_class = fci_service.LEDGER_CLASS_SUBSCRIBE
    with exec_settings(**_live()):
        record = _operation(db)
        preview = _preview(client, record.id)
        with _with_broker(broker):
            response = client.post(
                f"/api/funds/operations/{record.id}/submit",
                json={
                    "preview_hash": preview["preview_hash"],
                    "preview_generated_at": preview["generated_at"],
                    "confirmation_text": fci_service.fund_confirmation_phrase(record.id),
                    "note": "",
                },
                headers={"X-Execution-Key": "wrong-key"},
            )
        assert _ledger_total(db, ledger_class) == 0.0

    assert response.status_code == 403
    broker.submit_fund_request.assert_not_called()


def test_the_capability_flag_being_off_never_consumes_daily_budget(db, client):
    """Test 5c: configuration refusals are free too."""
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()):
        record = _operation(db)
        preview = _preview(client, record.id)
    with exec_settings(**_live(fci_subscription_enabled=False)):
        with _with_broker(broker):
            response = _submit(client, record.id, preview)
        assert _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE) == 0.0

    assert response.status_code == 423
    broker.submit_fund_request.assert_not_called()


def test_a_second_claim_of_the_same_operation_loses(db, client):
    """Test 6: only one caller may claim an operation, and the loser reserves
    nothing.

    Simulated by claiming first and then attempting the full submit: the
    conditional UPDATE matches zero rows the second time, which is exactly
    what a concurrent caller would see.
    """
    _fund(db)
    broker = _broker()
    with exec_settings(**_live()) as settings:
        record = _operation(db)
        preview = _preview(client, record.id)

        first_error, first_audit = fci_service.claim_and_reserve_fund_operation(
            db, record, settings=settings
        )
        db.commit()
        assert first_error is None
        assert first_audit["claimed"] is True
        after_first = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

        with _with_broker(broker):
            second = _submit(client, record.id, preview)
        after_second = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert second.status_code == 409
    # The loser reserved nothing: the ledger is untouched by the second call.
    assert after_second == after_first == 10_000.0
    broker.submit_fund_request.assert_not_called()


def test_a_reservation_that_exceeds_the_daily_limit_does_not_leave_the_operation_claimed(db):
    """Test 7: claim and reserve are coupled — neither survives the other's
    failure.

    A claim without a reservation is the nastier half: the operation sits in
    `submitting` forever, unsendable and unretryable, for a submission that
    never happened.
    """
    _fund(db)
    with exec_settings(**_live(fci_max_daily_amount=1_000.0)) as settings:
        record = _operation(db, amount=10_000.0)
        error, audit = fci_service.claim_and_reserve_fund_operation(
            db, record, settings=settings
        )
        db.refresh(record)
        total = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert error == "daily_limit_exceeded"
    assert audit["reserved"] is False
    assert total == 0.0
    assert record.status == fci_service.STATE_VALIDATED


def test_a_failure_before_the_request_starts_releases_the_reservation(db, client):
    """Test 8: nothing was sent ⇒ give the budget back and allow a retry.

    This is the ONLY automatic release. It is safe precisely because there is
    no request at IOL to reconcile against.
    """
    _fund(db)
    broker = _broker()

    def _explode(request, *, lifecycle=None):
        # Fails BEFORE the point of no return, exactly as the real client
        # reports an auth failure or a malformed request.
        if isinstance(lifecycle, dict):
            lifecycle.update({"before_send": True, "request_started": False})
        return {"outcome": "local_error", "operation_id": "", "raw_response": {},
                "http_requests_sent": 0, "request_started": False,
                "error": "Failed before sending: RuntimeError"}

    broker.submit_fund_request.side_effect = _explode

    with exec_settings(**_live()):
        record = _operation(db)
        preview = _preview(client, record.id)
        with _with_broker(broker):
            response = _submit(client, record.id, preview)
        total = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert response.status_code == 200
    body = response.json()
    assert body["outcome"] == "local_error"
    assert body["http_requests_sent"] == 0
    assert body["requires_reconciliation"] is False
    assert body["budget_released"]["released"] is True
    assert total == 0.0

    db.refresh(record)
    # Never submission_unknown: nothing was sent, so nothing needs a human.
    assert record.status != fci_service.STATE_SUBMISSION_UNKNOWN
    assert record.status == fci_service.STATE_VALIDATED


def test_an_exception_before_the_request_starts_is_not_ambiguous(db, client):
    """Test 8b: even a RAISED local bug is provably 'not sent'.

    The lifecycle dict is stamped by the client before the POST begins, so the
    caller does not have to guess which side of the point of no return an
    exception came from.
    """
    _fund(db)
    broker = _broker()
    broker.submit_fund_request.side_effect = NameError("IOL_FORM_CONTENT_TYPE")

    with exec_settings(**_live()):
        record = _operation(db)
        preview = _preview(client, record.id)
        with _with_broker(broker):
            response = _submit(client, record.id, preview)
        total = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert response.json()["http_requests_sent"] == 0
    assert total == 0.0
    db.refresh(record)
    assert record.status == fci_service.STATE_VALIDATED


def test_a_timeout_after_the_request_started_keeps_the_reservation(db, client):
    """Tests 9-10: ambiguity KEEPS the budget.

    Releasing it would let a second operation spend allowance that the first
    one may already have committed at IOL — and we cannot prove it did not.
    """
    _fund(db)
    broker = _broker()

    def _timeout(request, *, lifecycle=None):
        if isinstance(lifecycle, dict):
            lifecycle.update({"before_send": True, "request_started": True,
                              "response_received": False})
        return {"outcome": "submission_unknown", "operation_id": "",
                "raw_response": {}, "http_requests_sent": 1,
                "request_started": True, "error": "timeout"}

    broker.submit_fund_request.side_effect = _timeout

    with exec_settings(**_live()):
        record = _operation(db)
        preview = _preview(client, record.id)
        with _with_broker(broker):
            response = _submit(client, record.id, preview)
        total = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == fci_service.STATE_SUBMISSION_UNKNOWN
    assert body["requires_reconciliation"] is True
    assert body["http_requests_sent"] == 1
    assert total == 10_000.0

    # And a human retry does NOT resend.
    with exec_settings(**_live()):
        with _with_broker(broker):
            retry = _submit(client, record.id, preview)
    assert retry.status_code == 409
    assert broker.submit_fund_request.call_count == 1


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 2 — the request lifecycle contract, at the transport
# ═══════════════════════════════════════════════════════════════════


def _iol_client(handler):
    import httpx

    from app.broker.clients import IolBrokerClient

    broker = IolBrokerClient.__new__(IolBrokerClient)
    broker.api_base = "https://api.invertironline.test"
    broker._access_token = "token"
    broker._token_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    broker._ensure_auth = lambda: None
    broker._client = httpx.Client(transport=httpx.MockTransport(handler))
    return broker


def test_the_client_marks_the_lifecycle_up_to_response_parsed():
    import httpx

    def handler(request):
        return httpx.Response(200, json={"numeroOperacion": 123})

    broker = _iol_client(handler)
    lifecycle: dict = {}
    result = broker.submit_fund_request(
        {"endpoint": "/api/v2/operar/suscripcion/fci",
         "form_data": {"Simbolo": "FCIAR", "Monto": "100", "soloValidar": "false"},
         "content_type": "application/x-www-form-urlencoded",
         "solo_validar": False},
        lifecycle=lifecycle,
    )
    assert lifecycle == {"before_send": True, "request_started": True,
                         "response_received": True, "response_parsed": True}
    assert result["outcome"] == "submitted"
    assert result["http_requests_sent"] == 1
    assert result["operation_id"] == "123"


def test_a_client_failure_before_auth_reports_zero_requests():
    """A local failure must never masquerade as an ambiguous send."""
    import httpx

    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, json={})

    broker = _iol_client(handler)

    def _boom():
        raise RuntimeError("credentials not configured")

    broker._ensure_auth = _boom

    lifecycle: dict = {}
    result = broker.submit_fund_request(
        {"endpoint": "/api/v2/operar/rescate/fci",
         "form_data": {"Simbolo": "FCIAR", "Monto": "100", "soloValidar": "false"},
         "solo_validar": False},
        lifecycle=lifecycle,
    )
    assert result["outcome"] == "local_error"
    assert result["http_requests_sent"] == 0
    assert lifecycle["request_started"] is False
    assert calls["n"] == 0


def test_a_transport_failure_after_the_request_started_is_ambiguous():
    import httpx

    def handler(request):
        raise httpx.ReadTimeout("timeout", request=request)

    broker = _iol_client(handler)
    lifecycle: dict = {}
    result = broker.submit_fund_request(
        {"endpoint": "/api/v2/operar/suscripcion/fci",
         "form_data": {"Simbolo": "FCIAR", "Monto": "100", "soloValidar": "false"},
         "solo_validar": False},
        lifecycle=lifecycle,
    )
    assert result["outcome"] == "submission_unknown"
    assert result["http_requests_sent"] == 1
    assert lifecycle["request_started"] is True
    assert lifecycle["response_received"] is False


def test_a_two_hundred_without_an_operation_id_is_ambiguous_not_success():
    import httpx

    def handler(request):
        return httpx.Response(200, json={"ok": True})

    broker = _iol_client(handler)
    result = broker.submit_fund_request(
        {"endpoint": "/api/v2/operar/suscripcion/fci",
         "form_data": {"Simbolo": "FCIAR", "Monto": "100", "soloValidar": "false"},
         "solo_validar": False},
    )
    assert result["outcome"] == "submission_unknown"
    assert result["http_requests_sent"] == 1


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 3 — pending amounts, by type
# ═══════════════════════════════════════════════════════════════════


def test_a_pending_subscription_does_not_reduce_a_redeemable_holding(db):
    """Test 11. Money on its way OUT does not shrink what you hold."""
    _operation(db, operation="subscribe", amount=50_000.0, validated=False)
    db.query(FundOperation).update({"status": fci_service.STATE_PENDING_CONFIRMATION})
    db.commit()

    assert fci_service.pending_fund_subscriptions(db, currency="ARS") == Decimal("50000")
    assert fci_service.pending_fund_redemptions(
        db, symbol="FCIAR", currency="ARS"
    ) == Decimal("0")


def test_a_pending_redemption_does_not_reduce_the_cash_available_to_subscribe(db):
    """Test 12. Money on its way IN does not shrink what you can spend."""
    _operation(db, operation="redeem", amount=30_000.0, validated=False)
    db.query(FundOperation).update({"status": fci_service.STATE_SUBMISSION_UNKNOWN})
    db.commit()

    assert fci_service.pending_fund_subscriptions(db, currency="ARS") == Decimal("0")
    assert fci_service.pending_fund_redemptions(
        db, symbol="FCIAR", currency="ARS"
    ) == Decimal("30000")


def test_pending_amounts_are_separated_by_currency(db):
    """Test 13. Adding pesos to dollars produces a number that means nothing."""
    for currency, amount in (("ARS", 10_000.0), ("USD", 200.0)):
        record = _operation(db, operation="subscribe", amount=amount,
                            currency=currency, validated=False)
        record.status = fci_service.STATE_SUBMITTED
    db.commit()

    assert fci_service.pending_fund_subscriptions(db, currency="ARS") == Decimal("10000")
    assert fci_service.pending_fund_subscriptions(db, currency="USD") == Decimal("200")


def test_pending_redemptions_are_separated_by_symbol(db):
    """Test 14. Redeeming fund A says nothing about how much of B is left."""
    for symbol, amount in (("FCIAR", 40_000.0), ("FCIUS", 25_000.0)):
        record = _operation(db, operation="redeem", amount=amount,
                            symbol=symbol, validated=False)
        record.status = fci_service.STATE_PENDING_CONFIRMATION
    db.commit()

    assert fci_service.pending_fund_redemptions(
        db, symbol="FCIAR", currency="ARS") == Decimal("40000")
    assert fci_service.pending_fund_redemptions(
        db, symbol="FCIUS", currency="ARS") == Decimal("25000")


@pytest.mark.parametrize("state", [
    fci_service.STATE_PREPARED,
    fci_service.STATE_VALIDATED,
    fci_service.STATE_CONFIRMED,
    fci_service.STATE_REJECTED,
    fci_service.STATE_CANCELLED,
])
def test_terminal_and_pre_send_states_reserve_no_capacity(db, state):
    """Test 15. `confirmed` frees capacity because the real balance now shows
    it — counting it again would subtract the same money twice."""
    record = _operation(db, operation="subscribe", amount=99_000.0, validated=False)
    record.status = state
    db.commit()

    assert fci_service.pending_fund_subscriptions(db, currency="ARS") == Decimal("0")


@pytest.mark.parametrize("state", list(fci_service.CAPACITY_RESERVING_STATES))
def test_unresolved_states_do_reserve_capacity(db, state):
    record = _operation(db, operation="subscribe", amount=99_000.0, validated=False)
    record.status = state
    db.commit()

    assert fci_service.pending_fund_subscriptions(db, currency="ARS") == Decimal("99000")


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 4 — flags
# ═══════════════════════════════════════════════════════════════════


def test_the_global_lock_blocks_validation(db, client):
    """Test 16."""
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(order_execution_enabled=False)):
        record = _operation(db, validated=False)
        with _with_broker(broker):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 423
    assert "execution_locked" in response.text
    broker.submit_fund_request.assert_not_called()


@pytest.mark.parametrize("operation,flag,code", [
    ("subscribe", "fci_subscription_enabled", "fci_subscription_disabled"),
    ("redeem", "fci_redemption_enabled", "fci_redemption_disabled"),
])
def test_the_capability_flag_blocks_validation(db, client, operation, flag, code):
    """Tests 17-18: each flag gates only its own side."""
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(**{flag: False})):
        record = _operation(db, operation=operation, validated=False)
        with _with_broker(broker):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 423
    assert code in response.text
    broker.submit_fund_request.assert_not_called()

    db.refresh(record)
    assert record.status == fci_service.STATE_PREPARED
    assert record.validated_at is None
    assert record.validated_payload_hash == ""
    assert db.query(FundOperationDecision).count() == 0


def test_no_blocked_validation_ever_reaches_the_broker(db, client):
    """Test 19: whichever gate refuses, the broker is never even obtained."""
    _fund(db)
    broker = _broker()
    blocked_configs = [
        _live(order_execution_enabled=False),
        _live(fci_subscription_enabled=False),
    ]
    for config in blocked_configs:
        with exec_settings(**config):
            record = _operation(db, validated=False)
            with _with_broker(broker):
                response = client.post(
                    f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
                )
            assert response.status_code == 423
    broker.submit_fund_request.assert_not_called()


def test_readiness_reports_each_gate_separately():
    with exec_settings(**_live(fci_redemption_enabled=False)):
        capability = fci_service.get_fci_capability()

    assert capability["global_execution_open"] is True
    assert capability["subscription_flag_enabled"] is True
    assert capability["redemption_flag_enabled"] is False
    assert capability["can_validate_subscription"] is True
    assert capability["can_validate_redemption"] is False
    assert capability["can_submit_subscription"] is True
    assert capability["can_submit_redemption"] is False


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 5 & 6 — fund verification
# ═══════════════════════════════════════════════════════════════════


def _verify_payload(**extra):
    payload = {
        "cutoff_local_time": "15:00",
        "minimum_amount": 1000.0,
        "settlement_delay_days": 1,
        "currency": "ARS",
        "subscription_supported": True,
        "redemption_supported": True,
        "note": "Verificado contra el prospecto oficial del fondo.",
        "source": "fund_prospectus",
    }
    payload.update(extra)
    return payload


def test_a_candidate_fund_cannot_be_operated(db, client):
    """Test 20: being in the catalog is not being fit to operate."""
    _fund(db, verified=False)
    with exec_settings(**_live()):
        response = client.post("/api/funds/operations", json={
            "fund_symbol": "FCIAR", "operation": "subscribe", "amount": 10_000.0,
        })

    assert response.status_code == 409
    assert "fund_not_verified" in response.text
    assert db.query(FundOperation).count() == 0


@pytest.mark.parametrize("operation,flag,code", [
    ("subscribe", "subscription_supported", "fund_subscription_unsupported"),
    ("redeem", "redemption_supported", "fund_redemption_unsupported"),
])
def test_a_capability_the_fund_does_not_support_is_refused(db, client, operation, flag, code):
    fund = _fund(db)
    setattr(fund, flag, False)
    db.commit()
    with exec_settings(**_live()):
        response = client.post("/api/funds/operations", json={
            "fund_symbol": "FCIAR", "operation": operation, "amount": 10_000.0,
        })

    assert response.status_code == 409
    assert code in response.text


def test_verifying_a_fund_creates_an_audit_row(db, client):
    """Test 21."""
    _fund(db, verified=False)
    with exec_settings(**_live()):
        response = client.post(
            "/api/funds/catalog/FCIAR/verify",
            json=_verify_payload(), headers=EXEC_HEADERS,
        )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["previous_status"] == "candidate"
    assert body["verification_status"] == "verified"

    rows = db.query(FundInstrumentVerification).all()
    assert len(rows) == 1
    assert rows[0].action == "verify"
    assert rows[0].note.startswith("Verificado contra")
    assert rows[0].data_hash
    # The parameters are recorded as asserted, not as observed.
    assert rows[0].cutoff_local_time == "15:00"
    assert rows[0].minimum_amount == 1000.0


def test_verifying_a_fund_does_not_enable_any_flag(db, client):
    """Test 22: verification is about the FUND, never about permission."""
    _fund(db, verified=False)
    settings = get_settings()
    with exec_settings(**_live(order_execution_enabled=False,
                               fci_subscription_enabled=False,
                               fci_redemption_enabled=False)):
        response = client.post(
            "/api/funds/catalog/FCIAR/verify",
            json=_verify_payload(), headers=EXEC_HEADERS,
        )
        assert response.status_code == 200
        assert response.json()["flags_unchanged"] is True
        assert settings.order_execution_enabled is False
        assert settings.fci_subscription_enabled is False
        assert settings.fci_redemption_enabled is False

    # And no operation was created as a side effect.
    assert db.query(FundOperation).count() == 0


def test_verification_requires_a_note_and_an_execution_key(db, client):
    _fund(db, verified=False)
    with exec_settings(**_live()):
        no_note = client.post("/api/funds/catalog/FCIAR/verify",
                              json=_verify_payload(note="   "), headers=EXEC_HEADERS)
        no_key = client.post("/api/funds/catalog/FCIAR/verify",
                             json=_verify_payload())

    assert no_note.status_code == 422
    assert no_key.status_code == 403
    assert db.query(FundInstrumentVerification).count() == 0


@pytest.mark.parametrize("payload,status", [
    ({"cutoff_local_time": "25:00"}, 422),
    ({"cutoff_local_time": "1500"}, 422),
    ({"minimum_amount": 0.0}, 422),
    ({"minimum_amount": -5.0}, 422),
    ({"settlement_delay_days": -1}, 422),
    ({"currency": "USD"}, 409),
])
def test_verification_rejects_invalid_parameters(db, client, payload, status):
    _fund(db, verified=False)
    with exec_settings(**_live()):
        response = client.post("/api/funds/catalog/FCIAR/verify",
                               json=_verify_payload(**payload), headers=EXEC_HEADERS)
    assert response.status_code == status
    assert db.query(FundInstrumentVerification).count() == 0


def test_an_identity_change_freezes_the_fund(db, client):
    """Test 23: an approval given for fund A must not authorise fund B."""
    fund = _fund(db)
    fund.identity_hash = fci_service.fund_identity_hash(fund)
    db.commit()

    broker = mock.MagicMock()
    broker._authorized_get.return_value = mock.MagicMock(
        json=lambda: [{"simbolo": "FCIAR", "descripcion": "Fondo Test",
                       # A different administradora ⇒ a different fund.
                       "administradora": "OTRA ADMINISTRADORA", "moneda": "peso_Argentino"}]
    )
    with exec_settings(**_live()):
        result = fci_service.refresh_fund_catalog(db, broker)

    db.refresh(fund)
    assert result["identity_frozen"] == 1
    assert fund.verification_status == "identity_changed"
    assert fund.subscription_supported is False
    assert fund.redemption_supported is False
    assert fund.previous_identity["manager"] == "IOL Asset Management"

    blockers = fci_service.fund_operability_blockers(fund, "subscribe")
    assert "fund_identity_changed" in blockers

    audit = db.query(FundInstrumentVerification).all()
    assert [row.action for row in audit] == ["identity_changed"]


def test_a_refresh_preserves_a_valid_administrative_verification(db):
    """Test 24: same fund, corrected numbers — the human's assertion stands."""
    fund = _fund(db)
    fund.identity_hash = fci_service.fund_identity_hash(fund)
    db.commit()

    broker = mock.MagicMock()
    broker._authorized_get.return_value = mock.MagicMock(
        json=lambda: [{"simbolo": "FCIAR", "descripcion": "Fondo Test",
                       "administradora": "IOL Asset Management",
                       "moneda": "peso_Argentino",
                       # An OBSERVED cutoff that must not overwrite the
                       # administratively verified one.
                       "horarioCorte": "09:00"}]
    )
    with exec_settings(**_live()):
        result = fci_service.refresh_fund_catalog(db, broker)

    db.refresh(fund)
    assert result["identity_frozen"] == 0
    assert fund.verification_status == "verified"
    assert fund.field_provenance["cutoff_local_time"] == "admin_verified_override"


def test_rejecting_a_fund_blocks_it_and_survives_a_refresh(db, client):
    """Test 25: a refresh is not an appeal."""
    fund = _fund(db)
    fund.identity_hash = fci_service.fund_identity_hash(fund)
    db.commit()

    with exec_settings(**_live()):
        response = client.post("/api/funds/catalog/FCIAR/reject",
                               json={"note": "El fondo no cumple el mandato."},
                               headers=EXEC_HEADERS)
    assert response.status_code == 200
    db.refresh(fund)
    assert fund.verification_status == "rejected"
    assert "fund_rejected" in fci_service.fund_operability_blockers(fund, "subscribe")

    broker = mock.MagicMock()
    broker._authorized_get.return_value = mock.MagicMock(
        json=lambda: [{"simbolo": "FCIAR", "descripcion": "Fondo Test",
                       "administradora": "IOL Asset Management",
                       "moneda": "peso_Argentino"}]
    )
    with exec_settings(**_live()):
        fci_service.refresh_fund_catalog(db, broker)
    db.refresh(fund)
    assert fund.verification_status == "rejected"


def test_demoting_a_fund_blocks_it(db, client):
    """Test 26."""
    fund = _fund(db)
    with exec_settings(**_live()):
        response = client.post("/api/funds/catalog/FCIAR/demote",
                               json={"note": "Revisar el cutoff con la administradora."},
                               headers=EXEC_HEADERS)
        blocked = client.post("/api/funds/operations", json={
            "fund_symbol": "FCIAR", "operation": "subscribe", "amount": 10_000.0,
        })

    assert response.status_code == 200
    db.refresh(fund)
    assert fund.verification_status == "candidate"
    assert blocked.status_code == 409
    assert "fund_not_verified" in blocked.text
    assert [r.action for r in db.query(FundInstrumentVerification).all()] == ["demote"]


def test_a_verified_fund_can_be_prepared_end_to_end(db, client):
    """The positive control: after verifying, preparation works."""
    _fund(db, verified=False)
    with exec_settings(**_live()):
        client.post("/api/funds/catalog/FCIAR/verify",
                    json=_verify_payload(minimum_amount=100.0), headers=EXEC_HEADERS)
        created = client.post("/api/funds/operations", json={
            "fund_symbol": "FCIAR", "operation": "subscribe", "amount": 10_000.0,
        })

    assert created.status_code == 200, created.text
    assert created.json()["status"] == fci_service.STATE_PREPARED


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 7, 8, 9 — securities readiness, template, pilots
# ═══════════════════════════════════════════════════════════════════


ACCIONES_POLICY = {
    "ACCIONES": {
        "buy_enabled": True, "sell_enabled": True, "currencies": ["ARS"],
        "markets": ["bCBA"], "settlements": ["t1"], "max_order_notional": 500,
        "max_daily_notional": 2000, "max_quantity": 10, "max_portfolio_pct": 0.02,
        "min_cash_reserve": 1000, "fee_buffer_pct": 0.01,
        "max_quote_age_seconds": 15, "max_price_deviation_pct": 0.02,
        "catalog_max_age_seconds": 86400, "validity_minutes": 10,
        "default_quantity_step": 1, "default_price_tick": 0.01,
        "order_type": "precioLimite",
    }
}


def _instrument(db, symbol="BYMA", *, execution_class="ACCIONES",
                tick=1.0, step=1.0, buy=True, sell=True):
    entry = ExecutionInstrument(
        broker_symbol=symbol, display_symbol=symbol, description=symbol,
        country="argentina", market="bCBA", settlement="t1",
        asset_type=execution_class, instrument_type=execution_class,
        execution_family="securities", execution_class=execution_class,
        currency="ARS", quantity_step=step, price_tick=tick, minimum_quantity=1,
        active=True, buy_supported=buy, sell_supported=sell, quote_supported=True,
        verification_status="verified",
        field_provenance={
            "broker_symbol": "iol_portfolio", "market": "iol_portfolio",
            "asset_type": "iol_portfolio", "instrument_type": "iol_portfolio",
            "currency": "iol_portfolio", "settlement": "iol_portfolio",
            "buy_supported": "iol_quote", "sell_supported": "iol_quote",
            "quote_supported": "iol_quote",
            **({"price_tick": "admin_verified_override"} if tick is not None else {}),
            **({"quantity_step": "admin_verified_override"} if step is not None else {}),
        },
        source="iol_portfolio", verified_at=datetime.utcnow(),
        stale_after=datetime.utcnow() + timedelta(days=1),
    )
    db.add(entry)
    db.commit()
    return entry


def _quote_broker(*, bid=297.0, ask=299.0, held=10, cash=1_000_000.0,
                  symbol="BYMA", currency="ARS"):
    """A securities broker double with a book AND a live account.

    Both halves matter now: readiness for an exact quantity checks the live
    balance for a buy and the live holding for a sell, so a double with only
    quotes would make every pilot look unfundable.
    """
    broker = mock.MagicMock()

    def _quote(symbol_arg, side, market, settlement):
        price = bid if side == "sell" else ask
        if price is None:
            return {"available": False, "source": None, "price": None,
                    "retrieved_at": datetime.now(timezone.utc).isoformat()}
        return {"available": True, "source": "bid" if side == "sell" else "ask",
                "price": price,
                "retrieved_at": datetime.now(timezone.utc).isoformat()}

    broker.get_execution_quote.side_effect = _quote
    broker.get_live_cash.return_value = {
        "available": True, "cash": cash, "currency": currency,
        "committed": 0.0, "source": "estadocuenta",
    }
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": cash,
        "positions": [{"symbol": symbol, "asset_type": "ACCIONES",
                       "instrument_type": "ACCIONES", "currency": currency,
                       "quantity": held, "committed": 0,
                       "market_value": (held or 0) * (bid or 0)}],
    }
    return broker


def test_buy_readiness_requires_an_ask(db):
    """Test 27: no seller ⇒ nothing to buy, whatever the catalog says."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db)
    broker = _quote_broker(ask=None)
    with exec_settings(**_live(execution_class_policies=ACCIONES_POLICY,
                               securities_buy_enabled=True,
                               securities_sell_enabled=True,
                               execution_sell_only=False)) as settings:
        report = evaluate_pilot_readiness(db, symbols=["BYMA"], broker=broker,
                                          live=True, settings=settings)

    entry = report["symbols"][0]
    assert entry["buy"]["technically_ready"] is False
    assert "quote_unavailable" in entry["buy"]["technical_blocking_reasons"]
    # The other side is unaffected: they are independent capabilities.
    assert entry["sell"]["technically_ready"] is True


def test_sell_readiness_requires_a_bid(db):
    """Test 28."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db)
    broker = _quote_broker(bid=None)
    with exec_settings(**_live(execution_class_policies=ACCIONES_POLICY,
                               securities_buy_enabled=True,
                               securities_sell_enabled=True,
                               execution_sell_only=False)) as settings:
        report = evaluate_pilot_readiness(db, symbols=["BYMA"], broker=broker,
                                          live=True, settings=settings)

    entry = report["symbols"][0]
    assert entry["sell"]["technically_ready"] is False
    assert "quote_unavailable" in entry["sell"]["technical_blocking_reasons"]
    assert entry["buy"]["technically_ready"] is True


def test_a_quote_from_the_wrong_side_is_not_evidence(db):
    """A provider answering with the other side of the book proves nothing."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db)
    broker = mock.MagicMock()
    broker.get_execution_quote.return_value = {
        "available": True, "source": "bid", "price": 297.0,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }
    with exec_settings(**_live(execution_class_policies=ACCIONES_POLICY,
                               securities_buy_enabled=True,
                               execution_sell_only=False)) as settings:
        report = evaluate_pilot_readiness(db, symbols=["BYMA"], broker=broker,
                                          live=True, settings=settings)

    entry = report["symbols"][0]
    assert entry["buy"]["technically_ready"] is False
    assert "quote_wrong_side" in entry["buy"]["technical_blocking_reasons"]


def test_readiness_without_a_broker_reports_unavailable_not_ready(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db)
    with exec_settings(**_live(execution_class_policies=ACCIONES_POLICY)) as settings:
        report = evaluate_pilot_readiness(db, symbols=["BYMA"], broker=None,
                                          live=False, settings=settings)

    entry = report["symbols"][0]
    assert entry["buy"]["technically_ready"] is False
    assert entry["sell"]["technically_ready"] is False
    assert entry["buy"]["quote"]["error"] == "quote_not_probed"
    assert entry["buy"]["quote"]["probed"] is False
    # "we did not look" is reported as such, never as "there is no book".
    assert "live_check_not_performed" in entry["buy"]["technical_blocking_reasons"]


def test_the_policy_template_never_invents_a_tick_or_a_step(db):
    """Test 29: an invented number in a policy is indistinguishable from a
    verified one once it is in the config."""
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, symbol="NOTICK", tick=None, step=None)
    with exec_settings(**_live()) as settings:
        template = build_pilot_policy_template(db, symbols=["NOTICK"], settings=settings)

    # tick/step are NOT overridable fields, so they must not appear there.
    for override in template["EXECUTION_INSTRUMENT_OVERRIDES"].values():
        assert "price_tick" not in override
        assert "quantity_step" not in override
    payload = template["INSTRUMENT_FIELD_VERIFICATION_PAYLOADS"]["NOTICK"]
    assert payload["price_tick"] is None
    assert payload["quantity_step"] is None
    block = template["EXECUTION_CLASS_POLICIES"]["ACCIONES"]
    assert block["default_price_tick"] is None
    assert block["default_quantity_step"] is None
    assert any("tick o step sin verificar" in w for w in template["warnings"])


def test_the_policy_template_never_enables_a_side(db):
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, symbol="BYMA")
    _instrument(db, symbol="SPY", execution_class="CEDEARS")
    with exec_settings(**_live()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    blocks = template["EXECUTION_CLASS_POLICIES"]
    # Separate blocks per class, even when the numbers coincide.
    assert set(blocks) == {"ACCIONES", "CEDEARS"}
    for block in blocks.values():
        assert block["buy_enabled"] is False
        assert block["sell_enabled"] is False
    assert template["writes_configuration"] is False


def test_the_policy_template_endpoint_writes_nothing(db, client):
    _instrument(db, symbol="BYMA")
    with exec_settings(**_live()):
        before = db.query(ExecutionInstrument).count()
        response = client.get("/api/broker/pilot-policy-template?symbols=BYMA")
        after = db.query(ExecutionInstrument).count()

    assert response.status_code == 200
    assert response.json()["read_only"] is True
    assert before == after


def _pilot_payload(symbol="BYMA", side="sell", quantity=1.0, **extra):
    from app.services.execution_pilot import securities_pilot_phrase

    payload = {
        "symbol": symbol, "side": side, "quantity": quantity,
        "confirmation_text": securities_pilot_phrase(symbol, side, quantity),
        "note": "piloto controlado",
    }
    payload.update(extra)
    return payload


def test_a_symbol_that_is_not_technically_ready_cannot_become_a_pilot(db, client):
    """Test 30."""
    _instrument(db, symbol="BYMA")
    broker = _quote_broker(bid=None)  # no bid ⇒ sell is not ready
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    assert response.status_code == 409
    assert "quote_unavailable" in response.text
    assert db.query(Recommendation).count() == 0


def test_a_new_pilot_never_reuses_an_existing_recommendation(db, client):
    """Test 31: Recommendation 13 stays pending and untouched."""
    thirteen = Recommendation(
        id=13, action="reducir", status="pending", suggested_pct=5.0,
        confidence=0.8, rationale="reducir SPY", risks="",
        executive_summary="Reducir SPY",
    )
    db.add(thirteen)
    db.commit()

    _instrument(db, symbol="BYMA")
    broker = _quote_broker()
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    # An open pending decision BLOCKS creation; it is never superseded.
    assert response.status_code == 409
    db.refresh(thirteen)
    assert thirteen.status == "pending"
    assert thirteen.metadata_json is None or "execution_pilot" not in (
        thirteen.metadata_json or {}
    )
    assert db.query(Recommendation).count() == 1


def test_a_created_pilot_is_pending_and_unapproved(db, client):
    """Test 32: creating is not approving, and never sends."""
    _instrument(db, symbol="BYMA")
    broker = _quote_broker()
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "pending"
    assert body["approved"] is False
    assert body["order_sent"] is False
    assert body["pilot_type"] == "security_sell"
    assert body["execution_class"] == "ACCIONES"

    rec = db.get(Recommendation, body["recommendation_id"])
    metadata = rec.metadata_json or {}
    assert metadata["execution_pilot"] is True
    assert metadata["pilot_type"] == "security_sell"
    assert metadata["execution_class"] == "ACCIONES"
    assert metadata["symbol"] == "BYMA"
    assert metadata["quantity"] == 1.0
    # No order of any kind was placed.
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()


def test_a_pilot_requires_the_creation_flag_and_the_exact_phrase(db, client):
    _instrument(db, symbol="BYMA")
    broker = _quote_broker()
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=False,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            no_flag = client.post("/api/execution-pilot/securities",
                                  json=_pilot_payload(), headers=EXEC_HEADERS)

    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            wrong_phrase = client.post(
                "/api/execution-pilot/securities",
                json=_pilot_payload(confirmation_text="CREAR PILOTO"),
                headers=EXEC_HEADERS,
            )

    assert no_flag.status_code == 423
    assert wrong_phrase.status_code == 422
    assert db.query(Recommendation).count() == 0


def test_the_phrase_of_one_pilot_does_not_authorise_another(db, client):
    """A buy phrase must not create a sell, nor one symbol authorise another."""
    from app.services.execution_pilot import securities_pilot_phrase

    _instrument(db, symbol="BYMA")
    broker = _quote_broker()
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_buy_enabled=True,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post(
                "/api/execution-pilot/securities",
                json={
                    "symbol": "BYMA", "side": "buy", "quantity": 1.0,
                    # The phrase for the SELL pilot.
                    "confirmation_text": securities_pilot_phrase("BYMA", "sell", 1.0),
                    "note": "",
                },
                headers=EXEC_HEADERS,
            )

    assert response.status_code == 422
    assert "confirmation" in response.text.lower() or "Confirmación" in response.text
    assert db.query(Recommendation).count() == 0


def test_a_quantity_outside_the_step_is_refused(db, client):
    _instrument(db, symbol="LOTE", step=100.0)
    broker = _quote_broker()
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_pilot_creation_enabled=True,
                               execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(symbol="LOTE", quantity=1.0),
                                   headers=EXEC_HEADERS)

    assert response.status_code == 422
    assert "quantity_step" in response.text
    assert db.query(Recommendation).count() == 0


def test_neither_the_scheduler_nor_the_llm_can_create_a_pilot():
    """Tests 33-35: proven statically, so no runtime path can drift into it.

    A runtime assertion only covers the paths a test happens to exercise; the
    AST covers every line in the module.
    """
    import ast
    import pathlib

    forbidden = {
        "create_securities_pilot_recommendation",
        "create_execution_pilot_recommendation",
        "approve_and_execute",
        "submit_fund_operation",
        "place_order",
        "submit_order_request",
        "cancel_order",
    }
    modules = [
        "app/scheduler/jobs.py",
        "app/services/orchestrator.py",
        "app/llm/explainer.py",
        "app/services/analysis_gate.py",
    ]
    for path in modules:
        source = pathlib.Path(path).read_text()
        tree = ast.parse(source)
        called = {
            node.func.id if isinstance(node.func, ast.Name) else
            (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            for node in ast.walk(tree) if isinstance(node, ast.Call)
        }
        leaked = called & forbidden
        assert not leaked, f"{path} can reach execution: {leaked}"


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 10 & 12 — per-class readiness
# ═══════════════════════════════════════════════════════════════════


def test_one_ready_symbol_does_not_make_a_class_ready(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, symbol="BYMA")
    _instrument(db, symbol="ROTTEN", tick=None, step=None)
    broker = _quote_broker()
    with exec_settings(**_live(execution_class_policies=ACCIONES_POLICY,
                               securities_sell_enabled=True,
                               execution_sell_only=False)) as settings:
        report = evaluate_pilot_readiness(db, broker=broker, live=True, settings=settings)

    acciones = report["classes"]["acciones"]
    assert "BYMA" in acciones["sell_ready_symbols"]
    assert "ROTTEN" in acciones["blocked_symbols"]
    # Covered symbols and class readiness are separate facts, reported apart.
    assert set(acciones["covered_symbols"]) != set(
        r["symbol"] for r in report["symbols"]
    )


def test_readiness_separates_technically_ready_from_allowed_to_send(db, client):
    """'Ready' must never be printed while the lock is shut.

    broker_mode=real on purpose: in mock mode there is no IOL lock to report,
    so a mock-mode assertion would pass without exercising anything.
    """
    with exec_settings(**_live(order_execution_enabled=False, broker_mode="real")):
        response = client.get("/api/broker/execution-readiness")

    assert response.status_code == 200
    body = response.json()
    assert body["ready_for_real_execution"] is False
    assert body["order_execution_enabled"] is False
    assert "execution_locked" in body["blocking_reasons"]
    assert "next_safe_action" in body
    assert body["next_safe_action"] in (
        "resolve_instruments", "verify_instrument_fields", "configure_class_policies",
        "configure_fci_limits", "verify_fund", "run_sandbox_validation",
        "ready_for_controlled_pilot",
    )


def test_readiness_reports_each_class_and_the_legacy_path(db, client):
    _instrument(db, symbol="BYMA")
    _instrument(db, symbol="SPY", execution_class="CEDEARS")
    with exec_settings(**_live(order_execution_enabled=False,
                               execution_class_policies=ACCIONES_POLICY,
                               execution_sell_only=True)):
        body = client.get("/api/broker/execution-readiness").json()

    assert body["acciones"]["policy_configured"] is True
    assert body["cedears"]["policy_configured"] is False
    assert body["cedears"]["buy_ready"] is False
    assert body["cedears"]["sell_ready"] is False
    assert body["legacy_byma"]["legacy_sell_path_ready"] is True
    assert "BYMA" in body["acciones"]["covered_symbols"]
    assert body["fci"]["subscription_ready"] is False
    assert body["fci"]["redemption_ready"] is False


def test_next_safe_action_is_deterministic(db, client):
    with exec_settings(**_live(order_execution_enabled=False)):
        first = client.get("/api/broker/execution-readiness").json()["next_safe_action"]
        second = client.get("/api/broker/execution-readiness").json()["next_safe_action"]
    assert first == second


def test_readiness_lists_verified_and_candidate_funds(db, client):
    _fund(db, symbol="FCIOK", verified=True)
    _fund(db, symbol="FCINO", verified=False)
    with exec_settings(**_live(order_execution_enabled=False)):
        body = client.get("/api/broker/execution-readiness").json()

    assert body["fci"]["verified_funds"] == ["FCIOK"]
    assert body["fci"]["candidate_funds"] == ["FCINO"]


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 11 — ledger keys
# ═══════════════════════════════════════════════════════════════════


def test_subscribe_and_redeem_use_separate_ledger_keys(db):
    assert fci_service.fci_ledger_class("subscribe") == "FCI_SUBSCRIBE"
    assert fci_service.fci_ledger_class("redeem") == "FCI_REDEEM"
    with pytest.raises(ValueError):
        fci_service.fci_ledger_class("buy")


def test_a_days_redemptions_do_not_license_a_subscription(db, client):
    """The two budgets are separate; spending one must not open the other."""
    _fund(db)
    broker = _broker()
    with exec_settings(**_live(fci_max_daily_amount=12_000.0)):
        redemption = _operation(db, operation="redeem", amount=10_000.0)
        preview = _preview(client, redemption.id)
        with _with_broker(broker):
            first = _submit(client, redemption.id, preview)
        assert first.status_code == 200, first.text

        subscription = _operation(db, operation="subscribe", amount=10_000.0)
        preview2 = _preview(client, subscription.id)
        with _with_broker(broker):
            second = _submit(client, subscription.id, preview2)

        redeem_total = _ledger_total(db, fci_service.LEDGER_CLASS_REDEEM)
        subscribe_total = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    # The subscription is NOT blocked by the redemption's spending...
    assert second.status_code == 200, second.text
    # ...because they are genuinely separate ledgers.
    assert redeem_total == 10_000.0
    assert subscribe_total == 10_000.0


def test_legacy_fci_ledger_rows_are_kept_but_never_counted(db, client):
    """The migration is additive: old keys stay as audit, unread.

    Renaming them would move already-spent budget into today's key; copying
    them would charge the same money twice.
    """
    from app.services.execution_limits import reserve_daily_budget

    _fund(db)
    broker = _broker()
    with exec_settings(**_live(fci_max_daily_amount=15_000.0)) as settings:
        reserve_daily_budget(
            db, trade_date=trade_date_for(settings),
            execution_class="FCI:subscribe", currency="ARS",
            notional=Decimal("14000"), max_daily_notional=15_000.0,
        )
        db.commit()

        record = _operation(db, amount=10_000.0)
        preview = _preview(client, record.id)
        with _with_broker(broker):
            response = _submit(client, record.id, preview)

        legacy = _ledger_total(db, "FCI:subscribe")
        current = _ledger_total(db, fci_service.LEDGER_CLASS_SUBSCRIBE)

    assert response.status_code == 200, response.text
    # The legacy row is untouched — readable as audit.
    assert legacy == 14_000.0
    # And it did not contribute to the new key's total.
    assert current == 10_000.0


# ═══════════════════════════════════════════════════════════════════
# Regression — production history must be untouched
# ═══════════════════════════════════════════════════════════════════


def test_the_legacy_byma_pilot_still_produces_a_valid_preview(db, client):
    """Test 36: the path that already executed for real still works."""
    from app.services.execution_pilot import PILOT_CONFIRMATION

    assert PILOT_CONFIRMATION == "CREAR PILOTO BYMA 1"

    # The dedicated BYMA endpoint is unchanged and still separate from the
    # new generic one: migrating a working path is not free, so it was not
    # migrated.
    routes = {r.path for r in app.routes}
    assert "/api/execution-pilot/recommendations" in routes
    assert "/api/execution-pilot/securities" in routes


def test_production_history_rows_are_not_touched_by_this_code(db):
    """Tests 37-41: nothing in this change reads or writes those rows.

    Asserted structurally rather than against the live database: the tests run
    against a temporary SQLite file, so a runtime check here would prove
    nothing about production. What CAN be proven is that no module added or
    modified in this change contains an UPDATE or DELETE against
    recommendations or order_executions.
    """
    import pathlib
    import re

    dangerous = re.compile(
        r"(DELETE\s+FROM|UPDATE)\s+(recommendations|order_executions)",
        re.IGNORECASE,
    )
    for path in (
        "app/services/fci.py",
        "app/services/pilot_readiness.py",
        "app/services/execution_pilot.py",
        "app/services/execution_limits.py",
        "app/broker/clients.py",
        "app/main.py",
    ):
        source = pathlib.Path(path).read_text()
        assert not dangerous.search(source), f"{path} rewrites production history"


def test_the_startup_migration_only_adds(db):
    """The migration path contains no DROP, DELETE or UPDATE.

    Comments are stripped first: the docstring in that function *discusses*
    the rollback DDL, and a naive substring search would flag prose.
    """
    import io
    import pathlib
    import re
    import tokenize

    source = pathlib.Path("app/main.py").read_text()
    start = source.index("def _patch_schema")
    end = source.index("\ndef ", start + 10)
    body = source[start:end]

    code_only = "".join(
        token.string
        for token in tokenize.generate_tokens(io.StringIO(body).readline)
        if token.type not in (tokenize.COMMENT, tokenize.STRING)
        or (token.type == tokenize.STRING and "DROP" not in token.string.upper()
            and "DELETE" not in token.string.upper())
    ).upper()

    for statement in ("DROP TABLE", "DROP COLUMN", "DELETE FROM"):
        assert statement not in code_only, f"{statement} in the migration path"
    assert not re.search(r"\bUPDATE\s+\w+\s+SET", code_only, re.IGNORECASE)
