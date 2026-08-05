"""Residual defects from PR #138 and #139, at the transport level.

These tests go one layer deeper than the service-level ones: they use
`httpx.MockTransport` to count the requests that actually leave the process,
because a service-level mock cannot see a client that sends twice.

Covers:
- cancellation emitting EXACTLY one DELETE, even on 401;
- the FCI form contract on the wire (Simbolo / Monto / soloValidar);
- settlement no longer being part of instrument identity;
- buy and sell capabilities resolved from their OWN side of the book;
- settlement buckets with no label never funding a purchase.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.account_status import available_for_currency, diagnose_account_status
from app.broker.clients import IolBrokerClient
from app.broker.instrument_catalog import (
    IDENTITY_FIELDS,
    IDENTITY_HASH_VERSION,
    STATUS_IDENTITY_CHANGED,
    STATUS_VERIFIED,
    get_instrument,
    upsert_instrument,
    verification_status_of,
)
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import FundOperation
from app.services import fci as fci_service
from tests.testutils import verified_provenance

BACKEND_DIR = Path(__file__).resolve().parent.parent
SANDBOX = "https://sandbox.iol-test.local"


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


class _Recorder:
    """Counts every request that actually reaches the transport."""

    def __init__(self, responder):
        self.requests: list[httpx.Request] = []
        self._responder = responder

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._responder(request, len(self.requests))

    def of_method(self, method: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.method == method]


def _client(recorder, *, token="tok", expired=False) -> IolBrokerClient:
    broker = IolBrokerClient(api_base=SANDBOX, username="u", password="p")
    broker._client = httpx.Client(transport=httpx.MockTransport(recorder), timeout=5)
    broker._access_token = token
    broker._refresh_token = "refresh-tok"
    broker._expires_at = (
        datetime.now(timezone.utc) - timedelta(hours=1) if expired
        else datetime.now(timezone.utc) + timedelta(hours=1)
    )
    return broker


# ───────────────────────────────────────────────────────────────────
# B1 — cancellation emits exactly one DELETE
# ───────────────────────────────────────────────────────────────────


def test_a_401_on_delete_does_not_produce_a_second_delete():
    """REGRESSION: the client used to refresh the token and re-send.

    A DELETE is not idempotent from our side — IOL may already have applied
    it — and by the time we retry, the operation number may belong to a
    DIFFERENT, later order. So a 401 arriving AFTER the request was written is
    classified, never retried.
    """
    def responder(request, index):
        if request.url.path.endswith("/token"):
            return httpx.Response(200, json={
                "access_token": "fresh", "refresh_token": "r", "expires_in": 3600,
            })
        return httpx.Response(401, json={"message": "unauthorized"})

    recorder = _Recorder(responder)
    result = _client(recorder).cancel_order("183382168")

    assert len(recorder.of_method("DELETE")) == 1, "exactly one DELETE may be sent"
    assert result["http_requests_sent"] == 1
    # Ambiguous, because auth was valid when we sent it.
    assert result["outcome"] == "cancellation_unknown"


def test_a_403_on_delete_does_not_produce_a_second_delete():
    recorder = _Recorder(lambda request, index: httpx.Response(403, json={}))
    result = _client(recorder).cancel_order("183382168")
    assert len(recorder.of_method("DELETE")) == 1
    assert result["outcome"] == "cancellation_unknown"


def test_an_expired_token_is_renewed_BEFORE_the_single_delete():
    """Authentication is resolved before the point of no return, so the one
    DELETE we are allowed to send is the one that counts."""
    def responder(request, index):
        if request.url.path.endswith("/token"):
            return httpx.Response(200, json={
                "access_token": "fresh-token", "refresh_token": "r", "expires_in": 3600,
            })
        return httpx.Response(200, json={"ok": True})

    recorder = _Recorder(responder)
    result = _client(recorder, expired=True).cancel_order("183382168")

    deletes = recorder.of_method("DELETE")
    assert len(deletes) == 1
    assert deletes[0].headers["Authorization"] == "Bearer fresh-token"
    # The token call happened first.
    assert recorder.requests[0].url.path.endswith("/token")
    assert result["outcome"] == "cancelled"


@pytest.mark.parametrize("status,expected", [
    (200, "cancelled"),
    (204, "cancelled"),
    (400, "rejected"),
    (404, "rejected"),
    (409, "rejected"),
    (500, "cancellation_unknown"),
    (503, "cancellation_unknown"),
])
def test_cancellation_outcomes_never_send_more_than_one_delete(status, expected):
    recorder = _Recorder(lambda request, index: httpx.Response(status, json={}))
    result = _client(recorder).cancel_order("1")
    assert len(recorder.of_method("DELETE")) == 1
    assert result["outcome"] == expected
    assert result["http_requests_sent"] == 1


def test_a_network_failure_sends_one_delete_and_stays_unknown():
    def responder(request, index):
        raise httpx.ConnectError("boom")

    recorder = _Recorder(responder)
    result = _client(recorder).cancel_order("1")
    assert len(recorder.requests) == 1
    assert result["outcome"] == "cancellation_unknown"
    assert result["http_requests_sent"] == 1


def test_auth_failure_before_the_delete_sends_nothing():
    def responder(request, index):
        if request.url.path.endswith("/token"):
            return httpx.Response(401, json={})
        return httpx.Response(200, json={})

    recorder = _Recorder(responder)
    broker = _client(recorder, expired=True)
    broker._refresh_token = None
    result = broker.cancel_order("1")

    assert recorder.of_method("DELETE") == []
    assert result["http_requests_sent"] == 0
    assert result["outcome"] == "rejected"


def test_the_service_reports_the_real_request_count(db):
    """`delete_requests_sent` must come from the client, not be hardcoded —
    otherwise a double-send in the transport would be invisible."""
    from app.models.models import OrderExecution, Recommendation
    from app.services.cancellation import build_cancellation_preview, cancel_execution, cancellation_phrase

    db.add(Recommendation(
        id=1, action="rebalancear", status="execution_pending", suggested_pct=0.1,
        confidence=0.7, rationale="r", risks="k", executive_summary="s", metadata_json={},
    ))
    order = OrderExecution(
        recommendation_id=1, symbol="BYMA", side="sell", target_change_pct=-0.1,
        status="execution_sent", quantity_planned=1, quantity_sent=1,
        broker_order_id="183382168", broker_response={},
    )
    db.add(order)
    db.commit()

    broker = mock.MagicMock()
    broker.get_order_status.return_value = {"status": "iniciada", "raw_response": {}}
    broker.cancel_order.return_value = {
        "outcome": "cancelled", "endpoint_used": "/api/v2/operaciones/183382168",
        "raw_response": {}, "http_requests_sent": 1, "error": "",
    }

    with exec_settings(execution_admin_key="k", execution_preview_secret="s",
                       execution_preview_ttl_seconds=300,
                       order_cancellation_enabled=True):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            preview = build_cancellation_preview(db, order.id, execution_key="k")
            result = cancel_execution(
                db, order.id, execution_key="k",
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=cancellation_phrase(order.id),
                note="manual",
            )

    assert result["delete_requests_sent"] == 1
    assert result["cancellation_audit"][-1]["http_requests_sent"] == 1


# ───────────────────────────────────────────────────────────────────
# B2 — the FCI form contract, on the wire
# ───────────────────────────────────────────────────────────────────


def _fund_request(operation_kind="subscribe", amount=145.54, solo_validar=False):
    operation = FundOperation(
        fund_symbol="ADBAICA", operation=operation_kind, currency="ARS",
        amount=amount, status=fci_service.STATE_PREPARED,
    )
    request, error = fci_service.build_fund_request(operation, solo_validar=solo_validar)
    assert error is None, error
    return request


@pytest.mark.parametrize("operation_kind,path", [
    ("subscribe", "/api/v2/operar/suscripcion/fci"),
    ("redeem", "/api/v2/operar/rescate/fci"),
])
def test_fci_is_posted_form_urlencoded_with_the_documented_fields(operation_kind, path):
    recorder = _Recorder(lambda request, index: httpx.Response(
        200, json={"numeroOperacion": "FCI-9"}
    ))
    result = _client(recorder).submit_fund_request(
        _fund_request(operation_kind, solo_validar=False)
    )

    posts = recorder.of_method("POST")
    assert len(posts) == 1
    sent = posts[0]
    assert sent.url.path == path
    assert sent.headers["Content-Type"] == "application/x-www-form-urlencoded"
    # The body is form-encoded, never JSON.
    body = sent.content.decode()
    assert body == "Simbolo=ADBAICA&Monto=145.54&soloValidar=false"
    assert result["outcome"] == "submitted"
    assert result["operation_id"] == "FCI-9"


def test_solo_validar_true_is_classified_as_validated_and_creates_nothing():
    recorder = _Recorder(lambda request, index: httpx.Response(200, json={"ok": True}))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=True))

    body = recorder.of_method("POST")[0].content.decode()
    assert "soloValidar=true" in body
    assert result["outcome"] == "validated"
    # A validation never yields an operation id, even if one appeared.
    assert result["operation_id"] == ""


def test_solo_validar_true_that_echoes_an_id_still_is_not_an_execution():
    recorder = _Recorder(lambda request, index: httpx.Response(
        200, json={"numeroOperacion": "SHOULD-BE-IGNORED"}
    ))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=True))
    assert result["outcome"] == "validated"
    assert result["operation_id"] == ""


@pytest.mark.parametrize("status,expected", [
    (400, "rejected"),
    (422, "rejected"),
    (500, "submission_unknown"),
])
def test_fci_submission_outcomes_send_exactly_one_post(status, expected):
    recorder = _Recorder(lambda request, index: httpx.Response(status, json={}))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=False))
    assert len(recorder.of_method("POST")) == 1
    assert result["outcome"] == expected


def test_a_2xx_without_an_operation_id_is_submission_unknown():
    """We cannot prove the operation was created, and a duplicate
    subscription is real money."""
    recorder = _Recorder(lambda request, index: httpx.Response(200, json={"ok": True}))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=False))
    assert result["outcome"] == "submission_unknown"
    assert result["operation_id"] == ""


def test_a_401_on_an_fci_post_does_not_resend():
    recorder = _Recorder(lambda request, index: httpx.Response(401, json={}))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=False))
    assert len(recorder.of_method("POST")) == 1
    assert result["outcome"] == "rejected"


def test_an_fci_timeout_sends_one_post_and_stays_unknown():
    def responder(request, index):
        raise httpx.ReadTimeout("timeout")

    recorder = _Recorder(responder)
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=False))
    assert len(recorder.requests) == 1
    assert result["outcome"] == "submission_unknown"


def test_a_2xx_validation_error_body_is_a_definitive_rejection():
    """IOL answers 2xx with an error body when it refuses."""
    recorder = _Recorder(lambda request, index: httpx.Response(202, json=[
        {"title": "Monto", "description": "El monto es inferior al mínimo."}
    ]))
    result = _client(recorder).submit_fund_request(_fund_request(solo_validar=False))
    assert result["outcome"] == "rejected"
    assert "mínimo" in result["error"]


# ───────────────────────────────────────────────────────────────────
# B8 — settlement is not identity
# ───────────────────────────────────────────────────────────────────


def _verified(db, **overrides):
    kwargs = dict(
        broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, minimum_quantity=1.0,
        buy_supported=True, sell_supported=True, quote_supported=True,
        max_age_seconds=86400, provenance=verified_provenance(),
    )
    kwargs.update(overrides)
    entry, changed = upsert_instrument(db, **kwargs)
    db.commit()
    return entry, changed


def test_settlement_is_not_part_of_the_identity_hash():
    assert "settlement" not in IDENTITY_FIELDS


def test_changing_settlement_does_not_flag_an_identity_change(db):
    """REGRESSION: switching the class policy from t1 to t0 used to freeze
    the entire catalog as `identity_changed`."""
    _verified(db, settlement="t1")
    _, changed = _verified(db, settlement="t0")

    assert changed is False
    entry = get_instrument(db, "BYMA")
    assert verification_status_of(entry) == STATUS_VERIFIED
    # The new settlement IS recorded — it is just not identity.
    assert entry.settlement == "t0"


@pytest.mark.parametrize("mutation", [
    {"currency": "USD"},
    {"asset_type": "CEDEAR", "instrument_type": "CEDEAR"},
    {"market": "nYSE"},
    {"instrument_type": "ETF"},
])
def test_real_identity_changes_still_freeze(db, mutation):
    _verified(db)
    _, changed = _verified(db, **mutation)
    assert changed is True
    assert verification_status_of(get_instrument(db, "BYMA")) == STATUS_IDENTITY_CHANGED


def test_a_v1_hash_is_not_mistaken_for_an_identity_change(db):
    """A row whose hash was computed WITH settlement must not read as changed
    just because the definition moved on."""
    entry, _ = _verified(db, settlement="t1")
    # Simulate a PR #139 row: an old-format hash and no version marker.
    entry.raw_identity_hash = "legacy-v1-hash"
    entry.raw_identity = {
        "broker_symbol": "BYMA", "market": "bCBA", "settlement": "t1",
        "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
        "currency": "ARS", "country": "argentina",
    }
    db.commit()

    _, changed = _verified(db, settlement="t2")
    assert changed is False, "a version change is not the broker changing its mind"
    assert verification_status_of(get_instrument(db, "BYMA")) == STATUS_VERIFIED


def test_a_v1_hash_still_detects_a_real_change(db):
    entry, _ = _verified(db)
    entry.raw_identity_hash = "legacy-v1-hash"
    entry.raw_identity = {
        "broker_symbol": "BYMA", "market": "bCBA", "settlement": "t1",
        "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
        "currency": "ARS", "country": "argentina",
    }
    db.commit()

    _, changed = _verified(db, currency="USD")
    assert changed is True
    assert verification_status_of(get_instrument(db, "BYMA")) == STATUS_IDENTITY_CHANGED


def test_the_identity_hash_is_versioned():
    from app.broker.instrument_catalog import compute_identity_hash

    identity = {f: "x" for f in IDENTITY_FIELDS}
    assert IDENTITY_HASH_VERSION >= 2
    # Two identities differing only in a NON-identity field hash the same.
    assert compute_identity_hash({**identity, "settlement": "t0"}) == \
           compute_identity_hash({**identity, "settlement": "t2"})


# ───────────────────────────────────────────────────────────────────
# B9 — buy and sell capabilities are independent
# ───────────────────────────────────────────────────────────────────


def _resolver(db, *, bid: bool, ask: bool, tipo="acciones", moneda="peso_Argentino"):
    from app.broker.execution_class import CLASS_ACCIONES
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = {
        "simbolo": "GGAL", "tipo": tipo, "moneda": moneda,
        "mercado": "bCBA", "descripcion": "Grupo Galicia",
    }
    broker._authorized_get.return_value = response

    def quote(symbol, side, market, settlement):
        present = bid if side == "sell" else ask
        if not present:
            return {"available": False, "price": None, "source": "none"}
        return {"available": True, "price": 100.0,
                "source": "bid" if side == "sell" else "ask"}

    broker.get_execution_quote.side_effect = quote

    policy = {
        "buy_enabled": True, "sell_enabled": True,
        "currencies": ["ARS"], "markets": ["bCBA"], "settlements": ["t1"],
        "max_order_notional": 500.0, "max_daily_notional": 2000.0,
        "max_quantity": 10.0, "max_portfolio_pct": 0.02,
        "min_cash_reserve": 0.0, "fee_buffer_pct": 0.0,
        "max_quote_age_seconds": 15.0, "max_price_deviation_pct": 0.02,
        "catalog_max_age_seconds": 86400.0, "validity_minutes": 10.0,
        "default_quantity_step": 1.0, "default_price_tick": 0.01,
        "order_type": "precioLimite",
    }
    with exec_settings(execution_instrument_ticks={}):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: policy},
        )
    db.commit()
    return report, get_instrument(db, "GGAL"), broker


def test_both_sides_of_the_book_give_both_capabilities(db):
    _report, entry, broker = _resolver(db, bid=True, ask=True)
    assert entry.buy_supported is True
    assert entry.sell_supported is True
    # BOTH sides were actually asked for.
    sides = {call[0][1] for call in broker.get_execution_quote.call_args_list}
    assert sides == {"buy", "sell"}


def test_only_a_bid_does_not_authorise_buying(db):
    """REGRESSION: capabilities were derived from a single sell quote, so a
    bid alone claimed a buy was possible."""
    _report, entry, _broker = _resolver(db, bid=True, ask=False)
    assert entry.sell_supported is True
    assert entry.buy_supported is False


def test_only_an_ask_does_not_authorise_selling(db):
    _report, entry, _broker = _resolver(db, bid=False, ask=True)
    assert entry.buy_supported is True
    assert entry.sell_supported is False


def test_no_book_at_all_authorises_nothing(db):
    _report, entry, _broker = _resolver(db, bid=False, ask=False)
    assert entry.buy_supported is False
    assert entry.sell_supported is False
    assert entry.quote_supported is False


def test_a_quote_answering_with_the_wrong_side_is_not_evidence(db):
    """A provider that returns a bid when asked for an ask has not shown
    that the ask exists."""
    from app.broker.instrument_resolver import _side_quotable

    broker = mock.MagicMock()
    broker.get_execution_quote.return_value = {
        "available": True, "price": 100.0, "source": "bid",
    }
    assert _side_quotable(broker, "GGAL", "buy", "bCBA", "t1") is False
    assert _side_quotable(broker, "GGAL", "sell", "bCBA", "t1") is True


def test_a_non_positive_price_is_not_evidence():
    from app.broker.instrument_resolver import _side_quotable

    broker = mock.MagicMock()
    for price in (0, -1, None, float("nan")):
        broker.get_execution_quote.return_value = {
            "available": True, "price": price, "source": "bid",
        }
        assert _side_quotable(broker, "GGAL", "sell", "bCBA", "t1") is False


# ───────────────────────────────────────────────────────────────────
# B7 — settlement buckets
# ───────────────────────────────────────────────────────────────────


def _account(buckets, *, account_level=None):
    account = {"moneda": "peso_Argentino", "saldos": buckets}
    if account_level is not None:
        account["disponible"] = account_level
    return {"cuentas": [account]}


def test_a_bucket_without_a_liquidacion_label_does_not_fund_a_purchase():
    """REGRESSION: `label and label not in IMMEDIATE` let an unlabelled
    bucket through as if it were cash."""
    payload = _account([{"disponible": 999999.0, "disponibleOperar": 999999.0}])
    result = available_for_currency(payload, "ARS")
    assert result["available"] is False
    assert result["cash"] is None

    diagnosis = diagnose_account_status(payload)
    assert diagnosis["contract_valid"] is False
    assert "no_immediate_settlement_bucket_ARS" in diagnosis["blocking_reasons"]


@pytest.mark.parametrize("label", ["24hs", "t1", "48hs", "t2", "72hs", "desconocido", ""])
def test_only_an_explicitly_immediate_bucket_funds_a_purchase(label):
    payload = _account([{"liquidacion": label, "disponibleOperar": 500000.0}])
    assert available_for_currency(payload, "ARS")["available"] is False


@pytest.mark.parametrize("label", ["inmediato", "immediate", "0", "t0"])
def test_a_recognised_immediate_bucket_funds_a_purchase(label):
    payload = _account([{"liquidacion": label, "disponibleOperar": 1234.0}])
    result = available_for_currency(payload, "ARS")
    assert result["available"] is True
    assert result["cash"] == 1234.0


def test_immediate_plus_deferred_uses_only_the_immediate_part():
    payload = _account([
        {"liquidacion": "inmediato", "disponibleOperar": 1000.0},
        {"liquidacion": "24hs", "disponibleOperar": 50000.0},
    ])
    assert available_for_currency(payload, "ARS")["cash"] == 1000.0


def test_an_empty_bucket_collection_does_not_fall_back_to_the_account_field():
    """`saldos` present but empty means we have bucket data and it shows
    nothing immediate. Falling back would spend deferred money."""
    payload = _account([], account_level=777777.0)
    result = available_for_currency(payload, "ARS")
    assert result["available"] is False


def test_a_larger_account_field_never_overrides_the_immediate_bucket():
    payload = _account(
        [{"liquidacion": "inmediato", "disponibleOperar": 100.0}],
        account_level=999999.0,
    )
    assert available_for_currency(payload, "ARS")["cash"] == 100.0


def test_an_account_without_any_bucket_collection_may_use_its_own_field():
    """No buckets at all: the account-level field is the only contract on
    offer, and nothing contradicts it."""
    payload = {"cuentas": [{"moneda": "peso_Argentino", "disponible": 4321.0}]}
    result = available_for_currency(payload, "ARS")
    assert result["available"] is True
    assert result["cash"] == 4321.0


def test_an_unknown_payload_is_reported_not_guessed():
    diagnosis = diagnose_account_status({"otraCosa": 1})
    assert diagnosis["contract_valid"] is False
    assert "no_currency_accounts_found" in diagnosis["blocking_reasons"]
    assert available_for_currency({"otraCosa": 1}, "ARS")["available"] is False


# ───────────────────────────────────────────────────────────────────
# B10 + guards
# ───────────────────────────────────────────────────────────────────


def test_observed_fund_fields_do_not_make_a_fund_operable(db):
    """Reading a plausible-looking `horarioCorte` from an undocumented field
    is not evidence enough to time real money."""
    broker = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = [{
        "simbolo": "AAA", "descripcion": "Fondo A", "administradora": "IOL",
        "moneda": "peso_Argentino", "horarioCorte": "13:00", "montoMinimo": 500,
    }]
    broker._authorized_get.return_value = response

    fci_service.refresh_fund_catalog(db, broker)
    fund = fci_service.get_fund(db, "AAA")

    # The values ARE visible…
    assert fund.cutoff_local_time == "13:00"
    assert fund.minimum_amount == 500.0
    # …but their provenance is "observed", not verified, so the fund is a
    # candidate and cannot operate.
    assert fund.field_provenance["cutoff_local_time"] == fci_service.PROV_IOL_FCI_OBSERVED
    assert fund.verification_status == "candidate"
    assert fund.subscription_supported is False
    assert fund.redemption_supported is False


def test_capabilities_are_not_inferred_from_cutoff_and_minimum(db):
    broker = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = [{
        "simbolo": "BBB", "moneda": "peso_Argentino",
        "horarioCorte": "15:00", "montoMinimo": 1000, "plazoLiquidacion": 1,
    }]
    broker._authorized_get.return_value = response
    fci_service.refresh_fund_catalog(db, broker)

    fund = fci_service.get_fund(db, "BBB")
    assert fund.subscription_supported is False
    assert fund.redemption_supported is False


_FORBIDDEN = {
    "cancel_order", "cancel_execution", "submit_fund_request",
    "submit_fund_operation", "validate_fund_operation", "build_fund_request",
    "create_fund_operation", "reconcile_fund_operation",
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
def test_scheduler_and_llm_cannot_cancel_or_operate_funds(module_path):
    tree = ast.parse((BACKEND_DIR / module_path).read_text())
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


def test_fci_module_never_touches_order_execution():
    tree = ast.parse((BACKEND_DIR / "app" / "services" / "fci.py").read_text())
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.update(a.name for a in node.names)
    assert "OrderExecution" not in names
    assert "OrderExecution" not in imported


def test_new_fci_settings_default_fail_closed():
    from app.core.config import Settings

    settings = Settings()
    assert settings.fci_max_operation_amount == 0.0
    assert settings.fci_max_daily_amount == 0.0
    assert settings.fci_validation_ttl_seconds > 0
    assert settings.order_cancellation_enabled is False
