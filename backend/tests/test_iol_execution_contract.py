"""IOL Execution Contract & Sandbox Gate V1 — mandatory coverage.

Covers:
- explicit broker environments (mock/sandbox/real) that can never be
  confused, with the deprecated iol_use_sandbox flag mapped (not dead);
- fail-closed order policy (market/settlement/limit-orders-only/validity/
  quote age/price deviation) included in the signed preview;
- executable quotes: best bid for sell / best ask for buy, NEVER
  ultimoPrecio, never converted into market orders;
- the canonical form-urlencoded request builder (exact quantity, stable
  Decimal price, deterministic ART validity);
- typed submission outcomes (rejected vs submission_uncertain vs sent);
- a full end-to-end flow against httpx.MockTransport — zero real IOL.

No test here (or anywhere) touches the real IOL API.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
from unittest import mock
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.clients import IolBrokerClient
from app.broker.environment import resolve_execution_environment
from app.broker.order_request import build_iol_order_request, compute_order_validity
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.execution import (
    approve_and_execute,
    build_execution_preview,
    confirmation_phrase,
    get_execution_readiness,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
SANDBOX_BASE = "https://sandbox.iol-test.local"


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _make_snapshot(db: Session) -> PortfolioSnapshot:
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


def _make_recommendation(db: Session) -> Recommendation:
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol="AAPL",
        target_change_pct=-0.05, reason="test",
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


def _sandbox_settings(**extra):
    """Fully-configured sandbox execution environment. Note: the REAL lock
    (order_execution_enabled) stays false — sandbox has its own lock."""
    base = dict(
        broker_mode="sandbox",
        order_execution_enabled=False,
        sandbox_execution_enabled=True,
        iol_sandbox_api_base=SANDBOX_BASE,
        iol_sandbox_username="sbx-user",
        iol_sandbox_password="sbx-pass",
        iol_use_sandbox=False,
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        execution_max_recommendation_age_minutes=60,
        execution_max_order_value=1_000_000.0,
        execution_max_total_value=1_000_000.0,
        execution_max_portfolio_pct=0.50,
        iol_order_market="bCBA",
        iol_order_settlement="t1",
        iol_order_type="precioLimite",
        iol_order_validity_minutes=10,
        execution_max_quote_age_seconds=15,
        execution_max_price_deviation_pct=0.05,
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


def _mock_transport_broker(handler) -> IolBrokerClient:
    """IolBrokerClient wired to httpx.MockTransport — zero real network."""
    settings = get_settings()
    env = resolve_execution_environment(settings)
    broker = IolBrokerClient(
        api_base=env["api_base"], username=env["username"], password=env["password"]
    )
    broker._client = httpx.Client(transport=httpx.MockTransport(handler), timeout=10)
    return broker


def _token_response() -> httpx.Response:
    return httpx.Response(200, json={
        "access_token": "sbx-access-token", "refresh_token": "sbx-refresh", "expires_in": 3600,
    })


# ───────────────────────────────────────────────────────────────────
# Parte 1 — ambientes explícitos (tests 1-4)
# ───────────────────────────────────────────────────────────────────


def test_sandbox_never_uses_real_host():
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="https://api.invertironline.com")):
        env = resolve_execution_environment(get_settings())
    assert "sandbox_environment_invalid" in env["errors"]
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="https://proxy.example/api.invertironline.com")):
        env = resolve_execution_environment(get_settings())
    assert "sandbox_environment_invalid" in env["errors"]


def test_real_never_uses_sandbox_host():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://sandbox.iol-test.local",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert env["environment"] == "real"
    assert "real_environment_invalid" in env["errors"]


def test_empty_sandbox_base_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="")):
        env = resolve_execution_environment(get_settings())
        assert "sandbox_environment_not_configured" in env["errors"]
        preview = build_execution_preview(db, rec.id)
    assert "sandbox_environment_not_configured" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


def test_iol_use_sandbox_flag_is_not_dead_config():
    """DEPRECATED iol_use_sandbox=true must map to the sandbox environment."""
    with exec_settings(broker_mode="real", iol_use_sandbox=True,
                       iol_sandbox_api_base="", iol_sandbox_username="", iol_sandbox_password=""):
        env = resolve_execution_environment(get_settings())
    assert env["environment"] == "sandbox"
    assert "sandbox_environment_not_configured" in env["errors"]


# ───────────────────────────────────────────────────────────────────
# Parte 2/3 — política fail-closed en el preview (tests 5-8)
# ───────────────────────────────────────────────────────────────────


def test_empty_policy_blocks_real_preview(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_order_market="", iol_order_settlement=""):
        preview = build_execution_preview(db, rec.id)
    assert "iol_order_policy_not_configured" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


def test_empty_settlement_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(iol_order_settlement="")):
        preview = build_execution_preview(db, rec.id)
    assert "iol_order_policy_not_configured" in preview["blocking_reasons"]


def test_empty_market_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(iol_order_market="")):
        preview = build_execution_preview(db, rec.id)
    assert "iol_order_policy_not_configured" in preview["blocking_reasons"]


def test_market_order_type_is_banned(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(iol_order_type="precioMercado")):
        preview = build_execution_preview(db, rec.id)
    assert "unsupported_iol_order_type" in preview["blocking_reasons"]
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=2, price=1900,
        market="bCBA", settlement="t1", order_type="precioMercado",
        validity="2026-07-29T12:00:00",
    )
    assert req is None
    assert err == "unsupported_iol_order_type"


def test_venue_is_signed_policy_change_invalidates_hash(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        assert preview["execution_venue"]["market"] == "bCBA"
        assert preview["execution_venue"]["settlement"] == "t1"
        gen = datetime.fromisoformat(preview["generated_at"])
        with exec_settings(iol_order_settlement="t2"):
            changed = build_execution_preview(db, rec.id, generated_at=gen)
    assert changed["preview_hash"] != preview["preview_hash"]


# ───────────────────────────────────────────────────────────────────
# Parte 4 — cotización ejecutable (tests 9-12)
# ───────────────────────────────────────────────────────────────────


def _quote_via_transport(payload, side="sell"):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return _token_response()
        return httpx.Response(200, json=payload)

    with exec_settings(**_sandbox_settings()):
        broker = _mock_transport_broker(handler)
        return broker.get_execution_quote("AAPL", side, "bCBA", "t1")


def test_quote_without_book_side_is_unavailable():
    quote = _quote_via_transport({"puntas": {}, "ultimoPrecio": 1900})
    assert quote["available"] is False
    assert quote["price"] is None


def test_quote_never_falls_back_to_last_price():
    """ultimoPrecio present but no bid → NOT executable (never market order)."""
    quote = _quote_via_transport({"ultimoPrecio": 1900.0})
    assert quote["available"] is False
    buy_quote = _quote_via_transport({"puntas": {"precioCompra": 1900.0}, "ultimoPrecio": 1905.0}, side="buy")
    assert buy_quote["available"] is False  # buy needs the ask, bid is not enough


def _run_iol_flow(db, broker_quote, submit_result=None, settings_extra=None):
    """Run the validated flow in sandbox mode with a mocked broker object."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    fake_broker = mock.MagicMock()
    fake_broker.get_execution_quote.return_value = broker_quote
    fake_broker.submit_order_request.return_value = submit_result or {
        "outcome": "sent", "order_id": "SBX-1", "raw_response": {"numeroOperacion": "SBX-1"},
        "endpoint_used": "/api/v2/operar/Vender", "error": "",
    }
    with exec_settings(**_sandbox_settings(**(settings_extra or {}))):
        preview = build_execution_preview(db, rec.id)
        assert preview["can_submit_approval"] is True
        with mock.patch("app.services.execution._get_execution_broker", return_value=fake_broker):
            result = _reinforced_approve(db, rec, preview)
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    return result, oe, fake_broker, preview


def test_stale_quote_blocks_submission(db):
    stale = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
    result, oe, broker, _ = _run_iol_flow(db, {
        "available": True, "price": 1900.0, "source": "bid",
        "retrieved_at": stale, "market": "bCBA", "settlement": "t1",
    })
    assert result["status"] == "execution_failed"
    assert oe.status == "failed"
    assert "quote_stale" in oe.error_message
    assert oe.quantity_sent is None
    broker.submit_order_request.assert_not_called()


def test_price_deviation_exceeded_blocks_submission(db):
    result, oe, broker, _ = _run_iol_flow(db, {
        "available": True, "price": 3000.0, "source": "bid",  # ref is 1900
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }, settings_extra={"execution_max_price_deviation_pct": 0.02})
    assert result["status"] == "execution_failed"
    assert "price_deviation_exceeded" in oe.error_message
    broker.submit_order_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Parte 5 — builder canónico (tests 13-19)
# ───────────────────────────────────────────────────────────────────


def test_decimal_quantity_blocks():
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=14.5, price=1900,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="2026-07-29T12:00:00",
    )
    assert req is None
    assert err == "invalid_broker_quantity"


def test_zero_quantity_blocks():
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=0, price=1900,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="2026-07-29T12:00:00",
    )
    assert err == "invalid_broker_quantity"


def test_zero_price_blocks():
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=2, price=0,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="2026-07-29T12:00:00",
    )
    assert err == "invalid_broker_price"


def test_missing_validity_blocks():
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=2, price=1900,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="",
    )
    assert err == "order_validity_expired"


def test_expired_or_cross_day_validity_blocks():
    _, err = compute_order_validity(0)
    assert err == "order_validity_expired"
    _, err = compute_order_validity(-5)
    assert err == "order_validity_expired"
    from app.broker.order_request import _ART
    near_midnight = datetime(2026, 7, 29, 23, 55, tzinfo=_ART)
    _, err = compute_order_validity(10, now=near_midnight)
    assert err == "order_validity_expired"  # would cross the operating day
    validity, err = compute_order_validity(10, now=datetime(2026, 7, 29, 12, 0, tzinfo=_ART))
    assert err is None
    assert validity == "2026-07-29T12:10:00"


def test_request_is_form_urlencoded_never_json():
    req, err = build_iol_order_request(
        side="sell", symbol="AAPL", quantity=2, price=1900.0,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="2026-07-29T12:00:00",
    )
    assert err is None
    assert req["content_type"] == "application/x-www-form-urlencoded"
    assert req["endpoint"] == "/api/v2/operar/Vender"
    assert req["form_data"] == {
        "mercado": "bCBA", "simbolo": "AAPL", "cantidad": "2", "precio": "1900",
        "plazo": "t1", "validez": "2026-07-29T12:00:00", "tipoOrden": "precioLimite",
    }
    # All values are strings — this is a form body, not a JSON document.
    assert all(isinstance(v, str) for v in req["form_data"].values())


def test_price_serialization_is_stable_decimal():
    req, _ = build_iol_order_request(
        side="buy", symbol="AAPL", quantity=3, price=19730.50,
        market="bCBA", settlement="t2", order_type="precioLimite",
        validity="2026-07-29T12:00:00",
    )
    assert req["form_data"]["precio"] == "19730.5"
    assert req["endpoint"] == "/api/v2/operar/Comprar"


# ───────────────────────────────────────────────────────────────────
# Parte 6 — clasificación de resultados (tests 20-24)
# ───────────────────────────────────────────────────────────────────


def _submit_via_transport(order_response_factory):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return _token_response()
        return order_response_factory(request)

    with exec_settings(**_sandbox_settings()):
        broker = _mock_transport_broker(handler)
        req, _ = build_iol_order_request(
            side="sell", symbol="AAPL", quantity=2, price=1900,
            market="bCBA", settlement="t1", order_type="precioLimite",
            validity="2026-07-29T12:00:00",
        )
        return broker.submit_order_request(req)


def test_http_400_is_definitive_rejection():
    result = _submit_via_transport(lambda r: httpx.Response(400, json={"error": "mercado inválido"}))
    assert result["outcome"] == "rejected"


def test_timeout_is_submission_uncertain(db):
    def raise_timeout(request):
        raise httpx.ReadTimeout("read timeout after write", request=request)

    result = _submit_via_transport(raise_timeout)
    assert result["outcome"] == "submission_uncertain"

    # And at the service level it lands in manual reconciliation:
    flow_result, oe, _, _ = _run_iol_flow(db, {
        "available": True, "price": 1900.0, "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }, submit_result={
        "outcome": "submission_uncertain", "order_id": "", "raw_response": {},
        "endpoint_used": "/api/v2/operar/Vender", "error": "timeout",
    })
    assert flow_result["status"] == "manual_reconciliation_required"
    assert oe.status == "manual_reconciliation_required"


def test_http_500_is_submission_uncertain():
    result = _submit_via_transport(lambda r: httpx.Response(500, json={}))
    assert result["outcome"] == "submission_uncertain"


def test_2xx_without_numero_operacion_is_uncertain():
    result = _submit_via_transport(lambda r: httpx.Response(200, json={"ok": True}))
    assert result["outcome"] == "submission_uncertain"


def test_2xx_with_invalid_json_is_uncertain():
    result = _submit_via_transport(lambda r: httpx.Response(200, content=b"<html>not json</html>"))
    assert result["outcome"] == "submission_uncertain"


# ───────────────────────────────────────────────────────────────────
# Parte 10 — end-to-end completo con MockTransport (tests 25-27, 18-19)
# ───────────────────────────────────────────────────────────────────


def test_end_to_end_sandbox_flow_with_mock_transport(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    captured = {"order_posts": [], "status_gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "sandbox.iol-test.local", "must NEVER touch another host"
        if request.url.path == "/token":
            return _token_response()
        if request.url.path.startswith("/api/v2/Cotizaciones/detalle/bCBA/AAPL"):
            return httpx.Response(200, json={"puntas": {"precioCompra": 1900.0, "precioVenta": 1910.0}})
        if request.url.path == "/api/v2/operar/Vender":
            captured["order_posts"].append({
                "method": request.method,
                "content_type": request.headers.get("Content-Type"),
                "auth": request.headers.get("Authorization"),
                "body": request.content.decode(),
            })
            return httpx.Response(200, json={"numeroOperacion": 424242})
        if request.url.path.startswith("/api/v2/operaciones/"):
            captured["status_gets"] += 1
            return httpx.Response(200, json={"estado": "pendiente"})
        return httpx.Response(404, json={})

    with exec_settings(**_sandbox_settings()):
        broker = _mock_transport_broker(handler)
        preview = build_execution_preview(db, rec.id)
        assert preview["can_submit_approval"] is True
        expected_qty = preview["orders_preview"][0]["quantity_planned"]

        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
            # Second approval: no second POST ever
            second = approve_and_execute(db, rec.id)

    assert result["status"] == "approved"
    assert second["status_code"] == 409

    # Exactly ONE order POST, with the exact documented contract
    assert len(captured["order_posts"]) == 1
    post = captured["order_posts"][0]
    assert post["method"] == "POST"
    assert post["content_type"] == "application/x-www-form-urlencoded"
    assert not post["body"].strip().startswith("{"), "body must never be JSON"
    form = {k: v[0] for k, v in parse_qs(post["body"]).items()}
    assert form["mercado"] == "bCBA"
    assert form["simbolo"] == "AAPL"
    assert form["cantidad"] == str(int(expected_qty)) == "2"
    assert form["precio"] == "1900"
    assert form["plazo"] == "t1"
    assert form["tipoOrden"] == "precioLimite"
    assert form["validez"]  # present, deterministic ART timestamp

    # Persisted execution: sent, with numeroOperacion and exact audit
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "execution_sent"
    assert oe.broker_order_id == "424242"
    assert oe.quantity_sent == expected_qty
    audit = oe.broker_response["request_audit"]
    assert audit["iol_request"]["environment"] == "sandbox"
    assert audit["iol_request"]["api_host"] == "sandbox.iol-test.local"
    assert audit["iol_request"]["endpoint"] == "/api/v2/operar/Vender"
    assert audit["iol_request"]["content_type"] == "application/x-www-form-urlencoded"
    # The audit is EXACTLY what was sent
    for field in ("mercado", "simbolo", "cantidad", "precio", "plazo", "validez", "tipoOrden"):
        assert audit["iol_request"][field] == form[field]
    assert audit["execution_quote"]["source"] == "bid"
    assert audit["execution_quote"]["price"] == 1900.0

    # No secrets anywhere in the persisted response
    serialized = json.dumps(oe.broker_response)
    for secret in ("sbx-access-token", "sbx-user", "sbx-pass", "Bearer",
                   TEST_ADMIN_KEY, TEST_PREVIEW_SECRET, "Authorization"):
        assert secret not in serialized

    # Read-only status query works against the same mock transport
    with exec_settings(**_sandbox_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            from app.services.execution import refresh_broker_status
            status_result = refresh_broker_status(db, oe.id, execution_key=TEST_ADMIN_KEY)
    assert status_result["broker_status"] == "pendiente"
    assert status_result["status_unchanged"] is True
    assert captured["status_gets"] == 1


def test_uncertain_e2e_never_retries_and_blocks_second_approve(db):
    """Timeout during the POST → uncertain → manual reconciliation; the
    second approve is 409 and the wire never sees a second POST."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    post_attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return _token_response()
        if request.url.path.startswith("/api/v2/Cotizaciones/"):
            return httpx.Response(200, json={"puntas": {"precioCompra": 1900.0}})
        if request.url.path == "/api/v2/operar/Vender":
            post_attempts["n"] += 1
            raise httpx.ReadTimeout("timed out mid-send", request=request)
        return httpx.Response(404, json={})

    with exec_settings(**_sandbox_settings()):
        broker = _mock_transport_broker(handler)
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = _reinforced_approve(db, rec, preview)
            second = approve_and_execute(db, rec.id)

    assert result["status"] == "manual_reconciliation_required"
    assert second["status_code"] == 409
    assert post_attempts["n"] == 1  # at-most-once: no automatic retry
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
    assert oe.status == "manual_reconciliation_required"
    assert oe.quantity_sent == 2  # it may have reached IOL


# ───────────────────────────────────────────────────────────────────
# Compatibilidad y readiness (tests 28-29)
# ───────────────────────────────────────────────────────────────────


def test_mock_legacy_flow_still_works(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", order_execution_enabled=False, iol_use_sandbox=False):
        with mock.patch("app.services.execution.IolBrokerClient", side_effect=AssertionError("IOL touched")):
            result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"


def test_real_host_never_contacted_in_this_module():
    """The only hosts any test here can reach are mock transports; the
    sandbox environment refuses the real host outright."""
    with exec_settings(**_sandbox_settings()):
        env = resolve_execution_environment(get_settings())
        assert "api.invertironline.com" not in env["api_base"]


def test_readiness_production_shape():
    """Production-like settings: real broker, lock down, nothing configured →
    not ready, with the expected blocking reasons and no secrets."""
    with exec_settings(
        broker_mode="real", iol_use_sandbox=False, order_execution_enabled=False,
        sandbox_execution_enabled=False,
        iol_username="prod-user", iol_password="prod-pass",
        iol_real_username="", iol_real_password="",
        execution_admin_key="", execution_preview_secret="",
        execution_max_order_value=0.0, execution_max_total_value=0.0,
        execution_max_portfolio_pct=0.0,
        iol_order_market="", iol_order_settlement="",
    ):
        readiness = get_execution_readiness()
    assert readiness["environment"] == "real"
    assert readiness["api_host"] == "api.invertironline.com"
    assert readiness["order_execution_enabled"] is False
    assert readiness["ready_for_real_execution"] is False
    assert readiness["ready_for_sandbox_execution"] is False
    assert readiness["credentials_configured"] is True
    assert readiness["order_policy_configured"] is False
    for code in ("execution_locked", "execution_admin_key_not_configured",
                 "preview_signing_not_configured", "execution_limits_not_configured",
                 "iol_order_policy_not_configured"):
        assert code in readiness["blocking_reasons"]
    serialized = json.dumps(readiness)
    assert "prod-user" not in serialized
    assert "prod-pass" not in serialized


def test_readiness_sandbox_fully_configured():
    with exec_settings(**_sandbox_settings()):
        readiness = get_execution_readiness()
    assert readiness["environment"] == "sandbox"
    assert readiness["api_host"] == "sandbox.iol-test.local"
    assert readiness["ready_for_sandbox_execution"] is True
    assert readiness["ready_for_real_execution"] is False
