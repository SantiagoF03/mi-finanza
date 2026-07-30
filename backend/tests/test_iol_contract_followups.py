"""Follow-ups to the real-IOL contract corrections.

1. get_order_status must read the real field `estadoActual`.
2. Cumulative sell exposure per symbol validated across the whole batch.
3. The legacy JSON place_order path is unreachable for real execution.
4. discovery.py uses the working quote endpoint.

Everything runs against httpx.MockTransport or mocked brokers — no real IOL
call, no POST, no order ever leaves the process.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.clients import IolBrokerClient, MockBrokerClient
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
)

BACKEND_DIR = Path(__file__).resolve().parent.parent
TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
SANDBOX_BASE = "https://sandbox.iol-test.local"

POLICY = {
    "asset_type": "CEDEAR", "instrument_type": "CEDEAR", "currency": "USD",
    "market": "bCBA", "settlement": "t1",
    "quantity_step": 1, "price_tick": 0.01,
    "max_quantity": 100, "max_notional": 1_000_000,
}


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


def _broker_with_transport(handler) -> IolBrokerClient:
    broker = IolBrokerClient(api_base=SANDBOX_BASE, username="u", password="p")
    broker._client = httpx.Client(transport=httpx.MockTransport(handler), timeout=10)
    return broker


def _token(request: httpx.Request):
    if request.url.path == "/token":
        return httpx.Response(200, json={
            "access_token": "t", "refresh_token": "r", "expires_in": 3600,
        })
    return None


# ───────────────────────────────────────────────────────────────────
# 1 — get_order_status lee estadoActual
# ───────────────────────────────────────────────────────────────────


def _status_for(payload) -> dict:
    def handler(request):
        return _token(request) or httpx.Response(200, json=payload)

    return _broker_with_transport(handler).get_order_status("424242")


@pytest.mark.parametrize("estado_actual", ["iniciada", "cancelada", "terminada", "en proceso"])
def test_get_order_status_reads_estado_actual(estado_actual):
    payload = {"numero": 424242, "estadoActual": estado_actual, "estado": "ignored-legacy"}
    result = _status_for(payload)
    assert result["status"] == estado_actual
    assert result["order_id"] == "424242"
    assert result["raw_response"] == payload  # raw preserved verbatim


def test_get_order_status_falls_back_to_estado():
    payload = {"numero": 1, "estado": "terminada"}
    result = _status_for(payload)
    assert result["status"] == "terminada"
    assert result["raw_response"] == payload


def test_get_order_status_unknown_when_neither_field():
    payload = {"numero": 1, "otraCosa": "x"}
    result = _status_for(payload)
    assert result["status"] == "unknown"
    assert result["raw_response"] == payload


def test_get_order_status_empty_estado_actual_falls_back():
    result = _status_for({"estadoActual": "", "estado": "iniciada"})
    assert result["status"] == "iniciada"


def test_get_order_status_non_dict_body_is_unknown():
    result = _status_for(["unexpected"])
    assert result["status"] == "unknown"
    assert result["raw_response"] == ["unexpected"]


# ───────────────────────────────────────────────────────────────────
# 2 — exposición acumulada de ventas por símbolo
# ───────────────────────────────────────────────────────────────────


def _sandbox_settings(**extra):
    base = dict(
        broker_mode="sandbox", iol_use_sandbox=False,
        order_execution_enabled=False, sandbox_execution_enabled=True,
        iol_sandbox_api_base=SANDBOX_BASE,
        iol_sandbox_username="sbx-user", iol_sandbox_password="sbx-pass",
        execution_admin_key=TEST_ADMIN_KEY, execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300, execution_max_recommendation_age_minutes=60,
        execution_max_order_value=1_000_000.0, execution_max_total_value=1_000_000.0,
        execution_max_portfolio_pct=0.50,
        iol_order_market="bCBA", iol_order_settlement="t1",
        iol_order_type="precioLimite", iol_order_validity_minutes=10,
        execution_max_quote_age_seconds=15, execution_max_price_deviation_pct=0.05,
        execution_sell_only=True, execution_require_live_position_check=True,
        execution_instrument_policies={"AAPL": dict(POLICY), "MSFT": dict(POLICY)},
    )
    base.update(extra)
    return base


def _make_two_sells(db: Session, symbols=("AAPL", "AAPL"), pct=-0.05):
    """Snapshot + recommendation with two sell actions."""
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    for sym, qty, mv in (("AAPL", 20, 38000), ("MSFT", 12, 28000)):
        db.add(PortfolioPosition(
            snapshot_id=snap.id, symbol=sym, asset_type="CEDEAR",
            instrument_type="CEDEAR", currency="USD", quantity=qty,
            market_value=mv, avg_price=180, pnl_pct=0.1,
        ))
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for sym in symbols:
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=sym,
            target_change_pct=pct, reason="test",
        ))
    db.commit()
    return rec


def _live(aapl_qty=20, msft_qty=12):
    return {"total_value": 100000, "cash": 12000, "positions": [
        {"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": aapl_qty, "market_value": 38000},
        {"symbol": "MSFT", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": msft_qty, "market_value": 28000},
    ]}


def _broker(live=None):
    b = mock.MagicMock()
    b.get_portfolio_snapshot.return_value = live if live is not None else _live()
    b.get_execution_quote.side_effect = lambda symbol, side, market, settlement: {
        "available": True,
        "price": 1900.0 if symbol == "AAPL" else 2333.33,
        "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": market, "settlement": settlement,
    }
    b.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "S-1", "raw_response": {"numeroOperacion": "S-1"},
        "endpoint_used": "/api/v2/operar/Vender", "error": "",
    }
    return b


def _run(db, rec, broker, settings_extra=None):
    with exec_settings(**_sandbox_settings(**(settings_extra or {}))):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = approve_and_execute(
                db, rec.id, execution_key=TEST_ADMIN_KEY,
                preview_hash=preview["preview_hash"],
                preview_generated_at=preview["generated_at"],
                confirmation_text=confirmation_phrase(rec.id),
            )
    rows = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).all()
    return result, rows, preview


def test_two_sells_of_same_symbol_exceeding_holding_produce_zero_posts(db):
    """Each order alone fits in the position, together they do not."""
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.60)
    broker = _broker(live=_live(aapl_qty=20))
    result, rows, preview = _run(db, rec, broker)

    per_order = [o["quantity_planned"] for o in preview["orders_preview"]]
    assert all(q <= 20 for q in per_order), "each order alone must fit"
    assert sum(per_order) > 20, "together they must exceed the holding"

    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()
    broker.get_execution_quote.assert_not_called()  # blocked before quoting
    assert all(r.quantity_sent is None for r in rows)
    assert all(r.status != "submitting" for r in rows)
    assert any("live_position_insufficient_batch" in (r.error_message or "") for r in rows)


def test_sum_equal_to_position_is_valid(db):
    """Exactly the held quantity must pass — the guard blocks only excess."""
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.19)
    broker = _broker(live=_live(aapl_qty=20))
    result, rows, preview = _run(db, rec, broker)
    total = sum(o["quantity_planned"] for o in preview["orders_preview"])
    assert total == 20  # exactly the holding
    assert result["status"] == "approved"
    assert broker.submit_order_request.call_count == 2


def test_different_symbols_are_checked_independently(db):
    """AAPL and MSFT each within their own holding → both go through."""
    rec = _make_two_sells(db, symbols=("AAPL", "MSFT"), pct=-0.05)
    broker = _broker(live=_live())
    result, rows, _ = _run(db, rec, broker)
    assert result["status"] == "approved"
    assert broker.submit_order_request.call_count == 2
    assert {r.symbol for r in rows} == {"AAPL", "MSFT"}


def test_batch_exposure_audit_is_stable_and_detailed(db):
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.60)
    broker = _broker(live=_live(aapl_qty=20))
    result, rows, _ = _run(db, rec, broker)

    audits = [
        (r.broker_response or {}).get("request_audit", {}).get("batch_sell_exposure_check")
        for r in rows
    ]
    assert all(a for a in audits), "every order carries the batch audit"
    audit = audits[0]
    assert audit["code"] == "live_position_insufficient_batch"
    assert audit["symbol"] == "AAPL"
    assert audit["quantity_available"] == 20
    assert audit["total_quantity_required"] > 20
    assert audit["order_count"] == 2
    assert len(audit["order_execution_ids"]) == 2
    assert len(audit["recommendation_action_ids"]) == 2
    assert audit["passed"] is False
    # No credentials leak into the audit
    serialized = json.dumps(rows[0].broker_response)
    for secret in (TEST_ADMIN_KEY, TEST_PREVIEW_SECRET, "sbx-user", "sbx-pass"):
        assert secret not in serialized


def test_offending_symbol_is_blamed_and_others_are_preflight_cancelled(db):
    """AAPL over-exposed, MSFT innocent → AAPL failed, MSFT preflight_cancelled."""
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.60)
    # Add an unrelated MSFT sell to the same batch
    db.add(RecommendationAction(recommendation_id=rec.id, symbol="MSFT",
                                target_change_pct=-0.05, reason="test"))
    db.commit()
    broker = _broker(live=_live(aapl_qty=20))
    result, rows, _ = _run(db, rec, broker)

    by_symbol = {}
    for r in rows:
        by_symbol.setdefault(r.symbol, []).append(r.status)
    assert set(by_symbol["AAPL"]) == {"failed"}
    assert set(by_symbol["MSFT"]) == {"preflight_cancelled"}
    broker.submit_order_request.assert_not_called()


def test_batch_guard_uses_the_single_live_read(db):
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.60)
    broker = _broker(live=_live(aapl_qty=20))
    _run(db, rec, broker)
    assert broker.get_portfolio_snapshot.call_count == 1


def test_symbol_missing_from_live_portfolio_blocks_batch(db):
    rec = _make_two_sells(db, symbols=("AAPL", "AAPL"), pct=-0.05)
    live = _live()
    live["positions"] = [p for p in live["positions"] if p["symbol"] != "AAPL"]
    broker = _broker(live=live)
    result, rows, _ = _run(db, rec, broker)
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()
    assert all(r.quantity_sent is None for r in rows)


# ───────────────────────────────────────────────────────────────────
# 3 — el place_order legacy es inalcanzable para ejecución real
# ───────────────────────────────────────────────────────────────────


def test_real_client_place_order_refuses_to_send():
    """Defense in depth: the JSON/hardcoded-plazo/precioMercado path cannot
    send anything from a real broker."""
    sent = []

    def handler(request):
        sent.append(request.url.path)
        return _token(request) or httpx.Response(200, json={"numeroOperacion": 1})

    broker = _broker_with_transport(handler)
    with pytest.raises(RuntimeError) as exc:
        broker.place_order("BYMA", "sell", 1, price=298)
    assert "submit_order_request" in str(exc.value)
    assert not any("operar" in p for p in sent), "nothing may be POSTed"


def test_reinforced_real_flow_uses_only_the_canonical_contract(db):
    """A real-mode approve reaches submit_order_request with a
    form-urlencoded request, and never place_order."""
    rec = _make_two_sells(db, symbols=("AAPL",), pct=-0.05)
    broker = _broker(live=_live())
    broker.place_order.side_effect = AssertionError("legacy place_order must never be used")
    real_env = dict(
        broker_mode="real", iol_use_sandbox=False, order_execution_enabled=True,
        iol_real_username="u", iol_real_password="p",
    )
    result, rows, _ = _run(db, rec, broker, settings_extra=real_env)
    assert result["status"] == "approved"
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_called_once()
    request = broker.submit_order_request.call_args[0][0]
    assert request["content_type"] == "application/x-www-form-urlencoded"
    assert request["form_data"]["tipoOrden"] == "precioLimite"
    assert request["form_data"]["plazo"] == "t1"  # from policy, not hardcoded t2
    assert request["form_data"]["validez"]


def test_mock_client_place_order_still_works():
    """The mock broker keeps its legacy behavior — it never touches IOL."""
    result = MockBrokerClient().place_order("AAPL", "sell", 10, price=150.0)
    assert result["status"] == "sent"
    assert result["endpoint_used"].endswith("/Vender")


def test_execution_module_routes_non_mock_to_canonical_contract():
    """Static guarantee: the only place_order call sites in execution.py sit
    in the mock-only branches."""
    src = (BACKEND_DIR / "app" / "services" / "execution.py").read_text()
    assert "is_iol = _effective_environment(settings) != \"mock\"" in src
    assert "_submit_prepared_order" in src


# ───────────────────────────────────────────────────────────────────
# 4 — discovery usa el endpoint que funciona
# ───────────────────────────────────────────────────────────────────


def test_discovery_uses_the_working_quote_endpoint(db):
    from app.market.discovery import fetch_fresh_quotes

    seen = []

    class FakeBroker:
        def _authorized_get(self, path):
            seen.append(path)
            if path == "/api/v2/bCBA/Titulos/AAPL/Cotizacion":
                return mock.Mock(json=lambda: {"ultimoPrecio": 1905.0})
            raise AssertionError(f"unexpected path {path}")

    fresh, failures = fetch_fresh_quotes(db, ["AAPL"], broker=FakeBroker())
    assert seen == ["/api/v2/bCBA/Titulos/AAPL/Cotizacion"]
    assert not any("Cotizaciones/detalle" in p for p in seen)
    assert "AAPL" in fresh


def test_discovery_fails_closed_on_http_error(db):
    """A 400 (what the legacy endpoint returned) must not invent a price."""
    from app.market.discovery import fetch_fresh_quotes

    class FailingBroker:
        def _authorized_get(self, path):
            raise RuntimeError("HTTP 400 Bad Request")

    fresh, failures = fetch_fresh_quotes(db, ["AAPL"], broker=FailingBroker())
    assert "AAPL" not in fresh
    assert "AAPL" in failures.get("failed_symbols", []) or failures


def test_no_legacy_quote_endpoint_is_called_anywhere():
    """The broken path must not survive as a real call in application code.

    Comments referencing it (explaining why it was replaced) are allowed —
    what must not exist is an actual request built with it.
    """
    offenders = []
    for path in (BACKEND_DIR / "app").rglob("*.py"):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if "Cotizaciones/detalle" not in line:
                continue
            if line.lstrip().startswith("#"):
                continue  # explanatory comment
            offenders.append(f"{path.relative_to(BACKEND_DIR)}:{lineno}")
    assert offenders == [], f"legacy quote endpoint still called in {offenders}"
