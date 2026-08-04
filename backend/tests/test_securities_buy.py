"""Securities BUY support — live cash preflight, ACCIONES and CEDEARs.

The pilot could only sell, and only BYMA. These tests pin the buy path:

- a buy works for an instrument NOT in the portfolio (no position needed to
  get a price, no invented avg_price);
- the balance is read LIVE, in the right currency, immediately before the
  send — snapshot.cash can shrink an order but never authorise one;
- fees, the minimum reserve and already-pending buys are all subtracted;
- the best ASK is used, never the last price and never the bid;
- everything stays off unless SECURITIES_BUY_ENABLED is explicitly set.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.execution_class import CLASS_ACCIONES, CLASS_CEDEARS
from app.broker.instrument_catalog import upsert_instrument
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    InstrumentCatalog,
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
from app.services.execution_limits import evaluate_buy_cash
from tests.testutils import verified_provenance

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
SANDBOX_BASE = "https://sandbox.iol-test.local"

CLASS_POLICY = {
    "buy_enabled": True, "sell_enabled": True,
    "currencies": ["ARS"], "markets": ["bCBA"], "settlements": ["t1"],
    "max_order_notional": 100_000.0, "max_daily_notional": 500_000.0,
    "max_quantity": 1000.0, "max_portfolio_pct": 0.5,
    "min_cash_reserve": 0.0, "fee_buffer_pct": 0.0,
    "max_quote_age_seconds": 15.0, "max_price_deviation_pct": 0.5,
    "catalog_max_age_seconds": 86400.0, "validity_minutes": 10.0,
    "default_quantity_step": 1.0, "default_price_tick": 0.01,
    "order_type": "precioLimite",
}


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


def _settings(**extra):
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
        execution_max_quote_age_seconds=15, execution_max_price_deviation_pct=0.5,
        execution_require_live_position_check=True,
        execution_require_live_cash_check=True,
        # New contract: sell-only is off, capabilities are explicit.
        execution_sell_only=False,
        securities_buy_enabled=True, securities_sell_enabled=True,
        execution_instrument_policies={},
        execution_class_policies={CLASS_ACCIONES: dict(CLASS_POLICY),
                                  CLASS_CEDEARS: dict(CLASS_POLICY)},
        execution_instrument_overrides={},
        execution_instrument_ticks={},
        execution_denylist=[],
        execution_allow_override_limit_increase=False,
    )
    base.update(extra)
    return base


def _snapshot(db: Session, cash=100_000.0, currency="ARS", with_position=False):
    snap = PortfolioSnapshot(total_value=1_000_000.0, cash=cash, currency=currency)
    db.add(snap)
    db.flush()
    if with_position:
        db.add(PortfolioPosition(
            snapshot_id=snap.id, symbol="BYMA", asset_type="ACCIONES",
            instrument_type="ACCIONES", currency="ARS", quantity=50,
            market_value=15_000, avg_price=290, pnl_pct=0.0,
        ))
    db.flush()
    return snap


def _catalog(db, symbol="BYMA", asset_type="ACCIONES", currency="ARS", **extra):
    kwargs = dict(
        broker_symbol=symbol, asset_type=asset_type, instrument_type=asset_type,
        currency=currency, market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=1.0, minimum_quantity=1.0,
        buy_supported=True, sell_supported=True, quote_supported=True,
        max_age_seconds=86400,
        # Verified provenance: these tests are about execution, not about
        # the verification lifecycle (which has its own tests).
        provenance=verified_provenance("fund" if asset_type == "FondoComundeInversion"
                                       else "securities"),
    )
    kwargs.update(extra)
    upsert_instrument(db, **kwargs)
    db.commit()


def _catalog_price(db, symbol="BYMA", price=300.0):
    """Discovery-catalog reference price for an instrument we do not hold."""
    db.add(InstrumentCatalog(
        symbol=symbol, name=symbol, asset_type="ACCIONES", market="BCBA",
        currency="ARS", tradable=True, source="test", is_active=True,
        last_price=price,
    ))
    db.commit()


def _recommendation(db, symbol="BYMA", target_pct=0.01):
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.01, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol=symbol,
        target_change_pct=target_pct, reason="test",
    ))
    db.commit()
    return rec


def _broker(*, cash=100_000.0, cash_currency="ARS", ask=300.0, positions=None,
            cash_available=True):
    b = mock.MagicMock()
    b.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0,
        "cash": cash,
        "positions": positions if positions is not None else [],
    }
    b.get_live_cash.return_value = {
        "available": cash_available, "cash": cash if cash_available else None,
        "currency": cash_currency, "committed": 0.0,
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "source": "estadocuenta",
    }
    b.get_execution_quote.return_value = {
        "available": True, "price": ask, "source": "ask",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }
    b.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "SBX-1",
        "endpoint_used": "/api/v2/operar/Comprar", "raw_response": {"numeroOperacion": "SBX-1"},
        "error": "",
    }
    return b


def _approve(db, rec, preview, broker):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        return approve_and_execute(
            db, rec.id,
            execution_key=TEST_ADMIN_KEY,
            preview_hash=preview["preview_hash"],
            preview_generated_at=preview["generated_at"],
            confirmation_text=confirmation_phrase(rec.id),
        )


# ───────────────────────────────────────────────────────────────────
# 1 — buy an instrument we do NOT hold
# ───────────────────────────────────────────────────────────────────


def test_buy_preview_works_without_any_position(db):
    """No position must be required just to obtain a price."""
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["side"] == "buy"
    assert order["valid"] is True, order["blocked_reason"]
    assert order["price_ref_source"] == "instrument_catalog"
    assert order["quantity_planned"] > 0
    assert "invalid_order" not in preview["blocking_reasons"]


def test_buy_without_position_and_without_catalog_price_is_blocked(db):
    """No reference price at all → blocked, never an invented avg_price."""
    _snapshot(db)
    _catalog(db)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["valid"] is False
    assert "No price reference" in order["blocked_reason"]
    assert order["price_ref_source"] is None


def test_valid_buy_reaches_submission_with_the_ask_price(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(ask=305.0)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved", result
    sent = broker.submit_order_request.call_args[0][0]
    assert sent["endpoint"] == "/api/v2/operar/Comprar"
    assert sent["form_data"]["precio"] == "305"
    assert sent["form_data"]["tipoOrden"] == "precioLimite"
    # The quote was asked for the BUY side, i.e. the ask.
    assert broker.get_execution_quote.call_args[0][1] == "buy"


def test_buy_uses_the_ask_never_the_bid(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()
    broker.get_execution_quote.return_value = {
        "available": True, "price": 299.0, "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert db.query(OrderExecution).first().status == "failed"


# ───────────────────────────────────────────────────────────────────
# 2 — live cash preflight
# ───────────────────────────────────────────────────────────────────


def test_live_cash_is_read_immediately_before_submitting(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)

    broker.get_live_cash.assert_called_with("ARS")
    audit = (db.query(OrderExecution).first().broker_response or {})["request_audit"]
    assert audit["live_cash_check"]["passed"] is True


def test_insufficient_live_cash_blocks_and_sends_nothing(db):
    """snapshot.cash is generous, the real balance is not."""
    _snapshot(db, cash=1_000_000.0)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(cash=10.0)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "insufficient_live_cash" in result["message"]
    order = db.query(OrderExecution).first()
    assert order.status == "failed"
    assert order.quantity_sent is None


def test_unreadable_live_balance_blocks(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(cash_available=False)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "live_cash_unavailable" in result["message"]


def test_currency_mismatch_blocks(db):
    """Pesos and dollars are never interchangeable."""
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(cash_currency="USD")

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "currency_cash_mismatch" in result["message"]


def test_cash_that_shrinks_between_preview_and_approval_blocks(db):
    """The preview is not a reservation."""
    _snapshot(db, cash=100_000.0)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        assert preview["orders_preview"][0]["valid"] is True
        # Between review and approval the account was drained elsewhere.
        broker = _broker(cash=5.0)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "insufficient_live_cash" in result["message"]


def test_fee_buffer_and_reserve_are_subtracted(db):
    """Commissions, VAT and market fees are covered by a deliberate
    over-estimate, and the reserve is preserved."""
    policy = {**CLASS_POLICY, "fee_buffer_pct": 0.10, "min_cash_reserve": 5_000.0}
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    # notional ≈ 10_000 → with 10% fees = 11_000, + 5_000 reserve = 16_000.
    broker = _broker(cash=15_000.0)

    with exec_settings(**_settings(execution_class_policies={
        CLASS_ACCIONES: policy, CLASS_CEDEARS: policy,
    })):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "insufficient_live_cash" in result["message"]


def test_evaluate_buy_cash_accounts_for_pending_buys():
    """An order already at the broker is money no longer freely available."""
    error, audit = evaluate_buy_cash(
        live_cash={"available": True, "cash": 1000.0, "currency": "ARS"},
        required_notional=Decimal("600"),
        currency="ARS",
        fee_buffer_pct=0.0,
        min_cash_reserve=0.0,
        pending_notional=Decimal("500"),
    )
    assert error == "insufficient_live_cash"
    assert audit["pending_buy_notional"] == 500.0


def test_evaluate_buy_cash_passes_when_everything_fits():
    error, audit = evaluate_buy_cash(
        live_cash={"available": True, "cash": 1000.0, "currency": "ARS"},
        required_notional=Decimal("500"),
        currency="ARS",
        fee_buffer_pct=0.02,
        min_cash_reserve=100.0,
        pending_notional=Decimal("0"),
    )
    assert error is None
    assert audit["total_needed"] == 610.0


# ───────────────────────────────────────────────────────────────────
# 3 — buys stay off unless explicitly enabled
# ───────────────────────────────────────────────────────────────────


def test_buy_is_blocked_with_the_flag_off(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()

    with exec_settings(**_settings(securities_buy_enabled=False)):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert "buy_execution_disabled" in preview["blocking_reasons"]
    broker.submit_order_request.assert_not_called()
    assert result["status_code"] in (422, 423)
    assert db.query(OrderExecution).count() == 0


def test_deprecated_sell_only_still_forces_buys_off(db):
    """EXECUTION_SELL_ONLY=true keeps blocking buys even with the new flag."""
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)

    with exec_settings(**_settings(execution_sell_only=True, securities_buy_enabled=True)):
        preview = build_execution_preview(db, rec.id)

    assert "buy_execution_disabled" in preview["blocking_reasons"]


def test_class_level_buy_disable_blocks_even_with_the_global_flag(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    policy = {**CLASS_POLICY, "buy_enabled": False}

    with exec_settings(**_settings(execution_class_policies={
        CLASS_ACCIONES: policy, CLASS_CEDEARS: policy,
    })):
        preview = build_execution_preview(db, rec.id)

    assert "class_buy_disabled" in preview["blocking_reasons"]


# ───────────────────────────────────────────────────────────────────
# 4 — sizing guards on the buy path
# ───────────────────────────────────────────────────────────────────


def test_quantity_step_mismatch_blocks(db):
    _snapshot(db)
    _catalog(db, quantity_step=7.0)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    assert "quantity_step_mismatch" in preview["blocking_reasons"]


def test_price_tick_mismatch_blocks_before_sending(db):
    """IOL rejects incompatible decimals — we never round to fit."""
    _snapshot(db)
    _catalog(db, price_tick=0.5)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(ask=300.33)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "price_tick_mismatch" in result["message"]


def test_stale_quote_blocks_the_buy(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()
    broker.get_execution_quote.return_value = {
        "available": True, "price": 300.0, "source": "ask",
        "retrieved_at": "2020-01-01T00:00:00+00:00",
        "market": "bCBA", "settlement": "t1",
    }

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "quote_stale" in result["message"]


def test_market_closed_blocks_the_whole_batch(db):
    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()

    closed = {"open": False, "code": "market_closed", "reason": "outside_session",
              "timezone": "America/Argentina/Buenos_Aires", "open_time": "10:30",
              "close_time": "17:00", "deprecated_settings_in_use": False}
    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution.market_session_state", return_value=closed):
            result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    broker.get_execution_quote.assert_not_called()
    # Refused at authorization time: the rebuilt preview carries market_closed,
    # so approve never even reaches the batch preflight.
    assert result["code"] == "market_closed"
    assert db.query(OrderExecution).count() == 0


def test_market_closed_cancels_the_batch_if_it_reaches_preflight(db):
    """Defence in depth: even if authorization passed, the batch preflight
    re-checks the session before any quote or POST."""
    from app.services.execution import prepare_validated_execution_batch

    _snapshot(db)
    _catalog(db)
    broker = _broker()
    intent = OrderExecution(
        recommendation_id=1, recommendation_action_id=1, symbol="BYMA", side="buy",
        target_change_pct=0.01, status="execution_requested", quantity_planned=1,
        broker_response={},
    )
    db.add(intent)
    db.commit()

    closed = {"open": False, "code": "market_closed", "reason": "holiday",
              "timezone": "America/Argentina/Buenos_Aires", "open_time": "10:30",
              "close_time": "17:00", "deprecated_settings_in_use": False}
    with exec_settings(**_settings()):
        with mock.patch("app.services.execution.market_session_state", return_value=closed):
            prepared, error = prepare_validated_execution_batch(
                db, [intent], {}, broker, get_settings()
            )

    assert prepared is None and error == "market_closed"
    broker.get_execution_quote.assert_not_called()
    broker.submit_order_request.assert_not_called()
    db.refresh(intent)
    assert intent.status == "failed"
    assert intent.quantity_sent is None


def test_daily_notional_limit_blocks_a_later_batch(db):
    """A per-order limit says nothing about the twentieth batch of the day."""
    from app.services.execution_limits import reserve_daily_budget, trade_date_for

    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()

    with exec_settings(**_settings()):
        # Today's budget is already almost entirely consumed.
        reserve_daily_budget(
            db, trade_date=trade_date_for(get_settings()),
            execution_class=CLASS_ACCIONES, currency="ARS",
            notional=Decimal("499000"), max_daily_notional=500_000.0,
        )
        db.commit()
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "daily_limit_exceeded" in result["message"]


def test_daily_budget_is_only_consumed_at_the_point_of_no_return(db):
    """A blocked preflight never eats into the day's allowance."""
    from app.services.execution_limits import get_daily_totals, trade_date_for

    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker(cash=1.0)  # will fail the cash check

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)
        assert get_daily_totals(db, trade_date_for(get_settings()), CLASS_ACCIONES, "ARS") == 0


def test_successful_buy_consumes_the_daily_budget(db):
    """The budget is consumed in the order's OWN currency bucket."""
    from app.services.execution_limits import get_daily_totals, trade_date_for

    _snapshot(db)
    _catalog(db)
    _catalog_price(db, price=300.0)
    rec = _recommendation(db)
    broker = _broker()

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)
        today = trade_date_for(get_settings())
        assert get_daily_totals(db, today, CLASS_ACCIONES, "ARS") > 0
        # A different currency has its own, untouched budget.
        assert get_daily_totals(db, today, CLASS_ACCIONES, "USD") == 0


# ───────────────────────────────────────────────────────────────────
# 5 — cumulative exposure inside one batch
# ───────────────────────────────────────────────────────────────────


def test_two_buys_that_individually_fit_but_together_overdraw_are_blocked(db):
    """The sell path already aggregates per symbol; the buy path must
    aggregate cash the same way. Checking each order against the FULL balance
    would let two orders each pass while together overdrawing the account."""
    _snapshot(db, cash=1_000_000.0)
    _catalog(db, "BYMA")
    _catalog(db, "GGAL")
    _catalog_price(db, "BYMA", price=300.0)
    _catalog_price(db, "GGAL", price=300.0)

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.01, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for symbol in ("BYMA", "GGAL"):
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=symbol,
            target_change_pct=0.01, reason="test",
        ))
    db.commit()

    # Each order is ~10_000; the live balance covers ONE of them, not both.
    broker = _broker(cash=12_000.0)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        assert all(o["valid"] for o in preview["orders_preview"])
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert "insufficient_live_cash" in result["message"]
    orders = db.query(OrderExecution).all()
    assert all(o.quantity_sent is None for o in orders)


def test_two_buys_that_fit_together_are_both_prepared(db):
    """The aggregate check must not be over-eager either."""
    _snapshot(db, cash=1_000_000.0)
    _catalog(db, "BYMA")
    _catalog(db, "GGAL")
    _catalog_price(db, "BYMA", price=300.0)
    _catalog_price(db, "GGAL", price=300.0)

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.01, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for symbol in ("BYMA", "GGAL"):
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=symbol,
            target_change_pct=0.01, reason="test",
        ))
    db.commit()

    broker = _broker(cash=100_000.0)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved", result
    assert broker.submit_order_request.call_count == 2
