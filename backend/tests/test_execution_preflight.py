"""Final Execution Preflight V1.

Covers:
- the live position check and sell-only mode being NON-negotiable for
  sandbox/real (a single flag flip blocks everything, before any DB write);
- strict finite numeric validation (NaN / Infinity / negative / non-numeric
  never pass);
- ONE live portfolio read per batch, full preflight of every order before
  the first POST, and batch atomicity: if any order fails preflight, ZERO
  order requests are submitted;
- limits re-validated against the FRESH executable quote (per order, per
  symbol, per portfolio percentage and for the batch total);
- execution_ready as the only submittable state.

No test here touches the real IOL API.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.numeric import non_negative_decimal, positive_decimal, to_finite_decimal
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

POLICY = {
    "asset_type": "CEDEAR", "instrument_type": "CEDEAR", "currency": "USD",
    "market": "bCBA", "settlement": "t1",
    "quantity_step": 1, "price_tick": 0.01, "max_quantity": 100, "max_notional": 1_000_000,
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


def _live(positions=None, total_value=100000, cash=12000):
    if positions is None:
        positions = [{"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
                      "currency": "USD", "quantity": 20, "market_value": 38000}]
    return {"total_value": total_value, "cash": cash, "positions": positions}


def _two_symbol_live(aapl_qty=20, msft_qty=12, total_value=100000):
    return _live(positions=[
        {"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": aapl_qty, "market_value": 38000},
        {"symbol": "MSFT", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": msft_qty, "market_value": 28000},
    ], total_value=total_value)


# Snapshot-derived references: AAPL 38000/20 = 1900, MSFT 28000/12 = 2333.33
_DEFAULT_QUOTES = {"AAPL": 1900.0, "MSFT": 2333.33}


def _broker(live=None, quote_price=None, quotes=None):
    b = mock.MagicMock()
    b.get_portfolio_snapshot.return_value = live if live is not None else _live()

    def quote(symbol, side, market, settlement):
        if quotes and symbol in quotes:
            price = quotes[symbol]
        elif quote_price is not None:
            price = quote_price
        else:
            price = _DEFAULT_QUOTES.get(symbol, 1900.0)
        if price is None:
            return {"available": False, "price": None, "source": "none",
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "market": market, "settlement": settlement}
        return {"available": True, "price": price, "source": "bid",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "market": market, "settlement": settlement}

    b.get_execution_quote.side_effect = quote
    b.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "SBX-1", "raw_response": {"numeroOperacion": "SBX-1"},
        "endpoint_used": "/api/v2/operar/Vender", "error": "",
    }
    return b


def _approve(db, rec, preview, broker):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        return approve_and_execute(
            db, rec.id, execution_key=TEST_ADMIN_KEY,
            preview_hash=preview["preview_hash"],
            preview_generated_at=preview["generated_at"],
            confirmation_text=confirmation_phrase(rec.id),
        )


def _run(db, settings_extra=None, broker=None, symbols=("AAPL",), with_msft=False):
    _make_snapshot(db, with_msft=with_msft)
    rec = _make_recommendation(db, symbols=symbols)
    db.commit()
    b = broker or _broker()
    with exec_settings(**_sandbox_settings(**(settings_extra or {}))):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, b)
    rows = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).all()
    return result, rows, b, preview


# ───────────────────────────────────────────────────────────────────
# Parte 1/2 — live check obligatorio y sell-only estricto (tests 1-7)
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("env_extra", [
    dict(broker_mode="sandbox"),
    dict(broker_mode="real", iol_use_sandbox=False, order_execution_enabled=True,
         iol_real_username="u", iol_real_password="p"),
])
def test_live_check_false_blocks_sandbox_and_real(db, env_extra):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    broker = _broker()
    broker.get_portfolio_snapshot.side_effect = AssertionError("must not be reached")
    with exec_settings(**_sandbox_settings(execution_require_live_position_check=False, **env_extra)):
        preview = build_execution_preview(db, rec.id)
        readiness = get_execution_readiness()
        result = _approve(db, rec, preview, broker)
    assert "live_position_verification_required" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False
    assert "live_position_verification_required" in readiness["blocking_reasons"]
    assert readiness["ready_for_real_execution"] is False
    assert readiness["ready_for_sandbox_execution"] is False
    assert result["status_code"] == 423
    assert result["code"] == "live_position_verification_required"


def test_live_check_false_returns_423_before_any_db_write(db):
    from app.models.models import UserDecision

    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(execution_require_live_position_check=False)):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, _broker())
    assert result["status_code"] == 423
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "pending"
    assert db.query(UserDecision).count() == 0
    assert db.query(OrderExecution).count() == 0


@pytest.mark.parametrize("env_extra", [
    dict(broker_mode="sandbox"),
    dict(broker_mode="real", iol_use_sandbox=False, order_execution_enabled=True,
         iol_real_username="u", iol_real_password="p"),
])
def test_sell_only_false_blocks_sandbox_and_real(db, env_extra):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(execution_sell_only=False, **env_extra)):
        preview = build_execution_preview(db, rec.id)
        readiness = get_execution_readiness()
        result = _approve(db, rec, preview, _broker())
    assert "sell_only_mode_required" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False
    assert "sell_only_mode_required" in readiness["blocking_reasons"]
    assert result["status_code"] == 423
    assert db.query(OrderExecution).count() == 0


def test_buy_remains_blocked_even_with_sell_only_true(db):
    _make_snapshot(db)
    rec = _make_recommendation(db, target_pct=0.05)
    db.commit()
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, _broker())
    assert "buy_execution_disabled" in preview["blocking_reasons"]
    assert result["status_code"] in (422, 423)
    assert db.query(OrderExecution).count() == 0


def test_mock_legacy_exempt_from_v1_policy(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", iol_use_sandbox=False,
                       execution_sell_only=False,
                       execution_require_live_position_check=False,
                       execution_instrument_policies={}):
        result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"


def test_readiness_exposes_v1_invariants():
    with exec_settings(**_sandbox_settings()):
        readiness = get_execution_readiness()
    assert readiness["live_position_check_required"] is True
    assert readiness["sell_only_mode_required"] is True
    assert readiness["batch_preflight_enabled"] is True
    assert readiness["fresh_limit_revalidation_enabled"] is True


# ───────────────────────────────────────────────────────────────────
# Parte 3 — validación numérica finita (tests 8-15)
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), None, "abc", True, False])
def test_numeric_helpers_reject_non_finite(bad):
    assert positive_decimal(bad) is None
    assert non_negative_decimal(bad) is None
    if bad is not None and not isinstance(bad, bool) and bad != "abc":
        assert to_finite_decimal(bad) is None


def test_numeric_helpers_accept_valid_values():
    assert positive_decimal(2) == Decimal("2")
    assert positive_decimal("19730.5") == Decimal("19730.5")
    assert positive_decimal(0) is None
    assert positive_decimal(-1) is None
    assert non_negative_decimal(0) == Decimal("0")


@pytest.mark.parametrize("live_qty", [float("nan"), float("inf"), -5, "abc"])
def test_invalid_live_quantity_blocks(db, live_qty):
    live = _live(positions=[{"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
                             "currency": "USD", "quantity": live_qty, "market_value": 38000}])
    result, rows, broker, _ = _run(db, broker=_broker(live=live))
    assert result["status"] == "execution_failed"
    assert rows[0].status == "failed"
    assert "invalid_live_position_quantity" in rows[0].error_message
    assert rows[0].quantity_sent is None
    broker.submit_order_request.assert_not_called()


@pytest.mark.parametrize("total", [float("nan"), float("inf"), 0, -100])
def test_invalid_live_portfolio_value_blocks(db, total):
    result, rows, broker, _ = _run(db, broker=_broker(live=_live(total_value=total)))
    assert result["status"] == "execution_failed"
    assert rows[0].status == "failed"
    assert "invalid_portfolio_value" in rows[0].error_message
    broker.submit_order_request.assert_not_called()
    broker.get_execution_quote.assert_not_called()


@pytest.mark.parametrize("price", [float("nan"), float("inf"), 0, -10])
def test_invalid_fresh_quote_price_blocks(db, price):
    result, rows, broker, _ = _run(db, broker=_broker(quote_price=price))
    assert result["status"] == "execution_failed"
    assert rows[0].status == "failed"
    assert rows[0].quantity_sent is None
    broker.submit_order_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Parte 4/5 — un solo snapshot live, preflight atómico (tests 16-24)
# ───────────────────────────────────────────────────────────────────


def test_live_portfolio_read_once_for_multiple_orders(db):
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True,
                                   broker=_broker(live=_two_symbol_live()))
    assert result["status"] == "approved"
    assert len(rows) == 2
    assert broker.get_portfolio_snapshot.call_count == 1  # ONE read per batch


def test_all_quotes_and_requests_built_before_first_post(db):
    """Ordering: portfolio → all quotes → all POSTs. No interleaving."""
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"))
    db.commit()
    calls = []
    broker = _broker(live=_two_symbol_live())

    def spy_portfolio():
        calls.append("portfolio")
        return _two_symbol_live()

    def spy_quote(symbol, side, market, settlement):
        calls.append(f"quote:{symbol}")
        return {"available": True, "price": _DEFAULT_QUOTES[symbol], "source": "bid",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "market": market, "settlement": settlement}

    def spy_submit(req):
        calls.append(f"submit:{req['form_data']['simbolo']}")
        # Every order must already be execution_ready or submitting
        statuses = {r.status for r in db.query(OrderExecution).filter(
            OrderExecution.recommendation_id == rec.id).all()}
        assert statuses <= {"execution_ready", "submitting", "execution_sent"}, statuses
        return {"outcome": "sent", "order_id": "S", "raw_response": {"numeroOperacion": "S"},
                "endpoint_used": "/api/v2/operar/Vender", "error": ""}

    broker.get_portfolio_snapshot.side_effect = spy_portfolio
    broker.get_execution_quote.side_effect = spy_quote
    broker.submit_order_request.side_effect = spy_submit

    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved"
    assert calls[0] == "portfolio"
    quote_idx = [i for i, c in enumerate(calls) if c.startswith("quote:")]
    submit_idx = [i for i, c in enumerate(calls) if c.startswith("submit:")]
    assert len(quote_idx) == 2 and len(submit_idx) == 2
    assert max(quote_idx) < min(submit_idx), f"a POST happened before all quotes: {calls}"


def test_all_orders_execution_ready_and_audited_before_first_post(db):
    _make_snapshot(db, with_msft=True)
    rec = _make_recommendation(db, symbols=("AAPL", "MSFT"))
    db.commit()
    observed = {}
    broker = _broker(live=_two_symbol_live())

    def spy_submit(req):
        if "snapshot" not in observed:
            rows = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).all()
            observed["snapshot"] = [(r.status, bool((r.broker_response or {}).get("request_audit", {}).get("iol_request")),
                                     (r.broker_response or {}).get("request_audit", {}).get("batch_preflight"))
                                    for r in rows]
        return {"outcome": "sent", "order_id": "S", "raw_response": {"numeroOperacion": "S"},
                "endpoint_used": "/api/v2/operar/Vender", "error": ""}

    broker.submit_order_request.side_effect = spy_submit
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)

    statuses = [s for s, _, _ in observed["snapshot"]]
    assert all(s in ("execution_ready", "submitting") for s in statuses), statuses
    assert all(has_request for _, has_request, _ in observed["snapshot"])  # audit persisted
    assert all(batch and batch["passed"] for _, _, batch in observed["snapshot"])  # batch total persisted


def test_second_order_failure_produces_zero_posts(db):
    """ATOMICITY: MSFT has no live position → not a single POST is made."""
    live = _live(positions=[{"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
                             "currency": "USD", "quantity": 20, "market_value": 38000}])
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True,
                                   broker=_broker(live=live))
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()
    statuses = {r.symbol: r.status for r in rows}
    assert statuses["MSFT"] == "failed"
    assert statuses["AAPL"] == "preflight_cancelled"
    assert all(r.quantity_sent is None for r in rows)


def test_insufficient_quantity_in_one_order_blocks_whole_batch(db):
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True,
                                   broker=_broker(live=_two_symbol_live(msft_qty=0)))
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()
    assert {r.status for r in rows} == {"failed", "preflight_cancelled"}


def test_missing_quote_in_one_order_blocks_whole_batch(db):
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True,
                                   broker=_broker(live=_two_symbol_live(), quotes={"MSFT": None}))
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()


def test_stale_quote_in_one_order_blocks_whole_batch(db):
    broker = _broker(live=_two_symbol_live())

    def stale_for_msft(symbol, side, market, settlement):
        ts = datetime.now(timezone.utc)
        if symbol == "MSFT":
            ts -= timedelta(seconds=600)
        return {"available": True, "price": 1900.0, "source": "bid",
                "retrieved_at": ts.isoformat(), "market": market, "settlement": settlement}

    broker.get_execution_quote.side_effect = stale_for_msft
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True, broker=broker)
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()


def test_price_deviation_in_one_order_blocks_whole_batch(db):
    result, rows, broker, _ = _run(
        db, symbols=("AAPL", "MSFT"), with_msft=True,
        broker=_broker(live=_two_symbol_live(), quotes={"AAPL": 1900.0, "MSFT": 9999.0}),
    )
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Parte 6/7/8 — límites revalidados con precio fresco (tests 25-32)
# ───────────────────────────────────────────────────────────────────


def test_fresh_symbol_notional_limit_blocks(db):
    """Snapshot notional passes the policy, the FRESH one does not."""
    result, rows, broker, _ = _run(db, broker=_broker(quote_price=1950.0), settings_extra={
        "execution_instrument_policies": {"AAPL": {**POLICY, "max_notional": 3850}},
    })
    assert result["status"] == "execution_failed"
    assert "fresh_symbol_notional_limit_exceeded" in rows[0].error_message
    broker.submit_order_request.assert_not_called()


def test_fresh_order_limit_blocks(db):
    result, rows, broker, _ = _run(db, broker=_broker(quote_price=1950.0),
                                   settings_extra={"execution_max_order_value": 3850})
    assert result["status"] == "execution_failed"
    assert "fresh_order_limit_exceeded" in rows[0].error_message
    broker.submit_order_request.assert_not_called()


def test_fresh_total_limit_blocks_entire_batch(db):
    """Snapshot total (3800 + 4666.66 = 8466.66) passes the 8600 cap, but the
    FRESH total (3990 + 4900 = 8890) does not → zero POSTs."""
    result, rows, broker, _ = _run(
        db, symbols=("AAPL", "MSFT"), with_msft=True,
        broker=_broker(live=_two_symbol_live(), quotes={"AAPL": 1995.0, "MSFT": 2450.0}),
        settings_extra={"execution_max_total_value": 8600},
    )
    assert result["status"] == "execution_failed"
    broker.submit_order_request.assert_not_called()
    assert all("fresh_total_limit_exceeded" in (r.error_message or "") for r in rows)


def test_fresh_portfolio_pct_limit_blocks(db):
    """Snapshot pct 0.038 passes the 0.0385 cap; the fresh one (0.039) does not."""
    result, rows, broker, _ = _run(db, broker=_broker(quote_price=1950.0),
                                   settings_extra={"execution_max_portfolio_pct": 0.0385})
    assert result["status"] == "execution_failed"
    assert "fresh_portfolio_pct_limit_exceeded" in rows[0].error_message
    broker.submit_order_request.assert_not_called()


def test_fresh_price_within_limits_proceeds_and_audits(db):
    result, rows, broker, preview = _run(db, broker=_broker(quote_price=1950.0))
    assert result["status"] == "approved"
    audit = rows[0].broker_response["request_audit"]
    # 2 × 1950 = 3900.00, over a live portfolio of 100000 → 0.039
    assert audit["actual_notional"] == "3900.00"
    assert audit["fresh_portfolio_pct"] == "0.039000"
    assert audit["live_portfolio_value_used"] == "100000.00"
    assert audit["batch_preflight"]["total_actual_notional"] == "3900.00"
    assert audit["batch_preflight"]["passed"] is True
    # Signed snapshot notional stays available for comparison
    assert audit["estimated_notional"] == preview["orders_preview"][0]["estimated_notional"]


def test_quantity_sent_is_exactly_signed_and_never_resized(db):
    # Bigger live position must NOT grow the order
    result_big, rows_big, broker_big, preview = _run(db, broker=_broker(live=_live(positions=[
        {"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": 500, "market_value": 38000}])))
    signed = preview["orders_preview"][0]["quantity_planned"]
    assert result_big["status"] == "approved"
    assert rows_big[0].quantity_sent == signed == 2
    assert broker_big.submit_order_request.call_args[0][0]["form_data"]["cantidad"] == "2"


def test_smaller_live_position_blocks_instead_of_shrinking(db):
    result, rows, broker, _ = _run(db, broker=_broker(live=_live(positions=[
        {"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "USD", "quantity": 1, "market_value": 1900}])))
    assert result["status"] == "execution_failed"
    assert "live_position_insufficient" in rows[0].error_message
    assert rows[0].quantity_sent is None
    broker.submit_order_request.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Parte 9 — envío y durabilidad (tests 33-39)
# ───────────────────────────────────────────────────────────────────


def test_one_post_per_order_and_second_approval_makes_none(db):
    result, rows, broker, _ = _run(db, symbols=("AAPL", "MSFT"), with_msft=True,
                                   broker=_broker(live=_two_symbol_live()))
    assert result["status"] == "approved"
    assert broker.submit_order_request.call_count == 2  # one per order, exactly
    with exec_settings(**_sandbox_settings()):
        second = approve_and_execute(db, rows[0].recommendation_id)
    assert second["status_code"] == 409
    assert broker.submit_order_request.call_count == 2


def test_timeout_still_goes_to_manual_reconciliation(db):
    broker = _broker()
    broker.submit_order_request.return_value = {
        "outcome": "submission_uncertain", "order_id": "", "raw_response": {},
        "endpoint_used": "/api/v2/operar/Vender", "error": "read timeout",
    }
    result, rows, broker, _ = _run(db, broker=broker)
    assert result["status"] == "manual_reconciliation_required"
    assert rows[0].status == "manual_reconciliation_required"
    assert rows[0].quantity_sent == 2  # it may have reached IOL


def test_preflight_failures_never_become_manual_reconciliation(db):
    for broker in (
        _broker(live=_live(positions=[])),
        _broker(live=_live(total_value=float("nan"))),
        _broker(quote_price=float("inf")),
    ):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=engine)
        session = sessionmaker(bind=engine)()
        try:
            result, rows, b, _ = _run(session, broker=broker)
            assert result["status"] == "execution_failed"
            assert all(r.status in ("failed", "preflight_cancelled") for r in rows)
            assert all(r.status != "manual_reconciliation_required" for r in rows)
            b.submit_order_request.assert_not_called()
        finally:
            session.close()


def test_execution_ready_is_not_resubmittable(db):
    """A crash leaving an order execution_ready must not be auto-resent."""
    from app.services.execution import _NO_RESUBMIT_STATUSES

    assert "execution_ready" in _NO_RESUBMIT_STATUSES

    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.flush()
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).first()
    rec.status = "execution_pending"
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id,
        symbol="AAPL", side="sell", target_change_pct=-0.05,
        status="execution_ready", validation_status="passed",
        quantity_planned=2, quantity=2, quantity_sent=None,
    ))
    db.commit()
    broker = _broker()
    with exec_settings(**_sandbox_settings()):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 409
    broker.submit_order_request.assert_not_called()


def test_audit_has_no_credentials(db):
    result, rows, broker, _ = _run(db)
    serialized = json.dumps(rows[0].broker_response)
    for secret in ("sbx-user", "sbx-pass", TEST_ADMIN_KEY, TEST_PREVIEW_SECRET,
                   "Bearer", "Authorization", "password"):
        assert secret not in serialized
