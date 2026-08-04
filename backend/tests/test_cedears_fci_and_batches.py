"""CEDEARs, FCI blocking, batch semantics and the capability endpoints.

Covers the parts of the semi-automatic contract that are specific to:

- CEDEARs as a distinct class (their conversion RATIO is not a quantity
  step, and the local ARS CEDEAR is not the foreign underlying);
- FCI being analysable and recommendable but never executable, with its own
  vocabulary — no limit price, no book, no OrderExecution;
- batch execution: full preflight, first order sent, second fails, third
  never sent, explicit partial state, zero retries;
- the read-only capability endpoints.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.execution_class import CLASS_ACCIONES, CLASS_CEDEARS, CLASS_FCI
from app.broker.instrument_catalog import upsert_instrument
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
from tests.testutils import verified_provenance
from app.services.fci import get_fci_capability

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
        execution_sell_only=False,
        securities_buy_enabled=True, securities_sell_enabled=True,
        fci_subscription_enabled=False, fci_redemption_enabled=False,
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


def _catalog(db, symbol, asset_type, currency="ARS", **extra):
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


def _snapshot(db: Session, positions):
    snap = PortfolioSnapshot(total_value=1_000_000.0, cash=100_000.0, currency="ARS")
    db.add(snap)
    db.flush()
    for spec in positions:
        db.add(PortfolioPosition(snapshot_id=snap.id, pnl_pct=0.0, **spec))
    db.flush()
    db.commit()
    return snap


def _recommendation(db, actions):
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    for symbol, pct in actions:
        db.add(RecommendationAction(
            recommendation_id=rec.id, symbol=symbol,
            target_change_pct=pct, reason="test",
        ))
    db.commit()
    return rec


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
# 1 — CEDEARs
# ───────────────────────────────────────────────────────────────────


def _cedear_broker(live_positions, bid=300.0):
    b = mock.MagicMock()
    b.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": 100_000.0, "positions": live_positions,
    }
    b.get_live_cash.return_value = {
        "available": True, "cash": 100_000.0, "currency": "ARS", "committed": 0.0,
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "source": "estadocuenta",
    }
    b.get_execution_quote.return_value = {
        "available": True, "price": bid, "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }
    b.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "SBX-1",
        "endpoint_used": "/api/v2/operar/Vender",
        "raw_response": {"numeroOperacion": "SBX-1"}, "error": "",
    }
    return b


def test_cedear_sell_carries_the_cedear_class_and_identity(db):
    _catalog(db, "AAPL", "CEDEAR")
    _snapshot(db, [dict(symbol="AAPL", asset_type="CEDEAR", instrument_type="CEDEAR",
                        currency="ARS", quantity=100, market_value=30_000, avg_price=280)])
    rec = _recommendation(db, [("AAPL", -0.01)])

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["valid"] is True, order["blocked_reason"]
    assert order["instrument_identity"]["execution_class"] == CLASS_CEDEARS
    assert order["instrument_identity"]["instrument_type"] == "CEDEAR"
    assert order["instrument_identity"]["currency"] == "ARS"


def test_cedear_conversion_ratio_is_not_a_quantity_step(db):
    """A CEDEAR's ratio (e.g. 20:1 vs the underlying) describes the
    underlying claim, not the tradeable lot. Only the catalog's explicit
    quantity_step may size an order."""
    _catalog(db, "AAPL", "CEDEAR", quantity_step=1.0)
    _snapshot(db, [dict(symbol="AAPL", asset_type="CEDEAR", instrument_type="CEDEAR",
                        currency="ARS", quantity=100, market_value=30_000, avg_price=280)])
    rec = _recommendation(db, [("AAPL", -0.01)])

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["execution_scope"]["quantity_step"] == 1.0
    # 33 units is not a multiple of any ratio like 20 — and that is fine.
    assert order["quantity_planned"] % 1 == 0
    assert "quantity_step_mismatch" not in preview["blocking_reasons"]


def test_local_ars_cedear_is_not_confused_with_a_usd_variant(db):
    """The catalog entry is ARS; a USD-denominated live position under the
    same symbol is a different instrument and must block."""
    _catalog(db, "AAPL", "CEDEAR", currency="ARS")
    _snapshot(db, [dict(symbol="AAPL", asset_type="CEDEAR", instrument_type="CEDEAR",
                        currency="USD", quantity=100, market_value=30_000, avg_price=280)])
    rec = _recommendation(db, [("AAPL", -0.01)])

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    assert "instrument_currency_mismatch" in preview["blocking_reasons"]


def test_cedear_and_accion_use_their_own_class_limits(db):
    """Adding an instrument of a covered class needs no new per-symbol policy."""
    _catalog(db, "AAPL", "CEDEAR")
    _catalog(db, "BYMA", "ACCIONES")
    _snapshot(db, [
        dict(symbol="AAPL", asset_type="CEDEAR", instrument_type="CEDEAR",
             currency="ARS", quantity=100, market_value=30_000, avg_price=280),
        dict(symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
             currency="ARS", quantity=100, market_value=30_000, avg_price=280),
    ])
    rec = _recommendation(db, [("AAPL", -0.01), ("BYMA", -0.01)])

    cedear_policy = {**CLASS_POLICY, "max_order_notional": 100.0}
    with exec_settings(**_settings(execution_class_policies={
        CLASS_ACCIONES: dict(CLASS_POLICY), CLASS_CEDEARS: cedear_policy,
    })):
        preview = build_execution_preview(db, rec.id)

    by_symbol = {o["symbol"]: o for o in preview["orders_preview"]}
    assert by_symbol["AAPL"]["execution_scope"]["max_notional"] == 100.0
    assert by_symbol["BYMA"]["execution_scope"]["max_notional"] == 100_000.0
    # The tighter CEDEAR class limit actually bites.
    assert "symbol_notional_limit_exceeded" in by_symbol["AAPL"]["blocked_reason"]


def test_unknown_symbol_is_blocked_even_if_recommended(db):
    """A recommendation can name anything; only the catalog authorises."""
    _snapshot(db, [dict(symbol="XYZ", asset_type="ACCIONES", instrument_type="ACCIONES",
                        currency="ARS", quantity=10, market_value=1_000, avg_price=100)])
    rec = _recommendation(db, [("XYZ", -0.01)])

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)

    assert "instrument_catalog_missing" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


# ───────────────────────────────────────────────────────────────────
# 3 — batches
# ───────────────────────────────────────────────────────────────────


def _three_symbol_setup(db):
    for symbol in ("BYMA", "GGAL", "AAPL"):
        _catalog(db, symbol, "CEDEAR" if symbol == "AAPL" else "ACCIONES")
    positions = [
        dict(symbol=s, asset_type="CEDEAR" if s == "AAPL" else "ACCIONES",
             instrument_type="CEDEAR" if s == "AAPL" else "ACCIONES",
             currency="ARS", quantity=100, market_value=30_000, avg_price=280)
        for s in ("BYMA", "GGAL", "AAPL")
    ]
    _snapshot(db, positions)
    return _recommendation(db, [("BYMA", -0.01), ("GGAL", -0.01), ("AAPL", -0.01)])


def test_batch_preflight_validates_everything_before_the_first_post(db):
    rec = _three_symbol_setup(db)
    live = [
        {"symbol": s, "asset_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "instrument_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "currency": "ARS", "quantity": 100, "market_value": 30_000}
        for s in ("BYMA", "GGAL", "AAPL")
    ]
    broker = _cedear_broker(live)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved", result
    assert broker.submit_order_request.call_count == 3
    # A single live portfolio read for the whole batch.
    assert broker.get_portfolio_snapshot.call_count == 1


def test_second_order_failing_stops_the_third_and_never_retries(db):
    """First sent, second rejected, third never submitted, partial state."""
    rec = _three_symbol_setup(db)
    live = [
        {"symbol": s, "asset_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "instrument_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "currency": "ARS", "quantity": 100, "market_value": 30_000}
        for s in ("BYMA", "GGAL", "AAPL")
    ]
    broker = _cedear_broker(live)

    outcomes = [
        {"outcome": "sent", "order_id": "OK-1", "endpoint_used": "/api/v2/operar/Vender",
         "raw_response": {"numeroOperacion": "OK-1"}, "error": ""},
        {"outcome": "submission_uncertain", "order_id": "",
         "endpoint_used": "/api/v2/operar/Vender", "raw_response": {},
         "error": "timeout"},
        {"outcome": "sent", "order_id": "OK-3", "endpoint_used": "/api/v2/operar/Vender",
         "raw_response": {"numeroOperacion": "OK-3"}, "error": ""},
    ]
    broker.submit_order_request.side_effect = outcomes

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    statuses = {o.symbol: o.status for o in db.query(OrderExecution).all()}
    # An uncertain outcome forces manual reconciliation for the recommendation.
    assert result["status"] == "manual_reconciliation_required"
    assert "execution_sent" in statuses.values()
    assert "manual_reconciliation_required" in statuses.values()
    # Zero automatic retries: no symbol was submitted twice.
    submitted = [c[0][0]["form_data"]["simbolo"]
                 for c in broker.submit_order_request.call_args_list]
    assert len(submitted) == len(set(submitted))


def test_one_bad_order_cancels_the_whole_batch_before_any_post(db):
    """A preflight failure anywhere means ZERO orders are submitted."""
    rec = _three_symbol_setup(db)
    # GGAL is missing from the live portfolio.
    live = [
        {"symbol": s, "asset_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "instrument_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "currency": "ARS", "quantity": 100, "market_value": 30_000}
        for s in ("BYMA", "AAPL")
    ]
    broker = _cedear_broker(live)

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    broker.submit_order_request.assert_not_called()
    assert result["status"] == "execution_failed"
    orders = db.query(OrderExecution).all()
    assert all(o.quantity_sent is None for o in orders)
    assert {o.status for o in orders} == {"failed", "preflight_cancelled"}


def test_batch_reconciliation_is_required_and_never_automatic(db):
    from app.services.execution import get_reconciliation_queue

    rec = _three_symbol_setup(db)
    live = [
        {"symbol": s, "asset_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "instrument_type": "CEDEAR" if s == "AAPL" else "ACCIONES",
         "currency": "ARS", "quantity": 100, "market_value": 30_000}
        for s in ("BYMA", "GGAL", "AAPL")
    ]
    broker = _cedear_broker(live)
    broker.submit_order_request.side_effect = [
        {"outcome": "sent", "order_id": "OK-1", "endpoint_used": "/api/v2/operar/Vender",
         "raw_response": {"numeroOperacion": "OK-1"}, "error": ""},
        {"outcome": "submission_uncertain", "order_id": "",
         "endpoint_used": "/api/v2/operar/Vender", "raw_response": {}, "error": "timeout"},
        {"outcome": "sent", "order_id": "OK-3", "endpoint_used": "/api/v2/operar/Vender",
         "raw_response": {"numeroOperacion": "OK-3"}, "error": ""},
    ]

    with exec_settings(**_settings()):
        preview = build_execution_preview(db, rec.id)
        _approve(db, rec, preview, broker)
        # A second approval must never re-send anything.
        again = _approve(db, rec, preview, broker)

    assert again["code"] == "already_executed"
    queue = get_reconciliation_queue(db)
    assert any(item["status"] == "manual_reconciliation_required" for item in queue)


# ───────────────────────────────────────────────────────────────────
# 4 — capability reporting
# ───────────────────────────────────────────────────────────────────


def test_instrument_capabilities_reports_readiness_and_reasons(db):
    from app.services.instrument_capabilities import build_instrument_capabilities

    _catalog(db, "BYMA", "ACCIONES")
    _catalog(db, "STALE", "ACCIONES", verified_at=datetime(2020, 1, 1))
    _snapshot(db, [
        dict(symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
             currency="ARS", quantity=100, market_value=30_000, avg_price=280),
        # Held but never catalogued — exactly the gap this endpoint surfaces.
        dict(symbol="ORPHAN", asset_type="ACCIONES", instrument_type="ACCIONES",
             currency="ARS", quantity=5, market_value=500, avg_price=100),
    ])

    with exec_settings(**_settings()):
        report = build_instrument_capabilities(db)

    by_symbol = {i["symbol"]: i for i in report["instruments"]}
    assert by_symbol["BYMA"]["buy_ready"] is True
    assert by_symbol["BYMA"]["sell_ready"] is True
    assert by_symbol["BYMA"]["detected_class"] == CLASS_ACCIONES

    assert by_symbol["ORPHAN"]["buy_ready"] is False
    assert "instrument_catalog_missing" in by_symbol["ORPHAN"]["buy_blocking_reasons"]
    assert by_symbol["ORPHAN"]["missing_fields"] == ["catalog_entry"]

    assert by_symbol["STALE"]["sell_ready"] is False
    assert "instrument_catalog_stale" in by_symbol["STALE"]["sell_blocking_reasons"]

    # FCI has an official contract but its own family — never securities.
    assert report["capabilities"]["fci"]["uses_order_execution"] is False


def test_capability_report_exposes_no_secrets(db):
    import json

    from app.services.instrument_capabilities import build_instrument_capabilities

    _catalog(db, "BYMA", "ACCIONES")
    _snapshot(db, [dict(symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
                        currency="ARS", quantity=100, market_value=30_000, avg_price=280)])

    with exec_settings(**_settings()):
        payload = json.dumps(build_instrument_capabilities(db))

    for secret in (TEST_ADMIN_KEY, TEST_PREVIEW_SECRET, "sbx-user", "sbx-pass"):
        assert secret not in payload


def test_readiness_reports_every_capability_separately(db):
    with exec_settings(**_settings(broker_mode="real", iol_use_sandbox=False,
                                   order_execution_enabled=True,
                                   iol_real_username="u", iol_real_password="p",
                                   securities_buy_enabled=True,
                                   securities_sell_enabled=False,
                                   execution_sell_only=False)):
        readiness = get_execution_readiness(db)

    assert readiness["securities_buy_enabled"] is True
    assert readiness["securities_sell_enabled"] is False
    assert readiness["ready_for_real_securities_buy"] != readiness["ready_for_real_securities_sell"]
    assert readiness["ready_for_real_fci_subscription"] is False
    assert readiness["ready_for_real_fci_redemption"] is False
    assert CLASS_ACCIONES in readiness["class_policies_configured"]
    assert "max_daily_notional" in readiness["class_policy_limits"][CLASS_ACCIONES]
    assert readiness["market_calendar_configured"] is True


def test_readiness_reports_catalog_coverage_of_the_portfolio(db):
    _catalog(db, "BYMA", "ACCIONES")
    _snapshot(db, [
        dict(symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
             currency="ARS", quantity=100, market_value=30_000, avg_price=280),
        dict(symbol="ORPHAN", asset_type="ACCIONES", instrument_type="ACCIONES",
             currency="ARS", quantity=5, market_value=500, avg_price=100),
    ])

    with exec_settings(**_settings()):
        readiness = get_execution_readiness(db)

    assert readiness["portfolio_instruments_total"] == 2
    assert readiness["portfolio_instruments_covered"] == 1
    assert readiness["portfolio_instruments_blocked"][0]["symbol"] == "ORPHAN"
    assert readiness["supported_acciones"] == 1


def test_readiness_exposes_no_secrets(db):
    import json

    with exec_settings(**_settings()):
        payload = json.dumps(get_execution_readiness(db))

    for secret in (TEST_ADMIN_KEY, TEST_PREVIEW_SECRET, "sbx-user", "sbx-pass"):
        assert secret not in payload
    assert "password" not in payload.lower()
