"""Preview and approval against the REAL calendar — no global patch.

The suite-wide fixture in conftest forces an open session so ordinary
execution tests do not fail on evenings and weekends. That convenience can
hide a calendar regression: if the gate stopped working, every patched test
would still pass.

These tests opt OUT of that fixture (@pytest.mark.real_market_calendar) and
drive preview + approve through the genuine `market_session_state`, pinning
the clock via a configured schedule and an explicit `now`.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
from tests.testutils import verified_provenance

pytestmark = pytest.mark.real_market_calendar

ART = ZoneInfo("America/Argentina/Buenos_Aires")
TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"

CLASS_POLICY = {
    "buy_enabled": True, "sell_enabled": True,
    "currencies": ["ARS"], "markets": ["bCBA"], "settlements": ["t1"],
    "max_order_notional": 100_000.0, "max_daily_notional": 500_000.0,
    "max_quantity": 1000.0, "max_portfolio_pct": 0.5,
    "min_cash_reserve": 0.0, "fee_buffer_pct": 0.0,
    "max_quote_age_seconds": 15.0, "max_price_deviation_pct": 0.5,
    "catalog_max_age_seconds": 86400.0, "validity_minutes": 10.0,
    "default_quantity_step": 1.0, "default_price_tick": 1.0,
    "order_type": "precioLimite",
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


def _settings(**extra):
    base = dict(
        broker_mode="sandbox", iol_use_sandbox=False,
        order_execution_enabled=False, sandbox_execution_enabled=True,
        iol_sandbox_api_base="https://sandbox.iol-test.local",
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
        execution_instrument_policies={},
        execution_class_policies={CLASS_ACCIONES: dict(CLASS_POLICY),
                                  CLASS_CEDEARS: dict(CLASS_POLICY)},
        execution_instrument_overrides={},
        execution_instrument_ticks={},
        execution_denylist=[],
        execution_allow_override_limit_increase=False,
        # THE REAL CALENDAR — not patched away.
        scheduler_market_open_time="10:30",
        scheduler_market_close_time="17:00",
        scheduler_timezone="America/Argentina/Buenos_Aires",
        market_holidays=["2026-08-05"],
    )
    base.update(extra)
    return base


def _setup(db):
    snap = PortfolioSnapshot(total_value=1_000_000.0, cash=500_000.0, currency="ARS")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="BYMA", asset_type="ACCIONES",
        instrument_type="ACCIONES", currency="ARS", quantity=100,
        market_value=30_000, avg_price=290, pnl_pct=0.0,
    ))
    db.add(InstrumentCatalog(
        symbol="BYMA", name="BYMA", asset_type="ACCIONES", market="BCBA",
        currency="ARS", tradable=True, source="test", is_active=True, last_price=300.0,
    ))
    db.flush()
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=1.0, minimum_quantity=1.0,
        buy_supported=True, sell_supported=True, quote_supported=True,
        max_age_seconds=86400, provenance=verified_provenance(),
    )
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.01, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(recommendation_id=rec.id, symbol="BYMA",
                                target_change_pct=-0.01, reason="test"))
    db.commit()
    return rec


def _broker():
    broker = mock.MagicMock()
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": 500_000.0,
        "positions": [{"symbol": "BYMA", "asset_type": "ACCIONES",
                       "instrument_type": "ACCIONES", "currency": "ARS",
                       "quantity": 100, "market_value": 30_000}],
    }
    broker.get_live_cash.return_value = {
        "available": True, "cash": 500_000.0, "currency": "ARS", "committed": 0.0,
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "source": "estadocuenta",
    }
    broker.get_execution_quote.return_value = {
        "available": True, "price": 300.0, "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }
    broker.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "SBX-1",
        "endpoint_used": "/api/v2/operar/Vender",
        "raw_response": {"numeroOperacion": "SBX-1"}, "error": "",
    }
    return broker


@contextmanager
def _clock(at: datetime):
    """Freeze the calendar's clock, using the REAL session logic."""
    from app.market import calendar as calendar_module

    real = calendar_module.market_session_state

    def _at_fixed_time(settings, *, now=None):
        return real(settings, now=now if now is not None else at)

    with mock.patch.object(calendar_module, "market_session_state", _at_fixed_time), \
         mock.patch("app.services.execution.market_session_state", _at_fixed_time):
        yield


def _approve(db, rec, preview, broker):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        return approve_and_execute(
            db, rec.id, execution_key=TEST_ADMIN_KEY,
            preview_hash=preview["preview_hash"],
            preview_generated_at=preview["generated_at"],
            confirmation_text=confirmation_phrase(rec.id),
        )


# ───────────────────────────────────────────────────────────────────
# Preview + approve driven by the REAL calendar
# ───────────────────────────────────────────────────────────────────


def test_preview_and_approve_succeed_inside_the_session(db):
    """Tuesday 2026-08-04, 12:00 ART — squarely inside 10:30–17:00."""
    rec = _setup(db)
    broker = _broker()
    with exec_settings(**_settings()), _clock(datetime(2026, 8, 4, 12, 0, tzinfo=ART)):
        preview = build_execution_preview(db, rec.id)
        assert preview["market_session"]["open"] is True
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved", result
    assert broker.submit_order_request.call_count == 1


@pytest.mark.parametrize("at,reason", [
    (datetime(2026, 8, 4, 10, 29, tzinfo=ART), "outside_session"),   # before open
    (datetime(2026, 8, 4, 17, 0, tzinfo=ART), "outside_session"),    # at close
    (datetime(2026, 8, 4, 20, 0, tzinfo=ART), "outside_session"),    # evening
    (datetime(2026, 8, 8, 12, 0, tzinfo=ART), "weekend"),            # Saturday
    (datetime(2026, 8, 5, 12, 0, tzinfo=ART), "holiday"),            # configured holiday
])
def test_approval_is_refused_outside_the_real_session(db, at, reason):
    rec = _setup(db)
    broker = _broker()
    with exec_settings(**_settings()), _clock(at):
        preview = build_execution_preview(db, rec.id)
        assert "market_closed" in preview["blocking_reasons"]
        assert preview["market_session"]["reason"] == reason
        result = _approve(db, rec, preview, broker)

    assert result["code"] == "market_closed"
    broker.submit_order_request.assert_not_called()
    assert db.query(OrderExecution).count() == 0


def test_the_session_opens_exactly_at_1030_end_to_end(db):
    """One minute apart, opposite outcomes — with no patched session."""
    rec = _setup(db)
    with exec_settings(**_settings()):
        with _clock(datetime(2026, 8, 4, 10, 29, tzinfo=ART)):
            before = build_execution_preview(db, rec.id)
        with _clock(datetime(2026, 8, 4, 10, 30, tzinfo=ART)):
            after = build_execution_preview(db, rec.id)

    assert "market_closed" in before["blocking_reasons"]
    assert "market_closed" not in after["blocking_reasons"]


def test_an_unresolvable_schedule_blocks_approval(db):
    """A schedule we cannot read must never be treated as "probably open"."""
    rec = _setup(db)
    broker = _broker()
    with exec_settings(**_settings(scheduler_market_open_time="17:00",
                                   scheduler_market_close_time="10:30")):
        with _clock(datetime(2026, 8, 4, 12, 0, tzinfo=ART)):
            preview = build_execution_preview(db, rec.id)
            result = _approve(db, rec, preview, broker)

    assert "market_schedule_unknown" in preview["blocking_reasons"]
    assert result["code"] == "market_schedule_unknown"
    broker.submit_order_request.assert_not_called()


def test_the_global_fixture_is_genuinely_opted_out_here():
    """Guard on the guard: if conftest's patch were still active, the tests
    above would prove nothing."""
    from app.market.calendar import market_session_state
    from app.services import execution

    with exec_settings(**_settings()):
        real = market_session_state(
            get_settings(), now=datetime(2026, 8, 8, 12, 0, tzinfo=ART)
        )
    assert real["open"] is False and real["reason"] == "weekend"
    assert execution.market_session_state is market_session_state
