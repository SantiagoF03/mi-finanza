"""Activation hotfix: buy pilots, readiness split, lazy broker, FCI lifecycle.

Three claims this file exists to defend:

1. **You prepare with the locks shut.** `technically_ready` is about the
   instrument; `activation_ready` is about permission. Merging them makes the
   system unpreparable, because the correct resting state — every flag false —
   then reads as "not ready" and an operator cannot tell a missing tick from a
   closed padlock.

2. **Refusing costs nothing.** A blocked request must not instantiate a
   broker, must not reach the network, and must not consume daily budget. Most
   of these tests assert on the broker-factory call count or on the ledger,
   not just on the status code.

3. **"Never sent" is provable, not guessed.** Building a request and sending it
   are separate steps with separate failure modes: a serialization bug is a
   local error, and only a failure after the send begins is ambiguous.

Nothing here touches IOL. Every broker is a double or an httpx.MockTransport
against a fake host.
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
    FundBudgetReservation,
    FundInstrument,
    FundInstrumentVerification,
    FundOperation,
    InstrumentCatalog,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services import fci as fci_service
from app.services.execution_limits import trade_date_for

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
EXEC_HEADERS = {"X-Execution-Key": TEST_ADMIN_KEY}

# Deliberately NOT BYMA. BYMA has a legacy per-symbol policy that short-cuts
# the class path, so a green BYMA test would prove the legacy bridge works,
# not that ACCIONES-by-class works.
PILOT_ACCION = "GGAL"
PILOT_CEDEAR = "AAPL"


# ───────────────────────────────────────────────────────────────────
# fixtures
# ───────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/hotfix.db", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture
def client(db):
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


CLASS_POLICIES = {
    "ACCIONES": {
        "buy_enabled": True, "sell_enabled": True, "currencies": ["ARS"],
        "markets": ["bCBA"], "settlements": ["t1"], "max_order_notional": 5000,
        "max_daily_notional": 20000, "max_quantity": 10, "max_portfolio_pct": 0.5,
        "min_cash_reserve": 0, "fee_buffer_pct": 0.01,
        "max_quote_age_seconds": 300, "max_price_deviation_pct": 0.05,
        "catalog_max_age_seconds": 86400, "validity_minutes": 10,
        "default_quantity_step": None, "default_price_tick": None,
        "order_type": "precioLimite",
    },
    "CEDEARS": {
        "buy_enabled": True, "sell_enabled": True, "currencies": ["ARS"],
        "markets": ["bCBA"], "settlements": ["t1"], "max_order_notional": 5000,
        "max_daily_notional": 20000, "max_quantity": 10, "max_portfolio_pct": 0.5,
        "min_cash_reserve": 0, "fee_buffer_pct": 0.01,
        "max_quote_age_seconds": 300, "max_price_deviation_pct": 0.05,
        "catalog_max_age_seconds": 86400, "validity_minutes": 10,
        "default_quantity_step": None, "default_price_tick": None,
        "order_type": "precioLimite",
    },
}


def _prepared(**extra):
    """Settings for PREPARING a pilot: every execution lock SHUT.

    That is the point: preparation must work here, or the system can only ever
    be prepared in a state we refuse to run production in.
    """
    base = dict(
        api_key="",
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_execution_enabled=False,
        securities_buy_enabled=False,
        securities_sell_enabled=False,
        execution_sell_only=False,
        execution_pilot_creation_enabled=True,
        execution_class_policies=CLASS_POLICIES,
        execution_instrument_overrides={},
        execution_instrument_policies={},
        execution_denylist=[],
        iol_order_market="bCBA",
        iol_order_settlement="t1",
        broker_mode="mock",
        scheduler_timezone="America/Argentina/Buenos_Aires",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _instrument(db, symbol=PILOT_ACCION, *, execution_class="ACCIONES",
                tick=0.01, step=1.0, buy=True, sell=True, minimum=1.0):
    entry = ExecutionInstrument(
        broker_symbol=symbol, display_symbol=symbol, description=symbol,
        country="argentina", market="bCBA", settlement="t1",
        asset_type=execution_class, instrument_type=execution_class,
        execution_family="securities", execution_class=execution_class,
        currency="ARS", quantity_step=step, price_tick=tick,
        minimum_quantity=minimum, active=True,
        buy_supported=buy, sell_supported=sell, quote_supported=True,
        verification_status="verified",
        field_provenance={
            "broker_symbol": "iol_portfolio", "market": "iol_portfolio",
            "asset_type": "iol_portfolio", "instrument_type": "iol_portfolio",
            "currency": "iol_portfolio", "settlement": "iol_portfolio",
            "quote_supported": "iol_quote",
            **({"buy_supported": "iol_quote"} if buy else {}),
            **({"sell_supported": "iol_quote"} if sell else {}),
            **({"price_tick": "admin_verified_override"} if tick is not None else {}),
            **({"quantity_step": "admin_verified_override"} if step is not None else {}),
        },
        source="iol_portfolio", verified_at=datetime.utcnow(),
        stale_after=datetime.utcnow() + timedelta(days=1),
        raw_identity={"last_price": 100.0},
    )
    db.add(entry)
    # The discovery catalog is where a BUY gets its price REFERENCE from when
    # we hold nothing — which is the whole point of a buy pilot. Without it
    # the plan cannot be sized, and "no position" would silently become the
    # blocker again.
    db.add(InstrumentCatalog(
        symbol=symbol, name=symbol, asset_type=execution_class, market="bCBA",
        currency="ARS", is_active=True, last_price=100.0,
    ))
    db.commit()
    return entry


def _snapshot(db, *, positions=(), cash=500_000.0):
    snap = PortfolioSnapshot(total_value=1_000_000.0, cash=cash)
    db.add(snap)
    db.flush()
    for symbol, quantity, price in positions:
        db.add(PortfolioPosition(
            snapshot_id=snap.id, symbol=symbol, quantity=quantity,
            avg_price=price, market_value=quantity * price, currency="ARS",
            asset_type="ACCIONES", instrument_type="ACCIONES", pnl_pct=0.0,
        ))
    db.commit()
    return snap


def _securities_broker(*, bid=100.0, ask=101.0, held=10, cash=500_000.0,
                       symbol=PILOT_ACCION):
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
        "available": True, "cash": cash, "currency": "ARS",
        "committed": 0.0, "source": "estadocuenta",
    }
    positions = []
    if held:
        positions.append({
            "symbol": symbol, "asset_type": "ACCIONES",
            "instrument_type": "ACCIONES", "currency": "ARS",
            "quantity": held, "committed": 0,
            "market_value": held * (bid or 0),
        })
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 1_000_000.0, "cash": cash, "positions": positions,
    }
    return broker


class _CountingFactory:
    """A broker factory that records how many times it was asked to build one.

    The count is the assertion: "blocked" has to mean zero instantiations, not
    just a 4xx after the client was already built and authenticated.
    """

    def __init__(self, broker=None):
        self.calls = 0
        self.broker = broker or mock.MagicMock()

    def __call__(self):
        self.calls += 1
        return self.broker


def _pilot_payload(symbol=PILOT_ACCION, side="sell", quantity=1.0, **extra):
    from app.services.execution_pilot import securities_pilot_phrase

    payload = {
        "symbol": symbol, "side": side, "quantity": quantity,
        "confirmation_text": securities_pilot_phrase(symbol, side, quantity),
        "note": "piloto controlado",
    }
    payload.update(extra)
    return payload


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 1 — quantity_override on BOTH sides
# ═══════════════════════════════════════════════════════════════════


def _pilot_rec(db, *, symbol, side, quantity, pilot=True, reference_price=100.0):
    """A generic securities pilot, as the creator actually writes one.

    `exact_notional_at_creation` is part of that contract: the creator runs a
    live readiness probe and persists what it saw, and the preview prices
    itself from that instead of the discovery catalog. A generic pilot without
    it now fails closed — covered directly in
    tests/test_pilot_reference_price.py.
    """
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.0, confidence=1.0,
        rationale="piloto", risks="", executive_summary="piloto",
        metadata_json=({
            "execution_pilot": True, "pilot_type": f"security_{side}",
            "symbol": symbol, "side": side, "quantity": quantity,
            "exact_notional_at_creation": quantity * reference_price,
        } if pilot else {"execution_pilot": False}),
    )
    db.add(rec)
    db.flush()
    action = RecommendationAction(
        recommendation_id=rec.id, symbol=symbol,
        target_change_pct=(0.0001 if side == "buy" else -0.0001),
        reason="piloto", quantity_override=quantity,
    )
    db.add(action)
    db.commit()
    return rec, action


def test_a_buy_pilot_with_an_override_produces_a_valid_plan(db):
    """Test 1. The override used to be refused outright for buys."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=())
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="buy", quantity=1)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert len(orders) == 1
    assert orders[0]["side"] == "buy"
    assert orders[0]["valid"] is True, orders[0]["blocked_reason"]
    assert orders[0]["quantity_planned"] == 1
    assert orders[0]["quantity_override"] == 1


def test_a_sell_pilot_with_an_override_produces_a_valid_plan(db):
    """Test 2. The path that already worked must keep working."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="sell", quantity=1)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["side"] == "sell"
    assert orders[0]["valid"] is True, orders[0]["blocked_reason"]
    assert orders[0]["quantity_planned"] == 1


def test_a_buy_pilot_does_not_require_a_prior_position(db):
    """Test 3. Buying something we already hold is not the interesting case."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_CEDEAR, execution_class="CEDEARS")
    # A snapshot with NO position in the symbol at all.
    snapshot = _snapshot(db, positions=((PILOT_ACCION, 5, 100.0),))
    rec, action = _pilot_rec(db, symbol=PILOT_CEDEAR, side="buy", quantity=1)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["valid"] is True, orders[0]["blocked_reason"]
    # Priced from the pilot's OWN live observation, not the discovery
    # catalog: the catalog's last price can be hours old and from another
    # session, which is what made the deviation guard compare two unrelated
    # prices. The claim this test makes — a buy needs no prior position —
    # is unchanged.
    assert orders[0]["price_ref_source"] == "pilot_live_quote_at_creation"


def test_a_sell_pilot_without_a_position_is_blocked(db):
    """Test 6 (sell side). Short selling is not a thing this system does."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=())
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="sell", quantity=1)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["valid"] is False
    assert "no position" in orders[0]["blocked_reason"].lower() or \
           "position" in orders[0]["blocked_reason"].lower()


def test_a_non_pilot_recommendation_never_honours_an_override(db):
    """Test 9. An automatic recommendation must not bypass the sizing."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="sell", quantity=1,
                             pilot=False)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["valid"] is False
    assert "execution-pilot" in orders[0]["blocked_reason"]


def test_a_pilot_whose_declared_side_contradicts_the_plan_is_blocked(db):
    """A pilot that says buy while the plan computes sell executes neither."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="buy", quantity=1)
    # Metadata says buy; the action's negative pct computes a sell.
    action.target_change_pct = -0.5
    db.commit()

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["valid"] is False
    # The reference check catches this first now, and more precisely: the
    # stored notional was observed on ONE side of the book, so a metadata side
    # that contradicts the computed one makes the reference unusable. Either
    # way the order is refused — that is what this test is about.
    assert (
        "side" in orders[0]["blocked_reason"]
        or orders[0]["pilot_reference_error"] == "pilot_reference_identity_mismatch"
    )


def test_an_override_that_exceeds_the_snapshot_position_is_blocked(db):
    """Test 4 (step/limit family): the override never resizes itself."""
    from app.services.execution import _build_order_previews

    _instrument(db, PILOT_ACCION)
    snapshot = _snapshot(db, positions=((PILOT_ACCION, 1, 100.0),))
    rec, action = _pilot_rec(db, symbol=PILOT_ACCION, side="sell", quantity=5)

    orders = _build_order_previews([action], snapshot, rec, db)

    assert orders[0]["valid"] is False
    assert "exceeds the snapshot position" in orders[0]["blocked_reason"]


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 2 — technical vs activation
# ═══════════════════════════════════════════════════════════════════


def test_technical_readiness_can_be_true_with_every_flag_off(db):
    """Test 6. The whole point of the split.

    This is the state an operator must be able to reach BEFORE opening
    anything: the instrument is correct, and we are simply not allowed to
    send yet.
    """
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker()
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="sell", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    sell = report["symbols"][0]["sell"]
    assert sell["technically_ready"] is True, sell["technical_blocking_reasons"]
    assert sell["technical_blocking_reasons"] == []
    assert sell["activation_ready"] is False
    assert "execution_locked" in sell["activation_blocking_reasons"]
    assert "sell_execution_disabled" in sell["activation_blocking_reasons"]
    assert sell["ready_for_real_execution"] is False


def test_activation_blockers_never_appear_among_technical_ones(db):
    """Test 7. A flag is not a fact about the instrument."""
    from app.services.pilot_readiness import ACTIVATION_CODES, evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker()
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="buy", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    for side in ("buy", "sell"):
        block = report["symbols"][0][side]
        assert not (set(block["technical_blocking_reasons"]) & ACTIVATION_CODES)


def test_opening_the_flags_flips_activation_but_not_technical(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker()
    with exec_settings(**_prepared(order_execution_enabled=True,
                                   securities_sell_enabled=True)) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="sell", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    sell = report["symbols"][0]["sell"]
    assert sell["technically_ready"] is True
    assert sell["activation_ready"] is True
    assert sell["activation_blocking_reasons"] == []


def test_execution_sell_only_blocks_buy_as_activation_not_technical(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker()
    with exec_settings(**_prepared(execution_sell_only=True,
                                   order_execution_enabled=True,
                                   securities_buy_enabled=True)) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="buy", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    buy = report["symbols"][0]["buy"]
    assert buy["technically_ready"] is True, buy["technical_blocking_reasons"]
    assert "execution_sell_only_blocks_buy" in buy["activation_blocking_reasons"]


def test_creating_a_pilot_does_not_require_the_execution_lock_open(db, client):
    """Test 8's other half: preparing is allowed while sending is not."""
    _instrument(db, PILOT_ACCION)
    _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    factory = _CountingFactory(_securities_broker())

    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    assert response.status_code == 200, response.text
    assert response.json()["order_execution_enabled"] is False
    assert response.json()["approved"] is False


def test_approval_is_still_blocked_with_the_execution_lock_shut(db, client):
    """Test 8. Preparing is not approving."""
    _instrument(db, PILOT_ACCION)
    _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    factory = _CountingFactory(_securities_broker())

    with exec_settings(**_prepared(broker_mode="real")):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            created = client.post("/api/execution-pilot/securities",
                                  json=_pilot_payload(), headers=EXEC_HEADERS)
            assert created.status_code == 200, created.text
            rec_id = created.json()["recommendation_id"]
            approved = client.post(
                f"/api/recommendations/{rec_id}/approve",
                json={"note": "x", "preview_hash": "", "preview_generated_at": "",
                      "confirmation_text": ""},
                headers=EXEC_HEADERS,
            )

    assert approved.status_code >= 400
    rec = db.get(Recommendation, rec_id)
    db.refresh(rec)
    assert rec.status == "pending"


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 3 — live readiness for an exact quantity
# ═══════════════════════════════════════════════════════════════════


def test_live_false_creates_no_broker(db, client):
    """Test 39."""
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.get(f"/api/broker/pilot-readiness?symbols={PILOT_ACCION}")

    assert response.status_code == 200
    assert factory.calls == 0
    body = response.json()
    assert body["live"] is False
    # It does not claim a balance or a holding is available, because it did
    # not look.
    assert body["symbols"][0]["buy"]["technically_ready"] is False
    assert "live_check_not_performed" in \
        body["symbols"][0]["buy"]["technical_blocking_reasons"]


def test_live_true_requires_explicit_symbols(db, client):
    """Test 40."""
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.get(
                "/api/broker/pilot-readiness?live=true&side=sell&quantity=1",
                headers=EXEC_HEADERS,
            )

    assert response.status_code == 422
    assert factory.calls == 0


def test_live_true_rejects_more_than_ten_symbols(db, client):
    """Test 41."""
    from app.services.pilot_readiness import MAX_LIVE_SYMBOLS

    symbols = ",".join(f"SYM{i}" for i in range(MAX_LIVE_SYMBOLS + 1))
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={symbols}"
                "&side=sell&quantity=1",
                headers=EXEC_HEADERS,
            )

    assert response.status_code == 422
    assert factory.calls == 0


def test_live_true_requires_side_quantity_and_the_credential(db, client):
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            no_key = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell&quantity=1"
            )
            no_side = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&quantity=1", headers=EXEC_HEADERS,
            )
            no_qty = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell", headers=EXEC_HEADERS,
            )

    assert no_key.status_code == 403
    assert no_side.status_code == 422
    assert no_qty.status_code == 422
    assert factory.calls == 0


def test_live_true_probes_only_the_requested_side(db, client):
    """Test 42."""
    _instrument(db, PILOT_ACCION)
    broker = _securities_broker()
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell&quantity=1",
                headers=EXEC_HEADERS,
            )

    assert response.status_code == 200, response.text
    probed_sides = {call.args[1] for call in broker.get_execution_quote.call_args_list}
    assert probed_sides == {"sell"}


def test_live_true_does_not_walk_the_whole_catalog(db, client):
    """Test 43."""
    for symbol in (PILOT_ACCION, PILOT_CEDEAR, "OTRO"):
        _instrument(db, symbol)
    broker = _securities_broker()
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell&quantity=1",
                headers=EXEC_HEADERS,
            )

    body = response.json()
    assert [r["symbol"] for r in body["symbols"]] == [PILOT_ACCION]
    probed_symbols = {call.args[0] for call in broker.get_execution_quote.call_args_list}
    assert probed_symbols == {PILOT_ACCION}


def test_a_buy_without_live_cash_is_technically_blocked(db):
    """Test 4."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker(cash=1.0)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="buy", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    buy = report["symbols"][0]["buy"]
    assert buy["technically_ready"] is False
    assert "insufficient_live_cash" in buy["technical_blocking_reasons"]
    assert buy["exact_notional"] == pytest.approx(101.0)


def test_a_sell_without_a_live_position_is_technically_blocked(db):
    """Test 5."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker(held=0)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="sell", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    sell = report["symbols"][0]["sell"]
    assert sell["technically_ready"] is False
    assert "live_position_missing" in sell["technical_blocking_reasons"]


def test_a_sell_larger_than_the_live_position_is_blocked(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker(held=1)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="sell", quantity=5,
            live=True, broker=broker, settings=settings,
        )

    sell = report["symbols"][0]["sell"]
    assert "live_position_insufficient" in sell["technical_blocking_reasons"]
    assert sell["live_check"]["detail"]["available_quantity"] == 1.0


def test_the_exact_notional_is_quantity_times_the_right_side(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION)
    broker = _securities_broker(bid=100.0, ask=101.0)
    with exec_settings(**_prepared()) as settings:
        buy = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="buy", quantity=3,
            live=True, broker=broker, settings=settings,
        )["symbols"][0]["buy"]
        sell = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side="sell", quantity=3,
            live=True, broker=broker, settings=settings,
        )["symbols"][0]["sell"]

    # Buy is priced at the ask, sell at the bid. Never the other way round.
    assert buy["exact_notional"] == pytest.approx(303.0)
    assert sell["exact_notional"] == pytest.approx(300.0)


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 4 — the template must round-trip
# ═══════════════════════════════════════════════════════════════════


def test_the_generated_template_loads_without_errors(db):
    """Tests 11-12. A draft the application rejects is worse than no draft.

    The previous template emitted `default_price_tick: null` into a schema
    that demanded a positive number, and tick/step into an override schema
    with no such fields — so pasting it into production produced
    `class_policy_invalid` and the operator found out afterwards.
    """
    from app.broker.execution_class import load_class_policies, load_instrument_overrides
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, PILOT_ACCION)
    _instrument(db, PILOT_CEDEAR, execution_class="CEDEARS")

    with exec_settings(**_prepared()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    with exec_settings(
        execution_class_policies=template["EXECUTION_CLASS_POLICIES"],
        execution_instrument_overrides=template["EXECUTION_INSTRUMENT_OVERRIDES"],
    ) as loaded:
        policies, policy_errors = load_class_policies(loaded)
        overrides, override_errors = load_instrument_overrides(loaded)

    assert policy_errors == [], policy_errors
    assert override_errors == [], override_errors
    assert set(policies) == {"ACCIONES", "CEDEARS"}


def test_the_template_keeps_tick_and_step_out_of_overrides(db):
    """Test 13. That schema has no such fields; putting them there is what
    made the whole map unloadable."""
    from app.broker.execution_class import OVERRIDABLE_FIELDS
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, PILOT_ACCION, tick=None, step=None)
    with exec_settings(**_prepared()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    for override in template["EXECUTION_INSTRUMENT_OVERRIDES"].values():
        assert set(override) <= set(OVERRIDABLE_FIELDS)
        assert "price_tick" not in override
        assert "quantity_step" not in override

    # They live in a clearly separate, non-variable section instead.
    payload = template["INSTRUMENT_FIELD_VERIFICATION_PAYLOADS"][PILOT_ACCION]
    assert payload["price_tick"] is None
    assert payload["quantity_step"] is None


def test_a_null_class_default_does_not_make_an_instrument_operable(db):
    """A nullable default is not a loophole: it still fails closed."""
    from app.broker.execution_class import validate_class_policy

    policy = dict(CLASS_POLICIES["ACCIONES"])
    resolved, error = validate_class_policy("ACCIONES", policy)
    assert error is None
    assert resolved["default_price_tick"] is None
    assert resolved["default_quantity_step"] is None

    # A wrong number is still worse than no number.
    bad = {**policy, "default_price_tick": 0}
    _, bad_error = validate_class_policy("ACCIONES", bad)
    assert bad_error == "class_policy_invalid"


def test_a_class_max_quantity_never_falls_below_a_members_lot(db):
    """Test 14. A limit of 1 on an instrument whose lot is 100 refuses every
    order it will ever see."""
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, "LOTE", step=100.0)
    with exec_settings(**_prepared()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    block = template["EXECUTION_CLASS_POLICIES"]["ACCIONES"]
    assert block["max_quantity"] >= 100.0


def test_the_template_reports_a_minimum_lot_above_the_pilot_limit(db):
    """Test 14 (reporting half). It does NOT widen the limit to fit."""
    from app.services.pilot_readiness import (
        PILOT_MAX_ORDER_NOTIONAL,
        build_pilot_policy_template,
    )

    entry = _instrument(db, "CARO", step=100.0)
    entry.raw_identity = {"last_price": 100.0}  # lot ≈ 10 000, limit is 500
    db.commit()

    with exec_settings(**_prepared()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    assert any("pilot_limit_below_minimum_lot" in w for w in template["warnings"])
    block = template["EXECUTION_CLASS_POLICIES"]["ACCIONES"]
    assert block["max_order_notional"] == PILOT_MAX_ORDER_NOTIONAL


def test_the_template_override_never_widens_the_class(db):
    """Test 15."""
    from app.broker.execution_class import apply_override
    from app.services.pilot_readiness import build_pilot_policy_template

    _instrument(db, PILOT_ACCION, step=1.0)
    _instrument(db, "LOTE", step=100.0)
    with exec_settings(**_prepared()) as settings:
        template = build_pilot_policy_template(db, settings=settings)

    from app.broker.execution_class import validate_class_policy

    policy, error = validate_class_policy(
        "ACCIONES", template["EXECUTION_CLASS_POLICIES"]["ACCIONES"]
    )
    assert error is None
    for symbol, override in template["EXECUTION_INSTRUMENT_OVERRIDES"].items():
        _, codes = apply_override(policy, override, allow_increase=False)
        assert "override_increases_limit" not in codes, (symbol, override)


def test_the_readiness_reports_the_minimum_valid_quantity(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, "LOTE", step=100.0, minimum=100.0)
    broker = _securities_broker(symbol="LOTE", held=1000)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=["LOTE"], side="sell", quantity=100,
            live=True, broker=broker, settings=settings,
        )

    entry = report["symbols"][0]
    assert entry["minimum_valid_quantity"] == 100.0
    assert entry["minimum_valid_notional"] == pytest.approx(10_000.0)


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 5 — buy and sell verified independently
# ═══════════════════════════════════════════════════════════════════


def test_only_a_bid_still_allows_selling(db):
    """Test 16. An absent ask must not invalidate a sale."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION, buy=False, sell=True)
    broker = _securities_broker(ask=None)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side=None, quantity=None,
            live=True, broker=broker, settings=settings,
        )

    entry = report["symbols"][0]
    assert entry["sell"]["technically_ready"] is True, \
        entry["sell"]["technical_blocking_reasons"]
    assert entry["buy"]["technically_ready"] is False


def test_only_an_ask_still_allows_buying(db):
    """Test 17."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION, buy=True, sell=False)
    broker = _securities_broker(bid=None)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side=None, quantity=None,
            live=True, broker=broker, settings=settings,
        )

    entry = report["symbols"][0]
    assert entry["buy"]["technically_ready"] is True, \
        entry["buy"]["technical_blocking_reasons"]
    assert entry["sell"]["technically_ready"] is False


def test_capability_survives_a_momentarily_empty_book(db):
    """Test 18. Liquidity now and contractual capability are different facts."""
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _instrument(db, PILOT_ACCION, buy=True, sell=True)
    broker = _securities_broker(bid=None, ask=None)
    with exec_settings(**_prepared()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=[PILOT_ACCION], side=None, quantity=None,
            live=True, broker=broker, settings=settings,
        )

    entry = report["symbols"][0]
    for side in ("buy", "sell"):
        # The contract still permits it...
        assert entry[side]["capability_verified"] is True
        # ...there is simply nothing to trade against right now.
        assert entry[side]["quote_available_now"] is False
        assert entry[side]["technically_ready"] is False


def test_identity_can_be_verified_while_one_side_is_not(db):
    """Test 19."""
    from app.services.pilot_readiness import instrument_verification

    entry = _instrument(db, PILOT_ACCION, buy=False, sell=True)
    verification = instrument_verification(entry)

    assert verification["identity_verified"] is True
    assert verification["sell_verified"] is True
    assert verification["buy_verified"] is False
    assert "buy_unsupported" in verification["buy_missing"]
    # The identity fields are NOT listed as the reason buy is unverified.
    assert "currency" not in verification["buy_missing"]


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 6 — lazy broker
# ═══════════════════════════════════════════════════════════════════


def _fund(db, symbol="FCIAR", *, verified=True, subscribe=True, redeem=True):
    fund = FundInstrument(
        symbol=symbol, name="Fondo Test", manager="IOL AM", currency="ARS",
        cutoff_local_time="23:59", settlement_delay_days=1, minimum_amount=100.0,
        subscription_supported=subscribe and verified,
        redemption_supported=redeem and verified,
        active=True,
        verification_status="verified" if verified else "candidate",
        field_provenance={
            "symbol": "iol_fci_catalog",
            "currency": "admin_verified_override",
            "cutoff_local_time": "admin_verified_override",
            "minimum_amount": "admin_verified_override",
        } if verified else {"symbol": "iol_fci_catalog"},
        source="iol_fci_catalog", verified_at=datetime.utcnow(),
    )
    db.add(fund)
    db.commit()
    return fund


def _fund_operation(db, *, operation="subscribe", amount=1000.0, symbol="FCIAR",
                    validated=False):
    record = FundOperation(
        fund_symbol=symbol, operation=operation, currency="ARS", amount=amount,
        status=fci_service.STATE_PREPARED, cutoff_local_time="23:59",
        settlement_delay_days=1,
    )
    db.add(record)
    db.commit()
    if validated:
        fund = fci_service.get_fund(db, symbol)
        record.status = fci_service.STATE_VALIDATED
        record.validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        record.validated_payload_hash = fci_service.validation_payload_hash(record)
        record.validated_fund_hash = fci_service.fund_verification_hash(fund, operation)
        db.commit()
    return record


def _fci_live(**extra):
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
        market_holidays=[],
    )
    base.update(extra)
    return base


def test_the_fci_global_lock_creates_no_broker(db, client):
    """Test 20. Blocked must mean zero instantiations, not a late refusal.

    The route used to build the broker — and authenticate — before the service
    checked the lock, so a request the lock was about to refuse still reached
    IOL on its way to being refused.
    """
    _fund(db)
    factory = _CountingFactory()
    with exec_settings(**_fci_live(order_execution_enabled=False)):
        record = _fund_operation(db)
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 423
    assert "execution_locked" in response.text
    assert factory.calls == 0
    assert factory.broker.submit_fund_request.call_count == 0


@pytest.mark.parametrize("operation,flag,code", [
    ("subscribe", "fci_subscription_enabled", "fci_subscription_disabled"),
    ("redeem", "fci_redemption_enabled", "fci_redemption_disabled"),
])
def test_the_fci_capability_flag_creates_no_broker(db, client, operation, flag, code):
    """Test 21."""
    _fund(db)
    factory = _CountingFactory()
    with exec_settings(**_fci_live(**{flag: False})):
        record = _fund_operation(db, operation=operation)
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 423
    assert code in response.text
    assert factory.calls == 0


def test_an_unoperable_fund_creates_no_broker_during_validation(db, client):
    _fund(db, verified=False)
    factory = _CountingFactory()
    with exec_settings(**_fci_live()):
        record = _fund_operation(db)
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 409
    assert "fund_not_verified" in response.text
    assert factory.calls == 0


def test_an_invalid_pilot_payload_creates_no_broker(db, client):
    """Test 22."""
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())

    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            bad_key = client.post("/api/execution-pilot/securities",
                                  json=_pilot_payload(),
                                  headers={"X-Execution-Key": "wrong"})
            bad_phrase = client.post(
                "/api/execution-pilot/securities",
                json=_pilot_payload(confirmation_text="NOPE"), headers=EXEC_HEADERS,
            )
            bad_side = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(side="hold"),
                                   headers=EXEC_HEADERS)
            unknown = client.post("/api/execution-pilot/securities",
                                  json=_pilot_payload(symbol="NOSUCH"),
                                  headers=EXEC_HEADERS)

    assert bad_key.status_code == 403
    assert bad_phrase.status_code == 422
    assert bad_side.status_code == 422
    assert unknown.status_code == 409
    assert factory.calls == 0


def test_a_disabled_pilot_flag_creates_no_broker(db, client):
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared(execution_pilot_creation_enabled=False)):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    assert response.status_code == 423
    assert factory.calls == 0


def test_live_readiness_creates_the_broker_only_after_the_gates(db, client):
    """Test 23."""
    _instrument(db, PILOT_ACCION)
    factory = _CountingFactory(_securities_broker())
    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            refused = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell&quantity=1"
            )
            assert refused.status_code == 403
            assert factory.calls == 0

            allowed = client.get(
                f"/api/broker/pilot-readiness?live=true&symbols={PILOT_ACCION}"
                "&side=sell&quantity=1",
                headers=EXEC_HEADERS,
            )

    assert allowed.status_code == 200
    assert factory.calls == 1


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 7 — the fund is re-checked at every stage
# ═══════════════════════════════════════════════════════════════════


def _demote(client, symbol="FCIAR"):
    return client.post(f"/api/funds/catalog/{symbol}/demote",
                       json={"note": "revisar con la administradora"},
                       headers=EXEC_HEADERS)


def test_a_demoted_fund_blocks_validation(db, client):
    """Test 24."""
    _fund(db)
    factory = _CountingFactory()
    with exec_settings(**_fci_live()):
        record = _fund_operation(db)
        assert _demote(client).status_code == 200
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post(
                f"/api/funds/operations/{record.id}/validate", headers=EXEC_HEADERS
            )

    assert response.status_code == 409
    assert "fund_not_verified" in response.text
    assert factory.calls == 0


def test_a_demoted_fund_blocks_the_preview(db, client):
    """Test 25."""
    _fund(db)
    with exec_settings(**_fci_live()):
        record = _fund_operation(db, validated=True)
        before = client.get(f"/api/funds/operations/{record.id}/preview").json()
        assert before["can_submit"] is True, before["blocking_reasons"]

        assert _demote(client).status_code == 200
        after = client.get(f"/api/funds/operations/{record.id}/preview").json()

    assert after["can_submit"] is False
    assert "fund_not_verified" in after["blocking_reasons"]


def test_a_demoted_fund_blocks_the_submit(db, client):
    """Test 26."""
    _fund(db)
    broker = mock.MagicMock()
    with exec_settings(**_fci_live()):
        record = _fund_operation(db, validated=True)
        preview = client.get(f"/api/funds/operations/{record.id}/preview").json()
        assert _demote(client).status_code == 200

        with mock.patch("app.services.execution._get_execution_broker",
                        return_value=broker):
            response = client.post(
                f"/api/funds/operations/{record.id}/submit",
                json={
                    "preview_hash": preview["preview_hash"],
                    "preview_generated_at": preview["generated_at"],
                    "confirmation_text": fci_service.fund_confirmation_phrase(record.id),
                    "note": "x",
                },
                headers=EXEC_HEADERS,
            )
        ledger = db.query(ExecutionDailyNotional).count()

    assert response.status_code == 409
    broker.submit_fund_request.assert_not_called()
    assert ledger == 0


def test_withdrawing_a_capability_invalidates_the_validation(db, client):
    """Test 27. The request bytes are identical; the authorisation is not."""
    fund = _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        assert fci_service.validation_state(
            record, settings=settings, fund=fund
        )["valid"] is True

        fund.subscription_supported = False
        db.commit()
        state = fci_service.validation_state(record, settings=settings, fund=fund)

    assert state["valid"] is False
    assert state["code"] == "fund_verification_changed"


def test_changing_the_fund_identity_invalidates_the_validation(db):
    """Test 28."""
    fund = _fund(db)
    fund.identity_hash = fci_service.fund_identity_hash(fund)
    db.commit()

    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        fund.identity_hash = "0" * 64
        db.commit()
        state = fci_service.validation_state(record, settings=settings, fund=fund)

    assert state["valid"] is False
    assert state["code"] == "fund_verification_changed"


@pytest.mark.parametrize("field,value", [
    ("cutoff_local_time", "09:00"),
    ("minimum_amount", 99999.0),
    ("verification_status", "candidate"),
    ("active", False),
])
def test_any_change_to_the_verified_fund_invalidates_the_validation(db, field, value):
    fund = _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        setattr(fund, field, value)
        db.commit()
        state = fci_service.validation_state(record, settings=settings, fund=fund)

    assert state["valid"] is False
    assert state["code"] == "fund_verification_changed"


def test_withdrawing_redemption_does_not_invalidate_a_subscription(db):
    """Only the capability THIS operation needs is part of its hash."""
    fund = _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, operation="subscribe", validated=True)
        fund.redemption_supported = False
        db.commit()
        state = fci_service.validation_state(record, settings=settings, fund=fund)

    assert state["valid"] is True


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 8 — identity decision
# ═══════════════════════════════════════════════════════════════════


def _freeze(db, symbol="FCIAR"):
    fund = fci_service.get_fund(db, symbol)
    fund.verification_status = fci_service.FUND_STATUS_IDENTITY_CHANGED
    fund.previous_identity = {"symbol": symbol, "currency": "ARS",
                              "manager": "IOL AM"}
    fund.pending_identity = {"symbol": symbol, "currency": "ARS",
                             "manager": "OTRA AM"}
    db.commit()
    return fund


def test_a_frozen_fund_cannot_be_demoted_to_dodge_the_decision(db, client):
    """Test 29. Demoting would dismiss the change rather than decide it."""
    _fund(db)
    fund = _freeze(db)
    with exec_settings(**_fci_live()):
        response = _demote(client)

    db.refresh(fund)
    assert response.status_code == 409
    assert "identity-decision" in response.text
    assert fund.verification_status == fci_service.FUND_STATUS_IDENTITY_CHANGED


def test_a_frozen_fund_cannot_be_verified_either(db, client):
    _fund(db)
    _freeze(db)
    with exec_settings(**_fci_live()):
        response = client.post("/api/funds/catalog/FCIAR/verify", json={
            "cutoff_local_time": "15:00", "minimum_amount": 100.0,
            "settlement_delay_days": 1, "currency": "ARS",
            "subscription_supported": True, "redemption_supported": True,
            "note": "intento de verificar un fondo congelado",
        }, headers=EXEC_HEADERS)

    assert response.status_code == 409
    assert "cambio de identidad" in response.text


def test_accepting_an_identity_change_returns_the_fund_to_candidate(db, client):
    _fund(db)
    fund = _freeze(db)
    with exec_settings(**_fci_live()):
        state = client.get("/api/funds/catalog/FCIAR").json()
        response = client.post("/api/funds/catalog/FCIAR/identity-decision", json={
            "decision": "accept",
            "expected_pending_identity_hash": state["pending_identity_hash"],
            "note": "confirmado con la administradora",
        }, headers=EXEC_HEADERS)

    db.refresh(fund)
    assert response.status_code == 200, response.text
    assert fund.verification_status == fci_service.FUND_STATUS_CANDIDATE
    assert fund.pending_identity is None
    # Accepting is NOT re-verifying: what was verified described another fund.
    assert fund.subscription_supported is False
    assert fund.redemption_supported is False
    assert response.json()["requires_reverification"] is True
    assert [r.action for r in db.query(FundInstrumentVerification).all()] == \
        ["identity_accept"]


def test_rejecting_an_identity_change_deactivates_the_fund(db, client):
    _fund(db)
    fund = _freeze(db)
    with exec_settings(**_fci_live()):
        state = client.get("/api/funds/catalog/FCIAR").json()
        response = client.post("/api/funds/catalog/FCIAR/identity-decision", json={
            "decision": "reject",
            "expected_pending_identity_hash": state["pending_identity_hash"],
            "note": "no es el mismo fondo",
        }, headers=EXEC_HEADERS)

    db.refresh(fund)
    assert response.status_code == 200
    assert fund.verification_status == fci_service.FUND_STATUS_REJECTED
    assert fund.active is False


def test_an_identity_decision_requires_the_matching_pending_hash(db, client):
    """A decision made while looking at one change must not apply to another."""
    _fund(db)
    _freeze(db)
    with exec_settings(**_fci_live()):
        response = client.post("/api/funds/catalog/FCIAR/identity-decision", json={
            "decision": "accept",
            "expected_pending_identity_hash": "0" * 64,
            "note": "hash viejo",
        }, headers=EXEC_HEADERS)

    assert response.status_code == 409
    assert "hash de identidad pendiente no coincide" in response.text


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 9 — build vs send, at the transport
# ═══════════════════════════════════════════════════════════════════


def _iol_client(handler=None, *, broken_build=False, broken_send=False,
                broken_json=False):
    import httpx

    from app.broker.clients import IolBrokerClient

    def default_handler(request):
        return httpx.Response(200, json={"numeroOperacion": 123})

    broker = IolBrokerClient.__new__(IolBrokerClient)
    broker.api_base = "https://api.invertironline.test"
    broker._access_token = "token"
    broker._token_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    broker._ensure_auth = lambda: None

    real = httpx.Client(transport=httpx.MockTransport(handler or default_handler))

    class _Client:
        def build_request(self, *args, **kwargs):
            if broken_build:
                raise ValueError("cannot serialize form data")
            return real.build_request(*args, **kwargs)

        def send(self, request_obj):
            if broken_send:
                raise httpx.ReadTimeout("timeout", request=request_obj)
            response = real.send(request_obj)
            if broken_json:
                class _Broken:
                    status_code = response.status_code

                    def json(self):
                        raise ValueError("not json")
                return _Broken()
            return response

    broker._client = _Client()
    return broker


FCI_REQUEST = {
    "endpoint": "/api/v2/operar/suscripcion/fci",
    "form_data": {"Simbolo": "FCIAR", "Monto": "100", "soloValidar": "false"},
    "content_type": "application/x-www-form-urlencoded",
    "solo_validar": False,
}


def test_a_build_failure_is_a_local_error_with_zero_requests():
    """Test 31. Building and sending are separate failures.

    A serialization bug never put a byte on the wire. Calling it
    `submission_unknown` strands the operation until a human checks IOL for a
    request that was never assembled.
    """
    broker = _iol_client(broken_build=True)
    lifecycle: dict = {}
    result = broker.submit_fund_request(FCI_REQUEST, lifecycle=lifecycle)

    assert result["outcome"] == "local_error"
    assert result["http_requests_sent"] == 0
    assert lifecycle["request_started"] is False
    assert lifecycle["response_received"] is False


def test_a_send_failure_is_ambiguous_with_one_request():
    """Test 32."""
    broker = _iol_client(broken_send=True)
    lifecycle: dict = {}
    result = broker.submit_fund_request(FCI_REQUEST, lifecycle=lifecycle)

    assert result["outcome"] == "submission_unknown"
    assert result["http_requests_sent"] == 1
    assert lifecycle["request_started"] is True
    assert lifecycle["response_received"] is False


def test_a_parsing_failure_after_a_response_is_classified_consistently():
    """Test 33. A response arrived; we just could not read it."""
    broker = _iol_client(broken_json=True)
    lifecycle: dict = {}
    result = broker.submit_fund_request(FCI_REQUEST, lifecycle=lifecycle)

    assert lifecycle["request_started"] is True
    assert lifecycle["response_received"] is True
    # We received something unreadable — that is not "parsed".
    assert lifecycle["response_parsed"] is False
    # A 200 with no readable operation id is ambiguous, not success.
    assert result["outcome"] == "submission_unknown"
    assert result["http_requests_sent"] == 1


def test_a_successful_round_trip_marks_the_whole_lifecycle():
    broker = _iol_client()
    lifecycle: dict = {}
    result = broker.submit_fund_request(FCI_REQUEST, lifecycle=lifecycle)

    assert lifecycle == {"before_send": True, "request_started": True,
                         "response_received": True, "response_parsed": True}
    assert result["outcome"] == "submitted"
    assert result["operation_id"] == "123"


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 10 — persistent, idempotent reservation
# ═══════════════════════════════════════════════════════════════════


def _ledger(db, execution_class, currency="ARS", trade_date=None, settings=None):
    row = (
        db.query(ExecutionDailyNotional)
        .filter(
            ExecutionDailyNotional.trade_date ==
            (trade_date or trade_date_for(settings or get_settings())),
            ExecutionDailyNotional.execution_class == execution_class,
            ExecutionDailyNotional.currency == currency,
        )
        .first()
    )
    return row


def test_the_reservation_records_its_own_key(db):
    _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        error, _ = fci_service.claim_and_reserve_fund_operation(
            db, record, settings=settings
        )
        db.commit()
        reservation = db.query(FundBudgetReservation).one()

    assert error is None
    assert reservation.fund_operation_id == record.id
    assert reservation.ledger_class == fci_service.LEDGER_CLASS_SUBSCRIBE
    assert reservation.currency == "ARS"
    assert reservation.reserved_notional == 1000.0
    assert reservation.status == "reserved"


def test_release_uses_the_stored_trade_date_not_today(db):
    """Test 34. A release after midnight must not shrink TODAY's allowance."""
    from app.services.execution_limits import reserve_daily_budget

    _fund(db)
    stale_date = "2020-01-02"
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        # A reservation made on a day that is no longer today.
        reserve_daily_budget(
            db, trade_date=stale_date,
            execution_class=fci_service.LEDGER_CLASS_SUBSCRIBE, currency="ARS",
            notional=Decimal("1000"), max_daily_notional=500_000.0,
        )
        db.add(FundBudgetReservation(
            fund_operation_id=record.id, trade_date=stale_date,
            ledger_class=fci_service.LEDGER_CLASS_SUBSCRIBE, currency="ARS",
            reserved_notional=1000.0, status="reserved",
        ))
        # And separate spending today, which must NOT be touched.
        reserve_daily_budget(
            db, trade_date=trade_date_for(settings),
            execution_class=fci_service.LEDGER_CLASS_SUBSCRIBE, currency="ARS",
            notional=Decimal("7000"), max_daily_notional=500_000.0,
        )
        db.commit()

        result = fci_service.release_fund_budget(
            db, record, settings=settings, reason="test"
        )
        db.commit()

        old_row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE,
                          trade_date=stale_date)
        today_row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)

    assert result["released"] is True
    assert result["trade_date"] == stale_date
    assert old_row.submitted_notional == 0.0
    assert today_row.submitted_notional == 7000.0


def test_release_is_idempotent(db):
    """Test 35. `submitted_notional` is spending authority."""
    _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        fci_service.claim_and_reserve_fund_operation(db, record, settings=settings)
        db.commit()
        record.status = fci_service.STATE_VALIDATED
        db.commit()

        first = fci_service.release_fund_budget(
            db, record, settings=settings, reason="never_sent")
        db.commit()
        second = fci_service.release_fund_budget(
            db, record, settings=settings, reason="never_sent")
        db.commit()
        row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)

    assert first["released"] is True
    assert second["released"] is False
    assert second["reason"] == "budget_already_released"
    # The second call gave nothing back a second time.
    assert row.submitted_notional == 0.0


def test_release_decrements_order_count_without_going_negative(db):
    """Test 36."""
    _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        fci_service.claim_and_reserve_fund_operation(db, record, settings=settings)
        db.commit()
        row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)
        assert row.order_count == 1

        record.status = fci_service.STATE_VALIDATED
        db.commit()
        fci_service.release_fund_budget(db, record, settings=settings, reason="x")
        db.commit()
        db.refresh(row)

    assert row.order_count == 0
    assert row.submitted_notional == 0.0


def test_an_operation_that_reached_the_broker_is_never_released(db):
    _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        fci_service.claim_and_reserve_fund_operation(db, record, settings=settings)
        db.commit()
        record.status = fci_service.STATE_SUBMISSION_UNKNOWN
        db.commit()

        result = fci_service.release_fund_budget(
            db, record, settings=settings, reason="intento")
        db.commit()
        row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)

    assert result["released"] is False
    assert result["reason"] == "operation_reached_broker"
    assert row.submitted_notional == 1000.0


def test_two_claims_of_the_same_operation_leave_one_winner(db):
    """Test 37."""
    _fund(db)
    with exec_settings(**_fci_live()) as settings:
        record = _fund_operation(db, validated=True)
        first_error, _ = fci_service.claim_and_reserve_fund_operation(
            db, record, settings=settings)
        db.commit()
        second_error, _ = fci_service.claim_and_reserve_fund_operation(
            db, record, settings=settings)
        row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)
        reservations = db.query(FundBudgetReservation).count()

    assert first_error is None
    assert second_error == "fund_operation_already_submitted"
    assert row.submitted_notional == 1000.0
    assert reservations == 1


def test_concurrent_reservations_cannot_exceed_the_daily_limit(db):
    """Test 38. The limit lives in the WHERE clause, not in Python."""
    _fund(db)
    with exec_settings(**_fci_live(fci_max_daily_amount=1500.0)) as settings:
        first = _fund_operation(db, validated=True, amount=1000.0)
        second = _fund_operation(db, validated=True, amount=1000.0)

        first_error, _ = fci_service.claim_and_reserve_fund_operation(
            db, first, settings=settings)
        db.commit()
        second_error, _ = fci_service.claim_and_reserve_fund_operation(
            db, second, settings=settings)
        db.commit()
        row = _ledger(db, fci_service.LEDGER_CLASS_SUBSCRIBE, settings=settings)

    assert first_error is None
    assert second_error == "daily_limit_exceeded"
    assert row.submitted_notional == 1000.0
    db.refresh(second)
    assert second.status == fci_service.STATE_VALIDATED


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 11 — reconciliation only for states that reached the broker
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("state", [
    fci_service.STATE_PREPARED,
    fci_service.STATE_VALIDATION_REQUESTED,
    fci_service.STATE_VALIDATED,
    fci_service.STATE_APPROVAL_REQUESTED,
])
def test_reconciling_an_operation_that_never_left_is_blocked(db, client, state):
    """Test 30. Reconciling is DECLARING what IOL did.

    An operation that never left this process has nothing at IOL to declare,
    so marking it confirmed would invent a subscription — and the fabricated
    record would then reserve capacity and read as real money.
    """
    _fund(db)
    with exec_settings(**_fci_live()):
        record = _fund_operation(db)
        record.status = state
        db.commit()
        response = client.post(
            f"/api/funds/operations/{record.id}/reconcile",
            json={"outcome": "confirmed", "note": "revisado en IOL"},
            headers=EXEC_HEADERS,
        )

    assert response.status_code == 409
    assert "nunca llegó al broker" in response.text
    db.refresh(record)
    assert record.status == state


@pytest.mark.parametrize("state", [
    fci_service.STATE_SUBMITTING,
    fci_service.STATE_SUBMITTED,
    fci_service.STATE_PENDING_CONFIRMATION,
    fci_service.STATE_SUBMISSION_UNKNOWN,
    fci_service.STATE_RECONCILIATION_REQUIRED,
])
def test_reconciling_a_state_that_reached_the_broker_is_allowed(db, client, state):
    _fund(db)
    with exec_settings(**_fci_live()):
        record = _fund_operation(db)
        record.status = state
        db.commit()
        response = client.post(
            f"/api/funds/operations/{record.id}/reconcile",
            json={"outcome": "confirmed", "note": "confirmado en el panel de IOL"},
            headers=EXEC_HEADERS,
        )

    assert response.status_code == 200, response.text
    db.refresh(record)
    assert record.status == fci_service.STATE_CONFIRMED


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 12 — next_safe_action per capability
# ═══════════════════════════════════════════════════════════════════


def test_missing_fci_limits_do_not_block_the_securities_path(db, client):
    """The capabilities do not depend on each other, so neither should their
    instructions."""
    _instrument(db, PILOT_ACCION)
    with exec_settings(**_prepared(fci_max_operation_amount=0.0,
                                   fci_max_daily_amount=0.0)):
        body = client.get("/api/broker/execution-readiness").json()

    actions = body["next_safe_actions"]
    assert actions["fci_subscription"] == "configure_fci_limits"
    assert actions["fci_redemption"] == "configure_fci_limits"
    # ACCIONES is unaffected by FCI being unconfigured.
    assert actions["acciones"] == "ready_for_controlled_acciones_pilot"


def test_each_capability_reports_its_own_next_action(db, client):
    _instrument(db, PILOT_ACCION)
    with exec_settings(**_prepared(execution_class_policies={
        "ACCIONES": CLASS_POLICIES["ACCIONES"]
    })):
        actions = client.get("/api/broker/execution-readiness").json()["next_safe_actions"]

    assert set(actions) == {"acciones", "cedears", "fci_subscription", "fci_redemption"}
    # CEDEARS has no policy, so its instruction differs from ACCIONES'.
    assert actions["cedears"] == "configure_class_policy_cedears"
    assert actions["acciones"] != actions["cedears"]


def test_the_headline_action_agrees_with_the_per_capability_map(db, client):
    with exec_settings(**_prepared()):
        body = client.get("/api/broker/execution-readiness").json()

    assert body["next_safe_action"] in set(body["next_safe_actions"].values())


@pytest.mark.parametrize("enabled", [True, False])
def test_readiness_reports_the_pilot_creation_flag(db, client, enabled):
    """Creating a pilot has its OWN gate, and the report has to show it.

    Without this field an operator could not tell from the readiness response
    whether pilot creation was open — the only way to find out was to inspect
    the environment by hand, or to try and read the 423.
    """
    with exec_settings(**_prepared(execution_pilot_creation_enabled=enabled)):
        body = client.get("/api/broker/execution-readiness").json()

    assert body["execution_pilot_creation_enabled"] is enabled


def test_the_pilot_creation_flag_is_reported_as_a_real_boolean(db, client):
    """A truthy string would read as enabled in the UI while the backend
    still refused, which is the worst of both."""
    with exec_settings(**_prepared(execution_pilot_creation_enabled=False)):
        body = client.get("/api/broker/execution-readiness").json()

    assert body["execution_pilot_creation_enabled"] is False
    assert isinstance(body["execution_pilot_creation_enabled"], bool)


# ═══════════════════════════════════════════════════════════════════
# BLOQUEO 13 + regression — production history
# ═══════════════════════════════════════════════════════════════════


def test_an_open_recommendation_blocks_a_new_pilot(db, client):
    """Recommendation 13 must be decided by a human, never by this code."""
    thirteen = Recommendation(
        id=13, action="reducir", status="pending", suggested_pct=5.0,
        confidence=0.8, rationale="reducir SPY", risks="",
        executive_summary="Reducir SPY",
    )
    db.add(thirteen)
    db.commit()

    _instrument(db, PILOT_ACCION)
    _snapshot(db, positions=((PILOT_ACCION, 10, 100.0),))
    factory = _CountingFactory(_securities_broker())

    with exec_settings(**_prepared()):
        with mock.patch("app.services.execution._get_execution_broker", factory):
            response = client.post("/api/execution-pilot/securities",
                                   json=_pilot_payload(), headers=EXEC_HEADERS)

    assert response.status_code == 409
    db.refresh(thirteen)
    assert thirteen.status == "pending"
    assert db.query(Recommendation).count() == 1


def test_the_legacy_byma_pilot_path_is_unchanged(db, client):
    """Test 10. The path that already executed for real is not migrated."""
    from app.services.execution_pilot import (
        PILOT_CONFIRMATION,
        PILOT_QUANTITY,
        PILOT_SIDE,
        PILOT_SYMBOL,
    )

    assert (PILOT_SYMBOL, PILOT_SIDE, PILOT_QUANTITY) == ("BYMA", "sell", 1)
    assert PILOT_CONFIRMATION == "CREAR PILOTO BYMA 1"

    routes = {r.path for r in app.routes}
    assert "/api/execution-pilot/recommendations" in routes
    assert "/api/execution-pilot/securities" in routes


def test_the_new_pilot_candidate_is_not_hardcoded_to_byma():
    """BYMA has a legacy per-symbol policy that short-cuts the class path, so
    piloting it would prove the bridge works, not the class flow."""
    import pathlib

    source = pathlib.Path("app/services/execution_pilot.py").read_text()
    generic = source[source.index("def create_securities_pilot_recommendation"):]
    assert '"BYMA"' not in generic
    assert "PILOT_SYMBOL" not in generic


@pytest.mark.parametrize("path", [
    "app/services/fci.py",
    "app/services/pilot_readiness.py",
    "app/services/execution_pilot.py",
    "app/services/execution_limits.py",
    "app/services/execution.py",
    "app/broker/clients.py",
    "app/main.py",
])
def test_no_touched_module_rewrites_production_history(path):
    """Tests 44-48, asserted structurally.

    These tests run against a temporary SQLite file, so a runtime check would
    prove nothing about production. What CAN be proven is that nothing this
    change touches contains an UPDATE or DELETE against those tables.
    """
    import pathlib
    import re

    dangerous = re.compile(
        r"(DELETE\s+FROM|UPDATE)\s+(recommendations|order_executions)",
        re.IGNORECASE,
    )
    source = pathlib.Path(path).read_text()
    assert not dangerous.search(source), f"{path} rewrites production history"


def test_production_history_shape_is_preserved(db):
    """Tests 44-48. The rows this app already holds keep their meaning.

    Written against a fresh database on purpose: it asserts that the CODE can
    still represent that history, which is what a schema change could break.
    """
    rec12 = Recommendation(
        id=12, action="reducir", status="approved", suggested_pct=5.0,
        confidence=0.9, rationale="piloto BYMA", risks="",
        executive_summary="Vender 1 BYMA",
    )
    rec13 = Recommendation(
        id=13, action="reducir", status="pending", suggested_pct=5.0,
        confidence=0.8, rationale="reducir SPY", risks="",
        executive_summary="Reducir SPY",
    )
    db.add_all([rec12, rec13])
    db.flush()
    execution = OrderExecution(
        recommendation_id=12, symbol="BYMA", side="sell", quantity=1,
        target_change_pct=-5.0, status="executed",
        broker_order_id="183382167", executed_price=297.0,
    )
    db.add(execution)
    db.commit()

    assert db.get(Recommendation, 12).status == "approved"
    assert db.get(Recommendation, 13).status == "pending"
    stored = db.query(OrderExecution).one()
    assert stored.status == "executed"
    assert stored.broker_order_id == "183382167"
    assert stored.executed_price == 297.0
    assert stored.quantity == 1
    assert stored.symbol == "BYMA"


def test_neither_the_scheduler_nor_the_llm_can_execute_or_create_pilots():
    """Tests 49-50, proven over the AST rather than by exercising paths."""
    import ast
    import pathlib

    forbidden = {
        "create_securities_pilot_recommendation",
        "create_execution_pilot_recommendation",
        "approve_and_execute",
        "submit_fund_operation",
        "validate_fund_operation",
        "place_order",
        "submit_order_request",
        "cancel_order",
        "claim_and_reserve_fund_operation",
    }
    for path in ("app/scheduler/jobs.py", "app/services/orchestrator.py",
                 "app/llm/explainer.py", "app/services/analysis_gate.py",
                 "app/news/ingestion.py"):
        tree = ast.parse(pathlib.Path(path).read_text())
        called = {
            node.func.id if isinstance(node.func, ast.Name) else
            (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            for node in ast.walk(tree) if isinstance(node, ast.Call)
        }
        leaked = called & forbidden
        assert not leaked, f"{path} can reach execution: {leaked}"
