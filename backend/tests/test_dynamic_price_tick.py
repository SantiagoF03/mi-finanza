"""Dynamic price tick for bCBA equities.

The premise: for an equity on bCBA the minimum price alteration is a function
of the price, not a property of the instrument. COME at 41.10 ticks in 0.05;
the same COME at 50.01 ticks in 0.10. So a stored tick is only ever right
inside one band, and an order priced with yesterday's band is not a valid
order today.

What follows from that, and what this file defends:

- the tick is never stored as an answer — the RULE is stored, and the answer
  is recomputed from whatever price is in play;
- the preview signs the rule, the band and the tick it computed, so an
  approval cannot carry a different one;
- the preflight recomputes from the fresh quote and refuses when the band
  moved, BEFORE anything irreversible happens;
- everything outside the audited scope (bonds, other venues, other
  currencies, FCI) keeps its fixed-tick behaviour and blocks without one.

Nothing here touches IOL: every broker is a double.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.price_rules import (
    KNOWN_PRICE_TICK_RULES,
    RULE_BYMA_EQUITY_V1,
    PriceTickRuleError,
    price_tick_rule_applies,
    resolve_byma_equity_price_band,
    resolve_byma_equity_price_tick,
    resolve_price_tick_for_rule,
)
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    ExecutionInstrument,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


# ═══════════════════════════════════════════════════════════════════
# A — the pure rule
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("price,expected", [
    # Every boundary from the specification, and both sides of each one.
    ("0.50", "0.001"),
    ("1.00", "0.001"),
    ("1.01", "0.01"),
    ("5.00", "0.01"),
    ("10.00", "0.01"),
    ("10.01", "0.05"),
    ("41.10", "0.05"),   # COME, the case that prompted this
    ("50.00", "0.05"),
    ("50.01", "0.10"),
    ("75.00", "0.10"),
    ("100.00", "0.10"),
    ("100.01", "0.25"),
    ("175.00", "0.25"),
    ("250.00", "0.25"),
    ("250.01", "0.50"),
    ("10000.00", "0.50"),
])
def test_every_band_boundary(price, expected):
    assert resolve_byma_equity_price_tick(price) == Decimal(expected)


def test_boundaries_are_inclusive_upper_edges():
    """50.00 is still the 0.05 band; 50.01 is the first price of the 0.10 one.

    Stated as its own test because an off-by-one-cent here misprices real
    orders, and the two sides of the comparison are different ticks.
    """
    assert resolve_byma_equity_price_tick(Decimal("50.00")) == Decimal("0.05")
    assert resolve_byma_equity_price_tick(Decimal("50.01")) == Decimal("0.10")


@pytest.mark.parametrize("bad", [
    None, 0, "0", -1, "-0.01", float("nan"), float("inf"), float("-inf"),
    True, False, "", "abc", [], {},
])
def test_an_unusable_price_is_refused_not_rounded(bad):
    """Fail closed. Returning a tick for a price we could not read would be
    the system inventing the number that decides whether an order is
    well-formed."""
    with pytest.raises(PriceTickRuleError):
        resolve_byma_equity_price_tick(bad)


def test_floats_do_not_decide_a_band():
    """The comparison is Decimal end to end.

    With floats, whether 50.01 lands above or below 50.00 can hinge on binary
    representation — and those two answers are different ticks.
    """
    assert resolve_byma_equity_price_tick(50.01) == Decimal("0.10")
    assert resolve_byma_equity_price_tick("50.01") == Decimal("0.10")
    assert resolve_byma_equity_price_tick(Decimal("50.01")) == Decimal("0.10")


def test_the_band_description_is_stable_and_labelled():
    band = resolve_byma_equity_price_band("41.10")
    assert band["rule"] == RULE_BYMA_EQUITY_V1
    assert band["tick"] == Decimal("0.05")
    assert band["lower_exclusive"] == "10.00"
    assert band["upper_inclusive"] == "50.00"
    # The label is what the preflight compares, so it must be deterministic.
    assert band["band"] == resolve_byma_equity_price_band("49.99")["band"]
    assert band["band"] != resolve_byma_equity_price_band("50.01")["band"]


def test_an_unknown_rule_is_never_dispatched_to_the_only_one_we_have():
    with pytest.raises(PriceTickRuleError):
        resolve_price_tick_for_rule("SOMETHING_ELSE_V9", "41.10")
    with pytest.raises(PriceTickRuleError):
        resolve_price_tick_for_rule(None, "41.10")
    assert resolve_price_tick_for_rule(RULE_BYMA_EQUITY_V1, "41.10") == Decimal("0.05")


def test_the_rule_table_lives_in_exactly_one_module():
    """Three copies of a band table is three chances to disagree, and the one
    that disagrees is the one that prices a real order."""
    import pathlib

    for path in ("app/services/execution.py", "app/services/pilot_readiness.py",
                 "app/broker/order_request.py"):
        source = pathlib.Path(path).read_text()
        assert "0.001" not in source or "price_rules" in source, path
        # No module other than price_rules may hardcode a band edge.
        assert "250.01" not in source, f"{path} duplicates the band table"


# ═══════════════════════════════════════════════════════════════════
# B — applicability
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("execution_class", ["ACCIONES", "CEDEARS"])
def test_the_rule_applies_to_bcba_equities_in_pesos(execution_class):
    assert price_tick_rule_applies(
        RULE_BYMA_EQUITY_V1, execution_family="securities",
        execution_class=execution_class, market="bCBA", currency="ARS",
    ) is True


@pytest.mark.parametrize("kwargs", [
    # One wrong field each — every one of them must be enough to refuse.
    {"market": "NYSE"},
    {"currency": "USD"},
    {"execution_class": "BONOS"},
    {"execution_class": "FCI"},
    {"execution_class": "ETF"},
    {"execution_class": None},
    {"execution_family": "fund"},
    {"execution_family": ""},
])
def test_the_rule_does_not_leak_outside_its_audited_scope(kwargs):
    """A band table checked for argentine equities in pesos says nothing about
    a bond, a letra, an FCI or another venue. Guessing which bands those use
    is exactly the failure this module exists to prevent."""
    base = dict(execution_family="securities", execution_class="ACCIONES",
                market="bCBA", currency="ARS")
    base.update(kwargs)
    assert price_tick_rule_applies(RULE_BYMA_EQUITY_V1, **base) is False


def test_an_unknown_rule_never_falls_back_to_the_known_one():
    assert price_tick_rule_applies(
        "MADE_UP_RULE", execution_family="securities",
        execution_class="ACCIONES", market="bCBA", currency="ARS",
    ) is False
    assert price_tick_rule_applies(
        None, execution_family="securities", execution_class="ACCIONES",
        market="bCBA", currency="ARS",
    ) is False


# ═══════════════════════════════════════════════════════════════════
# C — catalog and administrative verification
# ═══════════════════════════════════════════════════════════════════


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/ticks.db", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


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


def _come(db, symbol="COME", *, execution_class="ACCIONES", market="bCBA",
          currency="ARS", price_tick=None, rule=None, rule_verified=True):
    """COME exactly as production has it: everything verified but the tick."""
    provenance = {
        "broker_symbol": "iol_title_detail", "asset_type": "iol_title_detail",
        "instrument_type": "iol_title_detail", "currency": "iol_title_detail",
        "market": "iol_title_detail",
        "buy_supported": "iol_quote", "sell_supported": "iol_quote",
        "quote_supported": "iol_quote",
        "quantity_step": "admin_verified_override",
        "minimum_quantity": "admin_verified_override",
    }
    if price_tick is not None:
        provenance["price_tick"] = "admin_verified_override"
    if rule and rule_verified:
        provenance["price_tick_rule"] = "admin_verified_override"
    entry = ExecutionInstrument(
        broker_symbol=symbol, display_symbol=symbol, description=symbol,
        country="argentina", market=market, settlement="t1",
        asset_type=execution_class, instrument_type=execution_class,
        execution_family="securities", execution_class=execution_class,
        currency=currency, quantity_step=1.0, minimum_quantity=1.0,
        price_tick=price_tick, price_tick_rule=rule,
        active=True, buy_supported=True, sell_supported=True, quote_supported=True,
        verification_status="candidate", field_provenance=provenance,
        source="resolver:universe", verified_at=datetime.utcnow(),
        stale_after=datetime.utcnow() + timedelta(days=1),
    )
    db.add(entry)
    db.commit()
    return entry


def test_come_without_a_tick_or_a_rule_stays_a_candidate(db):
    """Production's exact state: step and minimum verified, tick missing."""
    from app.broker.instrument_catalog import (
        catalog_entry_status,
        recompute_verification_status,
        unverified_fields,
    )

    entry = _come(db)
    assert "price_tick" in unverified_fields(entry)
    assert recompute_verification_status(entry) == "candidate"
    code, details = catalog_entry_status(entry)
    assert code in ("instrument_catalog_incomplete", "instrument_not_verified")


def test_verifying_the_rule_makes_come_verified(db):
    """The whole point: no fixed tick is invented, and the instrument becomes
    executable because a human vouched for the RULE."""
    from app.broker.instrument_catalog import (
        PROV_ADMIN_OVERRIDE,
        catalog_entry_status,
        unverified_fields,
        verify_fields,
    )

    entry = _come(db)
    result = verify_fields(
        entry, fields={"price_tick_rule": RULE_BYMA_EQUITY_V1},
        provenance_source=PROV_ADMIN_OVERRIDE,
        note="Regla de bandas de precio BYMA/IOL verificada administrativamente.",
    )
    db.commit()

    assert result["changed"] is True
    assert entry.price_tick_rule == RULE_BYMA_EQUITY_V1
    assert entry.price_tick_rule_verified_at is not None
    # No fixed tick was invented on the way.
    assert entry.price_tick is None
    assert unverified_fields(entry) == []
    assert entry.verification_status == "verified"
    assert catalog_entry_status(entry)[0] is None
    # And it is auditable.
    assert any(a["event"] == "fields_verified" for a in entry.verification_audit)


def test_an_unknown_rule_identifier_is_refused(db):
    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE, verify_fields

    entry = _come(db)
    result = verify_fields(
        entry, fields={"price_tick_rule": "TOTALLY_MADE_UP"},
        provenance_source=PROV_ADMIN_OVERRIDE, note="x",
    )
    assert result["changed"] is False
    assert result["reason"] == "unknown_price_tick_rule"
    assert entry.price_tick_rule is None


@pytest.mark.parametrize("kwargs", [
    {"market": "NYSE"},
    {"currency": "USD"},
    {"execution_class": "BONOS"},
])
def test_a_rule_incompatible_with_the_instrument_is_refused(db, kwargs):
    from app.broker.instrument_catalog import PROV_ADMIN_OVERRIDE, verify_fields

    entry = _come(db, **kwargs)
    result = verify_fields(
        entry, fields={"price_tick_rule": RULE_BYMA_EQUITY_V1},
        provenance_source=PROV_ADMIN_OVERRIDE, note="x",
    )
    assert result["changed"] is False
    assert result["reason"] == "price_tick_rule_not_applicable"
    assert entry.price_tick_rule is None


def test_a_rule_without_a_verifying_provenance_does_not_count(db):
    """A rule that merely appears on the row is not trusted — exactly like a
    tick that came from a class default."""
    from app.broker.instrument_catalog import unverified_fields, verified_price_tick_rule

    entry = _come(db, rule=RULE_BYMA_EQUITY_V1, rule_verified=False)
    assert verified_price_tick_rule(entry) is None
    assert "price_tick" in unverified_fields(entry)


def test_a_verified_rule_stops_applying_if_the_identity_moves(db):
    """A rule verified for a bCBA equity must not survive the instrument
    becoming something else."""
    from app.broker.instrument_catalog import unverified_fields, verified_price_tick_rule

    entry = _come(db, rule=RULE_BYMA_EQUITY_V1)
    assert verified_price_tick_rule(entry) == RULE_BYMA_EQUITY_V1

    entry.market = "NYSE"
    db.commit()
    assert verified_price_tick_rule(entry) is None
    assert "price_tick" in unverified_fields(entry)


def test_a_fixed_tick_instrument_is_unaffected(db):
    """Everything outside the dynamic scope keeps working exactly as before."""
    from app.broker.instrument_catalog import unverified_fields, verified_price_tick_rule

    entry = _come(db, price_tick=0.01)
    assert verified_price_tick_rule(entry) is None
    assert unverified_fields(entry) == []


def test_an_instrument_with_neither_is_blocked(db):
    from app.broker.instrument_catalog import has_usable_price_tick

    assert has_usable_price_tick(_come(db)) is False
    assert has_usable_price_tick(_come(db, symbol="A", price_tick=0.01)) is True
    assert has_usable_price_tick(
        _come(db, symbol="B", rule=RULE_BYMA_EQUITY_V1)) is True


# ═══════════════════════════════════════════════════════════════════
# D — preview
# ═══════════════════════════════════════════════════════════════════


CLASS_POLICIES = {
    "ACCIONES": {
        "buy_enabled": True, "sell_enabled": True, "currencies": ["ARS"],
        "markets": ["bCBA"], "settlements": ["t1"], "max_order_notional": 5000,
        "max_daily_notional": 20000, "max_quantity": 10, "max_portfolio_pct": 0.9,
        "min_cash_reserve": 0, "fee_buffer_pct": 0.01,
        "max_quote_age_seconds": 300, "max_price_deviation_pct": 0.5,
        "catalog_max_age_seconds": 86400, "validity_minutes": 10,
        "default_quantity_step": None, "default_price_tick": None,
        "order_type": "precioLimite",
    },
}
CLASS_POLICIES["CEDEARS"] = {**CLASS_POLICIES["ACCIONES"]}


def _settings(**extra):
    base = dict(
        api_key="",
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_execution_enabled=False,
        securities_buy_enabled=False,
        securities_sell_enabled=False,
        execution_sell_only=False,
        execution_class_policies=CLASS_POLICIES,
        execution_instrument_overrides={},
        execution_instrument_policies={},
        execution_denylist=[],
        execution_max_order_value=100000.0,
        execution_max_total_value=100000.0,
        execution_max_portfolio_pct=0.9,
        execution_max_quote_age_seconds=300,
        execution_max_price_deviation_pct=0.5,
        execution_quote_clock_skew_seconds=2,
        iol_order_market="bCBA",
        iol_order_settlement="t1",
        iol_order_type="precioLimite",
        iol_order_validity_minutes=10,
        broker_mode="real",
        scheduler_timezone="America/Argentina/Buenos_Aires",
        market_holidays=[],
    )
    base.update(extra)
    return base


def _snapshot(db, *, symbol="COME", quantity=10, price=41.10):
    snap = PortfolioSnapshot(total_value=100000.0, cash=50000.0)
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol=symbol, quantity=quantity,
        avg_price=price, market_value=quantity * price, currency="ARS",
        asset_type="ACCIONES", instrument_type="ACCIONES", pnl_pct=0.0,
    ))
    db.commit()
    return snap


def _recommendation(db, *, symbol="COME", side="sell", quantity=1):
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.0, confidence=1.0,
        rationale="piloto", risks="", executive_summary="piloto",
        metadata_json={"execution_pilot": True, "pilot_type": f"security_{side}",
                       "symbol": symbol, "side": side, "quantity": quantity},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol=symbol,
        target_change_pct=(0.0001 if side == "buy" else -0.0001),
        reason="piloto", quantity_override=quantity,
    ))
    db.commit()
    return rec


def _preview_order(db, rec):
    from app.services.execution import build_execution_preview

    preview = build_execution_preview(db, rec.id)
    return preview, preview["orders_preview"][0]


def test_the_preview_signs_the_dynamic_tick_for_come(db):
    """COME at 41.10 → band >10<=50, tick 0.05, recorded and signed."""
    _come(db, rule=RULE_BYMA_EQUITY_V1)
    _snapshot(db)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        preview, order = _preview_order(db, rec)

    assert order["price_tick_mode"] == "dynamic"
    assert order["price_tick_rule"] == RULE_BYMA_EQUITY_V1
    assert order["price_tick_band"] == ">10.00<=50.00"
    assert order["effective_price_tick"] == "0.05"
    assert order["reference_price_used_for_tick"] == "41.1"
    assert preview["preview_hash"]


def test_a_cedear_above_250_signs_the_top_band(db):
    _come(db, symbol="SPY", execution_class="CEDEARS", rule=RULE_BYMA_EQUITY_V1)
    _snapshot(db, symbol="SPY", price=300.00)
    rec = _recommendation(db, symbol="SPY")

    with exec_settings(**_settings()):
        _preview, order = _preview_order(db, rec)

    assert order["price_tick_rule"] == RULE_BYMA_EQUITY_V1
    assert order["price_tick_band"] == ">250.00"
    assert order["effective_price_tick"] == "0.50"


def test_a_fixed_tick_instrument_previews_as_fixed(db):
    _come(db, price_tick=0.01)
    _snapshot(db)
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        _preview, order = _preview_order(db, rec)

    assert order["price_tick_mode"] == "fixed"
    assert order["price_tick_rule"] is None
    assert order["price_tick_band"] is None
    assert order["effective_price_tick"] == "0.01"


@pytest.mark.parametrize("price_a,price_b", [
    ("41.10", "60.00"),   # different band → different tick
    ("41.10", "45.00"),   # same band, different reference price
])
def test_the_hmac_changes_when_the_tick_inputs_change(db, price_a, price_b):
    """The rule, the band, the tick and the reference price are all signed, so
    an approval cannot carry a tick decision the reviewer never saw."""
    _come(db, rule=RULE_BYMA_EQUITY_V1)
    _snapshot(db, price=float(price_a))
    rec = _recommendation(db)

    with exec_settings(**_settings()):
        first, _ = _preview_order(db, rec)

    # Re-price the same instrument and rebuild.
    position = db.query(PortfolioPosition).one()
    position.avg_price = float(price_b)
    position.market_value = position.quantity * float(price_b)
    db.commit()

    with exec_settings(**_settings()):
        second, _ = _preview_order(db, rec)

    assert first["preview_hash"] != second["preview_hash"]


def test_the_client_cannot_supply_a_tick(db):
    """Nothing read from the request feeds the tick decision — it is derived
    from stored state and the rule, and then signed."""
    import inspect

    from app.services import execution

    source = inspect.getsource(execution._attach_price_tick)
    # The only inputs are the orders, the settings and the db session.
    assert "payload" not in source
    assert "request" not in source
    signature = inspect.signature(execution._attach_price_tick)
    assert list(signature.parameters) == ["orders", "settings", "db"]


def test_the_preview_never_touches_the_broker(db):
    """It is rebuilt server-side during approve to verify the HMAC, so it must
    be a deterministic function of stored state. A live quote here would make
    the hash never match."""
    import inspect

    from app.services import execution

    signature = inspect.signature(execution.build_execution_preview)
    assert "broker" not in signature.parameters

    _come(db, rule=RULE_BYMA_EQUITY_V1)
    _snapshot(db)
    rec = _recommendation(db)
    with exec_settings(**_settings()):
        first, _ = _preview_order(db, rec)
        second, _ = _preview_order(
            db, rec
        )
    # Same stored state ⇒ same signed tick, every time.
    assert first["orders_preview"][0]["effective_price_tick"] == \
        second["orders_preview"][0]["effective_price_tick"]


# ═══════════════════════════════════════════════════════════════════
# E — preflight
# ═══════════════════════════════════════════════════════════════════


def _quote(price, *, side="sell", age_seconds=0):
    stamp = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "available": True,
        "source": "bid" if side == "sell" else "ask",
        "price": price,
        "retrieved_at": stamp.isoformat(),
        "market": "bCBA",
        "settlement": "t1",
    }


def _preflight(db, *, preview_price, fresh_price, side="sell",
               quote_override=None, settings_extra=None):
    """Run the real preflight for ONE order. Never sends anything."""
    from app.broker.execution_scope import effective_policy_for, load_authorization_context
    from app.services.execution import _preflight_one_order

    entry = _come(db, rule=RULE_BYMA_EQUITY_V1)
    snapshot = _snapshot(db, price=preview_price)
    rec = _recommendation(db, side=side)

    with exec_settings(**_settings(**(settings_extra or {}))) as settings:
        preview, preview_order = _preview_order(db, rec)

        order_exec = OrderExecution(
            recommendation_id=rec.id,
            recommendation_action_id=preview_order["recommendation_action_id"],
            symbol="COME", side=side, quantity=1, quantity_planned=1,
            target_change_pct=-0.0001, status="preflight",
        )
        db.add(order_exec)
        db.commit()

        broker = mock.MagicMock()
        broker.get_execution_quote.return_value = (
            quote_override if quote_override is not None
            else _quote(fresh_price, side=side)
        )

        context = load_authorization_context(settings)
        policies = {"COME": effective_policy_for(db, "COME", settings, context)}
        live_positions = [{
            "symbol": "COME", "quantity": 10, "committed": 0,
            "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
            "currency": "ARS", "market_value": 10 * preview_price,
        }]
        detail: dict = {}
        result, error = _preflight_one_order(
            order_exec, preview_order, broker, settings,
            live_positions, Decimal("100000"), policies,
            db=db,
            live_cash_by_currency={"ARS": {"available": True, "cash": 100000.0,
                                           "currency": "ARS", "committed": 0.0}},
            pending_buys={}, trade_date="2026-08-06",
            daily_reserved={}, cash_reserved={},
            error_detail=detail,
        )
    return result, error, detail, broker


def test_a_band_change_between_preview_and_preflight_blocks(db):
    """49.95 → 50.05 crosses from tick 0.05 to 0.10. The signed limit price is
    no longer a legal order, so this must not be sent."""
    result, error, detail, broker = _preflight(
        db, preview_price=49.95, fresh_price=50.05
    )

    assert error == "price_tick_changed_repreview_required"
    assert result is None
    # Nothing was sent, and no order-placing method was even reached.
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()
    # Enough to regenerate the preview, and nothing more.
    assert detail["previous_band"] == ">10.00<=50.00"
    assert detail["current_band"] == ">50.00<=100.00"
    assert detail["previous_tick"] == "0.05"
    assert detail["current_tick"] == "0.10"
    assert detail["repreview_required"] is True
    assert set(detail) == {
        "code", "symbol", "previous_band", "current_band",
        "previous_tick", "current_tick", "price_tick_rule", "repreview_required",
    }


def test_a_band_change_downwards_also_blocks(db):
    """50.05 → 49.95 is a change too. The direction does not make a stale tick
    valid."""
    _result, error, detail, broker = _preflight(
        db, preview_price=50.05, fresh_price=49.95
    )

    assert error == "price_tick_changed_repreview_required"
    assert detail["previous_tick"] == "0.10"
    assert detail["current_tick"] == "0.05"
    broker.place_order.assert_not_called()


def test_a_move_inside_the_same_band_does_not_block_on_the_tick(db):
    """49.90 → 49.95 is the same band. Whatever else may stop it, it is not
    the tick."""
    result, error, _detail, _broker = _preflight(
        db, preview_price=49.90, fresh_price=49.95
    )

    assert error != "price_tick_changed_repreview_required"
    if error is None:
        assert result["audit"]["price_tick"]["effective_price_tick"] == "0.05"
        assert result["audit"]["price_tick"]["preview_effective_price_tick"] == "0.05"


def test_an_excessive_deviation_inside_one_band_keeps_its_own_code(db):
    """Conflating the two would tell an operator to re-preview when the real
    problem is that the market ran away."""
    _result, error, detail, _broker = _preflight(
        db, preview_price=11.00, fresh_price=49.00,
        settings_extra={"execution_max_price_deviation_pct": 0.02},
    )

    # Same band (>10<=50) the whole way, so this is a deviation problem.
    assert error == "price_deviation_exceeded"
    assert detail == {}


def test_a_quote_from_the_wrong_side_blocks_before_any_tick_maths(db):
    _result, error, _detail, broker = _preflight(
        db, preview_price=41.10, fresh_price=41.10,
        quote_override={
            "available": True, "source": "ask", "price": 41.10,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    assert error == "quote_unavailable"
    broker.place_order.assert_not_called()


def test_a_stale_quote_blocks(db):
    _result, error, _detail, broker = _preflight(
        db, preview_price=41.10, fresh_price=41.10,
        quote_override=_quote(41.10, age_seconds=100000),
    )
    assert error == "quote_stale"
    broker.place_order.assert_not_called()


@pytest.mark.parametrize("scenario", [
    dict(preview_price=49.95, fresh_price=50.05),
    dict(preview_price=50.05, fresh_price=49.95),
    dict(preview_price=41.10, fresh_price=41.10, stale=True),
])
def test_no_preflight_branch_reaches_the_iol_order_endpoints(db, scenario):
    """Every blocking branch, and none of them may have called an order
    endpoint — a blocked preflight that already sent something is the failure
    the whole design exists to prevent."""
    scenario = dict(scenario)
    if scenario.pop("stale", False):
        scenario["quote_override"] = _quote(41.10, age_seconds=100000)
    _result, error, _detail, broker = _preflight(db, **scenario)

    assert error is not None
    for method in ("place_order", "submit_order_request", "cancel_order",
                   "submit_fund_request"):
        assert not getattr(broker, method).called, (method, scenario)


def test_a_blocked_tick_change_leaves_the_order_unsent(db):
    """No numeroOperacion, no submission_unknown, no reconciliation for a
    request that never existed."""
    _result, error, _detail, _broker = _preflight(
        db, preview_price=49.95, fresh_price=50.05
    )
    assert error == "price_tick_changed_repreview_required"

    execution = db.query(OrderExecution).one()
    db.refresh(execution)
    assert not execution.broker_order_id
    assert execution.status != "submission_unknown"
    assert execution.status != "manual_reconciliation_required"


# ═══════════════════════════════════════════════════════════════════
# F — the order builder
# ═══════════════════════════════════════════════════════════════════


def _build(**extra):
    from app.broker.order_request import build_iol_order_request

    kwargs = dict(
        side="sell", symbol="COME", quantity=1, price=Decimal("41.10"),
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity="2026-08-06T17:00:00", price_tick=Decimal("0.05"),
    )
    kwargs.update(extra)
    return build_iol_order_request(**kwargs)


def test_a_price_that_is_a_multiple_of_the_tick_serializes():
    request, error = _build()
    assert error is None
    assert request["form_data"]["precio"] == "41.1"
    assert request["form_data"]["tipoOrden"] == "precioLimite"


def test_a_price_incompatible_with_the_tick_is_refused_not_truncated():
    """41.12 is not a multiple of 0.05. Rounding it to fit would silently
    change the limit price a human reviewed."""
    request, error = _build(price=Decimal("41.12"))
    assert error == "price_tick_mismatch"
    assert request is None


def test_the_dynamic_tick_flows_into_the_builder():
    """A price valid under one band's tick is refused under another's."""
    ok, err_ok = _build(price=Decimal("50.05"), price_tick=Decimal("0.05"))
    assert err_ok is None and ok is not None
    # 50.05 is NOT a multiple of 0.10 — the band above.
    bad, err_bad = _build(price=Decimal("50.05"), price_tick=Decimal("0.10"))
    assert err_bad == "price_tick_mismatch"
    assert bad is None


def test_the_builder_never_produces_a_market_order():
    _request, error = _build(order_type="precioMercado")
    assert error == "unsupported_iol_order_type"
    request, _ = _build()
    assert "precioMercado" not in str(request)


@pytest.mark.parametrize("side,endpoint", [
    ("buy", "/api/v2/operar/Comprar"),
    ("sell", "/api/v2/operar/Vender"),
])
def test_each_side_keeps_its_own_endpoint(side, endpoint):
    request, error = _build(side=side)
    assert error is None
    assert request["endpoint"] == endpoint
    assert request["content_type"] == "application/x-www-form-urlencoded"


def test_the_builder_makes_no_network_call():
    """Pure and deterministic: no import of a client, no session, no quote."""
    import pathlib

    source = pathlib.Path("app/broker/order_request.py").read_text()
    for forbidden in ("httpx", "requests", "get_execution_quote", "Session"):
        assert forbidden not in source, forbidden


# ═══════════════════════════════════════════════════════════════════
# G — regression
# ═══════════════════════════════════════════════════════════════════


def test_the_legacy_per_symbol_path_keeps_its_fixed_tick(db):
    """The BYMA pilot that already executed for real used
    EXECUTION_INSTRUMENT_POLICIES, whose tick is stated by a human. It is not
    re-priced with a rule it was never verified against."""
    from app.broker.execution_scope import effective_policy_for

    legacy = {"BYMA": {"asset_type": "ACCIONES", "instrument_type": "ACCIONES",
                       "currency": "ARS", "market": "bCBA", "settlement": "t1",
                       "price_tick": 1.0, "quantity_step": 1.0,
                       "max_quantity": 1.0, "max_notional": 500.0}}
    with exec_settings(**_settings(execution_instrument_policies=legacy)) as settings:
        policy = effective_policy_for(db, "BYMA", settings)

    assert policy is not None
    assert policy["policy_source"] == "legacy_instrument_policy"
    assert policy["price_tick"] == 1.0
    assert policy["price_tick_rule"] is None


def test_the_legacy_path_resolves_its_tick_as_fixed(db):
    from app.services.execution import resolve_effective_tick

    policy = {"price_tick": 1.0, "price_tick_rule": None}
    info, error = resolve_effective_tick(policy, Decimal("297"))
    assert error is None
    assert info["price_tick_mode"] == "fixed"
    assert info["effective_price_tick"] == Decimal("1.0")


def test_an_instrument_with_no_tick_and_no_rule_fails_closed(db):
    from app.services.execution import resolve_effective_tick

    info, error = resolve_effective_tick({"price_tick": None, "price_tick_rule": None},
                                         Decimal("41.10"))
    assert info is None
    assert error == "price_tick_unverified"


def test_production_history_shape_is_preserved(db):
    """Recommendation 12/13 and OrderExecution 1 keep their meaning — asserted
    against a fresh database, so it tests the CODE, which is what a schema
    change could break."""
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
    db.add(OrderExecution(
        recommendation_id=12, symbol="BYMA", side="sell", quantity=1,
        target_change_pct=-5.0, status="executed",
        broker_order_id="183382167", executed_price=297.0,
    ))
    db.commit()

    assert db.get(Recommendation, 12).status == "approved"
    assert db.get(Recommendation, 13).status == "pending"
    stored = db.query(OrderExecution).one()
    assert (stored.status, stored.broker_order_id, stored.executed_price,
            stored.quantity, stored.symbol) == \
        ("executed", "183382167", 297.0, 1, "BYMA")


def test_the_migration_is_additive_only():
    """New columns are added; nothing is dropped, deleted or rewritten."""
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
        if token.type not in (tokenize.COMMENT,)
        and not (token.type == tokenize.STRING
                 and ("DROP" in token.string.upper()
                      or "DELETE" in token.string.upper()))
    ).upper()
    for statement in ("DROP TABLE", "DROP COLUMN", "DELETE FROM"):
        assert statement not in code_only
    assert not re.search(r"\bUPDATE\s+\w+\s+SET", code_only, re.IGNORECASE)
    # And the new columns are declared as additions.
    assert "price_tick_rule" in body


def test_the_migration_runs_on_an_empty_and_on_an_existing_database(tmp_path):
    """Both directions: a fresh schema, and one created before these columns
    existed."""
    from sqlalchemy import inspect as sa_inspect

    from app.main import _patch_schema

    # 1. Empty database.
    fresh = create_engine(f"sqlite:///{tmp_path}/fresh.db")
    Base.metadata.create_all(bind=fresh)
    _patch_schema(fresh)
    columns = {c["name"] for c in sa_inspect(fresh).get_columns("execution_instruments")}
    assert {"price_tick_rule", "price_tick_rule_verified_at",
            "price_tick_observed"} <= columns

    # 2. A pre-existing table WITHOUT the new columns, carrying a row.
    legacy = create_engine(f"sqlite:///{tmp_path}/legacy.db")
    with legacy.begin() as conn:
        from sqlalchemy import text
        conn.execute(text(
            "CREATE TABLE execution_instruments ("
            " id INTEGER PRIMARY KEY, broker_symbol VARCHAR(30),"
            " price_tick FLOAT, quantity_step FLOAT)"
        ))
        conn.execute(text(
            "INSERT INTO execution_instruments (broker_symbol, price_tick, quantity_step)"
            " VALUES ('BYMA', 1.0, 1.0)"
        ))
    _patch_schema(legacy)
    with legacy.begin() as conn:
        from sqlalchemy import text
        row = conn.execute(text(
            "SELECT broker_symbol, price_tick, price_tick_rule"
            " FROM execution_instruments"
        )).fetchone()
    # The historical row survives untouched, and reads as "no rule verified" —
    # so nothing that was blocked becomes allowed by the migration.
    assert row[0] == "BYMA"
    assert row[1] == 1.0
    assert row[2] is None


def test_neither_the_scheduler_nor_the_llm_can_price_or_send_an_order():
    import ast
    import pathlib

    forbidden = {
        "approve_and_execute", "place_order", "submit_order_request",
        "prepare_validated_execution_batch", "_submit_prepared_order",
        "verify_fields", "build_iol_order_request",
    }
    for path in ("app/scheduler/jobs.py", "app/services/orchestrator.py",
                 "app/llm/explainer.py", "app/news/ingestion.py"):
        tree = ast.parse(pathlib.Path(path).read_text())
        called = {
            node.func.id if isinstance(node.func, ast.Name) else
            (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            for node in ast.walk(tree) if isinstance(node, ast.Call)
        }
        leaked = called & forbidden
        assert not leaked, f"{path}: {leaked}"


def test_every_execution_flag_still_defaults_to_false():
    from app.core.config import Settings

    defaults = Settings.model_fields
    for flag in ("order_execution_enabled", "sandbox_execution_enabled",
                 "securities_buy_enabled", "securities_sell_enabled",
                 "fci_subscription_enabled", "fci_redemption_enabled",
                 "order_cancellation_enabled", "execution_pilot_creation_enabled"):
        assert defaults[flag].default is False, flag
    assert defaults["execution_sell_only"].default is True
    assert defaults["execution_require_live_position_check"].default is True
    assert defaults["execution_require_live_cash_check"].default is True


def test_the_known_rule_set_is_explicit():
    """A rule identifier is a promise that code can evaluate it."""
    assert KNOWN_PRICE_TICK_RULES == frozenset({RULE_BYMA_EQUITY_V1})
