"""A generic securities pilot prices its preview from its OWN live quote.

The problem: the preview's reference price came from
`InstrumentCatalog.last_price`, which can be hours old and drawn from a
different session. The preflight then measured deviation as

    |fresh_price - reference| / reference

against a 2% tolerance. For Recommendation 15 that was |41.26 - 43.23| / 43.23
≈ 4.6% — a "deviation" between two unrelated prices, not between what a human
reviewed and what the market is doing now. The order would have been refused
for a move that never happened.

A generic pilot already runs a live readiness probe moments before it is
created, and persists what that probe saw. Dividing that notional back out by
the pilot's quantity gives a per-unit reference that is deterministic,
persisted, auditable, and actually about this order.

Three things it is NOT:

- not a fresh quote — it is labelled `pilot_live_quote_at_creation`;
- not the price sent — the order is priced by the fresh best ask/bid resolved
  at preflight;
- not a fallback — a generic pilot missing it FAILS CLOSED rather than
  quietly using the catalog again.

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

from app.broker.price_rules import RULE_BYMA_EQUITY_V1
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    ExecutionInstrument,
    InstrumentCatalog,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.execution import (
    is_generic_securities_pilot,
    pilot_reference_price,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"

# The production case: COME, ask 41.26, quantity 1, catalog last_price 43.23.
LIVE_ASK = 41.26
STALE_CATALOG_PRICE = 43.23


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/pilotref.db", connect_args={"check_same_thread": False}
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


CLASS_POLICIES = {
    "ACCIONES": {
        "buy_enabled": True, "sell_enabled": True, "currencies": ["ARS"],
        "markets": ["bCBA"], "settlements": ["t1"], "max_order_notional": 500,
        "max_daily_notional": 20000, "max_quantity": 10, "max_portfolio_pct": 0.9,
        "min_cash_reserve": 0, "fee_buffer_pct": 0.01,
        "max_quote_age_seconds": 300,
        # The real production value. Never widened by this change.
        "max_price_deviation_pct": 0.02,
        "catalog_max_age_seconds": 86400, "validity_minutes": 10,
        "default_quantity_step": None, "default_price_tick": None,
        "order_type": "precioLimite",
    },
}


def _settings(**extra):
    base = dict(
        api_key="",
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        order_execution_enabled=False,
        securities_buy_enabled=True,
        securities_sell_enabled=False,
        execution_sell_only=False,
        execution_pilot_creation_enabled=False,
        execution_class_policies=CLASS_POLICIES,
        execution_instrument_overrides={},
        execution_instrument_policies={},
        execution_denylist=[],
        execution_max_order_value=100000.0,
        execution_max_total_value=100000.0,
        execution_max_portfolio_pct=0.9,
        execution_max_quote_age_seconds=300,
        execution_max_price_deviation_pct=0.02,
        execution_quote_clock_skew_seconds=2,
        execution_max_recommendation_age_minutes=60,
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


def _come(db, symbol="COME", *, catalog_price=STALE_CATALOG_PRICE):
    """COME as production has it: verified, dynamic tick rule, and a catalog
    last_price that is NOT the live ask."""
    existing = db.query(ExecutionInstrument).filter(
        ExecutionInstrument.broker_symbol == symbol).first()
    if existing is not None:
        return existing
    entry = ExecutionInstrument(
        broker_symbol=symbol, display_symbol=symbol, description=symbol,
        country="argentina", market="bCBA", settlement="t1",
        asset_type="ACCIONES", instrument_type="ACCIONES",
        execution_family="securities", execution_class="ACCIONES",
        currency="ARS", quantity_step=1.0, minimum_quantity=1.0,
        price_tick=None, price_tick_rule=RULE_BYMA_EQUITY_V1,
        active=True, buy_supported=True, sell_supported=True, quote_supported=True,
        verification_status="verified",
        field_provenance={
            "broker_symbol": "iol_title_detail", "asset_type": "iol_title_detail",
            "instrument_type": "iol_title_detail", "currency": "iol_title_detail",
            "market": "iol_title_detail", "buy_supported": "iol_quote",
            "sell_supported": "iol_quote", "quote_supported": "iol_quote",
            "quantity_step": "admin_verified_override",
            "minimum_quantity": "admin_verified_override",
            "price_tick_rule": "admin_verified_override",
        },
        source="resolver:universe", verified_at=datetime.utcnow(),
        stale_after=datetime.utcnow() + timedelta(days=1),
    )
    db.add(entry)
    if catalog_price is not None:
        db.add(InstrumentCatalog(
            symbol=symbol, name=symbol, asset_type="ACCIONES", market="bCBA",
            currency="ARS", is_active=True, last_price=catalog_price,
        ))
    db.commit()
    return entry


def _snapshot(db, *, positions=(), cash=100000.0):
    snap = PortfolioSnapshot(total_value=100000.0, cash=cash)
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


def _pilot(db, *, symbol="COME", side="buy", quantity=1,
           exact_notional=LIVE_ASK, metadata_overrides=None,
           remove_fields=(), generic=True):
    """A pilot recommendation, generic or legacy."""
    if generic:
        metadata = {
            "execution_pilot": True,
            "pilot_type": f"security_{side}",
            "execution_class": "ACCIONES",
            "symbol": symbol, "side": side, "quantity": quantity,
            "currency": "ARS", "market": "bCBA", "settlement": "t1",
            "created_manually": True,
            "exact_notional_at_creation": exact_notional,
        }
    else:
        # The LEGACY BYMA pilot: no pilot_type, no live reference.
        metadata = {
            "execution_pilot": True,
            "pilot_symbol": symbol, "pilot_side": side,
            "pilot_quantity": quantity, "created_manually": True,
        }
    if metadata_overrides is not None:
        metadata.update(metadata_overrides)
    for field in remove_fields:
        metadata.pop(field, None)

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.0, confidence=1.0,
        rationale="piloto", risks="", executive_summary="piloto",
        metadata_json=metadata,
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


def _preview(db, rec):
    from app.services.execution import build_execution_preview

    result = build_execution_preview(db, rec.id)
    return result, result["orders_preview"][0]


# ═══════════════════════════════════════════════════════════════════
# The reference itself
# ═══════════════════════════════════════════════════════════════════


def test_a_generic_buy_pilot_prices_from_its_own_live_quote(db):
    """Test 1 — and the production case, exactly."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db, exact_notional=LIVE_ASK, quantity=1)

    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "pilot_live_quote_at_creation"
    assert order["snapshot_price_ref"] == pytest.approx(41.26)
    assert order["estimated_notional"] == pytest.approx(41.26)
    assert order["valid"] is True, order["blocked_reason"]


def test_a_generic_sell_pilot_prices_from_the_observed_bid(db):
    """Test 2."""
    _come(db)
    _snapshot(db, positions=(("COME", 10, 50.0),))
    rec, _action = _pilot(db, side="sell", exact_notional=41.05, quantity=1)

    with exec_settings(**_settings(securities_sell_enabled=True)):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "pilot_live_quote_at_creation"
    assert order["snapshot_price_ref"] == pytest.approx(41.05)
    # NOT the position's derived 50.0.
    assert order["snapshot_price_ref"] != pytest.approx(50.0)


def test_the_reference_divides_the_notional_by_the_quantity(db):
    """Test 3: 300 / 3 = 100."""
    _come(db)
    _snapshot(db)
    rec, action = _pilot(db, exact_notional=300.0, quantity=3)

    price, error = pilot_reference_price(rec, action)
    assert error is None
    assert price == Decimal("100")


def test_the_derivation_uses_decimal_not_float(db):
    """A float division would make the anchor of a percentage comparison
    depend on binary representation."""
    _come(db)
    rec, action = _pilot(db, exact_notional="123.75", quantity=3)

    price, error = pilot_reference_price(rec, action)
    assert error is None
    assert isinstance(price, Decimal)
    assert price == Decimal("41.25")


def test_the_price_ref_source_names_what_it_actually_is(db):
    """Test 4. It is a live quote OBSERVED AT CREATION, not a fresh one."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db)

    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "pilot_live_quote_at_creation"


def test_a_stale_catalog_price_never_replaces_the_pilot_reference(db):
    """Test 5. The catalog says 120, the pilot observed 100 — 100 wins."""
    _come(db, catalog_price=120.0)
    _snapshot(db)
    rec, _action = _pilot(db, exact_notional=100.0, quantity=1)

    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["snapshot_price_ref"] == pytest.approx(100.0)
    assert order["price_ref_source"] == "pilot_live_quote_at_creation"


# ═══════════════════════════════════════════════════════════════════
# Scope: only generic securities pilots
# ═══════════════════════════════════════════════════════════════════


def test_a_normal_recommendation_is_unaffected(db):
    """Test 6. Automatic recommendations keep the existing resolution order."""
    _come(db, catalog_price=120.0)
    _snapshot(db)

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=10.0, confidence=0.8,
        rationale="normal", risks="", executive_summary="normal",
        metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol="COME", target_change_pct=5.0,
        reason="normal",
    ))
    db.commit()

    assert is_generic_securities_pilot(rec) is False
    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "instrument_catalog"
    assert order["snapshot_price_ref"] == pytest.approx(120.0)


def test_a_normal_recommendation_still_prefers_a_live_position(db):
    """The existing precedence — position, then catalog, then avg_price — is
    untouched for anything that is not a generic pilot."""
    _come(db, catalog_price=120.0)
    _snapshot(db, positions=(("COME", 10, 77.0),))

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=10.0, confidence=0.8,
        rationale="normal", risks="", executive_summary="normal", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol="COME", target_change_pct=5.0,
        reason="normal",
    ))
    db.commit()

    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "live_position"
    assert order["snapshot_price_ref"] == pytest.approx(77.0)


def test_the_legacy_byma_pilot_is_unaffected(db):
    """Test 7. It has no pilot_type and never stored a live reference, so it
    must keep resolving exactly as before."""
    _come(db, symbol="BYMA", catalog_price=297.0)
    _snapshot(db, positions=(("BYMA", 10, 297.0),))
    rec, _action = _pilot(db, symbol="BYMA", side="sell", quantity=1, generic=False)

    assert is_generic_securities_pilot(rec) is False
    with exec_settings(**_settings(securities_sell_enabled=True)):
        _result, order = _preview(db, rec)

    assert order["price_ref_source"] == "live_position"
    assert order["pilot_reference_error"] is None


def test_the_discriminator_needs_a_generic_pilot_type(db):
    _come(db)
    generic, _ = _pilot(db)
    legacy, _ = _pilot(db, generic=False)

    assert is_generic_securities_pilot(generic) is True
    assert is_generic_securities_pilot(legacy) is False
    # And execution_pilot alone is NOT enough.
    legacy.metadata_json = {"execution_pilot": True}
    assert is_generic_securities_pilot(legacy) is False


# ═══════════════════════════════════════════════════════════════════
# Fail closed
# ═══════════════════════════════════════════════════════════════════


def test_a_generic_pilot_without_a_reference_fails_closed(db):
    """Test 8. NO silent fallback to the catalog — that is the bug.

    The catalog price is present and usable here, which is exactly why this
    matters: the easy behaviour would be to use it.
    """
    _come(db, catalog_price=120.0)
    _snapshot(db)
    rec, _action = _pilot(db, remove_fields=("exact_notional_at_creation",))

    with exec_settings(**_settings()):
        _result, order = _preview(db, rec)

    assert order["valid"] is False
    assert order["pilot_reference_error"] == "pilot_reference_price_missing"
    assert "pilot_reference_price_missing" in order["blocked_reason"]
    # It did NOT quietly price itself from the catalog.
    assert order["price_ref_source"] != "instrument_catalog"


@pytest.mark.parametrize("notional,code", [
    (0, "pilot_reference_price_invalid"),
    (-1, "pilot_reference_price_invalid"),
    (float("nan"), "pilot_reference_price_invalid"),
    (float("inf"), "pilot_reference_price_invalid"),
    (float("-inf"), "pilot_reference_price_invalid"),
    ("abc", "pilot_reference_price_invalid"),
    (True, "pilot_reference_price_invalid"),
    (None, "pilot_reference_price_missing"),
])
def test_an_unusable_notional_fails_closed(db, notional, code):
    """Test 9."""
    _come(db)
    rec, action = _pilot(db, exact_notional=notional)

    price, error = pilot_reference_price(rec, action)
    assert price is None
    assert error == code


@pytest.mark.parametrize("quantity", [0, -1, None, "x", float("nan")])
def test_an_unusable_pilot_quantity_fails_closed(db, quantity):
    """Test 10. Dividing by it would produce a price nobody quoted."""
    _come(db)
    rec, action = _pilot(db)
    rec.metadata_json = {**rec.metadata_json, "quantity": quantity}
    db.commit()

    price, error = pilot_reference_price(rec, action)
    assert price is None
    assert error == "pilot_reference_price_invalid"


def test_a_symbol_mismatch_fails_closed(db):
    """Test 11. One of the two is wrong and we cannot tell which."""
    _come(db)
    rec, action = _pilot(db, symbol="COME")
    rec.metadata_json = {**rec.metadata_json, "symbol": "GGAL"}
    db.commit()

    price, error = pilot_reference_price(rec, action)
    assert price is None
    assert error == "pilot_reference_identity_mismatch"


def test_a_side_mismatch_fails_closed(db):
    """Test 12. The notional was observed on ONE side of the book."""
    _come(db)
    rec, action = _pilot(db, side="buy")
    rec.metadata_json = {**rec.metadata_json, "side": "sell"}
    db.commit()

    price, error = pilot_reference_price(rec, action)
    assert price is None
    assert error == "pilot_reference_identity_mismatch"


def test_a_quantity_override_mismatch_fails_closed(db):
    """Test 13. The notional was observed for the pilot's quantity; dividing
    by a different one gives a per-unit price that was never quoted."""
    _come(db)
    rec, action = _pilot(db, quantity=1, exact_notional=41.26)
    action.quantity_override = 5
    db.commit()

    price, error = pilot_reference_price(rec, action)
    assert price is None
    assert error == "pilot_reference_identity_mismatch"


def test_every_failure_mode_blocks_the_preview_not_just_the_helper(db):
    """The codes have to reach the preview, or nothing is actually blocked."""
    cases = [
        ({}, ("exact_notional_at_creation",), "pilot_reference_price_missing"),
        ({"exact_notional_at_creation": 0}, (), "pilot_reference_price_invalid"),
        ({"symbol": "OTRO"}, (), "pilot_reference_identity_mismatch"),
        ({"side": "sell"}, (), "pilot_reference_identity_mismatch"),
    ]
    _come(db, catalog_price=120.0)
    _snapshot(db)
    for overrides, remove, code in cases:
        rec, _action = _pilot(db, metadata_overrides=overrides,
                              remove_fields=remove)
        with exec_settings(**_settings()):
            _result, order = _preview(db, rec)
        assert order["valid"] is False, (overrides, remove)
        assert order["pilot_reference_error"] == code, (overrides, remove)


# ═══════════════════════════════════════════════════════════════════
# Preview determinism and the HMAC
# ═══════════════════════════════════════════════════════════════════


def test_changing_the_pilot_reference_changes_the_hash(db):
    """Test 14."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db, exact_notional=41.26)

    with exec_settings(**_settings()):
        first, _ = _preview(db, rec)

    rec.metadata_json = {**rec.metadata_json, "exact_notional_at_creation": 45.00}
    db.commit()

    with exec_settings(**_settings()):
        second, _ = _preview(db, rec)

    assert first["preview_hash"] != second["preview_hash"]


def test_the_preview_is_deterministic_across_rebuilds(db):
    """Test 15. `approve` rebuilds the preview server-side to verify the HMAC,
    so the reference must be a pure function of stored state — no quote, no
    clock, no network."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db)

    with exec_settings(**_settings()):
        first, first_order = _preview(db, rec)
        second, second_order = _preview(db, rec)

    assert first_order["snapshot_price_ref"] == second_order["snapshot_price_ref"]
    assert first_order["price_ref_source"] == second_order["price_ref_source"]
    # The hashes differ only by generated_at, so compare the signed order body.
    assert first["orders_preview"][0]["estimated_notional"] == \
        second["orders_preview"][0]["estimated_notional"]


def test_the_preview_makes_no_network_call(db):
    """A GET preview must have no side effects and no broker."""
    import inspect

    from app.services import execution

    signature = inspect.signature(execution.build_execution_preview)
    assert "broker" not in signature.parameters
    source = inspect.getsource(execution.pilot_reference_price)
    for forbidden in ("get_execution_quote", "broker", "httpx", "commit"):
        assert forbidden not in source, forbidden


# ═══════════════════════════════════════════════════════════════════
# Preflight: the reference anchors deviation, the fresh quote prices
# ═══════════════════════════════════════════════════════════════════


def _quote(price, *, side="buy", age_seconds=0):
    stamp = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "available": True,
        "source": "bid" if side == "sell" else "ask",
        "price": price,
        "retrieved_at": stamp.isoformat(),
        "market": "bCBA", "settlement": "t1",
    }


def _preflight(db, *, reference, fresh_price, side="buy", quote_override=None,
               settings_extra=None):
    """Run the real preflight for ONE order. Sends nothing."""
    from app.broker.execution_scope import effective_policy_for, load_authorization_context
    from app.services.execution import _preflight_one_order

    _come(db)
    _snapshot(db, positions=(("COME", 10, 41.0),))
    rec, _action = _pilot(db, side=side, exact_notional=reference, quantity=1)

    with exec_settings(**_settings(**(settings_extra or {}))) as settings:
        _result, preview_order = _preview(db, rec)

        order_exec = OrderExecution(
            recommendation_id=rec.id,
            recommendation_action_id=preview_order["recommendation_action_id"],
            symbol="COME", side=side, quantity=1, quantity_planned=1,
            target_change_pct=(0.0001 if side == "buy" else -0.0001),
            status="preflight",
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
            "currency": "ARS", "market_value": 410.0,
        }]
        detail: dict = {}
        result, error = _preflight_one_order(
            order_exec, preview_order, broker, settings,
            live_positions, Decimal("100000"), policies,
            db=db,
            live_cash_by_currency={"ARS": {"available": True, "cash": 100000.0,
                                           "currency": "ARS", "committed": 0.0}},
            pending_buys={}, trade_date="2026-08-07",
            daily_reserved={}, cash_reserved={},
            error_detail=detail,
        )
    return result, error, broker, preview_order


def test_a_move_inside_two_percent_passes_the_deviation_guard(db):
    """Test 16. Reference 41.26, fresh 41.50 → 0.58%."""
    result, error, broker, _preview_order = _preflight(
        db, reference=41.26, fresh_price=41.50
    )

    assert error != "price_deviation_exceeded"
    if error is None:
        assert result is not None
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()


def test_a_move_beyond_two_percent_is_refused_before_sending(db):
    """Test 17. The guard is NOT relaxed — it is merely anchored correctly."""
    _result, error, broker, _preview_order = _preflight(
        db, reference=41.26, fresh_price=45.00
    )

    assert error == "price_deviation_exceeded"
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()


def test_the_production_case_no_longer_fails_on_an_unrelated_price(db):
    """Reference 41.26 (what the pilot observed) vs fresh 41.26 → 0%.

    Anchored to the stale catalog's 43.23 this was 4.6% and blocked.
    """
    _result, error, _broker, preview_order = _preflight(
        db, reference=41.26, fresh_price=41.25
    )

    assert preview_order["snapshot_price_ref"] == pytest.approx(41.26)
    assert error != "price_deviation_exceeded"


def test_the_order_is_priced_with_the_fresh_quote_not_the_reference(db):
    """Tests 18 and 19. The stored reference sizes and anchors; it never
    becomes the price sent."""
    result, error, _broker, _preview_order = _preflight(
        db, reference=41.25, fresh_price=41.30
    )

    assert error is None, error
    precio = result["order_request"]["form_data"]["precio"]
    assert Decimal(precio) == Decimal("41.3")
    assert Decimal(precio) != Decimal("41.25")


def test_the_dynamic_tick_is_still_resolved_from_the_fresh_price(db):
    """Test 20."""
    result, error, _broker, _preview_order = _preflight(
        db, reference=41.25, fresh_price=41.30
    )

    assert error is None, error
    tick = result["audit"]["price_tick"]
    assert tick["price_tick_mode"] == "dynamic"
    assert tick["price_tick_rule"] == RULE_BYMA_EQUITY_V1
    assert tick["effective_price_tick"] == "0.05"
    assert tick["reference_price_used_for_tick"] == "41.3"


def test_a_band_change_still_requires_a_new_preview(db):
    """Test 21. PR #144's guard is untouched: reference 49.95 (band >10<=50),
    fresh 50.05 (band >50<=100)."""
    _result, error, broker, _preview_order = _preflight(
        db, reference=49.95, fresh_price=50.05,
        settings_extra={"execution_max_price_deviation_pct": 0.5},
    )

    assert error == "price_tick_changed_repreview_required"
    broker.place_order.assert_not_called()


def test_a_stale_quote_still_blocks(db):
    """Test 24-adjacent: quote freshness is unchanged."""
    _result, error, broker, _preview_order = _preflight(
        db, reference=41.26, fresh_price=41.25,
        quote_override=_quote(41.25, age_seconds=100000),
    )
    assert error == "quote_stale"
    broker.place_order.assert_not_called()


def test_a_quote_from_the_wrong_side_still_blocks(db):
    _result, error, broker, _preview_order = _preflight(
        db, reference=41.26, fresh_price=41.25,
        quote_override=_quote(41.25, side="sell"),   # a bid, for a buy
    )
    assert error == "quote_unavailable"
    broker.place_order.assert_not_called()


def test_no_preflight_branch_reaches_an_order_endpoint(db):
    """Every blocking branch, and none may have sent anything."""
    scenarios = [
        dict(reference=41.26, fresh_price=45.00),
        dict(reference=49.95, fresh_price=50.05,
             settings_extra={"execution_max_price_deviation_pct": 0.5}),
        dict(reference=41.26, fresh_price=41.25,
             quote_override=_quote(41.25, age_seconds=100000)),
    ]
    for scenario in scenarios:
        _result, error, broker, _preview_order = _preflight(db, **scenario)
        assert error is not None, scenario
        for method in ("place_order", "submit_order_request", "cancel_order",
                       "submit_fund_request"):
            assert not getattr(broker, method).called, (method, scenario)


# ═══════════════════════════════════════════════════════════════════
# Everything else stays as it was
# ═══════════════════════════════════════════════════════════════════


def test_the_global_execution_lock_still_blocks_the_preview(db):
    """Test 26. With ORDER_EXECUTION_ENABLED=false the ONLY global blocker
    should be execution_locked — the production expectation for Rec 15."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db)

    with exec_settings(**_settings(order_execution_enabled=False)):
        result, order = _preview(db, rec)

    assert result["can_submit_approval"] is False
    assert "execution_locked" in result["blocking_reasons"]
    assert order["valid"] is True, order["blocked_reason"]


def test_securities_buy_disabled_still_blocks(db):
    """Test 27."""
    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db)

    with exec_settings(**_settings(securities_buy_enabled=False,
                                   execution_sell_only=True)):
        result, _order = _preview(db, rec)

    assert result["can_submit_approval"] is False


def test_live_cash_is_still_required_for_a_buy(db):
    """Test 22."""
    from app.broker.execution_scope import effective_policy_for, load_authorization_context
    from app.services.execution import _preflight_one_order

    _come(db)
    _snapshot(db)
    rec, _action = _pilot(db)

    with exec_settings(**_settings()) as settings:
        _result, preview_order = _preview(db, rec)
        order_exec = OrderExecution(
            recommendation_id=rec.id,
            recommendation_action_id=preview_order["recommendation_action_id"],
            symbol="COME", side="buy", quantity=1, quantity_planned=1,
            target_change_pct=0.0001, status="preflight",
        )
        db.add(order_exec)
        db.commit()

        broker = mock.MagicMock()
        broker.get_execution_quote.return_value = _quote(41.25)
        context = load_authorization_context(settings)
        _result2, error = _preflight_one_order(
            order_exec, preview_order, broker, settings, [], Decimal("100000"),
            {"COME": effective_policy_for(db, "COME", settings, context)},
            db=db,
            # No live cash at all.
            live_cash_by_currency={},
            pending_buys={}, trade_date="2026-08-07",
            daily_reserved={}, cash_reserved={},
        )

    assert error in ("live_cash_unavailable", "insufficient_live_cash",
                     "currency_cash_mismatch")
    broker.place_order.assert_not_called()


def test_live_position_is_still_required_for_a_sell(db):
    """Test 23. No short selling."""
    from app.broker.execution_scope import effective_policy_for, load_authorization_context
    from app.services.execution import _preflight_one_order

    _come(db)
    _snapshot(db, positions=(("COME", 10, 41.0),))
    rec, _action = _pilot(db, side="sell", exact_notional=41.05)

    with exec_settings(**_settings(securities_sell_enabled=True)) as settings:
        _result, preview_order = _preview(db, rec)
        order_exec = OrderExecution(
            recommendation_id=rec.id,
            recommendation_action_id=preview_order["recommendation_action_id"],
            symbol="COME", side="sell", quantity=1, quantity_planned=1,
            target_change_pct=-0.0001, status="preflight",
        )
        db.add(order_exec)
        db.commit()

        broker = mock.MagicMock()
        broker.get_execution_quote.return_value = _quote(41.05, side="sell")
        context = load_authorization_context(settings)
        _result2, error = _preflight_one_order(
            order_exec, preview_order, broker, settings,
            [],  # no live positions
            Decimal("100000"),
            {"COME": effective_policy_for(db, "COME", settings, context)},
            db=db, live_cash_by_currency={}, pending_buys={},
            trade_date="2026-08-07", daily_reserved={}, cash_reserved={},
        )

    assert error is not None
    broker.place_order.assert_not_called()


def test_the_deviation_tolerance_was_not_widened():
    """The fix anchors the guard correctly; it does not loosen it."""
    assert CLASS_POLICIES["ACCIONES"]["max_price_deviation_pct"] == 0.02

    from app.core.config import Settings

    # The global default is unchanged, and the preflight reads the tolerance
    # from the policy/settings rather than carrying one of its own.
    assert Settings.model_fields["execution_max_price_deviation_pct"].default == 0.0


def test_the_scheduler_and_the_llm_still_cannot_execute():
    """Test 28."""
    import ast
    import pathlib

    forbidden = {
        "approve_and_execute", "place_order", "submit_order_request",
        "prepare_validated_execution_batch", "_submit_prepared_order",
        "create_securities_pilot_recommendation",
    }
    for path in ("app/scheduler/jobs.py", "app/services/orchestrator.py",
                 "app/llm/explainer.py", "app/news/ingestion.py"):
        tree = ast.parse(pathlib.Path(path).read_text())
        called = {
            node.func.id if isinstance(node.func, ast.Name) else
            (node.func.attr if isinstance(node.func, ast.Attribute) else "")
            for node in ast.walk(tree) if isinstance(node, ast.Call)
        }
        assert not (called & forbidden), path


def test_nothing_here_writes_to_a_recommendation(db):
    """Test 29. Recommendation 15 is a live pending pilot: the preview path
    must read it and never write it."""
    _come(db)
    _snapshot(db)
    rec = Recommendation(
        id=15, action="rebalancear", status="pending", suggested_pct=0.0,
        confidence=1.0, rationale="piloto COME", risks="",
        executive_summary="[PILOTO] Comprar 1 COME",
        metadata_json={
            "execution_pilot": True, "pilot_type": "security_buy",
            "symbol": "COME", "side": "buy", "quantity": 1,
            "exact_notional_at_creation": 41.26,
        },
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=15, symbol="COME", target_change_pct=0.0001,
        reason="piloto", quantity_override=1,
    ))
    db.commit()
    before = dict(rec.metadata_json)

    with exec_settings(**_settings()):
        result, order = _preview(db, rec)
        _preview(db, rec)

    db.refresh(rec)
    assert rec.status == "pending"
    assert rec.metadata_json == before
    assert result["recommendation_id"] == 15
    assert order["snapshot_price_ref"] == pytest.approx(41.26)
    assert db.query(OrderExecution).count() == 0
