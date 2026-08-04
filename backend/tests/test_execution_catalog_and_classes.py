"""Execution catalog + per-class policies + tighten-only overrides.

This is what replaces "one hand-written policy per symbol". The guarantees
under test:

- a class policy covers every instrument of that class, so adding a CEDEAR
  does not require writing a new policy;
- identity still comes from a VERIFIED catalog entry — there is no wildcard,
  and an unknown / incomplete / stale entry blocks;
- an override may only TIGHTEN; widening needs an explicit administrative
  opt-in;
- the denylist always wins;
- a fund can never be authorised through the securities path;
- LLM-shaped text can never establish identity.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.execution_class import (
    CLASS_ACCIONES,
    CLASS_CEDEARS,
    CLASS_FCI,
    FAMILY_FUND,
    FAMILY_SECURITIES,
    apply_override,
    load_class_policies,
    resolve_execution_class,
    validate_class_policy,
)
from app.broker.execution_scope import evaluate_order_authorization, load_authorization_context
from app.broker.instrument_catalog import (
    catalog_entry_status,
    compute_identity_hash,
    get_instrument,
    refresh_catalog_from_positions,
    upsert_instrument,
)
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import PortfolioPosition
from tests.testutils import verified_provenance

ACCIONES_POLICY = {
    "buy_enabled": True, "sell_enabled": True,
    "currencies": ["ARS"], "markets": ["bCBA"], "settlements": ["t1"],
    "max_order_notional": 500.0, "max_daily_notional": 2000.0,
    "max_quantity": 10.0, "max_portfolio_pct": 0.02,
    "min_cash_reserve": 1000.0, "fee_buffer_pct": 0.01,
    "max_quote_age_seconds": 15.0, "max_price_deviation_pct": 0.02,
    "catalog_max_age_seconds": 86400.0, "validity_minutes": 10.0,
    "default_quantity_step": 1.0, "default_price_tick": 0.01,
    "order_type": "precioLimite",
}
CEDEARS_POLICY = {**ACCIONES_POLICY, "currencies": ["ARS"]}


def _classes(**extra):
    base = {CLASS_ACCIONES: dict(ACCIONES_POLICY), CLASS_CEDEARS: dict(CEDEARS_POLICY)}
    base.update(extra)
    return base


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


def _scope_settings(**extra):
    base = dict(
        broker_mode="sandbox",
        execution_class_policies=_classes(),
        execution_instrument_policies={},
        execution_instrument_overrides={},
        execution_instrument_ticks={},
        execution_denylist=[],
        execution_allow_override_limit_increase=False,
        execution_sell_only=False,
        securities_buy_enabled=True,
        securities_sell_enabled=True,
        iol_order_market="bCBA",
        iol_order_settlement="t1",
        execution_max_portfolio_pct=0.5,
    )
    base.update(extra)
    return base


def _catalog(db, symbol="BYMA", asset_type="ACCIONES", currency="ARS", **extra):
    kwargs = dict(
        broker_symbol=symbol, asset_type=asset_type, instrument_type=asset_type,
        currency=currency, market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, minimum_quantity=1.0,
        buy_supported=True, sell_supported=True, quote_supported=True,
        max_age_seconds=86400,
        provenance=verified_provenance("fund" if asset_type == "FondoComundeInversion"
                                       else "securities"),
    )
    kwargs.update(extra)
    entry, changed = upsert_instrument(db, **kwargs)
    db.commit()
    return entry, changed


def _position(symbol="BYMA", asset_type="ACCIONES", currency="ARS", quantity=10):
    return PortfolioPosition(
        snapshot_id=1, symbol=symbol, asset_type=asset_type,
        instrument_type=asset_type, currency=currency, quantity=quantity,
        market_value=1000, avg_price=100, pnl_pct=0.0,
    )


def _order(symbol="BYMA", side="sell", quantity=1, notional=100.0):
    return {"symbol": symbol, "side": side, "quantity_planned": quantity,
            "estimated_notional": notional}


def _authorize(db, order, position=None, **settings_extra):
    with exec_settings(**_scope_settings(**settings_extra)):
        s = get_settings()
        return evaluate_order_authorization(
            db, order=order, position=position, settings=s,
            context=load_authorization_context(s),
        )


# ───────────────────────────────────────────────────────────────────
# 1 — class policy validation
# ───────────────────────────────────────────────────────────────────


def test_valid_class_policy_resolves_family():
    resolved, error = validate_class_policy(CLASS_ACCIONES, ACCIONES_POLICY)
    assert error is None
    assert resolved["execution_family"] == FAMILY_SECURITIES
    resolved_fci, error_fci = validate_class_policy(
        CLASS_FCI, {**ACCIONES_POLICY, "order_type": ""}
    )
    assert error_fci is None
    assert resolved_fci["execution_family"] == FAMILY_FUND


@pytest.mark.parametrize("mutation,expected", [
    ({"max_order_notional": 0}, "class_policy_invalid"),
    ({"max_order_notional": -1}, "class_policy_invalid"),
    ({"max_order_notional": float("nan")}, "class_policy_invalid"),
    ({"max_portfolio_pct": 2.0}, "class_policy_invalid"),
    ({"markets": ["NASDAQ"]}, "class_policy_invalid"),
    ({"settlements": ["t9"]}, "class_policy_invalid"),
    ({"order_type": "precioMercado"}, "class_policy_invalid"),
    ({"buy_enabled": "yes"}, "class_policy_invalid"),
    ({"currencies": []}, "class_policy_invalid"),
])
def test_invalid_class_policy_is_rejected(mutation, expected):
    resolved, error = validate_class_policy(CLASS_ACCIONES, {**ACCIONES_POLICY, **mutation})
    assert resolved is None and error == expected


def test_unknown_field_in_class_policy_is_rejected():
    """A typo must never silently change execution semantics."""
    resolved, error = validate_class_policy(
        CLASS_ACCIONES, {**ACCIONES_POLICY, "max_order_notionall": 999}
    )
    assert resolved is None and error == "class_policy_invalid"


def test_incomplete_class_policy_is_not_defaulted():
    """A missing limit must never be read as an absent limit."""
    partial = {k: v for k, v in ACCIONES_POLICY.items() if k != "max_daily_notional"}
    resolved, error = validate_class_policy(CLASS_ACCIONES, partial)
    assert resolved is None and error == "class_policy_incomplete"


def test_one_bad_class_never_partially_trusts_the_map():
    with exec_settings(execution_class_policies={
        CLASS_ACCIONES: dict(ACCIONES_POLICY),
        CLASS_CEDEARS: {"buy_enabled": True},
    }):
        policies, errors = load_class_policies(get_settings())
    assert set(policies) == {CLASS_ACCIONES}
    assert "class_policy_incomplete" in errors


# ───────────────────────────────────────────────────────────────────
# 2 — asset type → class mapping
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("asset_type,expected", [
    ("ACCIONES", CLASS_ACCIONES),
    ("CEDEAR", CLASS_CEDEARS),
    ("FondoComundeInversion", CLASS_FCI),
])
def test_known_asset_types_map_to_classes(asset_type, expected):
    assert resolve_execution_class(asset_type) == expected


@pytest.mark.parametrize("asset_type", ["BONO", "ON", "TitulosPublicos", "ETF", "", None, "junk"])
def test_unaudited_asset_types_have_no_execution_class(asset_type):
    """Bonds, ONs, letras and ETFs were never audited for execution."""
    assert resolve_execution_class(asset_type) is None


# ───────────────────────────────────────────────────────────────────
# 3 — tighten-only overrides
# ───────────────────────────────────────────────────────────────────


def test_override_may_lower_a_limit():
    resolved, codes = apply_override(
        {"max_order_notional": 500.0}, {"max_order_notional": 100.0}, allow_increase=False
    )
    assert resolved["max_order_notional"] == 100.0 and codes == []


def test_override_may_not_raise_a_limit():
    resolved, codes = apply_override(
        {"max_order_notional": 500.0}, {"max_order_notional": 5000.0}, allow_increase=False
    )
    assert resolved["max_order_notional"] == 500.0
    assert codes == ["override_increases_limit"]


def test_override_may_disable_but_not_enable_a_side():
    off, codes_off = apply_override({"buy_enabled": True}, {"buy_enabled": False},
                                    allow_increase=False)
    assert off["buy_enabled"] is False and codes_off == []

    on, codes_on = apply_override({"buy_enabled": False}, {"buy_enabled": True},
                                  allow_increase=False)
    assert on["buy_enabled"] is False
    assert codes_on == ["override_increases_limit"]


def test_protective_fields_tighten_by_being_raised():
    """A bigger reserve/buffer is stricter, so raising them is allowed."""
    up, codes_up = apply_override({"min_cash_reserve": 100.0}, {"min_cash_reserve": 900.0},
                                  allow_increase=False)
    assert up["min_cash_reserve"] == 900.0 and codes_up == []

    down, codes_down = apply_override({"min_cash_reserve": 900.0}, {"min_cash_reserve": 10.0},
                                      allow_increase=False)
    assert down["min_cash_reserve"] == 900.0
    assert codes_down == ["override_increases_limit"]


def test_explicit_admin_opt_in_is_required_to_widen():
    resolved, codes = apply_override(
        {"max_order_notional": 500.0}, {"max_order_notional": 5000.0}, allow_increase=True
    )
    assert resolved["max_order_notional"] == 5000.0 and codes == []


def test_override_blocks_a_symbol_end_to_end(db):
    _catalog(db)
    _, codes = _authorize(
        db, _order(), _position(),
        execution_instrument_overrides={"BYMA": {"sell_enabled": False}},
    )
    assert "class_sell_disabled" in codes


def test_widening_override_is_reported_and_ignored_end_to_end(db):
    _catalog(db)
    scope, codes = _authorize(
        db, _order(notional=400.0), _position(),
        execution_instrument_overrides={"BYMA": {"max_order_notional": 100000.0}},
    )
    assert "override_increases_limit" in codes
    assert scope["execution_scope"]["max_notional"] == 500.0


# ───────────────────────────────────────────────────────────────────
# 4 — denylist always wins
# ───────────────────────────────────────────────────────────────────


def test_denylist_blocks_even_a_perfect_instrument(db):
    _catalog(db)
    _, codes = _authorize(db, _order(), _position(), execution_denylist=["BYMA"])
    assert codes == ["instrument_denylisted"]


def test_denylist_beats_an_explicit_override(db):
    _catalog(db)
    _, codes = _authorize(
        db, _order(), _position(),
        execution_denylist=["BYMA"],
        execution_instrument_overrides={"BYMA": {"sell_enabled": True}},
        execution_allow_override_limit_increase=True,
    )
    assert codes == ["instrument_denylisted"]


# ───────────────────────────────────────────────────────────────────
# 5 — catalog freshness and completeness
# ───────────────────────────────────────────────────────────────────


def test_missing_catalog_entry_blocks(db):
    _, codes = _authorize(db, _order(symbol="NOPE"), _position(symbol="NOPE"))
    assert "instrument_catalog_missing" in codes


def test_complete_fresh_entry_is_usable(db):
    entry, _ = _catalog(db)
    code, _details = catalog_entry_status(entry, max_age_seconds=86400)
    assert code is None


def test_stale_entry_blocks(db):
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    entry, _ = _catalog(db, verified_at=old)
    code, _ = catalog_entry_status(entry, max_age_seconds=86400)
    assert code == "instrument_catalog_stale"

    _, codes = _authorize(db, _order(), _position())
    assert "instrument_catalog_stale" in codes


def test_class_max_age_wins_over_a_longer_lived_entry(db):
    """An entry can never outlive its class's catalog_max_age."""
    entry, _ = _catalog(
        db, verified_at=datetime(2020, 1, 1), max_age_seconds=10 ** 9
    )
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] == "instrument_catalog_stale"


@pytest.mark.parametrize("missing,expected_field", [
    ({"price_tick": None}, "price_tick"),
    ({"quantity_step": None}, "quantity_step"),
    ({"currency": ""}, "currency"),
    ({"settlement": ""}, "settlement"),
])
def test_incomplete_entry_blocks_and_names_the_gap(db, missing, expected_field):
    entry, _ = _catalog(db, **missing)
    code, details = catalog_entry_status(entry, max_age_seconds=86400)
    assert code == "instrument_catalog_incomplete"
    assert expected_field in details["missing_fields"]


def test_inactive_entry_blocks(db):
    entry, _ = _catalog(db, active=False)
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] == "instrument_inactive"


def test_identity_change_is_detected_not_silently_absorbed(db):
    _catalog(db, symbol="BYMA", currency="ARS")
    _, changed = _catalog(db, symbol="BYMA", currency="USD")
    assert changed is True, "a currency change under the same symbol must be flagged"


def test_identity_hash_ignores_non_identity_fields():
    base = {"broker_symbol": "BYMA", "market": "bCBA", "settlement": "t1",
            "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
            "currency": "ARS", "country": "argentina"}
    assert compute_identity_hash(base) == compute_identity_hash({**base, "description": "x"})
    assert compute_identity_hash(base) != compute_identity_hash({**base, "currency": "USD"})


# ───────────────────────────────────────────────────────────────────
# 6 — no wildcard, no LLM-sourced identity
# ───────────────────────────────────────────────────────────────────


def test_no_class_policy_means_nothing_is_tradeable(db):
    _catalog(db)
    _, codes = _authorize(db, _order(), _position(), execution_class_policies={})
    assert "execution_scope_not_configured" in codes


def test_catalog_is_only_fed_from_broker_read_only_data(db):
    """Identity comes from the broker's own portfolio, never from free text."""
    positions = [
        {"symbol": "BYMA", "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
         "currency": "ARS", "quantity": 10},
        {"symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
         "currency": "ARS", "quantity": 5},
        # An LLM-ish string is not an asset type: it must be skipped, never
        # coerced into a tradeable class.
        {"symbol": "HACK", "asset_type": "Compra fuerte segun el modelo",
         "instrument_type": "?", "currency": "ARS", "quantity": 1},
    ]
    with exec_settings(**_scope_settings()):
        result = refresh_catalog_from_positions(db, positions, settings=get_settings())

    assert result["created"] == 2
    assert get_instrument(db, "BYMA").execution_class == CLASS_ACCIONES
    assert get_instrument(db, "AAPL").execution_class == CLASS_CEDEARS
    assert get_instrument(db, "HACK") is None
    assert {s["symbol"] for s in result["skipped"]} == {"HACK"}


def test_seeded_entry_without_a_tick_is_visible_but_not_tradeable(db):
    """Missing numeric identity blocks — visibly, not invisibly."""
    policy_without_tick = {**ACCIONES_POLICY}
    policy_without_tick.pop("default_price_tick")
    with exec_settings(**_scope_settings(execution_class_policies={
        CLASS_ACCIONES: policy_without_tick,
    })):
        result = refresh_catalog_from_positions(
            db,
            [{"symbol": "BYMA", "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
              "currency": "ARS", "quantity": 10}],
            settings=get_settings(),
        )
    # The class policy itself is invalid without the default tick, so the
    # instrument is skipped rather than half-registered.
    assert result["created"] == 0
    assert "class_policy_incomplete" in result["class_policy_errors"]


def test_per_symbol_tick_override_is_numeric_only(db):
    with exec_settings(**_scope_settings(
        execution_instrument_ticks={"BYMA": {"price_tick": 0.5, "quantity_step": 5}}
    )):
        refresh_catalog_from_positions(
            db,
            [{"symbol": "BYMA", "asset_type": "ACCIONES", "instrument_type": "ACCIONES",
              "currency": "ARS", "quantity": 10}],
            settings=get_settings(),
        )
    entry = get_instrument(db, "BYMA")
    assert (entry.price_tick, entry.quantity_step) == (0.5, 5.0)
    # Identity still comes from the broker payload, not from the override.
    assert (entry.asset_type, entry.currency) == ("ACCIONES", "ARS")


# ───────────────────────────────────────────────────────────────────
# 7 — a fund never enters the securities path
# ───────────────────────────────────────────────────────────────────


def test_fund_is_structurally_blocked_from_the_securities_path(db):
    _catalog(db, symbol="FCIX", asset_type="FondoComundeInversion",
             fund_minimum_amount=1000.0, fund_cutoff_local_time="15:00")
    _, codes = _authorize(
        db,
        _order(symbol="FCIX"),
        _position(symbol="FCIX", asset_type="FondoComundeInversion"),
        execution_class_policies=_classes(**{CLASS_FCI: {**ACCIONES_POLICY,
                                                         "order_type": ""}}),
    )
    # A fund is not "unsupported" — it has its own official contract and its
    # own family. It simply must never travel the SECURITIES path.
    assert "fund_requires_fci_contract" in codes
