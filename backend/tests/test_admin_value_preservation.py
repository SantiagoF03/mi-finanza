"""An administratively verified VALUE survives an automatic refresh.

The production bug: COME had `quantity_step=1` and `minimum_quantity=1`
verified by a human. A `POST /api/broker/instruments/resolve` — whose class
policy could only offer `default_quantity_step=null` — wiped both values while
carefully preserving their `admin_verified_override` labels. The row ended up
saying:

    quantity_step = None
    field_provenance["quantity_step"] = "admin_verified_override"
    verification_status = "verified"

A verified value that no longer exists. The catalog reported `ok`, the
resolver's own response said `verified` with `unverified_fields: []`, and the
next request said `instrument_catalog_incomplete`.

Two things were wrong, and both are fixed here:

1. the value and its provenance were merged by different rules, in different
   places — so they could disagree;
2. `unverified_fields()` trusted the label without checking that the value it
   certifies is still usable.

Nothing here touches IOL: every broker is a double.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.instrument_catalog import (
    PROV_ADMIN_OVERRIDE,
    PROV_CLASS_DEFAULT,
    PROV_IOL_QUOTE,
    PROV_IOL_TITLE_DETAIL,
    catalog_entry_status,
    get_instrument,
    recompute_verification_status,
    unverified_fields,
    upsert_instrument,
    verified_price_tick_rule,
    verify_fields,
)
from app.broker.price_rules import RULE_BYMA_EQUITY_V1
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import Recommendation

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


@pytest.fixture
def db(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path}/catalog.db", connect_args={"check_same_thread": False}
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


IDENTITY = dict(
    asset_type="ACCIONES", instrument_type="ACCIONES", currency="ARS",
    market="bCBA", settlement="t1", country="argentina",
)

BROKER_PROVENANCE = {
    "broker_symbol": PROV_IOL_TITLE_DETAIL,
    "asset_type": PROV_IOL_TITLE_DETAIL,
    "instrument_type": PROV_IOL_TITLE_DETAIL,
    "currency": PROV_IOL_TITLE_DETAIL,
    "market": PROV_IOL_TITLE_DETAIL,
    "buy_supported": PROV_IOL_QUOTE,
    "sell_supported": PROV_IOL_QUOTE,
    "quote_supported": PROV_IOL_QUOTE,
}


def _seed(db, symbol="COME", *, quantity_step=1.0, minimum_quantity=1.0,
          price_tick=None, admin_fields=("quantity_step", "minimum_quantity")):
    """An instrument whose numeric fields a human verified administratively."""
    provenance = dict(BROKER_PROVENANCE)
    for field in admin_fields:
        provenance[field] = PROV_ADMIN_OVERRIDE
    entry, _ = upsert_instrument(
        db, broker_symbol=symbol, source="seed", provenance=provenance,
        quantity_step=quantity_step, minimum_quantity=minimum_quantity,
        price_tick=price_tick, buy_supported=True, sell_supported=True,
        quote_supported=True, **IDENTITY,
    )
    db.commit()
    return entry


def _refresh(db, symbol="COME", *, quantity_step=None, minimum_quantity=None,
             price_tick=None, provenance=None, **identity_overrides):
    """An automatic refresh, exactly as /resolve performs one: the class
    policy can only offer defaults for the numeric fields."""
    merged = dict(BROKER_PROVENANCE)
    merged.update({
        "quantity_step": PROV_CLASS_DEFAULT,
        "minimum_quantity": PROV_CLASS_DEFAULT,
        "price_tick": PROV_CLASS_DEFAULT,
    })
    merged.update(provenance or {})
    identity = {**IDENTITY, **identity_overrides}
    entry, changed = upsert_instrument(
        db, broker_symbol=symbol, source="resolver:universe", provenance=merged,
        quantity_step=quantity_step, minimum_quantity=minimum_quantity,
        price_tick=price_tick, buy_supported=True, sell_supported=True,
        quote_supported=True, **identity,
    )
    db.commit()
    return entry, changed


# ═══════════════════════════════════════════════════════════════════
# Cases A–E from the specification
# ═══════════════════════════════════════════════════════════════════


def test_case_a_an_admin_quantity_step_survives_a_null_refresh(db):
    """The exact production failure."""
    _seed(db)
    entry, _ = _refresh(db, quantity_step=None)

    assert entry.quantity_step == 1.0
    assert entry.field_provenance["quantity_step"] == PROV_ADMIN_OVERRIDE


def test_case_a_an_admin_minimum_quantity_survives_a_null_refresh(db):
    _seed(db)
    entry, _ = _refresh(db, minimum_quantity=None)

    assert entry.minimum_quantity == 1.0
    assert entry.field_provenance["minimum_quantity"] == PROV_ADMIN_OVERRIDE


def test_case_a_an_admin_fixed_price_tick_survives_a_null_refresh(db):
    """Instruments that legitimately use a FIXED tick are protected too."""
    _seed(db, price_tick=0.25, admin_fields=("quantity_step", "minimum_quantity",
                                             "price_tick"))
    entry, _ = _refresh(db, price_tick=None)

    assert entry.price_tick == 0.25
    assert entry.field_provenance["price_tick"] == PROV_ADMIN_OVERRIDE


def test_case_b_a_different_class_default_does_not_overwrite_an_admin_value(db):
    """A guess is not more authoritative because it is non-null.

    A class default of 100 replacing a human-verified 1 would silently make
    every order of this instrument a hundred times bigger than approved.
    """
    _seed(db)
    entry, _ = _refresh(db, quantity_step=100.0, minimum_quantity=100.0)

    assert entry.quantity_step == 1.0
    assert entry.minimum_quantity == 1.0
    assert entry.field_provenance["quantity_step"] == PROV_ADMIN_OVERRIDE


def test_case_c_an_explicit_administrative_update_does_replace_the_value(db):
    """This is how an operator corrects their own earlier reading."""
    entry = _seed(db)
    result = verify_fields(
        entry, fields={"quantity_step": 2.0},
        provenance_source=PROV_ADMIN_OVERRIDE,
        note="Lote mínimo corregido contra la ficha del instrumento.",
    )
    db.commit()

    assert result["changed"] is True
    assert entry.quantity_step == 2.0
    assert entry.field_provenance["quantity_step"] == PROV_ADMIN_OVERRIDE


def test_case_c_an_admin_refresh_also_replaces_the_value(db):
    """Same rule through the upsert path: admin outranks admin."""
    _seed(db)
    entry, _ = _refresh(
        db, quantity_step=2.0,
        provenance={"quantity_step": PROV_ADMIN_OVERRIDE},
    )
    assert entry.quantity_step == 2.0


def test_case_d_without_a_prior_admin_value_the_refresh_stays_fail_closed(db):
    """Nothing to preserve ⇒ nothing is invented."""
    _refresh(db, symbol="NUEVO", quantity_step=None)
    entry = get_instrument(db, "NUEVO")

    assert entry.quantity_step is None
    assert entry.field_provenance["quantity_step"] == PROV_CLASS_DEFAULT
    assert "quantity_step" in unverified_fields(entry)
    assert recompute_verification_status(entry) == "candidate"
    assert catalog_entry_status(entry)[0] is not None


def test_case_e_a_genuinely_authoritative_source_does_replace_an_admin_value(db):
    """The preserved case is exactly one: admin verified, refresh could only
    guess.

    If IOL itself now states a different minimum lot, the venue is telling us
    something the human's earlier reading no longer matches. Keeping the old
    number there would be preferring a stale note over the broker — so a real
    source wins, and the provenance moves with it.
    """
    _seed(db)
    entry, _ = _refresh(
        db, quantity_step=5.0,
        provenance={"quantity_step": PROV_IOL_TITLE_DETAIL},
    )

    assert entry.quantity_step == 5.0
    assert entry.field_provenance["quantity_step"] == PROV_IOL_TITLE_DETAIL


def test_the_value_and_the_provenance_never_disagree(db):
    """The invariant the bug violated, stated directly.

    Whichever side wins, the value and the label move together.
    """
    scenarios = [
        ({"quantity_step": None}, None),
        ({"quantity_step": 100.0}, None),
        ({"quantity_step": 7.0}, {"quantity_step": PROV_IOL_TITLE_DETAIL}),
        ({"quantity_step": 3.0}, {"quantity_step": PROV_ADMIN_OVERRIDE}),
    ]
    for index, (values, provenance) in enumerate(scenarios):
        symbol = f"SYM{index}"
        _seed(db, symbol=symbol)
        entry, _ = _refresh(db, symbol=symbol, provenance=provenance, **values)

        origin = entry.field_provenance["quantity_step"]
        if origin == PROV_ADMIN_OVERRIDE and provenance is None:
            # Preserved: the stored admin value is still there.
            assert entry.quantity_step == 1.0, symbol
        else:
            # Replaced: the label matches the source that supplied the value.
            assert entry.quantity_step == values["quantity_step"], symbol


# ═══════════════════════════════════════════════════════════════════
# The second bug: a label must not certify a missing value
# ═══════════════════════════════════════════════════════════════════


def test_a_provenance_label_without_a_value_certifies_nothing(db):
    """Constructed directly, because the upsert can no longer produce it.

    This is the state the resolver's response reported as `verified` with
    `unverified_fields: []` while the row was unusable.
    """
    entry = _seed(db)
    entry.quantity_step = None
    db.commit()

    assert entry.field_provenance["quantity_step"] == PROV_ADMIN_OVERRIDE
    assert "quantity_step" in unverified_fields(entry)
    assert recompute_verification_status(entry) == "candidate"


def test_a_refresh_can_never_produce_the_incoherent_state(db):
    """quantity_step=None + admin provenance + verified must be unreachable."""
    _seed(db)
    entry, _ = _refresh(db, quantity_step=None, minimum_quantity=None)

    incoherent = (
        entry.quantity_step is None
        and entry.field_provenance.get("quantity_step") == PROV_ADMIN_OVERRIDE
        and entry.verification_status == "verified"
    )
    assert not incoherent
    # And in fact the value survived, so the label still certifies something.
    assert entry.quantity_step == 1.0
    assert "quantity_step" not in unverified_fields(entry)
    # This fixture has neither a fixed tick nor a rule, so it is correctly a
    # candidate — for the tick, which is a different missing field entirely.
    assert unverified_fields(entry) == ["price_tick"]


def test_a_fixed_tick_label_without_a_value_does_not_verify(db):
    entry = _seed(db, price_tick=0.25,
                  admin_fields=("quantity_step", "minimum_quantity", "price_tick"))
    entry.price_tick = None
    db.commit()

    # No dynamic rule either ⇒ nothing can price the decimals.
    assert verified_price_tick_rule(entry) is None
    assert "price_tick" in unverified_fields(entry)


# ═══════════════════════════════════════════════════════════════════
# COME end to end, and the dynamic rule
# ═══════════════════════════════════════════════════════════════════


def _come(db):
    """COME exactly as production had it before the refresh."""
    entry = _seed(db, symbol="COME")
    verify_fields(
        entry, fields={"price_tick_rule": RULE_BYMA_EQUITY_V1},
        provenance_source=PROV_ADMIN_OVERRIDE,
        note="Regla de bandas de precio BYMA/IOL verificada administrativamente.",
    )
    db.commit()
    return entry


def test_come_survives_the_refresh_that_broke_it(db):
    """The whole reproduction: verified COME, then /resolve with null defaults."""
    entry = _come(db)
    assert entry.verification_status == "verified"

    entry, identity_changed = _refresh(
        db, symbol="COME", quantity_step=None, price_tick=None,
        minimum_quantity=None,
    )

    assert identity_changed is False
    assert entry.quantity_step == 1.0
    assert entry.minimum_quantity == 1.0
    # The dynamic rule is untouched — it already survived the original bug and
    # must keep surviving.
    assert entry.price_tick_rule == RULE_BYMA_EQUITY_V1
    assert entry.field_provenance["price_tick_rule"] == PROV_ADMIN_OVERRIDE
    assert verified_price_tick_rule(entry) == RULE_BYMA_EQUITY_V1
    # And no fixed tick was invented on the way.
    assert entry.price_tick is None

    assert unverified_fields(entry) == []
    assert entry.verification_status == "verified"
    assert catalog_entry_status(entry)[0] is None


def test_a_refresh_never_converts_the_rule_into_a_fixed_tick(db):
    _come(db)
    entry, _ = _refresh(db, symbol="COME", price_tick=0.05,
                        provenance={"price_tick": PROV_CLASS_DEFAULT})

    assert entry.price_tick_rule == RULE_BYMA_EQUITY_V1
    assert verified_price_tick_rule(entry) == RULE_BYMA_EQUITY_V1


def test_readiness_agrees_after_the_refresh(db):
    """The user-visible symptom: technically_ready went false."""
    from app.services.pilot_readiness import instrument_verification

    _come(db)
    entry, _ = _refresh(db, symbol="COME", quantity_step=None,
                        minimum_quantity=None, price_tick=None)

    verification = instrument_verification(entry)
    assert verification["identity_verified"] is True
    assert verification["buy_verified"] is True
    assert verification["sell_verified"] is True
    assert verification["identity_missing"] == []


def test_the_capabilities_report_agrees_after_the_refresh(db):
    """`instrument-capabilities` is what reported the incoherence first."""
    from app.services.instrument_capabilities import build_instrument_capabilities

    _come(db)
    _refresh(db, symbol="COME", quantity_step=None, minimum_quantity=None,
             price_tick=None)

    with exec_settings(**_settings()):
        report = build_instrument_capabilities(db)

    item = next(i for i in report["instruments"] if i["symbol"] == "COME")
    assert item["catalog_status"] == "ok"
    assert item["missing_fields"] == []
    assert item["dynamic_price_tick_rule_verified"] is True
    assert item["identity"]["quantity_step"] == 1.0
    assert item["identity"]["minimum_quantity"] == 1.0


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


def _settings(**extra):
    base = dict(
        api_key="",
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        order_execution_enabled=False,
        securities_buy_enabled=False,
        securities_sell_enabled=False,
        execution_sell_only=True,
        execution_pilot_creation_enabled=False,
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


def _come_broker(*, ask=41.10, bid=41.05, cash=100000.0):
    broker = mock.MagicMock()

    def _quote(symbol, side, market, settlement):
        price = bid if side == "sell" else ask
        return {"available": True, "source": "bid" if side == "sell" else "ask",
                "price": price,
                "retrieved_at": datetime.now(timezone.utc).isoformat()}

    broker.get_execution_quote.side_effect = _quote
    broker.get_live_cash.return_value = {
        "available": True, "cash": cash, "currency": "ARS",
        "committed": 0.0, "source": "estadocuenta",
    }
    broker.get_portfolio_snapshot.return_value = {
        "total_value": 100000.0, "cash": cash,
        "positions": [{"symbol": "COME", "asset_type": "ACCIONES",
                       "instrument_type": "ACCIONES", "currency": "ARS",
                       "quantity": 10, "committed": 0, "market_value": 410.5}],
    }
    return broker


def test_pilot_readiness_is_technically_ready_again_after_the_refresh(db):
    from app.services.pilot_readiness import evaluate_pilot_readiness

    _come(db)
    _refresh(db, symbol="COME", quantity_step=None, minimum_quantity=None,
             price_tick=None)

    broker = _come_broker()
    with exec_settings(**_settings()) as settings:
        report = evaluate_pilot_readiness(
            db, symbols=["COME"], side="buy", quantity=1,
            live=True, broker=broker, settings=settings,
        )

    buy = report["symbols"][0]["buy"]
    assert buy["technically_ready"] is True, buy["technical_blocking_reasons"]
    assert buy["effective_price_tick"] == "0.05"
    # Still not permission: every lock is shut.
    assert buy["activation_ready"] is False
    assert "execution_locked" in buy["activation_blocking_reasons"]


# ═══════════════════════════════════════════════════════════════════
# The resolver's own response must describe the row it wrote
# ═══════════════════════════════════════════════════════════════════


def _resolver_broker(*, mercado="bCBA"):
    broker = mock.MagicMock()
    response = mock.MagicMock()
    response.json.return_value = {
        "simbolo": "COME", "tipo": "acciones", "moneda": "peso_Argentino",
        "mercado": mercado, "descripcion": "Sociedad Comercial del Plata",
    }
    broker._authorized_get.return_value = response
    broker.get_execution_quote.side_effect = lambda symbol, side, market, settlement: {
        "available": True, "price": 41.10,
        "source": "bid" if side == "sell" else "ask",
    }
    return broker


def test_the_resolver_response_matches_the_row_it_persisted(db):
    """The response said `verified` / `unverified_fields: []` while the stored
    row was incomplete. Whatever it reports must be re-derivable from the row.
    """
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    _come(db)
    with exec_settings(**_settings()) as settings:
        report = resolve_instrument(
            db, symbol="COME", broker=_resolver_broker(), settings=settings,
            source=SOURCE_UNIVERSE,
            class_policies={"ACCIONES": dict(CLASS_POLICIES["ACCIONES"],
                                             execution_class="ACCIONES",
                                             execution_family="securities")},
        )
    db.commit()

    entry = get_instrument(db, "COME")
    assert report["resolved"] is True
    assert report["verification_status"] == entry.verification_status
    assert report["unverified_fields"] == unverified_fields(entry)
    # And the row it describes is genuinely operable.
    assert entry.quantity_step == 1.0
    assert catalog_entry_status(entry)[0] is None


def test_a_resolver_response_that_says_verified_means_the_row_is_usable(db):
    """The property that was violated: `verified` must imply operable."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    # A symbol with NO administrative verification at all.
    with exec_settings(**_settings()) as settings:
        report = resolve_instrument(
            db, symbol="COME", broker=_resolver_broker(), settings=settings,
            source=SOURCE_UNIVERSE,
            class_policies={"ACCIONES": dict(CLASS_POLICIES["ACCIONES"],
                                             execution_class="ACCIONES",
                                             execution_family="securities")},
        )
    db.commit()

    entry = get_instrument(db, "COME")
    if report["verification_status"] == "verified":
        assert catalog_entry_status(entry)[0] is None
    else:
        # Which is the expected outcome here: no tick, no rule, no step.
        assert report["unverified_fields"]
        assert entry.quantity_step is None


# ═══════════════════════════════════════════════════════════════════
# Identity changes must still freeze
# ═══════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("field,value", [
    ("market", "NYSE"),
    ("currency", "USD"),
    ("asset_type", "BONOS"),
    ("instrument_type", "BONOS"),
])
def test_preserving_an_admin_value_never_hides_a_real_identity_change(db, field, value):
    """Keeping a human's number must not make the instrument look unchanged
    when the broker is now describing something else entirely."""
    _come(db)
    entry, identity_changed = _refresh(db, symbol="COME", **{field: value})

    assert identity_changed is True
    assert entry.verification_status == "identity_changed"
    assert entry.pending_identity is not None
    assert catalog_entry_status(entry)[0] == "instrument_identity_changed"


def test_an_identity_change_does_not_silently_rewrite_the_stored_values(db):
    """A frozen entry keeps what it had; nothing is absorbed."""
    _come(db)
    entry, _ = _refresh(db, symbol="COME", currency="USD", quantity_step=99.0)

    assert entry.verification_status == "identity_changed"
    assert entry.quantity_step == 1.0
    assert entry.currency == "ARS"


# ═══════════════════════════════════════════════════════════════════
# Regression
# ═══════════════════════════════════════════════════════════════════


def test_no_recommendation_is_touched_by_any_of_this(db):
    """Recommendation 14 is a pending pilot. Nothing in the catalog path may
    read, write or supersede a recommendation."""
    import pathlib
    import re

    rec = Recommendation(
        id=14, action="rebalancear", status="pending", suggested_pct=0.0,
        confidence=1.0, rationale="piloto COME", risks="",
        executive_summary="[PILOTO] Comprar 1 COME",
        metadata_json={"execution_pilot": True, "pilot_type": "security_buy",
                       "symbol": "COME", "side": "buy", "quantity": 1},
    )
    db.add(rec)
    db.commit()

    _come(db)
    _refresh(db, symbol="COME", quantity_step=None)

    db.refresh(rec)
    assert rec.status == "pending"
    assert rec.metadata_json["execution_pilot"] is True

    source = pathlib.Path("app/broker/instrument_catalog.py").read_text()
    assert not re.search(r"(UPDATE|DELETE\s+FROM)\s+recommendations", source, re.I)
    assert "Recommendation" not in source


def test_the_catalog_never_invents_a_quantity_step():
    """No default, no fallback, no hardcoded symbol."""
    import pathlib

    source = pathlib.Path("app/broker/instrument_catalog.py").read_text()
    assert "COME" not in source
    # The only writes to the field go through the merge helper or an explicit
    # administrative verification.
    assert source.count("entry.quantity_step =") == 0


def test_verify_fields_still_refuses_an_invalid_value(db):
    entry = _seed(db)
    for bad in (0, -1, None, "abc"):
        result = verify_fields(
            entry, fields={"quantity_step": bad},
            provenance_source=PROV_ADMIN_OVERRIDE, note="x",
        )
        assert result["changed"] is False
        assert entry.quantity_step == 1.0
