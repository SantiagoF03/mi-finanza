"""Catalog verification states, per-field provenance and identity changes.

PR #138 accepted an instrument as executable if its fields were merely
*present*. That is not verification: a currency copied from the portfolio is
evidence, a tick invented by a class default is a guess, and a symbol that
changed currency overnight is a different instrument. These tests pin the
distinction.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.execution_class import CLASS_ACCIONES, CLASS_CEDEARS
from app.broker.instrument_catalog import (
    PROV_ADMIN_OVERRIDE,
    PROV_CLASS_DEFAULT,
    PROV_IOL_PORTFOLIO,
    PROV_IOL_QUOTE,
    PROV_IOL_TITLE_DETAIL,
    STATUS_CANDIDATE,
    STATUS_IDENTITY_CHANGED,
    STATUS_REJECTED,
    STATUS_VERIFIED,
    accept_identity_change,
    catalog_entry_status,
    get_instrument,
    refresh_catalog_from_positions,
    reject_identity_change,
    unverified_fields,
    upsert_instrument,
    verification_status_of,
    verify_fields,
)
from app.broker.execution_scope import evaluate_order_authorization, load_authorization_context
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import ExecutionInstrument, PortfolioPosition
from tests.testutils import verified_provenance

CLASS_POLICY = {
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
        execution_class_policies={CLASS_ACCIONES: dict(CLASS_POLICY),
                                  CLASS_CEDEARS: dict(CLASS_POLICY)},
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
        market_universe_assets=["BYMA", "GGAL"],
        watchlist_assets=[],
        whitelist_assets=[],
    )
    base.update(extra)
    return base


def _position(symbol="BYMA", asset_type="ACCIONES", currency="ARS"):
    return PortfolioPosition(
        snapshot_id=1, symbol=symbol, asset_type=asset_type,
        instrument_type=asset_type, currency=currency, quantity=10,
        market_value=1000, avg_price=100, pnl_pct=0.0,
    )


def _authorize(db, symbol="BYMA", side="sell", position=None, **extra):
    order = {"symbol": symbol, "side": side, "quantity_planned": 1,
             "estimated_notional": 100.0}
    with exec_settings(**_scope_settings(**extra)):
        s = get_settings()
        return evaluate_order_authorization(
            db, order=order, position=position, settings=s,
            context=load_authorization_context(s),
        )


# ───────────────────────────────────────────────────────────────────
# 1 — verification states
# ───────────────────────────────────────────────────────────────────


def test_entry_without_provenance_is_a_candidate_not_verified(db):
    """Fields being PRESENT is not the same as fields being VERIFIED."""
    entry, _ = upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
    )
    db.commit()
    assert verification_status_of(entry) == STATUS_CANDIDATE
    assert unverified_fields(entry)


def test_candidate_cannot_authorise_an_order(db):
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
    )
    db.commit()
    _, codes = _authorize(db, position=_position())
    assert "instrument_not_verified" in codes


def test_fully_verified_entry_authorises(db):
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    entry = get_instrument(db, "BYMA")
    assert verification_status_of(entry) == STATUS_VERIFIED
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] is None

    _, codes = _authorize(db, position=_position())
    assert codes == []


def test_pre_existing_row_without_status_reads_as_candidate(db):
    """A PR #138 row was never verified by anyone. It must not be
    grandfathered into `verified` just because the column is empty."""
    entry = ExecutionInstrument(
        broker_symbol="OLD", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", execution_class="ACCIONES",
        execution_family="securities", quantity_step=1.0, price_tick=0.01,
        active=True, buy_supported=True, sell_supported=True,
        verification_status="", field_provenance={}, verified_at=datetime.utcnow(),
    )
    db.add(entry)
    db.commit()
    assert verification_status_of(entry) == STATUS_CANDIDATE
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] == "instrument_not_verified"


# ───────────────────────────────────────────────────────────────────
# 2 — a class default never verifies anything
# ───────────────────────────────────────────────────────────────────


def test_class_default_tick_does_not_count_as_verified(db):
    """A policy supplies a LIMIT. It has not looked at the instrument and
    must never claim it verified its minimum price alteration."""
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
        provenance={**verified_provenance(), "price_tick": PROV_CLASS_DEFAULT},
    )
    db.commit()
    entry = get_instrument(db, "BYMA")
    assert "price_tick" in unverified_fields(entry)
    assert verification_status_of(entry) == STATUS_CANDIDATE
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] == "instrument_not_verified"


def test_admin_override_promotes_a_tick_to_verified(db):
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
        provenance={**verified_provenance(), "price_tick": PROV_CLASS_DEFAULT},
    )
    db.commit()
    entry = get_instrument(db, "BYMA")

    result = verify_fields(entry, fields={"price_tick": 1.0},
                           provenance_source=PROV_ADMIN_OVERRIDE, note="verificado en IOL")
    db.commit()
    assert result["verification_status"] == STATUS_VERIFIED
    assert entry.price_tick == 1.0
    assert entry.field_provenance["price_tick"] == PROV_ADMIN_OVERRIDE
    assert entry.verification_audit[-1]["event"] == "fields_verified"


def test_capabilities_are_not_inferred_from_the_execution_family(db):
    """Holding something proves nothing about being able to buy or sell it."""
    with exec_settings(**_scope_settings()):
        refresh_catalog_from_positions(
            db,
            [{"symbol": "BYMA", "asset_type": "ACCIONES",
              "instrument_type": "ACCIONES", "currency": "ARS", "quantity": 10}],
            settings=get_settings(),
        )
    entry = get_instrument(db, "BYMA")
    assert entry.buy_supported is False
    assert entry.sell_supported is False
    assert entry.quote_supported is False
    provenance = entry.field_provenance
    assert provenance["buy_supported"] == PROV_CLASS_DEFAULT
    assert provenance["sell_supported"] == PROV_CLASS_DEFAULT
    # Identity DID come from the portfolio — that part is real evidence.
    assert provenance["currency"] == PROV_IOL_PORTFOLIO
    assert provenance["asset_type"] == PROV_IOL_PORTFOLIO
    assert verification_status_of(entry) == STATUS_CANDIDATE


def test_market_and_settlement_from_a_policy_are_marked_as_defaults(db):
    with exec_settings(**_scope_settings()):
        refresh_catalog_from_positions(
            db,
            [{"symbol": "BYMA", "asset_type": "ACCIONES",
              "instrument_type": "ACCIONES", "currency": "ARS", "quantity": 10}],
            settings=get_settings(),
        )
    provenance = get_instrument(db, "BYMA").field_provenance
    assert provenance["market"] == PROV_CLASS_DEFAULT
    assert provenance["settlement"] == PROV_CLASS_DEFAULT


# ───────────────────────────────────────────────────────────────────
# 3 — identity changes freeze the instrument
# ───────────────────────────────────────────────────────────────────


def _verified_byma(db, currency="ARS"):
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency=currency, market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    return get_instrument(db, "BYMA")


def test_currency_change_ars_to_usd_blocks_immediately(db):
    entry = _verified_byma(db, currency="ARS")
    verified_at_before = entry.verified_at

    _, changed = upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="USD", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    entry = get_instrument(db, "BYMA")

    assert changed is True
    assert verification_status_of(entry) == STATUS_IDENTITY_CHANGED
    # The OLD identity is preserved; the new one is only *proposed*.
    assert entry.currency == "ARS"
    assert entry.pending_identity["currency"] == "USD"
    assert entry.previous_identity["currency"] == "ARS"
    # verified_at must NOT move: nothing was verified by detecting a change.
    assert entry.verified_at == verified_at_before

    code, details = catalog_entry_status(entry, max_age_seconds=86400)
    assert code == "instrument_identity_changed"
    assert details["pending_identity"]["currency"] == "USD"


@pytest.mark.parametrize("mutation", [
    {"currency": "USD"},
    {"asset_type": "CEDEAR", "instrument_type": "CEDEAR"},
    {"instrument_type": "ETF"},
])
def test_any_identity_field_change_freezes_the_instrument(db, mutation):
    _verified_byma(db)
    base = dict(
        broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, buy_supported=True, sell_supported=True,
        quote_supported=True, max_age_seconds=86400, provenance=verified_provenance(),
    )
    upsert_instrument(db, **{**base, **mutation})
    db.commit()
    assert verification_status_of(get_instrument(db, "BYMA")) == STATUS_IDENTITY_CHANGED


def test_identity_changed_blocks_both_buy_and_sell(db):
    _verified_byma(db)
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="USD", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    for side in ("buy", "sell"):
        _, codes = _authorize(db, side=side, position=_position())
        assert "instrument_identity_changed" in codes


def test_identity_changed_is_not_cleared_by_another_automatic_refresh(db):
    """The whole point: a second refresh must not quietly re-verify it."""
    _verified_byma(db)
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="USD", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()

    # Several more refreshes, including one that re-states the ORIGINAL
    # identity — none of them may resolve a human decision.
    for currency in ("USD", "USD", "ARS"):
        upsert_instrument(
            db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
            currency=currency, market="bCBA", settlement="t1", source="refresh",
            quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
            provenance=verified_provenance(),
        )
        db.commit()
        assert verification_status_of(get_instrument(db, "BYMA")) == STATUS_IDENTITY_CHANGED


def test_accepting_an_identity_change_returns_to_candidate_not_verified(db):
    """Accepting that IOL now calls this a dollar instrument is NOT the same
    as having re-verified its tick and capabilities under that identity."""
    _verified_byma(db)
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="USD", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    entry = get_instrument(db, "BYMA")

    result = accept_identity_change(entry, note="revisado con IOL")
    db.commit()

    assert result["verification_status"] == STATUS_CANDIDATE
    assert entry.currency == "USD"
    assert entry.pending_identity is None
    assert entry.buy_supported is False and entry.sell_supported is False
    assert entry.field_provenance["price_tick"] == PROV_CLASS_DEFAULT
    assert entry.verification_audit[-1]["event"] == "identity_change_accepted"

    _, codes = _authorize(db, position=_position(currency="USD"))
    assert "instrument_not_verified" in codes


def test_rejecting_an_identity_change_parks_the_instrument(db):
    _verified_byma(db)
    upsert_instrument(
        db, broker_symbol="BYMA", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="USD", market="bCBA", settlement="t1", source="refresh",
        quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    entry = get_instrument(db, "BYMA")

    reject_identity_change(entry, note="no reconocemos este cambio")
    db.commit()

    assert verification_status_of(entry) == STATUS_REJECTED
    code, _ = catalog_entry_status(entry, max_age_seconds=86400)
    assert code == "instrument_rejected"


def test_a_brand_new_entry_is_not_an_identity_change(db):
    """Populating an empty row is not the broker changing its mind."""
    entry, changed = upsert_instrument(
        db, broker_symbol="NEW", asset_type="ACCIONES", instrument_type="ACCIONES",
        currency="ARS", market="bCBA", settlement="t1", source="test",
        quantity_step=1.0, price_tick=0.01, max_age_seconds=86400,
        provenance=verified_provenance(),
    )
    db.commit()
    assert changed is False
    assert verification_status_of(entry) == STATUS_VERIFIED


# ───────────────────────────────────────────────────────────────────
# 4 — on-demand resolution of instruments we do not hold
# ───────────────────────────────────────────────────────────────────


def _resolver_broker(*, tipo="acciones", moneda="peso_Argentino", mercado="bCBA",
                     quote_available=True, extra_detail=None,
                     bid=True, ask=True):
    """A resolver broker whose two sides of the book are set independently.

    `bid` backs a SELL quote, `ask` backs a BUY quote. They are separate on
    purpose: an instrument can legitimately have one and not the other.
    """
    broker = mock.MagicMock()
    detail = {"simbolo": "GGAL", "tipo": tipo, "moneda": moneda,
              "mercado": mercado, "descripcion": "Grupo Galicia"}
    detail.update(extra_detail or {})
    response = mock.MagicMock()
    response.json.return_value = detail
    broker._authorized_get.return_value = response

    if not quote_available:
        bid = ask = False

    def _quote(symbol, side, market, settlement):
        has_side = bid if side == "sell" else ask
        if not has_side:
            return {"available": False, "price": None, "source": "none"}
        return {
            "available": True,
            "price": 100.0,
            "source": "bid" if side == "sell" else "ask",
        }

    broker.get_execution_quote.side_effect = _quote
    return broker


def test_not_held_instrument_resolves_read_only_to_candidate(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker()
    with exec_settings(**_scope_settings()):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE,
            class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    db.commit()

    assert report["resolved"] is True
    entry = get_instrument(db, "GGAL")
    assert entry is not None
    # Identity IS verified (title detail); the tick is not (class default).
    assert entry.field_provenance["currency"] == PROV_IOL_TITLE_DETAIL
    assert entry.field_provenance["quote_supported"] == PROV_IOL_QUOTE
    assert entry.field_provenance["price_tick"] == PROV_CLASS_DEFAULT
    assert report["verification_status"] == STATUS_CANDIDATE
    assert "price_tick" in report["unverified_fields"]
    # And it is NOT buyable yet.
    broker.submit_order_request.assert_not_called()


def test_resolution_never_writes_orders(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument
    from app.models.models import OrderExecution

    broker = _resolver_broker()
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    db.commit()
    assert db.query(OrderExecution).count() == 0
    broker.submit_order_request.assert_not_called()
    broker.cancel_order.assert_not_called()


def test_resolved_candidate_cannot_be_bought_until_verified(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker()
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    db.commit()

    _, codes = _authorize(db, symbol="GGAL", side="buy")
    assert "instrument_not_verified" in codes

    # After an administrative verification of the numeric fields it becomes
    # executable — and only then.
    entry = get_instrument(db, "GGAL")
    verify_fields(entry, fields={"price_tick": 0.5, "quantity_step": 1.0},
                  provenance_source=PROV_ADMIN_OVERRIDE, note="verificado")
    db.commit()
    _, codes_after = _authorize(db, symbol="GGAL", side="buy")
    assert "instrument_not_verified" not in codes_after


def test_a_symbol_nobody_asked_for_is_never_resolved(db):
    """"The user typed it" is not evidence a symbol is tradeable."""
    from app.broker.instrument_resolver import admissible_symbols, resolve_missing_instruments

    broker = _resolver_broker()
    with exec_settings(**_scope_settings(market_universe_assets=["BYMA"])):
        allowed = admissible_symbols(db, get_settings())
        result = resolve_missing_instruments(
            db, symbols=["HACKME"], broker=broker, settings=get_settings()
        )

    assert "HACKME" not in allowed
    assert result["resolved"][0]["error"] == "symbol_source_not_admissible"
    assert get_instrument(db, "HACKME") is None
    broker._authorized_get.assert_not_called()


def test_symbols_from_recommendations_and_watchlist_are_admissible(db):
    from app.broker.instrument_resolver import (
        SOURCE_RECOMMENDATION,
        SOURCE_WATCHLIST,
        admissible_symbols,
    )
    from app.models.models import Recommendation, RecommendationAction

    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.1, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(recommendation_id=rec.id, symbol="PAMP",
                                target_change_pct=0.01, reason="test"))
    db.commit()

    with exec_settings(**_scope_settings(watchlist_assets=["TXAR"],
                                         market_universe_assets=[])):
        allowed = admissible_symbols(db, get_settings())

    assert allowed.get("PAMP") == SOURCE_RECOMMENDATION
    assert allowed.get("TXAR") == SOURCE_WATCHLIST


def test_resolution_refuses_a_symbol_on_the_wrong_market(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado="nYSE")
    with exec_settings(**_scope_settings()):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    assert report["error"] == "instrument_market_mismatch"
    assert get_instrument(db, "GGAL") is None


# ───────────────────────────────────────────────────────────────────
# Market canonicalisation
#
# IOL spells the venue inconsistently: the title detail has returned "bcba"
# where the policy says "bCBA". They are the same market, so a case-sensitive
# comparison refused real instruments — BYMA and SPY both failed in production
# with instrument_market_mismatch.
#
# Tolerating a spelling is safe. Inventing a venue is not, so an unrecognised
# market must still be refused, never coerced.
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("observed", ["bcba", "BCBA", "bCBA", "BcBa", " bcba "])
def test_a_differently_spelled_market_resolves_to_the_canonical_one(db, observed):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado=observed)
    with exec_settings(**_scope_settings()):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )

    assert report.get("error") is None, report
    assert report["resolved"] is True
    # The report carries the AUTHORISED spelling, not what IOL happened to say.
    assert report["market"] == "bCBA"


@pytest.mark.parametrize("observed", ["bcba", "BCBA", "bCBA"])
def test_the_catalog_persists_the_canonical_market(db, observed):
    """Persisting the canonical value is what keeps every downstream
    comparison exact — execution_scope compares `entry.market` to the policy
    with `!=`, and that check must stay strict."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado=observed)
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )

    entry = get_instrument(db, "GGAL")
    assert entry is not None
    assert entry.market == "bCBA"
    # And the identity hash is built from the canonical value, so the same
    # instrument spelled two ways does not read as an identity change.
    assert entry.raw_identity.get("market") in (None, "bCBA")


def test_quotes_are_requested_with_the_canonical_market(db):
    """Both sides, and the fetch that precedes them, use the authorised
    spelling — a quote asked for on `bcba` is a quote for a venue the rest of
    the system does not recognise."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado="bcba")
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )

    markets_used = {call.args[2] for call in broker.get_execution_quote.call_args_list}
    sides_probed = {call.args[1] for call in broker.get_execution_quote.call_args_list}
    assert markets_used == {"bCBA"}
    assert sides_probed == {"buy", "sell"}


def test_an_unknown_market_is_still_refused_with_both_values(db):
    """`NYSE` is not a spelling of `bCBA`. It must not be coerced into one."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado="NYSE")
    with exec_settings(**_scope_settings()):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )

    assert report["error"] == "instrument_market_mismatch"
    assert report["observed_market"] == "NYSE"
    assert report["allowed_markets"] == ["bCBA"]
    assert get_instrument(db, "GGAL") is None


@pytest.mark.parametrize("observed", ["", "   ", "NASDAQ", "bcba_extra", "cba"])
def test_the_canonicaliser_only_ever_returns_an_authorised_value(observed):
    """The helper cannot manufacture a venue: every non-None answer is a
    member of the allowed list it was given."""
    from app.broker.instrument_resolver import _canonical_market

    allowed = ["bCBA"]
    result = _canonical_market(observed, allowed)
    assert result is None or result in allowed


def test_the_canonicaliser_returns_the_policys_own_variant():
    from app.broker.instrument_resolver import _canonical_market

    # Whatever the policy declares is what comes back — the helper never
    # imposes a spelling of its own.
    assert _canonical_market("bcba", ["bCBA"]) == "bCBA"
    assert _canonical_market("bCBA", ["BCBA"]) == "BCBA"
    assert _canonical_market("BCBA", ["bcba"]) == "bcba"
    assert _canonical_market("nyse", ["bCBA"]) is None
    assert _canonical_market("bcba", []) is None


def test_resolving_never_sends_an_order(db):
    """The resolver is read-only. A canonicalised market must not open a path
    to sending anything."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado="bcba")
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )

    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()
    broker.cancel_order.assert_not_called()
    broker.submit_fund_request.assert_not_called()


def test_the_legacy_byma_path_is_untouched_by_this_change():
    """The BYMA pilot that already executed for real goes through
    EXECUTION_INSTRUMENT_POLICIES, not the resolver — this fix must not reach
    it."""
    import pathlib

    source = pathlib.Path("app/broker/instrument_scope.py").read_text()
    # The legacy per-symbol path still compares exactly, deliberately: it is
    # configured by a human, so there is no IOL spelling to tolerate.
    assert '_canonical_market' not in source
    assert 'policy["market"] != (settings.iol_order_market or "")' in source


def test_a_canonicalised_instrument_passes_the_downstream_policy_check(db):
    """End to end: the production symptom was that resolution failed. This
    proves the resolved entry is then accepted by the check that compares
    `entry.market` to the policy exactly."""
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(mercado="bcba")
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
        entry = get_instrument(db, "GGAL")

    assert entry.market in CLASS_POLICY["markets"]


def test_resolution_refuses_a_currency_outside_the_class(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(moneda="dolar_Estadounidense")
    with exec_settings(**_scope_settings()):
        report = resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    assert report["error"] == "instrument_currency_mismatch"


def test_official_tick_metadata_is_verified_when_present(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(extra_detail={"alteracionMinima": 0.5})
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    db.commit()
    entry = get_instrument(db, "GGAL")
    assert entry.price_tick == 0.5
    assert entry.field_provenance["price_tick"] == PROV_IOL_TITLE_DETAIL


def test_no_quote_means_no_buy_or_sell_capability(db):
    from app.broker.instrument_resolver import SOURCE_UNIVERSE, resolve_instrument

    broker = _resolver_broker(quote_available=False)
    with exec_settings(**_scope_settings()):
        resolve_instrument(
            db, symbol="GGAL", broker=broker, settings=get_settings(),
            source=SOURCE_UNIVERSE, class_policies={CLASS_ACCIONES: dict(CLASS_POLICY)},
        )
    db.commit()
    entry = get_instrument(db, "GGAL")
    assert entry.quote_supported is False
    assert entry.buy_supported is False
    assert entry.sell_supported is False
