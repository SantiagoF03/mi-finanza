"""Tests for P1 (IOL real operations with /Vender and /Comprar) and P2 (dynamic universe from catalog).

Covers all 11 acceptance criteria:
1. side=sell uses /api/v2/operar/Vender
2. side=buy uses /api/v2/operar/Comprar or blocked safely
3. sell not sent if symbol not in portfolio
4. sell not sent if quantity_planned <= 0
5. validation errors leave clear state without breaking recommendation
6. scheduler never executes orders
7. instrument_catalog refresh creates/updates instruments
8. universe dinámico fed from instrument_catalog
9. external_opportunity can come from catalog without manual whitelist
10. whitelist is optional override, not primary dependency
11. unchanged, cooldown, triage and external_opportunities remain intact
"""

import inspect
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import Base
from app.models.models import (
    InstrumentCatalog,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _make_snapshot(db: Session, total_value=100000, cash=12000) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=total_value, cash=cash, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="MSFT", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=12,
        market_value=28000, avg_price=340, pnl_pct=0.08,
    ))
    db.flush()
    return snap


def _make_recommendation(db: Session, action="rebalancear", status="pending",
                         symbol="AAPL", target_pct=-0.05) -> Recommendation:
    rec = Recommendation(
        action=action, status=status, suggested_pct=0.05, confidence=0.7,
        rationale="Test", risks="Test", executive_summary="Test", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol=symbol,
        target_change_pct=target_pct, reason="Test reason",
    ))
    db.flush()
    return rec


# ===========================================================================
# CRITERION 1: side=sell uses /api/v2/operar/Vender
# ===========================================================================


def test_sell_uses_vender_endpoint():
    """IolBrokerClient maps sell to /api/v2/operar/Vender."""
    from app.broker.clients import IolBrokerClient
    assert IolBrokerClient._IOL_SIDE_ENDPOINTS["sell"] == "/api/v2/operar/Vender"


def test_mock_broker_sell_returns_vender_endpoint():
    """MockBrokerClient.place_order(side=sell) reports Vender endpoint."""
    from app.broker.clients import MockBrokerClient
    result = MockBrokerClient().place_order("AAPL", "sell", 10, price=150.0)
    assert result["endpoint_used"] == "/api/v2/operar/Vender"
    assert result["status"] == "sent"


def test_sell_execution_records_endpoint(db):
    """Full sell execution persists endpoint_used via broker response."""
    _make_snapshot(db)
    rec = _make_recommendation(db, symbol="AAPL", target_pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute
    with patch("app.services.execution._get_execution_broker") as mock_fn:
        mock_broker = mock_fn.return_value
        mock_broker.place_order.return_value = {
            "order_id": "MOCK-SELL-1", "status": "sent",
            "endpoint_used": "/api/v2/operar/Vender",
            "raw_response": {"mock": True},
        }
        result = approve_and_execute(db, rec.id)

    assert result["executions"][0]["endpoint_used"] == "/api/v2/operar/Vender"
    assert result["executions"][0]["status"] == "execution_sent"

    # Verify in DB
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).first()
    assert oe.endpoint_used == "/api/v2/operar/Vender"


# ===========================================================================
# CRITERION 2: side=buy uses /api/v2/operar/Comprar or blocked safely
# ===========================================================================


def test_buy_uses_comprar_endpoint():
    """IolBrokerClient maps buy to /api/v2/operar/Comprar."""
    from app.broker.clients import IolBrokerClient
    assert IolBrokerClient._IOL_SIDE_ENDPOINTS["buy"] == "/api/v2/operar/Comprar"


def test_mock_broker_buy_returns_comprar_endpoint():
    """MockBrokerClient.place_order(side=buy) reports Comprar endpoint."""
    from app.broker.clients import MockBrokerClient
    result = MockBrokerClient().place_order("AAPL", "buy", 5)
    assert result["endpoint_used"] == "/api/v2/operar/Comprar"


def test_buy_execution_uses_comprar_or_blocked(db):
    """Buy orders either use /Comprar endpoint or are safely blocked."""
    _make_snapshot(db)
    rec = _make_recommendation(db, symbol="AAPL", target_pct=0.05)  # positive = buy
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)

    exec_info = result["executions"][0]
    # The buy may be executed (if price available) or blocked
    assert exec_info["side"] == "buy"
    # Either validated + sent, or blocked with clear reason
    if exec_info["validation_status"] == "failed":
        assert exec_info["blocked_reason"] != ""
    else:
        assert exec_info["validation_status"] == "passed"


# ===========================================================================
# CRITERION 3: sell not sent if symbol not in portfolio
# ===========================================================================


def test_sell_blocked_if_symbol_not_in_portfolio(db):
    """Sell for symbol not in portfolio is validation_failed, not sent to broker."""
    _make_snapshot(db)  # has AAPL, MSFT only
    rec = _make_recommendation(db, symbol="TSLA", target_pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)

    exec_info = result["executions"][0]
    assert exec_info["validation_status"] == "failed"
    assert exec_info["status"] == "validation_failed"
    assert "TSLA" in exec_info["blocked_reason"]


# ===========================================================================
# CRITERION 4: sell not sent if quantity_planned <= 0
# ===========================================================================


def test_sell_blocked_if_quantity_zero(db):
    """Sell with tiny target producing 0 quantity is blocked."""
    snap = PortfolioSnapshot(total_value=100000, cash=90000, currency="ARS")
    db.add(snap)
    db.flush()
    # High price per unit: 50000/1 = 50000. target = 100000*0.0001 = 10
    # floor(10/50000) = 0 → blocked
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="EXPENSIVE", asset_type="BONO",
        instrument_type="BONO", currency="ARS", quantity=1,
        market_value=50000, avg_price=50000, pnl_pct=0,
    ))
    db.flush()

    rec = _make_recommendation(db, symbol="EXPENSIVE", target_pct=-0.0001)
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)

    exec_info = result["executions"][0]
    assert exec_info["validation_status"] == "failed"
    assert "rounds to 0" in exec_info["blocked_reason"]


# ===========================================================================
# CRITERION 5: validation errors leave clear state, don't break recommendation
# ===========================================================================


def test_validation_failure_preserves_recommendation(db):
    """Validation failures still leave recommendation as approved."""
    _make_snapshot(db)
    rec = _make_recommendation(db, symbol="NONEXIST", target_pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)

    assert result["status"] == "approved"
    assert result["executions"][0]["status"] == "validation_failed"
    updated_rec = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
    assert updated_rec.status == "approved"


def test_no_snapshot_fails_gracefully(db):
    """If no snapshot exists, sell validation fails gracefully."""
    rec = _make_recommendation(db, symbol="AAPL", target_pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)

    exec_info = result["executions"][0]
    assert exec_info["status"] == "validation_failed"
    assert exec_info["blocked_reason"] != ""


# ===========================================================================
# CRITERION 6: scheduler never executes orders
# ===========================================================================


def test_scheduler_never_imports_execution():
    """Scheduler module does not import from services.execution."""
    from app.scheduler import jobs
    source = inspect.getsource(jobs)
    assert "approve_and_execute" not in source
    assert "place_order" not in source
    assert "OrderExecution" not in source


def test_orchestrator_never_executes():
    """Orchestrator (called by scheduler) never triggers order execution."""
    from app.services import orchestrator
    source = inspect.getsource(orchestrator)
    assert "approve_and_execute" not in source
    assert "place_order" not in source


# ===========================================================================
# CRITERION 7: instrument_catalog refresh creates/updates instruments
# ===========================================================================


def test_catalog_refresh_seed_mode(db):
    """In mock mode, refresh seeds from KNOWN_ASSET_TYPES."""
    from app.market.discovery import refresh_instrument_catalog
    with patch("app.market.discovery.get_settings") as mock_s:
        mock_s.return_value = MagicMock(broker_mode="mock", iol_username="")
        result = refresh_instrument_catalog(db)

    assert result["created"] > 0
    count = db.query(InstrumentCatalog).count()
    assert count > 0


def test_catalog_refresh_updates_existing(db):
    """Refreshing again updates existing, doesn't duplicate."""
    from app.market.discovery import refresh_instrument_catalog
    with patch("app.market.discovery.get_settings") as mock_s:
        mock_s.return_value = MagicMock(broker_mode="mock", iol_username="")
        r1 = refresh_instrument_catalog(db)
        r2 = refresh_instrument_catalog(db)

    assert r2["created"] == 0  # all existing now
    assert r1["created"] > 0


# ===========================================================================
# CRITERION 8: dynamic universe fed from instrument_catalog
# ===========================================================================


def test_catalog_symbols_feed_allowed_assets(db):
    """build_allowed_assets includes catalog symbols in external_allowed."""
    db.add(InstrumentCatalog(
        symbol="NEWSTOCK", name="New Stock", asset_type="ACCIONES", market="BCBA",
        currency="ARS", source="iol_discovery", tradable=True,
        is_active=True, eligible_for_external_discovery=True,
    ))
    db.commit()

    from app.market.discovery import get_eligible_universe_symbols
    catalog_syms = get_eligible_universe_symbols(db)
    assert "NEWSTOCK" in catalog_syms

    from app.recommendations.universe import build_allowed_assets
    allowed = build_allowed_assets([], catalog_symbols=catalog_syms)
    assert "NEWSTOCK" in allowed["catalog_dynamic"]
    assert "NEWSTOCK" in allowed["external_allowed"]


# ===========================================================================
# CRITERION 9: external_opportunity from catalog without manual whitelist
# ===========================================================================


def test_external_opportunity_from_catalog_no_whitelist(db):
    """A catalog-discovered symbol appears as external opportunity without whitelist."""
    db.add(InstrumentCatalog(
        symbol="DISCOVERED", name="Discovered", asset_type="CEDEAR", market="BCBA",
        currency="ARS", source="iol_discovery", tradable=True,
        is_active=True, eligible_for_external_discovery=True,
    ))
    db.commit()

    from app.market.discovery import get_eligible_universe_symbols
    catalog_syms = get_eligible_universe_symbols(db)

    from app.recommendations.universe import build_allowed_assets
    with patch("app.recommendations.universe.get_settings") as mock_s:
        s = MagicMock()
        s.whitelist_assets = []
        s.watchlist_assets = []
        s.market_universe_assets = []
        mock_s.return_value = s
        allowed = build_allowed_assets([], catalog_symbols=catalog_syms)

    assert "DISCOVERED" in allowed["external_allowed"]

    from app.market.candidates import generate_external_candidates
    candidates = generate_external_candidates(
        news_opportunities=[], allowed_assets=allowed, positions=[],
    )
    syms = {c["symbol"] for c in candidates}
    assert "DISCOVERED" in syms

    disc = next(c for c in candidates if c["symbol"] == "DISCOVERED")
    assert "catalog" in disc["source_types"]
    assert disc["actionable_external"] is True


# ===========================================================================
# CRITERION 10: whitelist is optional override, not primary dependency
# ===========================================================================


def test_whitelist_optional_not_primary(db):
    """External universe works without whitelist if catalog is populated."""
    db.add(InstrumentCatalog(
        symbol="CATONLY", name="", asset_type="ACCIONES", market="BCBA",
        currency="ARS", source="iol_discovery", tradable=True,
        is_active=True, eligible_for_external_discovery=True,
    ))
    db.commit()

    from app.market.discovery import get_eligible_universe_symbols
    catalog_syms = get_eligible_universe_symbols(db)

    from app.recommendations.universe import build_allowed_assets
    with patch("app.recommendations.universe.get_settings") as mock_s:
        s = MagicMock()
        s.whitelist_assets = []
        s.watchlist_assets = []
        s.market_universe_assets = []
        mock_s.return_value = s
        allowed = build_allowed_assets([], catalog_symbols=catalog_syms)

    assert len(allowed["whitelist"]) == 0
    assert "CATONLY" in allowed["external_allowed"]


def test_whitelist_as_override():
    """Whitelist symbols add to main_allowed even without catalog."""
    from app.recommendations.universe import build_allowed_assets
    with patch("app.recommendations.universe.get_settings") as mock_s:
        s = MagicMock()
        s.whitelist_assets = ["OVERRIDE"]
        s.watchlist_assets = []
        s.market_universe_assets = []
        mock_s.return_value = s
        allowed = build_allowed_assets([], catalog_symbols=set())

    assert "OVERRIDE" in allowed["main_allowed"]
    assert "OVERRIDE" in allowed["whitelist"]


# ===========================================================================
# CRITERION 11: unchanged, cooldown, triage, external_opportunities intact
# ===========================================================================


def test_unchanged_detection_still_works():
    """detect_unchanged function still exists and works."""
    from app.recommendations.unchanged import detect_unchanged

    rec_new = {
        "action": "mantener", "suggested_pct": 0.0, "confidence": 0.5,
        "status": "pending", "blocked_reason": "", "risks": "",
        "actions": [], "_news_items": [], "external_opportunities": [],
    }
    unchanged, reason = detect_unchanged(rec_new, None, {})
    assert unchanged is False


def test_classify_opportunity_includes_catalog():
    """classify_opportunity_status recognizes 'catalog' tracking."""
    from app.recommendations.universe import classify_opportunity_status

    allowed = {
        "holdings": {"AAPL"},
        "catalog_dynamic": {"NEWONE"},
        "watchlist": set(),
        "universe": set(),
    }
    assert classify_opportunity_status("AAPL", allowed) == "in_holdings"
    assert classify_opportunity_status("NEWONE", allowed) == "catalog"
    assert classify_opportunity_status("UNKNOWN", allowed) == "untracked"


def test_plan_order_quantity_calculation(db):
    """_plan_order correctly calculates sell quantity from snapshot."""
    from app.services.execution import _plan_order

    snap = _make_snapshot(db, total_value=100000, cash=12000)
    db.commit()
    # Reload with positions
    from app.services.execution import _get_latest_snapshot
    loaded_snap = _get_latest_snapshot(db)

    action = MagicMock()
    action.symbol = "AAPL"
    action.target_change_pct = -0.05

    plan = _plan_order(action, loaded_snap)
    assert plan["valid"] is True
    assert plan["side"] == "sell"
    assert plan["portfolio_value_used"] == 100000
    assert plan["position_value_used"] == 38000
    # price_per_unit = 38000/20 = 1900, target = 100000*0.05 = 5000
    # quantity = floor(5000/1900) = 2
    assert plan["quantity_planned"] == 2


def test_exec_to_dict_includes_endpoint_used(db):
    """_exec_to_dict includes endpoint_used field."""
    rec = _make_recommendation(db)
    oe = OrderExecution(
        recommendation_id=rec.id, symbol="AAPL", side="sell",
        target_change_pct=-0.05, status="execution_sent",
        endpoint_used="/api/v2/operar/Vender",
        portfolio_value_used=100000, position_value_used=38000,
        quantity_planned=2.0, quantity_sent=2.0,
        validation_status="passed",
    )
    db.add(oe)
    db.flush()

    from app.services.execution import _exec_to_dict
    d = _exec_to_dict(oe)
    assert d["endpoint_used"] == "/api/v2/operar/Vender"
    assert d["portfolio_value_used"] == 100000
    assert d["validation_status"] == "passed"
