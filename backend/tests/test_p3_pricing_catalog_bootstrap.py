"""Tests for P3 gaps: execution pricing safety, catalog asset_type resolution,
catalog auto-bootstrap, and scheduler safety.

Required tests:
1. approve_and_execute doesn't send snapshot-derived price as limit order by default
2. Block execution if no fresh quote / usable price
3. Symbol in InstrumentCatalog resolves valid asset_type even if not in holdings/KNOWN_ASSET_TYPES
4. Empty catalog gets bootstrapped without breaking cycle
5. Scheduler still doesn't import/trigger execution
"""

import inspect
from datetime import datetime, timezone
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


def _make_snapshot(db: Session) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.flush()
    return snap


def _make_recommendation(db: Session) -> Recommendation:
    rec = Recommendation(
        action="rebalancear", status="pending",
        suggested_pct=0.05, confidence=0.7,
        rationale="Test", risks="Test", executive_summary="Test",
        metadata_json={},
    )
    db.add(rec)
    db.flush()
    act = RecommendationAction(
        recommendation_id=rec.id, symbol="AAPL",
        target_change_pct=-0.05, reason="Sobreconcentración",
    )
    db.add(act)
    db.flush()
    return rec


# ---------------------------------------------------------------------------
# Test 1: approve_and_execute does NOT send snapshot-derived price as limit
# ---------------------------------------------------------------------------


def test_no_snapshot_price_sent_to_broker(db):
    """approve_and_execute must NOT pass snapshot-derived price to broker.place_order.
    The price argument should come from _get_fresh_quote, not from _plan_order's
    snapshot_price_ref.
    """
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_broker_fn:
        mock_broker = mock_broker_fn.return_value
        # Mark as mock so _get_fresh_quote returns market_order (price=None)
        mock_broker._mock_orders = []
        mock_broker.place_order.return_value = {
            "order_id": "MOCK-001", "status": "sent",
            "endpoint_used": "/api/v2/operar/Vender",
            "raw_response": {"mock": True},
        }

        result = approve_and_execute(db, rec.id)

    assert result["status"] == "approved"
    assert len(result["executions"]) == 1
    assert result["executions"][0]["status"] == "execution_sent"

    # The key assertion: price passed to broker must NOT be the snapshot-derived
    # price (38000/20 = 1900). In mock mode it should be None (market order).
    call_args = mock_broker.place_order.call_args
    price_sent = call_args.kwargs.get("price") if call_args.kwargs else call_args[1].get("price")
    # price should be None (market order), NOT 1900.0 (snapshot-derived)
    assert price_sent is None, (
        f"Broker received price={price_sent}, expected None (market order). "
        f"Snapshot-derived price must never be sent to broker."
    )


# ---------------------------------------------------------------------------
# Test 2: Block execution if no fresh quote available
# ---------------------------------------------------------------------------


def test_execution_blocked_without_fresh_quote(db):
    """If _get_fresh_quote returns available=False, order must be blocked
    with validation_failed status.
    """
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_broker_fn, \
         patch("app.services.execution._get_fresh_quote") as mock_quote:
        mock_broker = mock_broker_fn.return_value
        mock_quote.return_value = {"available": False, "price": None, "source": "none"}

        result = approve_and_execute(db, rec.id)

    assert result["status"] == "approved"
    assert len(result["executions"]) == 1
    exec_result = result["executions"][0]
    assert exec_result["status"] == "validation_failed"
    assert "fresh quote" in exec_result["blocked_reason"].lower() or "cotización" in exec_result["blocked_reason"].lower()
    # Broker should NOT have been called
    mock_broker.place_order.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: Symbol in InstrumentCatalog resolves valid asset_type
# ---------------------------------------------------------------------------


def test_catalog_symbol_resolves_asset_type(db):
    """A symbol present in InstrumentCatalog but NOT in holdings or KNOWN_ASSET_TYPES
    should resolve to a valid asset_type via catalog_map.
    """
    # Add a symbol to catalog that's not in KNOWN_ASSET_TYPES
    cat = InstrumentCatalog(
        symbol="NEWSTOCK", name="New Stock SA", asset_type="ACCIONES",
        market="BCBA", currency="ARS", tradable=True,
        source="iol_discovery", is_active=True,
        eligible_for_external_discovery=True,
        last_seen_at=datetime.now(timezone.utc),
    )
    db.add(cat)
    db.commit()

    from app.market.assets import KNOWN_ASSET_TYPES, build_catalog_asset_type_map, resolve_asset_type

    # Verify it's NOT in the static map
    assert "NEWSTOCK" not in KNOWN_ASSET_TYPES

    # Build catalog map from DB
    catalog_map = build_catalog_asset_type_map(db)
    assert "NEWSTOCK" in catalog_map
    assert catalog_map["NEWSTOCK"] == "ACCIONES"

    # Resolve — should find it via catalog
    asset_type, status, *_ = resolve_asset_type("NEWSTOCK", catalog_map=catalog_map)
    assert asset_type == "ACCIONES"
    assert status == "known_valid"

    # Without catalog_map, it would fall to DESCONOCIDO
    asset_type_no_catalog, status_no_catalog, *_ = resolve_asset_type("NEWSTOCK")
    assert asset_type_no_catalog == "DESCONOCIDO"
    assert status_no_catalog == "unknown"


# ---------------------------------------------------------------------------
# Test 4: Empty catalog gets bootstrapped without breaking cycle
# ---------------------------------------------------------------------------


def test_empty_catalog_auto_bootstrap(db):
    """When catalog is empty, run_cycle should auto-bootstrap it
    via refresh_instrument_catalog without breaking the cycle.
    """
    # Verify catalog is empty
    count = db.query(InstrumentCatalog).count()
    assert count == 0

    from app.services.orchestrator import run_cycle

    with patch("app.services.orchestrator._get_broker") as mock_broker_fn, \
         patch("app.services.orchestrator.refresh_instrument_catalog") as mock_refresh, \
         patch("app.services.orchestrator.get_eligible_universe_symbols") as mock_universe, \
         patch("app.services.orchestrator.run_ingestion", return_value={"status": "ok"}), \
         patch("app.services.orchestrator.get_engine_eligible_news", return_value=[]), \
         patch("app.services.orchestrator.get_llm_eligible_news", return_value=[]), \
         patch("app.services.orchestrator.generate_recommendation") as mock_gen_rec, \
         patch("app.services.orchestrator.enforce_rules") as mock_rules, \
         patch("app.services.orchestrator.generate_external_candidates", return_value=[]):

        # First call returns empty (triggers bootstrap), second returns seeded symbols
        mock_universe.side_effect = [set(), {"AAPL", "MSFT"}]

        mock_broker = mock_broker_fn.return_value
        mock_broker.get_portfolio_snapshot.return_value = {
            "cash": 10000, "currency": "USD",
            "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR",
                          "instrument_type": "CEDEAR", "currency": "USD",
                          "quantity": 10, "market_value": 19000,
                          "avg_price": 180, "pnl_pct": 0.05}],
        }

        mock_gen_rec.return_value = {
            "action": "mantener", "status": "pending", "suggested_pct": 0,
            "confidence": 0.5, "rationale": "Test", "risks": "None",
            "executive_summary": "OK", "actions": [],
            "external_opportunities": [],
        }
        mock_rules.side_effect = lambda rec, *a, **kw: rec

        result = run_cycle(db, source="test")

    # refresh_instrument_catalog should have been called (bootstrap)
    mock_refresh.assert_called_once_with(db)
    # Cycle should complete successfully
    assert result["status"] in ("pending", "blocked")
    assert "recommendation_id" in result


# ---------------------------------------------------------------------------
# Test 5: Scheduler still doesn't import/trigger execution
# ---------------------------------------------------------------------------


def test_scheduler_no_execution_imports():
    """Scheduler module must NEVER import from services.execution.
    This is a critical safety invariant.
    """
    from app.scheduler import jobs

    source = inspect.getsource(jobs)
    assert "approve_and_execute" not in source, "Scheduler imports approve_and_execute!"
    assert "place_order" not in source, "Scheduler imports place_order!"
    assert "from app.services.execution" not in source, "Scheduler imports execution module!"

    # Also verify scheduled functions individually
    for fn_name in ["scheduled_ingestion", "scheduled_full_cycle"]:
        fn = getattr(jobs, fn_name, None)
        if fn:
            fn_source = inspect.getsource(fn)
            assert "approve_and_execute" not in fn_source
            assert "place_order" not in fn_source
            assert "OrderExecution" not in fn_source
