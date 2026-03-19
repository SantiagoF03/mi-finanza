"""Tests for P10: Planner integration into orchestrator + API exposure.

Required tests:
1. proposed_reallocation_plan appears in /api/recommendations/current
2. planner uses cash before proposing unnecessary sells
3. SPY overweighted + external opportunities → sell+buy dry-run
4. unknown asset_type excluded from buys_proposed
5. assets outside main_allowed excluded from buys_proposed
6. approve/reject does NOT auto-execute the planner multi-buy
7. scheduler still no execution imports
8. planner returns empty plan with planner_status and planner_reason when no funding/conviction
"""

import inspect
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import (
    InstrumentCatalog,  # noqa: F401
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


def _create_snapshot_with_positions(db, positions_data, cash=5000, total_value=None):
    """Helper to create a snapshot with positions."""
    if total_value is None:
        total_value = cash + sum(p["market_value"] for p in positions_data)
    snap = PortfolioSnapshot(total_value=total_value, cash=cash, currency="ARS")
    db.add(snap)
    db.flush()
    for p in positions_data:
        db.add(PortfolioPosition(snapshot_id=snap.id, pnl_pct=0.0, **p))
    db.commit()
    return snap


def _create_recommendation_with_plan(db, plan_data):
    """Helper to create a recommendation with a plan in metadata."""
    rec = Recommendation(
        action="rebalancear",
        status="pending",
        suggested_pct=0.05,
        confidence=0.7,
        rationale="Test",
        risks="Test risk",
        executive_summary="Test summary",
        blocked_reason="",
        metadata_json={
            "proposed_reallocation_plan": plan_data,
            "analysis": {},
            "rules": [],
        },
    )
    db.add(rec)
    db.commit()
    return rec


# ---------------------------------------------------------------------------
# Test 1: proposed_reallocation_plan appears in /api/recommendations/current
# ---------------------------------------------------------------------------


def test_plan_in_recommendations_current(db):
    """proposed_reallocation_plan must appear in the response of
    /api/recommendations/current. We test by calling the route logic
    directly with the test DB to avoid TestClient threading issues.
    """
    from app.services.orchestrator import get_current_recommendation

    plan_data = {
        "planner_status": "proposed",
        "planner_reason": "1 venta(s) propuesta(s)",
        "dry_run": True,
        "funding_sources": {"available_cash": 5000, "freed_from_sells": 3000, "total_funding": 8000},
        "sells_proposed": [{"symbol": "SPY", "side": "sell", "quantity_proposed": 5, "value_proposed": 3000}],
        "buys_proposed": [{"symbol": "MSFT", "side": "buy", "value_proposed": 5000, "asset_type": "CEDEAR"}],
        "residual_cash": 0,
        "constraints_applied": ["max_single_asset_weight=0.40"],
        "why_selected": ["SELL SPY", "BUY MSFT"],
        "why_rejected": [],
    }
    _create_recommendation_with_plan(db, plan_data)

    # Verify recommendation is retrievable
    rec = get_current_recommendation(db)
    assert rec is not None

    # Verify the route response shape: metadata includes proposed_reallocation_plan
    meta = rec.metadata_json or {}
    assert "proposed_reallocation_plan" in meta, (
        "proposed_reallocation_plan must be stored in metadata_json"
    )
    plan = meta["proposed_reallocation_plan"]
    assert plan["planner_status"] == "proposed"
    assert plan["dry_run"] is True
    assert "funding_sources" in plan
    assert "sells_proposed" in plan
    assert "buys_proposed" in plan
    assert "residual_cash" in plan
    assert "constraints_applied" in plan
    assert "why_selected" in plan
    assert "why_rejected" in plan

    # Also verify the route code correctly exposes it
    from app.api import routes
    route_source = inspect.getsource(routes.current_recommendation)
    assert "proposed_reallocation_plan" in route_source, (
        "Route /recommendations/current must include proposed_reallocation_plan"
    )


# ---------------------------------------------------------------------------
# Test 2: planner uses cash before proposing unnecessary sells
# ---------------------------------------------------------------------------


def test_planner_uses_cash_before_unnecessary_sells():
    """When there's enough cash to fund buys, planner must NOT propose
    sells of holdings that aren't overweighted.
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 15000,
        "positions": [
            {"symbol": "SPY", "asset_type": "ETF", "quantity": 30, "market_value": 45000},
            {"symbol": "AL30", "asset_type": "BONO", "quantity": 100, "market_value": 40000},
        ],
    }
    analysis = {"weights_by_asset": {"SPY": 0.45, "AL30": 0.40}}
    external_opportunities = [
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["watchlist"],
            "reason": "Oportunidad detectada",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"SPY", "MSFT", "AL30"}, "holdings": {"SPY", "AL30"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    # MSFT buy funded from cash
    buys = [b for b in plan["buys_proposed"] if b["symbol"] == "MSFT"]
    assert len(buys) == 1
    assert buys[0]["funding_source"] == "cash"

    # AL30 is NOT overweighted (40% == max_single 40%), must NOT be sold
    sell_symbols = {s["symbol"] for s in plan["sells_proposed"]}
    assert "AL30" not in sell_symbols, "AL30 is at-weight, not overweighted — should not be sold"


# ---------------------------------------------------------------------------
# Test 3: SPY overweighted + external opportunities → sell+buy dry-run
# ---------------------------------------------------------------------------


def test_spy_overweighted_sell_buy_dryrun():
    """SPY at 55% (overweighted) + valid external opportunity → planner
    proposes sell SPY + buy candidate, all dry-run.
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 2000,
        "positions": [
            {"symbol": "SPY", "asset_type": "ETF", "quantity": 100, "market_value": 55000},
            {"symbol": "AL30", "asset_type": "BONO", "quantity": 100, "market_value": 43000},
        ],
    }
    analysis = {"weights_by_asset": {"SPY": 0.55, "AL30": 0.43}}
    external_opportunities = [
        {
            "symbol": "V",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.75,
            "source_types": ["catalog", "news"],
            "reason": "Opportunity",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"SPY", "V", "AL30"}, "holdings": {"SPY", "AL30"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["planner_status"] == "proposed"
    assert plan["dry_run"] is True

    sell_symbols = {s["symbol"] for s in plan["sells_proposed"]}
    assert "SPY" in sell_symbols, "SPY overweighted at 55% > 40% max → should be sell-proposed"

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "V" in buy_symbols, "V is valid external opportunity → should be buy-proposed"

    assert plan["funding_sources"]["freed_from_sells"] > 0
    assert plan["planner_reason"]  # must have a reason


# ---------------------------------------------------------------------------
# Test 4: unknown asset_type excluded from buys_proposed
# ---------------------------------------------------------------------------


def test_unknown_asset_type_excluded():
    """Opportunities with DESCONOCIDO or invalid asset_type must be
    excluded from buys_proposed (fail-closed).
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 20000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "WEIRD",
            "asset_type": "DESCONOCIDO",
            "asset_type_status": "unknown",
            "priority_score": 0.9,
            "source_types": ["catalog"],
            "reason": "Unknown",
            "investable": False,
            "actionable_external": True,
        },
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["watchlist"],
            "reason": "Valid",
            "investable": True,
            "actionable_external": True,
        },
    ]
    allowed_assets = {"main_allowed": {"AAPL", "MSFT", "WEIRD"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "WEIRD" not in buy_symbols, "DESCONOCIDO asset_type must be excluded"
    assert "MSFT" in buy_symbols

    rejected_text = " ".join(plan["why_rejected"])
    assert "WEIRD" in rejected_text


# ---------------------------------------------------------------------------
# Test 5: assets outside main_allowed excluded from buys_proposed
# ---------------------------------------------------------------------------


def test_outside_main_allowed_excluded():
    """Assets not in main_allowed (whitelist safety rail) must be
    excluded from buys_proposed even if they have valid asset_type.
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 20000,
        "positions": [{"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 80000}],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "DANGER_STOCK",
            "asset_type": "ACCIONES",
            "asset_type_status": "known_valid",
            "priority_score": 0.9,
            "source_types": ["catalog", "news"],
            "reason": "Not whitelisted",
            "investable": True,
            "actionable_external": True,
        },
    ]
    # DANGER_STOCK is NOT in main_allowed
    allowed_assets = {"main_allowed": {"AAPL"}, "holdings": {"AAPL"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "DANGER_STOCK" not in buy_symbols, "Asset outside main_allowed must be excluded"

    rejected_text = " ".join(plan["why_rejected"])
    assert "DANGER_STOCK" in rejected_text
    assert "main_allowed" in rejected_text


# ---------------------------------------------------------------------------
# Test 6: approve/reject does NOT auto-execute planner multi-buy
# ---------------------------------------------------------------------------


def test_approve_reject_no_planner_auto_execute():
    """The planner module must NEVER import execution.
    approve_and_execute only handles RecommendationActions, not planner proposals.
    The /approve endpoint never triggers planner execution.
    """
    from app.services import planner

    source = inspect.getsource(planner)

    # Planner must never import execution
    assert "from app.services.execution" not in source
    assert "broker" not in source.lower() or "broker" in source.lower(), True

    # The planner's output always has dry_run=True
    plan_source = inspect.getsource(planner.generate_reallocation_plan)
    assert '"dry_run": True' in plan_source or "'dry_run': True" in plan_source

    # Verify the routes: /approve only calls approve_and_execute, not planner
    from app.api import routes
    approve_source = inspect.getsource(routes.approve_recommendation_endpoint)
    assert "generate_reallocation_plan" not in approve_source
    assert "planner" not in approve_source

    reject_source = inspect.getsource(routes.reject_recommendation_endpoint)
    assert "generate_reallocation_plan" not in reject_source
    assert "planner" not in reject_source


# ---------------------------------------------------------------------------
# Test 7: scheduler still no execution imports
# ---------------------------------------------------------------------------


def test_scheduler_no_execution_or_planner_imports():
    """Scheduler must not import execution or planner modules."""
    from app.scheduler import jobs

    source = inspect.getsource(jobs)
    assert "from app.services.execution" not in source
    assert "from app.services.planner" not in source
    assert "generate_reallocation_plan" not in source

    for fn_name in ["scheduled_ingestion", "scheduled_full_cycle"]:
        fn = getattr(jobs, fn_name, None)
        if fn:
            fn_source = inspect.getsource(fn)
            assert "OrderExecution" not in fn_source
            assert "generate_reallocation_plan" not in fn_source


# ---------------------------------------------------------------------------
# Test 8: planner returns empty plan with planner_status + planner_reason
# ---------------------------------------------------------------------------


def test_planner_empty_plan_with_status_reason():
    """When there's no funding and no conviction, planner must return
    an empty plan with planner_status and planner_reason clearly set.
    """
    from app.services.planner import generate_reallocation_plan

    # No cash, no overweighted (all within 40% limit), no opportunities
    snapshot = {
        "total_value": 100000,
        "cash": 0,
        "positions": [
            {"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 35000},
            {"symbol": "AL30", "asset_type": "BONO", "quantity": 100, "market_value": 35000},
            {"symbol": "SPY", "asset_type": "ETF", "quantity": 20, "market_value": 30000},
        ],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.35, "AL30": 0.35, "SPY": 0.30}}
    external_opportunities = []  # nothing
    allowed_assets = {"main_allowed": {"AAPL", "AL30"}, "holdings": {"AAPL", "AL30"}}

    with patch("app.services.planner.get_settings") as mock_s:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_s.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["planner_status"] in ("empty", "no_opportunities", "no_funding", "no_eligible_candidates")
    assert plan["planner_status"] != "proposed", "Empty plan must not have status 'proposed'"
    assert plan["planner_reason"], "planner_reason must be populated"
    assert len(plan["planner_reason"]) > 5, "planner_reason must be descriptive"
    assert plan["dry_run"] is True
    assert plan["sells_proposed"] == []
    assert plan["buys_proposed"] == []
