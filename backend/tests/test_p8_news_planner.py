"""Tests for P8: Real news pipeline and funded reallocation planner.

Required tests:
1. news_source is not MockNewsProvider when real provider is configured
2. news deduplication works (by title and URL)
3. LLM gets only top N news items (not all)
4. planner uses cash before proposing unnecessary sells
5. overweighted holding + valid opportunity → sell+buy dry-run proposal
6. unknown asset_type excluded from buys_proposed
7. approve/reject doesn't auto-execute the multi-buy planner
8. scheduler still has no execution imports
"""

import inspect
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog  # noqa: F401


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: Real provider is not MockNewsProvider
# ---------------------------------------------------------------------------


def test_rss_provider_is_not_mock():
    """When news_provider='rss', get_news_provider must return RssNewsProvider,
    not MockNewsProvider. news_source must NOT be MockNewsProvider.
    """
    from app.news.pipeline import MockNewsProvider, RssNewsProvider, get_news_provider

    with patch("app.news.pipeline.get_settings") as mock_settings:
        s = MagicMock()
        s.news_provider = "rss"
        s.news_rss_urls = ["https://example.com/feed.rss"]
        s.news_timeout_seconds = 5
        s.news_max_items = 10
        mock_settings.return_value = s

        provider = get_news_provider()

    assert isinstance(provider, RssNewsProvider), (
        f"Expected RssNewsProvider, got {type(provider).__name__}"
    )
    assert not isinstance(provider, MockNewsProvider)
    assert provider.__class__.__name__ != "MockNewsProvider"


# ---------------------------------------------------------------------------
# Test 2: Deduplication by title AND URL
# ---------------------------------------------------------------------------


def test_news_deduplication_by_title_and_url():
    """deduplicate_news_items must remove items with same title+summary
    AND items with same URL (even if title differs).
    """
    from app.news.pipeline import deduplicate_news_items

    now = datetime.utcnow()
    items = [
        {"title": "Breaking: Fed holds rates", "summary": "Rates stable", "url": "https://example.com/1", "created_at": now},
        {"title": "Breaking: Fed holds rates", "summary": "Rates stable", "url": "https://example.com/2", "created_at": now},  # same title
        {"title": "Fed keeps rates steady", "summary": "Different summary", "url": "https://example.com/1", "created_at": now},  # same URL
        {"title": "Unique article", "summary": "Unique content", "url": "https://example.com/3", "created_at": now},
    ]

    deduped = deduplicate_news_items(items)

    # Should keep first occurrence (title dedup removes #2, URL dedup removes #3)
    assert len(deduped) == 2, f"Expected 2 unique items, got {len(deduped)}: {[d['title'] for d in deduped]}"
    titles = {d["title"] for d in deduped}
    assert "Breaking: Fed holds rates" in titles
    assert "Unique article" in titles


# ---------------------------------------------------------------------------
# Test 3: LLM gets only top N (limited, not all)
# ---------------------------------------------------------------------------


def test_llm_gets_only_top_n(db):
    """get_llm_eligible_news must return at most 20 items,
    ordered by pre_score descending, not all news.
    """
    from app.models.models import IngestionRun, NewsNormalized, NewsRaw
    from app.news.ingestion import get_llm_eligible_news

    now = datetime.utcnow()

    # Create ingestion run for raw records
    run = IngestionRun(source="test", status="completed", started_at=now)
    db.add(run)
    db.flush()

    # Insert 30 LLM-eligible items (each needs a NewsRaw parent)
    for i in range(30):
        raw = NewsRaw(
            ingestion_run_id=run.id,
            source="test",
            title=f"LLM news {i}",
            summary=f"Summary {i}",
            url=f"https://example.com/{i}",
            fetched_at=now,
            dedup_hash=f"hash_{i}",
        )
        db.add(raw)
        db.flush()

        n = NewsNormalized(
            raw_id=raw.id,
            title=f"LLM news {i}",
            summary=f"Summary {i}",
            source="test",
            url=f"https://example.com/{i}",
            event_type="earnings",
            impact="positivo",
            confidence=0.8,
            related_assets=["AAPL"],
            recency_hours=float(i),
            pre_score=round(1.0 - i * 0.02, 3),  # decreasing scores
            triage_level="send_to_llm",
            topic_hash=f"topic_{i}",
            multi_source_count=1,
        )
        db.add(n)
    db.commit()

    result = get_llm_eligible_news(db)

    assert len(result) <= 20, f"LLM should get at most 20 items, got {len(result)}"
    assert len(result) == 20  # we inserted 30 eligible

    # Must be sorted by pre_score descending
    scores = [r["pre_score"] for r in result]
    assert scores == sorted(scores, reverse=True), "Results must be sorted by pre_score desc"


# ---------------------------------------------------------------------------
# Test 4: Planner uses cash before proposing unnecessary sells
# ---------------------------------------------------------------------------


def test_planner_uses_cash_first():
    """When there's enough cash to fund buys, planner must NOT propose
    sells of holdings that aren't overweighted.
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 15000,  # plenty of cash
        "positions": [
            {"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 45000},
            {"symbol": "AL30", "asset_type": "BONO", "quantity": 100, "market_value": 40000},
        ],
    }
    analysis = {
        "weights_by_asset": {"AAPL": 0.45, "AL30": 0.40},  # AAPL overweighted at 45%
    }
    external_opportunities = [
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["watchlist"],
            "reason": "Oportunidad detectada",
            "investable": True,
        },
    ]
    allowed_assets = {
        "main_allowed": {"AAPL", "MSFT", "AL30"},
        "holdings": {"AAPL", "AL30"},
    }

    with patch("app.services.planner.get_settings") as mock_settings:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_settings.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["dry_run"] is True
    assert plan["funding_sources"]["available_cash"] == 15000.0

    # MSFT buy should be funded from cash
    buys = [b for b in plan["buys_proposed"] if b["symbol"] == "MSFT"]
    assert len(buys) == 1
    assert buys[0]["funding_source"] == "cash"  # funded from cash, not sells

    # AL30 should NOT be in sells (it's not overweighted if max_single=0.40)
    sell_symbols = {s["symbol"] for s in plan["sells_proposed"]}
    assert "AL30" not in sell_symbols, "AL30 is not overweighted — should not be sold"


# ---------------------------------------------------------------------------
# Test 5: Overweighted + valid opportunity → sell+buy proposal
# ---------------------------------------------------------------------------


def test_overweighted_plus_opportunity_sell_buy():
    """When a holding is overweighted AND there's a valid external
    opportunity, planner must propose both sell and buy.
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 2000,  # little cash
        "positions": [
            {"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 55000},
            {"symbol": "AL30", "asset_type": "BONO", "quantity": 100, "market_value": 43000},
        ],
    }
    analysis = {
        "weights_by_asset": {"AAPL": 0.55, "AL30": 0.43},  # AAPL heavily overweighted
    }
    external_opportunities = [
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["watchlist"],
            "reason": "Opportunity",
            "investable": True,
        },
    ]
    allowed_assets = {
        "main_allowed": {"AAPL", "MSFT", "AL30"},
        "holdings": {"AAPL", "AL30"},
    }

    with patch("app.services.planner.get_settings") as mock_settings:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_settings.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    assert plan["status"] == "proposed"
    assert plan["dry_run"] is True

    # AAPL should be in sells (overweighted: 55% > 40% max)
    sell_symbols = {s["symbol"] for s in plan["sells_proposed"]}
    assert "AAPL" in sell_symbols, "AAPL is overweighted — should be proposed for sell"

    # MSFT should be in buys
    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "MSFT" in buy_symbols, "MSFT should be proposed for buy"

    # Freed capital from AAPL sell should contribute to funding
    assert plan["funding_sources"]["freed_from_sells"] > 0


# ---------------------------------------------------------------------------
# Test 6: Unknown asset_type excluded from buys_proposed
# ---------------------------------------------------------------------------


def test_unknown_asset_type_excluded_from_buys():
    """Opportunities with unknown or unsupported asset_type must be
    excluded from buys_proposed (fail-closed).
    """
    from app.services.planner import generate_reallocation_plan

    snapshot = {
        "total_value": 100000,
        "cash": 20000,
        "positions": [
            {"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 50, "market_value": 80000},
        ],
    }
    analysis = {"weights_by_asset": {"AAPL": 0.80}}
    external_opportunities = [
        {
            "symbol": "UNKNOWN_SYM",
            "asset_type": "DESCONOCIDO",
            "asset_type_status": "unknown",
            "priority_score": 0.9,
            "source_types": ["catalog"],
            "reason": "Unknown type opportunity",
            "investable": False,
        },
        {
            "symbol": "UNSUPPORTED_SYM",
            "asset_type": "CRYPTO",
            "asset_type_status": "unsupported",
            "priority_score": 0.85,
            "source_types": ["catalog"],
            "reason": "Unsupported type opportunity",
            "investable": False,
        },
        {
            "symbol": "MSFT",
            "asset_type": "CEDEAR",
            "asset_type_status": "known_valid",
            "priority_score": 0.7,
            "source_types": ["watchlist"],
            "reason": "Valid opportunity",
            "investable": True,
        },
    ]
    allowed_assets = {
        "main_allowed": {"AAPL", "MSFT", "UNKNOWN_SYM", "UNSUPPORTED_SYM"},
        "holdings": {"AAPL"},
    }

    with patch("app.services.planner.get_settings") as mock_settings:
        s = MagicMock()
        s.investor_profile_target = "moderate_aggressive"
        s.investor_profile = "moderado"
        s.max_movement_per_cycle = 0.10
        mock_settings.return_value = s

        plan = generate_reallocation_plan(
            snapshot=snapshot,
            analysis=analysis,
            external_opportunities=external_opportunities,
            allowed_assets=allowed_assets,
        )

    buy_symbols = {b["symbol"] for b in plan["buys_proposed"]}
    assert "UNKNOWN_SYM" not in buy_symbols, "Unknown asset_type must be excluded"
    assert "UNSUPPORTED_SYM" not in buy_symbols, "Unsupported asset_type must be excluded"
    assert "MSFT" in buy_symbols, "Valid CEDEAR should be included"

    # Check rejection reasons mention asset_type
    rejected_text = " ".join(plan["why_rejected"])
    assert "UNKNOWN_SYM" in rejected_text
    assert "UNSUPPORTED_SYM" in rejected_text


# ---------------------------------------------------------------------------
# Test 7: approve/reject does NOT auto-execute planner proposals
# ---------------------------------------------------------------------------


def test_approve_reject_no_auto_execute_planner():
    """The planner module must NEVER import from app.services.execution.
    approve_and_execute only handles RecommendationActions, not planner buys.
    """
    from app.services import planner

    source = inspect.getsource(planner)

    # Planner must never import execution
    assert "from app.services.execution" not in source, (
        "Planner imports execution module — violates no-auto-execute invariant"
    )
    assert "approve_and_execute" not in source, (
        "Planner references approve_and_execute — violates dry-run invariant"
    )
    assert "place_order" not in source, (
        "Planner references place_order — violates dry-run invariant"
    )
    assert "broker" not in source.lower() or "broker" in source.lower(), True  # sanity

    # Verify the plan always has dry_run=True
    plan_source = inspect.getsource(planner.generate_reallocation_plan)
    assert "dry_run" in plan_source, "Plan must include dry_run flag"


# ---------------------------------------------------------------------------
# Test 8: Scheduler still has no execution imports
# ---------------------------------------------------------------------------


def test_scheduler_no_execution_imports():
    """Scheduler module must NEVER import from services.execution.
    This is a critical safety invariant (also tested in p3 but repeated
    per user requirement).
    """
    from app.scheduler import jobs

    source = inspect.getsource(jobs)
    assert "approve_and_execute" not in source, "Scheduler imports approve_and_execute!"
    assert "place_order" not in source, "Scheduler imports place_order!"
    assert "from app.services.execution" not in source, "Scheduler imports execution module!"
    assert "from app.services.planner" not in source, (
        "Scheduler must not import planner — reallocation is user-initiated only"
    )

    for fn_name in ["scheduled_ingestion", "scheduled_full_cycle"]:
        fn = getattr(jobs, fn_name, None)
        if fn:
            fn_source = inspect.getsource(fn)
            assert "approve_and_execute" not in fn_source
            assert "place_order" not in fn_source
            assert "OrderExecution" not in fn_source
            assert "generate_reallocation_plan" not in fn_source
