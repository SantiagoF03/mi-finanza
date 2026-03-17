"""Tests for P9: NewsEvent persistence fix for RSS provider fields.

Required tests:
1. RSS item with url/source doesn't crash NewsEvent creation
2. analysis/run with news_provider=rss doesn't crash
3. Backward compat: MockNewsProvider items still persist correctly
4. Deduplication uses title/url when both exist
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog, NewsEvent  # noqa: F401


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: RSS item with url/source persists without crash
# ---------------------------------------------------------------------------


def test_rss_item_with_url_source_persists(db):
    """An RSS news item that includes url, source, published_at must
    persist to NewsEvent without TypeError.
    """
    from app.services.orchestrator import _persist_news_without_duplicates

    rss_items = [
        {
            "title": "Fed holds rates steady",
            "event_type": "tasas",
            "impact": "neutro",
            "confidence": 0.7,
            "related_assets": ["SPY"],
            "summary": "The Federal Reserve kept rates unchanged.",
            "url": "https://reuters.com/article/123",
            "source": "reuters.com",
            "published_at": datetime.utcnow() - timedelta(hours=2),
            "created_at": datetime.utcnow(),
            # Extra field that might come from RSS parsing — should be ignored
            "link": "https://reuters.com/article/123",
        },
    ]

    inserted = _persist_news_without_duplicates(db, rss_items)
    db.commit()

    assert inserted == 1

    # Verify fields persisted correctly
    event = db.query(NewsEvent).first()
    assert event is not None
    assert event.title == "Fed holds rates steady"
    assert event.url == "https://reuters.com/article/123"
    assert event.source == "reuters.com"
    assert event.published_at is not None


# ---------------------------------------------------------------------------
# Test 2: analysis/run cycle with RSS provider doesn't crash
# ---------------------------------------------------------------------------


def test_run_cycle_with_rss_provider_no_crash(db):
    """run_cycle must complete without TypeError when news_provider=rss
    and the provider returns items with url/source fields.
    """
    from app.models.models import PortfolioPosition, PortfolioSnapshot
    from app.services.orchestrator import run_cycle

    # Create a minimal snapshot
    snap = PortfolioSnapshot(total_value=10000, cash=5000, currency="ARS")
    db.add(snap)
    db.flush()
    pos = PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="ARS", quantity=10,
        market_value=5000, pnl_pct=0.0,
    )
    db.add(pos)
    db.commit()

    fake_rss_items = [
        {
            "title": "AAPL earnings beat expectations",
            "event_type": "earnings",
            "impact": "positivo",
            "confidence": 0.75,
            "related_assets": ["AAPL"],
            "summary": "Apple reported strong Q1 results.",
            "url": "https://investing.com/news/456",
            "source": "investing.com",
            "created_at": datetime.utcnow(),
        },
    ]

    with patch("app.services.orchestrator.get_news_provider") as mock_gnp, \
         patch("app.services.orchestrator.get_provider_info") as mock_gpi:
        provider = MagicMock()
        provider.__class__.__name__ = "RssNewsProvider"
        provider.get_recent_news.return_value = fake_rss_items
        mock_gnp.return_value = provider
        mock_gpi.return_value = {"provider_class": "RssNewsProvider", "is_mock": False}

        with patch("app.services.orchestrator._load_news_items") as mock_load:
            mock_load.return_value = (fake_rss_items, "RssNewsProvider", False, provider)

            result = run_cycle(db, source="test")

    # Must not crash — either produces recommendation or skips on cooldown
    assert "error" not in result or "cooldown" in result.get("message", "").lower() or result.get("recommendation_id")


# ---------------------------------------------------------------------------
# Test 3: MockNewsProvider backward compat — items still persist
# ---------------------------------------------------------------------------


def test_mock_provider_backward_compat(db):
    """MockNewsProvider items (no url, no source) must still persist correctly."""
    from app.news.pipeline import MockNewsProvider
    from app.services.orchestrator import _persist_news_without_duplicates

    provider = MockNewsProvider()
    items = provider.get_recent_news(["AAPL", "AL30"])

    inserted = _persist_news_without_duplicates(db, items)
    db.commit()

    assert inserted == len(items)
    assert inserted > 0

    events = db.query(NewsEvent).all()
    for ev in events:
        assert ev.title  # title is always present
        assert ev.url is None  # mock items don't have URL
        assert ev.source is None  # mock items don't have source


# ---------------------------------------------------------------------------
# Test 4: Deduplication uses title/url — same URL skipped
# ---------------------------------------------------------------------------


def test_dedup_uses_url_in_persist(db):
    """When two items have the same URL but different titles,
    the second should be deduplicated.
    """
    from app.services.orchestrator import _persist_news_without_duplicates

    items = [
        {
            "title": "First article about topic",
            "event_type": "earnings",
            "impact": "positivo",
            "confidence": 0.7,
            "related_assets": ["MSFT"],
            "summary": "Summary A",
            "url": "https://reuters.com/same-article",
            "source": "reuters.com",
            "created_at": datetime.utcnow(),
        },
        {
            "title": "Updated headline for same article",
            "event_type": "earnings",
            "impact": "positivo",
            "confidence": 0.72,
            "related_assets": ["MSFT"],
            "summary": "Summary B — slightly different",
            "url": "https://reuters.com/same-article",  # same URL
            "source": "reuters.com",
            "created_at": datetime.utcnow(),
        },
        {
            "title": "Completely different article",
            "event_type": "tasas",
            "impact": "neutro",
            "confidence": 0.6,
            "related_assets": ["SPY"],
            "summary": "Different content entirely",
            "url": "https://investing.com/different",
            "source": "investing.com",
            "created_at": datetime.utcnow(),
        },
    ]

    inserted = _persist_news_without_duplicates(db, items)
    db.commit()

    assert inserted == 2, f"Expected 2 (dedup by URL), got {inserted}"

    events = db.query(NewsEvent).all()
    titles = {e.title for e in events}
    assert "First article about topic" in titles
    assert "Completely different article" in titles
    assert "Updated headline for same article" not in titles
