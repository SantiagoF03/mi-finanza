"""Tests for provider fallback consistency.

When _load_news_items falls back to MockNewsProvider, the returned
provider (4th tuple value) must be the mock — not the original that
failed — so that get_provider_info reports the effective provider.

Tests:
1. Real provider succeeds → provider_info matches real provider
2. Real provider returns empty → fallback mock + provider_info is mock
3. Real provider raises exception → fallback mock + provider_info is mock
"""

from unittest.mock import MagicMock, patch

from app.news.pipeline import MockNewsProvider, RssNewsProvider, get_provider_info


def _make_rss_provider(items=None, raise_exc=None):
    """Create a fake RssNewsProvider that returns items or raises."""
    provider = RssNewsProvider(
        urls=["https://example.com/feed.rss"],
        timeout_seconds=5,
        max_items=20,
    )
    if raise_exc:
        provider.get_recent_news = MagicMock(side_effect=raise_exc)
    else:
        provider.get_recent_news = MagicMock(return_value=items or [])
    return provider


# ---------------------------------------------------------------------------
# Test 1: Real provider succeeds → provider_info is real, not mock
# ---------------------------------------------------------------------------


def test_real_provider_success_provider_info_consistent():
    """When the real provider returns items, the effective provider must be
    the real one, and get_provider_info must report it as non-mock.
    """
    from app.services.orchestrator import _load_news_items

    fake_items = [
        {
            "title": "Fed holds rates",
            "event_type": "tasas",
            "impact": "neutro",
            "confidence": 0.7,
            "related_assets": ["SPY"],
            "summary": "Rates stable.",
            "url": "https://reuters.com/1",
            "source": "reuters.com",
        },
    ]
    provider = _make_rss_provider(items=fake_items)

    items, source, is_mock, effective = _load_news_items(
        [{"symbol": "SPY"}], provider=provider,
    )

    assert len(items) == 1
    assert is_mock is False
    assert source == "RssNewsProvider"
    assert isinstance(effective, RssNewsProvider)

    info = get_provider_info(effective)
    assert info["provider_class"] == "RssNewsProvider"
    assert info["is_mock"] is False


# ---------------------------------------------------------------------------
# Test 2: Real provider returns empty → fallback mock, provider_info is mock
# ---------------------------------------------------------------------------


def test_real_provider_empty_fallback_provider_info_is_mock():
    """When the real provider returns [], the system falls back to
    MockNewsProvider. The effective provider must be the mock, and
    get_provider_info must report is_mock=True.
    """
    from app.services.orchestrator import _load_news_items

    provider = _make_rss_provider(items=[])  # returns nothing

    items, source, is_mock, effective = _load_news_items(
        [{"symbol": "AAPL"}], provider=provider,
    )

    assert len(items) > 0, "Fallback mock should produce items"
    assert is_mock is True
    assert "MockNewsProvider(fallback)" in source
    assert isinstance(effective, MockNewsProvider), (
        f"Effective provider must be MockNewsProvider, got {type(effective).__name__}"
    )

    info = get_provider_info(effective)
    assert info["provider_class"] == "MockNewsProvider"
    assert info["is_mock"] is True


# ---------------------------------------------------------------------------
# Test 3: Real provider raises → fallback mock, provider_info is mock
# ---------------------------------------------------------------------------


def test_real_provider_exception_fallback_provider_info_is_mock():
    """When the real provider raises an exception, the system falls back
    to MockNewsProvider. The effective provider must be the mock.
    """
    from app.services.orchestrator import _load_news_items

    provider = _make_rss_provider(raise_exc=ConnectionError("timeout"))

    items, source, is_mock, effective = _load_news_items(
        [{"symbol": "AL30"}], provider=provider,
    )

    assert len(items) > 0, "Fallback mock should produce items"
    assert is_mock is True
    assert "MockNewsProvider(fallback)" in source
    assert isinstance(effective, MockNewsProvider), (
        f"Effective provider must be MockNewsProvider, got {type(effective).__name__}"
    )

    info = get_provider_info(effective)
    assert info["provider_class"] == "MockNewsProvider"
    assert info["is_mock"] is True
