"""Sprint 29 — Feed health: redirect handling, feed resilience, stats accuracy.

Tests:
1. httpx client uses follow_redirects=True and User-Agent header
2. Failed feeds don't break the pipeline (graceful fallback)
3. Feed stats correctly track ok/error per feed
4. company_specific items are prioritized above macro_generic before truncation
5. Dedup and recency filter remain intact after changes
6. Default feed list no longer includes Yahoo Finance RSS
7. Redirect responses (301/302) are followed successfully
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from xml.etree.ElementTree import Element, SubElement, tostring

import pytest

from app.news.pipeline import (
    RssNewsProvider,
    classify_news_relevance,
    deduplicate_news_items,
    parse_rss_items,
)
from app.core.config import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rss_xml(items: list[dict]) -> str:
    """Build minimal RSS XML from a list of {title, description, link, pubDate} dicts."""
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "Test Feed"
    for item_data in items:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = item_data.get("title", "")
        SubElement(item, "description").text = item_data.get("description", "")
        SubElement(item, "link").text = item_data.get("link", "")
        if "pubDate" in item_data:
            SubElement(item, "pubDate").text = item_data["pubDate"]
    return tostring(rss, encoding="unicode")


def _recent_rfc2822() -> str:
    """Return a pubDate string for 1 hour ago in RFC 2822."""
    dt = datetime.now(timezone.utc) - timedelta(hours=1)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")


# ---------------------------------------------------------------------------
# Test 1: httpx client configuration
# ---------------------------------------------------------------------------

class TestHttpxClientConfig:
    """Verify RssNewsProvider creates httpx.Client with correct settings."""

    def test_follow_redirects_and_user_agent(self):
        """The httpx.Client must use follow_redirects=True and a User-Agent header."""
        provider = RssNewsProvider(urls=[], timeout_seconds=5, max_items=10)

        # Patch httpx.Client to capture constructor args
        captured = {}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with patch("app.news.pipeline.httpx.Client", FakeClient):
            provider.get_recent_news([])

        assert captured.get("follow_redirects") is True, "follow_redirects must be True"
        headers = captured.get("headers", {})
        assert "User-Agent" in headers, "User-Agent header must be set"
        assert "MiFinanza" in headers["User-Agent"]


# ---------------------------------------------------------------------------
# Test 2: Failed feeds don't break the pipeline
# ---------------------------------------------------------------------------

class TestFeedResilience:
    """One broken feed must not prevent other feeds from being processed."""

    def test_one_feed_fails_others_succeed(self):
        """If feed #1 raises, feed #2 items are still returned."""
        good_xml = _build_rss_xml([
            {"title": "AAPL beats earnings estimates", "description": "Strong Q3",
             "link": "https://example.com/1", "pubDate": _recent_rfc2822()},
        ])

        call_count = {"n": 0}

        class FakeResponse:
            def __init__(self, text, status_code=200):
                self.text = text
                self.status_code = status_code
                self.is_redirect = False

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise Exception(f"HTTP {self.status_code}")

        class FakeClient:
            def __init__(self, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def get(self, url, **kwargs):
                call_count["n"] += 1
                if "broken" in url:
                    raise ConnectionError("DNS resolution failed")
                return FakeResponse(good_xml)

        provider = RssNewsProvider(
            urls=["https://broken.example.com/feed", "https://good.example.com/feed"],
            timeout_seconds=5,
            max_items=20,
        )

        with patch("app.news.pipeline.httpx.Client", FakeClient):
            items = provider.get_recent_news(["AAPL"])

        assert len(items) >= 1
        stats = provider.last_fetch_stats
        assert stats["feeds_attempted"] == 2
        assert stats["feeds_ok"] == 1
        # The broken feed should appear as error in feed_details
        details = stats["feed_details"]
        errors = [d for d in details if d["status"] == "error"]
        assert len(errors) == 1
        assert "broken" in errors[0]["url"]


# ---------------------------------------------------------------------------
# Test 3: Feed stats accuracy
# ---------------------------------------------------------------------------

class TestFeedStats:
    """Verify last_fetch_stats tracks all metrics correctly."""

    def test_stats_include_company_specific_count(self):
        """Stats must report company_specific_count and macro_generic_count."""
        xml = _build_rss_xml([
            {"title": "TSLA earnings beat expectations", "description": "Revenue up 20%",
             "link": "https://example.com/1", "pubDate": _recent_rfc2822()},
            {"title": "Markets rally on optimism", "description": "Global stocks up",
             "link": "https://example.com/2", "pubDate": _recent_rfc2822()},
        ])

        class FakeResponse:
            def __init__(self):
                self.text = xml
                self.status_code = 200
            def raise_for_status(self):
                pass

        class FakeClient:
            def __init__(self, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def get(self, url, **kwargs):
                return FakeResponse()

        provider = RssNewsProvider(urls=["https://test.com/feed"], timeout_seconds=5, max_items=20)
        with patch("app.news.pipeline.httpx.Client", FakeClient):
            provider.get_recent_news(["TSLA"])

        stats = provider.last_fetch_stats
        assert "company_specific_count" in stats
        assert "macro_generic_count" in stats
        assert stats["company_specific_count"] >= 1  # "earnings" triggers company_specific
        assert stats["feeds_ok"] == 1


# ---------------------------------------------------------------------------
# Test 4: company_specific prioritized before truncation
# ---------------------------------------------------------------------------

class TestCompanySpecificPriority:
    """company_specific items must come before macro_generic after sorting."""

    def test_company_specific_sorted_first(self):
        """When max_items truncates, company_specific items survive over macro."""
        now = datetime.now(timezone.utc) - timedelta(hours=1)
        pub_date = now.strftime("%a, %d %b %Y %H:%M:%S +0000")

        # Build 3 macro + 2 company-specific items
        items_data = []
        for i in range(3):
            items_data.append({
                "title": f"Global markets move on inflation #{i}",
                "description": "Macro headline",
                "link": f"https://example.com/macro{i}",
                "pubDate": pub_date,
            })
        for i in range(2):
            items_data.append({
                "title": f"AAPL earnings beat estimates #{i}",
                "description": "Strong quarterly results",
                "link": f"https://example.com/company{i}",
                "pubDate": pub_date,
            })

        xml = _build_rss_xml(items_data)

        class FakeResponse:
            def __init__(self):
                self.text = xml
                self.status_code = 200
            def raise_for_status(self):
                pass

        class FakeClient:
            def __init__(self, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def get(self, url, **kwargs):
                return FakeResponse()

        # max_items=3 forces truncation — company_specific should be in the top
        provider = RssNewsProvider(urls=["https://test.com/feed"], timeout_seconds=5, max_items=3)
        with patch("app.news.pipeline.httpx.Client", FakeClient):
            result = provider.get_recent_news(["AAPL"])

        assert len(result) == 3
        company_items = [r for r in result if r.get("news_relevance") == "company_specific"]
        assert len(company_items) == 2, "Both company_specific items must survive truncation"


# ---------------------------------------------------------------------------
# Test 5: Dedup and recency filter unchanged
# ---------------------------------------------------------------------------

class TestDedupAndRecency:
    """Verify dedup and recency filter work correctly after changes."""

    def test_dedup_by_title_and_url(self):
        items = [
            {"title": "AAPL earnings", "summary": "Good", "url": "https://a.com/1"},
            {"title": "AAPL earnings", "summary": "Good", "url": "https://a.com/1"},
            {"title": "AAPL earnings", "summary": "Different", "url": "https://a.com/2"},
        ]
        result = deduplicate_news_items(items)
        assert len(result) == 2

    def test_old_items_filtered_by_recency(self):
        """Items older than their event_type window must be dropped."""
        old_date = (datetime.now(timezone.utc) - timedelta(hours=50)).strftime(
            "%a, %d %b %Y %H:%M:%S +0000"
        )
        recent_date = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime(
            "%a, %d %b %Y %H:%M:%S +0000"
        )

        xml = _build_rss_xml([
            {"title": "Old earnings news", "description": "Stale",
             "link": "https://example.com/old", "pubDate": old_date},
            {"title": "Fresh earnings beat", "description": "New",
             "link": "https://example.com/new", "pubDate": recent_date},
        ])

        class FakeResponse:
            def __init__(self):
                self.text = xml
                self.status_code = 200
            def raise_for_status(self):
                pass

        class FakeClient:
            def __init__(self, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def get(self, url, **kwargs):
                return FakeResponse()

        provider = RssNewsProvider(urls=["https://test.com/feed"], timeout_seconds=5, max_items=20)
        with patch("app.news.pipeline.httpx.Client", FakeClient):
            result = provider.get_recent_news([])

        # Old earnings (50h) > 24h window → filtered out. Only fresh one survives.
        assert len(result) == 1
        assert "Fresh" in result[0]["title"]


# ---------------------------------------------------------------------------
# Test 6: Default feeds no longer include Yahoo Finance
# ---------------------------------------------------------------------------

class TestDefaultFeeds:
    """Verify default config uses stable feeds."""

    def test_no_yahoo_finance_in_defaults(self):
        """Yahoo Finance RSS (deprecated/404) must not be in default feeds."""
        settings = Settings()
        for url in settings.news_rss_urls:
            assert "feeds.finance.yahoo.com" not in url, f"Dead Yahoo feed found: {url}"

    def test_cnbc_in_defaults(self):
        """CNBC business RSS should be in default feeds."""
        settings = Settings()
        cnbc_feeds = [u for u in settings.news_rss_urls if "cnbc.com" in u]
        assert len(cnbc_feeds) >= 1, "CNBC feed must be in defaults"

    def test_four_feeds_configured(self):
        """Default config should have 4 feeds."""
        settings = Settings()
        assert len(settings.news_rss_urls) == 4


# ---------------------------------------------------------------------------
# Test 7: classify_news_relevance still works correctly
# ---------------------------------------------------------------------------

class TestClassifyNewsRelevance:
    """Verify company_specific vs macro_generic classification."""

    @pytest.mark.parametrize("title,expected", [
        ("AAPL beats earnings expectations", "company_specific"),
        ("Tesla CEO resigns amid controversy", "company_specific"),
        ("Microsoft acquires gaming company", "company_specific"),
        ("Fed raises rates, markets drop", "macro_generic"),
        ("Global markets rally on optimism", "macro_generic"),
        ("Nvidia revenue surges 200%", "company_specific"),
        ("Oil prices climb on supply fears", "macro_generic"),
        ("Google announces stock split", "company_specific"),
        ("Dividend increase announced by JPM", "company_specific"),
        ("Amazon lawsuit settlement reached", "company_specific"),
    ])
    def test_relevance_classification(self, title, expected):
        result = classify_news_relevance(title, "")
        assert result == expected, f"'{title}' should be {expected}, got {result}"


# ---------------------------------------------------------------------------
# Test 8: Redirect handling (301/302 followed)
# ---------------------------------------------------------------------------

class TestRedirectHandling:
    """Verify that 301/302 redirects are followed via httpx config."""

    def test_redirect_followed_via_client_config(self):
        """The httpx.Client constructor must pass follow_redirects=True."""
        import app.news.pipeline as pipeline_mod

        captured_kwargs = {}
        original_client = pipeline_mod.httpx.Client

        class SpyClient:
            def __init__(self, **kwargs):
                captured_kwargs.update(kwargs)
                self._real = original_client(**kwargs)

            def __enter__(self):
                self._real.__enter__()
                return self

            def __exit__(self, *args):
                return self._real.__exit__(*args)

            def get(self, url, **kwargs):
                # Don't actually fetch — raise to short-circuit
                raise StopIteration("spy done")

        provider = RssNewsProvider(urls=["https://example.com/feed"], timeout_seconds=5, max_items=10)
        with patch.object(pipeline_mod.httpx, "Client", SpyClient):
            provider.get_recent_news([])

        assert captured_kwargs.get("follow_redirects") is True
