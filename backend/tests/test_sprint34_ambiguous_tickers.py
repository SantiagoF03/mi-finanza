"""Sprint 34 — False positive reduction for ambiguous ticker matching.

Two fixes:
1. held_mentions uses word-boundary regex instead of substring matching
   (prevents "V" matching any text containing letter "v")
2. AMBIGUOUS_TICKERS set filters common English words from extract_market_symbols
   (prevents "MA", "PG", "HD", "BAC" false positives from news body text)

Tests:
1.  Irrelevant news does NOT map to V (portfolio held)
2.  Irrelevant news does NOT map to MA (regex extraction)
3.  Irrelevant news does NOT map to PG, BAC, HD
4.  Real Visa news DOES map to V (when "V" appears as whole word in title)
5.  Real Mastercard news DOES map via held_mentions with word boundary
6.  extract_market_symbols filters ambiguous tickers
7.  extract_market_symbols still extracts non-ambiguous tickers
8.  held_mentions word-boundary prevents substring false positives
9.  held_mentions word-boundary allows exact word matches
10. No regression: standard tickers still extracted
11. No regression: PSEUDO_TICKER_BLOCKLIST still works
12. No regression: classify_news_event structure unchanged
"""

import re
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify(title, summary="", portfolio=None):
    from app.news.pipeline import classify_news_event
    return classify_news_event(title, summary, portfolio or [])


def _extract(text):
    from app.news.pipeline import extract_market_symbols
    return extract_market_symbols(text)


# ---------------------------------------------------------------------------
# Test 1: Irrelevant news does NOT map to V
# ---------------------------------------------------------------------------

class TestNoFalsePositiveV:
    """V should not match news that merely contains the letter 'v'."""

    def test_v_not_matched_in_generic_market_news(self):
        """'V' held in portfolio should NOT match generic market news."""
        result = _classify(
            "Mexico stocks higher at close of trade; S&P/BMV IPC up 0.37%",
            "The benchmark index advanced on Thursday.",
            portfolio=["V", "AAPL"],
        )
        assert "V" not in result["related_assets"]

    def test_v_not_matched_in_unrelated_headline(self):
        """'V' should not match via held_mentions substring in random text."""
        result = _classify(
            "Federal Reserve holds rates steady amid volatility",
            "Investors evaluate market conditions.",
            portfolio=["V"],
        )
        assert "V" not in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 2: Irrelevant news does NOT map to MA
# ---------------------------------------------------------------------------

class TestNoFalsePositiveMA:
    """MA should not be extracted from text where it's not a ticker reference."""

    def test_ma_not_extracted_from_unrelated_news(self):
        """'MA' appearing incidentally in body should be filtered."""
        result = _classify(
            "Hyundai recalls 58,000 Palisade SUVs in South Korea over safety concerns",
            "The MA recall affects 2024 models. Hyundai said it will fix the issue.",
            portfolio=[],
        )
        assert "MA" not in result["related_assets"]

    def test_ma_not_matched_via_portfolio_substring(self):
        """MA held in portfolio, 'ma' appears as substring → should not match."""
        result = _classify(
            "Global markets rally on optimism about trade deal",
            "Emerging markets showed strength.",
            portfolio=["MA"],
        )
        # "ma" is in "markets" but should NOT match with word boundary
        assert "MA" not in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 3: Irrelevant news does NOT map to PG, BAC, HD
# ---------------------------------------------------------------------------

class TestNoFalsePositivePgBacHd:

    def test_pg_not_extracted_from_body(self):
        """PG should be filtered as ambiguous ticker."""
        result = _classify(
            "Bybit Institutional Strengthens Market Position with PG-13 rated funds",
            "The exchange noted growing interest.",
            portfolio=[],
        )
        assert "PG" not in result["related_assets"]

    def test_bac_not_extracted_from_body(self):
        """BAC should be filtered as ambiguous ticker."""
        result = _classify(
            "Pfizer to seek FDA approval for Lyme disease vaccine",
            "The BAC report highlighted clinical trial results.",
            portfolio=[],
        )
        assert "BAC" not in result["related_assets"]

    def test_hd_not_extracted_from_body(self):
        """HD should be filtered as ambiguous ticker."""
        result = _classify(
            "Apollo's private credit fund limits investor withdrawals",
            "HD video conference discussed the fund's strategy.",
            portfolio=[],
        )
        assert "HD" not in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 4: Real company news DOES map correctly
# ---------------------------------------------------------------------------

class TestRealCompanyNewsStillWorks:
    """Legitimate company references must still produce correct matches."""

    def test_visa_detected_by_held_mentions_word_boundary(self):
        """If V is held and 'V' appears as whole word in text → should match."""
        result = _classify(
            "V reports record quarterly earnings, shares rise 5%",
            "Visa Inc. beat analyst expectations.",
            portfolio=["V"],
        )
        assert "V" in result["related_assets"]

    def test_aapl_still_detected_normally(self):
        """Standard non-ambiguous tickers still work."""
        result = _classify(
            "Apple AAPL announces new iPhone lineup",
            "The company revealed three new models.",
            portfolio=[],
        )
        assert "AAPL" in result["related_assets"]

    def test_googl_still_detected_normally(self):
        symbols = _extract("GOOGL announces new AI features at developer conference")
        assert "GOOGL" in symbols

    def test_meta_still_detected_normally(self):
        symbols = _extract("META reports strong advertising revenue growth")
        assert "META" in symbols

    def test_meli_still_detected_normally(self):
        symbols = _extract("MELI expands logistics network across Latin America")
        assert "MELI" in symbols

    def test_tsla_still_detected_normally(self):
        symbols = _extract("TSLA deliveries beat expectations in Q3")
        assert "TSLA" in symbols

    def test_held_symbol_exact_match_works(self):
        """Non-ambiguous held symbol with exact word match → detected."""
        result = _classify(
            "GGAL shares surge on Argentine bank sector rally",
            "Banco Galicia led the gains.",
            portfolio=["GGAL"],
        )
        assert "GGAL" in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 5: extract_market_symbols filtering
# ---------------------------------------------------------------------------

class TestExtractMarketSymbolsFiltering:

    def test_ambiguous_tickers_filtered(self):
        """Ambiguous tickers should not appear in extracted symbols."""
        from app.news.pipeline import AMBIGUOUS_TICKERS
        text = "The MA and PG report showed HD content about BAC analysis"
        symbols = _extract(text)
        for amb in ["MA", "PG", "HD", "BAC"]:
            assert amb not in symbols, f"{amb} should be filtered as ambiguous"

    def test_non_ambiguous_still_extracted(self):
        text = "AAPL GOOGL MELI TSLA NVDA reported earnings"
        symbols = _extract(text)
        for sym in ["AAPL", "GOOGL", "MELI", "TSLA", "NVDA"]:
            assert sym in symbols

    def test_existing_blacklist_still_works(self):
        """USD, ARS, FED, etc. still blocked."""
        symbols = _extract("USD ARS FED CPI IPC AI ETF are common terms")
        for blocked in ["USD", "ARS", "FED", "CPI", "IPC", "AI", "ETF"]:
            assert blocked not in symbols


# ---------------------------------------------------------------------------
# Test 6: held_mentions word-boundary matching
# ---------------------------------------------------------------------------

class TestHeldMentionsWordBoundary:

    def test_short_ticker_no_substring_match(self):
        """'V' as portfolio symbol should NOT match 'volatility'."""
        result = _classify(
            "Market volatility increases across sectors",
            "",
            portfolio=["V"],
        )
        assert "V" not in result["related_assets"]

    def test_two_char_ticker_no_substring_match(self):
        """'MA' should NOT match 'market' or 'materials'."""
        result = _classify(
            "Raw materials prices surge in commodities market",
            "",
            portfolio=["MA"],
        )
        assert "MA" not in result["related_assets"]

    def test_exact_word_match_works(self):
        """'AAPL' as exact word in text → detected via held_mentions."""
        result = _classify(
            "Analysts upgrade AAPL after strong iPhone sales",
            "",
            portfolio=["AAPL"],
        )
        assert "AAPL" in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 7: No regression — classify_news_event structure
# ---------------------------------------------------------------------------

class TestNoRegressionStructure:

    def test_classify_returns_all_required_fields(self):
        result = _classify("Test headline about earnings", "Test summary", ["AAPL"])
        assert "event_type" in result
        assert "impact" in result
        assert "confidence" in result
        assert "related_assets" in result
        assert "news_relevance" in result

    def test_classify_confidence_range(self):
        result = _classify("Banco central sube tasas por inflación",
                           "La medida busca contener inflación", ["GGAL", "AL30"])
        assert 0.5 <= result["confidence"] <= 0.95


# ---------------------------------------------------------------------------
# Test 8: AMBIGUOUS_TICKERS is a frozenset
# ---------------------------------------------------------------------------

class TestAmbiguousTickersConfig:

    def test_ambiguous_tickers_is_frozenset(self):
        from app.news.pipeline import AMBIGUOUS_TICKERS
        assert isinstance(AMBIGUOUS_TICKERS, frozenset)

    def test_known_problematic_tickers_in_set(self):
        from app.news.pipeline import AMBIGUOUS_TICKERS
        for sym in ["V", "MA", "PG", "BAC", "HD", "DIS", "LOW", "CAT"]:
            assert sym in AMBIGUOUS_TICKERS, f"{sym} should be in AMBIGUOUS_TICKERS"

    def test_real_tickers_not_in_set(self):
        from app.news.pipeline import AMBIGUOUS_TICKERS
        for sym in ["AAPL", "GOOGL", "META", "TSLA", "MELI", "NVDA", "GGAL"]:
            assert sym not in AMBIGUOUS_TICKERS, f"{sym} should NOT be in AMBIGUOUS_TICKERS"
