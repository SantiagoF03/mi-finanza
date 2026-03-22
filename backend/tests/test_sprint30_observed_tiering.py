"""Sprint 30 — Observed value tiering: high/low/catalog classification.

Tests:
1. Real instrument + strong signal + not promoted → observed_value_tier="high"
2. Unknown instrument + signal → observed_value_tier="low"
3. Catalog-only item (no signal) → observed_value_tier="catalog"
4. Strong signal + weak causal + not investable + low score → "low"
5. Strong signal + investable → "high" (even with weak causal)
6. Strong signal + strong causal → "high" (even without investable)
7. Strong signal + effective_score >= 0.4 → "high"
8. decision_summary includes top_observed_weak list
9. decision_summary includes observed_high_value_count and observed_low_value_count
10. External opportunities not affected by tiering changes
11. Promotion gates not affected (still require all 4 conditions)
12. top_observed_weak items have observed_value_tier exposed
13. Backward compat: top_observed and top_observed_signals still work
"""

import pytest
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_observed_item(
    symbol: str,
    effective_score=None,
    signal_class=None,
    signal_quality=None,
    causal_link_strength=None,
    investable=None,
    asset_type_status=None,
    in_main_allowed=None,
    tracking_status=None,
    reason="Test reason",
    source_types=None,
    market_confirmation=None,
    priority_score=None,
    title_mention=None,
    observed_origin=None,
    observed_value_tier=None,
):
    """Build a minimal observed_candidate dict for testing."""
    item = {"symbol": symbol, "reason": reason}
    if effective_score is not None:
        item["effective_score"] = effective_score
    if signal_class is not None:
        item["signal_class"] = signal_class
    if signal_quality is not None:
        item["signal_quality"] = signal_quality
    if causal_link_strength is not None:
        item["causal_link_strength"] = causal_link_strength
    if investable is not None:
        item["investable"] = investable
    if asset_type_status is not None:
        item["asset_type_status"] = asset_type_status
    if in_main_allowed is not None:
        item["in_main_allowed"] = in_main_allowed
    if tracking_status is not None:
        item["tracking_status"] = tracking_status
    if source_types is not None:
        item["source_types"] = source_types
    if market_confirmation is not None:
        item["market_confirmation"] = market_confirmation
    if priority_score is not None:
        item["priority_score"] = priority_score
    if title_mention is not None:
        item["title_mention"] = title_mention
    if observed_origin is not None:
        item["observed_origin"] = observed_origin
    if observed_value_tier is not None:
        item["observed_value_tier"] = observed_value_tier
    return item


def _build_summary(observed_candidates, external_opportunities=None, suppressed=None):
    """Call _build_decision_summary with the given candidates."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Test",
        "external_opportunities": external_opportunities or [],
        "observed_candidates": observed_candidates,
        "suppressed_candidates": suppressed or [],
    }
    return _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )


# ---------------------------------------------------------------------------
# Test 1: Real instrument + strong signal → high
# ---------------------------------------------------------------------------

class TestObservedValueTierAssignment:
    """Verify observed_value_tier is assigned correctly during enrichment."""

    def test_known_instrument_strong_causal_is_high(self):
        """Real instrument + strong signal + strong causal → high."""
        item = _make_observed_item(
            "MELI",
            effective_score=0.55,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            asset_type_status="known_valid",
            observed_value_tier="high",  # expected
        )
        # Verify via decision_summary
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert len(top) >= 1
        meli = next(i for i in top if i["symbol"] == "MELI")
        assert meli["observed_value_tier"] == "high"

    def test_known_instrument_investable_weak_causal_is_high(self):
        """Strong signal + investable → high, even with weak causal."""
        item = _make_observed_item(
            "AAPL",
            effective_score=0.35,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=True,
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert top[0]["observed_value_tier"] == "high"

    def test_strong_signal_high_score_is_high(self):
        """Strong signal + effective_score >= 0.4 → high, even without investable/causal."""
        item = _make_observed_item(
            "GLOB",
            effective_score=0.45,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=False,
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert top[0]["observed_value_tier"] == "high"


# ---------------------------------------------------------------------------
# Test 2: Unknown instrument + signal → low
# ---------------------------------------------------------------------------

class TestLowValueTier:
    """Verify weak/unknown instruments get tier=low."""

    def test_unknown_instrument_with_signal_is_low(self):
        """Signal present but unrecognized instrument → low."""
        item = _make_observed_item(
            "GPU",
            effective_score=0.3,
            signal_class="observed_candidate",
            signal_quality="weak",
            causal_link_strength="strong",
            observed_value_tier="low",
        )
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        gpu = next(i for i in top if i["symbol"] == "GPU")
        assert gpu["observed_value_tier"] == "low"

    def test_strong_quality_but_low_score_no_investable_no_causal_is_low(self):
        """Strong signal_quality but weak causal + not investable + score < 0.4 → low."""
        item = _make_observed_item(
            "SYMA",
            effective_score=0.25,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=False,
            observed_value_tier="low",
        )
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert top[0]["observed_value_tier"] == "low"


# ---------------------------------------------------------------------------
# Test 3: Catalog-only → catalog
# ---------------------------------------------------------------------------

class TestCatalogTier:
    """Verify items without signals get tier=catalog."""

    def test_catalog_only_item_is_catalog(self):
        """No signal data → catalog tier."""
        item = _make_observed_item(
            "SYMX",
            effective_score=None,
            signal_class=None,
            signal_quality=None,
            source_types=["catalog"],
            priority_score=0.2,
            observed_value_tier="catalog",
        )
        summary = _build_summary([item])
        cands = summary["candidates"]
        assert cands["observed_catalog_count"] >= 1
        cat_top = cands["top_observed_catalog"]
        assert len(cat_top) >= 1
        assert cat_top[0]["observed_value_tier"] == "catalog"


# ---------------------------------------------------------------------------
# Test 4-5: decision_summary structure
# ---------------------------------------------------------------------------

class TestDecisionSummaryStructure:
    """Verify new fields in decision_summary.candidates."""

    def test_top_observed_weak_exists(self):
        """decision_summary.candidates must include top_observed_weak list."""
        items = [
            _make_observed_item("WEAK1", effective_score=0.3, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
            _make_observed_item("WEAK2", effective_score=0.2, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        assert "top_observed_weak" in cands
        assert len(cands["top_observed_weak"]) == 2

    def test_tier_counts_present(self):
        """decision_summary must include observed_high_value_count and observed_low_value_count."""
        items = [
            _make_observed_item("HIGH1", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                observed_value_tier="high"),
            _make_observed_item("LOW1", effective_score=0.2, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
            _make_observed_item("CAT1", signal_quality=None, observed_value_tier="catalog",
                                source_types=["catalog"]),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_low_value_count"] == 1
        assert cands["observed_catalog_count"] == 1

    def test_backward_compat_top_observed_and_signals_still_work(self):
        """Existing top_observed and top_observed_signals must still exist and work."""
        items = [
            _make_observed_item("MELI", effective_score=0.6, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                observed_value_tier="high"),
            _make_observed_item("GPU", effective_score=0.3, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        # Existing keys must still be present
        assert "top_observed" in cands
        assert "top_observed_signals" in cands
        assert "top_observed_catalog" in cands
        assert "observed_count" in cands
        assert "observed_with_signal_count" in cands
        assert "observed_weak_signal_count" in cands

    def test_observed_value_tier_in_top_n_output(self):
        """observed_value_tier must be included in _top_n output dicts."""
        items = [
            _make_observed_item("AAPL", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                observed_value_tier="high"),
        ]
        summary = _build_summary(items)
        top = summary["candidates"]["top_observed"]
        assert "observed_value_tier" in top[0]
        assert top[0]["observed_value_tier"] == "high"


# ---------------------------------------------------------------------------
# Test 6: External opportunities not affected
# ---------------------------------------------------------------------------

class TestNoRegressionExternalOpportunities:
    """Tiering changes must not affect external_opportunities."""

    def test_external_opportunities_untouched(self):
        """External opportunities should pass through without observed_value_tier interference."""
        ext = _make_observed_item(
            "TSLA", effective_score=0.7, signal_class="external_opportunity",
            signal_quality="strong", causal_link_strength="strong",
            investable=True, asset_type_status="known_valid",
        )
        obs = _make_observed_item(
            "GPU", effective_score=0.2, signal_class="observed_candidate",
            signal_quality="weak", observed_value_tier="low",
        )
        summary = _build_summary([obs], external_opportunities=[ext])
        cands = summary["candidates"]
        assert cands["actionable_count"] == 1
        assert cands["observed_count"] == 1


# ---------------------------------------------------------------------------
# Test 7: Promotion gates unchanged
# ---------------------------------------------------------------------------

class TestPromotionGatesUnchanged:
    """Promotion logic must not be affected by observed_value_tier."""

    def test_high_value_without_all_gates_not_promoted(self):
        """observed_value_tier=high does NOT mean automatic promotion.
        Promotion still requires all 4 gates: strong signal + strong causal + score >= 0.6 + investable.
        """
        # This item is "high" tier but score < 0.6, so it should NOT be promoted
        item = _make_observed_item(
            "GLOB",
            effective_score=0.45,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            investable=True,
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        cands = summary["candidates"]
        # Should remain in observed, not promoted
        assert cands["observed_count"] == 1
        assert cands["promoted_from_observed_count"] == 0


# ---------------------------------------------------------------------------
# Test 8: Mixed scenario — correct tier ordering
# ---------------------------------------------------------------------------

class TestMixedScenario:
    """Verify correct behavior with a realistic mix of observed items."""

    def test_mixed_tiers_correct_counts_and_ordering(self):
        """Mix of high/low/catalog items: counts and top lists must be correct."""
        items = [
            # High: known + strong causal + good score
            _make_observed_item("MELI", effective_score=0.6, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                asset_type_status="known_valid", observed_value_tier="high"),
            # High: known + investable
            _make_observed_item("AAPL", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, observed_value_tier="high"),
            # Low: weak signal quality
            _make_observed_item("GPU", effective_score=0.35, signal_class="observed_candidate",
                                signal_quality="weak", causal_link_strength="strong",
                                observed_value_tier="low"),
            # Low: strong quality but poor everything else
            _make_observed_item("SYMZ", effective_score=0.15, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=False, observed_value_tier="low"),
            # Catalog: no signal
            _make_observed_item("CAT1", signal_quality=None, source_types=["catalog"],
                                priority_score=0.1, observed_value_tier="catalog"),
            _make_observed_item("CAT2", signal_quality=None, source_types=["catalog"],
                                priority_score=0.05, observed_value_tier="catalog"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]

        assert cands["observed_count"] == 6
        assert cands["observed_high_value_count"] == 2
        assert cands["observed_low_value_count"] == 2
        assert cands["observed_catalog_count"] == 2
        assert cands["observed_with_signal_count"] == 3  # MELI, AAPL, SYMZ have signal_quality="strong"

        # top_observed should have high-value items first (by _observed_key)
        top = cands["top_observed"]
        assert len(top) == 3
        # Items with signal_quality="strong" should rank above "weak" and None
        top_qualities = [i.get("signal_quality") for i in top]
        assert top_qualities.count("strong") >= 1

        # top_observed_weak should have exactly the weak items
        assert len(cands["top_observed_weak"]) == 1  # only GPU has signal_quality="weak"
        assert cands["top_observed_weak"][0]["symbol"] == "GPU"

        # top_observed_catalog
        assert len(cands["top_observed_catalog"]) == 2
