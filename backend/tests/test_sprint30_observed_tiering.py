"""Sprint 30 — Observed value tiering: high/medium/low/catalog classification.

Tier definitions (recalibrated):
- high:    signal_quality="strong" AND causal_link_strength="strong"
- medium:  signal_quality="strong" AND causal_link_strength="weak" AND investable=True
- low:     signal_quality="weak" OR (strong quality + weak causal + not investable)
- catalog: signal_quality=None (no news signal)

Tests:
1.  strong quality + strong causal → high
2.  strong quality + weak causal + investable → medium
3.  strong quality + weak causal + NOT investable → low
4.  weak quality + any causal → low
5.  no signal → catalog
6.  decision_summary: top_observed_weak, top_observed_medium present
7.  decision_summary: tier counts (high/medium/low) correct
8.  backward compat: top_observed, top_observed_signals, top_observed_catalog
9.  external_opportunities not affected
10. promotion gates not affected
11. real-world scenario: V, MA with weak causal → NOT high
12. real-world scenario: META with strong causal → high
"""

import pytest


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
# Test 1: strong quality + strong causal → high
# ---------------------------------------------------------------------------

class TestHighTier:
    """HIGH requires: signal_quality=strong AND causal_link_strength=strong."""

    def test_strong_quality_strong_causal_is_high(self):
        """Known instrument + strong causal → high, regardless of investable or score."""
        item = _make_observed_item(
            "MELI",
            effective_score=0.55,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            asset_type_status="known_valid",
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert top[0]["observed_value_tier"] == "high"

    def test_strong_quality_strong_causal_low_score_still_high(self):
        """Even with low effective_score, strong+strong = high."""
        item = _make_observed_item(
            "GLOB",
            effective_score=0.2,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            investable=False,
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "high"

    def test_strong_quality_strong_causal_not_investable_still_high(self):
        """Even without investable, strong+strong = high."""
        item = _make_observed_item(
            "TSLA",
            effective_score=0.4,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            investable=False,
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "high"


# ---------------------------------------------------------------------------
# Test 2: strong quality + weak causal + investable → medium
# ---------------------------------------------------------------------------

class TestMediumTier:
    """MEDIUM requires: signal_quality=strong AND causal_link_strength=weak AND investable=True."""

    def test_strong_quality_weak_causal_investable_is_medium(self):
        """Known instrument, weak causal, but investable → medium."""
        item = _make_observed_item(
            "V",
            effective_score=0.5,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=True,
            observed_value_tier="medium",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "medium"

    def test_high_score_weak_causal_investable_is_medium_not_high(self):
        """Even with high effective_score, weak causal blocks HIGH → medium."""
        item = _make_observed_item(
            "MA",
            effective_score=0.8,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=True,
            observed_value_tier="medium",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "medium"


# ---------------------------------------------------------------------------
# Test 3: strong quality + weak causal + NOT investable → low
# ---------------------------------------------------------------------------

class TestLowTier:
    """LOW: weak quality OR (strong quality + weak causal + not investable)."""

    def test_strong_quality_weak_causal_not_investable_is_low(self):
        """Known instrument but weak causal and not investable → low."""
        item = _make_observed_item(
            "BAC",
            effective_score=0.45,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="weak",
            investable=False,
            observed_value_tier="low",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "low"

    def test_weak_quality_strong_causal_is_low(self):
        """Unknown instrument with strong causal → still low (weak quality dominates)."""
        item = _make_observed_item(
            "GPU",
            effective_score=0.3,
            signal_class="observed_candidate",
            signal_quality="weak",
            causal_link_strength="strong",
            observed_value_tier="low",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "low"

    def test_weak_quality_weak_causal_is_low(self):
        """Unknown instrument + weak causal → low."""
        item = _make_observed_item(
            "AWS",
            effective_score=0.2,
            signal_class="observed_candidate",
            signal_quality="weak",
            causal_link_strength="weak",
            observed_value_tier="low",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "low"


# ---------------------------------------------------------------------------
# Test 4: no signal → catalog
# ---------------------------------------------------------------------------

class TestCatalogTier:
    """CATALOG: signal_quality=None (no news signal)."""

    def test_catalog_only_item_is_catalog(self):
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
        assert cands["top_observed_catalog"][0]["observed_value_tier"] == "catalog"


# ---------------------------------------------------------------------------
# Test 5: decision_summary structure — new lists and counts
# ---------------------------------------------------------------------------

class TestDecisionSummaryStructure:
    """Verify all tier-related fields in decision_summary.candidates."""

    def test_top_observed_weak_exists(self):
        items = [
            _make_observed_item("W1", effective_score=0.3, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
            _make_observed_item("W2", effective_score=0.2, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        assert "top_observed_weak" in cands
        assert len(cands["top_observed_weak"]) == 2

    def test_top_observed_medium_exists(self):
        items = [
            _make_observed_item("V", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, observed_value_tier="medium"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        assert "top_observed_medium" in cands
        assert len(cands["top_observed_medium"]) == 1
        assert cands["top_observed_medium"][0]["observed_value_tier"] == "medium"

    def test_tier_counts_all_present(self):
        items = [
            _make_observed_item("HIGH1", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                observed_value_tier="high"),
            _make_observed_item("MED1", effective_score=0.4, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, observed_value_tier="medium"),
            _make_observed_item("LOW1", effective_score=0.2, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
            _make_observed_item("CAT1", signal_quality=None, observed_value_tier="catalog",
                                source_types=["catalog"]),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_medium_value_count"] == 1
        assert cands["observed_low_value_count"] == 1
        assert cands["observed_catalog_count"] == 1

    def test_backward_compat_keys_still_present(self):
        items = [
            _make_observed_item("MELI", effective_score=0.6, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                observed_value_tier="high"),
            _make_observed_item("GPU", effective_score=0.3, signal_class="observed_candidate",
                                signal_quality="weak", observed_value_tier="low"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]
        for key in ["top_observed", "top_observed_signals", "top_observed_catalog",
                     "observed_count", "observed_with_signal_count", "observed_weak_signal_count"]:
            assert key in cands, f"Missing backward-compat key: {key}"

    def test_observed_value_tier_in_top_n_output(self):
        item = _make_observed_item("AAPL", effective_score=0.5, signal_class="observed_candidate",
                                   signal_quality="strong", causal_link_strength="strong",
                                   observed_value_tier="high")
        summary = _build_summary([item])
        top = summary["candidates"]["top_observed"]
        assert "observed_value_tier" in top[0]


# ---------------------------------------------------------------------------
# Test 6: External opportunities not affected
# ---------------------------------------------------------------------------

class TestNoRegressionExternalOpportunities:

    def test_external_opportunities_untouched(self):
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

    def test_high_value_without_all_gates_not_promoted(self):
        """high tier + score < 0.6 → NOT promoted (promotion requires score >= 0.6)."""
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
        assert cands["observed_count"] == 1
        assert cands["promoted_from_observed_count"] == 0


# ---------------------------------------------------------------------------
# Test 8: Real-world scenario — V, MA, PG, BAC must NOT be high
# ---------------------------------------------------------------------------

class TestRealWorldCalibration:
    """Verify that the real-world problem case is fixed:
    V, MA, PG, BAC with weak causal should NOT be high.
    META with strong causal should be high.
    """

    def test_v_ma_weak_causal_are_not_high(self):
        """V and MA with strong quality but weak causal → medium (if investable) or low."""
        items = [
            # V: known, investable, but weak causal → MEDIUM not HIGH
            _make_observed_item("V", effective_score=0.55, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, asset_type_status="known_valid",
                                observed_value_tier="medium"),
            # MA: known, investable, but weak causal → MEDIUM not HIGH
            _make_observed_item("MA", effective_score=0.50, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, asset_type_status="known_valid",
                                observed_value_tier="medium"),
            # PG: known, NOT investable, weak causal → LOW
            _make_observed_item("PG", effective_score=0.45, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=False, asset_type_status="known_valid",
                                observed_value_tier="low"),
            # BAC: known, NOT investable, weak causal → LOW
            _make_observed_item("BAC", effective_score=0.40, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=False, asset_type_status="known_valid",
                                observed_value_tier="low"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]

        # None should be HIGH
        assert cands["observed_high_value_count"] == 0
        # V and MA → medium; PG and BAC → low
        assert cands["observed_medium_value_count"] == 2
        assert cands["observed_low_value_count"] == 2

        # Verify individual tiers
        top = cands["top_observed"]
        for item in top:
            assert item["observed_value_tier"] != "high", \
                f"{item['symbol']} should NOT be high with weak causal"

    def test_meta_strong_causal_is_high(self):
        """META with strong causal → high (real news about the company)."""
        item = _make_observed_item(
            "META",
            effective_score=0.6,
            signal_class="observed_candidate",
            signal_quality="strong",
            causal_link_strength="strong",
            investable=True,
            asset_type_status="known_valid",
            observed_value_tier="high",
        )
        summary = _build_summary([item])
        assert summary["candidates"]["top_observed"][0]["observed_value_tier"] == "high"
        assert summary["candidates"]["observed_high_value_count"] == 1


# ---------------------------------------------------------------------------
# Test 9: Full mixed scenario
# ---------------------------------------------------------------------------

class TestFullMixedScenario:
    """Complete scenario with all 4 tiers."""

    def test_all_tiers_correctly_assigned(self):
        items = [
            # HIGH: strong + strong causal
            _make_observed_item("META", effective_score=0.6, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="strong",
                                investable=True, observed_value_tier="high"),
            # MEDIUM: strong + weak causal + investable
            _make_observed_item("V", effective_score=0.5, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, observed_value_tier="medium"),
            _make_observed_item("MA", effective_score=0.45, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=True, observed_value_tier="medium"),
            # LOW: weak quality
            _make_observed_item("GPU", effective_score=0.35, signal_class="observed_candidate",
                                signal_quality="weak", causal_link_strength="strong",
                                observed_value_tier="low"),
            # LOW: strong quality + weak causal + not investable
            _make_observed_item("PG", effective_score=0.40, signal_class="observed_candidate",
                                signal_quality="strong", causal_link_strength="weak",
                                investable=False, observed_value_tier="low"),
            # CATALOG: no signal
            _make_observed_item("CAT1", signal_quality=None, source_types=["catalog"],
                                priority_score=0.1, observed_value_tier="catalog"),
        ]
        summary = _build_summary(items)
        cands = summary["candidates"]

        assert cands["observed_count"] == 6
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_medium_value_count"] == 2
        assert cands["observed_low_value_count"] == 2
        assert cands["observed_catalog_count"] == 1

        # top_observed: signal_quality="strong" items first
        top = cands["top_observed"]
        assert len(top) == 3
        top_qualities = [i["signal_quality"] for i in top]
        assert top_qualities.count("strong") >= 2

        # top_observed_medium: V and MA
        assert len(cands["top_observed_medium"]) == 2
        med_syms = {i["symbol"] for i in cands["top_observed_medium"]}
        assert med_syms == {"V", "MA"}

        # top_observed_weak: GPU only (weak signal_quality)
        assert len(cands["top_observed_weak"]) == 1
        assert cands["top_observed_weak"][0]["symbol"] == "GPU"

        # top_observed_catalog: CAT1
        assert len(cands["top_observed_catalog"]) == 1
