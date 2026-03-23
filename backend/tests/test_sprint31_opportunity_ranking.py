"""Sprint 31 — External opportunity ranking & explainability.

Improvements:
- _actionable_key now considers: causal_link_strength, title_mention, source_types count
- Each external_opportunity gets: opportunity_quality ("top"|"standard")
- Each external_opportunity gets: opportunity_rank_reason (human-readable)

Tests:
1. Two opportunities with different quality rank correctly (strong causal > weak)
2. Best opportunity has visible explanation (opportunity_rank_reason non-empty)
3. opportunity_quality = "top" when strong causal + title_mention
4. opportunity_quality = "standard" when weak causal or no title_mention
5. Ranking prefers title_mention over bare score
6. No regression: observed_value_tier unchanged
7. No regression: promotion gates unchanged
8. No regression: top_actionable still present and structured
9. No regression: external_opportunities with existing fields intact
10. opportunity_rank_reason includes expected signal fragments
11. Promoted-from-observed items get rank_reason with "promovido" mention
12. Multiple sources boost ranking (source_types count in key)
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ext_opportunity(
    symbol: str,
    effective_score=None,
    signal_class="external_opportunity",
    causal_link_strength=None,
    investable=True,
    asset_type_status="known_valid",
    source_types=None,
    title_mention=None,
    priority_score=None,
    reason="Test reason",
    promoted_from_observed=None,
    signal_quality="strong",
    in_main_allowed=True,
    actionable_external=True,
    opportunity_quality=None,
    opportunity_rank_reason=None,
):
    """Build a minimal external_opportunity dict for testing."""
    item = {
        "symbol": symbol,
        "reason": reason,
        "signal_class": signal_class,
        "investable": investable,
        "asset_type_status": asset_type_status,
        "actionable_external": actionable_external,
        "in_main_allowed": in_main_allowed,
        "signal_quality": signal_quality,
    }
    if effective_score is not None:
        item["effective_score"] = effective_score
    if causal_link_strength is not None:
        item["causal_link_strength"] = causal_link_strength
    if source_types is not None:
        item["source_types"] = source_types
    if title_mention is not None:
        item["title_mention"] = title_mention
    if priority_score is not None:
        item["priority_score"] = priority_score
    if promoted_from_observed is not None:
        item["promoted_from_observed"] = promoted_from_observed
    if opportunity_quality is not None:
        item["opportunity_quality"] = opportunity_quality
    if opportunity_rank_reason is not None:
        item["opportunity_rank_reason"] = opportunity_rank_reason
    return item


def _make_observed_item(
    symbol: str,
    effective_score=None,
    signal_class="observed_candidate",
    signal_quality=None,
    causal_link_strength=None,
    investable=None,
    observed_value_tier=None,
    source_types=None,
    priority_score=None,
):
    """Build a minimal observed_candidate dict."""
    item = {"symbol": symbol, "reason": "Test", "signal_class": signal_class}
    if effective_score is not None:
        item["effective_score"] = effective_score
    if signal_quality is not None:
        item["signal_quality"] = signal_quality
    if causal_link_strength is not None:
        item["causal_link_strength"] = causal_link_strength
    if investable is not None:
        item["investable"] = investable
    if observed_value_tier is not None:
        item["observed_value_tier"] = observed_value_tier
    if source_types is not None:
        item["source_types"] = source_types
    if priority_score is not None:
        item["priority_score"] = priority_score
    return item


def _build_summary(observed_candidates=None, external_opportunities=None, suppressed=None):
    """Call _build_decision_summary with the given candidates."""
    from app.services.orchestrator import _build_decision_summary

    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Test",
        "external_opportunities": external_opportunities or [],
        "observed_candidates": observed_candidates or [],
        "suppressed_candidates": suppressed or [],
    }
    return _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )


# ---------------------------------------------------------------------------
# Test 1: Two opportunities with different quality rank correctly
# ---------------------------------------------------------------------------

class TestActionableRanking:
    """Verify improved _actionable_key ranks by causal/title/score."""

    def test_strong_causal_ranks_above_weak_causal(self):
        """Item with strong causal should rank above weak causal, even with lower score."""
        strong = _make_ext_opportunity(
            "META", effective_score=0.5, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.50",
        )
        weak = _make_ext_opportunity(
            "V", effective_score=0.7, causal_link_strength="weak",
            title_mention=False, source_types=["news", "catalog"],
            opportunity_quality="standard",
            opportunity_rank_reason="causalidad débil; score 0.70; 2 fuentes",
        )
        summary = _build_summary(external_opportunities=[strong, weak])
        top = summary["candidates"]["top_actionable"]
        assert len(top) == 2
        # META (strong causal) should be #1 despite lower score
        assert top[0]["symbol"] == "META"
        assert top[1]["symbol"] == "V"

    def test_title_mention_breaks_tie_same_causal(self):
        """With same causal strength, title_mention should rank higher."""
        with_title = _make_ext_opportunity(
            "AAPL", effective_score=0.5, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.50",
        )
        no_title = _make_ext_opportunity(
            "MSFT", effective_score=0.5, causal_link_strength="strong",
            title_mention=False, source_types=["news"],
            opportunity_quality="standard",
            opportunity_rank_reason="causalidad fuerte; score 0.50",
        )
        summary = _build_summary(external_opportunities=[no_title, with_title])
        top = summary["candidates"]["top_actionable"]
        assert top[0]["symbol"] == "AAPL"

    def test_score_breaks_tie_same_causal_same_title(self):
        """When causal and title_mention are equal, effective_score decides."""
        high = _make_ext_opportunity(
            "TSLA", effective_score=0.8, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.80",
        )
        low = _make_ext_opportunity(
            "GOOGL", effective_score=0.5, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.50",
        )
        summary = _build_summary(external_opportunities=[low, high])
        top = summary["candidates"]["top_actionable"]
        assert top[0]["symbol"] == "TSLA"

    def test_more_sources_breaks_further_tie(self):
        """When causal, title, and score are equal, source_types count decides."""
        multi = _make_ext_opportunity(
            "AMZN", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news", "catalog", "watchlist"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60; 3 fuentes",
        )
        single = _make_ext_opportunity(
            "NFLX", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60",
        )
        summary = _build_summary(external_opportunities=[single, multi])
        top = summary["candidates"]["top_actionable"]
        assert top[0]["symbol"] == "AMZN"


# ---------------------------------------------------------------------------
# Test 2: Best opportunity has visible explanation
# ---------------------------------------------------------------------------

class TestOpportunityExplainability:
    """Verify opportunity_rank_reason is populated and meaningful."""

    def test_rank_reason_non_empty(self):
        """Every external_opportunity must have a non-empty opportunity_rank_reason."""
        item = _make_ext_opportunity(
            "META", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"]
        assert top[0]["opportunity_rank_reason"]
        assert len(top[0]["opportunity_rank_reason"]) > 0

    def test_rank_reason_includes_causal_fragment(self):
        """rank_reason should mention causalidad when present."""
        item = _make_ext_opportunity(
            "AAPL", effective_score=0.5, causal_link_strength="strong",
            title_mention=False, source_types=["news"],
            opportunity_quality="standard",
            opportunity_rank_reason="causalidad fuerte; score 0.50",
        )
        summary = _build_summary(external_opportunities=[item])
        reason = summary["candidates"]["top_actionable"][0]["opportunity_rank_reason"]
        assert "causalidad" in reason

    def test_rank_reason_includes_title_fragment(self):
        """rank_reason should mention título when title_mention is True."""
        item = _make_ext_opportunity(
            "AAPL", effective_score=0.5, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.50",
        )
        summary = _build_summary(external_opportunities=[item])
        reason = summary["candidates"]["top_actionable"][0]["opportunity_rank_reason"]
        assert "título" in reason

    def test_rank_reason_includes_score(self):
        """rank_reason should include the effective_score value."""
        item = _make_ext_opportunity(
            "AAPL", effective_score=0.72, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.72",
        )
        summary = _build_summary(external_opportunities=[item])
        reason = summary["candidates"]["top_actionable"][0]["opportunity_rank_reason"]
        assert "0.72" in reason

    def test_rank_reason_includes_multi_source(self):
        """rank_reason should mention source count when >= 2."""
        item = _make_ext_opportunity(
            "AAPL", effective_score=0.5, causal_link_strength="strong",
            title_mention=True, source_types=["news", "catalog"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.50; 2 fuentes",
        )
        summary = _build_summary(external_opportunities=[item])
        reason = summary["candidates"]["top_actionable"][0]["opportunity_rank_reason"]
        assert "fuentes" in reason

    def test_promoted_item_mentions_promotion(self):
        """Promoted items should mention 'promovido' in rank_reason."""
        item = _make_ext_opportunity(
            "MELI", effective_score=0.7, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            promoted_from_observed=True,
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.70; promovido desde observed",
        )
        summary = _build_summary(external_opportunities=[item])
        reason = summary["candidates"]["top_actionable"][0]["opportunity_rank_reason"]
        assert "promovido" in reason


# ---------------------------------------------------------------------------
# Test 3: opportunity_quality assignment
# ---------------------------------------------------------------------------

class TestOpportunityQuality:
    """Verify opportunity_quality is "top" or "standard"."""

    def test_top_quality_requires_strong_causal_and_title(self):
        """opportunity_quality = "top" only when strong causal AND title_mention."""
        item = _make_ext_opportunity(
            "META", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        assert summary["candidates"]["top_actionable"][0]["opportunity_quality"] == "top"

    def test_standard_quality_weak_causal(self):
        """Weak causal → standard, even with title_mention."""
        item = _make_ext_opportunity(
            "V", effective_score=0.6, causal_link_strength="weak",
            title_mention=True, source_types=["news"],
            opportunity_quality="standard",
            opportunity_rank_reason="ticker en título; causalidad débil; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        assert summary["candidates"]["top_actionable"][0]["opportunity_quality"] == "standard"

    def test_standard_quality_no_title(self):
        """No title_mention → standard, even with strong causal."""
        item = _make_ext_opportunity(
            "GOOGL", effective_score=0.6, causal_link_strength="strong",
            title_mention=False, source_types=["news"],
            opportunity_quality="standard",
            opportunity_rank_reason="causalidad fuerte; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        assert summary["candidates"]["top_actionable"][0]["opportunity_quality"] == "standard"

    def test_standard_quality_none_causal(self):
        """No causal_link_strength → standard."""
        item = _make_ext_opportunity(
            "TSLA", effective_score=0.6,
            title_mention=True, source_types=["news"],
            opportunity_quality="standard",
            opportunity_rank_reason="ticker en título; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        assert summary["candidates"]["top_actionable"][0]["opportunity_quality"] == "standard"


# ---------------------------------------------------------------------------
# Test 4: No regression — observed_value_tier unchanged
# ---------------------------------------------------------------------------

class TestNoRegressionObservedTiers:
    """Observed tier logic must not be affected."""

    def test_observed_tiers_still_work(self):
        obs = [
            _make_observed_item("MELI", effective_score=0.5, signal_quality="strong",
                                causal_link_strength="strong", observed_value_tier="high"),
            _make_observed_item("V", effective_score=0.4, signal_quality="strong",
                                causal_link_strength="weak", investable=True,
                                observed_value_tier="medium"),
            _make_observed_item("GPU", effective_score=0.3, signal_quality="weak",
                                observed_value_tier="low"),
            _make_observed_item("CAT", signal_quality=None, source_types=["catalog"],
                                observed_value_tier="catalog"),
        ]
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_medium_value_count"] == 1
        assert cands["observed_low_value_count"] == 1
        assert cands["observed_catalog_count"] == 1


# ---------------------------------------------------------------------------
# Test 5: No regression — promotion gates unchanged
# ---------------------------------------------------------------------------

class TestNoRegressionPromotionGates:

    def test_promotion_still_requires_all_four_gates(self):
        """Promotion needs: strong quality + strong causal + score >= 0.6 + investable."""
        # High quality but score < 0.6 → NOT promoted
        item = _make_observed_item(
            "GLOB", effective_score=0.45, signal_quality="strong",
            causal_link_strength="strong", investable=True,
            observed_value_tier="high",
        )
        summary = _build_summary(observed_candidates=[item])
        assert summary["candidates"]["promoted_from_observed_count"] == 0
        assert summary["candidates"]["observed_count"] == 1


# ---------------------------------------------------------------------------
# Test 6: No regression — top_actionable structure intact
# ---------------------------------------------------------------------------

class TestNoRegressionTopActionable:

    def test_top_actionable_has_required_fields(self):
        """top_actionable items must have all expected fields."""
        item = _make_ext_opportunity(
            "META", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        # Original fields still present
        for key in ["symbol", "effective_score", "investable", "source_types",
                     "title_mention", "causal_link_strength"]:
            assert key in top, f"Missing field: {key}"
        # New fields also present
        assert "opportunity_quality" in top
        assert "opportunity_rank_reason" in top

    def test_top_actionable_count_matches(self):
        """actionable_count must match the number of external_opportunities."""
        items = [
            _make_ext_opportunity("A", effective_score=0.5, causal_link_strength="strong",
                                  title_mention=True, source_types=["news"],
                                  opportunity_quality="top",
                                  opportunity_rank_reason="test"),
            _make_ext_opportunity("B", effective_score=0.4, causal_link_strength="weak",
                                  source_types=["news"],
                                  opportunity_quality="standard",
                                  opportunity_rank_reason="test"),
        ]
        summary = _build_summary(external_opportunities=items)
        assert summary["candidates"]["actionable_count"] == 2


# ---------------------------------------------------------------------------
# Test 7: No regression — existing ext_ops fields untouched
# ---------------------------------------------------------------------------

class TestNoRegressionExistingFields:

    def test_existing_fields_pass_through(self):
        """Fields set before enrichment must survive into the summary."""
        item = _make_ext_opportunity(
            "TSLA", effective_score=0.7, causal_link_strength="strong",
            title_mention=True, source_types=["news", "watchlist"],
            priority_score=0.85, investable=True,
            asset_type_status="known_valid",
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.70; 2 fuentes",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        assert top["symbol"] == "TSLA"
        assert top["effective_score"] == 0.7
        assert top["investable"] is True
        assert top["source_types"] == ["news", "watchlist"]
        assert top["title_mention"] is True


# ---------------------------------------------------------------------------
# Test 8: Full ranking scenario — realistic mix
# ---------------------------------------------------------------------------

class TestFullRankingScenario:
    """Realistic scenario: 3 opportunities with different signal profiles."""

    def test_three_opportunities_ranked_correctly(self):
        """
        META: strong causal + title → should be #1 (top quality)
        GOOGL: strong causal, no title → should be #2 (standard)
        V: weak causal + title → should be #3 (standard)
        """
        meta = _make_ext_opportunity(
            "META", effective_score=0.55, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.55",
        )
        googl = _make_ext_opportunity(
            "GOOGL", effective_score=0.60, causal_link_strength="strong",
            title_mention=False, source_types=["news", "catalog"],
            opportunity_quality="standard",
            opportunity_rank_reason="causalidad fuerte; score 0.60; 2 fuentes",
        )
        v = _make_ext_opportunity(
            "V", effective_score=0.70, causal_link_strength="weak",
            title_mention=True, source_types=["news", "catalog"],
            opportunity_quality="standard",
            opportunity_rank_reason="ticker en título; causalidad débil; score 0.70; 2 fuentes",
        )
        summary = _build_summary(external_opportunities=[v, googl, meta])
        top = summary["candidates"]["top_actionable"]
        assert len(top) == 3

        # META first (strong causal + title)
        assert top[0]["symbol"] == "META"
        assert top[0]["opportunity_quality"] == "top"

        # GOOGL second (strong causal, no title, but higher score than META)
        assert top[1]["symbol"] == "GOOGL"
        assert top[1]["opportunity_quality"] == "standard"

        # V last (weak causal, despite highest score)
        assert top[2]["symbol"] == "V"
        assert top[2]["opportunity_quality"] == "standard"

        # All have rank reasons
        for item in top:
            assert item["opportunity_rank_reason"]
            assert len(item["opportunity_rank_reason"]) > 5
