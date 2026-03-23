"""Sprint 33 — Operational status: actionable vs relevant_not_investable.

Separates two distinct product concepts:
- actionable: operable today (external_opportunities, investable)
- relevant_not_investable: strong signal + causal but can't operate (not in main_allowed)

Tests:
1.  Strong+causal+investable → stays in actionable (ext_ops), operational_status="actionable"
2.  Strong+causal+NOT investable → observed, operational_status="relevant_not_investable"
3.  Strong+weak causal+NOT investable → observed, NO operational_status set
4.  Weak quality → observed, NO operational_status set
5.  decision_summary: relevant_non_investable_count correct
6.  decision_summary: top_relevant_non_investable populated
7.  top_relevant_non_investable items have operational_status in output
8.  top_actionable items have operational_status="actionable"
9.  No regression: observed_value_tier unchanged
10. No regression: promotion gates unchanged
11. No regression: opportunity_quality still works
12. No regression: market_confirmation_reason still works
13. No regression: existing summary keys still present
14. Full mixed scenario: correct separation
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
    signal_quality="strong",
    in_main_allowed=True,
    actionable_external=True,
    market_confirmation=None,
    tracking_status=None,
    opportunity_quality=None,
    opportunity_rank_reason=None,
    market_confirmation_reason=None,
    operational_status=None,
    promoted_from_observed=None,
):
    item = {
        "symbol": symbol, "reason": reason, "signal_class": signal_class,
        "investable": investable, "asset_type_status": asset_type_status,
        "actionable_external": actionable_external, "in_main_allowed": in_main_allowed,
        "signal_quality": signal_quality,
    }
    if effective_score is not None: item["effective_score"] = effective_score
    if causal_link_strength is not None: item["causal_link_strength"] = causal_link_strength
    if source_types is not None: item["source_types"] = source_types
    if title_mention is not None: item["title_mention"] = title_mention
    if priority_score is not None: item["priority_score"] = priority_score
    if market_confirmation is not None: item["market_confirmation"] = market_confirmation
    if tracking_status is not None: item["tracking_status"] = tracking_status
    if opportunity_quality is not None: item["opportunity_quality"] = opportunity_quality
    if opportunity_rank_reason is not None: item["opportunity_rank_reason"] = opportunity_rank_reason
    if market_confirmation_reason is not None: item["market_confirmation_reason"] = market_confirmation_reason
    if operational_status is not None: item["operational_status"] = operational_status
    if promoted_from_observed is not None: item["promoted_from_observed"] = promoted_from_observed
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
    operational_status=None,
    asset_type_status=None,
):
    item = {"symbol": symbol, "reason": "Test", "signal_class": signal_class}
    if effective_score is not None: item["effective_score"] = effective_score
    if signal_quality is not None: item["signal_quality"] = signal_quality
    if causal_link_strength is not None: item["causal_link_strength"] = causal_link_strength
    if investable is not None: item["investable"] = investable
    if observed_value_tier is not None: item["observed_value_tier"] = observed_value_tier
    if source_types is not None: item["source_types"] = source_types
    if priority_score is not None: item["priority_score"] = priority_score
    if operational_status is not None: item["operational_status"] = operational_status
    if asset_type_status is not None: item["asset_type_status"] = asset_type_status
    return item


def _build_summary(observed_candidates=None, external_opportunities=None, suppressed=None):
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
# Test 1: Actionable items get operational_status="actionable"
# ---------------------------------------------------------------------------

class TestActionableStatus:

    def test_ext_op_has_actionable_status(self):
        """External opportunities must have operational_status='actionable' in top_n."""
        item = _make_ext_opportunity(
            "GOOGL", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news", "catalog"],
            operational_status="actionable",
            opportunity_quality="top",
            opportunity_rank_reason="test",
            market_confirmation_reason="test",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"]
        assert top[0]["operational_status"] == "actionable"

    def test_multiple_ext_ops_all_actionable(self):
        items = [
            _make_ext_opportunity("A", effective_score=0.5, operational_status="actionable",
                                  opportunity_quality="top", opportunity_rank_reason="t",
                                  market_confirmation_reason="t"),
            _make_ext_opportunity("B", effective_score=0.4, operational_status="actionable",
                                  opportunity_quality="standard", opportunity_rank_reason="t",
                                  market_confirmation_reason="t"),
        ]
        summary = _build_summary(external_opportunities=items)
        for item in summary["candidates"]["top_actionable"]:
            assert item["operational_status"] == "actionable"


# ---------------------------------------------------------------------------
# Test 2: Strong+causal+NOT investable → relevant_not_investable
# ---------------------------------------------------------------------------

class TestRelevantNonInvestable:

    def test_strong_causal_not_investable_gets_status(self):
        """Strong signal + strong causal + NOT investable → relevant_not_investable."""
        item = _make_observed_item(
            "META", effective_score=0.55,
            signal_quality="strong", causal_link_strength="strong",
            investable=False, observed_value_tier="high",
            operational_status="relevant_not_investable",
        )
        summary = _build_summary(observed_candidates=[item])
        cands = summary["candidates"]
        assert cands["relevant_non_investable_count"] == 1
        top = cands["top_relevant_non_investable"]
        assert len(top) == 1
        assert top[0]["symbol"] == "META"
        assert top[0]["operational_status"] == "relevant_not_investable"

    def test_strong_causal_investable_none_gets_status(self):
        """investable not set (None, not True) + strong+causal → relevant_not_investable."""
        item = _make_observed_item(
            "TSLA", effective_score=0.5,
            signal_quality="strong", causal_link_strength="strong",
            observed_value_tier="high",
            operational_status="relevant_not_investable",
        )
        summary = _build_summary(observed_candidates=[item])
        assert summary["candidates"]["relevant_non_investable_count"] == 1


# ---------------------------------------------------------------------------
# Test 3: Other observed items do NOT get operational_status
# ---------------------------------------------------------------------------

class TestNoStatusForOtherObserved:

    def test_strong_weak_causal_no_status(self):
        """Strong quality + weak causal → no operational_status."""
        item = _make_observed_item(
            "V", effective_score=0.5,
            signal_quality="strong", causal_link_strength="weak",
            investable=True, observed_value_tier="medium",
        )
        summary = _build_summary(observed_candidates=[item])
        cands = summary["candidates"]
        assert cands["relevant_non_investable_count"] == 0
        # operational_status should not be set
        top = cands["top_observed"]
        assert top[0].get("operational_status") is None

    def test_weak_quality_no_status(self):
        """Weak quality → no operational_status."""
        item = _make_observed_item(
            "GPU", effective_score=0.3,
            signal_quality="weak", causal_link_strength="strong",
            observed_value_tier="low",
        )
        summary = _build_summary(observed_candidates=[item])
        assert summary["candidates"]["relevant_non_investable_count"] == 0

    def test_catalog_no_status(self):
        """Catalog item → no operational_status."""
        item = _make_observed_item(
            "CAT1", signal_quality=None,
            observed_value_tier="catalog", source_types=["catalog"],
        )
        summary = _build_summary(observed_candidates=[item])
        assert summary["candidates"]["relevant_non_investable_count"] == 0


# ---------------------------------------------------------------------------
# Test 4: Summary structure
# ---------------------------------------------------------------------------

class TestSummaryStructure:

    def test_relevant_non_investable_count_in_summary(self):
        items = [
            _make_observed_item("META", effective_score=0.6, signal_quality="strong",
                                causal_link_strength="strong", investable=False,
                                observed_value_tier="high",
                                operational_status="relevant_not_investable"),
            _make_observed_item("TSLA", effective_score=0.5, signal_quality="strong",
                                causal_link_strength="strong",
                                observed_value_tier="high",
                                operational_status="relevant_not_investable"),
        ]
        summary = _build_summary(observed_candidates=items)
        cands = summary["candidates"]
        assert cands["relevant_non_investable_count"] == 2
        assert len(cands["top_relevant_non_investable"]) == 2

    def test_top_relevant_non_investable_key_exists(self):
        """Key must exist even when empty."""
        summary = _build_summary(observed_candidates=[])
        cands = summary["candidates"]
        assert "relevant_non_investable_count" in cands
        assert "top_relevant_non_investable" in cands
        assert cands["relevant_non_investable_count"] == 0
        assert cands["top_relevant_non_investable"] == []

    def test_operational_status_in_top_n_output(self):
        """operational_status must appear in _top_n dicts."""
        item = _make_observed_item(
            "META", effective_score=0.6, signal_quality="strong",
            causal_link_strength="strong", investable=False,
            observed_value_tier="high",
            operational_status="relevant_not_investable",
        )
        summary = _build_summary(observed_candidates=[item])
        top = summary["candidates"]["top_relevant_non_investable"][0]
        assert "operational_status" in top


# ---------------------------------------------------------------------------
# Test 5: No regression — observed tiers unchanged
# ---------------------------------------------------------------------------

class TestNoRegressionObservedTiers:

    def test_observed_tiers_unchanged(self):
        obs = [
            _make_observed_item("MELI", effective_score=0.5, signal_quality="strong",
                                causal_link_strength="strong", observed_value_tier="high",
                                operational_status="relevant_not_investable"),
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
# Test 6: No regression — promotion gates
# ---------------------------------------------------------------------------

class TestNoRegressionPromotionGates:

    def test_promotion_gates_unchanged(self):
        item = _make_observed_item(
            "GLOB", effective_score=0.45, signal_quality="strong",
            causal_link_strength="strong", investable=True,
            observed_value_tier="high",
        )
        summary = _build_summary(observed_candidates=[item])
        assert summary["candidates"]["promoted_from_observed_count"] == 0


# ---------------------------------------------------------------------------
# Test 7: No regression — existing summary keys
# ---------------------------------------------------------------------------

class TestNoRegressionExistingKeys:

    def test_all_existing_keys_present(self):
        ext = _make_ext_opportunity(
            "GOOGL", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            operational_status="actionable",
            opportunity_quality="top", opportunity_rank_reason="t",
            market_confirmation_reason="t",
        )
        obs = _make_observed_item(
            "GPU", effective_score=0.3, signal_quality="weak",
            observed_value_tier="low",
        )
        summary = _build_summary(
            external_opportunities=[ext],
            observed_candidates=[obs],
        )
        cands = summary["candidates"]
        expected_keys = [
            "actionable_count", "investable_count", "promoted_from_observed_count",
            "observed_count", "observed_with_signal_count", "observed_weak_signal_count",
            "observed_catalog_count", "observed_high_value_count", "observed_medium_value_count",
            "observed_low_value_count", "relevant_non_investable_count", "suppressed_count",
            "top_actionable", "top_relevant_non_investable", "top_observed",
            "top_observed_signals", "top_observed_medium", "top_observed_weak",
            "top_observed_catalog", "top_suppressed",
        ]
        for key in expected_keys:
            assert key in cands, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# Test 8: No regression — opportunity_quality + market_confirmation_reason
# ---------------------------------------------------------------------------

class TestNoRegressionSprint31And32:

    def test_opportunity_quality_still_in_top_actionable(self):
        item = _make_ext_opportunity(
            "META", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news"],
            operational_status="actionable",
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60",
            market_confirmation_reason="cotización disponible en mercado",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        assert top["opportunity_quality"] == "top"
        assert top["market_confirmation_reason"] == "cotización disponible en mercado"


# ---------------------------------------------------------------------------
# Test 9: Full mixed scenario
# ---------------------------------------------------------------------------

class TestFullMixedScenario:

    def test_realistic_separation(self):
        """
        GOOGL: actionable (ext_op, investable) → top_actionable
        META: strong+causal but NOT investable → top_relevant_non_investable
        V: strong+weak causal+investable → medium observed (not relevant_non_investable)
        GPU: weak quality → low observed
        CAT1: catalog → catalog observed
        """
        ext = [
            _make_ext_opportunity(
                "GOOGL", effective_score=0.65, causal_link_strength="strong",
                title_mention=True, source_types=["news", "catalog"],
                operational_status="actionable",
                opportunity_quality="top", opportunity_rank_reason="t",
                market_confirmation_reason="t",
            ),
        ]
        obs = [
            _make_observed_item(
                "META", effective_score=0.55, signal_quality="strong",
                causal_link_strength="strong", investable=False,
                observed_value_tier="high",
                operational_status="relevant_not_investable",
            ),
            _make_observed_item(
                "V", effective_score=0.45, signal_quality="strong",
                causal_link_strength="weak", investable=True,
                observed_value_tier="medium",
            ),
            _make_observed_item(
                "GPU", effective_score=0.30, signal_quality="weak",
                observed_value_tier="low",
            ),
            _make_observed_item(
                "CAT1", signal_quality=None,
                observed_value_tier="catalog", source_types=["catalog"],
            ),
        ]
        summary = _build_summary(external_opportunities=ext, observed_candidates=obs)
        cands = summary["candidates"]

        # Actionable
        assert cands["actionable_count"] == 1
        assert cands["top_actionable"][0]["symbol"] == "GOOGL"
        assert cands["top_actionable"][0]["operational_status"] == "actionable"

        # Relevant non-investable
        assert cands["relevant_non_investable_count"] == 1
        assert cands["top_relevant_non_investable"][0]["symbol"] == "META"
        assert cands["top_relevant_non_investable"][0]["operational_status"] == "relevant_not_investable"

        # V is NOT relevant_non_investable (weak causal)
        rni_syms = {i["symbol"] for i in cands["top_relevant_non_investable"]}
        assert "V" not in rni_syms

        # Observed tiers still correct
        assert cands["observed_high_value_count"] == 1  # META
        assert cands["observed_medium_value_count"] == 1  # V
        assert cands["observed_low_value_count"] == 1  # GPU
        assert cands["observed_catalog_count"] == 1  # CAT1

        # Total observed
        assert cands["observed_count"] == 4
