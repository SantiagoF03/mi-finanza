"""Sprint 32 — Market confirmation enrichment for external_opportunities.

Changes:
- "unconfirmed" upgraded to "quote_available" when instrument is known_valid + in catalog/tracked
- market_confirmation_reason added per opportunity (human-readable)
- _top_n exposes market_confirmation_reason

Tests:
1.  ext_op with catalog source + known_valid: unconfirmed → quote_available
2.  ext_op with tracked status + known_valid: unconfirmed → quote_available
3.  ext_op already "confirmed" stays "confirmed"
4.  ext_op already "contradicted" stays "contradicted"
5.  ext_op without catalog/tracking stays "unconfirmed"
6.  market_confirmation_reason populated for each status
7.  market_confirmation_reason mentions catálogo when in catalog
8.  No regression: observed_value_tier unchanged
9.  No regression: promotion gates unchanged
10. No regression: opportunity_quality and rank_reason still work
11. No regression: top_actionable structure intact
12. market_confirmation_reason visible in _top_n output
13. None market_confirmation treated same as unconfirmed
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
    market_confirmation=None,
    tracking_status=None,
    opportunity_quality=None,
    opportunity_rank_reason=None,
    market_confirmation_reason=None,
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
    if market_confirmation is not None:
        item["market_confirmation"] = market_confirmation
    if tracking_status is not None:
        item["tracking_status"] = tracking_status
    if opportunity_quality is not None:
        item["opportunity_quality"] = opportunity_quality
    if opportunity_rank_reason is not None:
        item["opportunity_rank_reason"] = opportunity_rank_reason
    if market_confirmation_reason is not None:
        item["market_confirmation_reason"] = market_confirmation_reason
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


def _enrich(items):
    """Call the real enrichment function."""
    from app.services.orchestrator import _enrich_market_confirmation
    _enrich_market_confirmation(items)
    return items


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
# Test 1: unconfirmed + catalog + known_valid → quote_available
# ---------------------------------------------------------------------------

class TestMarketConfirmationUpgrade:
    """Verify unconfirmed is upgraded to quote_available when evidence exists."""

    def test_catalog_known_valid_upgrades_to_quote_available(self):
        """In catalog + known_valid → quote_available."""
        items = _enrich([_make_ext_opportunity(
            "AAPL", effective_score=0.6,
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "quote_available"

    def test_tracked_known_valid_upgrades_to_quote_available(self):
        """Tracked (non-untracked) + known_valid → quote_available."""
        items = _enrich([_make_ext_opportunity(
            "MELI", effective_score=0.6,
            source_types=["news"],
            asset_type_status="known_valid",
            tracking_status="catalog",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "quote_available"

    def test_none_confirmation_upgrades_to_quote_available(self):
        """None market_confirmation (absent) + catalog → quote_available."""
        items = _enrich([_make_ext_opportunity(
            "TSLA", effective_score=0.5,
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            # market_confirmation not set → None
        )])
        assert items[0]["market_confirmation"] == "quote_available"

    def test_watchlist_tracking_upgrades(self):
        """tracking_status=watchlist + known_valid → quote_available."""
        items = _enrich([_make_ext_opportunity(
            "GLOB", effective_score=0.5,
            source_types=["news", "watchlist"],
            asset_type_status="known_valid",
            tracking_status="watchlist",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "quote_available"


# ---------------------------------------------------------------------------
# Test 2: confirmed/contradicted stay unchanged
# ---------------------------------------------------------------------------

class TestMarketConfirmationPreserved:
    """Confirmed and contradicted must NOT be overwritten."""

    def test_confirmed_stays_confirmed(self):
        items = _enrich([_make_ext_opportunity(
            "META", effective_score=0.7,
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="confirmed",
        )])
        assert items[0]["market_confirmation"] == "confirmed"

    def test_contradicted_stays_contradicted(self):
        items = _enrich([_make_ext_opportunity(
            "GOOGL", effective_score=0.4,
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="contradicted",
        )])
        assert items[0]["market_confirmation"] == "contradicted"


# ---------------------------------------------------------------------------
# Test 3: No catalog/tracking → stays unconfirmed
# ---------------------------------------------------------------------------

class TestMarketConfirmationNotUpgraded:
    """Without catalog or tracking evidence, stays unconfirmed."""

    def test_no_catalog_no_tracking_stays_unconfirmed(self):
        """news-only + known_valid but no catalog/tracking → unconfirmed."""
        items = _enrich([_make_ext_opportunity(
            "NEWCO", effective_score=0.5,
            source_types=["news"],
            asset_type_status="known_valid",
            tracking_status="untracked",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "unconfirmed"

    def test_unknown_asset_type_stays_unconfirmed(self):
        """Even with catalog source, unknown asset → stays unconfirmed."""
        items = _enrich([_make_ext_opportunity(
            "WEIRD", effective_score=0.5,
            source_types=["news", "catalog"],
            asset_type_status="unknown",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "unconfirmed"

    def test_no_source_types_stays_unconfirmed(self):
        """No source_types at all → stays unconfirmed."""
        items = _enrich([_make_ext_opportunity(
            "BARE", effective_score=0.5,
            asset_type_status="known_valid",
            market_confirmation="unconfirmed",
        )])
        assert items[0]["market_confirmation"] == "unconfirmed"


# ---------------------------------------------------------------------------
# Test 4: market_confirmation_reason content
# ---------------------------------------------------------------------------

class TestMarketConfirmationReason:
    """Verify market_confirmation_reason is populated correctly."""

    def test_reason_for_quote_available_with_catalog(self):
        items = _enrich([_make_ext_opportunity(
            "AAPL", effective_score=0.6,
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="unconfirmed",
        )])
        reason = items[0]["market_confirmation_reason"]
        assert "cotización disponible" in reason
        assert "catálogo IOL" in reason

    def test_reason_for_confirmed(self):
        items = _enrich([_make_ext_opportunity(
            "META", effective_score=0.7,
            source_types=["news"],
            market_confirmation="confirmed",
        )])
        reason = items[0]["market_confirmation_reason"]
        assert "confirma" in reason

    def test_reason_for_contradicted(self):
        items = _enrich([_make_ext_opportunity(
            "GOOGL", effective_score=0.4,
            source_types=["news"],
            market_confirmation="contradicted",
        )])
        reason = items[0]["market_confirmation_reason"]
        assert "contradice" in reason

    def test_reason_for_unconfirmed(self):
        items = _enrich([_make_ext_opportunity(
            "NEWCO", effective_score=0.5,
            source_types=["news"],
            asset_type_status="known_valid",
            tracking_status="untracked",
            market_confirmation="unconfirmed",
        )])
        reason = items[0]["market_confirmation_reason"]
        assert "sin datos" in reason

    def test_reason_mentions_tracking_when_tracked(self):
        items = _enrich([_make_ext_opportunity(
            "MELI", effective_score=0.6,
            source_types=["news"],
            asset_type_status="known_valid",
            tracking_status="watchlist",
            market_confirmation="unconfirmed",
        )])
        reason = items[0]["market_confirmation_reason"]
        assert "tracking" in reason


# ---------------------------------------------------------------------------
# Test 5: market_confirmation_reason visible in _top_n
# ---------------------------------------------------------------------------

class TestReasonInTopN:
    """market_confirmation_reason must be exposed via _top_n in decision_summary."""

    def test_reason_in_top_n_output(self):
        item = _make_ext_opportunity(
            "AAPL", effective_score=0.5, causal_link_strength="strong",
            source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="quote_available",
            market_confirmation_reason="cotización disponible en mercado; presente en catálogo IOL",
            opportunity_quality="top",
            opportunity_rank_reason="test",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        assert "market_confirmation_reason" in top
        assert top["market_confirmation_reason"] is not None
        assert "cotización" in top["market_confirmation_reason"]


# ---------------------------------------------------------------------------
# Test 6: No regression — observed tiers
# ---------------------------------------------------------------------------

class TestNoRegressionObservedTiers:

    def test_observed_tiers_unchanged(self):
        obs = [
            _make_observed_item("MELI", effective_score=0.5, signal_quality="strong",
                                causal_link_strength="strong", observed_value_tier="high"),
            _make_observed_item("V", effective_score=0.4, signal_quality="strong",
                                causal_link_strength="weak", investable=True,
                                observed_value_tier="medium"),
            _make_observed_item("GPU", effective_score=0.3, signal_quality="weak",
                                observed_value_tier="low"),
        ]
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_medium_value_count"] == 1
        assert cands["observed_low_value_count"] == 1


# ---------------------------------------------------------------------------
# Test 7: No regression — promotion gates
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
# Test 8: No regression — opportunity_quality and rank_reason
# ---------------------------------------------------------------------------

class TestNoRegressionOpportunityQuality:

    def test_opportunity_quality_still_works(self):
        item = _make_ext_opportunity(
            "META", effective_score=0.6, causal_link_strength="strong",
            title_mention=True, source_types=["news", "catalog"],
            asset_type_status="known_valid",
            market_confirmation="quote_available",
            market_confirmation_reason="cotización disponible en mercado",
            opportunity_quality="top",
            opportunity_rank_reason="ticker en título; causalidad fuerte; score 0.60; 2 fuentes",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        assert top["opportunity_quality"] == "top"
        assert "causalidad fuerte" in top["opportunity_rank_reason"]


# ---------------------------------------------------------------------------
# Test 9: No regression — top_actionable structure
# ---------------------------------------------------------------------------

class TestNoRegressionTopActionable:

    def test_top_actionable_fields_intact(self):
        item = _make_ext_opportunity(
            "TSLA", effective_score=0.7, causal_link_strength="strong",
            title_mention=True, source_types=["news", "catalog"],
            priority_score=0.85, investable=True,
            asset_type_status="known_valid",
            market_confirmation="confirmed",
            market_confirmation_reason="movimiento de precio confirma el evento",
            opportunity_quality="top",
            opportunity_rank_reason="test",
        )
        summary = _build_summary(external_opportunities=[item])
        top = summary["candidates"]["top_actionable"][0]
        for key in ["symbol", "effective_score", "investable", "source_types",
                     "title_mention", "causal_link_strength", "market_confirmation",
                     "opportunity_quality", "opportunity_rank_reason",
                     "market_confirmation_reason"]:
            assert key in top, f"Missing field: {key}"


# ---------------------------------------------------------------------------
# Test 10: Full scenario — mix of confirmation statuses
# ---------------------------------------------------------------------------

class TestFullMixedScenario:

    def test_mixed_confirmations_correct(self):
        """Realistic mix: confirmed stays, unconfirmed+catalog→quote_available, bare→stays."""
        items = [
            _make_ext_opportunity(
                "META", effective_score=0.7,
                source_types=["news", "catalog"],
                asset_type_status="known_valid",
                market_confirmation="confirmed",
            ),
            _make_ext_opportunity(
                "AAPL", effective_score=0.6,
                source_types=["news", "catalog"],
                asset_type_status="known_valid",
                market_confirmation="unconfirmed",
            ),
            _make_ext_opportunity(
                "NEWCO", effective_score=0.5,
                source_types=["news"],
                asset_type_status="known_valid",
                tracking_status="untracked",
                market_confirmation="unconfirmed",
            ),
        ]
        _enrich(items)

        # META: confirmed stays confirmed
        meta = next(i for i in items if i["symbol"] == "META")
        assert meta["market_confirmation"] == "confirmed"
        assert "confirma" in meta["market_confirmation_reason"]

        # AAPL: unconfirmed + catalog → quote_available
        aapl = next(i for i in items if i["symbol"] == "AAPL")
        assert aapl["market_confirmation"] == "quote_available"
        assert "cotización" in aapl["market_confirmation_reason"]

        # NEWCO: unconfirmed + no catalog/tracking → stays unconfirmed
        newco = next(i for i in items if i["symbol"] == "NEWCO")
        assert newco["market_confirmation"] == "unconfirmed"
        assert "sin datos" in newco["market_confirmation_reason"]


# ---------------------------------------------------------------------------
# Test 11: Empty list doesn't crash
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_list_safe(self):
        _enrich([])  # should not raise

    def test_item_without_optional_fields(self):
        """Minimal item with only symbol → unconfirmed + reason."""
        items = _enrich([{"symbol": "X"}])
        assert items[0]["market_confirmation_reason"] == "sin datos de mercado disponibles"
