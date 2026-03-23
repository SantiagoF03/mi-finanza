"""Sprint 35 — Separate signal from catalog in observed reporting.

Problem: With 1794 observed_candidates (1792 catalog + 2 signals), the summary
mixed signal items with catalog inventory, making it hard to see real signals.

Changes:
- top_observed now shows signals first (falls back to catalog only if no signals)
- top_observed_signals_real: explicit bucket for observed_origin="signal" items
- top_observed_catalog: uses observed_origin="catalog" (semantic, not signal_quality=None)
- observed_signal_count: count of items with real news signals
- observed_catalog_only_count: count of pure inventory items

Tests:
1. With 1000 catalog + 2 signal items, top_observed shows signals first
2. top_observed_signals_real only contains signal items
3. top_observed_catalog only contains catalog items
4. Counts are correct for mixed scenario
5. All existing keys still present (backward compat)
6. No regression: observed_value_tier, promotion, etc.
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_signal_item(symbol, effective_score=0.5, signal_quality="strong",
                      causal_link_strength="strong", investable=None,
                      observed_value_tier="high", observed_origin="signal",
                      operational_status=None, asset_type_status="known_valid"):
    item = {
        "symbol": symbol, "reason": "Real news about " + symbol,
        "signal_class": "observed_candidate",
        "effective_score": effective_score,
        "signal_quality": signal_quality,
        "causal_link_strength": causal_link_strength,
        "observed_value_tier": observed_value_tier,
        "observed_origin": observed_origin,
        "asset_type_status": asset_type_status,
    }
    if investable is not None:
        item["investable"] = investable
    if operational_status is not None:
        item["operational_status"] = operational_status
    return item


def _make_catalog_item(symbol, priority_score=0.1):
    return {
        "symbol": symbol, "reason": f"Observado desde catalog",
        "signal_class": None,
        "signal_quality": None,
        "causal_link_strength": None,
        "observed_value_tier": "catalog",
        "observed_origin": "catalog",
        "source_types": ["catalog"],
        "priority_score": priority_score,
    }


def _make_ext_opportunity(symbol, effective_score=0.6, operational_status="actionable",
                          opportunity_quality="top", opportunity_rank_reason="t",
                          market_confirmation_reason="t"):
    return {
        "symbol": symbol, "reason": "Test", "signal_class": "external_opportunity",
        "investable": True, "asset_type_status": "known_valid",
        "actionable_external": True, "in_main_allowed": True,
        "signal_quality": "strong", "effective_score": effective_score,
        "operational_status": operational_status,
        "opportunity_quality": opportunity_quality,
        "opportunity_rank_reason": opportunity_rank_reason,
        "market_confirmation_reason": market_confirmation_reason,
    }


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
# Test 1: top_observed shows signals first, even with thousands of catalog
# ---------------------------------------------------------------------------

class TestTopObservedPrioritizesSignals:

    def test_signals_shown_before_catalog(self):
        """With 100 catalog + 2 signal items, top_observed shows signals first."""
        obs = [_make_catalog_item(f"CAT{i}") for i in range(100)]
        obs.append(_make_signal_item("META", effective_score=0.55))
        obs.append(_make_signal_item("TSLA", effective_score=0.45, signal_quality="weak",
                                     causal_link_strength="weak", observed_value_tier="low"))
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]

        top = cands["top_observed"]
        assert len(top) == 3
        # Signal items rank before catalog due to _observed_key
        assert top[0]["symbol"] == "META"  # strong quality → score 2
        assert top[1]["symbol"] == "TSLA"  # weak quality → score 1
        # 3rd slot can be catalog (filling remaining spots)

        # top_observed_signals_real has ONLY the signals — no catalog
        signals = cands["top_observed_signals_real"]
        assert len(signals) == 2
        assert {i["symbol"] for i in signals} == {"META", "TSLA"}

    def test_top_observed_fallback_to_catalog_when_no_signals(self):
        """If there are zero signal items, top_observed shows catalog items."""
        obs = [_make_catalog_item(f"CAT{i}", priority_score=0.1 + i * 0.01) for i in range(5)]
        summary = _build_summary(observed_candidates=obs)
        top = summary["candidates"]["top_observed"]
        assert len(top) >= 1
        assert all(i["observed_origin"] == "catalog" for i in top)


# ---------------------------------------------------------------------------
# Test 2: top_observed_signals_real only contains signal items
# ---------------------------------------------------------------------------

class TestTopObservedSignalsReal:

    def test_only_signal_items(self):
        obs = [
            _make_signal_item("META", effective_score=0.6),
            _make_signal_item("TSLA", effective_score=0.4, signal_quality="weak",
                              observed_value_tier="low"),
            _make_catalog_item("CAT1"),
            _make_catalog_item("CAT2"),
        ]
        summary = _build_summary(observed_candidates=obs)
        top = summary["candidates"]["top_observed_signals_real"]
        assert len(top) == 2
        for item in top:
            assert item.get("observed_origin") == "signal"
        assert {i["symbol"] for i in top} == {"META", "TSLA"}

    def test_empty_when_no_signals(self):
        obs = [_make_catalog_item("CAT1")]
        summary = _build_summary(observed_candidates=obs)
        assert summary["candidates"]["top_observed_signals_real"] == []

    def test_key_exists_with_empty_input(self):
        summary = _build_summary(observed_candidates=[])
        assert "top_observed_signals_real" in summary["candidates"]


# ---------------------------------------------------------------------------
# Test 3: top_observed_catalog only contains catalog items
# ---------------------------------------------------------------------------

class TestTopObservedCatalog:

    def test_only_catalog_items(self):
        obs = [
            _make_signal_item("META"),
            _make_catalog_item("CAT1"),
            _make_catalog_item("CAT2"),
            _make_catalog_item("CAT3"),
        ]
        summary = _build_summary(observed_candidates=obs)
        top = summary["candidates"]["top_observed_catalog"]
        assert len(top) == 3
        for item in top:
            assert item.get("observed_origin") == "catalog"

    def test_empty_when_no_catalog(self):
        obs = [_make_signal_item("META")]
        summary = _build_summary(observed_candidates=obs)
        assert summary["candidates"]["top_observed_catalog"] == []


# ---------------------------------------------------------------------------
# Test 4: Counts are correct for mixed scenario
# ---------------------------------------------------------------------------

class TestCounts:

    def test_signal_and_catalog_counts(self):
        obs = [
            _make_signal_item("META", signal_quality="strong"),
            _make_signal_item("TSLA", signal_quality="weak", observed_value_tier="low"),
            _make_catalog_item("CAT1"),
            _make_catalog_item("CAT2"),
            _make_catalog_item("CAT3"),
        ]
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]

        assert cands["observed_count"] == 5
        assert cands["observed_signal_count"] == 2
        assert cands["observed_catalog_only_count"] == 3
        # Existing counts still correct
        assert cands["observed_with_signal_count"] == 1  # strong only
        assert cands["observed_weak_signal_count"] == 1  # weak only
        assert cands["observed_catalog_count"] == 3       # signal_quality=None

    def test_runtime_realistic_proportions(self):
        """Simulate real runtime: 1792 catalog + 2 signals."""
        obs = [_make_catalog_item(f"C{i}") for i in range(50)]  # scaled down
        obs.append(_make_signal_item("SIG1", effective_score=0.5))
        obs.append(_make_signal_item("SIG2", effective_score=0.3, signal_quality="weak",
                                     observed_value_tier="low"))
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]

        assert cands["observed_signal_count"] == 2
        assert cands["observed_catalog_only_count"] == 50
        assert cands["observed_count"] == 52

        # top_observed shows signals, not catalog
        top = cands["top_observed"]
        top_syms = [i["symbol"] for i in top]
        assert "SIG1" in top_syms
        assert "SIG2" in top_syms


# ---------------------------------------------------------------------------
# Test 5: Backward compatibility — all existing keys present
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:

    def test_all_existing_keys_present(self):
        ext = [_make_ext_opportunity("GOOGL")]
        obs = [
            _make_signal_item("META"),
            _make_catalog_item("CAT1"),
        ]
        summary = _build_summary(external_opportunities=ext, observed_candidates=obs)
        cands = summary["candidates"]

        existing_keys = [
            "actionable_count", "investable_count", "promoted_from_observed_count",
            "observed_count", "observed_with_signal_count", "observed_weak_signal_count",
            "observed_catalog_count", "observed_high_value_count",
            "observed_medium_value_count", "observed_low_value_count",
            "relevant_non_investable_count", "suppressed_count",
            "top_actionable", "top_relevant_non_investable", "top_observed",
            "top_observed_signals", "top_observed_medium", "top_observed_weak",
            "top_observed_catalog", "top_suppressed",
        ]
        for key in existing_keys:
            assert key in cands, f"Missing backward-compat key: {key}"

    def test_new_keys_present(self):
        summary = _build_summary(observed_candidates=[_make_signal_item("X")])
        cands = summary["candidates"]
        for key in ["observed_signal_count", "observed_catalog_only_count",
                     "top_observed_signals_real"]:
            assert key in cands, f"Missing new key: {key}"


# ---------------------------------------------------------------------------
# Test 6: No regression — observed tiers
# ---------------------------------------------------------------------------

class TestNoRegressionTiers:

    def test_observed_tiers_unchanged(self):
        obs = [
            _make_signal_item("HIGH", observed_value_tier="high"),
            _make_signal_item("MED", signal_quality="strong", causal_link_strength="weak",
                              investable=True, observed_value_tier="medium"),
            _make_signal_item("LOW", signal_quality="weak", observed_value_tier="low"),
            _make_catalog_item("CAT"),
        ]
        summary = _build_summary(observed_candidates=obs)
        cands = summary["candidates"]
        assert cands["observed_high_value_count"] == 1
        assert cands["observed_medium_value_count"] == 1
        assert cands["observed_low_value_count"] == 1
        assert cands["observed_catalog_count"] == 1


# ---------------------------------------------------------------------------
# Test 7: No regression — promotion gates
# ---------------------------------------------------------------------------

class TestNoRegressionPromotion:

    def test_promotion_gates_unchanged(self):
        obs = [_make_signal_item("X", effective_score=0.45, investable=True)]
        summary = _build_summary(observed_candidates=obs)
        assert summary["candidates"]["promoted_from_observed_count"] == 0


# ---------------------------------------------------------------------------
# Test 8: No regression — top_actionable, sprint 31-33 fields
# ---------------------------------------------------------------------------

class TestNoRegressionActionable:

    def test_top_actionable_fields_intact(self):
        ext = [_make_ext_opportunity("TSLA", effective_score=0.7)]
        summary = _build_summary(external_opportunities=ext)
        top = summary["candidates"]["top_actionable"][0]
        for key in ["symbol", "effective_score", "operational_status",
                     "opportunity_quality", "opportunity_rank_reason",
                     "market_confirmation_reason"]:
            assert key in top, f"Missing field: {key}"
