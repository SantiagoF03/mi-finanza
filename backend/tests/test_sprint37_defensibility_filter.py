"""Sprint 37 — Defensibility filter for weak observed signals.

Validates that weak non-defensible observed signals:
- Are removed from observed_candidates
- Appear in suppressed_candidates with suppression_reason
- Are visible in top_suppressed and suppressed_count
- Do NOT affect catalog items, strong signals, or high-score items

Root cause addressed: suppressed_candidates was always [] because the only
suppression path was `suppressed_by_contradiction` (market price contradicts).
Sprint 37 adds a second path: `weak_non_defensible` for signals with no causal
link, unrecognized instrument, and low score.
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_weak_signal(symbol, effective_score=0.35, **kwargs):
    """Prototypical weak non-defensible signal (MOEX-like)."""
    item = {
        "symbol": symbol,
        "reason": f"Generic news mentioning {symbol}",
        "signal_class": "observed_candidate",
        "effective_score": effective_score,
        "signal_quality": "weak",
        "causal_link_strength": "weak",
        "observed_value_tier": "low",
        "observed_origin": "signal",
        "asset_type_status": None,
        "title_mention": False,
    }
    item.update(kwargs)
    return item


def _make_strong_signal(symbol, effective_score=0.6, **kwargs):
    """Strong defensible signal — should NEVER be suppressed."""
    item = {
        "symbol": symbol,
        "reason": f"{symbol} reports strong earnings",
        "signal_class": "observed_candidate",
        "effective_score": effective_score,
        "signal_quality": "strong",
        "causal_link_strength": "strong",
        "observed_value_tier": "high",
        "observed_origin": "signal",
        "asset_type_status": "known_valid",
        "title_mention": True,
    }
    item.update(kwargs)
    return item


def _make_catalog(symbol, priority_score=0.1):
    """Pure catalog item — should NEVER be suppressed."""
    return {
        "symbol": symbol,
        "reason": "Observado desde catalog",
        "signal_class": None,
        "signal_quality": None,
        "causal_link_strength": None,
        "observed_value_tier": "catalog",
        "observed_origin": "catalog",
        "source_types": ["catalog"],
        "priority_score": priority_score,
    }


def _build(observed=None, ext=None, suppressed=None):
    """Build decision_summary from pre-formed observed/ext/suppressed lists."""
    from app.services.orchestrator import _build_decision_summary
    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Test",
        "external_opportunities": ext or [],
        "observed_candidates": observed or [],
        "suppressed_candidates": suppressed or [],
    }
    return _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )


# ---------------------------------------------------------------------------
# Test 1: _get_observed_suppression_reason unit tests
# ---------------------------------------------------------------------------

class TestSuppressionReason:

    def test_weak_signal_below_threshold_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.35)
        assert _get_observed_suppression_reason(item) == "weak_non_defensible"

    def test_weak_signal_at_threshold_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.4)
        assert _get_observed_suppression_reason(item) is None

    def test_weak_signal_above_threshold_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.55)
        assert _get_observed_suppression_reason(item) is None

    def test_strong_signal_quality_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("AAPL", effective_score=0.2, signal_quality="strong")
        assert _get_observed_suppression_reason(item) is None

    def test_strong_causal_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("AAPL", effective_score=0.2, causal_link_strength="strong")
        assert _get_observed_suppression_reason(item) is None

    def test_catalog_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_catalog("CAT1")
        assert _get_observed_suppression_reason(item) is None

    def test_none_score_treated_as_zero(self):
        """effective_score=None → treated as 0, below threshold → suppress."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("X", effective_score=None)
        # Override to ensure None
        item["effective_score"] = None
        assert _get_observed_suppression_reason(item) == "weak_non_defensible"

    def test_custom_threshold(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("X", effective_score=0.5)
        assert _get_observed_suppression_reason(item, score_threshold=0.6) == "weak_non_defensible"
        assert _get_observed_suppression_reason(item, score_threshold=0.5) is None


# ---------------------------------------------------------------------------
# Test 2: _split_observed_by_defensibility
# ---------------------------------------------------------------------------

class TestSplitByDefensibility:

    def test_empty_list(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        d, s = _split_observed_by_defensibility([])
        assert d == []
        assert s == []

    def test_all_defensible(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        items = [_make_strong_signal("META"), _make_catalog("C1")]
        d, s = _split_observed_by_defensibility(items)
        assert len(d) == 2
        assert len(s) == 0

    def test_all_suppressed(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        items = [_make_weak_signal("X1", 0.1), _make_weak_signal("X2", 0.2)]
        d, s = _split_observed_by_defensibility(items)
        assert len(d) == 0
        assert len(s) == 2
        assert all(i["suppression_reason"] == "weak_non_defensible" for i in s)

    def test_mixed_split(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        items = [
            _make_strong_signal("META"),
            _make_weak_signal("MOEX", 0.35),
            _make_catalog("C1"),
            _make_weak_signal("ABC", 0.1),
        ]
        d, s = _split_observed_by_defensibility(items)
        assert len(d) == 2  # META + C1
        assert len(s) == 2  # MOEX + ABC
        assert {i["symbol"] for i in d} == {"META", "C1"}
        assert {i["symbol"] for i in s} == {"MOEX", "ABC"}

    def test_suppressed_items_have_reason(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        items = [_make_weak_signal("X", 0.1)]
        _, s = _split_observed_by_defensibility(items)
        assert s[0]["suppression_reason"] == "weak_non_defensible"

    def test_defensible_items_no_reason(self):
        from app.services.orchestrator import _split_observed_by_defensibility
        items = [_make_strong_signal("META")]
        d, _ = _split_observed_by_defensibility(items)
        assert "suppression_reason" not in d[0]


# ---------------------------------------------------------------------------
# Test 3: End-to-end through decision_summary
# ---------------------------------------------------------------------------

class TestDecisionSummarySuppression:

    def test_suppressed_items_in_suppressed_count(self):
        """Suppressed items contribute to suppressed_count."""
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_non_defensible"
        ds = _build(observed=[], suppressed=suppressed)
        c = ds["candidates"]
        assert c["suppressed_count"] == 1

    def test_suppressed_items_in_top_suppressed(self):
        """Suppressed items appear in top_suppressed."""
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_non_defensible"
        ds = _build(observed=[], suppressed=suppressed)
        c = ds["candidates"]
        assert len(c["top_suppressed"]) == 1
        assert c["top_suppressed"][0]["symbol"] == "MOEX"
        assert c["top_suppressed"][0]["suppression_reason"] == "weak_non_defensible"

    def test_suppressed_not_in_observed_count(self):
        """Suppressed items don't inflate observed_count."""
        strong = _make_strong_signal("META")
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_non_defensible"
        ds = _build(observed=[strong], suppressed=suppressed)
        c = ds["candidates"]
        assert c["observed_count"] == 1  # only META
        assert c["suppressed_count"] == 1  # MOEX

    def test_mixed_observed_and_suppressed(self):
        """Combined scenario: strong observed + catalog + suppressed weak."""
        obs = [
            _make_strong_signal("META"),
            _make_catalog("C1"),
            _make_catalog("C2"),
        ]
        sup = [_make_weak_signal("MOEX", 0.2), _make_weak_signal("XYZ", 0.15)]
        for s in sup:
            s["suppression_reason"] = "weak_non_defensible"
        ds = _build(observed=obs, suppressed=sup)
        c = ds["candidates"]
        assert c["observed_count"] == 3
        assert c["observed_signal_count"] == 1  # META only
        assert c["observed_catalog_only_count"] == 2
        assert c["suppressed_count"] == 2
        assert len(c["top_suppressed"]) == 2


# ---------------------------------------------------------------------------
# Test 4: Boundary conditions
# ---------------------------------------------------------------------------

class TestBoundaryConditions:

    def test_weak_causal_strong_quality_not_suppressed(self):
        """Known instrument + weak causal → NOT suppressed (still useful to track)."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("V", effective_score=0.35, signal_quality="strong")
        assert _get_observed_suppression_reason(item) is None

    def test_strong_causal_weak_quality_not_suppressed(self):
        """Title mention + unknown instrument → NOT suppressed (causal link is real)."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("GPU", effective_score=0.35, causal_link_strength="strong")
        assert _get_observed_suppression_reason(item) is None

    def test_score_exactly_at_boundary(self):
        """effective_score == 0.4 (threshold) → NOT suppressed."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("X", effective_score=0.4)
        assert _get_observed_suppression_reason(item) is None

    def test_score_just_below_boundary(self):
        """effective_score == 0.39 → suppressed."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("X", effective_score=0.39)
        assert _get_observed_suppression_reason(item) == "weak_non_defensible"


# ---------------------------------------------------------------------------
# Test 5: Backward compatibility — old suppression_by_contradiction still works
# ---------------------------------------------------------------------------

class TestBackwardCompat:

    def test_contradiction_suppressed_still_counted(self):
        """Items suppressed by contradiction still appear in suppressed_candidates."""
        contradicted = _make_strong_signal("AAPL")
        contradicted["suppressed_by_contradiction"] = True
        ds = _build(observed=[], suppressed=[contradicted])
        c = ds["candidates"]
        assert c["suppressed_count"] == 1

    def test_both_suppression_paths_combined(self):
        """Contradiction + defensibility both contribute to suppressed_count."""
        s1 = _make_strong_signal("AAPL")
        s1["suppressed_by_contradiction"] = True
        s2 = _make_weak_signal("MOEX", 0.2)
        s2["suppression_reason"] = "weak_non_defensible"
        ds = _build(observed=[], suppressed=[s1, s2])
        c = ds["candidates"]
        assert c["suppressed_count"] == 2
        assert len(c["top_suppressed"]) == 2


# ---------------------------------------------------------------------------
# Test 6: Real-world calibration
# ---------------------------------------------------------------------------

class TestRealWorldCalibration:

    def test_moex_weak_unconfirmed_suppressed(self):
        """MOEX: weak signal, weak causal, low score → suppressed."""
        from app.services.orchestrator import _get_observed_suppression_reason
        moex = {
            "symbol": "MOEX", "reason": "Market volatility news",
            "observed_origin": "signal", "signal_quality": "weak",
            "causal_link_strength": "weak", "effective_score": 0.38,
            "market_confirmation": "unconfirmed",
        }
        assert _get_observed_suppression_reason(moex) == "weak_non_defensible"

    def test_meta_strong_not_suppressed(self):
        """META: strong signal, strong causal → never suppressed."""
        from app.services.orchestrator import _get_observed_suppression_reason
        meta = {
            "symbol": "META", "reason": "Meta reports earnings beat",
            "observed_origin": "signal", "signal_quality": "strong",
            "causal_link_strength": "strong", "effective_score": 0.7,
            "title_mention": True,
        }
        assert _get_observed_suppression_reason(meta) is None

    def test_catalog_spy_not_suppressed(self):
        """SPY from catalog → never suppressed (no signal to judge)."""
        from app.services.orchestrator import _get_observed_suppression_reason
        spy = _make_catalog("SPY")
        assert _get_observed_suppression_reason(spy) is None

    def test_weak_quality_but_high_score_not_suppressed(self):
        """Unknown instrument with high score → keep it (score saves it)."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("CRYPTO", effective_score=0.55)
        assert _get_observed_suppression_reason(item) is None
