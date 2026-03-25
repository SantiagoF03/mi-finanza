"""Sprint 37 — Defensibility filter for weak observed signals.

Validates that weak non-defensible observed signals:
- Are removed from observed_candidates
- Appear in suppressed_candidates with suppression_reason
- Are visible in top_suppressed and suppressed_count
- Do NOT affect catalog items, strong signals, or high-score items

Root cause addressed: suppressed_candidates was always [] because the only
suppression path was `suppressed_by_contradiction` (market price contradicts).
Sprint 37 adds a second path via _split_observed_candidates_by_defensibility.

Suppression reasons:
- "weak_signal_not_tracked": weak instrument + weak causal (MOEX-like noise)
- "weak_signal_low_score": known instrument + weak causal + score below threshold
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

    def test_weak_signal_not_tracked_suppressed(self):
        """Weak quality + weak causal → suppressed as weak_signal_not_tracked."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.35)
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_weak_signal_high_score_still_suppressed(self):
        """Weak quality items are suppressed regardless of score (instrument not tracked)."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.8)
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_strong_quality_strong_causal_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_strong_signal("META")
        assert _get_observed_suppression_reason(item) is None

    def test_strong_quality_weak_causal_high_score_not_suppressed(self):
        """Known instrument + weak causal + high score → defensible."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MA", effective_score=0.57, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) is None

    def test_strong_quality_weak_causal_low_score_suppressed(self):
        """Known instrument + weak causal + low score → weak_signal_low_score."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MA", effective_score=0.54, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) == "weak_signal_low_score"

    def test_catalog_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_catalog("CAT1")
        assert _get_observed_suppression_reason(item) is None

    def test_none_score_weak_signal_suppressed(self):
        """effective_score=None → treated as 0, weak quality → suppress."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("X")
        item["effective_score"] = None
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"


# ---------------------------------------------------------------------------
# Test 2: _is_defensible_observed_candidate
# ---------------------------------------------------------------------------

class TestIsDefensible:

    def test_strong_quality_strong_causal_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_strong_signal("META")
        assert _is_defensible_observed_candidate(item) is True

    def test_catalog_always_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_catalog("SPY")
        assert _is_defensible_observed_candidate(item) is True

    def test_weak_quality_not_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_weak_signal("MOEX", effective_score=0.8)
        assert _is_defensible_observed_candidate(item) is False

    def test_strong_quality_weak_causal_high_score_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_weak_signal("MA", effective_score=0.57, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _is_defensible_observed_candidate(item) is True

    def test_strong_quality_weak_causal_low_score_not_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_weak_signal("MA", effective_score=0.54, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _is_defensible_observed_candidate(item) is False


# ---------------------------------------------------------------------------
# Test 3: _split_observed_candidates_by_defensibility
# ---------------------------------------------------------------------------

class TestSplitByDefensibility:

    def test_empty_list(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        d, s = _split_observed_candidates_by_defensibility([])
        assert d == []
        assert s == []

    def test_all_defensible(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [_make_strong_signal("META"), _make_catalog("C1")]
        d, s = _split_observed_candidates_by_defensibility(items)
        assert len(d) == 2
        assert len(s) == 0

    def test_all_suppressed(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [_make_weak_signal("X1", 0.1), _make_weak_signal("X2", 0.2)]
        d, s = _split_observed_candidates_by_defensibility(items)
        assert len(d) == 0
        assert len(s) == 2
        assert all(i["suppression_reason"] == "weak_signal_not_tracked" for i in s)
        assert all(i["suppressed_by_defensibility_filter"] is True for i in s)

    def test_mixed_split(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [
            _make_strong_signal("META"),
            _make_weak_signal("MOEX", 0.35),
            _make_catalog("C1"),
            _make_weak_signal("ABC", 0.1),
        ]
        d, s = _split_observed_candidates_by_defensibility(items)
        assert len(d) == 2  # META + C1
        assert len(s) == 2  # MOEX + ABC
        assert {i["symbol"] for i in d} == {"META", "C1"}
        assert {i["symbol"] for i in s} == {"MOEX", "ABC"}

    def test_suppressed_items_have_reason_and_flag(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [_make_weak_signal("X", 0.1)]
        _, s = _split_observed_candidates_by_defensibility(items)
        assert s[0]["suppression_reason"] == "weak_signal_not_tracked"
        assert s[0]["suppressed_by_defensibility_filter"] is True

    def test_defensible_items_no_reason(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [_make_strong_signal("META")]
        d, _ = _split_observed_candidates_by_defensibility(items)
        assert "suppression_reason" not in d[0]
        assert "suppressed_by_defensibility_filter" not in d[0]


# ---------------------------------------------------------------------------
# Test 4: End-to-end through decision_summary
# ---------------------------------------------------------------------------

class TestDecisionSummarySuppression:

    def test_suppressed_items_in_suppressed_count(self):
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_signal_not_tracked"
        suppressed[0]["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=[], suppressed=suppressed)
        c = ds["candidates"]
        assert c["suppressed_count"] == 1

    def test_suppressed_items_in_top_suppressed(self):
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_signal_not_tracked"
        suppressed[0]["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=[], suppressed=suppressed)
        c = ds["candidates"]
        assert len(c["top_suppressed"]) == 1
        assert c["top_suppressed"][0]["symbol"] == "MOEX"
        assert c["top_suppressed"][0]["suppression_reason"] == "weak_signal_not_tracked"
        assert c["top_suppressed"][0]["suppressed_by_defensibility_filter"] is True

    def test_suppressed_not_in_observed_count(self):
        strong = _make_strong_signal("META")
        suppressed = [_make_weak_signal("MOEX", 0.3)]
        suppressed[0]["suppression_reason"] = "weak_signal_not_tracked"
        ds = _build(observed=[strong], suppressed=suppressed)
        c = ds["candidates"]
        assert c["observed_count"] == 1  # only META
        assert c["suppressed_count"] == 1  # MOEX

    def test_mixed_observed_and_suppressed(self):
        obs = [
            _make_strong_signal("META"),
            _make_catalog("C1"),
            _make_catalog("C2"),
        ]
        sup = [_make_weak_signal("MOEX", 0.2), _make_weak_signal("XYZ", 0.15)]
        for s in sup:
            s["suppression_reason"] = "weak_signal_not_tracked"
        ds = _build(observed=obs, suppressed=sup)
        c = ds["candidates"]
        assert c["observed_count"] == 3
        assert c["observed_signal_count"] == 1  # META only
        assert c["observed_catalog_only_count"] == 2
        assert c["suppressed_count"] == 2
        assert len(c["top_suppressed"]) == 2


# ---------------------------------------------------------------------------
# Test 5: Boundary conditions
# ---------------------------------------------------------------------------

class TestBoundaryConditions:

    def test_weak_causal_strong_quality_high_score_not_suppressed(self):
        """Known instrument + weak causal + score >= 0.55 → defensible."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("V", effective_score=0.55, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) is None

    def test_weak_causal_strong_quality_low_score_suppressed(self):
        """Known instrument + weak causal + score < 0.55 → weak_signal_low_score."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("V", effective_score=0.54, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) == "weak_signal_low_score"

    def test_strong_causal_weak_quality_not_suppressed_false(self):
        """Title mention + unknown instrument → still suppressed (weak quality dominates)."""
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = _make_weak_signal("GPU", effective_score=0.35, causal_link_strength="strong")
        assert _is_defensible_observed_candidate(item) is False


# ---------------------------------------------------------------------------
# Test 6: Backward compatibility — contradiction + defensibility coexist
# ---------------------------------------------------------------------------

class TestBackwardCompat:

    def test_contradiction_suppressed_still_counted(self):
        contradicted = _make_strong_signal("AAPL")
        contradicted["suppressed_by_contradiction"] = True
        ds = _build(observed=[], suppressed=[contradicted])
        c = ds["candidates"]
        assert c["suppressed_count"] == 1

    def test_both_suppression_paths_combined(self):
        s1 = _make_strong_signal("AAPL")
        s1["suppressed_by_contradiction"] = True
        s2 = _make_weak_signal("MOEX", 0.2)
        s2["suppression_reason"] = "weak_signal_not_tracked"
        s2["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=[], suppressed=[s1, s2])
        c = ds["candidates"]
        assert c["suppressed_count"] == 2
        assert len(c["top_suppressed"]) == 2


# ---------------------------------------------------------------------------
# Test 7: _annotate_observed_candidate
# ---------------------------------------------------------------------------

class TestAnnotateObservedCandidate:

    def test_signal_item_gets_all_fields(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "META", "effective_score": 0.6, "signal_class": "observed_candidate",
                "title_mention": True, "reason": "Meta earnings beat", "asset_type_status": "known_valid"}
        _annotate_observed_candidate(item)
        assert item["observed_origin"] == "signal"
        assert item["signal_quality"] == "strong"
        assert item["causal_link_strength"] == "strong"
        assert item["observed_value_tier"] == "high"

    def test_catalog_item_gets_none_fields(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "SPY", "effective_score": None, "signal_class": None,
                "source_types": ["catalog"]}
        _annotate_observed_candidate(item)
        assert item["observed_origin"] == "catalog"
        assert item["signal_quality"] is None
        assert item["causal_link_strength"] is None
        assert item["observed_value_tier"] == "catalog"

    def test_weak_instrument_gets_low_tier(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "MOEX", "effective_score": 0.5, "signal_class": "observed_candidate",
                "title_mention": False, "reason": "Market news", "asset_type_status": "unknown"}
        _annotate_observed_candidate(item)
        assert item["observed_origin"] == "signal"
        assert item["signal_quality"] == "weak"
        assert item["causal_link_strength"] == "weak"
        assert item["observed_value_tier"] == "low"

    def test_relevant_not_investable_status(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "MELI", "effective_score": 0.7, "signal_class": "observed_candidate",
                "title_mention": True, "reason": "MercadoLibre guidance",
                "asset_type_status": "known_valid", "investable": False}
        _annotate_observed_candidate(item)
        assert item["operational_status"] == "relevant_not_investable"


# ---------------------------------------------------------------------------
# Test 8: Real-world calibration
# ---------------------------------------------------------------------------

class TestRealWorldCalibration:

    def test_moex_weak_unconfirmed_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        moex = {
            "symbol": "MOEX", "reason": "Market volatility news",
            "observed_origin": "signal", "signal_quality": "weak",
            "causal_link_strength": "weak", "effective_score": 0.38,
            "market_confirmation": "unconfirmed",
        }
        assert _get_observed_suppression_reason(moex) == "weak_signal_not_tracked"

    def test_meta_strong_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        meta = {
            "symbol": "META", "reason": "Meta reports earnings beat",
            "observed_origin": "signal", "signal_quality": "strong",
            "causal_link_strength": "strong", "effective_score": 0.7,
            "title_mention": True,
        }
        assert _get_observed_suppression_reason(meta) is None

    def test_catalog_spy_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        spy = _make_catalog("SPY")
        assert _get_observed_suppression_reason(spy) is None

    def test_weak_quality_any_score_suppressed(self):
        """Unknown instrument → always suppressed regardless of score."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("CRYPTO", effective_score=0.9)
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"
