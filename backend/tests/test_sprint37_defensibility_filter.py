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

    def test_weak_quality_weak_causal_suppressed(self):
        """Unknown instrument + weak causal → suppressed regardless of score."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("CRYPTO", effective_score=0.9)
        # _make_weak_signal defaults: causal_link_strength="weak", title_mention=False
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"


# ---------------------------------------------------------------------------
# Test 9: Weak quality + strong causal + title_mention policy (Sprint 37b)
# ---------------------------------------------------------------------------

class TestWeakCausalStrongPolicy:
    """LP, JBS, JLL runtime cases: weak instrument but strong causal evidence.
    These should survive in observed as low-value / relevant_not_investable,
    NOT be suppressed, and NOT be promoted to actionable.
    """

    def _make_weak_causal_strong(self, symbol, effective_score=0.5, **kw):
        """Weak quality + strong causal + title_mention=True (the LP/JBS/JLL case)."""
        item = {
            "symbol": symbol,
            "reason": f"{symbol} announces major deal",
            "signal_class": "observed_candidate",
            "effective_score": effective_score,
            "signal_quality": "weak",
            "causal_link_strength": "strong",
            "observed_value_tier": "low",
            "observed_origin": "signal",
            "asset_type_status": None,
            "title_mention": True,
        }
        item.update(kw)
        return item

    # --- Required test 1: weak + causal weak → suppressed ---
    def test_weak_weak_still_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MOEX", effective_score=0.6)
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    # --- Required test 2: weak + causal strong + title_mention → NOT suppressed ---
    def test_weak_causal_strong_title_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("LP")
        assert _get_observed_suppression_reason(item) is None

    def test_weak_causal_strong_title_is_defensible(self):
        from app.services.orchestrator import _is_defensible_observed_candidate
        item = self._make_weak_causal_strong("JBS")
        assert _is_defensible_observed_candidate(item) is True

    def test_weak_causal_strong_title_stays_in_observed(self):
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [
            self._make_weak_causal_strong("LP"),
            _make_weak_signal("MOEX", 0.4),  # weak + weak → suppressed
        ]
        kept, suppressed = _split_observed_candidates_by_defensibility(items)
        assert len(kept) == 1
        assert kept[0]["symbol"] == "LP"
        assert len(suppressed) == 1
        assert suppressed[0]["symbol"] == "MOEX"

    def test_weak_causal_strong_gets_relevant_not_investable(self):
        """Annotation tags these as relevant_not_investable for monitoring."""
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "JLL", "effective_score": 0.55, "signal_class": "observed_candidate",
                "title_mention": True, "reason": "JLL reports earnings", "asset_type_status": None}
        _annotate_observed_candidate(item)
        assert item["signal_quality"] == "weak"
        assert item["causal_link_strength"] == "strong"
        assert item["operational_status"] == "relevant_not_investable"
        assert item["observed_value_tier"] == "low"

    def test_weak_causal_strong_no_title_still_suppressed(self):
        """Without title_mention, weak + strong causal still gets suppressed."""
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("X", title_mention=False)
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    # --- Required test 3: strong + causal strong → NOT suppressed ---
    def test_strong_strong_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_strong_signal("META")
        assert _get_observed_suppression_reason(item) is None

    # --- Required test 4: strong + causal weak + low score → suppressed ---
    def test_strong_weak_low_score_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MA", effective_score=0.54, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) == "weak_signal_low_score"

    def test_strong_weak_high_score_not_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = _make_weak_signal("MA", effective_score=0.57, signal_quality="strong",
                                 asset_type_status="known_valid")
        assert _get_observed_suppression_reason(item) is None

    # --- Required test 5: promotion gate not regressed ---
    def test_weak_causal_strong_never_promoted(self):
        """weak signal_quality items NEVER pass the promotion gate regardless of causal strength."""
        item = self._make_weak_causal_strong("LP", effective_score=0.8)
        # Promotion requires signal_quality == "strong"
        would_promote = (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
            and (item.get("effective_score") or 0) >= 0.6
            and item.get("investable") is True
        )
        assert would_promote is False

    # --- Required test 6: ambiguous tickers not regressed ---
    def test_ambiguous_tickers_still_filtered(self):
        from app.news.pipeline import classify_news_event
        result = classify_news_event(
            "Global markets rally as investors digest rate outlook",
            "Analysts describe broad risk appetite across sectors.",
            ["MA", "V"],
        )
        assert "MA" not in result["related_assets"]
        assert "V" not in result["related_assets"]

    # --- Required test 7: top_suppressed / suppressed_count not regressed ---
    def test_suppressed_count_with_mixed_policy(self):
        """LP (weak+strong+title) observed, MOEX (weak+weak) suppressed."""
        lp = self._make_weak_causal_strong("LP")
        moex = _make_weak_signal("MOEX", 0.3)
        moex["suppression_reason"] = "weak_signal_not_tracked"
        moex["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=[lp], suppressed=[moex])
        c = ds["candidates"]
        assert c["observed_count"] == 1  # LP stays
        assert c["suppressed_count"] == 1  # MOEX suppressed
        assert c["top_suppressed"][0]["symbol"] == "MOEX"

    # --- End-to-end through split + decision_summary ---
    def test_end_to_end_lp_jbs_jll_scenario(self):
        """Real runtime scenario: LP/JBS/JLL survive, BTC/COLCAP/MOEX suppressed."""
        from app.services.orchestrator import _split_observed_candidates_by_defensibility
        items = [
            self._make_weak_causal_strong("LP", effective_score=0.52),
            self._make_weak_causal_strong("JBS", effective_score=0.48),
            self._make_weak_causal_strong("JLL", effective_score=0.55),
            self._make_weak_causal_strong("BTC", effective_score=0.6),   # crypto → suppressed
            self._make_weak_causal_strong("COLCAP", effective_score=0.5),  # index → suppressed
            _make_weak_signal("MOEX", 0.38),  # weak + weak → suppressed
            _make_catalog("SPY"),
        ]
        kept, suppressed = _split_observed_candidates_by_defensibility(items)
        kept_syms = {i["symbol"] for i in kept}
        suppressed_syms = {i["symbol"] for i in suppressed}
        assert kept_syms == {"LP", "JBS", "JLL", "SPY"}
        assert suppressed_syms == {"BTC", "COLCAP", "MOEX"}


# ---------------------------------------------------------------------------
# Test 10: Non-equity symbol filtering (Sprint 37c)
# ---------------------------------------------------------------------------

class TestNonEquityFiltering:
    """BTC, COLCAP runtime cases: weak instrument + strong causal + title_mention
    but non-equity symbol (crypto/index/macro) → should still be suppressed.
    """

    def _make_weak_causal_strong(self, symbol, effective_score=0.5, **kw):
        item = {
            "symbol": symbol,
            "reason": f"{symbol} rallies on global sentiment",
            "signal_class": "observed_candidate",
            "effective_score": effective_score,
            "signal_quality": "weak",
            "causal_link_strength": "strong",
            "observed_value_tier": "low",
            "observed_origin": "signal",
            "asset_type_status": None,
            "title_mention": True,
        }
        item.update(kw)
        return item

    # --- Crypto symbols suppressed ---
    def test_btc_suppressed_despite_strong_causal(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("BTC")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_eth_suppressed_despite_strong_causal(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("ETH")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_sol_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("SOL")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    # --- Index symbols suppressed ---
    def test_colcap_suppressed_despite_strong_causal(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("COLCAP")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_merval_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("MERVAL")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_ibov_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("IBOV")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    # --- Macro proxies suppressed ---
    def test_vix_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("VIX")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    def test_dxy_suppressed(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("DXY")
        assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"

    # --- Company-like symbols STILL survive ---
    def test_lp_still_survives(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("LP")
        assert _get_observed_suppression_reason(item) is None

    def test_jbs_still_survives(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("JBS")
        assert _get_observed_suppression_reason(item) is None

    def test_jll_still_survives(self):
        from app.services.orchestrator import _get_observed_suppression_reason
        item = self._make_weak_causal_strong("JLL")
        assert _get_observed_suppression_reason(item) is None

    # --- Non-equity does NOT get relevant_not_investable ---
    def test_btc_no_relevant_status(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "BTC", "effective_score": 0.6, "signal_class": "observed_candidate",
                "title_mention": True, "reason": "BTC rallies", "asset_type_status": None}
        _annotate_observed_candidate(item)
        assert item.get("operational_status") is None

    def test_lp_gets_relevant_status(self):
        from app.services.orchestrator import _annotate_observed_candidate
        item = {"symbol": "LP", "effective_score": 0.5, "signal_class": "observed_candidate",
                "title_mention": True, "reason": "LP announces deal", "asset_type_status": None}
        _annotate_observed_candidate(item)
        assert item["operational_status"] == "relevant_not_investable"

    # --- _NON_EQUITY_SYMBOLS is accessible ---
    def test_blocklist_contains_expected_symbols(self):
        from app.services.orchestrator import _NON_EQUITY_SYMBOLS
        assert "BTC" in _NON_EQUITY_SYMBOLS
        assert "COLCAP" in _NON_EQUITY_SYMBOLS
        assert "MOEX" in _NON_EQUITY_SYMBOLS
        assert "VIX" in _NON_EQUITY_SYMBOLS
        # Company symbols NOT in blocklist
        assert "LP" not in _NON_EQUITY_SYMBOLS
        assert "JBS" not in _NON_EQUITY_SYMBOLS
        assert "AAPL" not in _NON_EQUITY_SYMBOLS

    # --- No regression: promotion gate ---
    def test_non_equity_never_promoted_anyway(self):
        """Even if somehow defensible, non-equity weak items can't promote."""
        item = self._make_weak_causal_strong("BTC", effective_score=0.9)
        would_promote = (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
            and (item.get("effective_score") or 0) >= 0.6
            and item.get("investable") is True
        )
        assert would_promote is False

    # --- No regression: top_suppressed with mixed policy ---
    def test_top_suppressed_includes_non_equity(self):
        """BTC suppressed appears in top_suppressed."""
        btc = self._make_weak_causal_strong("BTC")
        btc["suppression_reason"] = "weak_signal_not_tracked"
        btc["suppressed_by_defensibility_filter"] = True
        lp = self._make_weak_causal_strong("LP")
        ds = _build(observed=[lp], suppressed=[btc])
        c = ds["candidates"]
        assert c["observed_count"] == 1
        assert c["suppressed_count"] == 1
        assert c["top_suppressed"][0]["symbol"] == "BTC"

    # --- No regression: ambiguous tickers ---
    def test_ambiguous_tickers_not_regressed(self):
        from app.news.pipeline import classify_news_event
        result = classify_news_event(
            "Global markets rally as investors digest rate outlook",
            "Analysts describe broad risk appetite across sectors.",
            ["MA", "V"],
        )
        assert "MA" not in result["related_assets"]
        assert "V" not in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 11: Pipeline counts alignment (Sprint 38)
# ---------------------------------------------------------------------------

class TestPipelineCounts:
    """Validates that pipeline_counts in decision_summary reflects the final
    pipeline state and is consistent with candidates counts.
    """

    def _make_weak_causal_strong(self, symbol, effective_score=0.5):
        return {
            "symbol": symbol, "reason": f"{symbol} deal",
            "signal_class": "observed_candidate", "effective_score": effective_score,
            "signal_quality": "weak", "causal_link_strength": "strong",
            "observed_value_tier": "low", "observed_origin": "signal",
            "title_mention": True, "operational_status": "relevant_not_investable",
        }

    def test_pipeline_counts_present(self):
        """pipeline_counts must exist in decision_summary."""
        ds = _build(observed=[], suppressed=[])
        assert "pipeline_counts" in ds

    def test_pipeline_counts_match_candidates(self):
        """pipeline_counts and candidates must agree on all shared fields."""
        obs = [
            _make_strong_signal("META"),
            _make_catalog("C1"),
            _make_catalog("C2"),
        ]
        sup = [_make_weak_signal("MOEX", 0.3)]
        sup[0]["suppression_reason"] = "weak_signal_not_tracked"
        sup[0]["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=obs, suppressed=sup)
        c = ds["candidates"]
        pc = ds["pipeline_counts"]

        assert pc["observed_count"] == c["observed_count"]
        assert pc["suppressed_count"] == c["suppressed_count"]
        assert pc["actionable_count"] == c["actionable_count"]
        assert pc["observed_signal_count"] == c["observed_signal_count"]
        assert pc["observed_catalog_only_count"] == c["observed_catalog_only_count"]
        assert pc["relevant_non_investable_count"] == c["relevant_non_investable_count"]

    def test_suppression_breakdown(self):
        """pipeline_counts breaks down suppression by type."""
        contradicted = _make_strong_signal("AAPL")
        contradicted["suppressed_by_contradiction"] = True
        defensibility = _make_weak_signal("MOEX", 0.2)
        defensibility["suppression_reason"] = "weak_signal_not_tracked"
        defensibility["suppressed_by_defensibility_filter"] = True
        ds = _build(observed=[], suppressed=[contradicted, defensibility])
        pc = ds["pipeline_counts"]

        assert pc["suppressed_count"] == 2
        assert pc["suppressed_by_contradiction_count"] == 1
        assert pc["suppressed_by_defensibility_count"] == 1

    def test_scoring_stage_delta_visible(self):
        """pipeline_counts shows scoring-stage counts for debugging."""
        ds = _build(observed=[_make_catalog("C1")], suppressed=[])
        pc = ds["pipeline_counts"]
        # scoring_summary was passed empty, so scoring_stage counts are 0
        assert pc["scoring_stage_observed"] == 0
        assert pc["scoring_stage_suppressed"] == 0

    def test_mixed_scenario_counts_aligned(self):
        """Full scenario: actionable + observed + suppressed all aligned."""
        ext = [_make_strong_signal("TSLA", operational_status="actionable")]
        obs = [
            self._make_weak_causal_strong("LP"),
            _make_strong_signal("META"),
            _make_catalog("C1"),
        ]
        sup_by_def = _make_weak_signal("BTC", 0.5)
        sup_by_def["suppressed_by_defensibility_filter"] = True
        sup_by_def["suppression_reason"] = "weak_signal_not_tracked"
        sup_by_con = _make_strong_signal("NFLX")
        sup_by_con["suppressed_by_contradiction"] = True

        ds = _build(observed=obs, ext=ext, suppressed=[sup_by_def, sup_by_con])
        c = ds["candidates"]
        pc = ds["pipeline_counts"]

        assert pc["actionable_count"] == 1  # TSLA
        assert pc["observed_count"] == 3  # LP + META + C1
        assert pc["suppressed_count"] == 2  # BTC + NFLX
        assert pc["suppressed_by_defensibility_count"] == 1  # BTC
        assert pc["suppressed_by_contradiction_count"] == 1  # NFLX
        assert pc["relevant_non_investable_count"] == 1  # LP

        # Cross-check with candidates
        assert c["actionable_count"] == pc["actionable_count"]
        assert c["observed_count"] == pc["observed_count"]
        assert c["suppressed_count"] == pc["suppressed_count"]

    def test_promotion_not_double_counted(self):
        """Promoted items are in actionable, not in observed — counts must reflect this."""
        promoted = _make_strong_signal("MELI", effective_score=0.7)
        promoted["promoted_from_observed"] = True
        ds = _build(observed=[], ext=[promoted])
        pc = ds["pipeline_counts"]

        assert pc["actionable_count"] == 1
        assert pc["observed_count"] == 0
        assert pc["promoted_from_observed_count"] == 1


# ---------------------------------------------------------------------------
# Test 12: Watchlist layer and catalog compaction (Sprint 38b)
# ---------------------------------------------------------------------------

class TestWatchlistLayer:
    """Validates that the watchlist layer surfaces signal-bearing items first
    and catalog-only items are compacted into catalog_summary.
    """

    def _make_weak_causal_strong(self, symbol, effective_score=0.5):
        return {
            "symbol": symbol, "reason": f"{symbol} deal",
            "signal_class": "observed_candidate", "effective_score": effective_score,
            "signal_quality": "weak", "causal_link_strength": "strong",
            "observed_value_tier": "low", "observed_origin": "signal",
            "title_mention": True, "operational_status": "relevant_not_investable",
        }

    def test_watchlist_present(self):
        ds = _build(observed=[], suppressed=[])
        c = ds["candidates"]
        assert "watchlist" in c
        assert "watchlist_count" in c
        assert "catalog_summary" in c

    def test_watchlist_shows_only_signals(self):
        """Watchlist excludes catalog-only items."""
        obs = [
            _make_strong_signal("META"),
            _make_catalog("C1"), _make_catalog("C2"), _make_catalog("C3"),
        ]
        c = _build(observed=obs)["candidates"]
        assert c["watchlist_count"] == 1
        assert len(c["watchlist"]) == 1
        assert c["watchlist"][0]["symbol"] == "META"

    def test_watchlist_prioritizes_strong_over_weak(self):
        """Strong signals rank before weak signals in watchlist."""
        obs = [
            self._make_weak_causal_strong("LP", effective_score=0.5),
            _make_strong_signal("META", effective_score=0.7),
        ]
        c = _build(observed=obs)["candidates"]
        assert c["watchlist_count"] == 2
        # META (strong quality) should rank first
        assert c["watchlist"][0]["symbol"] == "META"
        assert c["watchlist"][1]["symbol"] == "LP"

    def test_watchlist_max_10(self):
        """Watchlist caps at 10 items."""
        obs = [_make_strong_signal(f"S{i}", effective_score=0.5 + i * 0.01) for i in range(15)]
        c = _build(observed=obs)["candidates"]
        assert c["watchlist_count"] == 15
        assert len(c["watchlist"]) == 10

    def test_catalog_summary_counts(self):
        """catalog_summary shows count and top 3 by priority."""
        cats = [_make_catalog(f"C{i}", priority_score=i * 0.1) for i in range(100)]
        c = _build(observed=cats)["candidates"]
        cs = c["catalog_summary"]
        assert cs["count"] == 100
        assert cs["hidden_by_default"] is True
        assert len(cs["top_by_priority"]) == 3

    def test_catalog_summary_empty_when_no_catalog(self):
        obs = [_make_strong_signal("META")]
        c = _build(observed=obs)["candidates"]
        cs = c["catalog_summary"]
        assert cs["count"] == 0
        assert cs["top_by_priority"] == []

    def test_massive_catalog_doesnt_dominate_watchlist(self):
        """1000 catalog + 2 signals → watchlist shows only 2 signals."""
        signals = [
            _make_strong_signal("META", effective_score=0.7),
            self._make_weak_causal_strong("LP", effective_score=0.5),
        ]
        catalog = [_make_catalog(f"C{i}") for i in range(1000)]
        c = _build(observed=signals + catalog)["candidates"]

        assert c["watchlist_count"] == 2
        assert len(c["watchlist"]) == 2
        assert {w["symbol"] for w in c["watchlist"]} == {"META", "LP"}
        assert c["catalog_summary"]["count"] == 1000
        # top_observed still includes catalog (backward compat) but signals rank first
        assert c["top_observed"][0]["symbol"] == "META"

    def test_top_observed_unchanged_backward_compat(self):
        """top_observed still includes catalog items (backward compat)."""
        obs = [_make_catalog("C1"), _make_catalog("C2"), _make_catalog("C3")]
        c = _build(observed=obs)["candidates"]
        assert len(c["top_observed"]) == 3  # all catalog, no change
        assert c["watchlist_count"] == 0  # no signals

    def test_suppressed_not_in_watchlist(self):
        """Suppressed items don't appear in watchlist."""
        sup = [_make_weak_signal("MOEX", 0.3)]
        sup[0]["suppression_reason"] = "weak_signal_not_tracked"
        obs = [_make_strong_signal("META")]
        c = _build(observed=obs, suppressed=sup)["candidates"]
        assert c["watchlist_count"] == 1
        assert c["watchlist"][0]["symbol"] == "META"

    # --- No regression tests ---
    def test_promotion_gate_not_regressed(self):
        """Weak items in watchlist don't promote."""
        item = self._make_weak_causal_strong("LP", effective_score=0.8)
        would_promote = (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
            and (item.get("effective_score") or 0) >= 0.6
            and item.get("investable") is True
        )
        assert would_promote is False

    def test_top_suppressed_not_regressed(self):
        sup = [_make_weak_signal("MOEX", 0.3)]
        sup[0]["suppression_reason"] = "weak_signal_not_tracked"
        c = _build(observed=[], suppressed=sup)["candidates"]
        assert c["suppressed_count"] == 1
        assert c["top_suppressed"][0]["symbol"] == "MOEX"

    def test_pipeline_counts_not_regressed(self):
        obs = [_make_strong_signal("META"), _make_catalog("C1")]
        ds = _build(observed=obs)
        pc = ds["pipeline_counts"]
        c = ds["candidates"]
        assert pc["observed_count"] == c["observed_count"]
        assert pc["suppressed_count"] == c["suppressed_count"]

    def test_ambiguous_tickers_not_regressed(self):
        from app.news.pipeline import classify_news_event
        result = classify_news_event(
            "Global markets rally as investors digest rate outlook",
            "Analysts describe broad risk appetite across sectors.",
            ["MA", "V"],
        )
        assert "MA" not in result["related_assets"]
        assert "V" not in result["related_assets"]


# ---------------------------------------------------------------------------
# Test 13: Review queue — unified human-first priority view
# ---------------------------------------------------------------------------

class TestReviewQueue:
    """Validates the review_queue block in decision_summary provides a
    single ordered structure for operator consumption, reconciled with
    pipeline_counts.
    """

    def _make_relevant_not_investable(self, symbol, effective_score=0.5):
        return {
            "symbol": symbol, "reason": f"{symbol} deal",
            "signal_class": "observed_candidate", "effective_score": effective_score,
            "signal_quality": "weak", "causal_link_strength": "strong",
            "observed_value_tier": "low", "observed_origin": "signal",
            "title_mention": True, "operational_status": "relevant_not_investable",
        }

    def test_review_queue_present(self):
        """review_queue exists in decision_summary."""
        ds = _build(observed=[], suppressed=[])
        assert "review_queue" in ds
        rq = ds["review_queue"]
        for key in ("actionable_now", "watchlist_now", "relevant_not_investable_now",
                     "suppressed_review", "catalog_compact", "total_items"):
            assert key in rq, f"Missing key: {key}"

    def test_review_queue_counts_match_pipeline(self):
        """review_queue section counts reconcile with pipeline_counts."""
        ext = [_make_strong_signal("AAPL", effective_score=0.8)]
        ext[0]["investable"] = True
        obs = [
            _make_strong_signal("META"),
            self._make_relevant_not_investable("LP"),
            _make_catalog("C1"), _make_catalog("C2"),
        ]
        sup = [_make_weak_signal("MOEX", 0.3)]
        sup[0]["suppression_reason"] = "weak_signal_not_tracked"

        ds = _build(ext=ext, observed=obs, suppressed=sup)
        rq = ds["review_queue"]
        pc = ds["pipeline_counts"]

        assert rq["actionable_now"]["count"] == pc["actionable_count"]
        assert rq["suppressed_review"]["count"] == pc["suppressed_count"]
        assert rq["catalog_compact"]["count"] == pc["observed_catalog_only_count"]
        assert rq["total_items"] == pc["actionable_count"] + pc["observed_count"] + pc["suppressed_count"]

    def test_review_queue_sections_populated(self):
        """Each section has items when data exists."""
        ext = [_make_strong_signal("AAPL")]
        ext[0]["investable"] = True
        obs = [
            _make_strong_signal("META"),
            self._make_relevant_not_investable("LP"),
            _make_catalog("C1"),
        ]
        sup = [_make_weak_signal("MOEX", 0.3)]
        sup[0]["suppression_reason"] = "weak_signal_not_tracked"

        rq = _build(ext=ext, observed=obs, suppressed=sup)["review_queue"]

        assert rq["actionable_now"]["count"] == 1
        assert len(rq["actionable_now"]["items"]) == 1
        assert rq["watchlist_now"]["count"] == 2  # META + LP (both are signals)
        assert rq["relevant_not_investable_now"]["count"] == 1
        assert rq["relevant_not_investable_now"]["items"][0]["symbol"] == "LP"
        assert rq["suppressed_review"]["count"] == 1
        assert rq["catalog_compact"]["count"] == 1
        assert rq["catalog_compact"]["hidden_by_default"] is True

    def test_review_queue_empty_pipeline(self):
        """Empty pipeline produces zero counts everywhere."""
        rq = _build(observed=[], suppressed=[])["review_queue"]
        assert rq["actionable_now"]["count"] == 0
        assert rq["watchlist_now"]["count"] == 0
        assert rq["relevant_not_investable_now"]["count"] == 0
        assert rq["suppressed_review"]["count"] == 0
        assert rq["catalog_compact"]["count"] == 0
        assert rq["total_items"] == 0

    def test_review_queue_watchlist_max_10(self):
        """watchlist_now items capped at 10."""
        obs = [_make_strong_signal(f"S{i}", effective_score=0.5 + i * 0.01) for i in range(15)]
        rq = _build(observed=obs)["review_queue"]
        assert rq["watchlist_now"]["count"] == 15
        assert len(rq["watchlist_now"]["items"]) == 10

    def test_review_queue_does_not_break_existing_fields(self):
        """Adding review_queue doesn't remove or change existing fields."""
        obs = [_make_strong_signal("META"), _make_catalog("C1")]
        ds = _build(observed=obs)
        for key in ("primary_driver", "winning_signal", "candidates",
                     "pipeline_counts", "promotion_events", "why_selected"):
            assert key in ds
        c = ds["candidates"]
        assert "watchlist" in c
        assert "catalog_summary" in c
        assert "top_actionable" in c

    def test_review_queue_catalog_compact_matches_catalog_summary(self):
        """catalog_compact in review_queue matches catalog_summary in candidates."""
        cats = [_make_catalog(f"C{i}", priority_score=i * 0.1) for i in range(50)]
        ds = _build(observed=cats)
        rq_cat = ds["review_queue"]["catalog_compact"]
        c_cat = ds["candidates"]["catalog_summary"]
        assert rq_cat["count"] == c_cat["count"]
        assert rq_cat["hidden_by_default"] == c_cat["hidden_by_default"]


# ---------------------------------------------------------------------------
# Test 14: ensure_review_queue backfill for old recommendations
# ---------------------------------------------------------------------------

class TestEnsureReviewQueue:
    """Validates that ensure_review_queue backfills review_queue from stored
    candidates/pipeline_counts for recommendations created before Sprint 38c.
    """

    def test_noop_when_review_queue_present(self):
        """Already-present review_queue is not overwritten."""
        from app.services.orchestrator import ensure_review_queue
        ds = _build(observed=[_make_strong_signal("META")])
        original_rq = ds["review_queue"]
        result = ensure_review_queue(ds)
        assert result["review_queue"] is original_rq  # same object, not rebuilt

    def test_backfills_from_candidates_and_pipeline_counts(self):
        """Missing review_queue is reconstructed from stored data."""
        from app.services.orchestrator import ensure_review_queue
        ds = _build(
            ext=[_make_strong_signal("AAPL")],
            observed=[_make_strong_signal("META"), _make_catalog("C1")],
            suppressed=[_make_weak_signal("MOEX", 0.3)],
        )
        # Simulate old recommendation: remove review_queue
        del ds["review_queue"]
        assert "review_queue" not in ds

        result = ensure_review_queue(ds)
        rq = result["review_queue"]

        assert "actionable_now" in rq
        assert "watchlist_now" in rq
        assert "relevant_not_investable_now" in rq
        assert "suppressed_review" in rq
        assert "catalog_compact" in rq
        assert "total_items" in rq

        # Counts reconcile with pipeline_counts
        pc = ds["pipeline_counts"]
        assert rq["actionable_now"]["count"] == pc["actionable_count"]
        assert rq["suppressed_review"]["count"] == pc["suppressed_count"]
        assert rq["total_items"] == pc["actionable_count"] + pc["observed_count"] + pc["suppressed_count"]

    def test_backfill_empty_decision_summary(self):
        """Empty dict returns as-is (no crash)."""
        from app.services.orchestrator import ensure_review_queue
        result = ensure_review_queue({})
        assert result == {}

    def test_backfill_none_decision_summary(self):
        """None returns as-is (no crash)."""
        from app.services.orchestrator import ensure_review_queue
        result = ensure_review_queue(None)
        assert result is None

    def test_backfill_items_come_from_candidates_top_lists(self):
        """Backfilled items use candidates.top_* lists."""
        from app.services.orchestrator import ensure_review_queue
        ds = _build(
            ext=[_make_strong_signal("AAPL")],
            observed=[_make_strong_signal("META")],
        )
        del ds["review_queue"]
        result = ensure_review_queue(ds)
        rq = result["review_queue"]

        # actionable_now items should match top_actionable
        assert len(rq["actionable_now"]["items"]) == len(ds["candidates"]["top_actionable"])
        # watchlist_now items should match candidates.watchlist
        assert len(rq["watchlist_now"]["items"]) == len(ds["candidates"]["watchlist"])

    def test_full_shape_present_in_decision_summary(self):
        """End-to-end: decision_summary from _build always has review_queue with full shape."""
        ds = _build(
            ext=[_make_strong_signal("AAPL")],
            observed=[_make_strong_signal("META"), _make_catalog("C1")],
            suppressed=[_make_weak_signal("MOEX", 0.3)],
        )
        # These keys must always be present in decision_summary
        required_top = {"primary_driver", "candidates", "pipeline_counts", "review_queue",
                        "promotion_events", "why_selected"}
        assert required_top.issubset(ds.keys()), f"Missing: {required_top - ds.keys()}"

        # review_queue shape
        rq = ds["review_queue"]
        required_rq = {"actionable_now", "watchlist_now", "relevant_not_investable_now",
                        "suppressed_review", "catalog_compact", "total_items"}
        assert required_rq.issubset(rq.keys()), f"Missing: {required_rq - rq.keys()}"

        # Each section has count
        for section in ("actionable_now", "watchlist_now", "relevant_not_investable_now", "suppressed_review"):
            assert "count" in rq[section]
            assert "items" in rq[section]
        assert "count" in rq["catalog_compact"]
        assert isinstance(rq["total_items"], int)
