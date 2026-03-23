"""Sprint 36 — Semantic consistency for observed reporting.

Resolves naming ambiguity:
- observed_signal_strong_count: explicit (new)
- observed_with_signal_count: backward-compat alias (= observed_signal_strong_count)
- observed_signal_count: total real signals (strong + weak)
- top_observed_signals_strong: explicit (new)
- top_observed_signals: backward-compat alias (= top_observed_signals_strong)
- top_observed_signals_real: all signals (strong + weak)
"""

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_signal(symbol, signal_quality="strong", effective_score=0.5,
                 causal_link_strength="strong", observed_value_tier="high",
                 observed_origin="signal", **kwargs):
    item = {
        "symbol": symbol, "reason": f"News about {symbol}",
        "signal_class": "observed_candidate",
        "effective_score": effective_score,
        "signal_quality": signal_quality,
        "causal_link_strength": causal_link_strength,
        "observed_value_tier": observed_value_tier,
        "observed_origin": observed_origin,
        "asset_type_status": "known_valid",
    }
    item.update(kwargs)
    return item


def _make_catalog(symbol, priority_score=0.1):
    return {
        "symbol": symbol, "reason": "Observado desde catalog",
        "signal_class": None, "signal_quality": None,
        "causal_link_strength": None, "observed_value_tier": "catalog",
        "observed_origin": "catalog", "source_types": ["catalog"],
        "priority_score": priority_score,
    }


def _build(observed=None, ext=None):
    from app.services.orchestrator import _build_decision_summary
    rec = {
        "action": "mantener", "actions": [], "rationale_reasons": [],
        "rationale": "Test",
        "external_opportunities": ext or [],
        "observed_candidates": observed or [],
        "suppressed_candidates": [],
    }
    return _build_decision_summary(
        rec=rec, scored_news=[], scoring_summary={},
        llm_input_meta={}, fresh_quote_meta={},
        unchanged=False, unchanged_reason="",
    )


# ---------------------------------------------------------------------------
# Test 1: Count semantics with strong + weak + catalog
# ---------------------------------------------------------------------------

class TestCountSemantics:

    def test_strong_weak_catalog_counts(self):
        """1 strong + 1 weak + 3 catalog → correct counts for all fields."""
        obs = [
            _make_signal("META", signal_quality="strong"),
            _make_signal("TSLA", signal_quality="weak", observed_value_tier="low"),
            _make_catalog("C1"), _make_catalog("C2"), _make_catalog("C3"),
        ]
        c = _build(observed=obs)["candidates"]

        # Total
        assert c["observed_count"] == 5

        # Signal counts
        assert c["observed_signal_count"] == 2           # strong + weak
        assert c["observed_signal_strong_count"] == 1     # strong only
        assert c["observed_weak_signal_count"] == 1       # weak only

        # Catalog counts
        assert c["observed_catalog_only_count"] == 3      # by origin
        assert c["observed_catalog_count"] == 3           # by signal_quality=None

        # Arithmetic identity
        assert c["observed_signal_count"] + c["observed_catalog_only_count"] == c["observed_count"]
        assert c["observed_signal_strong_count"] + c["observed_weak_signal_count"] == c["observed_signal_count"]

    def test_only_weak_signals(self):
        """1 weak signal + 2 catalog → strong count = 0."""
        obs = [
            _make_signal("GPU", signal_quality="weak", observed_value_tier="low"),
            _make_catalog("C1"), _make_catalog("C2"),
        ]
        c = _build(observed=obs)["candidates"]

        assert c["observed_signal_count"] == 1
        assert c["observed_signal_strong_count"] == 0
        assert c["observed_weak_signal_count"] == 1
        assert c["observed_catalog_only_count"] == 2

    def test_only_strong_signals(self):
        """2 strong signals + 0 catalog."""
        obs = [
            _make_signal("META", signal_quality="strong"),
            _make_signal("AAPL", signal_quality="strong"),
        ]
        c = _build(observed=obs)["candidates"]

        assert c["observed_signal_count"] == 2
        assert c["observed_signal_strong_count"] == 2
        assert c["observed_weak_signal_count"] == 0
        assert c["observed_catalog_only_count"] == 0

    def test_zero_signals(self):
        """0 signals + 3 catalog."""
        obs = [_make_catalog("C1"), _make_catalog("C2"), _make_catalog("C3")]
        c = _build(observed=obs)["candidates"]

        assert c["observed_signal_count"] == 0
        assert c["observed_signal_strong_count"] == 0
        assert c["observed_weak_signal_count"] == 0
        assert c["observed_catalog_only_count"] == 3

    def test_empty_input(self):
        c = _build(observed=[])["candidates"]
        assert c["observed_count"] == 0
        assert c["observed_signal_count"] == 0
        assert c["observed_signal_strong_count"] == 0
        assert c["observed_weak_signal_count"] == 0
        assert c["observed_catalog_only_count"] == 0


# ---------------------------------------------------------------------------
# Test 2: Backward-compat alias — observed_with_signal_count
# ---------------------------------------------------------------------------

class TestCountBackwardCompat:

    def test_observed_with_signal_count_equals_strong(self):
        """observed_with_signal_count must always equal observed_signal_strong_count."""
        obs = [
            _make_signal("A", signal_quality="strong"),
            _make_signal("B", signal_quality="strong"),
            _make_signal("C", signal_quality="weak", observed_value_tier="low"),
            _make_catalog("D"),
        ]
        c = _build(observed=obs)["candidates"]
        assert c["observed_with_signal_count"] == c["observed_signal_strong_count"]
        assert c["observed_with_signal_count"] == 2

    def test_alias_zero_when_no_strong(self):
        obs = [_make_signal("X", signal_quality="weak", observed_value_tier="low")]
        c = _build(observed=obs)["candidates"]
        assert c["observed_with_signal_count"] == 0
        assert c["observed_signal_strong_count"] == 0


# ---------------------------------------------------------------------------
# Test 3: List semantics
# ---------------------------------------------------------------------------

class TestListSemantics:

    def test_signals_real_contains_strong_and_weak(self):
        """top_observed_signals_real includes both strong and weak signals."""
        obs = [
            _make_signal("META", signal_quality="strong"),
            _make_signal("GPU", signal_quality="weak", observed_value_tier="low"),
            _make_catalog("C1"),
        ]
        c = _build(observed=obs)["candidates"]

        real = c["top_observed_signals_real"]
        assert len(real) == 2
        assert {i["symbol"] for i in real} == {"META", "GPU"}

    def test_signals_strong_contains_only_strong(self):
        """top_observed_signals_strong only includes strong quality."""
        obs = [
            _make_signal("META", signal_quality="strong"),
            _make_signal("GPU", signal_quality="weak", observed_value_tier="low"),
        ]
        c = _build(observed=obs)["candidates"]

        strong = c["top_observed_signals_strong"]
        assert len(strong) == 1
        assert strong[0]["symbol"] == "META"

    def test_catalog_list_no_signals(self):
        """top_observed_catalog must not contain signal items."""
        obs = [
            _make_signal("META"), _make_catalog("C1"), _make_catalog("C2"),
        ]
        c = _build(observed=obs)["candidates"]

        cat = c["top_observed_catalog"]
        assert len(cat) == 2
        for item in cat:
            assert item.get("observed_origin") == "catalog" or item.get("signal_quality") is None


# ---------------------------------------------------------------------------
# Test 4: Backward-compat alias — top_observed_signals
# ---------------------------------------------------------------------------

class TestListBackwardCompat:

    def test_top_observed_signals_equals_strong(self):
        """top_observed_signals must equal top_observed_signals_strong (same data)."""
        obs = [
            _make_signal("META", signal_quality="strong"),
            _make_signal("GPU", signal_quality="weak", observed_value_tier="low"),
        ]
        c = _build(observed=obs)["candidates"]
        assert c["top_observed_signals"] == c["top_observed_signals_strong"]


# ---------------------------------------------------------------------------
# Test 5: All keys present
# ---------------------------------------------------------------------------

class TestAllKeysPresent:

    def test_new_and_legacy_keys_coexist(self):
        obs = [_make_signal("X"), _make_catalog("Y")]
        c = _build(observed=obs)["candidates"]

        # New explicit keys
        for key in ["observed_signal_count", "observed_signal_strong_count",
                     "observed_catalog_only_count",
                     "top_observed_signals_real", "top_observed_signals_strong"]:
            assert key in c, f"Missing new key: {key}"

        # Legacy backward-compat keys
        for key in ["observed_with_signal_count", "observed_weak_signal_count",
                     "observed_catalog_count", "top_observed_signals",
                     "top_observed", "top_observed_medium", "top_observed_weak",
                     "top_observed_catalog", "top_suppressed"]:
            assert key in c, f"Missing legacy key: {key}"


# ---------------------------------------------------------------------------
# Test 6: Arithmetic identities always hold
# ---------------------------------------------------------------------------

class TestArithmeticIdentities:

    def test_signal_plus_catalog_equals_total(self):
        """signal_count + catalog_only_count == observed_count, always."""
        for obs in [
            [],
            [_make_catalog("C")],
            [_make_signal("S")],
            [_make_signal("S1"), _make_signal("S2", signal_quality="weak",
                                               observed_value_tier="low"),
             _make_catalog("C1"), _make_catalog("C2")],
        ]:
            c = _build(observed=obs)["candidates"]
            assert c["observed_signal_count"] + c["observed_catalog_only_count"] == c["observed_count"], \
                f"Identity failed for {len(obs)} items"

    def test_strong_plus_weak_equals_signal(self):
        """strong + weak == signal_count, always."""
        for obs in [
            [],
            [_make_signal("S", signal_quality="strong")],
            [_make_signal("S", signal_quality="weak", observed_value_tier="low")],
            [_make_signal("S1"), _make_signal("S2", signal_quality="weak",
                                               observed_value_tier="low")],
        ]:
            c = _build(observed=obs)["candidates"]
            assert c["observed_signal_strong_count"] + c["observed_weak_signal_count"] == c["observed_signal_count"]
