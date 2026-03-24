"""Sprint 37 — defensible observed signal filtering.

Goal:
- keep real observed opportunities when causal evidence is clear
- drop weak/untracked observed noise
- preserve promotion and ambiguous ticker protections
"""

from app.services.orchestrator import (
    _annotate_observed_candidate,
    _build_decision_summary,
    _get_observed_suppression_reason,
    _has_causal_link,
    _is_defensible_observed_candidate,
    _split_observed_candidates_by_defensibility,
)


def _observed_item(
    symbol: str,
    *,
    reason: str,
    effective_score: float | None,
    signal_class: str | None = "observed_candidate",
    title_mention: bool = False,
    asset_type_status: str | None = None,
    investable: bool | None = None,
    in_main_allowed: bool | None = None,
    tracking_status: str | None = None,
) -> dict:
    item = {
        "symbol": symbol,
        "reason": reason,
        "effective_score": effective_score,
        "signal_class": signal_class,
        "title_mention": title_mention,
    }
    if asset_type_status is not None:
        item["asset_type_status"] = asset_type_status
    if investable is not None:
        item["investable"] = investable
    if in_main_allowed is not None:
        item["in_main_allowed"] = in_main_allowed
    if tracking_status is not None:
        item["tracking_status"] = tracking_status
    return item


def test_defensible_company_news_survives_without_ticker_in_title():
    """Company-name causal link should survive the new observed filter."""
    item = _observed_item(
        "V",
        reason="Visa reports record quarterly earnings, beating expectations",
        effective_score=0.52,
        title_mention=False,
        asset_type_status="known_valid",
        investable=True,
        tracking_status="watchlist",
    )

    _annotate_observed_candidate(item)

    assert _has_causal_link(item) is True
    assert item["causal_link_strength"] == "strong"
    assert _is_defensible_observed_candidate(item) is True


def test_generic_foreign_index_signal_is_filtered_out():
    """Weak marginal symbols like foreign indexes should not survive as observed signals."""
    item = _observed_item(
        "MOEX",
        reason="Moscow Exchange index edges higher as global investors assess rates",
        effective_score=0.61,
        title_mention=False,
        asset_type_status="unknown",
        investable=False,
        tracking_status="untracked",
    )

    _annotate_observed_candidate(item)

    assert item["signal_quality"] == "weak"
    assert item["causal_link_strength"] == "weak"
    assert _is_defensible_observed_candidate(item) is False
    assert _get_observed_suppression_reason(item) == "weak_signal_not_tracked"


def test_filtered_weak_signal_appears_in_suppressed_bucket():
    """Discarded weak observed signals should be traceable in suppressed_candidates."""
    item = _observed_item(
        "MOEX",
        reason="Moscow Exchange index edges higher as global investors assess rates",
        effective_score=0.61,
        title_mention=False,
        asset_type_status="unknown",
        investable=False,
        tracking_status="untracked",
    )

    _annotate_observed_candidate(item)
    kept, suppressed = _split_observed_candidates_by_defensibility([item])

    assert kept == []
    assert len(suppressed) == 1
    assert suppressed[0]["symbol"] == "MOEX"
    assert suppressed[0]["suppression_reason"] == "weak_signal_not_tracked"
    assert suppressed[0]["suppressed_by_defensibility_filter"] is True


def test_known_tracked_weak_signal_can_still_survive_if_score_is_meaningful():
    """Weak causal observed items still survive when they are real tracked instruments."""
    item = _observed_item(
        "MA",
        reason="Payments stocks rise after stronger consumer spending data",
        effective_score=0.57,
        title_mention=False,
        asset_type_status="known_valid",
        investable=True,
        tracking_status="watchlist",
    )

    _annotate_observed_candidate(item)

    assert item["signal_quality"] == "strong"
    assert item["causal_link_strength"] == "weak"
    assert _is_defensible_observed_candidate(item) is True
    assert _get_observed_suppression_reason(item) is None


def test_low_score_weak_signal_gets_explicit_reason():
    """Tracked weak signals below threshold should be suppressed as low score."""
    item = _observed_item(
        "MA",
        reason="Payments stocks rise after stronger consumer spending data",
        effective_score=0.54,
        title_mention=False,
        asset_type_status="known_valid",
        investable=True,
        tracking_status="watchlist",
    )

    _annotate_observed_candidate(item)

    assert item["signal_quality"] == "strong"
    assert item["causal_link_strength"] == "weak"
    assert _is_defensible_observed_candidate(item) is False
    assert _get_observed_suppression_reason(item) == "weak_signal_low_score"


def test_observed_promotion_gate_not_regressed():
    """Strong causal + score threshold still qualifies for later promotion."""
    item = _observed_item(
        "MELI",
        reason="MercadoLibre raises guidance after strong quarter",
        effective_score=0.65,
        title_mention=False,
        asset_type_status="known_valid",
        investable=True,
        tracking_status="watchlist",
    )

    _annotate_observed_candidate(item)

    promotion_threshold = 0.6
    should_promote = (
        _is_defensible_observed_candidate(item)
        and item["signal_quality"] == "strong"
        and item["causal_link_strength"] == "strong"
        and item["effective_score"] >= promotion_threshold
        and item["investable"] is True
    )
    assert should_promote is True


def test_defensible_signal_does_not_appear_in_suppressed_bucket():
    """Strong causal observed items should not leak into suppressed_candidates."""
    item = _observed_item(
        "MELI",
        reason="MercadoLibre raises guidance after strong quarter",
        effective_score=0.65,
        title_mention=False,
        asset_type_status="known_valid",
        investable=True,
        tracking_status="watchlist",
    )

    _annotate_observed_candidate(item)
    kept, suppressed = _split_observed_candidates_by_defensibility([item])

    assert len(kept) == 1
    assert suppressed == []


def test_decision_summary_reflects_defensibility_suppression():
    """decision_summary top_suppressed should expose the filter discard reason."""
    suppressed_item = {
        "symbol": "MOEX",
        "reason": "Moscow Exchange index edges higher as global investors assess rates",
        "effective_score": 0.61,
        "signal_class": "observed_candidate",
        "signal_quality": "weak",
        "causal_link_strength": "weak",
        "observed_origin": "signal",
        "observed_value_tier": "low",
        "suppression_reason": "weak_signal_not_tracked",
        "suppressed_by_defensibility_filter": True,
    }

    summary = _build_decision_summary(
        rec={
            "action": "mantener",
            "actions": [],
            "rationale_reasons": [],
            "rationale": "Test",
            "external_opportunities": [],
            "observed_candidates": [],
            "suppressed_candidates": [suppressed_item],
        },
        scored_news=[],
        scoring_summary={"suppressed_count": 1},
        llm_input_meta={},
        fresh_quote_meta={},
        unchanged=False,
        unchanged_reason="",
    )

    candidates = summary["candidates"]
    assert candidates["suppressed_count"] == 1
    assert candidates["top_suppressed"][0]["symbol"] == "MOEX"
    assert candidates["top_suppressed"][0]["suppression_reason"] == "weak_signal_not_tracked"
    assert candidates["top_suppressed"][0]["suppressed_by_defensibility_filter"] is True


def test_ambiguous_ticker_mapping_fix_not_regressed():
    """Generic market text must still avoid ambiguous ticker false positives."""
    from app.news.pipeline import classify_news_event

    result = classify_news_event(
        "Global markets rally as investors digest rate outlook",
        "Analysts describe broad risk appetite across sectors.",
        ["MA", "V"],
    )

    assert "MA" not in result["related_assets"]
    assert "V" not in result["related_assets"]
