"""Tests for iteration 4: asset type resolver, normalized status, weight allocation.

Part A: asset_type_status normalization (known_valid | unknown | unsupported)
Part B: asset type resolver (backend/app/market/assets.py)
Part C: build_target_weights() unallocated bucket redistribution
"""

from app.market.assets import KNOWN_ASSET_TYPES, build_asset_type_map, resolve_asset_type
from app.market.candidates import generate_external_candidates
from app.portfolio.profiles import PROFILE_PRESETS, build_target_weights


# ---------------------------------------------------------------------------
# Part B: Asset type resolver
# ---------------------------------------------------------------------------


def test_resolve_from_positions():
    """Position-based lookup takes priority."""
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    at, status = resolve_asset_type("AAPL", positions=positions)
    assert at == "CEDEAR"
    assert status == "known_valid"


def test_resolve_from_known_map():
    """Static KNOWN_ASSET_TYPES map resolves symbols not in positions."""
    at, status = resolve_asset_type("TSLA", positions=[])
    assert at == "CEDEAR"
    assert status == "known_valid"


def test_resolve_etf_from_known_map():
    at, status = resolve_asset_type("SPY", positions=[])
    assert at == "ETF"
    assert status == "known_valid"


def test_resolve_bono_from_known_map():
    at, status = resolve_asset_type("AL30", positions=[])
    assert at == "BONO"
    assert status == "known_valid"


def test_resolve_unknown_symbol():
    """Completely unknown symbol returns DESCONOCIDO / unknown."""
    at, status = resolve_asset_type("XYZABC123", positions=[])
    assert at == "DESCONOCIDO"
    assert status == "unknown"


def test_resolve_with_extra_map():
    """Caller-provided extra_map overrides for symbols not in positions."""
    at, status = resolve_asset_type("CUSTOM", extra_map={"CUSTOM": "CEDEAR"})
    assert at == "CEDEAR"
    assert status == "known_valid"


def test_resolve_extra_map_unsupported():
    """Extra map with an unsupported type returns unsupported status."""
    at, status = resolve_asset_type("CRYPTO", extra_map={"CRYPTO": "CRYPTOCURRENCY"})
    assert at == "CRYPTOCURRENCY"
    assert status == "unsupported"


def test_resolve_positions_take_priority_over_known_map():
    """If positions say something different, positions win."""
    positions = [{"symbol": "SPY", "asset_type": "ACCIONES"}]
    at, status = resolve_asset_type("SPY", positions=positions)
    assert at == "ACCIONES"
    assert status == "known_valid"


def test_resolve_empty_symbol():
    at, status = resolve_asset_type("")
    assert at == "DESCONOCIDO"
    assert status == "unknown"


def test_build_asset_type_map():
    """build_asset_type_map combines positions and extra symbols."""
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    result = build_asset_type_map(positions, extra_symbols={"TSLA", "XYZUNK"})
    assert result["AAPL"] == ("CEDEAR", "known_valid")
    assert result["TSLA"] == ("CEDEAR", "known_valid")
    assert result["XYZUNK"][1] == "unknown"


# ---------------------------------------------------------------------------
# Part A: Normalized asset_type_status in candidates
# ---------------------------------------------------------------------------


def _make_allowed(watchlist=None, universe=None, holdings=None, whitelist=None):
    holdings = holdings or set()
    whitelist = whitelist or set()
    watchlist = watchlist or set()
    universe = universe or set()
    main_allowed = holdings | whitelist
    return {
        "holdings": holdings,
        "whitelist": whitelist,
        "watchlist": watchlist,
        "universe": universe,
        "main_allowed": main_allowed,
        "external_allowed": watchlist | universe,
    }


def test_candidate_known_valid_status():
    """A watchlist symbol with known type gets asset_type_status=known_valid."""
    allowed = _make_allowed(watchlist={"TSLA"})
    candidates = generate_external_candidates([], allowed, [])
    assert len(candidates) == 1
    c = candidates[0]
    assert c["asset_type"] == "CEDEAR"
    assert c["asset_type_status"] == "known_valid"
    assert c["asset_type_valid"] is True
    assert c["actionable_external"] is True


def test_candidate_unknown_status():
    """An unknown symbol gets asset_type_status=unknown, NOT unsupported."""
    allowed = _make_allowed(watchlist={"ZZZ123QQ"})
    candidates = generate_external_candidates([], allowed, [])
    assert len(candidates) == 1
    c = candidates[0]
    assert c["asset_type"] == "DESCONOCIDO"
    assert c["asset_type_status"] == "unknown"
    assert c["asset_type_valid"] is False
    # Unknown does NOT block actionable (it's in watchlist)
    assert c["actionable_external"] is True


def test_candidate_unknown_not_shown_as_unsupported():
    """DESCONOCIDO should show as 'unknown', not 'unsupported'."""
    allowed = _make_allowed(watchlist={"RANDXYZ"})
    candidates = generate_external_candidates([], allowed, [])
    c = candidates[0]
    assert c["asset_type_status"] != "unsupported"
    assert c["asset_type_status"] == "unknown"


def test_candidate_has_asset_type_status_field():
    """All candidates must include the asset_type_status field."""
    allowed = _make_allowed(watchlist={"TSLA"}, universe={"MELI", "XYZRAND"})
    candidates = generate_external_candidates([], allowed, [])
    for c in candidates:
        assert "asset_type_status" in c
        assert c["asset_type_status"] in ("known_valid", "unknown", "unsupported")


def test_candidate_in_universe_and_whitelist_is_investable():
    """A symbol in universe + whitelist should be investable with known_valid type."""
    # This mirrors the real runtime scenario: AAPL in MARKET_UNIVERSE + WHITELIST
    allowed = _make_allowed(universe={"AAPL", "NVDA"}, whitelist={"AAPL", "NVDA", "MSFT"})
    positions = []  # Not held
    candidates = generate_external_candidates([], allowed, positions)
    syms = {c["symbol"]: c for c in candidates}

    assert "AAPL" in syms
    aapl = syms["AAPL"]
    assert aapl["asset_type"] == "CEDEAR"
    assert aapl["asset_type_status"] == "known_valid"
    assert aapl["in_main_allowed"] is True
    assert aapl["investable"] is True
    assert aapl["actionable_external"] is True
    assert "inversión" in aapl["actionable_reason"].lower() or "whitelist" in aapl["actionable_reason"].lower()


def test_candidate_in_watchlist_not_in_whitelist_not_investable():
    """A watchlist symbol NOT in whitelist should be actionable but NOT investable."""
    allowed = _make_allowed(watchlist={"TSLA"})
    candidates = generate_external_candidates([], allowed, [])
    c = candidates[0]
    assert c["actionable_external"] is True
    assert c["investable"] is False
    assert c["in_main_allowed"] is False


def test_priority_score_increases_with_multiple_sources():
    """A symbol in news + watchlist should have higher priority than watchlist-only."""
    allowed = _make_allowed(watchlist={"TSLA", "NVDA"})
    news_ops = [
        {"symbol": "TSLA", "reason": "Tesla news", "confidence": 0.7, "event_type": "earnings", "impact": "positivo"},
    ]
    candidates = generate_external_candidates(news_ops, allowed, [])
    tsla = next(c for c in candidates if c["symbol"] == "TSLA")
    nvda = next(c for c in candidates if c["symbol"] == "NVDA")
    # TSLA has news + watchlist, NVDA has only watchlist
    assert tsla["priority_score"] > nvda["priority_score"]
    assert len(tsla["source_types"]) == 2


def test_priority_score_boosts_for_investable():
    """Investable candidates (in whitelist + known_valid) get a score boost."""
    # TSLA in watchlist only vs TSLA in watchlist + whitelist
    allowed_no_wl = _make_allowed(watchlist={"TSLA"})
    allowed_with_wl = _make_allowed(watchlist={"TSLA"}, whitelist={"TSLA"})
    c_no = generate_external_candidates([], allowed_no_wl, [])[0]
    c_wl = generate_external_candidates([], allowed_with_wl, [])[0]
    assert c_wl["priority_score"] > c_no["priority_score"]


def test_real_runtime_scenario_aapl_nvda_in_universe():
    """Reproduce the exact runtime scenario from the user's API response.

    Config: MARKET_UNIVERSE_ASSETS=NVDA,AAPL, both also in WHITELIST_ASSETS.
    Holdings include SPY, QQQ, BABA etc. but NOT AAPL/NVDA.
    Expected: AAPL/NVDA show as external with CEDEAR/known_valid, investable=True.
    """
    holdings = {"ACWI", "BABA", "BIDU", "BRKB", "BYMA", "GD35", "GLD", "PAMP", "QQQ", "SPY"}
    whitelist = holdings | {"AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "NVDA", "JPM", "V"}
    universe = {"NVDA", "AAPL"}
    allowed = _make_allowed(universe=universe, holdings=holdings, whitelist=whitelist)

    positions = [{"symbol": s, "asset_type": "CEDEAR"} for s in holdings]
    candidates = generate_external_candidates([], allowed, positions)
    syms = {c["symbol"]: c for c in candidates}

    # Both should appear (not in holdings)
    assert "AAPL" in syms
    assert "NVDA" in syms

    for sym in ["AAPL", "NVDA"]:
        c = syms[sym]
        # Type resolved correctly (NOT DESCONOCIDO)
        assert c["asset_type"] == "CEDEAR", f"{sym} should be CEDEAR, got {c['asset_type']}"
        assert c["asset_type_status"] == "known_valid"
        assert c["asset_type_valid"] is True
        # In whitelist → investable
        assert c["in_main_allowed"] is True
        assert c["investable"] is True
        # In universe → actionable
        assert c["actionable_external"] is True
        # No contradictory flags
        assert c["priority_score"] > 0.1  # More than base universe score


def test_unsupported_blocks_actionable():
    """A symbol with an unsupported (but known) type should not be actionable."""
    # FAKESYM is in watchlist but NOT in holdings (positions has a different symbol)
    # We use extra resolver via positions for a non-held symbol
    positions = [{"symbol": "OTHERSYM", "asset_type": "CEDEAR"}]
    allowed = _make_allowed(watchlist={"FAKESYM"}, holdings=set())
    # Use the resolver's extra_map indirectly: add FAKESYM to KNOWN_ASSET_TYPES temporarily
    from app.market import assets as assets_mod
    original = assets_mod.KNOWN_ASSET_TYPES.get("FAKESYM")
    assets_mod.KNOWN_ASSET_TYPES["FAKESYM"] = "CRYPTOCURRENCY"
    try:
        candidates = generate_external_candidates([], allowed, positions)
        c = candidates[0]
        assert c["asset_type_status"] == "unsupported"
        assert c["actionable_external"] is False
        assert "no soportado" in c["actionable_reason"].lower()
    finally:
        if original is None:
            del assets_mod.KNOWN_ASSET_TYPES["FAKESYM"]
        else:
            assets_mod.KNOWN_ASSET_TYPES["FAKESYM"] = original


# ---------------------------------------------------------------------------
# Part C: build_target_weights — unallocated redistribution
# ---------------------------------------------------------------------------


def test_weights_sum_to_one_all_buckets():
    """When all buckets are represented, weights should sum to 1.0."""
    positions = [
        {"symbol": "AAPL", "asset_type": "CEDEAR"},
        {"symbol": "GGAL", "asset_type": "ACCIONES"},
        {"symbol": "AL30", "asset_type": "BONO"},
        {"symbol": "FIMA", "asset_type": "FondoComundeInversion"},
        {"symbol": "RARO", "asset_type": "UNKNOWN_TYPE"},  # goes to 'otros'
    ]
    weights = build_target_weights(positions, profile="moderado")
    total = sum(weights.values())
    assert abs(total - 1.0) < 0.01, f"Weights sum to {total}, expected 1.0"


def test_weights_sum_to_one_missing_buckets():
    """When some buckets have no holdings, weights should still sum to 1.0."""
    # Only equity_exterior — missing renta_fija, equity_local, fci, otros
    positions = [
        {"symbol": "AAPL", "asset_type": "CEDEAR"},
    ]
    weights = build_target_weights(positions, profile="moderado")
    total = sum(weights.values())
    assert abs(total - 1.0) < 0.01, f"Weights sum to {total}, expected 1.0"


def test_weights_sum_to_one_empty_portfolio():
    """Even with no positions at all, weights should sum to 1.0 (all goes to CASH)."""
    weights = build_target_weights([], profile="moderado")
    total = sum(weights.values())
    assert abs(total - 1.0) < 0.01, f"Weights sum to {total}, expected 1.0"
    assert "CASH" in weights


def test_unallocated_goes_to_cash():
    """Unrepresented bucket weight should be added to CASH."""
    # Only CEDEAR positions — renta_fija, equity_local, fci, otros all unallocated
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    weights = build_target_weights(positions, profile="moderado")
    moderado = PROFILE_PRESETS["moderado"]

    expected_cash = moderado["cash"] + moderado["renta_fija"] + moderado["equity_local"] + moderado["fci"] + moderado["otros"]
    assert abs(weights["CASH"] - expected_cash) < 0.01


def test_weights_sum_to_one_all_profiles():
    """All profiles produce weights summing to 1.0 even with sparse positions."""
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR"}]
    for profile in PROFILE_PRESETS:
        weights = build_target_weights(positions, profile=profile)
        total = sum(weights.values())
        assert abs(total - 1.0) < 0.01, f"{profile}: weights sum to {total}"


def test_weights_no_unallocated_when_all_buckets_filled():
    """When all buckets have symbols, CASH should just be the preset cash target."""
    positions = [
        {"symbol": "AAPL", "asset_type": "CEDEAR"},
        {"symbol": "GGAL", "asset_type": "ACCIONES"},
        {"symbol": "AL30", "asset_type": "BONO"},
        {"symbol": "FIMA", "asset_type": "FondoComundeInversion"},
        {"symbol": "RARO", "asset_type": "RAROTYPE"},
    ]
    weights = build_target_weights(positions, profile="moderado")
    moderado = PROFILE_PRESETS["moderado"]
    assert abs(weights["CASH"] - moderado["cash"]) < 0.01
