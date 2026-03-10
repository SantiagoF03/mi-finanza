"""Market universe: dynamic allowed assets, watchlist, and asset type validation.

This module resolves the effective set of allowed assets for:
- main recommendation actions (holdings + whitelist override)
- external opportunities (watchlist + universe)

Hierarchy:
1. Holdings reales (snapshot.positions) → always auto-permitted for main actions
2. WHITELIST_ASSETS (.env) → manual override, also permitted for main actions
3. WATCHLIST_ASSETS (.env) → external assets to track as opportunities
4. MARKET_UNIVERSE_ASSETS (.env) → broader set of known/operable assets
"""

from __future__ import annotations

from app.core.config import get_settings

# Supported IOL asset types (for runtime validation)
VALID_ASSET_TYPES = {
    "CEDEAR",
    "ACCIONES",
    "TitulosPublicos",
    "FondoComundeInversion",
    "ETF",
    "BONO",
    "ON",  # obligaciones negociables
}
# "DESCONOCIDO" is NOT valid — it's a fallback marker for unknown types


def build_allowed_assets(snapshot_positions: list[dict]) -> dict:
    """Build the full allowed-assets map from holdings + config.

    Returns a dict with:
    - holdings: set of symbols from snapshot (auto-permitted)
    - whitelist: set from WHITELIST_ASSETS config (manual override)
    - watchlist: set from WATCHLIST_ASSETS config (external tracking)
    - universe: set from MARKET_UNIVERSE_ASSETS config (broader operable set)
    - main_allowed: union of holdings + whitelist (what can appear in main actions)
    - external_allowed: union of watchlist + universe (what can appear as opportunities)
    - all_known: union of everything
    """
    settings = get_settings()

    holdings = {p.get("symbol") for p in snapshot_positions if p.get("symbol")}
    whitelist = set(settings.whitelist_assets)
    watchlist = set(settings.watchlist_assets)
    universe = set(settings.market_universe_assets)

    main_allowed = holdings | whitelist
    external_allowed = watchlist | universe
    all_known = main_allowed | external_allowed

    return {
        "holdings": holdings,
        "whitelist": whitelist,
        "watchlist": watchlist,
        "universe": universe,
        "main_allowed": main_allowed,
        "external_allowed": external_allowed,
        "all_known": all_known,
    }


def is_valid_asset_type(asset_type: str) -> bool:
    """Check if an asset type is recognized."""
    return (asset_type or "").strip() in VALID_ASSET_TYPES


def classify_opportunity_status(symbol: str, allowed_assets: dict) -> str:
    """Classify whether an external opportunity is actionable.

    Returns:
    - "in_holdings": already held, should be in main actions not external
    - "watchlist": in watchlist, tracked for opportunities
    - "in_universe": in broader universe, eligible for opportunity
    - "untracked": not in any configured set
    """
    if symbol in allowed_assets.get("holdings", set()):
        return "in_holdings"
    if symbol in allowed_assets.get("watchlist", set()):
        return "watchlist"
    if symbol in allowed_assets.get("universe", set()):
        return "in_universe"
    return "untracked"
