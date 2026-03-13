"""Market universe: dynamic allowed assets, watchlist, and asset type validation.

This module resolves the effective set of allowed assets for:
- main recommendation actions (holdings + whitelist override)
- external opportunities (watchlist + universe + instrument_catalog dynamic)

Hierarchy:
1. Holdings reales (snapshot.positions) → always auto-permitted for main actions
2. WHITELIST_ASSETS (.env) → manual override, also permitted for main actions
3. WATCHLIST_ASSETS (.env) → external assets to track as opportunities
4. MARKET_UNIVERSE_ASSETS (.env) → manual broader set (optional override)
5. instrument_catalog (DB) → dynamic universe from IOL discovery (PRIMARY source)
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


def build_allowed_assets(snapshot_positions: list[dict], catalog_symbols: set[str] | None = None) -> dict:
    """Build the full allowed-assets map from holdings + config + catalog.

    catalog_symbols: set of eligible symbols from instrument_catalog (dynamic).
    If None, only manual config is used (backward compatible).

    Layers (distinct semantics):
    - catalog_dynamic: raw IOL-discovered instruments (bruto dinámico)
    - universe_curated: manual MARKET_UNIVERSE_ASSETS config only
    - universe: union of catalog_dynamic + universe_curated (análisis)
    - main_allowed: holdings | whitelist (capa de seguridad para operable)
    """
    settings = get_settings()

    holdings = {p.get("symbol") for p in snapshot_positions if p.get("symbol")}
    whitelist = set(settings.whitelist_assets)
    watchlist = set(settings.watchlist_assets)
    universe_curated = set(settings.market_universe_assets)
    catalog_dynamic = catalog_symbols or set()

    # Universe = manual curated + dynamic catalog (for analysis breadth)
    universe = universe_curated | catalog_dynamic

    main_allowed = holdings | whitelist
    external_allowed = watchlist | universe
    all_known = main_allowed | external_allowed

    return {
        "holdings": holdings,
        "whitelist": whitelist,
        "watchlist": watchlist,
        "universe_curated": universe_curated,
        "catalog_dynamic": catalog_dynamic,
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
    - "catalog": discovered via IOL instrument catalog (PRIMARY external source)
    - "watchlist": in watchlist, tracked for opportunities
    - "in_universe": in broader universe, eligible for opportunity
    - "untracked": not in any configured set
    """
    if symbol in allowed_assets.get("holdings", set()):
        return "in_holdings"
    if symbol in allowed_assets.get("catalog_dynamic", set()):
        return "catalog"
    if symbol in allowed_assets.get("watchlist", set()):
        return "watchlist"
    if symbol in allowed_assets.get("universe", set()):
        return "in_universe"
    return "untracked"
