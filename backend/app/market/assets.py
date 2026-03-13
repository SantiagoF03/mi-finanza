"""Asset type resolver for symbols not in current holdings.

Provides a centralized way to determine asset_type for any symbol,
using multiple sources in priority order:
1. Current holdings (positions) — most reliable
2. Configurable static map (KNOWN_ASSET_TYPES) — for watchlist/universe symbols
3. Heuristic inference — simple pattern-based fallback

This avoids the problem where external symbols always get "DESCONOCIDO"
because the only lookup was positions-based.
"""

from __future__ import annotations

from app.recommendations.universe import VALID_ASSET_TYPES

# Static map for well-known symbols that aren't in holdings.
# Users can extend this via config or this map can grow over time.
KNOWN_ASSET_TYPES: dict[str, str] = {
    # CEDEARs (common Argentine market)
    "AAPL": "CEDEAR",
    "MSFT": "CEDEAR",
    "GOOGL": "CEDEAR",
    "GOOG": "CEDEAR",
    "AMZN": "CEDEAR",
    "TSLA": "CEDEAR",
    "META": "CEDEAR",
    "NVDA": "CEDEAR",
    "NFLX": "CEDEAR",
    "DIS": "CEDEAR",
    "KO": "CEDEAR",
    "PEP": "CEDEAR",
    "WMT": "CEDEAR",
    "JPM": "CEDEAR",
    "V": "CEDEAR",
    "MA": "CEDEAR",
    "BA": "CEDEAR",
    "MELI": "CEDEAR",
    "GLOB": "CEDEAR",
    "BABA": "CEDEAR",
    "AMD": "CEDEAR",
    "INTC": "CEDEAR",
    "GOLD": "CEDEAR",
    "VALE": "CEDEAR",
    "PBR": "CEDEAR",
    "DESP": "CEDEAR",
    "BIOX": "CEDEAR",
    "VIST": "CEDEAR",
    "CAAP": "CEDEAR",
    "VZ": "CEDEAR",
    "JNJ": "CEDEAR",
    "T": "CEDEAR",
    "PFE": "CEDEAR",
    "ABBV": "CEDEAR",
    "MRK": "CEDEAR",
    "XOM": "CEDEAR",
    "CVX": "CEDEAR",
    "HD": "CEDEAR",
    "UNH": "CEDEAR",
    "COST": "CEDEAR",
    "CRM": "CEDEAR",
    "ADBE": "CEDEAR",
    "CSCO": "CEDEAR",
    "TXN": "CEDEAR",
    "QCOM": "CEDEAR",
    "AVGO": "CEDEAR",
    "ORCL": "CEDEAR",
    "IBM": "CEDEAR",
    "GE": "CEDEAR",
    "CAT": "CEDEAR",
    "DE": "CEDEAR",
    "MMM": "CEDEAR",
    "NKE": "CEDEAR",
    "MCD": "CEDEAR",
    "SBUX": "CEDEAR",
    "LMT": "CEDEAR",
    "RTX": "CEDEAR",
    "HON": "CEDEAR",
    "UPS": "CEDEAR",
    "AXP": "CEDEAR",
    "GS": "CEDEAR",
    "MS": "CEDEAR",
    "C": "CEDEAR",
    "WFC": "CEDEAR",
    "BRK.B": "CEDEAR",
    "SNAP": "CEDEAR",
    "UBER": "CEDEAR",
    "SQ": "CEDEAR",
    "SHOP": "CEDEAR",
    "SE": "CEDEAR",
    "SPOT": "CEDEAR",
    "ZM": "CEDEAR",
    "DOCU": "CEDEAR",
    "ABNB": "CEDEAR",
    "COIN": "CEDEAR",
    "PYPL": "CEDEAR",
    "SQ": "CEDEAR",
    "X": "CEDEAR",
    "FCX": "CEDEAR",
    "NEM": "CEDEAR",
    "BIDU": "CEDEAR",
    "JD": "CEDEAR",
    "NIO": "CEDEAR",
    "LI": "CEDEAR",
    "XPEV": "CEDEAR",
    "ERJ": "CEDEAR",
    # ETFs
    "SPY": "ETF",
    "QQQ": "ETF",
    "EEM": "ETF",
    "IWM": "ETF",
    "DIA": "ETF",
    "XLF": "ETF",
    "XLE": "ETF",
    "GLD": "ETF",
    "BND": "ETF",
    "VTI": "ETF",
    "VOO": "ETF",
    "ARKK": "ETF",
    # Bonos argentinos
    "AL30": "BONO",
    "AL35": "BONO",
    "GD30": "BONO",
    "GD35": "BONO",
    "GD38": "BONO",
    "GD41": "BONO",
    "GD46": "BONO",
    "AE38": "BONO",
    "AL29": "BONO",
    "AL41": "BONO",
    # Acciones argentinas
    "GGAL": "ACCIONES",
    "YPF": "ACCIONES",
    "PAMP": "ACCIONES",
    "BMA": "ACCIONES",
    "SUPV": "ACCIONES",
    "CEPU": "ACCIONES",
    "EDN": "ACCIONES",
    "TGSU2": "ACCIONES",
    "TXAR": "ACCIONES",
    "ALUA": "ACCIONES",
    "CRES": "ACCIONES",
    "LOMA": "ACCIONES",
    "MIRG": "ACCIONES",
    "TRAN": "ACCIONES",
    "COME": "ACCIONES",
    "BYMA": "ACCIONES",
    "IRSA": "ACCIONES",
    "VALO": "ACCIONES",
    # ONs
    "YMCIO": "ON",
    "YCA6O": "ON",
    "MRCAO": "ON",
    "CS38O": "ON",
    "TLCHO": "ON",
    "RCCJO": "ON",
    "MTCGO": "ON",
    "GNCXO": "ON",
    "SNS9O": "ON",
    "PGR7O": "ON",
    "MJ27O": "ON",
    "CP17O": "ON",
    # Letras / Títulos públicos
    "S31M5": "TitulosPublicos",
    "S14F5": "TitulosPublicos",
    "S31J5": "TitulosPublicos",
    "S29G5": "TitulosPublicos",
    "X18F5": "TitulosPublicos",
    "X20F5": "TitulosPublicos",
    "LECAP": "TitulosPublicos",
    "LECER": "TitulosPublicos",
    "BONCER": "TitulosPublicos",
    "TX26": "TitulosPublicos",
    "T2X5": "TitulosPublicos",
    "DICP": "TitulosPublicos",
    "PARP": "TitulosPublicos",
    "CUAP": "TitulosPublicos",
    # FCIs
    "CFINRA": "FondoComundeInversion",
    "FIMA": "FondoComundeInversion",
    "COFIN": "FondoComundeInversion",
}

# Heuristic patterns: suffix/prefix -> asset_type
# NOTE: ("D", "CEDEAR") was removed — too aggressive, catches letras like XN6D, DF6D.
# Real CEDEARs are covered by KNOWN_ASSET_TYPES or InstrumentCatalog.
_SUFFIX_RULES: list[tuple[str, str]] = [
    ("O", "ON"),        # ONs typically end in O
]


def resolve_asset_type(
    symbol: str,
    positions: list[dict] | None = None,
    extra_map: dict[str, str] | None = None,
    catalog_map: dict[str, str] | None = None,
) -> tuple[str, str, str]:
    """Resolve the asset_type for a symbol.

    Returns (asset_type, status, source) where:
    - status: "known_valid" | "unknown" | "unsupported"
    - source: which layer resolved it:
        "holdings", "extra_map", "catalog", "known_assets", "heuristic", "unknown"

    Resolution order:
    1. Positions (holdings) — direct lookup
    2. extra_map (caller-provided overrides)
    3. catalog_map (from InstrumentCatalog)
    4. KNOWN_ASSET_TYPES (static map)
    5. Heuristic inference
    6. Fallback to DESCONOCIDO / unknown
    """
    if not symbol:
        return "DESCONOCIDO", "unknown", "unknown"

    # 1. From positions
    if positions:
        for p in positions:
            if p.get("symbol") == symbol:
                at = p.get("asset_type") or p.get("instrument_type") or ""
                if at and at != "DESCONOCIDO":
                    status = "known_valid" if at in VALID_ASSET_TYPES else "unsupported"
                    return at, status, "holdings"

    # 2. From extra_map (caller can pass additional mappings)
    if extra_map and symbol in extra_map:
        at = extra_map[symbol]
        status = "known_valid" if at in VALID_ASSET_TYPES else "unsupported"
        return at, status, "extra_map"

    # 3. From InstrumentCatalog (dynamic discovery)
    if catalog_map and symbol in catalog_map:
        at = catalog_map[symbol]
        if at and at != "DESCONOCIDO":
            status = "known_valid" if at in VALID_ASSET_TYPES else "unsupported"
            return at, status, "catalog"

    # 4. From static known map
    if symbol in KNOWN_ASSET_TYPES:
        at = KNOWN_ASSET_TYPES[symbol]
        status = "known_valid" if at in VALID_ASSET_TYPES else "unsupported"
        return at, status, "known_assets"

    # 5. Heuristic: check suffix patterns
    upper = symbol.upper()
    for suffix, at in _SUFFIX_RULES:
        if len(upper) > 2 and upper.endswith(suffix):
            if at in VALID_ASSET_TYPES:
                return at, "known_valid", "heuristic"

    # 6. Unknown
    return "DESCONOCIDO", "unknown", "unknown"


def build_catalog_asset_type_map(db) -> dict[str, str]:
    """Build a symbol->asset_type map from the InstrumentCatalog table.

    This avoids coupling resolve_asset_type to SQLAlchemy directly.
    Import lazily to avoid circular imports.
    """
    from app.models.models import InstrumentCatalog

    results = (
        db.query(InstrumentCatalog.symbol, InstrumentCatalog.asset_type)
        .filter(InstrumentCatalog.is_active == True)  # noqa: E712
        .all()
    )
    return {r[0]: r[1] for r in results if r[1] and r[1] != "DESCONOCIDO"}


def build_asset_type_map(
    positions: list[dict],
    extra_symbols: set[str] | None = None,
) -> dict[str, tuple[str, str, str]]:
    """Build a complete asset_type map for positions + extra symbols.

    Returns dict of symbol -> (asset_type, asset_type_status, resolution_source).
    """
    result: dict[str, tuple[str, str, str]] = {}

    # All position symbols
    for p in positions:
        sym = p.get("symbol")
        if sym:
            result[sym] = resolve_asset_type(sym, positions=positions)

    # Extra symbols (watchlist, universe, news, etc.)
    if extra_symbols:
        for sym in extra_symbols:
            if sym not in result:
                result[sym] = resolve_asset_type(sym, positions=positions)

    return result
