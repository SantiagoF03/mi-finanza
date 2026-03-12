"""Investor profile presets for dynamic target weights.

Instead of hardcoding AAPL/MSFT/SPY as target weights, we define
bucket-level targets by investor profile, then distribute them
across the actual holdings based on their asset_type.

Buckets:
- cash: liquid cash
- renta_fija: BONO, ON, TitulosPublicos
- equity_exterior: CEDEAR, ETF
- equity_local: ACCIONES
- fci: FondoComundeInversion
- otros: anything else
"""

from __future__ import annotations

# Profile presets: bucket -> target weight
PROFILE_PRESETS: dict[str, dict[str, float]] = {
    "conservative": {
        "cash": 0.25,
        "renta_fija": 0.40,
        "equity_exterior": 0.15,
        "equity_local": 0.10,
        "fci": 0.05,
        "otros": 0.05,
    },
    "conservador": {
        "cash": 0.25,
        "renta_fija": 0.40,
        "equity_exterior": 0.15,
        "equity_local": 0.10,
        "fci": 0.05,
        "otros": 0.05,
    },
    "moderate": {
        "cash": 0.15,
        "renta_fija": 0.25,
        "equity_exterior": 0.30,
        "equity_local": 0.15,
        "fci": 0.10,
        "otros": 0.05,
    },
    "moderado": {
        "cash": 0.15,
        "renta_fija": 0.25,
        "equity_exterior": 0.30,
        "equity_local": 0.15,
        "fci": 0.10,
        "otros": 0.05,
    },
    "moderate_aggressive": {
        "cash": 0.10,
        "renta_fija": 0.15,
        "equity_exterior": 0.40,
        "equity_local": 0.20,
        "fci": 0.10,
        "otros": 0.05,
    },
    "aggressive": {
        "cash": 0.05,
        "renta_fija": 0.10,
        "equity_exterior": 0.45,
        "equity_local": 0.25,
        "fci": 0.10,
        "otros": 0.05,
    },
    "agresivo": {
        "cash": 0.05,
        "renta_fija": 0.10,
        "equity_exterior": 0.45,
        "equity_local": 0.25,
        "fci": 0.10,
        "otros": 0.05,
    },
}

# Canonical profile name mapping (aliases → canonical)
PROFILE_ALIASES: dict[str, str] = {
    "conservador": "conservative",
    "moderado": "moderate",
    "moderate_aggressive": "moderate_aggressive",
    "agresivo": "aggressive",
    "conservative": "conservative",
    "moderate": "moderate",
    "aggressive": "aggressive",
}

# Human-readable labels for rationale
PROFILE_LABELS: dict[str, str] = {
    "conservative": "conservador",
    "moderate": "moderado",
    "moderate_aggressive": "moderado-agresivo",
    "aggressive": "agresivo",
}

# Profile-specific thresholds
PROFILE_THRESHOLDS: dict[str, dict] = {
    "conservative": {
        "max_single_asset_weight": 0.30,
        "max_equity_band": 0.35,
        "max_us_equity_concentration": 0.25,
        "concentration_alert_threshold": 0.30,
    },
    "moderate": {
        "max_single_asset_weight": 0.35,
        "max_equity_band": 0.55,
        "max_us_equity_concentration": 0.40,
        "concentration_alert_threshold": 0.35,
    },
    "moderate_aggressive": {
        "max_single_asset_weight": 0.40,
        "max_equity_band": 0.70,
        "max_us_equity_concentration": 0.50,
        "concentration_alert_threshold": 0.40,
    },
    "aggressive": {
        "max_single_asset_weight": 0.45,
        "max_equity_band": 0.80,
        "max_us_equity_concentration": 0.60,
        "concentration_alert_threshold": 0.45,
    },
}


def resolve_profile(profile: str) -> str:
    """Resolve a profile name to its canonical form."""
    return PROFILE_ALIASES.get(profile, profile)


def get_profile_label(profile: str) -> str:
    """Get human-readable label for a profile."""
    canonical = resolve_profile(profile)
    return PROFILE_LABELS.get(canonical, canonical)


def get_profile_thresholds(profile: str) -> dict:
    """Get profile-specific thresholds."""
    canonical = resolve_profile(profile)
    return PROFILE_THRESHOLDS.get(canonical, PROFILE_THRESHOLDS["moderate"])

# Map asset_type -> bucket
ASSET_TYPE_TO_BUCKET: dict[str, str] = {
    "BONO": "renta_fija",
    "ON": "renta_fija",
    "TitulosPublicos": "renta_fija",
    "CEDEAR": "equity_exterior",
    "ETF": "equity_exterior",
    "ACCIONES": "equity_local",
    "FondoComundeInversion": "fci",
}


def get_bucket(asset_type: str) -> str:
    """Map an asset type to its bucket. Unknown types go to 'otros'."""
    return ASSET_TYPE_TO_BUCKET.get(asset_type, "otros")


def build_target_weights(positions: list[dict], profile: str = "moderado") -> dict[str, float]:
    """Build per-symbol target weights from profile preset + actual holdings.

    Strategy:
    1. Look up bucket targets from the profile preset
    2. Group held symbols by bucket
    3. Distribute each bucket's target equally among its symbols
    4. CASH always gets its own bucket weight directly
    5. Unallocated bucket weight (no holdings in that bucket) is
       redistributed to CASH so weights always sum to 1.0

    Returns dict like {"AAPL": 0.15, "MSFT": 0.15, "CASH": 0.15, ...}
    """
    preset = PROFILE_PRESETS.get(profile, PROFILE_PRESETS["moderado"])

    # Group symbols by bucket
    buckets: dict[str, list[str]] = {}
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        asset_type = p.get("asset_type") or p.get("instrument_type") or "DESCONOCIDO"
        bucket = get_bucket(asset_type)
        buckets.setdefault(bucket, []).append(sym)

    weights: dict[str, float] = {}
    unallocated = 0.0

    # Distribute bucket target equally among symbols in that bucket
    for bucket, target in preset.items():
        if bucket == "cash":
            weights["CASH"] = target
            continue
        symbols = buckets.get(bucket, [])
        if symbols:
            per_symbol = target / len(symbols)
            for sym in symbols:
                weights[sym] = round(weights.get(sym, 0) + per_symbol, 4)
        else:
            # No holdings in this bucket — collect unallocated weight
            unallocated += target

    # Redistribute unallocated weight to CASH
    if unallocated > 0:
        weights["CASH"] = round(weights.get("CASH", 0) + unallocated, 4)

    # Ensure CASH is always present
    if "CASH" not in weights:
        weights["CASH"] = preset.get("cash", 0.15)

    return weights
