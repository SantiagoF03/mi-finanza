"""Cluster-aware signal scoring, market confirmation, and signal classification.

This module sits between news ingestion and the recommendation engine.
It enriches news items (individual or cluster-sourced) with:
- signal_score: weighted quality score considering multi-source, relevance, etc.
- signal_class: holding_risk | holding_opportunity | external_opportunity | observed_candidate
- market_confirmation: confirmed | unconfirmed | contradicted

All functions are pure (no DB access) and take dicts as input.
generate_recommendation() calls score_and_classify_news() as a pre-processing step.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Parte A — Cluster-aware signal scoring
# ---------------------------------------------------------------------------

def score_news_item(item: dict, held_symbols: set, allowed_symbols: set) -> float:
    """Compute a signal_score for a single news/cluster item.

    Base: item's pre_score (0-1).
    Boosted by cluster signals when present (source_count, item_count, etc).
    Boosted by relevance to holdings/allowed universe.
    Penalized for weak signals (single source, low relevance).

    Returns a float 0-1 (capped).
    """
    base = float(item.get("pre_score", 0) or 0)
    score = base

    # --- Cluster-aware boosts (only present in cluster-sourced items) ---
    source_count = item.get("source_count", 1)
    item_count = item.get("item_count", 1)
    relevance = float(item.get("relevance_score", 0) or 0)

    # Multi-source boost: 2 sources +0.08, 3+ +0.15
    if source_count >= 3:
        score += 0.15
    elif source_count >= 2:
        score += 0.08

    # Multi-item boost: events covered by many articles are more significant
    if item_count >= 5:
        score += 0.10
    elif item_count >= 3:
        score += 0.05

    # High cluster relevance boost
    if relevance >= 0.7:
        score += 0.08
    elif relevance >= 0.5:
        score += 0.04

    # --- Holdings/universe relevance ---
    related = set(item.get("related_assets", []))

    if related & held_symbols:
        score += 0.12  # Directly affects what I own
    if related & allowed_symbols:
        score += 0.05  # Affects investable universe

    # Explicit flags from cluster
    if item.get("affects_holdings"):
        score += 0.06
    if item.get("affects_watchlist"):
        score += 0.03

    # --- Penalties ---
    # Single source, low confidence
    if source_count <= 1 and base < 0.4:
        score -= 0.10

    return round(min(max(score, 0.0), 1.0), 3)


# ---------------------------------------------------------------------------
# Parte B — Cheap market confirmation (no API, no LLM)
# ---------------------------------------------------------------------------

def compute_market_confirmation(
    item: dict,
    positions: list[dict],
) -> dict:
    """Compute cheap market confirmation using available portfolio data.

    Uses pnl_pct from holdings as a proxy for recent momentum.
    Returns a dict with confirmation_status and detail.

    Logic:
    - Negative event + negative pnl → confirmed
    - Negative event + positive pnl → contradicted (market disagrees)
    - Positive event + positive pnl → confirmed
    - Positive event + negative pnl → contradicted
    - Neutro/no overlap → unconfirmed
    """
    impact = item.get("impact", "neutro")
    related = set(item.get("related_assets", []))

    if not related:
        return {"status": "unconfirmed", "detail": "Sin activos relacionados en portfolio"}

    # Find matching positions
    position_map = {p["symbol"]: p for p in positions if p.get("symbol")}
    matched = [(sym, position_map[sym]) for sym in related if sym in position_map]

    if not matched:
        return {"status": "unconfirmed", "detail": "Activos no están en holdings actuales"}

    # Average pnl_pct of matched positions
    pnl_values = [p.get("pnl_pct", 0) for _, p in matched]
    avg_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0
    matched_symbols = [sym for sym, _ in matched]

    # Strong threshold: ±3% is meaningful movement
    significant = abs(avg_pnl) >= 0.03
    direction = "positive" if avg_pnl > 0 else "negative" if avg_pnl < 0 else "flat"

    if impact == "negativo":
        if direction == "negative" and significant:
            return {
                "status": "confirmed",
                "detail": f"Evento negativo confirmado: {', '.join(matched_symbols)} con PnL {round(avg_pnl*100,1)}%",
                "avg_pnl_pct": round(avg_pnl, 4),
            }
        elif direction == "positive" and significant:
            return {
                "status": "contradicted",
                "detail": f"Evento negativo pero mercado positivo: {', '.join(matched_symbols)} con PnL +{round(avg_pnl*100,1)}%",
                "avg_pnl_pct": round(avg_pnl, 4),
            }
    elif impact == "positivo":
        if direction == "positive" and significant:
            return {
                "status": "confirmed",
                "detail": f"Evento positivo confirmado: {', '.join(matched_symbols)} con PnL +{round(avg_pnl*100,1)}%",
                "avg_pnl_pct": round(avg_pnl, 4),
            }
        elif direction == "negative" and significant:
            return {
                "status": "contradicted",
                "detail": f"Evento positivo pero mercado negativo: {', '.join(matched_symbols)} con PnL {round(avg_pnl*100,1)}%",
                "avg_pnl_pct": round(avg_pnl, 4),
            }

    return {
        "status": "unconfirmed",
        "detail": f"Sin confirmación clara: PnL {round(avg_pnl*100,1)}% en {', '.join(matched_symbols)}",
        "avg_pnl_pct": round(avg_pnl, 4),
    }


# ---------------------------------------------------------------------------
# Parte C — Signal classification and ranking
# ---------------------------------------------------------------------------

_SIGNAL_CLASS_PRIORITY = {
    "holding_risk": 0,
    "holding_opportunity": 1,
    "external_opportunity": 2,
    "observed_candidate": 3,
}


def classify_signal(
    item: dict,
    held_symbols: set,
    main_allowed: set,
    watchlist: set,
    universe: set,
) -> str:
    """Classify a news/cluster item into one of four signal classes.

    Priority:
    1. holding_risk: negative impact + affects holdings
    2. holding_opportunity: positive impact + affects holdings
    3. external_opportunity: affects main_allowed/watchlist/universe (investable, not held)
    4. observed_candidate: everything else (catalog-only, untracked)
    """
    impact = item.get("impact", "neutro")
    related = set(item.get("related_assets", []))

    touches_holdings = bool(related & held_symbols) or item.get("affects_holdings", False)
    touches_allowed = bool(related & (main_allowed | watchlist | universe))

    if touches_holdings:
        if impact == "negativo":
            return "holding_risk"
        return "holding_opportunity"

    if touches_allowed:
        return "external_opportunity"

    return "observed_candidate"


def score_and_classify_news(
    news: list[dict],
    positions: list[dict],
    allowed_assets: dict,
) -> list[dict]:
    """Main entry point: score, classify, and rank all news/cluster items.

    Each item gets enriched with:
    - signal_score (float 0-1)
    - signal_class (str)
    - market_confirmation (dict)

    Returns items sorted by: signal_class priority (risk first), then signal_score desc.
    Items are NOT filtered — all are returned, just ranked better.
    """
    held_symbols = set(allowed_assets.get("holdings", set()))
    main_allowed = set(allowed_assets.get("main_allowed", set()))
    watchlist = set(allowed_assets.get("watchlist", set()))
    universe = set(allowed_assets.get("universe", set()))
    all_allowed = main_allowed | watchlist | universe

    scored = []
    for item in news:
        enriched = dict(item)  # shallow copy — don't mutate originals

        enriched["signal_score"] = score_news_item(item, held_symbols, all_allowed)
        enriched["signal_class"] = classify_signal(item, held_symbols, main_allowed, watchlist, universe)
        enriched["market_confirmation"] = compute_market_confirmation(item, positions)

        scored.append(enriched)

    # Sort: class priority first (holding_risk=0 first), then score descending
    scored.sort(key=lambda x: (
        _SIGNAL_CLASS_PRIORITY.get(x["signal_class"], 99),
        -x["signal_score"],
    ))

    return scored
