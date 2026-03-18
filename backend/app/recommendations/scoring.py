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
    catalog_prices: dict | None = None,
) -> dict:
    """Compute market confirmation using portfolio data + catalog prices.

    Two data sources (tried in order):
    1. Holdings: pnl_pct from portfolio positions (existing behavior)
    2. Catalog: variacion_pct from InstrumentCatalog (IOL daily % change)

    For holdings, uses pnl_pct (accumulated P&L).
    For non-holdings, uses variacion_pct (daily market change) from catalog.
    This allows market confirmation for external opportunities and observed candidates.

    Logic (same for both sources):
    - Negative event + negative change → confirmed
    - Negative event + positive change → contradicted
    - Positive event + positive change → confirmed
    - Positive event + negative change → contradicted
    - Neutro/no data → unconfirmed
    """
    impact = item.get("impact", "neutro")
    related = set(item.get("related_assets", []))

    if not related:
        return {"status": "unconfirmed", "detail": "Sin activos relacionados"}

    # --- Try holdings first (pnl_pct, existing behavior) ---
    position_map = {p["symbol"]: p for p in positions if p.get("symbol")}
    matched_holdings = [(sym, position_map[sym]) for sym in related if sym in position_map]

    if matched_holdings:
        pnl_values = [p.get("pnl_pct", 0) for _, p in matched_holdings]
        avg_pnl = sum(pnl_values) / len(pnl_values) if pnl_values else 0
        matched_symbols = [sym for sym, _ in matched_holdings]

        result = _evaluate_confirmation(
            impact, avg_pnl, matched_symbols, source="holdings", threshold=0.03,
        )
        if result:
            return result

    # --- Fallback: catalog prices for non-holdings ---
    if catalog_prices:
        matched_catalog = [
            (sym, catalog_prices[sym])
            for sym in related
            if sym in catalog_prices and sym not in position_map
        ]
        if matched_catalog:
            var_values = [
                p.get("variacion_pct", 0) or 0 for _, p in matched_catalog
            ]
            # variacion_pct from IOL is already in percentage (e.g., 3.5 = 3.5%)
            # Normalize to fraction for consistent threshold logic
            avg_var = sum(var_values) / len(var_values) if var_values else 0
            avg_var_frac = avg_var / 100.0  # 3.5 → 0.035

            matched_symbols = [sym for sym, _ in matched_catalog]

            result = _evaluate_confirmation(
                impact, avg_var_frac, matched_symbols, source="catalog", threshold=0.02,
            )
            if result:
                return result

            return {
                "status": "unconfirmed",
                "detail": f"Sin confirmación clara: variación {round(avg_var, 1)}% en {', '.join(matched_symbols)}",
                "variacion_pct": round(avg_var, 2),
                "source": "catalog",
            }

    # No data at all
    if not matched_holdings:
        return {"status": "unconfirmed", "detail": "Activos no están en holdings ni en catálogo"}

    # Holdings matched but inconclusive
    avg_pnl = sum(p.get("pnl_pct", 0) for _, p in matched_holdings) / len(matched_holdings)
    matched_symbols = [sym for sym, _ in matched_holdings]
    return {
        "status": "unconfirmed",
        "detail": f"Sin confirmación clara: PnL {round(avg_pnl*100,1)}% en {', '.join(matched_symbols)}",
        "avg_pnl_pct": round(avg_pnl, 4),
        "source": "holdings",
    }


def _evaluate_confirmation(
    impact: str,
    change_value: float,
    symbols: list[str],
    source: str,
    threshold: float,
) -> dict | None:
    """Shared confirmation logic for holdings (pnl) and catalog (variacion).

    Returns a result dict if confirmed/contradicted, None if inconclusive.
    change_value is a fraction (0.03 = 3%).
    """
    significant = abs(change_value) >= threshold
    direction = "positive" if change_value > 0 else "negative" if change_value < 0 else "flat"

    label = "PnL" if source == "holdings" else "variación"
    pct_display = round(change_value * 100, 1)
    sign = "+" if change_value > 0 else ""
    symbols_str = ", ".join(symbols)

    if impact == "negativo":
        if direction == "negative" and significant:
            return {
                "status": "confirmed",
                "detail": f"Evento negativo confirmado: {symbols_str} con {label} {pct_display}%",
                "avg_pnl_pct": round(change_value, 4),
                "source": source,
            }
        elif direction == "positive" and significant:
            return {
                "status": "contradicted",
                "detail": f"Evento negativo pero mercado positivo: {symbols_str} con {label} {sign}{pct_display}%",
                "avg_pnl_pct": round(change_value, 4),
                "source": source,
            }
    elif impact == "positivo":
        if direction == "positive" and significant:
            return {
                "status": "confirmed",
                "detail": f"Evento positivo confirmado: {symbols_str} con {label} {sign}{pct_display}%",
                "avg_pnl_pct": round(change_value, 4),
                "source": source,
            }
        elif direction == "negative" and significant:
            return {
                "status": "contradicted",
                "detail": f"Evento positivo pero mercado negativo: {symbols_str} con {label} {pct_display}%",
                "avg_pnl_pct": round(change_value, 4),
                "source": source,
            }

    return None


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
    universe_curated: set,
) -> str:
    """Classify a news/cluster item into one of four signal classes.

    Priority:
    1. holding_risk: negative impact + affects holdings
    2. holding_opportunity: positive impact + affects holdings
    3. external_opportunity: affects main_allowed/watchlist/universe_curated (NOT catalog_dynamic)
    4. observed_candidate: everything else (catalog-only, untracked)

    NOTE: catalog_dynamic is intentionally excluded from the universe check.
    Catalog-only items default to observed_candidate and require explicit
    promotion via promote_catalog_candidate() to become external_opportunity.
    """
    impact = item.get("impact", "neutro")
    related = set(item.get("related_assets", []))

    touches_holdings = bool(related & held_symbols) or item.get("affects_holdings", False)
    touches_allowed = bool(related & (main_allowed | watchlist | universe_curated))

    if touches_holdings:
        if impact == "negativo":
            return "holding_risk"
        return "holding_opportunity"

    if touches_allowed:
        return "external_opportunity"

    return "observed_candidate"


# ---------------------------------------------------------------------------
# Parte C.2 — Catalog promotion: observed_candidate → external_opportunity
# ---------------------------------------------------------------------------

# Minimum thresholds for catalog_dynamic promotion
_PROMO_MIN_SIGNAL_SCORE = 0.55
_PROMO_MIN_SOURCE_COUNT = 2


def promote_catalog_candidate(item: dict, catalog_dynamic: set) -> bool:
    """Determine if an observed_candidate from catalog_dynamic should be promoted
    to external_opportunity.

    Promotion requires STRONG evidence — multiple criteria must be met:
    1. Item relates to a catalog_dynamic symbol (mandatory)
    2. signal_score >= 0.55 (above noise threshold)
    3. market_confirmation != "contradicted" (hard block)
    4. At least ONE of:
       a. source_count >= 2 (multi-source corroboration)
       b. relevance_score >= 0.6 (high cluster relevance)
       c. external_opportunity_candidate flag from cluster triage
       d. llm_candidate flag from cluster triage
       e. market_confirmation status == "confirmed"

    Returns True if the item should be promoted.
    """
    if item.get("signal_class") != "observed_candidate":
        return False

    related = set(item.get("related_assets", []))
    if not (related & catalog_dynamic):
        return False  # Not from catalog_dynamic — don't touch

    score = item.get("signal_score", 0)
    if score < _PROMO_MIN_SIGNAL_SCORE:
        return False

    # Hard block: contradicted by market → never promote
    mkt_conf = (item.get("market_confirmation") or {}).get("status", "")
    if mkt_conf == "contradicted":
        return False

    # At least one strong evidence criterion
    source_count = item.get("source_count", 1)
    relevance = float(item.get("relevance_score", 0) or 0)

    if source_count >= _PROMO_MIN_SOURCE_COUNT:
        return True
    if relevance >= 0.6:
        return True
    if item.get("external_opportunity_candidate"):
        return True
    if item.get("llm_candidate"):
        return True
    if mkt_conf == "confirmed":
        return True

    return False


# ---------------------------------------------------------------------------
# Parte C.3 — Effective score: signal_score adjusted by market confirmation
# ---------------------------------------------------------------------------

# Confirmation adjustments to signal_score for ranking purposes
_CONFIRMATION_BOOST = 0.10   # confirmed → +0.10
_CONTRADICTION_PENALTY = 0.15  # contradicted → -0.15
_SUPPRESSION_THRESHOLD = 0.45  # contradicted + effective_score < this → suppressed


def compute_effective_score(item: dict) -> float:
    """Compute an effective_score that integrates signal_score + market confirmation.

    confirmed → boost, contradicted → penalty, unconfirmed → unchanged.
    Used for final ranking. The original signal_score is preserved unchanged.
    """
    base = item.get("signal_score", 0)
    conf_status = (item.get("market_confirmation") or {}).get("status", "")

    if conf_status == "confirmed":
        return round(min(base + _CONFIRMATION_BOOST, 1.0), 3)
    elif conf_status == "contradicted":
        return round(max(base - _CONTRADICTION_PENALTY, 0.0), 3)
    return round(base, 3)


def score_and_classify_news(
    news: list[dict],
    positions: list[dict],
    allowed_assets: dict,
    catalog_prices: dict | None = None,
) -> list[dict]:
    """Main entry point: score, classify, and rank all news/cluster items.

    Each item gets enriched with:
    - signal_score (float 0-1, raw quality score)
    - effective_score (float 0-1, signal_score adjusted by market confirmation)
    - signal_class (str)
    - market_confirmation (dict)
    - promoted_from_observed (bool, only if promoted)
    - suppressed_by_contradiction (bool, if contradicted + weak effective_score)

    Classification uses universe_curated (NOT universe which includes catalog_dynamic).
    catalog_dynamic items default to observed_candidate and are promoted only if
    they meet strong evidence criteria via promote_catalog_candidate().

    catalog_prices: optional {symbol: {"last_price": float, "variacion_pct": float}}
    from InstrumentCatalog — enables market confirmation for non-holdings.

    Returns items sorted by: signal_class priority (risk first), then effective_score desc.
    Items are NOT filtered — all are returned, just ranked better.
    """
    held_symbols = set(allowed_assets.get("holdings", set()))
    main_allowed = set(allowed_assets.get("main_allowed", set()))
    watchlist = set(allowed_assets.get("watchlist", set()))
    universe_curated = set(allowed_assets.get("universe_curated", set()))
    catalog_dynamic = set(allowed_assets.get("catalog_dynamic", set()))
    # all_allowed still includes catalog_dynamic for scoring boosts (not classification)
    all_allowed = main_allowed | watchlist | universe_curated | catalog_dynamic

    scored = []
    for item in news:
        enriched = dict(item)  # shallow copy — don't mutate originals

        enriched["signal_score"] = score_news_item(item, held_symbols, all_allowed)
        enriched["signal_class"] = classify_signal(
            item, held_symbols, main_allowed, watchlist, universe_curated,
        )
        enriched["market_confirmation"] = compute_market_confirmation(
            item, positions, catalog_prices=catalog_prices,
        )

        # Effective score: signal_score adjusted by market confirmation
        enriched["effective_score"] = compute_effective_score(enriched)

        # Promotion: catalog_dynamic observed_candidate → external_opportunity
        # (uses market_confirmation: contradicted blocks, confirmed helps)
        if promote_catalog_candidate(enriched, catalog_dynamic):
            enriched["signal_class"] = "external_opportunity"
            enriched["promoted_from_observed"] = True

        # Suppression: contradicted + weak effective_score → flag for observability
        conf_status = (enriched["market_confirmation"] or {}).get("status", "")
        if (conf_status == "contradicted"
                and enriched["effective_score"] < _SUPPRESSION_THRESHOLD
                and enriched["signal_class"] in ("external_opportunity", "observed_candidate")):
            enriched["suppressed_by_contradiction"] = True

        scored.append(enriched)

    # Sort: class priority first (holding_risk=0 first), then effective_score desc
    scored.sort(key=lambda x: (
        _SIGNAL_CLASS_PRIORITY.get(x["signal_class"], 99),
        -x["effective_score"],
    ))

    return scored


# ---------------------------------------------------------------------------
# Parte D — LLM input curation
# ---------------------------------------------------------------------------

# Minimum effective_score to send an item to the LLM (filters noise)
_LLM_MIN_EFFECTIVE_SCORE = 0.30
_LLM_MAX_ITEMS = 15


def curate_llm_input(
    scored_news: list[dict],
    max_items: int = _LLM_MAX_ITEMS,
) -> tuple[list[dict], dict]:
    """Curate scored news for LLM input: exclude noise, rank by quality.

    Exclusion policy:
    1. suppressed_by_contradiction → excluded (contradicted + weak)
    2. effective_score < 0.30 → excluded (too weak for LLM context)
    3. observed_candidate without promotion → excluded (unvetted noise)

    Ranking (items that pass):
    1. signal_class priority (holding_risk first)
    2. effective_score descending
    3. source_count descending (tiebreaker)

    Returns (curated_items, llm_input_meta).
    curated_items: list of enriched news dicts, capped at max_items.
    llm_input_meta: observability dict for metadata_json.
    """
    excluded_suppressed = 0
    excluded_weak = 0
    excluded_observed = 0
    eligible = []

    for item in scored_news:
        # Exclusion 1: suppressed by contradiction
        if item.get("suppressed_by_contradiction"):
            excluded_suppressed += 1
            continue

        # Exclusion 2: weak effective_score
        if item.get("effective_score", 0) < _LLM_MIN_EFFECTIVE_SCORE:
            excluded_weak += 1
            continue

        # Exclusion 3: observed_candidate not promoted
        if (item.get("signal_class") == "observed_candidate"
                and not item.get("promoted_from_observed")):
            excluded_observed += 1
            continue

        eligible.append(item)

    # Rank: class priority → effective_score desc → source_count desc
    eligible.sort(key=lambda x: (
        _SIGNAL_CLASS_PRIORITY.get(x.get("signal_class", ""), 99),
        -x.get("effective_score", 0),
        -x.get("source_count", 1),
    ))

    curated = eligible[:max_items]

    # Observability: breakdown of what was sent by signal_class
    sent_classes: dict[str, int] = {}
    for item in curated:
        cls = item.get("signal_class", "unknown")
        sent_classes[cls] = sent_classes.get(cls, 0) + 1

    llm_input_meta = {
        "total_scored": len(scored_news),
        "excluded_suppressed": excluded_suppressed,
        "excluded_weak": excluded_weak,
        "excluded_observed": excluded_observed,
        "eligible_count": len(eligible),
        "sent_count": len(curated),
        "sent_classes": sent_classes,
        "max_items": max_items,
    }

    return curated, llm_input_meta


# ---------------------------------------------------------------------------
# Parte E — Fresh quote shortlist and refinement
# ---------------------------------------------------------------------------

_SHORTLIST_MAX_SYMBOLS = 8


def build_shortlist(
    scored_news: list[dict],
    holdings: set,
    max_symbols: int = _SHORTLIST_MAX_SYMBOLS,
) -> tuple[list[str], dict]:
    """Build a small shortlist of symbols that deserve fresh quotes.

    Priority (in order, deduplicated):
    1. Holdings mentioned in holding_risk signals (highest urgency)
    2. Holdings mentioned in holding_opportunity signals
    3. Symbols from top external_opportunities (by effective_score)
    4. Symbols from promoted_from_observed items

    Returns (symbols, shortlist_meta).
    """
    seen: set[str] = set()
    ordered: list[str] = []

    def _add_symbols(item: dict, filter_set: set | None = None):
        for sym in item.get("related_assets", []):
            if sym not in seen and (filter_set is None or sym in filter_set):
                seen.add(sym)
                ordered.append(sym)

    # Pass 1: holding_risk symbols (only holdings)
    for item in scored_news:
        if item.get("signal_class") == "holding_risk":
            _add_symbols(item, holdings)

    # Pass 2: holding_opportunity symbols (only holdings)
    for item in scored_news:
        if item.get("signal_class") == "holding_opportunity":
            _add_symbols(item, holdings)

    # Pass 3: top external_opportunities by effective_score
    externals = sorted(
        [i for i in scored_news if i.get("signal_class") == "external_opportunity"],
        key=lambda x: -x.get("effective_score", 0),
    )
    for item in externals:
        _add_symbols(item)

    # Pass 4: promoted from observed
    for item in scored_news:
        if item.get("promoted_from_observed"):
            _add_symbols(item)

    result = ordered[:max_symbols]

    shortlist_meta = {
        "total_candidates": len(ordered),
        "selected_count": len(result),
        "max_symbols": max_symbols,
        "symbols": result,
    }

    return result, shortlist_meta


def refine_with_fresh_quotes(
    scored_news: list[dict],
    fresh_prices: dict,
    positions: list[dict],
) -> tuple[list[dict], dict]:
    """Re-evaluate market_confirmation for items whose symbols got fresh quotes.

    For each scored item that has related_assets in fresh_prices and whose
    current confirmation source is NOT 'holdings' (pnl_pct takes priority),
    recompute market_confirmation using the fresh variacion_pct.

    Also recomputes effective_score and suppression flag for affected items.

    Returns (updated_scored_news, refinement_meta).
    Items are returned in the same order, with affected items updated in-place copies.
    """
    fresh_symbols = set(fresh_prices.keys())
    if not fresh_symbols:
        return scored_news, {"refined_count": 0, "symbols_used": []}

    refined_count = 0
    symbols_actually_used: set[str] = set()
    result = []

    for item in scored_news:
        related = set(item.get("related_assets", []))
        current_source = (item.get("market_confirmation") or {}).get("source", "")

        # Only refine if: item touches fresh symbols AND current source isn't holdings
        if (related & fresh_symbols) and current_source != "holdings":
            updated = dict(item)  # shallow copy

            # Build a mini catalog_prices with only fresh data for this item's symbols
            fresh_for_item = {
                sym: fresh_prices[sym]
                for sym in related
                if sym in fresh_prices
            }

            # Recompute market_confirmation using fresh data
            # compute_market_confirmation labels catalog-sourced results as "catalog";
            # since we're passing fresh data, relabel to "fresh_quote" for traceability.
            new_conf = compute_market_confirmation(
                updated, positions, catalog_prices=fresh_for_item,
            )

            # Only update if the catalog path produced a result (which is our fresh data)
            if new_conf.get("source") == "catalog":
                new_conf["source"] = "fresh_quote"
                updated["market_confirmation"] = new_conf
                updated["effective_score"] = compute_effective_score(updated)

                # Re-evaluate suppression
                conf_status = (new_conf or {}).get("status", "")
                was_suppressed = updated.get("suppressed_by_contradiction", False)
                should_suppress = (
                    conf_status == "contradicted"
                    and updated["effective_score"] < _SUPPRESSION_THRESHOLD
                    and updated.get("signal_class") in ("external_opportunity", "observed_candidate")
                )

                if should_suppress:
                    updated["suppressed_by_contradiction"] = True
                elif was_suppressed and conf_status != "contradicted":
                    # Fresh data un-contradicted → un-suppress
                    updated.pop("suppressed_by_contradiction", None)

                refined_count += 1
                for sym in related & fresh_symbols:
                    symbols_actually_used.add(sym)
                result.append(updated)
                continue

        result.append(item)

    refinement_meta = {
        "refined_count": refined_count,
        "symbols_used": sorted(symbols_actually_used),
    }

    return result, refinement_meta
