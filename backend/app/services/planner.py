"""Funded Reallocation Planner — dry-run only, NEVER auto-executes.

Generates a proposed_reallocation_plan that combines:
- Available cash (primary funding source)
- Overweighted holdings (secondary: proposed sells to free capital)
- Prioritized external opportunities with valid asset_type
- Investor profile constraints (concentration, equity bands)
- Whitelist / main_allowed safety rail
- Minimum confidence threshold

Output: proposed_reallocation_plan block with:
- funding_sources, sells_proposed[], buys_proposed[]
- residual_cash, constraints_applied[], why_selected[], why_rejected[]

CRITICAL INVARIANTS:
- This module NEVER sends orders to the broker
- This module NEVER imports the execution module
- Output is a proposal for human review only
- Fail-closed: unknown asset_type → excluded from buys_proposed
- Scheduler NEVER calls this module
"""

from __future__ import annotations

import math

from app.core.config import get_settings
from app.market.assets import resolve_asset_type
from app.portfolio.profiles import (
    ASSET_TYPE_TO_BUCKET,
    get_profile_label,
    get_profile_thresholds,
    resolve_profile,
)
from app.recommendations.universe import VALID_ASSET_TYPES


# Minimum confidence to include a buy candidate
MIN_CONFIDENCE_DEFAULT = 0.50


def generate_reallocation_plan(
    snapshot: dict,
    analysis: dict,
    external_opportunities: list[dict],
    allowed_assets: dict,
    catalog_map: dict[str, str] | None = None,
    min_confidence: float = MIN_CONFIDENCE_DEFAULT,
) -> dict:
    """Generate a funded reallocation plan (dry-run).

    This is a PROPOSAL only — no orders are sent.
    The user must approve/reject via the existing flow.

    Args:
        snapshot: portfolio snapshot dict (total_value, cash, positions)
        analysis: portfolio analysis (weights_by_asset, alerts, etc.)
        external_opportunities: from generate_external_candidates output
        allowed_assets: from build_allowed_assets
        catalog_map: symbol->asset_type from InstrumentCatalog
        min_confidence: minimum confidence/priority_score for buy candidates

    Returns:
        proposed_reallocation_plan dict
    """
    settings = get_settings()
    profile = settings.investor_profile_target or settings.investor_profile
    canonical_profile = resolve_profile(profile)
    profile_label = get_profile_label(profile)
    thresholds = get_profile_thresholds(profile)

    positions = snapshot.get("positions", [])
    total_value = snapshot.get("total_value", 0)
    available_cash = snapshot.get("cash", 0)
    main_allowed = allowed_assets.get("main_allowed", set())
    max_single = thresholds.get("max_single_asset_weight", 0.40)
    max_move = settings.max_movement_per_cycle

    constraints_applied = []
    why_selected = []
    why_rejected = []
    sells_proposed = []
    buys_proposed = []

    # -----------------------------------------------------------------------
    # Step 1: Identify overweighted holdings → potential sell candidates
    # -----------------------------------------------------------------------
    weights = analysis.get("weights_by_asset", {})
    overweighted = {}
    for p in positions:
        sym = p.get("symbol", "")
        weight = weights.get(sym, 0)
        if weight > max_single and total_value > 0:
            excess_pct = weight - max_single
            excess_value = excess_pct * total_value
            overweighted[sym] = {
                "current_weight": round(weight, 4),
                "target_max": max_single,
                "excess_pct": round(excess_pct, 4),
                "excess_value": round(excess_value, 2),
                "quantity": p.get("quantity", 0),
                "market_value": p.get("market_value", 0),
                "asset_type": p.get("asset_type", "DESCONOCIDO"),
            }

    constraints_applied.append(f"max_single_asset_weight={max_single}")
    constraints_applied.append(f"profile={profile_label}")
    constraints_applied.append(f"max_movement_per_cycle={max_move}")
    constraints_applied.append(f"min_confidence={min_confidence}")

    # Propose sells for overweighted (capped by max_move)
    freed_from_sells = 0.0
    for sym, info in overweighted.items():
        sell_pct = min(info["excess_pct"], max_move)
        sell_value = sell_pct * total_value
        price_per_unit = (
            info["market_value"] / info["quantity"]
            if info["quantity"] > 0
            else 0
        )
        sell_qty = math.floor(sell_value / price_per_unit) if price_per_unit > 0 else 0

        if sell_qty <= 0:
            continue

        actual_sell_value = sell_qty * price_per_unit
        freed_from_sells += actual_sell_value

        sells_proposed.append({
            "symbol": sym,
            "side": "sell",
            "quantity_proposed": sell_qty,
            "value_proposed": round(actual_sell_value, 2),
            "reason": f"Sobreconcentración: {round(info['current_weight']*100,1)}% > {int(max_single*100)}% máx",
            "asset_type": info["asset_type"],
            "current_weight": info["current_weight"],
            "target_max_weight": max_single,
        })
        why_selected.append(
            f"SELL {sym}: peso actual {round(info['current_weight']*100,1)}% excede {int(max_single*100)}% → "
            f"propuesta reducir {sell_qty} unidades (~${actual_sell_value:.0f})"
        )

    # -----------------------------------------------------------------------
    # Step 2: Calculate total funding available
    # -----------------------------------------------------------------------
    total_funding = available_cash + freed_from_sells
    funding_sources = {
        "available_cash": round(available_cash, 2),
        "freed_from_sells": round(freed_from_sells, 2),
        "total_funding": round(total_funding, 2),
    }

    # -----------------------------------------------------------------------
    # Step 3: Filter and rank buy candidates
    # -----------------------------------------------------------------------
    buy_candidates = []
    held_symbols = {p.get("symbol") for p in positions if p.get("symbol")}

    for opp in external_opportunities:
        sym = opp.get("symbol", "")
        if not sym or sym in held_symbols:
            why_rejected.append(f"{sym}: ya en cartera")
            continue

        # Must be in main_allowed (safety rail)
        if sym not in main_allowed:
            why_rejected.append(f"{sym}: no está en main_allowed (whitelist)")
            continue

        # Must have known_valid asset_type (fail-closed)
        at = opp.get("asset_type", "DESCONOCIDO")
        at_status = opp.get("asset_type_status", "unknown")

        if at_status != "known_valid":
            why_rejected.append(f"{sym}: asset_type '{at}' status='{at_status}' (no válido)")
            continue

        if at not in VALID_ASSET_TYPES:
            why_rejected.append(f"{sym}: asset_type '{at}' no soportado")
            continue

        # Minimum confidence/priority_score
        score = opp.get("priority_score", 0)
        if score < min_confidence:
            why_rejected.append(
                f"{sym}: priority_score {score:.2f} < min_confidence {min_confidence}"
            )
            continue

        buy_candidates.append({
            "symbol": sym,
            "asset_type": at,
            "priority_score": score,
            "source_types": opp.get("source_types", []),
            "reason": opp.get("reason", ""),
            "investable": opp.get("investable", False),
        })

    # Sort by priority_score descending
    buy_candidates.sort(key=lambda x: x["priority_score"], reverse=True)

    # -----------------------------------------------------------------------
    # Step 4: Allocate funding to buy candidates (cash first, then sells)
    # -----------------------------------------------------------------------
    remaining_funding = total_funding
    # Cap per-buy: don't put more than max_single of total_value in any new position
    max_per_buy = max_single * total_value if total_value > 0 else 0
    # Also cap by max_move per cycle
    max_per_buy = min(max_per_buy, max_move * total_value) if total_value > 0 else 0

    constraints_applied.append(f"max_per_buy=${max_per_buy:.0f}")

    for candidate in buy_candidates:
        if remaining_funding <= 0:
            why_rejected.append(
                f"{candidate['symbol']}: sin fondos disponibles restantes"
            )
            continue

        # Allocate proportionally to priority_score, capped
        alloc_value = min(remaining_funding, max_per_buy)

        # For now: propose value-based (quantity requires live pricing)
        buys_proposed.append({
            "symbol": candidate["symbol"],
            "side": "buy",
            "value_proposed": round(alloc_value, 2),
            "asset_type": candidate["asset_type"],
            "priority_score": candidate["priority_score"],
            "source_types": candidate["source_types"],
            "reason": candidate["reason"],
            "funding_source": "cash" if available_cash >= alloc_value else "mixed",
        })
        why_selected.append(
            f"BUY {candidate['symbol']}: score {candidate['priority_score']:.2f}, "
            f"tipo {candidate['asset_type']}, ~${alloc_value:.0f}"
        )
        remaining_funding -= alloc_value

    residual_cash = round(remaining_funding, 2)

    # -----------------------------------------------------------------------
    # Step 5: Build final plan
    # -----------------------------------------------------------------------
    plan_status = "empty"
    planner_reason = ""

    if sells_proposed or buys_proposed:
        plan_status = "proposed"
        parts = []
        if sells_proposed:
            parts.append(f"{len(sells_proposed)} venta(s) propuesta(s) por sobreconcentración")
        if buys_proposed:
            parts.append(f"{len(buys_proposed)} compra(s) propuesta(s) con oportunidades externas")
        planner_reason = "; ".join(parts)
    elif not external_opportunities:
        plan_status = "no_opportunities"
        planner_reason = "Sin oportunidades externas detectadas para evaluar"
    elif not buy_candidates and external_opportunities:
        plan_status = "no_eligible_candidates"
        planner_reason = "Oportunidades evaluadas pero ninguna cumple criterios (asset_type/whitelist/confidence)"
    elif total_funding <= 0 and not overweighted:
        plan_status = "no_funding"
        planner_reason = "Sin cash disponible ni holdings sobreponderados para financiar compras"
    else:
        planner_reason = "Sin propuestas concretas (condiciones no alcanzadas)"

    return {
        "planner_status": plan_status,
        "planner_reason": planner_reason,
        "dry_run": True,  # Always true — NEVER auto-executed
        "profile": profile_label,
        "profile_canonical": canonical_profile,
        "funding_sources": funding_sources,
        "sells_proposed": sells_proposed,
        "buys_proposed": buys_proposed,
        "residual_cash": residual_cash,
        "constraints_applied": constraints_applied,
        "why_selected": why_selected,
        "why_rejected": why_rejected,
        "total_candidates_evaluated": len(external_opportunities),
        "candidates_accepted": len(buys_proposed),
        "candidates_rejected": len(why_rejected),
    }
