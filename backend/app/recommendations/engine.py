from app.core.config import get_settings
from app.portfolio.profiles import get_profile_label, get_profile_thresholds, resolve_profile

# Minimum signal_score to count as a meaningful hit (filters noise)
_MIN_SIGNAL_SCORE = 0.35


def generate_recommendation(snapshot: dict, analysis: dict, news: list[dict], max_move: float) -> dict:
    settings = get_settings()
    profile = settings.investor_profile_target or settings.investor_profile
    canonical_profile = resolve_profile(profile)
    profile_label = get_profile_label(profile)
    thresholds = get_profile_thresholds(profile)

    alerts = analysis.get("alerts", [])

    # --- Signal-aware hit classification ---
    # When items come enriched with signal_score/signal_class (from scoring.py),
    # use them for better filtering. When not present, fall back to legacy behavior.
    negative_hits = []
    positive_hits = []
    for n in news:
        score = n.get("signal_score", n.get("pre_score", 0.5))
        if score < _MIN_SIGNAL_SCORE:
            continue  # Filter weak/noisy signals
        if n.get("impact") == "negativo":
            negative_hits.append(n)
        elif n.get("impact") == "positivo":
            positive_hits.append(n)

    held_symbols = [p.get("symbol") for p in snapshot.get("positions", []) if p.get("symbol")]
    held_set = set(held_symbols)

    action = "mantener"
    pct = 0.0
    raw_pct = 0.0
    actions = []
    external_opportunities = []
    candidate_deviations = {}
    rationale = f"Cartera estable sin señales fuertes (perfil {profile_label})."
    risks = "Riesgo moderado de mercado."
    confidence = 0.55
    rationale_reasons = []

    # --- External opportunities with signal_class awareness ---
    for item in news:
        signal_class = item.get("signal_class", "")
        item_score = item.get("signal_score", item.get("pre_score", 0.5))

        # Skip very weak signals for opportunity detection too
        if item_score < _MIN_SIGNAL_SCORE:
            continue

        for symbol in item.get("related_assets", []):
            if symbol not in held_set:
                external_opportunities.append(
                    {
                        "symbol": symbol,
                        "reason": item.get("title") or "Oportunidad detectada por noticia externa",
                        "confidence": item.get("confidence", 0.5),
                        "event_type": item.get("event_type", "otro"),
                        "impact": item.get("impact", "neutro"),
                        "signal_class": signal_class,
                        "signal_score": item_score,
                        "source_count": item.get("source_count", 1),
                    }
                )

    dedup_ops = []
    seen = set()
    for op in external_opportunities:
        key = (op["symbol"], op["event_type"], op["impact"])
        if key in seen:
            continue
        seen.add(key)
        dedup_ops.append(op)
    external_opportunities = dedup_ops

    # Sort external opportunities by signal_score (best first)
    external_opportunities.sort(key=lambda x: x.get("signal_score", 0), reverse=True)

    if "Portfolio vacío o sin valor." in alerts:
        confidence = 0.3
        rationale = "No hay evidencia suficiente por portfolio vacío."

    elif any("Sobreconcentración" in a for a in alerts) and analysis.get("weights_by_asset"):
        candidate_weights = {
            symbol: weight for symbol, weight in analysis["weights_by_asset"].items() if symbol in held_set
        }
        if candidate_weights:
            symbol = max(candidate_weights, key=candidate_weights.get)
            weight = candidate_weights[symbol]
            max_single = thresholds.get("max_single_asset_weight", 0.40)
            action = "reducir riesgo"
            pct = min(max_move, 0.08)
            actions = [{"symbol": symbol, "target_change_pct": -pct, "reason": "Sobreconcentración"}]
            rationale_reasons.append({
                "type": "concentration_reason",
                "detail": f"{symbol} representa {round(weight*100,1)}% del portfolio, excede el límite de {int(max_single*100)}% para perfil {profile_label}.",
            })

            # Check overlap
            overlap_alerts = analysis.get("overlap_alerts", [])
            overlapping = [oa for oa in overlap_alerts if symbol in oa.get("symbols", [])]
            if overlapping:
                oa = overlapping[0]
                rationale_reasons.append({
                    "type": "overlap_reason",
                    "detail": f"Overlap detectado con {', '.join(oa['symbols'])} que combinan {round(oa['combined_weight']*100,1)}% del portfolio.",
                })

            rationale_reasons.append({
                "type": "target_profile_reason",
                "detail": f"Para el perfil {profile_label}, se sugiere recortar y pasar a liquidez hasta que la posición esté dentro de bandas.",
            })
            rationale = _build_rationale(rationale_reasons, profile_label)
            confidence = 0.72

            # Boost confidence if holding_risk signals confirm concentration concern
            holding_risks = [n for n in news if n.get("signal_class") == "holding_risk"
                            and symbol in (n.get("related_assets") or [])]
            if holding_risks:
                confidence = min(0.85, confidence + 0.08)
                rationale_reasons.append({
                    "type": "signal_confirmation_reason",
                    "detail": f"Riesgo confirmado por {len(holding_risks)} señal(es) negativa(s) sobre {symbol}.",
                })
                rationale = _build_rationale(rationale_reasons, profile_label)

    elif any(abs(v) > 0.07 for v in analysis.get("rebalance_deviation", {}).values()):
        candidate_deviations = {
            symbol: dev
            for symbol, dev in analysis.get("rebalance_deviation", {}).items()
            if symbol in held_set
        }
        if candidate_deviations:
            worst = max(candidate_deviations, key=lambda k: abs(candidate_deviations[k]))
            dev = candidate_deviations[worst]
            action = "rebalancear"
            raw_pct = abs(dev) * 0.5
            pct = round(min(max_move, max(0.02, raw_pct)), 4)
            severity = min(abs(dev) / 0.20, 1.0)
            confidence = round(0.55 + severity * 0.15, 2)
            actions = [{"symbol": worst, "target_change_pct": round(-pct if dev > 0 else pct, 4), "reason": "Desvío vs objetivo"}]

            actual_weight = analysis.get("weights_by_asset", {}).get(worst, 0)
            target_weight = actual_weight - dev

            rationale_reasons.append({
                "type": "target_profile_reason",
                "detail": f"Desvío de {worst}: actual {round(actual_weight*100,1)}% vs target {round(target_weight*100,1)}% (perfil {profile_label}).",
            })

            # Check if equity band exceeded
            equity_weight = analysis.get("equity_weight", 0)
            max_equity = thresholds.get("max_equity_band", 0.70)
            if equity_weight > max_equity:
                rationale_reasons.append({
                    "type": "risk_reduction_reason",
                    "detail": f"Equity total ({round(equity_weight*100,1)}%) excede banda del perfil {profile_label} ({int(max_equity*100)}%). Reducir exposición a renta variable.",
                })

            # Check overlap
            overlap_alerts = analysis.get("overlap_alerts", [])
            overlapping = [oa for oa in overlap_alerts if worst in oa.get("symbols", [])]
            if overlapping:
                oa = overlapping[0]
                rationale_reasons.append({
                    "type": "overlap_reason",
                    "detail": f"Overlap: {', '.join(oa['symbols'])} combinan {round(oa['combined_weight']*100,1)}% del portfolio. Reducir redundancia.",
                })

            if dev > 0:
                rationale_reasons.append({
                    "type": "risk_reduction_reason",
                    "detail": f"Se sugiere reducir {worst} y pasar a liquidez. No hay reasignación multi-activo automática en esta versión.",
                })
            else:
                rationale_reasons.append({
                    "type": "return_expectation_reason",
                    "detail": f"Se sugiere aumentar {worst} desde liquidez para acercar al target del perfil.",
                })

            rationale = _build_rationale(rationale_reasons, profile_label)

    elif positive_hits:
        # Pick the best positive hit by signal_score (not just the first)
        best_positive = max(positive_hits, key=lambda n: n.get("signal_score", n.get("pre_score", 0)))
        related_in_portfolio = [
            s for s in best_positive.get("related_assets", []) if s in held_set
        ]
        if related_in_portfolio:
            asset = related_in_portfolio[0]
            action = "aumentar posición"
            pct = min(max_move, 0.04)
            actions = [{"symbol": asset, "target_change_pct": pct, "reason": "Evento positivo consistente"}]

            # Detail with signal quality
            source_count = best_positive.get("source_count", 1)
            detail = f"Catalizador positivo en {asset}"
            if source_count >= 2:
                detail += f" (confirmado por {source_count} fuentes)"
            detail += " con impacto acotado."
            rationale_reasons.append({
                "type": "return_expectation_reason",
                "detail": detail,
            })
            rationale_reasons.append({
                "type": "target_profile_reason",
                "detail": f"Movimiento compatible con perfil {profile_label}.",
            })
            rationale = _build_rationale(rationale_reasons, profile_label)
            confidence = 0.58

            # Boost if market-confirmed
            mkt_conf = best_positive.get("market_confirmation", {})
            if mkt_conf.get("status") == "confirmed":
                confidence = min(0.75, confidence + 0.10)
                rationale_reasons.append({
                    "type": "signal_confirmation_reason",
                    "detail": f"Confirmación de mercado: {mkt_conf.get('detail', '')}",
                })
                rationale = _build_rationale(rationale_reasons, profile_label)
            elif mkt_conf.get("status") == "contradicted":
                confidence = max(0.40, confidence - 0.10)
                risks += f" Señal contradecida por mercado ({mkt_conf.get('detail', '')})."

    if not news:
        confidence = min(confidence, 0.5)
        risks += " Sin noticias recientes."

    if negative_hits and positive_hits:
        confidence = max(0.4, confidence - 0.15)
        risks += " Señales mixtas por noticias contradictorias."

    invalid_symbols = [a["symbol"] for a in actions if a.get("symbol") not in held_set]
    if invalid_symbols:
        action = "mantener"
        pct = 0.0
        actions = []
        confidence = min(confidence, 0.45)
        rationale = (
            "Se degradó a mantener porque la sugerencia refería activos fuera del snapshot actual: "
            + ", ".join(invalid_symbols)
            + "."
        )
        rationale_reasons = []

    # --- Rebalance observability ---
    rebalance_obs = {}
    if action == "rebalancear" and candidate_deviations:
        max_dev_symbol = max(candidate_deviations, key=lambda k: abs(candidate_deviations[k]))
        rebalance_obs = {
            "max_rebalance_deviation": round(candidate_deviations[max_dev_symbol], 4),
            "suggested_pct_raw": round(raw_pct, 4),
            "suggested_pct_cap": max_move,
            "suggested_pct_final": pct,
            "suggested_pct_cap_applied": raw_pct > max_move,
        }

    return {
        "action": action,
        "suggested_pct": pct,
        "confidence": round(confidence, 2),
        "rationale": rationale,
        "risks": risks,
        "executive_summary": f"Sugerencia: {action}. Movimiento sugerido: {round(pct*100,2)}% del portfolio. Perfil: {profile_label}.",
        "actions": actions,
        "external_opportunities": external_opportunities,
        "rebalance_observability": rebalance_obs,
        "rationale_reasons": rationale_reasons,
        "profile_applied": canonical_profile,
        "profile_label": profile_label,
        "status": "pending",
    }


def _build_rationale(reasons: list[dict], profile_label: str) -> str:
    """Build a structured rationale string from reason list."""
    if not reasons:
        return f"Cartera estable (perfil {profile_label})."
    parts = [r["detail"] for r in reasons]
    return " ".join(parts)
