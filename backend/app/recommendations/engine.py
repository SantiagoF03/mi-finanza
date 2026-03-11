def generate_recommendation(snapshot: dict, analysis: dict, news: list[dict], max_move: float) -> dict:
    alerts = analysis.get("alerts", [])
    negative_hits = [n for n in news if n.get("impact") == "negativo"]
    positive_hits = [n for n in news if n.get("impact") == "positivo"]

    held_symbols = [p.get("symbol") for p in snapshot.get("positions", []) if p.get("symbol")]
    held_set = set(held_symbols)

    action = "mantener"
    pct = 0.0
    raw_pct = 0.0
    actions = []
    external_opportunities = []
    candidate_deviations = {}
    rationale = "Cartera estable sin señales fuertes."
    risks = "Riesgo moderado de mercado."
    confidence = 0.55

    for item in news:
        for symbol in item.get("related_assets", []):
            if symbol not in held_set:
                external_opportunities.append(
                    {
                        "symbol": symbol,
                        "reason": item.get("title") or "Oportunidad detectada por noticia externa",
                        "confidence": item.get("confidence", 0.5),
                        "event_type": item.get("event_type", "otro"),
                        "impact": item.get("impact", "neutro"),
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

    if "Portfolio vacío o sin valor." in alerts:
        confidence = 0.3
        rationale = "No hay evidencia suficiente por portfolio vacío."

    elif "Sobreconcentración en un activo > 40%." in alerts and analysis.get("weights_by_asset"):
        candidate_weights = {
            symbol: weight for symbol, weight in analysis["weights_by_asset"].items() if symbol in held_set
        }
        if candidate_weights:
            symbol = max(candidate_weights, key=candidate_weights.get)
            action = "reducir riesgo"
            pct = min(max_move, 0.08)
            actions = [{"symbol": symbol, "target_change_pct": -pct, "reason": "Sobreconcentración"}]
            rationale = f"{symbol} excede concentración tolerada; conviene recortar y pasar a liquidez."
            confidence = 0.72

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
            # Scale: suggest correcting ~50% of the worst deviation per cycle,
            # capped at max_move. Minimum 2% to avoid trivial suggestions.
            raw_pct = abs(dev) * 0.5
            pct = round(min(max_move, max(0.02, raw_pct)), 4)
            # Scale confidence with deviation severity (20% dev = max severity)
            severity = min(abs(dev) / 0.20, 1.0)
            confidence = round(0.55 + severity * 0.15, 2)
            actions = [{"symbol": worst, "target_change_pct": round(-pct if dev > 0 else pct, 4), "reason": "Desvío vs objetivo"}]
            rationale = "Se detectó desvío material contra cartera objetivo."

    elif positive_hits:
        related_in_portfolio = []
        for item in positive_hits:
            for symbol in item.get("related_assets", []):
                if symbol in held_set:
                    related_in_portfolio.append(symbol)
        if related_in_portfolio:
            asset = related_in_portfolio[0]
            action = "aumentar posición"
            pct = min(max_move, 0.04)
            actions = [{"symbol": asset, "target_change_pct": pct, "reason": "Evento positivo consistente"}]
            rationale = f"Catalizador positivo en {asset} con impacto acotado y perfil moderado."
            confidence = 0.58

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
        "executive_summary": f"Sugerencia: {action}. Movimiento sugerido: {round(pct*100,2)}% del portfolio.",
        "actions": actions,
        "external_opportunities": external_opportunities,
        "rebalance_observability": rebalance_obs,
    }
