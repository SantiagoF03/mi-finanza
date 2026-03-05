def generate_recommendation(snapshot: dict, analysis: dict, news: list[dict], max_move: float) -> dict:
    alerts = analysis.get("alerts", [])
    negative_hits = [n for n in news if n.get("impact") == "negativo"]
    positive_hits = [n for n in news if n.get("impact") == "positivo"]

    action = "mantener"
    pct = 0.0
    actions = []
    rationale = "Cartera estable sin señales fuertes."
    risks = "Riesgo moderado de mercado."
    confidence = 0.55

    if "Portfolio vacío o sin valor." in alerts:
        confidence = 0.3
        rationale = "No hay evidencia suficiente por portfolio vacío."

    elif "Sobreconcentración en un activo > 40%." in alerts and analysis.get("weights_by_asset"):
        symbol = max(analysis["weights_by_asset"], key=analysis["weights_by_asset"].get)
        action = "reducir riesgo"
        pct = min(max_move, 0.08)
        actions = [{"symbol": symbol, "target_change_pct": -pct, "reason": "Sobreconcentración"}]
        rationale = f"{symbol} excede concentración tolerada; conviene recortar y pasar a liquidez."
        confidence = 0.72

    elif any(abs(v) > 0.07 for v in analysis.get("rebalance_deviation", {}).values()):
        worst = max(analysis["rebalance_deviation"], key=lambda k: abs(analysis["rebalance_deviation"][k]))
        dev = analysis["rebalance_deviation"][worst]
        action = "rebalancear"
        pct = min(max_move, abs(dev))
        actions = [{"symbol": worst, "target_change_pct": -pct if dev > 0 else pct, "reason": "Desvío vs objetivo"}]
        rationale = "Se detectó desvío material contra cartera objetivo."
        confidence = 0.66

    elif positive_hits:
        asset = positive_hits[0]["related_assets"][0]
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

    return {
        "action": action,
        "suggested_pct": pct,
        "confidence": round(confidence, 2),
        "rationale": rationale,
        "risks": risks,
        "executive_summary": f"Sugerencia: {action}. Movimiento sugerido: {round(pct*100,2)}% del portfolio.",
        "actions": actions,
    }
