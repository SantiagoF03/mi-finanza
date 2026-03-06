def analyze_portfolio(snapshot: dict, target_weights: dict | None = None) -> dict:
    target_weights = target_weights or {"AAPL": 0.25, "MSFT": 0.20, "SPY": 0.25, "AL30": 0.15, "CASH": 0.15}
    positions = snapshot.get("positions", [])
    cash = snapshot.get("cash", 0)

    alerts = []
    if cash is None:
        cash = 0
        alerts.append("Cash faltante: se asumió 0.")
    if cash < 0:
        alerts.append("Cash negativo detectado.")

    total_value = snapshot.get("total_value") or (cash + sum(max(0, p.get("market_value", 0)) for p in positions))
    if total_value <= 0:
        return {
            "weights_by_asset": {"CASH": 1.0},
            "weights_by_currency": {snapshot.get("currency", "USD"): 1.0},
            "concentration_score": 0.0,
            "risk_score": 0.0,
            "rebalance_deviation": {k: round(-v, 4) for k, v in target_weights.items()},
            "alerts": alerts + ["Portfolio vacío o sin valor."],
        }

    weights_by_asset = {p["symbol"]: round(max(0, p["market_value"]) / total_value, 4) for p in positions if p.get("symbol")}
    cash_weight = max(0, cash) / total_value
    weights_by_asset["CASH"] = round(cash_weight, 4)

    weights_by_currency = {}
    for p in positions:
        ccy = p.get("currency", snapshot.get("currency", "USD"))
        weights_by_currency[ccy] = weights_by_currency.get(ccy, 0) + max(0, p.get("market_value", 0)) / total_value
    base_ccy = snapshot.get("currency", "USD")
    weights_by_currency[base_ccy] = weights_by_currency.get(base_ccy, 0) + cash_weight
    weights_by_currency = {k: round(v, 4) for k, v in weights_by_currency.items()}

    concentration_score = round(max(weights_by_asset.values()), 4)
    pnl_volatility_proxy = sum(abs(p.get("pnl_pct", 0)) for p in positions) / max(len(positions), 1)
    risk_score = round(min(1.0, concentration_score * 0.7 + pnl_volatility_proxy * 0.3), 4)

    rebalance_deviation = {symbol: round(weights_by_asset.get(symbol, 0) - target, 4) for symbol, target in target_weights.items()}

    if concentration_score > 0.40:
        alerts.append("Sobreconcentración en un activo > 40%.")
    if any(v > 0.70 for v in weights_by_currency.values()):
        alerts.append("Exceso de exposición por moneda > 70%.")
    if any(abs(v) > 0.07 for v in rebalance_deviation.values()):
        alerts.append("Desvío relevante vs cartera objetivo detectado.")

    return {
        "weights_by_asset": weights_by_asset,
        "weights_by_currency": weights_by_currency,
        "concentration_score": concentration_score,
        "risk_score": risk_score,
        "rebalance_deviation": rebalance_deviation,
        "alerts": alerts,
    }
