from app.core.config import get_settings
from app.portfolio.profiles import build_target_weights, get_bucket


def _infer_economic_currency(symbol: str, asset_type: str, trading_currency: str) -> str:
    """Infer the economic exposure currency for a position.

    CEDEARs and ETFs represent USD-denominated assets even though they
    trade in ARS on the Argentine market. Bonos with GD prefix are
    dollar-linked globals.
    """
    at = (asset_type or "").upper()

    # CEDEARs and ETFs → USD economic exposure
    if at in {"CEDEAR", "ETF"}:
        return "USD"

    # Bonos: GD* = dollar-linked globals, AL* = peso-linked
    if at == "BONO":
        sym = (symbol or "").upper()
        if sym.startswith("GD") or sym.startswith("AE"):
            return "USD"
        return trading_currency

    # ACCIONES → ARS (local equities)
    if at == "ACCIONES":
        return "ARS"

    # FCI, ON, TitulosPublicos, DESCONOCIDO, others → use trading currency
    return trading_currency


def analyze_portfolio(snapshot: dict, target_weights: dict | None = None) -> dict:
    positions = snapshot.get("positions", [])
    if target_weights is None:
        settings = get_settings()
        target_weights = build_target_weights(positions, profile=settings.investor_profile)
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

    # Economic currency exposure (not just trading currency)
    weights_by_currency: dict[str, float] = {}
    for p in positions:
        asset_type = p.get("asset_type") or p.get("instrument_type") or "DESCONOCIDO"
        trading_ccy = p.get("currency", snapshot.get("currency", "ARS"))
        econ_ccy = _infer_economic_currency(p.get("symbol", ""), asset_type, trading_ccy)
        mv = max(0, p.get("market_value", 0))
        weights_by_currency[econ_ccy] = weights_by_currency.get(econ_ccy, 0) + mv / total_value

    # Cash goes to snapshot base currency
    base_ccy = snapshot.get("currency", "ARS")
    weights_by_currency[base_ccy] = weights_by_currency.get(base_ccy, 0) + cash_weight
    weights_by_currency = {k: round(v, 4) for k, v in weights_by_currency.items()}

    # Bucket-level analysis for transparency
    weights_by_bucket: dict[str, float] = {}
    for p in positions:
        asset_type = p.get("asset_type") or p.get("instrument_type") or "DESCONOCIDO"
        bucket = get_bucket(asset_type)
        mv = max(0, p.get("market_value", 0))
        weights_by_bucket[bucket] = weights_by_bucket.get(bucket, 0) + mv / total_value
    weights_by_bucket["cash"] = weights_by_bucket.get("cash", 0) + cash_weight
    weights_by_bucket = {k: round(v, 4) for k, v in weights_by_bucket.items()}

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
        "weights_by_bucket": weights_by_bucket,
        "concentration_score": concentration_score,
        "risk_score": risk_score,
        "rebalance_deviation": rebalance_deviation,
        "alerts": alerts,
    }
