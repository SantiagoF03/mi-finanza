from datetime import datetime, timedelta


def get_mock_news() -> list[dict]:
    now = datetime.utcnow()
    return [
        {
            "title": "La FED mantiene tasas y sugiere prudencia",
            "event_type": "tasas",
            "impact": "neutro",
            "confidence": 0.74,
            "related_assets": ["SPY", "QQQ"],
            "summary": "La señal reduce volatilidad extrema, sin gatillo fuerte de compra.",
            "created_at": now,
        },
        {
            "title": "Resultados sólidos en sector tecnológico",
            "event_type": "earnings",
            "impact": "positivo",
            "confidence": 0.68,
            "related_assets": ["AAPL", "MSFT"],
            "summary": "Mejora de márgenes y guidance estable.",
            "created_at": now - timedelta(hours=8),
        },
        {
            "title": "Nueva tensión geopolítica impacta bonos emergentes",
            "event_type": "geopolítico",
            "impact": "negativo",
            "confidence": 0.61,
            "related_assets": ["AL30"],
            "summary": "Riesgo país y volatilidad en deuda soberana.",
            "created_at": now - timedelta(days=1),
        },
    ]
