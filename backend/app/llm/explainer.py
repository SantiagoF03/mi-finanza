from __future__ import annotations

import json

import httpx

from app.core.config import get_settings


def _call_llm(prompt: str) -> str:
    settings = get_settings()
    if not settings.llm_enabled:
        raise RuntimeError("LLM disabled")
    if settings.llm_provider != "openai":
        raise RuntimeError(f"Unsupported LLM provider: {settings.llm_provider}")
    if not settings.llm_api_key:
        raise RuntimeError("LLM API key missing")

    payload = {
        "model": settings.llm_model,
        "messages": [
            {"role": "system", "content": "Sos un asistente financiero prudente. No inventes datos."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    with httpx.Client(timeout=settings.llm_timeout_seconds) as client:
        resp = client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {settings.llm_api_key}", "Content-Type": "application/json"},
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
    return data["choices"][0]["message"]["content"].strip()


def summarize_news(news_items: list[dict], snapshot: dict, analysis: dict) -> str | None:
    settings = get_settings()
    if not settings.llm_enabled or not news_items:
        return None

    prompt = (
        "Resumí en 4 bullets claros las noticias recientes relevantes para una cartera moderada.\n"
        f"Holdings: {[p.get('symbol') for p in snapshot.get('positions', [])]}\n"
        f"Alerts análisis: {analysis.get('alerts', [])}\n"
        f"Noticias: {json.dumps(news_items[:10], ensure_ascii=False)}"
    )
    return _call_llm(prompt)


def explain_recommendation(
    recommendation: dict,
    snapshot: dict,
    analysis: dict,
    news_items: list[dict],
    unchanged: bool = False,
) -> str | None:
    settings = get_settings()
    if not settings.llm_enabled:
        return None

    prompt = (
        "Explicá en lenguaje simple la recomendación rule-based para un inversor moderado.\n"
        "No cambies acción, activos, porcentajes ni reglas. Solo explicá.\n"
        f"Unchanged: {unchanged}\n"
        f"Recomendación: {json.dumps(recommendation, ensure_ascii=False)}\n"
        f"Holdings: {[p.get('symbol') for p in snapshot.get('positions', [])]}\n"
        f"Análisis: {json.dumps(analysis, ensure_ascii=False)}\n"
        f"Noticias: {json.dumps(news_items[:8], ensure_ascii=False)}"
    )
    return _call_llm(prompt)
