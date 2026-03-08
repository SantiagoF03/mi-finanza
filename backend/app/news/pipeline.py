from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import re
from typing import Iterable
from xml.etree import ElementTree

import httpx

from app.core.config import get_settings


POSITIVE_KEYWORDS = ["sube", "crece", "supera", "mejora", "positivo", "récord", "record", "fuerte"]
NEGATIVE_KEYWORDS = ["cae", "baja", "riesgo", "demanda", "débil", "debil", "negativo", "crisis", "guerra"]
EVENT_TYPE_KEYWORDS = {
    "earnings": ["earnings", "resultado", "balance", "trimestre"],
    "guidance": ["guidance", "proyección", "proyeccion", "forecast"],
    "inflación": ["inflación", "inflacion", "cpi", "ipc"],
    "tasas": ["tasa", "rates", "fed", "banco central"],
    "regulatorio": ["regulator", "regulación", "regulacion", "normativa", "ley"],
    "geopolítico": ["guerra", "conflicto", "geopol", "sanción", "sancion"],
    "sectorial": ["sector", "industria", "mercado"],
    "ia": ["ai", "ia", "artificial intelligence"],
}


class NewsProvider(ABC):
    @abstractmethod
    def get_recent_news(self, portfolio_symbols: list[str]) -> list[dict]: ...


class MockNewsProvider(NewsProvider):
    def get_recent_news(self, portfolio_symbols: list[str]) -> list[dict]:
        now = datetime.utcnow()
        universe = portfolio_symbols or ["SPY", "AL30"]
        symbol_a = universe[0]
        symbol_b = universe[min(1, len(universe) - 1)]
        return [
            {
                "title": f"La FED mantiene tasas y sugiere prudencia para {symbol_a}",
                "event_type": "tasas",
                "impact": "neutro",
                "confidence": 0.74,
                "related_assets": [symbol_a],
                "summary": "La señal reduce volatilidad extrema, sin gatillo fuerte de compra.",
                "created_at": now,
            },
            {
                "title": f"Resultados sólidos en sectores relevantes de cartera y {symbol_b}",
                "event_type": "earnings",
                "impact": "positivo",
                "confidence": 0.68,
                "related_assets": [symbol_a, symbol_b],
                "summary": "Mejora de márgenes y guidance estable.",
                "created_at": now - timedelta(hours=8),
            },
            {
                "title": f"Nueva tensión geopolítica impacta activos emergentes como {symbol_b}",
                "event_type": "geopolítico",
                "impact": "negativo",
                "confidence": 0.61,
                "related_assets": [symbol_b],
                "summary": "Riesgo macro y volatilidad en activos de riesgo.",
                "created_at": now - timedelta(days=1),
            },
        ]


class RssNewsProvider(NewsProvider):
    def __init__(self, urls: list[str], timeout_seconds: int, max_items: int) -> None:
        self.urls = urls
        self.timeout_seconds = timeout_seconds
        self.max_items = max_items

    def get_recent_news(self, portfolio_symbols: list[str]) -> list[dict]:
        items: list[dict] = []
        with httpx.Client(timeout=self.timeout_seconds) as client:
            for url in self.urls:
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    items.extend(parse_rss_items(resp.text, portfolio_symbols))
                except Exception:
                    continue
        items.sort(key=lambda x: x["created_at"], reverse=True)
        return deduplicate_news_items(items)[: self.max_items]


def extract_market_symbols(text: str) -> list[str]:
    candidates = re.findall(r"\b[A-Z]{2,6}\b", text)
    blacklist = {"USD", "ARS", "FED", "CPI", "IPC", "AI", "ETF"}
    out = []
    for c in candidates:
        if c in blacklist:
            continue
        if c not in out:
            out.append(c)
    return out


def classify_news_event(title: str, summary: str, portfolio_symbols: list[str]) -> dict:
    text = f"{title} {summary}".lower()

    event_type = "otro"
    for candidate, words in EVENT_TYPE_KEYWORDS.items():
        if any(w in text for w in words):
            event_type = candidate
            break

    pos_hits = sum(1 for w in POSITIVE_KEYWORDS if w in text)
    neg_hits = sum(1 for w in NEGATIVE_KEYWORDS if w in text)

    impact = "neutro"
    if pos_hits > neg_hits:
        impact = "positivo"
    elif neg_hits > pos_hits:
        impact = "negativo"

    confidence = 0.55
    if event_type != "otro":
        confidence += 0.15
    if impact != "neutro":
        confidence += 0.1
    confidence = round(min(0.95, confidence), 2)

    raw_text = f"{title} {summary}"
    detected = extract_market_symbols(raw_text)
    held_mentions = [s for s in portfolio_symbols if s.lower() in text]
    related_assets = []
    for s in held_mentions + detected:
        if s not in related_assets:
            related_assets.append(s)

    return {
        "event_type": event_type,
        "impact": impact,
        "confidence": confidence,
        "related_assets": related_assets,
    }


def parse_rss_items(xml_text: str, portfolio_symbols: list[str]) -> list[dict]:
    now = datetime.now(timezone.utc)
    root = ElementTree.fromstring(xml_text)
    records = []

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        summary = (item.findtext("description") or "").strip()
        pub_date_raw = (item.findtext("pubDate") or "").strip()

        created_at = now
        if pub_date_raw:
            try:
                created_at = parsedate_to_datetime(pub_date_raw)
                if created_at.tzinfo is not None:
                    created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    created_at = created_at.replace(tzinfo=None)
            except Exception:
                created_at = datetime.utcnow()

        if not title:
            continue

        classified = classify_news_event(title, summary, portfolio_symbols)
        records.append(
            {
                "title": title,
                "event_type": classified["event_type"],
                "impact": classified["impact"],
                "confidence": classified["confidence"],
                "related_assets": classified["related_assets"],
                "summary": summary[:1000],
                "created_at": created_at,
            }
        )

    return records


def deduplicate_news_items(items: Iterable[dict]) -> list[dict]:
    deduped = []
    seen: set[str] = set()
    for item in items:
        key = f"{item.get('title','').strip().lower()}|{item.get('summary','').strip().lower()}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def get_news_provider() -> NewsProvider:
    settings = get_settings()
    if settings.news_provider == "rss":
        return RssNewsProvider(settings.news_rss_urls, settings.news_timeout_seconds, settings.news_max_items)
    return MockNewsProvider()
