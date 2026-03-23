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

# Keywords that indicate a news item is company-specific (not just macro/market-wide).
# Used by classify_news_relevance() to prioritize items more likely to produce
# causal_link_strength=strong matches.
COMPANY_SPECIFIC_KEYWORDS = [
    # English corporate events
    "earnings", "revenue", "profit", "quarterly", "annual results",
    "guidance", "forecast", "outlook", "estimates",
    "beats", "misses", "exceeds", "tops", "falls short",
    "dividend", "buyback", "repurchase", "share repurchase",
    "acquisition", "merger", "acquires", "merges", "takeover", "deal",
    "ipo", "listing", "public offering",
    "upgrade", "downgrade", "price target", "rating",
    "ceo", "cfo", "executive", "appoints", "resigns", "departs",
    "lawsuit", "settlement", "sec", "investigation", "fine", "penalty",
    "partnership", "contract", "wins contract", "awarded",
    "recall", "fda", "approval", "patent",
    "stock split", "spin-off", "spinoff",
    # Spanish corporate events
    "ganancias", "ingresos", "resultados", "beneficio",
    "dividendo", "recompra",
    "adquisición", "adquisicion", "fusión", "fusion",
    "demanda judicial", "multa",
    "nombramiento", "renuncia",
    "acuerdo", "contrato",
]


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
    """Real RSS news provider with recency filtering.

    Applies hard recency windows: hard news ≤24h, macro/sectorial ≤48h.
    Tracks per-feed fetch status for observability.
    """

    # Recency windows by event type (hours)
    _RECENCY_LIMITS: dict[str, float] = {
        "earnings": 24, "guidance": 24, "tasas": 24,
        "geopolítico": 24, "regulatorio": 48, "inflación": 48,
        "sectorial": 48, "ia": 48, "otro": 24,
    }

    def __init__(self, urls: list[str], timeout_seconds: int, max_items: int) -> None:
        self.urls = urls
        self.timeout_seconds = timeout_seconds
        self.max_items = max_items
        self.last_fetch_stats: dict = {}

    def get_recent_news(self, portfolio_symbols: list[str]) -> list[dict]:
        items: list[dict] = []
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        feed_stats: list[dict] = []

        with httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": "MiFinanza/1.0 (RSS Reader)"},
        ) as client:
            for url in self.urls:
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                    feed_items = parse_rss_items(resp.text, portfolio_symbols)
                    # Tag source from feed URL domain
                    domain = url.split("//")[-1].split("/")[0].replace("www.", "")
                    for fi in feed_items:
                        fi["source"] = fi.get("source") or domain
                    items.extend(feed_items)
                    feed_stats.append({"url": url, "status": "ok", "items": len(feed_items)})
                except Exception as exc:
                    feed_stats.append({"url": url, "status": "error", "error": str(exc)[:100]})
                    continue

        # Recency filter: drop items older than their event_type window
        filtered = []
        for item in items:
            created = item.get("created_at")
            if created:
                age_hours = max(0.0, (now - created).total_seconds() / 3600)
                event_type = item.get("event_type", "otro")
                max_age = self._RECENCY_LIMITS.get(event_type, 24)
                if age_hours > max_age:
                    continue
            filtered.append(item)

        # Prioritize company_specific news above macro_generic before truncation.
        # Within each group, sort by recency (newest first).
        filtered.sort(
            key=lambda x: (
                0 if x.get("news_relevance") == "company_specific" else 1,
                -(x.get("created_at") or now).timestamp(),
            ),
        )
        deduped = deduplicate_news_items(filtered)

        company_count = sum(1 for d in deduped if d.get("news_relevance") == "company_specific")
        self.last_fetch_stats = {
            "feeds_attempted": len(self.urls),
            "feeds_ok": sum(1 for f in feed_stats if f["status"] == "ok"),
            "total_raw": len(items),
            "after_recency_filter": len(filtered),
            "after_dedup": len(deduped),
            "company_specific_count": company_count,
            "macro_generic_count": len(deduped) - company_count,
            "returned": min(len(deduped), self.max_items),
            "feed_details": feed_stats,
        }

        return deduped[: self.max_items]


def extract_market_symbols(text: str) -> list[str]:
    candidates = re.findall(r"\b[A-Z]{2,6}\b", text)
    blacklist = {"USD", "ARS", "FED", "CPI", "IPC", "AI", "ETF"}
    out = []
    for c in candidates:
        if c in blacklist:
            continue
        if c in AMBIGUOUS_TICKERS:
            continue
        if c not in out:
            out.append(c)
    return out


# Tickers that are common English words or abbreviations.
# These produce rampant false positives when matched by regex alone.
# They are ONLY included in related_assets via held_mentions (portfolio match)
# with word-boundary matching, never from raw text extraction.
AMBIGUOUS_TICKERS = frozenset({
    "ALL",   # Allstate — but "ALL" is an English word
    "AN",    # AutoNation — but "AN" is extremely common
    "ARE",   # Alexandria Real Estate — English verb
    "BAC",   # Bank of America — matches "back", abbreviations
    "BIG",   # Big Lots — English word
    "CAT",   # Caterpillar — English word
    "CAN",   # not a real ticker but common word
    "DIS",   # Disney — but "DIS" appears in "dis-", "discuss", etc.
    "ED",    # Consolidated Edison — common name/word
    "FOR",   # not a real ticker but common word
    "HD",    # Home Depot — common abbreviation (high definition)
    "HAS",   # Hasbro — English verb
    "IT",    # Gartner — English pronoun (also in PSEUDO_TICKER_BLOCKLIST)
    "LOW",   # Lowe's — English word
    "MA",    # Mastercard — common abbreviation (Massachusetts, etc.)
    "MAN",   # ManpowerGroup — English word
    "MAR",   # Marriott — common word (Spanish month)
    "NOW",   # ServiceNow — English word
    "ON",    # ON Semiconductor — English preposition
    "PG",    # Procter & Gamble — common abbreviation (parental guidance)
    "RE",    # Everest Group — English prefix
    "SO",    # Southern Company — English word
    "SU",    # Suncor — Spanish possessive pronoun
    "TWO",   # Two Harbors — English number word
    "V",     # Visa — single letter (only 1 char, won't regex-match, but in held_mentions)
    "WAS",   # not a real ticker but matched via held_mentions
    "X",     # US Steel — single letter
})


def classify_news_relevance(title: str, summary: str) -> str:
    """Classify a news item as 'company_specific' or 'macro_generic'.

    Uses simple keyword matching on the title (primary) and summary (secondary).
    Company-specific news is more likely to produce causal matches with symbols.
    """
    title_lower = title.lower()
    # Title match is the strongest signal
    if any(kw in title_lower for kw in COMPANY_SPECIFIC_KEYWORDS):
        return "company_specific"
    # Check summary only for high-confidence corporate event keywords
    summary_lower = summary.lower()
    if any(kw in summary_lower for kw in COMPANY_SPECIFIC_KEYWORDS[:20]):  # top 20 most reliable
        return "company_specific"
    return "macro_generic"


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

    # Boost confidence for company-specific news
    news_relevance = classify_news_relevance(title, summary)
    if news_relevance == "company_specific":
        confidence += 0.05

    confidence = round(min(0.95, confidence), 2)

    raw_text = f"{title} {summary}"
    detected = extract_market_symbols(raw_text)
    # Word-boundary matching for portfolio symbols — prevents "V" matching
    # every text containing the letter "v", or "MA" matching "market".
    held_mentions = [
        s for s in portfolio_symbols
        if re.search(r'\b' + re.escape(s) + r'\b', text, re.IGNORECASE)
    ]
    related_assets = []
    for s in held_mentions + detected:
        if s not in related_assets:
            related_assets.append(s)

    return {
        "event_type": event_type,
        "impact": impact,
        "confidence": confidence,
        "related_assets": related_assets,
        "news_relevance": news_relevance,
    }


def parse_rss_items(xml_text: str, portfolio_symbols: list[str]) -> list[dict]:
    now = datetime.now(timezone.utc)
    root = ElementTree.fromstring(xml_text)
    records = []

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        summary = (item.findtext("description") or "").strip()
        link = (item.findtext("link") or "").strip()
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
                "news_relevance": classified.get("news_relevance", "macro_generic"),
                "summary": summary[:1000],
                "url": link,
                "source": "",  # will be set by ingestion from feed URL
                "created_at": created_at,
            }
        )

    return records


def deduplicate_news_items(items: Iterable[dict]) -> list[dict]:
    """Deduplicate news items by title+summary AND by URL.

    Uses three dedup keys:
    1. Normalized title (lowercase, stripped)
    2. URL (if present, canonicalized)
    3. Title+summary combination (original)
    """
    deduped = []
    seen_titles: set[str] = set()
    seen_urls: set[str] = set()
    for item in items:
        title_norm = (item.get("title") or "").strip().lower()
        summary_norm = (item.get("summary") or "").strip().lower()
        url_raw = (item.get("url") or item.get("link") or "").strip().lower()

        # Title-based dedup
        title_key = f"{title_norm}|{summary_norm}"
        if title_key in seen_titles:
            continue

        # URL-based dedup (skip empty/generic URLs)
        if url_raw and url_raw not in ("http://", "https://", ""):
            if url_raw in seen_urls:
                continue
            seen_urls.add(url_raw)

        seen_titles.add(title_key)
        deduped.append(item)
    return deduped


def get_news_provider() -> NewsProvider:
    settings = get_settings()
    if settings.news_provider == "rss":
        return RssNewsProvider(settings.news_rss_urls, settings.news_timeout_seconds, settings.news_max_items)
    return MockNewsProvider()


def get_provider_info(provider: NewsProvider) -> dict:
    """Return observability info about the news provider being used."""
    is_mock = isinstance(provider, MockNewsProvider)
    info: dict = {
        "provider_class": provider.__class__.__name__,
        "is_mock": is_mock,
    }
    if isinstance(provider, RssNewsProvider):
        info["feeds_configured"] = len(provider.urls)
        info["max_items"] = provider.max_items
        if provider.last_fetch_stats:
            info["last_fetch_stats"] = provider.last_fetch_stats
    return info
