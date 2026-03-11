"""Market event ingestion pipeline.

Parts A+B: Fetches news, deduplicates, normalizes, applies recency filter
and pre-scoring, creates MarketEvents and alerts when warranted.

Triage levels (NOT investment decisions — only analysis routing):
- store_only: persisted but no further processing
- observe: shown in recent events, no LLM call
- send_to_llm: qualifies for LLM explanation in next cycle
- trigger_recalc: triggers a full analysis recalculation
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.models import (
    IngestionRun,
    MarketEvent,
    NewsNormalized,
    NewsRaw,
)
from app.news.pipeline import (
    classify_news_event,
    deduplicate_news_items,
    get_news_provider,
)

# ---------------------------------------------------------------------------
# Recency windows by event type (Part B)
# ---------------------------------------------------------------------------

# Max age in hours for each event category to be considered actionable.
RECENCY_WINDOWS: dict[str, float] = {
    "earnings": 24,
    "guidance": 24,
    "tasas": 24,
    "geopolítico": 24,
    "inflación": 48,
    "regulatorio": 48,
    "sectorial": 48,
    "ia": 48,
    "otro": 24,
}

# Sources considered high-quality for scoring boost
TOP_TIER_SOURCES: set[str] = {
    "reuters", "bloomberg", "investing.com", "wsj",
    "financial times", "cnbc", "ambito", "infobae",
}

# Event types that are hard-news (higher urgency)
HARD_NEWS_TYPES: set[str] = {"earnings", "guidance", "tasas", "geopolítico", "inflación", "regulatorio"}


def _dedup_hash(title: str, url: str) -> str:
    """Compute a simple dedup hash from title + url."""
    raw = f"{title.strip().lower()}|{url.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _compute_recency_hours(published_at: datetime | None, now: datetime) -> float:
    """Hours since publication. Returns 9999 if unknown."""
    if not published_at:
        return 9999.0
    delta = now - published_at
    return max(0.0, delta.total_seconds() / 3600)


def _compute_pre_score(
    event_type: str,
    impact: str,
    confidence: float,
    recency_hours: float,
    related_assets: list[str],
    source: str,
    held_symbols: set[str],
    watchlist_symbols: set[str],
    universe_symbols: set[str],
) -> float:
    """Compute a cheap pre-score (0.0–1.0) without LLM.

    Signals:
    - mentions holdings → +0.25
    - mentions watchlist/universe → +0.10
    - recency (fresher = higher) → up to +0.20
    - top-tier source → +0.10
    - hard news type → +0.10
    - non-neutral impact → +0.10
    - high confidence → up to +0.15
    """
    score = 0.0

    # Asset relevance
    related_set = set(related_assets)
    if related_set & held_symbols:
        score += 0.25
    if related_set & (watchlist_symbols | universe_symbols):
        score += 0.10

    # Recency: linear decay over window
    window = RECENCY_WINDOWS.get(event_type, 24)
    if recency_hours <= window:
        score += 0.20 * (1.0 - recency_hours / window)

    # Source quality
    source_lower = source.lower()
    if any(t in source_lower for t in TOP_TIER_SOURCES):
        score += 0.10

    # Event type urgency
    if event_type in HARD_NEWS_TYPES:
        score += 0.10

    # Impact
    if impact != "neutro":
        score += 0.10

    # Confidence
    score += confidence * 0.15

    return round(min(1.0, score), 3)


def _assign_triage_level(
    pre_score: float,
    recency_hours: float,
    event_type: str,
    mentions_holding: bool,
) -> str:
    """Assign triage level based on pre-score and context.

    NOT a binary filter — four graduated levels.
    """
    window = RECENCY_WINDOWS.get(event_type, 24)

    # Too old → store only
    if recency_hours > window * 2:
        return "store_only"

    # High-urgency holding news → trigger recalc
    if mentions_holding and pre_score >= 0.50 and recency_hours <= window:
        return "trigger_recalc"

    # Good score and recent → send to LLM
    if pre_score >= 0.40 and recency_hours <= window:
        return "send_to_llm"

    # Moderate relevance → observe
    if pre_score >= 0.20 and recency_hours <= window * 1.5:
        return "observe"

    return "store_only"


def _severity_from_triage(triage_level: str, impact: str) -> str:
    """Map triage + impact → severity for MarketEvent."""
    if triage_level == "trigger_recalc":
        return "critical" if impact == "negativo" else "high"
    if triage_level == "send_to_llm":
        return "medium"
    return "low"


def run_ingestion(db: Session, source_label: str = "manual") -> dict:
    """Execute a full ingestion run.

    1. Fetch news from configured provider
    2. Deduplicate against news_raw
    3. Normalize + classify
    4. Apply recency filter + pre-scoring
    5. Create MarketEvents for observe+ items
    6. Return run summary
    """
    settings = get_settings()
    now = datetime.utcnow()

    run = IngestionRun(source=source_label, status="running", started_at=now)
    db.add(run)
    db.flush()

    try:
        # Fetch
        provider = get_news_provider()
        held_symbols = set(settings.whitelist_assets)  # approximate; real holdings come from snapshot
        raw_items = provider.get_recent_news(list(held_symbols))
        raw_items = deduplicate_news_items(raw_items)
        run.items_fetched = len(raw_items)

        watchlist_set = set(settings.watchlist_assets)
        universe_set = set(settings.market_universe_assets)

        new_count = 0
        filtered_count = 0
        events_count = 0

        for item in raw_items:
            title = (item.get("title") or "").strip()
            summary = (item.get("summary") or "").strip()
            url = item.get("url", "")
            published_at = item.get("created_at") or item.get("published_at")
            source_name = item.get("source", source_label)

            if not title:
                continue

            # Dedup against news_raw
            dhash = _dedup_hash(title, url)
            existing = db.query(NewsRaw).filter(NewsRaw.dedup_hash == dhash).first()
            if existing:
                continue

            # Persist raw
            raw_row = NewsRaw(
                ingestion_run_id=run.id,
                source=source_name,
                title=title,
                summary=summary[:2000],
                url=url,
                published_at=published_at if isinstance(published_at, datetime) else None,
                fetched_at=now,
                dedup_hash=dhash,
            )
            db.add(raw_row)
            db.flush()
            new_count += 1

            # Classify
            classified = classify_news_event(title, summary, list(held_symbols))

            # Recency
            pub_dt = raw_row.published_at
            recency_hours = _compute_recency_hours(pub_dt, now)

            # Pre-score
            pre_score = _compute_pre_score(
                event_type=classified["event_type"],
                impact=classified["impact"],
                confidence=classified["confidence"],
                recency_hours=recency_hours,
                related_assets=classified["related_assets"],
                source=source_name,
                held_symbols=held_symbols,
                watchlist_symbols=watchlist_set,
                universe_symbols=universe_set,
            )

            mentions_holding = bool(set(classified["related_assets"]) & held_symbols)
            triage = _assign_triage_level(pre_score, recency_hours, classified["event_type"], mentions_holding)

            # Persist normalized
            norm_row = NewsNormalized(
                raw_id=raw_row.id,
                title=title,
                summary=summary[:2000],
                source=source_name,
                url=url,
                published_at=raw_row.published_at,
                event_type=classified["event_type"],
                impact=classified["impact"],
                confidence=classified["confidence"],
                related_assets=classified["related_assets"],
                recency_hours=round(recency_hours, 2),
                pre_score=pre_score,
                triage_level=triage,
            )
            db.add(norm_row)
            db.flush()

            if triage == "store_only":
                filtered_count += 1
                continue

            # Create MarketEvent for observe+ items
            severity = _severity_from_triage(triage, classified["impact"])
            trigger_type = "news_holding" if mentions_holding else "news_macro"

            event = MarketEvent(
                news_normalized_id=norm_row.id,
                event_type=classified["event_type"],
                severity=severity,
                trigger_type=trigger_type,
                affected_symbols=classified["related_assets"],
                message=title,
                triggered_recalc=False,
                acknowledged=False,
            )
            db.add(event)
            db.flush()
            events_count += 1

        run.items_new = new_count
        run.items_filtered = filtered_count
        run.events_created = events_count
        run.status = "completed"
        run.finished_at = datetime.utcnow()
        db.commit()

    except Exception as exc:
        run.status = "failed"
        run.error = str(exc)[:500]
        run.finished_at = datetime.utcnow()
        db.commit()
        return {"status": "failed", "error": str(exc)[:500], "run_id": run.id}

    return {
        "status": "completed",
        "run_id": run.id,
        "items_fetched": run.items_fetched,
        "items_new": run.items_new,
        "items_filtered": run.items_filtered,
        "events_created": run.events_created,
    }


def get_pending_recalc_events(db: Session) -> list[MarketEvent]:
    """Get trigger_recalc events that haven't triggered yet."""
    from app.models.models import NewsNormalized as NN

    return (
        db.query(MarketEvent)
        .join(NN, MarketEvent.news_normalized_id == NN.id)
        .filter(NN.triage_level == "trigger_recalc")
        .filter(MarketEvent.triggered_recalc == False)  # noqa: E712
        .order_by(desc(MarketEvent.created_at))
        .all()
    )


def get_recent_events(db: Session, limit: int = 30) -> list[dict]:
    """Return recent market events for API consumption."""
    events = db.query(MarketEvent).order_by(desc(MarketEvent.created_at)).limit(limit).all()
    return [
        {
            "id": e.id,
            "event_type": e.event_type,
            "severity": e.severity,
            "trigger_type": e.trigger_type,
            "affected_symbols": e.affected_symbols,
            "message": e.message,
            "triggered_recalc": e.triggered_recalc,
            "acknowledged": e.acknowledged,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in events
    ]


def get_active_alerts(db: Session) -> list[dict]:
    """Return unacknowledged events with severity >= medium."""
    events = (
        db.query(MarketEvent)
        .filter(MarketEvent.acknowledged == False)  # noqa: E712
        .filter(MarketEvent.severity.in_(["medium", "high", "critical"]))
        .order_by(desc(MarketEvent.created_at))
        .all()
    )
    return [
        {
            "id": e.id,
            "event_type": e.event_type,
            "severity": e.severity,
            "trigger_type": e.trigger_type,
            "affected_symbols": e.affected_symbols,
            "message": e.message,
            "triggered_recalc": e.triggered_recalc,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in events
    ]
