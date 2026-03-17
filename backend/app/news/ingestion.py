"""Market event ingestion pipeline.

Fetches news, deduplicates, normalizes, applies recency filter
and pre-scoring, creates MarketEvents and alerts when warranted.

Triage levels (NOT investment decisions — only analysis routing):
- store_only: persisted but no further processing
- observe: shown in recent events, no LLM call
- send_to_llm: qualifies for LLM explanation in next cycle
- trigger_recalc: triggers a full analysis recalculation
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.models import (
    EventCluster,
    IngestionRun,
    MarketEvent,
    NewsNormalized,
    NewsRaw,
    PortfolioPosition,
    PortfolioSnapshot,
)
from app.news.pipeline import (
    classify_news_event,
    deduplicate_news_items,
    get_news_provider,
)

# ---------------------------------------------------------------------------
# Recency windows by event type
# ---------------------------------------------------------------------------

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

TOP_TIER_SOURCES: set[str] = {
    "reuters", "bloomberg", "investing.com", "wsj",
    "financial times", "cnbc", "ambito", "infobae",
}

HARD_NEWS_TYPES: set[str] = {"earnings", "guidance", "tasas", "geopolítico", "inflación", "regulatorio"}


# ---------------------------------------------------------------------------
# Dedup helpers (Part C — improved)
# ---------------------------------------------------------------------------


def _canonicalize_url(url: str) -> str:
    """Normalize URL for dedup: strip query params, fragments, trailing slashes."""
    url = (url or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        clean = urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))
        return clean.lower()
    except Exception:
        return url.lower().strip()


def _normalize_title(title: str) -> str:
    """Normalize title for dedup: lowercase, strip punctuation, collapse whitespace."""
    t = (title or "").strip().lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _dedup_hash(title: str, url: str) -> str:
    """Compute dedup hash. Uses canonical URL if present, falls back to normalized title."""
    canon_url = _canonicalize_url(url)
    if canon_url and canon_url not in ("http://", "https://"):
        raw = f"url|{canon_url}"
    else:
        raw = f"title|{_normalize_title(title)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _topic_hash(title: str, related_assets: list[str], event_type: str) -> str:
    """Compute a lightweight topic hash for multi-source repetition detection (Part E).

    Groups news by: normalized key terms + symbols + event type.
    NOT semantic similarity — just a cheap textual fingerprint.
    """
    norm = _normalize_title(title)
    stopwords = {"the", "and", "for", "that", "with", "from", "this", "will", "pero",
                 "para", "como", "que", "una", "los", "las", "del", "por"}
    words = sorted(set(w for w in norm.split() if len(w) > 3 and w not in stopwords))[:6]
    symbols = sorted(set(s.upper() for s in related_assets))[:4]
    raw = f"{event_type}|{'_'.join(symbols)}|{'_'.join(words)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _compute_recency_hours(published_at: datetime | None, now: datetime) -> float:
    """Hours since publication. Returns 9999 if unknown."""
    if not published_at:
        return 9999.0
    delta = now - published_at
    return max(0.0, delta.total_seconds() / 3600)


# ---------------------------------------------------------------------------
# Holdings resolution (Part D)
# ---------------------------------------------------------------------------


def _load_real_holdings(db: Session) -> tuple[set[str], str]:
    """Load holdings from the latest persisted portfolio snapshot.

    Returns (held_symbols, source_label).
    Falls back to whitelist if no snapshot exists.
    """
    latest_snapshot = db.query(PortfolioSnapshot).order_by(desc(PortfolioSnapshot.id)).first()
    if latest_snapshot:
        positions = (
            db.query(PortfolioPosition)
            .filter(PortfolioPosition.snapshot_id == latest_snapshot.id)
            .all()
        )
        symbols = {p.symbol for p in positions if p.symbol}
        if symbols:
            return symbols, "snapshot"

    settings = get_settings()
    return set(settings.whitelist_assets), "whitelist"


# ---------------------------------------------------------------------------
# Pre-scoring
# ---------------------------------------------------------------------------


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
    multi_source_count: int = 1,
) -> float:
    """Compute a cheap pre-score (0.0–1.0) without LLM."""
    score = 0.0

    related_set = set(related_assets)
    if related_set & held_symbols:
        score += 0.25
    if related_set & (watchlist_symbols | universe_symbols):
        score += 0.10

    window = RECENCY_WINDOWS.get(event_type, 24)
    if recency_hours <= window:
        score += 0.20 * (1.0 - recency_hours / window)

    source_lower = source.lower()
    if any(t in source_lower for t in TOP_TIER_SOURCES):
        score += 0.10

    if event_type in HARD_NEWS_TYPES:
        score += 0.10

    if impact != "neutro":
        score += 0.10

    score += confidence * 0.15

    # Multi-source repetition boost (Part E)
    if multi_source_count > 1:
        score += 0.05 * min(multi_source_count - 1, 3)

    return round(min(1.0, score), 3)


def _assign_triage_level(
    pre_score: float,
    recency_hours: float,
    event_type: str,
    mentions_holding: bool,
) -> str:
    """Assign triage level based on pre-score and context."""
    window = RECENCY_WINDOWS.get(event_type, 24)

    if recency_hours > window * 2:
        return "store_only"

    if mentions_holding and pre_score >= 0.50 and recency_hours <= window:
        return "trigger_recalc"

    if pre_score >= 0.40 and recency_hours <= window:
        return "send_to_llm"

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


def _resolve_trigger_type(
    event_type: str,
    mentions_holding: bool,
    impact: str,
    related_assets: list[str],
    watchlist_symbols: set[str],
    universe_symbols: set[str],
) -> str:
    """Assign a richer trigger_type (Part F)."""
    related_set = set(related_assets)

    if mentions_holding:
        if impact == "negativo":
            return "holding_risk"
        if impact == "positivo":
            return "holding_opportunity"
        return "holding_signal"

    if related_set & (watchlist_symbols | universe_symbols):
        return "external_opportunity"

    if event_type in ("tasas", "inflación", "geopolítico"):
        return "macro_risk" if impact == "negativo" else "macro_signal"

    if event_type == "sectorial":
        return "sector_rotation"

    return "news_macro"


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------


def run_ingestion(db: Session, source_label: str = "manual") -> dict:
    """Execute a full ingestion run."""
    settings = get_settings()
    now = datetime.utcnow()

    # Part D: Use real holdings from snapshot
    held_symbols, holdings_source = _load_real_holdings(db)

    run = IngestionRun(source=source_label, status="running", started_at=now, holdings_source=holdings_source)
    db.add(run)
    db.flush()

    try:
        provider = get_news_provider()
        raw_items = provider.get_recent_news(list(held_symbols))
        raw_items = deduplicate_news_items(raw_items)
        run.items_fetched = len(raw_items)

        watchlist_set = set(settings.watchlist_assets)
        universe_set = set(settings.market_universe_assets)

        new_count = 0
        filtered_count = 0
        events_count = 0
        triage_counts = {"store_only": 0, "observe": 0, "send_to_llm": 0, "trigger_recalc": 0}

        for item in raw_items:
            title = (item.get("title") or "").strip()
            summary = (item.get("summary") or "").strip()
            url = item.get("url") or item.get("link", "")
            published_at = item.get("created_at") or item.get("published_at")
            source_name = item.get("source", source_label)

            if not title:
                continue

            dhash = _dedup_hash(title, url)
            existing = db.query(NewsRaw).filter(NewsRaw.dedup_hash == dhash).first()
            if existing:
                continue

            raw_row = NewsRaw(
                ingestion_run_id=run.id,
                source=source_name,
                title=title,
                summary=summary[:2000],
                url=_canonicalize_url(url) or url,
                published_at=published_at if isinstance(published_at, datetime) else None,
                fetched_at=now,
                dedup_hash=dhash,
            )
            db.add(raw_row)
            db.flush()
            new_count += 1

            classified = classify_news_event(title, summary, list(held_symbols))
            pub_dt = raw_row.published_at
            recency_hours = _compute_recency_hours(pub_dt, now)

            # Part E: Multi-source repetition
            th = _topic_hash(title, classified["related_assets"], classified["event_type"])
            recent_cutoff = now - timedelta(hours=72)
            multi_count = (
                db.query(func.count(NewsNormalized.id))
                .filter(NewsNormalized.topic_hash == th)
                .filter(NewsNormalized.created_at >= recent_cutoff)
                .scalar()
            ) or 0
            multi_count += 1  # include current

            mentions_holding = bool(set(classified["related_assets"]) & held_symbols)

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
                multi_source_count=multi_count,
            )

            triage = _assign_triage_level(pre_score, recency_hours, classified["event_type"], mentions_holding)
            triage_counts[triage] = triage_counts.get(triage, 0) + 1

            norm_row = NewsNormalized(
                raw_id=raw_row.id,
                title=title,
                summary=summary[:2000],
                source=source_name,
                url=_canonicalize_url(url) or url,
                published_at=raw_row.published_at,
                event_type=classified["event_type"],
                impact=classified["impact"],
                confidence=classified["confidence"],
                related_assets=classified["related_assets"],
                recency_hours=round(recency_hours, 2),
                pre_score=pre_score,
                triage_level=triage,
                topic_hash=th,
                multi_source_count=multi_count,
            )
            db.add(norm_row)
            db.flush()

            if triage == "store_only":
                filtered_count += 1
                continue

            # Part F: Richer trigger types
            trigger_type = _resolve_trigger_type(
                event_type=classified["event_type"],
                mentions_holding=mentions_holding,
                impact=classified["impact"],
                related_assets=classified["related_assets"],
                watchlist_symbols=watchlist_set,
                universe_symbols=universe_set,
            )

            severity = _severity_from_triage(triage, classified["impact"])

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

    # --- Post-ingestion clustering (best-effort, never breaks ingestion) ---
    clustering_meta = {}
    try:
        clustering_meta = build_or_update_clusters(db)
        db.commit()
    except Exception as exc:
        clustering_meta = {"status": "failed", "error": str(exc)[:200]}
        try:
            db.rollback()
        except Exception:
            pass

    return {
        "status": "completed",
        "run_id": run.id,
        "items_fetched": run.items_fetched,
        "items_new": run.items_new,
        "items_filtered": run.items_filtered,
        "events_created": run.events_created,
        "holdings_source": holdings_source,
        "triage_counts": triage_counts,
        "clustering": clustering_meta,
    }


# ---------------------------------------------------------------------------
# Part A: LLM-eligible news from triage
# ---------------------------------------------------------------------------


def _news_rows_to_dicts(rows) -> list[dict]:
    """Convert NewsNormalized rows to dicts for engine/LLM consumption."""
    return [
        {
            "title": r.title,
            "summary": r.summary,
            "event_type": r.event_type,
            "impact": r.impact,
            "confidence": r.confidence,
            "related_assets": r.related_assets or [],
            "created_at": r.published_at or r.created_at,
            "source": r.source,
            "pre_score": r.pre_score,
            "triage_level": r.triage_level,
            "multi_source_count": r.multi_source_count,
        }
        for r in rows
    ]


def get_engine_eligible_news(db: Session, hours_back: int = 72) -> list[dict]:
    """Return news items eligible for the main recommendation engine.

    Includes observe + send_to_llm + trigger_recalc (excludes store_only).
    This is the primary news input for generate_recommendation().
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    rows = (
        db.query(NewsNormalized)
        .filter(NewsNormalized.triage_level.in_(["observe", "send_to_llm", "trigger_recalc"]))
        .filter(NewsNormalized.created_at >= cutoff)
        .order_by(desc(NewsNormalized.pre_score))
        .limit(30)
        .all()
    )
    return _news_rows_to_dicts(rows)


def get_llm_eligible_news(db: Session, hours_back: int = 72) -> list[dict]:
    """Return news items eligible for LLM analysis (send_to_llm + trigger_recalc).

    This is the ONLY source of news for the LLM layer — stricter than engine.
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    rows = (
        db.query(NewsNormalized)
        .filter(NewsNormalized.triage_level.in_(["send_to_llm", "trigger_recalc"]))
        .filter(NewsNormalized.created_at >= cutoff)
        .order_by(desc(NewsNormalized.pre_score))
        .limit(20)
        .all()
    )
    return _news_rows_to_dicts(rows)


# ---------------------------------------------------------------------------
# Cluster-aware eligible news — same dict format, deduped by cluster
# ---------------------------------------------------------------------------

def _cluster_to_news_dict(cluster: "EventCluster") -> dict:
    """Convert an EventCluster to the same dict format as _news_rows_to_dicts.

    This allows generate_recommendation(), llm_summarize(), and llm_explain()
    to consume clusters without any signature changes.
    """
    return {
        "title": cluster.canonical_title,
        "summary": cluster.consolidated_summary,
        "event_type": cluster.event_type,
        "impact": _cluster_impact(cluster),
        "confidence": cluster.relevance_score,
        "related_assets": cluster.affected_symbols or [],
        "created_at": cluster.latest_published_at or cluster.updated_at,
        "source": ", ".join(cluster.sources_list or []),
        "pre_score": cluster.relevance_score,
        "triage_level": cluster.triage_max,
        "multi_source_count": cluster.source_count,
        # Cluster traceability fields (extra — ignored by legacy consumers)
        "cluster_id": cluster.id,
        "cluster_key": cluster.cluster_key,
        "item_count": cluster.item_count,
        "source_count": cluster.source_count,
        "sources_list": cluster.sources_list or [],
        "relevance_score": cluster.relevance_score,
        "llm_candidate": cluster.llm_candidate,
        "external_opportunity_candidate": cluster.external_opportunity_candidate,
        "affects_holdings": cluster.affects_holdings,
        "affects_watchlist": cluster.affects_watchlist,
        "affected_sectors": cluster.affected_sectors or [],
    }


def _cluster_impact(cluster: "EventCluster") -> str:
    """Derive impact label from cluster event_type heuristic.

    Uses same logic as the classify pipeline but at cluster level.
    """
    negative_types = {"crisis", "regulación", "downgrade", "recesión", "default", "sell-off"}
    positive_types = {"upgrade", "ipo", "earnings_beat", "expansion", "deal"}
    et = (cluster.event_type or "").lower()
    if et in negative_types:
        return "negativo"
    if et in positive_types:
        return "positivo"
    return "neutro"


def get_engine_eligible_clusters(db: Session, hours_back: int = 72, limit: int = 30) -> list[dict]:
    """Return top clusters eligible for the recommendation engine.

    Clusters with triage_max in (observe, send_to_llm, trigger_recalc).
    Returns the same dict format as get_engine_eligible_news() for backward compat.
    Each cluster replaces N individual items → deduplication by event.
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    clusters = (
        db.query(EventCluster)
        .filter(EventCluster.triage_max.in_(["observe", "send_to_llm", "trigger_recalc"]))
        .filter(EventCluster.updated_at >= cutoff)
        .order_by(desc(EventCluster.relevance_score))
        .limit(limit)
        .all()
    )
    return [_cluster_to_news_dict(c) for c in clusters]


def get_llm_eligible_clusters(db: Session, hours_back: int = 72, limit: int = 20) -> list[dict]:
    """Return top clusters eligible for LLM analysis.

    Only clusters with llm_candidate=True (triage_max in send_to_llm/trigger_recalc).
    Returns same dict format as get_llm_eligible_news().
    """
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    clusters = (
        db.query(EventCluster)
        .filter(EventCluster.llm_candidate == True)  # noqa: E712
        .filter(EventCluster.updated_at >= cutoff)
        .order_by(desc(EventCluster.relevance_score))
        .limit(limit)
        .all()
    )
    return [_cluster_to_news_dict(c) for c in clusters]


def has_llm_eligible_news(db: Session, hours_back: int = 72) -> bool:
    """Check if there are any LLM-eligible news items without loading them all."""
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    return (
        db.query(func.count(NewsNormalized.id))
        .filter(NewsNormalized.triage_level.in_(["send_to_llm", "trigger_recalc"]))
        .filter(NewsNormalized.created_at >= cutoff)
        .scalar()
        or 0
    ) > 0


def get_pending_recalc_events(db: Session) -> list[MarketEvent]:
    """Get trigger_recalc events that haven't triggered yet."""
    return (
        db.query(MarketEvent)
        .join(NewsNormalized, MarketEvent.news_normalized_id == NewsNormalized.id)
        .filter(NewsNormalized.triage_level == "trigger_recalc")
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


# ---------------------------------------------------------------------------
# Event clustering — groups NewsNormalized into canonical MarketEvent clusters
# ---------------------------------------------------------------------------

_TRIAGE_RANK = {"store_only": 0, "observe": 1, "send_to_llm": 2, "trigger_recalc": 3}

_SECTOR_KEYWORDS: dict[str, str] = {
    "energia": "energía", "energy": "energía", "oil": "energía", "petrol": "energía",
    "tech": "tecnología", "tecnolog": "tecnología", "software": "tecnología", "ia": "tecnología",
    "financ": "financiero", "banco": "financiero", "bank": "financiero",
    "salud": "salud", "health": "salud", "pharma": "salud",
    "consumo": "consumo", "retail": "consumo",
    "agro": "agro", "agri": "agro", "soja": "agro", "trigo": "agro",
    "mining": "minería", "miner": "minería", "litio": "minería",
    "real estate": "inmobiliario", "inmobil": "inmobiliario",
}


def _compute_time_bucket(dt: datetime | None) -> str:
    """Compute a 12-hour time bucket string from a datetime.

    Format: YYYY-MM-DD_H0 or YYYY-MM-DD_H1 (first/second half of day).
    This ensures that news about the same topic separated by >12h
    land in different clusters, preventing eternal cluster growth.
    """
    if not dt:
        dt = datetime.utcnow()
    half = "H0" if dt.hour < 12 else "H1"
    return f"{dt.strftime('%Y-%m-%d')}_{half}"


def _make_cluster_key(topic_hash: str, time_bucket: str) -> str:
    """Deterministic cluster key: topic_hash + time_bucket."""
    return f"{topic_hash}_{time_bucket}"


def _infer_sectors(title: str, summary: str) -> list[str]:
    """Infer affected sectors from text using keyword matching."""
    text = f"{title} {summary}".lower()
    sectors = set()
    for keyword, sector in _SECTOR_KEYWORDS.items():
        if keyword in text:
            sectors.add(sector)
    return sorted(sectors)


def build_or_update_clusters(db: Session, hours_back: int = 72) -> dict:
    """Group recent NewsNormalized items into EventClusters.

    Clustering key: topic_hash + time_bucket (12h window).
    Creates new clusters or updates existing ones.
    Assigns event_cluster_id FK on each NewsNormalized item.

    Returns summary stats for observability.
    """
    settings = get_settings()
    held_symbols, _ = _load_real_holdings(db)
    watchlist_set = set(settings.watchlist_assets)

    cutoff = datetime.utcnow() - timedelta(hours=hours_back)

    # Fetch all recent NewsNormalized that have a topic_hash
    items = (
        db.query(NewsNormalized)
        .filter(NewsNormalized.created_at >= cutoff)
        .filter(NewsNormalized.topic_hash != "")
        .order_by(NewsNormalized.created_at)
        .all()
    )

    if not items:
        return {"status": "no_items", "clusters_created": 0, "clusters_updated": 0}

    # Group by cluster_key
    groups: dict[str, list[NewsNormalized]] = {}
    for item in items:
        pub_dt = item.published_at or item.created_at
        tb = _compute_time_bucket(pub_dt)
        ck = _make_cluster_key(item.topic_hash, tb)
        groups.setdefault(ck, []).append(item)

    created = 0
    updated = 0

    for cluster_key, group_items in groups.items():
        # Determine topic_hash and time_bucket from key
        parts = cluster_key.rsplit("_", 2)  # topic_hash _ YYYY-MM-DD _ H0/H1
        topic_hash = parts[0] if len(parts) >= 3 else cluster_key.split("_")[0]
        time_bucket = "_".join(parts[-2:]) if len(parts) >= 3 else _compute_time_bucket(None)

        existing = db.query(EventCluster).filter(EventCluster.cluster_key == cluster_key).first()

        # Compute aggregates from group
        all_symbols: list[str] = []
        all_sources: set[str] = set()
        all_sectors: set[str] = set()
        max_score = 0.0
        max_triage = "store_only"
        best_item: NewsNormalized = group_items[0]
        pub_dates: list[datetime] = []
        has_holding_mention = False
        has_watchlist_mention = False
        has_llm = False
        has_external = False

        for item in group_items:
            # Symbols
            for s in (item.related_assets or []):
                if s not in all_symbols:
                    all_symbols.append(s)

            # Sources
            if item.source:
                all_sources.add(item.source)

            # Sectors
            for sec in _infer_sectors(item.title, item.summary):
                all_sectors.add(sec)

            # Published dates
            pub = item.published_at or item.created_at
            if pub:
                pub_dates.append(pub)

            # Best item by pre_score (for canonical title)
            if item.pre_score > best_item.pre_score:
                best_item = item

            # Scores and triage
            if item.pre_score > max_score:
                max_score = item.pre_score
            if _TRIAGE_RANK.get(item.triage_level, 0) > _TRIAGE_RANK.get(max_triage, 0):
                max_triage = item.triage_level

            # Flags
            item_symbols = set(item.related_assets or [])
            if item_symbols & held_symbols:
                has_holding_mention = True
            if item_symbols & watchlist_set:
                has_watchlist_mention = True
            if item.triage_level in ("send_to_llm", "trigger_recalc"):
                has_llm = True
            if item_symbols and not (item_symbols & held_symbols):
                has_external = True

        # Consolidated summary: unique summaries from top-scored items (max 3)
        sorted_items = sorted(group_items, key=lambda x: x.pre_score, reverse=True)
        seen_summaries: set[str] = set()
        summary_parts: list[str] = []
        for si in sorted_items[:3]:
            s_norm = (si.summary or "").strip()
            if s_norm and s_norm.lower() not in seen_summaries:
                seen_summaries.add(s_norm.lower())
                summary_parts.append(s_norm)
        consolidated_summary = " | ".join(summary_parts)[:2000]

        first_pub = min(pub_dates) if pub_dates else None
        latest_pub = max(pub_dates) if pub_dates else None
        sources_sorted = sorted(all_sources)

        if existing:
            existing.canonical_title = best_item.title
            existing.consolidated_summary = consolidated_summary
            existing.event_type = best_item.event_type
            existing.item_count = len(group_items)
            existing.source_count = len(all_sources)
            existing.sources_list = sources_sorted
            existing.first_published_at = first_pub
            existing.latest_published_at = latest_pub
            existing.affected_symbols = all_symbols
            existing.affected_sectors = sorted(all_sectors)
            existing.relevance_score = round(max_score, 3)
            existing.triage_max = max_triage
            existing.affects_holdings = has_holding_mention
            existing.affects_watchlist = has_watchlist_mention
            existing.llm_candidate = has_llm
            existing.external_opportunity_candidate = has_external
            existing.updated_at = datetime.utcnow()
            cluster_id = existing.id
            updated += 1
        else:
            cluster = EventCluster(
                cluster_key=cluster_key,
                topic_hash=topic_hash,
                time_bucket=time_bucket,
                canonical_title=best_item.title,
                consolidated_summary=consolidated_summary,
                event_type=best_item.event_type,
                item_count=len(group_items),
                source_count=len(all_sources),
                sources_list=sources_sorted,
                first_published_at=first_pub,
                latest_published_at=latest_pub,
                affected_symbols=all_symbols,
                affected_sectors=sorted(all_sectors),
                relevance_score=round(max_score, 3),
                triage_max=max_triage,
                affects_holdings=has_holding_mention,
                affects_watchlist=has_watchlist_mention,
                llm_candidate=has_llm,
                external_opportunity_candidate=has_external,
            )
            db.add(cluster)
            db.flush()
            cluster_id = cluster.id
            created += 1

        # Assign FK on all items in this group
        for item in group_items:
            if item.event_cluster_id != cluster_id:
                item.event_cluster_id = cluster_id

    db.flush()
    return {
        "status": "completed",
        "clusters_created": created,
        "clusters_updated": updated,
        "clusters_touched": created + updated,
        "items_clustered": len(items),
    }


def get_recent_clusters(db: Session, limit: int = 20, include_items: bool = False) -> list[dict]:
    """Return recent EventClusters for API consumption."""
    clusters = (
        db.query(EventCluster)
        .order_by(desc(EventCluster.updated_at))
        .limit(limit)
        .all()
    )

    result = []
    for c in clusters:
        entry = {
            "id": c.id,
            "cluster_key": c.cluster_key,
            "topic_hash": c.topic_hash,
            "time_bucket": c.time_bucket,
            "canonical_title": c.canonical_title,
            "consolidated_summary": c.consolidated_summary,
            "event_type": c.event_type,
            "item_count": c.item_count,
            "source_count": c.source_count,
            "sources_list": c.sources_list,
            "first_published_at": c.first_published_at.isoformat() if c.first_published_at else None,
            "latest_published_at": c.latest_published_at.isoformat() if c.latest_published_at else None,
            "affected_symbols": c.affected_symbols,
            "affected_sectors": c.affected_sectors,
            "relevance_score": c.relevance_score,
            "triage_max": c.triage_max,
            "affects_holdings": c.affects_holdings,
            "affects_watchlist": c.affects_watchlist,
            "llm_candidate": c.llm_candidate,
            "external_opportunity_candidate": c.external_opportunity_candidate,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "updated_at": c.updated_at.isoformat() if c.updated_at else None,
        }
        if include_items:
            items = (
                db.query(NewsNormalized)
                .filter(NewsNormalized.event_cluster_id == c.id)
                .order_by(desc(NewsNormalized.pre_score))
                .all()
            )
            entry["items"] = [
                {
                    "id": i.id,
                    "title": i.title,
                    "summary": i.summary,
                    "source": i.source,
                    "url": i.url,
                    "event_type": i.event_type,
                    "impact": i.impact,
                    "confidence": i.confidence,
                    "related_assets": i.related_assets or [],
                    "pre_score": i.pre_score,
                    "triage_level": i.triage_level,
                    "published_at": i.published_at.isoformat() if i.published_at else None,
                }
                for i in items
            ]
        result.append(entry)

    return result
