from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.llm.explainer import explain_recommendation as llm_explain, summarize_news as llm_summarize
from app.market.candidates import generate_external_candidates
from app.market.assets import build_catalog_asset_type_map
from app.market.discovery import get_eligible_universe_symbols, refresh_instrument_catalog
from app.models.models import NewsEvent, PortfolioPosition, PortfolioSnapshot, Recommendation, RecommendationAction
from app.news.ingestion import get_engine_eligible_news, get_llm_eligible_news, run_ingestion
from app.news.pipeline import MockNewsProvider, deduplicate_news_items, get_news_provider, get_provider_info
from app.portfolio.analyzer import analyze_portfolio
from app.recommendations.engine import generate_recommendation
from app.recommendations.unchanged import detect_unchanged
from app.recommendations.universe import build_allowed_assets
from app.rules.engine import enforce_rules
from app.services.logs import app_log

_broker_singletons: dict[str, object] = {}


def _get_broker():
    settings = get_settings()
    if settings.broker_mode == "mock":
        if "mock" not in _broker_singletons:
            _broker_singletons["mock"] = MockBrokerClient()
        return _broker_singletons["mock"]

    if "real" not in _broker_singletons:
        _broker_singletons["real"] = IolBrokerClient()
    return _broker_singletons["real"]


def get_current_recommendation(db: Session) -> Recommendation | None:
    return (
        db.query(Recommendation)
        .filter(Recommendation.status.in_(["pending", "blocked"]))
        .order_by(desc(Recommendation.created_at))
        .first()
    )


def _supersede_open_recommendations(db: Session, new_id: int) -> None:
    open_recs = db.query(Recommendation).filter(Recommendation.status.in_(["pending", "blocked"])).all()
    for rec in open_recs:
        if rec.id != new_id:
            rec.status = "superseded"
            rec.replaced_by_id = new_id
            rec.superseded_at = datetime.utcnow()


def _load_news_items(snapshot_positions: list[dict]) -> tuple[list[dict], str, bool]:
    """Legacy news loading — used only for NewsEvent persistence (backward compat).

    Returns (items, source_label, is_mock) where is_mock=True when MockNewsProvider
    is the actual data source (whether directly or via fallback).
    """
    provider = get_news_provider()
    symbols = [p.get("symbol") for p in snapshot_positions if p.get("symbol")]

    items = []
    source = provider.__class__.__name__
    is_mock = isinstance(provider, MockNewsProvider)

    try:
        items = deduplicate_news_items(provider.get_recent_news(symbols))
    except Exception:
        items = []

    if not items and not isinstance(provider, MockNewsProvider):
        mock_provider = MockNewsProvider()
        source = f"{provider.__class__.__name__}->MockNewsProvider(fallback)"
        items = deduplicate_news_items(mock_provider.get_recent_news(symbols))
        is_mock = True

    return items, source, is_mock


# Fields accepted by NewsEvent constructor (must match model columns)
_NEWS_EVENT_FIELDS = {
    "title", "event_type", "impact", "confidence", "related_assets",
    "summary", "source", "url", "published_at", "created_at",
}


def _persist_news_without_duplicates(db: Session, news_items: list[dict]) -> int:
    inserted = 0
    for n in news_items:
        title = (n.get("title") or "").strip()
        summary = (n.get("summary") or "").strip()
        if not title:
            continue

        # Dedup: check by title+summary, and by URL if present
        url_val = (n.get("url") or "").strip()
        exists = (
            db.query(NewsEvent)
            .filter(NewsEvent.title == title)
            .filter(NewsEvent.summary == summary)
            .first()
        )
        if not exists and url_val:
            exists = (
                db.query(NewsEvent)
                .filter(NewsEvent.url == url_val)
                .first()
            )
        if exists:
            continue

        # Sanitize: only pass known NewsEvent fields (P1 fix)
        safe_payload = {k: v for k, v in n.items() if k in _NEWS_EVENT_FIELDS}

        try:
            db.add(NewsEvent(**safe_payload))
            inserted += 1
        except Exception:
            # P3: skip malformed item, don't crash the whole cycle
            db.rollback()
            continue
    return inserted


def run_cycle(db: Session, source: str = "manual") -> dict:
    settings = get_settings()

    latest = db.query(Recommendation).order_by(desc(Recommendation.created_at)).first()
    if latest:
        cooldown_until = latest.created_at + timedelta(seconds=settings.trigger_cooldown_seconds)
        now = datetime.utcnow()
        if now < cooldown_until:
            remaining_seconds = int((cooldown_until - now).total_seconds())
            remaining_minutes = round(remaining_seconds / 60, 2)
            app_log(
                db,
                "Análisis omitido por cooldown (idempotencia)",
                context={"recommendation_id": latest.id, "source": source, "remaining_seconds": remaining_seconds},
            )
            return {
                "status": "cooldown",
                "skipped": True,
                "message": "Todavía no podés generar una nueva recomendación.",
                "cooldown_remaining_seconds": remaining_seconds,
                "cooldown_remaining_minutes": remaining_minutes,
                "reason": "cooldown",
                "recommendation_id": latest.id,
            }

    broker = _get_broker()
    broker_mode = settings.broker_mode
    try:
        raw = broker.get_portfolio_snapshot()
    except Exception as exc:
        if settings.broker_mode == "real":
            app_log(db, "Fallo broker real, fallback a mock", level="WARNING", context={"source": source, "error": str(exc)})
            broker_mode = "mock_fallback"
            raw = MockBrokerClient().get_portfolio_snapshot()
        else:
            raise

    raw_cash = raw.get("cash", 0)
    positions = raw.get("positions", [])
    total = raw_cash + sum(p.get("market_value", 0) for p in positions)

    snapshot = PortfolioSnapshot(total_value=total, cash=raw_cash, currency=raw.get("currency", "USD"))
    db.add(snapshot)
    db.flush()
    for p in positions:
        db.add(PortfolioPosition(snapshot_id=snapshot.id, **p))

    # --- Legacy news persistence (backward compat for NewsEvent table) ---
    news_items, news_source, news_is_mock = _load_news_items(positions)
    inserted_news = _persist_news_without_duplicates(db, news_items)

    # Provider observability
    try:
        news_provider_info = get_provider_info(get_news_provider())
    except Exception:
        news_provider_info = {"provider_class": "unknown", "is_mock": True}

    # --- Ingestion: ensure triage pipeline has run (best-effort) ---
    ingestion_meta = {}
    try:
        ingestion_result = run_ingestion(db, source_label=f"cycle_{source}")
        ingestion_meta = {
            "ingestion_status": ingestion_result.get("status"),
            "items_fetched": ingestion_result.get("items_fetched", 0),
            "items_new": ingestion_result.get("items_new", 0),
            "triage_counts": ingestion_result.get("triage_counts", {}),
            "holdings_source": ingestion_result.get("holdings_source", "unknown"),
        }
    except Exception as exc:
        ingestion_meta = {"ingestion_status": "failed", "ingestion_error": str(exc)[:200]}

    # --- Main engine uses triaged news (observe + send_to_llm + trigger_recalc) ---
    engine_news = get_engine_eligible_news(db)

    snapshot_dict = {
        "total_value": total,
        "cash": raw_cash,
        "currency": raw.get("currency", "USD"),
        "positions": positions,
    }
    analysis = analyze_portfolio(snapshot_dict)

    # Build dynamic universe from instrument_catalog — auto-bootstrap if empty or stale
    try:
        catalog_symbols = get_eligible_universe_symbols(db)

        # Auto-bootstrap: if catalog is empty, seed it
        if not catalog_symbols:
            app_log(db, "Catálogo vacío, ejecutando bootstrap automático", context={"source": source})
            try:
                refresh_instrument_catalog(db)
                catalog_symbols = get_eligible_universe_symbols(db)
            except Exception as exc:
                app_log(db, "Bootstrap de catálogo falló (safe fallback)", level="WARNING",
                        context={"error": str(exc)[:200]})

        # Staleness check: refresh if catalog hasn't been updated in 24h
        if catalog_symbols:
            try:
                from app.models.models import InstrumentCatalog
                from sqlalchemy import func
                last_seen = db.query(func.max(InstrumentCatalog.last_seen_at)).scalar()
                if last_seen:
                    from datetime import timezone as tz
                    if last_seen.tzinfo is None:
                        last_seen = last_seen.replace(tzinfo=tz.utc)
                    staleness = datetime.now(tz.utc) - last_seen
                    if staleness > timedelta(hours=24):
                        app_log(db, "Catálogo stale, refrescando", context={
                            "staleness_hours": round(staleness.total_seconds() / 3600, 1),
                        })
                        try:
                            refresh_instrument_catalog(db)
                            catalog_symbols = get_eligible_universe_symbols(db)
                        except Exception:
                            pass  # keep existing catalog_symbols
            except Exception:
                pass  # staleness check is best-effort

    except Exception:
        catalog_symbols = set()

    # Build catalog_map for asset_type classification priority
    try:
        catalog_map = build_catalog_asset_type_map(db)
    except Exception:
        catalog_map = {}

    allowed_assets = build_allowed_assets(positions, catalog_symbols=catalog_symbols)
    rec = generate_recommendation(snapshot_dict, analysis, engine_news, settings.max_movement_per_cycle)

    # Replace news-only external_opportunities with full candidate sourcing
    rec["external_opportunities"] = generate_external_candidates(
        news_opportunities=rec.get("external_opportunities", []),
        allowed_assets=allowed_assets,
        positions=positions,
        catalog_map=catalog_map,
    )

    rec = enforce_rules(rec, settings.whitelist_assets, settings.max_movement_per_cycle, holdings=allowed_assets["holdings"])

    # --- Unchanged detection ---
    prev_rec = (
        db.query(Recommendation)
        .filter(Recommendation.status.in_(["pending", "blocked", "superseded", "approved", "rejected"]))
        .order_by(desc(Recommendation.created_at))
        .first()
    )
    rec["_news_items"] = engine_news
    unchanged, unchanged_reason = detect_unchanged(
        rec,
        prev_rec,
        analysis,
        pct_threshold=settings.recommendation_unchanged_pct_threshold,
        risk_threshold=settings.recommendation_unchanged_risk_threshold,
    )
    rec.pop("_news_items", None)

    # --- LLM explanation layer (best-effort, never breaks cycle) ---
    # LLM only receives send_to_llm + trigger_recalc (stricter than engine)
    llm_news_items = get_llm_eligible_news(db)

    news_summary = None
    recommendation_explanation_llm = None
    if llm_news_items:
        try:
            news_summary = llm_summarize(llm_news_items, snapshot_dict, analysis)
        except Exception:
            news_summary = None

        try:
            recommendation_explanation_llm = llm_explain(
                rec, snapshot_dict, analysis, llm_news_items, unchanged=unchanged
            )
        except Exception:
            recommendation_explanation_llm = None

    rec_model = Recommendation(
        action=rec["action"],
        status=rec["status"],
        suggested_pct=rec["suggested_pct"],
        confidence=rec["confidence"],
        rationale=rec["rationale"],
        risks=rec["risks"],
        executive_summary=rec["executive_summary"],
        blocked_reason=rec.get("blocked_reason", ""),
        metadata_json={
            "analysis": analysis,
            "rules": rec.get("blocked_reasons", []),
            "source": source,
            "broker_mode": broker_mode,
            "news_source": news_source,
            "news_is_mock": news_is_mock,
            "news_inserted": inserted_news,
            "news_used_engine": len(engine_news),
            "news_used_llm": len(llm_news_items),
            "news_provider_info": news_provider_info,
            "ingestion": ingestion_meta,
            "external_opportunities": rec.get("external_opportunities", []),
            "allowed_assets": {
                "holdings": sorted(allowed_assets["holdings"]),
                "whitelist": sorted(allowed_assets["whitelist"]),
                "watchlist": sorted(allowed_assets["watchlist"]),
                "universe_curated": sorted(allowed_assets.get("universe_curated", set())),
                "catalog_dynamic": sorted(list(allowed_assets.get("catalog_dynamic", set()))[:50]),
                "catalog_dynamic_count": len(allowed_assets.get("catalog_dynamic", set())),
                "universe": sorted(list(allowed_assets["universe"])[:50]),  # cap for metadata size
                "main_allowed": sorted(allowed_assets["main_allowed"]),
            },
            "unchanged": unchanged,
            "unchanged_reason": unchanged_reason,
            "news_summary": news_summary,
            "recommendation_explanation_llm": recommendation_explanation_llm,
            "rebalance_observability": rec.get("rebalance_observability", {}),
            "rationale_reasons": rec.get("rationale_reasons", []),
            "profile_applied": rec.get("profile_applied"),
            "profile_label": rec.get("profile_label"),
        },
    )
    db.add(rec_model)
    db.flush()
    for item in rec["actions"]:
        db.add(RecommendationAction(recommendation_id=rec_model.id, **item))

    _supersede_open_recommendations(db, rec_model.id)
    db.commit()
    app_log(
        db,
        "Ciclo de análisis ejecutado",
        context={
            "recommendation_id": rec_model.id,
            "source": source,
            "broker_mode": broker_mode,
            "news_source": news_source,
            "news_inserted": inserted_news,
            "unchanged": unchanged,
            "profile_applied": rec.get("profile_applied"),
        },
    )

    return {
        "snapshot_id": snapshot.id,
        "recommendation_id": rec_model.id,
        "analysis": analysis,
        "status": rec_model.status,
        "blocked_reason": rec_model.blocked_reason,
        "broker_mode": broker_mode,
        "news_source": news_source,
        "unchanged": unchanged,
        "unchanged_reason": unchanged_reason,
    }
