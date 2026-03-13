from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.llm.explainer import explain_recommendation as llm_explain, summarize_news as llm_summarize
from app.market.candidates import generate_external_candidates
from app.market.discovery import get_eligible_universe_symbols
from app.models.models import NewsEvent, PortfolioPosition, PortfolioSnapshot, Recommendation, RecommendationAction
from app.news.ingestion import get_engine_eligible_news, get_llm_eligible_news, run_ingestion
from app.news.pipeline import MockNewsProvider, deduplicate_news_items, get_news_provider
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


def _load_news_items(snapshot_positions: list[dict]) -> tuple[list[dict], str]:
    """Legacy news loading — used only for NewsEvent persistence (backward compat)."""
    provider = get_news_provider()
    symbols = [p.get("symbol") for p in snapshot_positions if p.get("symbol")]

    items = []
    source = provider.__class__.__name__
    try:
        items = deduplicate_news_items(provider.get_recent_news(symbols))
    except Exception:
        items = []

    if not items and not isinstance(provider, MockNewsProvider):
        mock_provider = MockNewsProvider()
        source = f"{provider.__class__.__name__}->MockNewsProvider"
        items = deduplicate_news_items(mock_provider.get_recent_news(symbols))

    return items, source


def _persist_news_without_duplicates(db: Session, news_items: list[dict]) -> int:
    inserted = 0
    for n in news_items:
        title = (n.get("title") or "").strip()
        summary = (n.get("summary") or "").strip()
        if not title:
            continue
        exists = (
            db.query(NewsEvent)
            .filter(NewsEvent.title == title)
            .filter(NewsEvent.summary == summary)
            .first()
        )
        if exists:
            continue
        db.add(NewsEvent(**n))
        inserted += 1
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
    news_items, news_source = _load_news_items(positions)
    inserted_news = _persist_news_without_duplicates(db, news_items)

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

    # Build dynamic universe from instrument_catalog (P1)
    try:
        catalog_symbols = get_eligible_universe_symbols(db)
    except Exception:
        catalog_symbols = set()

    allowed_assets = build_allowed_assets(positions, catalog_symbols=catalog_symbols)
    rec = generate_recommendation(snapshot_dict, analysis, engine_news, settings.max_movement_per_cycle)

    # Replace news-only external_opportunities with full candidate sourcing
    rec["external_opportunities"] = generate_external_candidates(
        news_opportunities=rec.get("external_opportunities", []),
        allowed_assets=allowed_assets,
        positions=positions,
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
            "news_inserted": inserted_news,
            "news_used_engine": len(engine_news),
            "news_used_llm": len(llm_news_items),
            "ingestion": ingestion_meta,
            "external_opportunities": rec.get("external_opportunities", []),
            "allowed_assets": {
                "holdings": sorted(allowed_assets["holdings"]),
                "whitelist": sorted(allowed_assets["whitelist"]),
                "watchlist": sorted(allowed_assets["watchlist"]),
                "universe": sorted(list(allowed_assets["universe"])[:50]),  # cap for metadata size
                "catalog_dynamic_count": len(allowed_assets.get("catalog_dynamic", set())),
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
