from datetime import datetime, timedelta

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.llm.explainer import explain_recommendation as llm_explain, summarize_news as llm_summarize
from app.market.candidates import generate_external_candidates
from app.market.assets import SYMBOL_COMPANY_NAMES, build_catalog_asset_type_map
from app.market.discovery import build_catalog_price_map, fetch_fresh_quotes, get_eligible_universe_symbols, refresh_instrument_catalog
from app.models.models import NewsEvent, PortfolioPosition, PortfolioSnapshot, Recommendation, RecommendationAction
from app.news.ingestion import (
    get_engine_eligible_clusters,
    get_engine_eligible_news,
    get_llm_eligible_clusters,
    get_llm_eligible_news,
    run_ingestion,
)
from app.recommendations.scoring import build_shortlist, curate_llm_input, refine_with_fresh_quotes, score_and_classify_news
from app.news.pipeline import MockNewsProvider, deduplicate_news_items, get_news_provider, get_provider_info
from app.portfolio.analyzer import analyze_portfolio
from app.recommendations.engine import generate_recommendation
from app.recommendations.unchanged import detect_unchanged
from app.recommendations.universe import build_allowed_assets
from app.rules.engine import enforce_rules
from app.services.logs import app_log
from app.services.planner import generate_reallocation_plan

_broker_singletons: dict[str, object] = {}


def _has_causal_link(item: dict) -> bool:
    """Check if a news signal has a defensible causal link to its symbol.

    Returns True if:
    1. The ticker appears in the news title (title_mention), OR
    2. The company/issuer name appears in the news title (reason field).
    """
    if item.get("title_mention"):
        return True
    symbol = item.get("symbol", "")
    reason = item.get("reason", "")
    if not reason or not symbol:
        return False
    names = SYMBOL_COMPANY_NAMES.get(symbol)
    if not names:
        return False
    reason_lower = reason.lower()
    return any(name in reason_lower for name in names)


def _enrich_market_confirmation(opportunities: list[dict]) -> None:
    """Enrich market_confirmation for external_opportunities in-place.

    Upgrades "unconfirmed" → "quote_available" when the instrument has real market
    evidence (in catalog, known_valid, or tracked) but no directional price confirmation.
    Also adds market_confirmation_reason (human-readable).

    SAFE: effective_score only reacts to "confirmed"/"contradicted";
    "quote_available" behaves identically to "unconfirmed" in scoring.
    """
    for opp in opportunities:
        conf = opp.get("market_confirmation")
        in_catalog = "catalog" in (opp.get("source_types") or [])
        is_known = opp.get("asset_type_status") == "known_valid"
        is_tracked = opp.get("tracking_status") not in (None, "untracked")

        # Upgrade: real instrument with market presence but no directional signal
        if conf in ("unconfirmed", None) and is_known and (in_catalog or is_tracked):
            opp["market_confirmation"] = "quote_available"

        # Build human-readable reason
        conf_now = opp.get("market_confirmation")
        if conf_now == "confirmed":
            opp["market_confirmation_reason"] = "movimiento de precio confirma el evento"
        elif conf_now == "contradicted":
            opp["market_confirmation_reason"] = "movimiento de precio contradice el evento"
        elif conf_now == "quote_available":
            detail = "cotización disponible en mercado"
            if in_catalog:
                detail += "; presente en catálogo IOL"
            if is_tracked:
                detail += f"; tracking: {opp.get('tracking_status')}"
            opp["market_confirmation_reason"] = detail
        else:
            opp["market_confirmation_reason"] = "sin datos de mercado disponibles"


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


def _load_news_items(
    snapshot_positions: list[dict],
    provider: "NewsProvider | None" = None,
) -> tuple[list[dict], str, bool, "NewsProvider"]:
    """News loading — returns (items, source_label, is_mock, effective_provider).

    The 4th value is the provider that actually produced the news items.
    If the real provider fails or returns empty and we fall back to mock,
    the returned provider is the MockNewsProvider instance (not the original).
    """
    if provider is None:
        provider = get_news_provider()
    symbols = [p.get("symbol") for p in snapshot_positions if p.get("symbol")]

    items = []
    source = provider.__class__.__name__
    is_mock = isinstance(provider, MockNewsProvider)
    effective_provider = provider

    try:
        items = deduplicate_news_items(provider.get_recent_news(symbols))
    except Exception:
        items = []

    if not items and not isinstance(provider, MockNewsProvider):
        mock_provider = MockNewsProvider()
        source = f"{provider.__class__.__name__}->MockNewsProvider(fallback)"
        items = deduplicate_news_items(mock_provider.get_recent_news(symbols))
        is_mock = True
        effective_provider = mock_provider

    return items, source, is_mock, effective_provider


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


def _build_scoring_summary(scored_news: list[dict]) -> dict:
    """Build observability summary from scored news items.

    ranked_signals_preview: top 5 items ordered by effective_score descending.
    Includes promoted_count, suppressed_count, and confirmation source breakdown.
    """
    by_class: dict[str, int] = {}
    by_confirmation: dict[str, int] = {}
    by_confirmation_source: dict[str, int] = {}
    actionable_count = 0
    observed_count = 0
    promoted_count = 0
    suppressed_count = 0

    entries = []
    for item in scored_news:
        cls = item.get("signal_class", "unknown")
        by_class[cls] = by_class.get(cls, 0) + 1

        if cls == "observed_candidate":
            observed_count += 1
        else:
            actionable_count += 1

        if item.get("promoted_from_observed"):
            promoted_count += 1
        if item.get("suppressed_by_contradiction"):
            suppressed_count += 1

        conf_dict = item.get("market_confirmation") or {}
        conf = conf_dict.get("status", "unknown")
        by_confirmation[conf] = by_confirmation.get(conf, 0) + 1

        conf_source = conf_dict.get("source", "none")
        by_confirmation_source[conf_source] = by_confirmation_source.get(conf_source, 0) + 1

        entries.append({
            "title": item.get("title", "")[:100],
            "signal_score": item.get("signal_score", 0),
            "effective_score": item.get("effective_score", item.get("signal_score", 0)),
            "signal_class": cls,
            "market_confirmation": conf,
            "confirmation_source": conf_source,
            "source_count": item.get("source_count", 1),
            "related_assets": item.get("related_assets", [])[:5],
            "promoted_from_observed": item.get("promoted_from_observed", False),
            "suppressed_by_contradiction": item.get("suppressed_by_contradiction", False),
        })

    # Sort by effective_score descending for preview
    entries.sort(key=lambda x: x["effective_score"], reverse=True)

    # Top actionable preview: exclude suppressed items so the preview
    # shows the real top signals, not noise.
    actionable_entries = [e for e in entries if not e.get("suppressed_by_contradiction")]

    return {
        "total_signals": len(scored_news),
        "actionable_count": actionable_count,
        "observed_count": observed_count,
        "promoted_count": promoted_count,
        "suppressed_count": suppressed_count,
        "by_class": by_class,
        "by_confirmation": by_confirmation,
        "by_confirmation_source": by_confirmation_source,
        "ranked_signals_preview": actionable_entries[:5],
    }


def _extract_cluster_traceability(news_items: list[dict]) -> list[dict]:
    """Extract cluster traceability from cluster-sourced news dicts.

    Only includes items that have cluster_id (from _cluster_to_news_dict).
    """
    result = []
    for item in news_items:
        cid = item.get("cluster_id")
        if cid is not None:
            result.append({
                "cluster_id": cid,
                "cluster_key": item.get("cluster_key"),
                "item_count": item.get("item_count"),
                "source_count": item.get("source_count"),
                "sources_list": item.get("sources_list", []),
                "relevance_score": item.get("relevance_score"),
                "llm_candidate": item.get("llm_candidate"),
                "external_opportunity_candidate": item.get("external_opportunity_candidate"),
                "affects_holdings": item.get("affects_holdings"),
                "affects_watchlist": item.get("affects_watchlist"),
                "affected_sectors": item.get("affected_sectors", []),
            })
    return result


def _build_decision_summary(
    rec: dict,
    scored_news: list[dict],
    scoring_summary: dict,
    llm_input_meta: dict,
    fresh_quote_meta: dict,
    unchanged: bool,
    unchanged_reason: str,
) -> dict:
    """Build a unified decision explainability summary.

    Derives all fields from real pipeline data — never invents explanations.
    The primary_driver is inferred from the engine's actual decision branch
    (action + rationale_reasons), not from a separate scoring pass.
    """
    action = rec.get("action", "mantener")
    actions = rec.get("actions", [])
    rationale_reasons = rec.get("rationale_reasons", [])
    reason_types = {r.get("type") for r in rationale_reasons}

    # --- Primary driver: infer from engine decision ---
    if unchanged:
        primary_driver = "unchanged"
    elif action == "reducir riesgo":
        primary_driver = "concentration"
    elif action == "rebalancear":
        primary_driver = "rebalance"
    elif action == "aumentar posición":
        primary_driver = "positive_signal"
    elif not scored_news:
        primary_driver = "empty_portfolio" if "Portfolio vacío" in rec.get("rationale", "") else "no_signal"
    else:
        primary_driver = "no_signal"

    # --- Winning signal: the item from scored_news that drove the action ---
    winning_signal = None
    if actions and primary_driver in ("positive_signal", "concentration", "rebalance"):
        target_symbol = actions[0].get("symbol")
        if target_symbol:
            # Find the best scored_news item for this symbol
            candidates = [
                n for n in scored_news
                if target_symbol in (n.get("related_assets") or [])
            ]
            if candidates:
                best = max(candidates, key=lambda n: n.get("effective_score", 0))
                winning_signal = {
                    "symbol": target_symbol,
                    "title": best.get("title", "")[:120],
                    "signal_class": best.get("signal_class"),
                    "signal_score": best.get("signal_score"),
                    "effective_score": best.get("effective_score"),
                    "source_count": best.get("source_count", 1),
                    "market_confirmation": (best.get("market_confirmation") or {}).get("status"),
                    "confirmation_source": (best.get("market_confirmation") or {}).get("source"),
                    "promoted_from_observed": best.get("promoted_from_observed", False),
                }

    # --- Confirmation used for winning signal ---
    confirmation_used = {}
    if winning_signal:
        confirmation_used = {
            "status": winning_signal.get("market_confirmation"),
            "source": winning_signal.get("confirmation_source"),
        }

    # --- Shortlist ---
    shortlist_data = fresh_quote_meta.get("shortlist", {})
    shortlist_used = shortlist_data.get("symbols", [])

    # --- LLM input summary ---
    llm_summary = {
        "sent_count": llm_input_meta.get("sent_count", 0),
        "excluded_count": (
            llm_input_meta.get("excluded_suppressed", 0)
            + llm_input_meta.get("excluded_weak", 0)
            + llm_input_meta.get("excluded_observed", 0)
        ),
        "sent_classes": llm_input_meta.get("sent_classes", {}),
    }

    # --- Candidates summary with top 3 from each group ---
    ext_ops = rec.get("external_opportunities", [])
    obs_cands = rec.get("observed_candidates", [])
    sup_cands = rec.get("suppressed_candidates", [])

    def _top_n(items, n=3, sort_key=None):
        source = items
        if sort_key:
            source = sorted(items, key=sort_key, reverse=True)
        return [
            {"symbol": i.get("symbol"), "effective_score": i.get("effective_score"),
             "signal_class": i.get("signal_class"), "market_confirmation": i.get("market_confirmation"),
             "reason": i.get("reason"), "source_types": i.get("source_types"),
             "investable": i.get("investable"), "asset_type_status": i.get("asset_type_status"),
             "title_mention": i.get("title_mention"),
             "observed_origin": i.get("observed_origin"),
             "signal_quality": i.get("signal_quality"),
             "causal_link_strength": i.get("causal_link_strength"),
             "observed_value_tier": i.get("observed_value_tier"),
             "opportunity_quality": i.get("opportunity_quality"),
             "opportunity_rank_reason": i.get("opportunity_rank_reason"),
             "market_confirmation_reason": i.get("market_confirmation_reason"),
             "operational_status": i.get("operational_status")}
            for i in source[:n]
        ]

    # Sort keys for quality ranking:
    # top_actionable: causal strength → title mention → effective_score → source diversity → priority
    # top_observed: known_valid first, then by effective_score/priority_score
    _actionable_key = lambda x: (
        1 if x.get("investable") else 0,
        1 if x.get("causal_link_strength") == "strong" else 0,
        1 if x.get("title_mention") else 0,
        x.get("effective_score") or 0,
        len(x.get("source_types") or []),
        x.get("priority_score") or 0,
    )
    _observed_key = lambda x: (
        2 if x.get("signal_quality") == "strong" else (1 if x.get("signal_quality") == "weak" else 0),
        1 if x.get("causal_link_strength") == "strong" else 0,
        1 if x.get("asset_type_status") == "known_valid" else 0,
        x.get("effective_score") or 0,
        x.get("priority_score") or 0,
    )

    investable_items = [i for i in ext_ops if i.get("investable")]

    # Split observed by signal_quality for explainability
    obs_strong = [i for i in obs_cands if i.get("signal_quality") == "strong"]
    obs_weak = [i for i in obs_cands if i.get("signal_quality") == "weak"]
    obs_catalog = [i for i in obs_cands if i.get("signal_quality") is None]

    # Split observed by value tier for operational clarity
    obs_high = [i for i in obs_cands if i.get("observed_value_tier") == "high"]
    obs_medium = [i for i in obs_cands if i.get("observed_value_tier") == "medium"]
    obs_low = [i for i in obs_cands if i.get("observed_value_tier") == "low"]

    # Relevant non-investable: strong signals the user can't operate yet
    obs_relevant_non_investable = [
        i for i in obs_cands
        if i.get("operational_status") == "relevant_not_investable"
    ]

    promoted_from_observed = [i for i in ext_ops if i.get("promoted_from_observed")]

    candidates = {
        "actionable_count": len(ext_ops),
        "investable_count": len(investable_items),
        "promoted_from_observed_count": len(promoted_from_observed),
        "observed_count": len(obs_cands),
        "observed_with_signal_count": len(obs_strong),
        "observed_weak_signal_count": len(obs_weak),
        "observed_catalog_count": len(obs_catalog),
        "observed_high_value_count": len(obs_high),
        "observed_medium_value_count": len(obs_medium),
        "observed_low_value_count": len(obs_low),
        "relevant_non_investable_count": len(obs_relevant_non_investable),
        "suppressed_count": len(sup_cands),
        "top_actionable": _top_n(ext_ops, sort_key=_actionable_key),
        "top_relevant_non_investable": _top_n(obs_relevant_non_investable, sort_key=_observed_key),
        "top_observed": _top_n(obs_cands, sort_key=_observed_key),
        "top_observed_signals": _top_n(obs_strong, sort_key=_observed_key),
        "top_observed_medium": _top_n(obs_medium, sort_key=_observed_key),
        "top_observed_weak": _top_n(obs_weak, sort_key=_observed_key),
        "top_observed_catalog": _top_n(obs_catalog, sort_key=_observed_key),
        "top_suppressed": _top_n(sup_cands),
    }

    # --- Promotion events ---
    refinement = fresh_quote_meta.get("refinement", {})
    promotion_events = {
        "promoted_count": scoring_summary.get("promoted_count", 0),
        "suppressed_count": scoring_summary.get("suppressed_count", 0),
        "fresh_promoted": refinement.get("promotions", 0),
        "fresh_demoted": refinement.get("demotions", 0),
    }

    # --- Why selected: derive from rationale_reasons ---
    if unchanged:
        why_selected = f"Sin cambios significativos: {unchanged_reason}"
    elif rationale_reasons:
        why_selected = " ".join(r.get("detail", "") for r in rationale_reasons[:3])
    else:
        why_selected = rec.get("rationale", "Sin señales suficientes para actuar.")

    return {
        "primary_driver": primary_driver,
        "winning_signal": winning_signal,
        "confirmation_used": confirmation_used,
        "shortlist_used": shortlist_used,
        "llm_input": llm_summary,
        "candidates": candidates,
        "promotion_events": promotion_events,
        "why_selected": why_selected,
    }


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

    # --- News persistence + observability (same provider instance) ---
    news_provider_instance = get_news_provider()
    news_items, news_source, news_is_mock, news_provider_instance = _load_news_items(
        positions, provider=news_provider_instance,
    )
    inserted_news = _persist_news_without_duplicates(db, news_items)

    # Provider observability — uses the SAME instance that did the actual fetch
    try:
        news_provider_info = get_provider_info(news_provider_instance)
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
    # When use_clusters=True, prefer cluster-level deduplication over raw items
    news_mode = "clusters" if settings.use_clusters else "individual"
    if settings.use_clusters:
        engine_news = get_engine_eligible_clusters(db)
        if not engine_news:
            engine_news = get_engine_eligible_news(db)
            news_mode = "individual_fallback"
    else:
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

    # Build catalog price map for market confirmation on non-holdings
    try:
        catalog_prices = build_catalog_price_map(db)
    except Exception:
        catalog_prices = {}

    allowed_assets = build_allowed_assets(positions, catalog_symbols=catalog_symbols)

    # --- Score and classify news signals (cluster-aware + market confirmation) ---
    scored_news = score_and_classify_news(engine_news, positions, allowed_assets, catalog_prices=catalog_prices)

    # --- Fresh quote refinement for shortlist symbols (best-effort) ---
    fresh_quote_meta = {}
    try:
        shortlist, shortlist_meta = build_shortlist(
            scored_news, allowed_assets.get("holdings", set()),
            known_symbols=allowed_assets.get("all_known"),
        )
        fresh_prices, fetch_meta = fetch_fresh_quotes(db, shortlist, broker=broker)
        if fresh_prices:
            scored_news, refinement_meta = refine_with_fresh_quotes(
                scored_news, fresh_prices, positions,
                catalog_dynamic=allowed_assets.get("catalog_dynamic", set()),
            )
        else:
            refinement_meta = {"refined_count": 0, "symbols_used": [],
                               "promotions": 0, "demotions": 0}
        fresh_quote_meta = {
            "shortlist": shortlist_meta,
            "fetch": fetch_meta,
            "refinement": refinement_meta,
        }
    except Exception as exc:
        fresh_quote_meta = {"error": str(exc)[:200]}

    scoring_summary = _build_scoring_summary(scored_news)

    rec = generate_recommendation(snapshot_dict, analysis, scored_news, settings.max_movement_per_cycle)

    # Replace news-only external_opportunities with full candidate sourcing
    all_candidates = generate_external_candidates(
        news_opportunities=rec.get("external_opportunities", []),
        allowed_assets=allowed_assets,
        positions=positions,
        catalog_map=catalog_map,
    )

    # Split: only truly operable items stay in external_opportunities.
    # Operable = actionable_external AND investable (in whitelist + known_valid).
    # Everything else (catalog-only, actionable-but-not-investable) → observed_candidates.
    rec["external_opportunities"] = [c for c in all_candidates if c.get("actionable_external") and c.get("investable")]
    observed_from_candidates = [c for c in all_candidates if not (c.get("actionable_external") and c.get("investable"))]
    # Merge engine-observed + candidate-observed, deduplicate by symbol.
    # Policy: keep the entry with the best effective_score per symbol,
    # enrich the winner with metadata fields from the loser(s).
    _ENRICH_KEYS = (
        "asset_type_status", "asset_type", "source_types", "investable",
        "actionable_external", "priority_score", "tracking_status",
        "actionable_reason", "in_main_allowed", "asset_type_source",
        "title_mention",
    )
    raw_observed = rec.get("observed_candidates", []) + observed_from_candidates
    seen_observed: dict[str, dict] = {}
    for item in raw_observed:
        sym = item.get("symbol")
        if not sym:
            continue
        if sym not in seen_observed:
            seen_observed[sym] = item
        else:
            existing = seen_observed[sym]
            new_score = item.get("effective_score") or 0
            old_score = existing.get("effective_score") or 0
            if new_score > old_score:
                winner, loser = item, existing
                seen_observed[sym] = winner
            else:
                winner, loser = existing, item
            # Enrich winner with non-None fields from loser
            for key in _ENRICH_KEYS:
                if winner.get(key) is None and loser.get(key) is not None:
                    winner[key] = loser[key]
    merged_observed = list(seen_observed.values())
    # Tag each observed with its origin for explainability:
    # "signal" = has a real news signal (effective_score present)
    # "catalog" = pure catalog/universe/watchlist discovery (no signal data)
    for item in merged_observed:
        has_signal = item.get("effective_score") is not None or item.get("signal_class") is not None
        item["observed_origin"] = "signal" if has_signal else "catalog"
        # signal_quality: "strong" if the symbol is a known financial instrument,
        # "weak" if it has a signal but is unrecognized (GPU, AWS, etc.)
        if has_signal:
            is_known = (
                item.get("asset_type_status") == "known_valid"
                or item.get("in_main_allowed") is True
                or item.get("tracking_status") not in (None, "untracked")
            )
            item["signal_quality"] = "strong" if is_known else "weak"
        else:
            item["signal_quality"] = None
        # causal_link_strength: "strong" if news appears causally related to this symbol,
        # "weak" if the symbol has a signal but the causal link is vague/lateral.
        # Heuristics: (1) ticker in headline, (2) company name in headline.
        if has_signal:
            item["causal_link_strength"] = "strong" if _has_causal_link(item) else "weak"
        else:
            item["causal_link_strength"] = None
        # observed_value_tier: actionable summary of how useful this observed item is.
        # "high"    = known instrument + strong causal link (real news about this company)
        # "medium"  = known instrument + weak causal but investable (contextual, not noise)
        # "low"     = weak instrument OR (known but weak causal + not investable)
        # "catalog" = pure inventory, no news signal at all
        if not has_signal:
            item["observed_value_tier"] = "catalog"
        elif (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
        ):
            item["observed_value_tier"] = "high"
        elif (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "weak"
            and item.get("investable") is True
        ):
            item["observed_value_tier"] = "medium"
        else:
            item["observed_value_tier"] = "low"
        # operational_status: honest product-level classification
        # "relevant_not_investable" = strong signal + strong causal but NOT investable
        # (real opportunity the user can't operate with current config)
        if (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
            and item.get("investable") is not True
        ):
            item["operational_status"] = "relevant_not_investable"
    merged_observed.sort(key=lambda x: (x.get("effective_score") or 0, x.get("priority_score") or 0), reverse=True)

    # --- Observed → Actionable promotion ---
    # Promote observed candidates that meet ALL quality gates to external_opportunities.
    # This allows high-quality signals to become actionable even if they arrived
    # via the observed path (e.g. catalog-discovered symbols with strong news signals).
    # Gate: strong instrument + strong causal link + high score + investable.
    _PROMOTION_SCORE_THRESHOLD = 0.6
    promoted = []
    remaining_observed = []
    for item in merged_observed:
        if (
            item.get("signal_quality") == "strong"
            and item.get("causal_link_strength") == "strong"
            and (item.get("effective_score") or 0) >= _PROMOTION_SCORE_THRESHOLD
            and item.get("investable") is True
        ):
            item["promoted_from_observed"] = True
            item["actionable_external"] = True
            promoted.append(item)
        else:
            remaining_observed.append(item)
    rec["external_opportunities"] = rec.get("external_opportunities", []) + promoted
    rec["observed_candidates"] = remaining_observed

    # --- Opportunity quality & rank reason enrichment ---
    # Tag each external_opportunity with explainability fields BEFORE enforce_rules
    # so they survive into the final output and decision_summary.
    for opp in rec.get("external_opportunities", []):
        opp["operational_status"] = "actionable"
        # opportunity_quality: "top" if strong causal evidence, "standard" otherwise
        has_strong_causal = opp.get("causal_link_strength") == "strong"
        has_title = opp.get("title_mention") is True
        opp["opportunity_quality"] = "top" if (has_strong_causal and has_title) else "standard"

        # opportunity_rank_reason: human-readable explanation of ranking signals
        reasons = []
        if has_title:
            reasons.append("ticker en título")
        if has_strong_causal:
            reasons.append("causalidad fuerte")
        elif opp.get("causal_link_strength") == "weak":
            reasons.append("causalidad débil")
        score = opp.get("effective_score")
        if score is not None:
            reasons.append(f"score {score:.2f}")
        src = opp.get("source_types") or []
        if len(src) >= 2:
            reasons.append(f"{len(src)} fuentes")
        if opp.get("promoted_from_observed"):
            reasons.append("promovido desde observed")
        opp["opportunity_rank_reason"] = "; ".join(reasons) if reasons else "sin señales destacadas"

    # --- Market confirmation enrichment for external_opportunities ---
    _enrich_market_confirmation(rec.get("external_opportunities", []))

    rec = enforce_rules(rec, settings.whitelist_assets, settings.max_movement_per_cycle, holdings=allowed_assets["holdings"])

    # --- Planner: funded reallocation dry-run ---
    proposed_reallocation_plan = {}
    try:
        proposed_reallocation_plan = generate_reallocation_plan(
            snapshot=snapshot_dict,
            analysis=analysis,
            external_opportunities=rec.get("external_opportunities", []),
            allowed_assets=allowed_assets,
            catalog_map=catalog_map,
        )
    except Exception as exc:
        proposed_reallocation_plan = {
            "planner_status": "error",
            "planner_reason": f"Planner falló: {str(exc)[:200]}",
            "dry_run": True,
            "sells_proposed": [],
            "buys_proposed": [],
        }

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
    # Curate LLM input from scored_news (excludes suppressed, weak, observed).
    # Fallback to raw llm_news_items when scored_news is empty (backward compat).
    llm_input_meta = {}
    if scored_news:
        llm_news_items, llm_input_meta = curate_llm_input(scored_news)
    else:
        # Fallback: no scoring available — use raw DB-sourced items
        if settings.use_clusters:
            llm_news_items = get_llm_eligible_clusters(db)
            if not llm_news_items:
                llm_news_items = get_llm_eligible_news(db)
        else:
            llm_news_items = get_llm_eligible_news(db)
        llm_input_meta = {"fallback": True, "sent_count": len(llm_news_items)}

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

    # --- Decision explainability summary ---
    decision_summary = _build_decision_summary(
        rec, scored_news, scoring_summary, llm_input_meta,
        fresh_quote_meta, unchanged, unchanged_reason,
    )

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
            "observed_candidates": rec.get("observed_candidates", []),
            "suppressed_candidates": rec.get("suppressed_candidates", []),
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
            "proposed_reallocation_plan": proposed_reallocation_plan,
            "rebalance_observability": rec.get("rebalance_observability", {}),
            "rationale_reasons": rec.get("rationale_reasons", []),
            "profile_applied": rec.get("profile_applied"),
            "profile_label": rec.get("profile_label"),
            "news_mode": news_mode,
            "cluster_traceability": _extract_cluster_traceability(engine_news) if news_mode != "individual" else None,
            "scoring_summary": scoring_summary,
            "fresh_quote_meta": fresh_quote_meta,
            "llm_input_meta": llm_input_meta,
            "decision_summary": decision_summary,
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
