from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.db.session import get_db
from app.market.discovery import get_catalog_instruments, get_eligible_universe_symbols, refresh_instrument_catalog
from app.models.models import MarketEvent, NewsEvent, PortfolioSnapshot, PushSubscription, Recommendation, UserDecision, UserSettings
from app.news.ingestion import get_active_alerts, get_recent_clusters, get_recent_events, run_ingestion
from app.portfolio.profiles import PROFILE_PRESETS, get_profile_label, get_profile_thresholds, resolve_profile
from app.schemas.schemas import DecisionIn
from app.services.execution import (
    approve_and_execute,
    get_execution_by_id,
    get_recent_executions,
    reject_recommendation,
)
from app.services.orchestrator import ensure_review_queue, get_current_recommendation, run_cycle

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/broker/ping")
def broker_ping():
    settings = get_settings()
    client = MockBrokerClient() if settings.broker_mode == "mock" else IolBrokerClient()
    return client.ping()


@router.post("/analysis/run")
def run_manual_analysis(db: Session = Depends(get_db)):
    cycle_result = run_cycle(db, source="manual")
    # Persist notification audit trail (best-effort, does not affect cycle result)
    try:
        from app.notifications.dispatcher import dispatch_recommendation_alerts
        dispatch_recommendation_alerts(db, cycle_result)
    except Exception:
        pass
    return cycle_result


@router.get("/portfolio/summary")
def portfolio_summary(db: Session = Depends(get_db)):
    snapshot = db.query(PortfolioSnapshot).options(joinedload(PortfolioSnapshot.positions)).order_by(desc(PortfolioSnapshot.id)).first()
    if not snapshot:
        raise HTTPException(404, "No snapshots yet")
    return {
        "id": snapshot.id,
        "total_value": snapshot.total_value,
        "cash": snapshot.cash,
        "currency": snapshot.currency,
        "created_at": snapshot.created_at,
        "positions": [
            {
                "symbol": p.symbol,
                "asset_type": p.asset_type,
                "instrument_type": p.instrument_type,
                "currency": p.currency,
                "quantity": p.quantity,
                "market_value": p.market_value,
                "avg_price": p.avg_price,
                "pnl_pct": p.pnl_pct,
            }
            for p in snapshot.positions
        ],
    }


@router.get("/portfolio/analysis")
def portfolio_analysis(db: Session = Depends(get_db)):
    rec = get_current_recommendation(db)
    if not rec:
        rec = db.query(Recommendation).order_by(desc(Recommendation.id)).first()
    if not rec:
        raise HTTPException(404, "No analysis yet")
    return rec.metadata_json.get("analysis", {})


@router.get("/news/recent")
def recent_news(db: Session = Depends(get_db)):
    return db.query(NewsEvent).order_by(desc(NewsEvent.created_at)).limit(10).all()


@router.get("/recommendations/current")
def current_recommendation(db: Session = Depends(get_db)):
    rec = get_current_recommendation(db)
    if not rec:
        raise HTTPException(404, "No active recommendation")
    rec = db.query(Recommendation).options(joinedload(Recommendation.actions)).filter(Recommendation.id == rec.id).first()
    meta = rec.metadata_json or {}
    return {
        "id": rec.id,
        "action": rec.action,
        "status": rec.status,
        "blocked_reason": rec.blocked_reason,
        "suggested_pct": rec.suggested_pct,
        "confidence": rec.confidence,
        "rationale": rec.rationale,
        "risks": rec.risks,
        "executive_summary": rec.executive_summary,
        "created_at": rec.created_at,
        "rules_applied": meta.get("rules", []),
        "broker_mode": meta.get("broker_mode", "unknown"),
        "news_source": meta.get("news_source"),
        "news_is_mock": meta.get("news_is_mock"),
        "news_provider_info": meta.get("news_provider_info", {}),
        "external_opportunities": meta.get("external_opportunities", []),
        "observed_candidates": meta.get("observed_candidates", []),
        "suppressed_candidates": meta.get("suppressed_candidates", []),
        "allowed_assets": meta.get("allowed_assets", {}),
        "unchanged": meta.get("unchanged", False),
        "unchanged_reason": meta.get("unchanged_reason", ""),
        "news_summary": meta.get("news_summary"),
        "recommendation_explanation_llm": meta.get("recommendation_explanation_llm"),
        "rebalance_observability": meta.get("rebalance_observability", {}),
        "rationale_reasons": meta.get("rationale_reasons", []),
        "profile_applied": meta.get("profile_applied"),
        "profile_label": meta.get("profile_label"),
        "proposed_reallocation_plan": meta.get("proposed_reallocation_plan", {}),
        "news_mode": meta.get("news_mode", "individual"),
        "cluster_traceability": meta.get("cluster_traceability") or [],
        "scoring_summary": meta.get("scoring_summary") or {},
        "fresh_quote_meta": meta.get("fresh_quote_meta") or {},
        "decision_summary": ensure_review_queue(meta.get("decision_summary") or {}),
        "notification_audit": meta.get("notification_audit"),
        "actions": [{"symbol": a.symbol, "target_change_pct": a.target_change_pct, "reason": a.reason} for a in rec.actions],
    }


@router.get("/history")
def history(db: Session = Depends(get_db)):
    recs = db.query(Recommendation).order_by(desc(Recommendation.created_at)).limit(50).all()
    decisions = {d.recommendation_id: d for d in db.query(UserDecision).all()}
    return [
        {
            "id": r.id,
            "date": r.created_at,
            "action": r.action,
            "status": r.status,
            "blocked_reason": r.blocked_reason,
            "summary": r.executive_summary,
            "decision": decisions.get(r.id).decision if decisions.get(r.id) else "pendiente",
        }
        for r in recs
    ]


@router.post("/recommendations/{recommendation_id}/decision")
def recommendation_decision(recommendation_id: int, payload: DecisionIn, db: Session = Depends(get_db)):
    """Unified decision endpoint — delegates to approve_and_execute or reject_recommendation.

    This ensures there is exactly ONE semantic path for each decision type:
    - approved → delegates to approve_and_execute (triggers real execution)
    - rejected → delegates to reject_recommendation (no execution)
    """
    if payload.decision not in {"approved", "rejected"}:
        raise HTTPException(400, "Decision debe ser approved o rejected")

    if payload.decision == "approved":
        result = approve_and_execute(db, recommendation_id, note=payload.note or "")
        if "error" in result:
            raise HTTPException(result.get("status_code", 400), result["error"])
        return result

    # rejected
    result = reject_recommendation(db, recommendation_id, note=payload.note or "")
    if "error" in result:
        raise HTTPException(result.get("status_code", 400), result["error"])
    return result


# ---------------------------------------------------------------------------
# Market events & alerts (Part F)
# ---------------------------------------------------------------------------


@router.get("/events/recent")
def recent_events(db: Session = Depends(get_db)):
    return get_recent_events(db, limit=30)


@router.get("/alerts/current")
def current_alerts(db: Session = Depends(get_db)):
    return get_active_alerts(db)


@router.post("/events/run-ingestion")
def manual_ingestion(db: Session = Depends(get_db)):
    return run_ingestion(db, source_label="manual")


@router.get("/events/clusters/recent")
def recent_event_clusters(
    limit: int = 20,
    include_items: bool = False,
    db: Session = Depends(get_db),
):
    """Return recent EventClusters — grouped market events with consolidated metadata.

    Use ?include_items=true to include the individual NewsNormalized items per cluster.
    """
    return get_recent_clusters(db, limit=limit, include_items=include_items)


@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    event = db.query(MarketEvent).filter(MarketEvent.id == alert_id).first()
    if not event:
        raise HTTPException(404, "Alert not found")
    event.acknowledged = True
    db.commit()
    return {"status": "ok", "alert_id": alert_id}


# ---------------------------------------------------------------------------
# Profile settings (GAP 4) — now with DB persistence (P4)
# ---------------------------------------------------------------------------

VALID_PROFILES = {"conservative", "moderate", "moderate_aggressive", "aggressive",
                  "conservador", "moderado", "agresivo"}

_PERSISTED_SETTINGS_KEYS = {
    "investor_profile_target",
    "notification_enabled",
    "notification_min_severity",
    "notification_cooldown_seconds",
    "notification_channel",
    "max_single_asset_weight",
    "max_equity_band",
    "max_us_equity_concentration",
}


def _load_persisted_settings(db: Session) -> None:
    """Load persisted settings from DB into the in-memory Settings singleton."""
    settings = get_settings()
    rows = db.query(UserSettings).filter(UserSettings.key.in_(_PERSISTED_SETTINGS_KEYS)).all()
    for row in rows:
        key = row.key
        val = row.value
        if key == "investor_profile_target" and val:
            settings.investor_profile_target = val
        elif key == "notification_enabled":
            settings.notification_enabled = val.lower() in ("true", "1", "yes")
        elif key == "notification_min_severity" and val:
            settings.notification_min_severity = val
        elif key == "notification_cooldown_seconds" and val:
            try:
                settings.notification_cooldown_seconds = int(val)
            except ValueError:
                pass
        elif key == "notification_channel" and val:
            settings.notification_channel = val
        elif key == "max_single_asset_weight" and val:
            try:
                settings.max_single_asset_weight = float(val)
            except ValueError:
                pass
        elif key == "max_equity_band" and val:
            try:
                settings.max_equity_band = float(val)
            except ValueError:
                pass
        elif key == "max_us_equity_concentration" and val:
            try:
                settings.max_us_equity_concentration = float(val)
            except ValueError:
                pass


def _persist_setting(db: Session, key: str, value: str) -> None:
    """Upsert a single setting into DB."""
    existing = db.query(UserSettings).filter(UserSettings.key == key).first()
    if existing:
        existing.value = value
    else:
        db.add(UserSettings(key=key, value=value))


class ProfileSettingsIn(BaseModel):
    investor_profile_target: str | None = None
    max_single_asset_weight: float | None = None
    max_equity_band: float | None = None
    max_us_equity_concentration: float | None = None


@router.get("/profile/settings")
def get_profile_settings(db: Session = Depends(get_db)):
    _load_persisted_settings(db)
    settings = get_settings()
    profile = settings.investor_profile_target or settings.investor_profile
    canonical = resolve_profile(profile)
    thresholds = get_profile_thresholds(profile)

    return {
        "investor_profile_target": canonical,
        "profile_label": get_profile_label(profile),
        "available_profiles": ["conservative", "moderate", "moderate_aggressive", "aggressive"],
        "thresholds": thresholds,
        "overrides": {
            "max_single_asset_weight": settings.max_single_asset_weight or None,
            "max_equity_band": settings.max_equity_band or None,
            "max_us_equity_concentration": settings.max_us_equity_concentration or None,
        },
        "bucket_targets": PROFILE_PRESETS.get(canonical, PROFILE_PRESETS.get("moderate", {})),
    }


@router.put("/profile/settings")
def update_profile_settings(payload: ProfileSettingsIn, db: Session = Depends(get_db)):
    settings = get_settings()

    if payload.investor_profile_target is not None:
        profile = payload.investor_profile_target
        canonical = resolve_profile(profile)
        if canonical not in {"conservative", "moderate", "moderate_aggressive", "aggressive"}:
            raise HTTPException(400, f"Perfil inválido: {profile}. Válidos: conservative, moderate, moderate_aggressive, aggressive")
        settings.investor_profile_target = canonical
        _persist_setting(db, "investor_profile_target", canonical)

    if payload.max_single_asset_weight is not None:
        if not 0 <= payload.max_single_asset_weight <= 1:
            raise HTTPException(400, "max_single_asset_weight debe estar entre 0 y 1")
        settings.max_single_asset_weight = payload.max_single_asset_weight
        _persist_setting(db, "max_single_asset_weight", str(payload.max_single_asset_weight))

    if payload.max_equity_band is not None:
        if not 0 <= payload.max_equity_band <= 1:
            raise HTTPException(400, "max_equity_band debe estar entre 0 y 1")
        settings.max_equity_band = payload.max_equity_band
        _persist_setting(db, "max_equity_band", str(payload.max_equity_band))

    if payload.max_us_equity_concentration is not None:
        if not 0 <= payload.max_us_equity_concentration <= 1:
            raise HTTPException(400, "max_us_equity_concentration debe estar entre 0 y 1")
        settings.max_us_equity_concentration = payload.max_us_equity_concentration
        _persist_setting(db, "max_us_equity_concentration", str(payload.max_us_equity_concentration))

    db.commit()
    return get_profile_settings(db)


# ---------------------------------------------------------------------------
# Execution layer — approve triggers IOL execution (Priority 2)
# ---------------------------------------------------------------------------


class ApproveIn(BaseModel):
    note: str = ""


@router.post("/recommendations/{recommendation_id}/approve")
def approve_recommendation_endpoint(recommendation_id: int, payload: ApproveIn = None, db: Session = Depends(get_db)):
    """Approve a recommendation and trigger order execution via broker.

    This is THE ONLY way to trigger real execution. Scheduler NEVER executes orders.
    """
    note = payload.note if payload else ""
    result = approve_and_execute(db, recommendation_id, note=note)
    if "error" in result:
        raise HTTPException(result.get("status_code", 400), result["error"])
    return result


@router.post("/recommendations/{recommendation_id}/reject")
def reject_recommendation_endpoint(recommendation_id: int, payload: ApproveIn = None, db: Session = Depends(get_db)):
    """Reject a recommendation. No orders are placed."""
    note = payload.note if payload else ""
    result = reject_recommendation(db, recommendation_id, note=note)
    if "error" in result:
        raise HTTPException(result.get("status_code", 400), result["error"])
    return result


@router.get("/executions/recent")
def recent_executions(db: Session = Depends(get_db)):
    return get_recent_executions(db, limit=20)


@router.get("/executions/{execution_id}")
def get_execution(execution_id: int, db: Session = Depends(get_db)):
    result = get_execution_by_id(db, execution_id)
    if not result:
        raise HTTPException(404, "Execution not found")
    return result


# ---------------------------------------------------------------------------
# Notification settings (Priority 3) — now with DB persistence (P4)
# ---------------------------------------------------------------------------


class NotificationSettingsIn(BaseModel):
    notification_enabled: bool | None = None
    notification_min_severity: str | None = None
    notification_cooldown_seconds: int | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None


@router.get("/notifications/settings")
def get_notification_settings(db: Session = Depends(get_db)):
    _load_persisted_settings(db)
    settings = get_settings()
    return {
        "notification_enabled": settings.notification_enabled,
        "notification_channel": settings.notification_channel,
        "notification_min_severity": settings.notification_min_severity,
        "notification_cooldown_seconds": settings.notification_cooldown_seconds,
        "telegram_configured": bool(settings.telegram_bot_token and settings.telegram_chat_id),
    }


@router.put("/notifications/settings")
def update_notification_settings(payload: NotificationSettingsIn, db: Session = Depends(get_db)):
    settings = get_settings()

    if payload.notification_enabled is not None:
        settings.notification_enabled = payload.notification_enabled
        _persist_setting(db, "notification_enabled", str(payload.notification_enabled))

    if payload.notification_min_severity is not None:
        valid = {"low", "medium", "high", "critical"}
        if payload.notification_min_severity not in valid:
            raise HTTPException(400, f"Severidad inválida. Válidas: {', '.join(sorted(valid))}")
        settings.notification_min_severity = payload.notification_min_severity
        _persist_setting(db, "notification_min_severity", payload.notification_min_severity)

    if payload.notification_cooldown_seconds is not None:
        if payload.notification_cooldown_seconds < 0:
            raise HTTPException(400, "Cooldown debe ser >= 0")
        settings.notification_cooldown_seconds = payload.notification_cooldown_seconds
        _persist_setting(db, "notification_cooldown_seconds", str(payload.notification_cooldown_seconds))

    if payload.telegram_bot_token is not None:
        settings.telegram_bot_token = payload.telegram_bot_token

    if payload.telegram_chat_id is not None:
        settings.telegram_chat_id = payload.telegram_chat_id

    db.commit()
    return get_notification_settings(db)


# ---------------------------------------------------------------------------
# Web Push subscriptions (Priority 3 — PWA)
# ---------------------------------------------------------------------------


class PushSubscriptionIn(BaseModel):
    endpoint: str
    keys: dict  # {p256dh: str, auth: str}


@router.post("/push/subscribe")
def push_subscribe(payload: PushSubscriptionIn, db: Session = Depends(get_db)):
    """Register a web push subscription."""
    existing = db.query(PushSubscription).filter(PushSubscription.endpoint == payload.endpoint).first()
    if existing:
        return {"status": "already_subscribed", "id": existing.id}

    sub = PushSubscription(
        endpoint=payload.endpoint,
        p256dh=payload.keys.get("p256dh", ""),
        auth=payload.keys.get("auth", ""),
    )
    db.add(sub)
    db.commit()
    return {"status": "subscribed", "id": sub.id}


@router.get("/push/vapid-public-key")
def get_vapid_public_key():
    settings = get_settings()
    return {"vapid_public_key": settings.vapid_public_key}


@router.post("/push/test")
def push_test(db: Session = Depends(get_db)):
    """Send a test push notification to all active subscriptions."""
    from app.notifications.dispatcher import send_web_push_to_all
    result = send_web_push_to_all(
        db,
        title="Mi Finanza - Test",
        body="Push notifications funcionan correctamente.",
        severity="low",
        deep_link="/",
    )
    return result


# ---------------------------------------------------------------------------
# Instrument Catalog & Dynamic Universe (Priority 1)
# ---------------------------------------------------------------------------


@router.get("/instruments/catalog")
def get_instruments_catalog(
    active_only: bool = True,
    eligible_only: bool = False,
    asset_type: str | None = None,
    db: Session = Depends(get_db),
):
    """Get instruments from the catalog with optional filters."""
    asset_types = [asset_type] if asset_type else None
    instruments = get_catalog_instruments(
        db,
        active_only=active_only,
        eligible_only=eligible_only,
        asset_types=asset_types,
    )
    return {
        "count": len(instruments),
        "instruments": instruments,
    }


@router.post("/instruments/refresh")
def refresh_instruments(db: Session = Depends(get_db)):
    """Refresh the instrument catalog from IOL or static seed."""
    result = refresh_instrument_catalog(db)
    return result


@router.get("/universe/current")
def current_universe(db: Session = Depends(get_db)):
    """Get the current dynamic universe: holdings + catalog eligible + config."""
    from app.recommendations.universe import build_allowed_assets

    # Get latest snapshot positions
    snapshot = (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )
    positions = []
    if snapshot:
        positions = [
            {"symbol": p.symbol, "asset_type": p.asset_type}
            for p in snapshot.positions
        ]

    catalog_symbols = get_eligible_universe_symbols(db)
    allowed = build_allowed_assets(positions, catalog_symbols=catalog_symbols)

    return {
        "holdings": sorted(allowed["holdings"]),
        "whitelist": sorted(allowed["whitelist"]),
        "watchlist": sorted(allowed["watchlist"]),
        "universe_curated": sorted(allowed.get("universe_curated", set())),
        "catalog_dynamic_count": len(allowed.get("catalog_dynamic", set())),
        "catalog_dynamic_sample": sorted(list(allowed.get("catalog_dynamic", set())))[:50],
        "universe_total": len(allowed["universe"]),
        "universe_sample": sorted(list(allowed["universe"]))[:50],
        "main_allowed": sorted(allowed["main_allowed"]),
        "external_allowed_count": len(allowed["external_allowed"]),
        "all_known_count": len(allowed["all_known"]),
    }
