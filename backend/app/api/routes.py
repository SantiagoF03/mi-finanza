from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.core.config import get_settings
from app.db.session import get_db
from app.models.models import NewsEvent, PortfolioSnapshot, Recommendation, UserDecision
from app.schemas.schemas import DecisionIn
from app.services.orchestrator import get_current_recommendation, run_cycle

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
    return run_cycle(db, source="manual")


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
        "external_opportunities": meta.get("external_opportunities", []),
        "allowed_assets": meta.get("allowed_assets", {}),
        "unchanged": meta.get("unchanged", False),
        "unchanged_reason": meta.get("unchanged_reason", ""),
        "news_summary": meta.get("news_summary"),
        "recommendation_explanation_llm": meta.get("recommendation_explanation_llm"),
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
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        raise HTTPException(404, "Recommendation not found")
    if rec.status not in {"pending", "blocked"}:
        raise HTTPException(400, "Recommendation cerrada")
    if payload.decision not in {"approved", "rejected"}:
        raise HTTPException(400, "Decision debe ser approved o rejected")

    rec.status = payload.decision
    decision = UserDecision(recommendation_id=recommendation_id, decision=payload.decision, note=payload.note)
    db.add(decision)
    db.commit()
    return {"status": "ok", "decision_id": decision.id, "recommendation_status": rec.status}
