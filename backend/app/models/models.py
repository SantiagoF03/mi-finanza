from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    total_value: Mapped[float] = mapped_column(Float)
    cash: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(10), default="USD")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    positions: Mapped[list["PortfolioPosition"]] = relationship(back_populates="snapshot", cascade="all, delete")


class PortfolioPosition(Base):
    __tablename__ = "portfolio_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("portfolio_snapshots.id"))
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    asset_type: Mapped[str] = mapped_column(String(30))
    instrument_type: Mapped[str] = mapped_column(String(30), default="UNKNOWN")
    currency: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[float] = mapped_column(Float)
    market_value: Mapped[float] = mapped_column(Float)
    avg_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl_pct: Mapped[float] = mapped_column(Float)
    snapshot: Mapped[PortfolioSnapshot] = relationship(back_populates="positions")


class NewsEvent(Base):
    __tablename__ = "news_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(50))
    impact: Mapped[str] = mapped_column(String(20))
    confidence: Mapped[float] = mapped_column(Float)
    related_assets: Mapped[list[str]] = mapped_column(JSON)
    summary: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Recommendation(Base):
    __tablename__ = "recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    action: Mapped[str] = mapped_column(String(30))
    status: Mapped[str] = mapped_column(String(20), default="pending")
    suggested_pct: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float)
    rationale: Mapped[str] = mapped_column(Text)
    risks: Mapped[str] = mapped_column(Text)
    executive_summary: Mapped[str] = mapped_column(Text)
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    replaced_by_id: Mapped[int | None] = mapped_column(ForeignKey("recommendations.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    actions: Mapped[list["RecommendationAction"]] = relationship(back_populates="recommendation", cascade="all, delete")


class RecommendationAction(Base):
    __tablename__ = "recommendation_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation_id: Mapped[int] = mapped_column(ForeignKey("recommendations.id"))
    symbol: Mapped[str] = mapped_column(String(20))
    target_change_pct: Mapped[float] = mapped_column(Float)
    reason: Mapped[str] = mapped_column(Text)
    recommendation: Mapped[Recommendation] = relationship(back_populates="actions")


class UserDecision(Base):
    __tablename__ = "user_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation_id: Mapped[int] = mapped_column(ForeignKey("recommendations.id"))
    decision: Mapped[str] = mapped_column(String(20))
    note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AppLog(Base):
    __tablename__ = "app_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    level: Mapped[str] = mapped_column(String(10), default="INFO")
    message: Mapped[str] = mapped_column(Text)
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RuleConfig(Base):
    __tablename__ = "rule_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    max_movement_pct: Mapped[float] = mapped_column(Float, default=0.10)
    min_liquidity_pct: Mapped[float] = mapped_column(Float, default=0.05)
    leverage_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    auto_execution_enabled: Mapped[bool] = mapped_column(Boolean, default=False)


# ---------------------------------------------------------------------------
# Market event ingestion & alerting
# ---------------------------------------------------------------------------


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(20), default="running")
    items_fetched: Mapped[int] = mapped_column(Integer, default=0)
    items_new: Mapped[int] = mapped_column(Integer, default=0)
    items_filtered: Mapped[int] = mapped_column(Integer, default=0)
    events_created: Mapped[int] = mapped_column(Integer, default=0)
    alerts_created: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    holdings_source: Mapped[str] = mapped_column(String(30), default="whitelist")  # snapshot | whitelist
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class NewsRaw(Base):
    __tablename__ = "news_raw"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ingestion_run_id: Mapped[int | None] = mapped_column(ForeignKey("ingestion_runs.id"), nullable=True)
    source: Mapped[str] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(String(500))
    summary: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(String(1000), default="")
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    dedup_hash: Mapped[str] = mapped_column(String(64), index=True, default="")


class NewsNormalized(Base):
    __tablename__ = "news_normalized"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_id: Mapped[int] = mapped_column(ForeignKey("news_raw.id"))
    title: Mapped[str] = mapped_column(String(500))
    summary: Mapped[str] = mapped_column(Text, default="")
    source: Mapped[str] = mapped_column(String(200))
    url: Mapped[str] = mapped_column(String(1000), default="")
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    event_type: Mapped[str] = mapped_column(String(50))
    impact: Mapped[str] = mapped_column(String(20))
    confidence: Mapped[float] = mapped_column(Float)
    related_assets: Mapped[list[str]] = mapped_column(JSON, default=list)
    recency_hours: Mapped[float] = mapped_column(Float, default=0.0)
    pre_score: Mapped[float] = mapped_column(Float, default=0.0)
    triage_level: Mapped[str] = mapped_column(String(20), default="store_only")
    topic_hash: Mapped[str] = mapped_column(String(32), default="", index=True)
    multi_source_count: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class MarketEvent(Base):
    __tablename__ = "market_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    news_normalized_id: Mapped[int | None] = mapped_column(ForeignKey("news_normalized.id"), nullable=True)
    event_type: Mapped[str] = mapped_column(String(50))
    severity: Mapped[str] = mapped_column(String(20))
    trigger_type: Mapped[str] = mapped_column(String(30))
    affected_symbols: Mapped[list[str]] = mapped_column(JSON, default=list)
    message: Mapped[str] = mapped_column(Text)
    triggered_recalc: Mapped[bool] = mapped_column(Boolean, default=False)
    recalc_recommendation_id: Mapped[int | None] = mapped_column(ForeignKey("recommendations.id"), nullable=True)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ---------------------------------------------------------------------------
# Execution layer — user-approved order execution
# States: pending → execution_requested → execution_sent →
#   executed | partially_executed | rejected_by_broker | failed
# ---------------------------------------------------------------------------


class OrderExecution(Base):
    __tablename__ = "order_executions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation_id: Mapped[int] = mapped_column(ForeignKey("recommendations.id"))
    recommendation_action_id: Mapped[int | None] = mapped_column(ForeignKey("recommendation_actions.id"), nullable=True)
    symbol: Mapped[str] = mapped_column(String(20))
    side: Mapped[str] = mapped_column(String(10))  # buy | sell
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    target_change_pct: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(30), default="pending")
    broker_order_id: Mapped[str] = mapped_column(String(100), default="")
    broker_response: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str] = mapped_column(Text, default="")
    executed_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    executed_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    recommendation: Mapped[Recommendation] = relationship()


class PushSubscription(Base):
    __tablename__ = "push_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    endpoint: Mapped[str] = mapped_column(Text)
    p256dh: Mapped[str] = mapped_column(String(200))
    auth: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
