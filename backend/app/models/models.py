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
    currency: Mapped[str] = mapped_column(String(10))
    quantity: Mapped[float] = mapped_column(Float)
    market_value: Mapped[float] = mapped_column(Float)
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
