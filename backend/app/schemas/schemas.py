from datetime import datetime
from typing import List

from pydantic import BaseModel


class PositionOut(BaseModel):
    symbol: str
    asset_type: str
    currency: str
    quantity: float
    market_value: float
    pnl_pct: float


class SnapshotOut(BaseModel):
    id: int
    total_value: float
    cash: float
    currency: str
    created_at: datetime
    positions: List[PositionOut]


class AnalysisOut(BaseModel):
    weights_by_asset: dict
    weights_by_currency: dict
    concentration_score: float
    risk_score: float
    rebalance_deviation: dict
    alerts: list[str]


class NewsOut(BaseModel):
    id: int
    title: str
    event_type: str
    impact: str
    confidence: float
    related_assets: list[str]
    summary: str
    created_at: datetime


class RecommendationActionOut(BaseModel):
    symbol: str
    target_change_pct: float
    reason: str


class RecommendationOut(BaseModel):
    id: int
    action: str
    suggested_pct: float
    confidence: float
    rationale: str
    risks: str
    executive_summary: str
    created_at: datetime
    actions: list[RecommendationActionOut]


class DecisionIn(BaseModel):
    decision: str
    note: str = ""
