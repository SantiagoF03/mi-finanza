from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
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
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
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
    # Explicit unit quantity, honored ONLY for an execution-pilot
    # recommendation (metadata_json.execution_pilot=true). Automatic
    # recommendations always leave this NULL and keep deriving the quantity
    # from target_change_pct.
    quantity_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
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
    event_cluster_id: Mapped[int | None] = mapped_column(ForeignKey("event_clusters.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    event_cluster: Mapped["EventCluster | None"] = relationship(back_populates="items")


class EventCluster(Base):
    __tablename__ = "event_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cluster_key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    topic_hash: Mapped[str] = mapped_column(String(32), index=True)
    time_bucket: Mapped[str] = mapped_column(String(20), index=True)
    canonical_title: Mapped[str] = mapped_column(String(500))
    consolidated_summary: Mapped[str] = mapped_column(Text, default="")
    event_type: Mapped[str] = mapped_column(String(50))
    item_count: Mapped[int] = mapped_column(Integer, default=1)
    source_count: Mapped[int] = mapped_column(Integer, default=1)
    sources_list: Mapped[list[str]] = mapped_column(JSON, default=list)
    first_published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latest_published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    affected_symbols: Mapped[list[str]] = mapped_column(JSON, default=list)
    affected_sectors: Mapped[list[str]] = mapped_column(JSON, default=list)
    relevance_score: Mapped[float] = mapped_column(Float, default=0.0)
    triage_max: Mapped[str] = mapped_column(String(20), default="store_only")
    affects_holdings: Mapped[bool] = mapped_column(Boolean, default=False)
    affects_watchlist: Mapped[bool] = mapped_column(Boolean, default=False)
    llm_candidate: Mapped[bool] = mapped_column(Boolean, default=False)
    external_opportunity_candidate: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    items: Mapped[list["NewsNormalized"]] = relationship(back_populates="event_cluster")


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
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    portfolio_value_used: Mapped[float | None] = mapped_column(Float, nullable=True)
    position_value_used: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantity_planned: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantity_sent: Mapped[float | None] = mapped_column(Float, nullable=True)
    validation_status: Mapped[str] = mapped_column(String(30), default="")
    endpoint_used: Mapped[str] = mapped_column(String(100), default="")
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


# ---------------------------------------------------------------------------
# Instrument Catalog — dynamic universe of tradeable instruments (P1)
# ---------------------------------------------------------------------------


class InstrumentCatalog(Base):
    __tablename__ = "instrument_catalog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    asset_type: Mapped[str] = mapped_column(String(30), default="DESCONOCIDO")
    market: Mapped[str] = mapped_column(String(30), default="BCBA")
    currency: Mapped[str] = mapped_column(String(10), default="ARS")
    tradable: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(50), default="iol_discovery")
    source_category: Mapped[str] = mapped_column(String(50), default="")
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    avg_volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    investable_local: Mapped[bool] = mapped_column(Boolean, default=True)
    eligible_for_external_discovery: Mapped[bool] = mapped_column(Boolean, default=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ---------------------------------------------------------------------------
# Execution Instrument Catalog — execution-grade instrument identity.
#
# Deliberately SEPARATE from InstrumentCatalog: that one is a market-data /
# discovery universe (what may be analysed), this one is the authority on
# what may be *traded* and with which exact identity. An analysis universe
# must never be able to authorise an order by itself.
# ---------------------------------------------------------------------------


class ExecutionInstrument(Base):
    """One tradeable instrument, verified and auditable.

    An entry is only usable while it is complete, active and fresh
    (verified_at within the class catalog_max_age). Anything else fails
    closed with a stable blocking code — there is no wildcard.
    """

    __tablename__ = "execution_instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Identity as the BROKER knows it — this is what travels in the order.
    broker_symbol: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    # Identity as the USER sees it (may differ, e.g. display suffixes).
    display_symbol: Mapped[str] = mapped_column(String(30), default="")
    description: Mapped[str] = mapped_column(String(200), default="")
    country: Mapped[str] = mapped_column(String(30), default="argentina")
    market: Mapped[str] = mapped_column(String(30), default="")
    settlement: Mapped[str] = mapped_column(String(10), default="")
    asset_type: Mapped[str] = mapped_column(String(30), default="")
    instrument_type: Mapped[str] = mapped_column(String(30), default="")
    # "securities" | "fund" — decides WHICH execution contract applies.
    execution_family: Mapped[str] = mapped_column(String(20), default="")
    # Logical class used to resolve the policy: ACCIONES | CEDEARS | FCI.
    execution_class: Mapped[str] = mapped_column(String(20), default="", index=True)
    currency: Mapped[str] = mapped_column(String(10), default="")
    quantity_step: Mapped[float | None] = mapped_column(Float, nullable=True)
    # A FIXED tick, for instruments whose minimum alteration is a constant.
    # Still the authority wherever no dynamic rule is verified.
    price_tick: Mapped[float | None] = mapped_column(Float, nullable=True)
    minimum_quantity: Mapped[float | None] = mapped_column(Float, nullable=True)

    # --- Dynamic price tick -----------------------------------------------
    # For bCBA equities the minimum alteration depends on the ORDER'S PRICE,
    # so a single stored number is only correct while the price stays inside
    # one band. Three deliberately separate things:
    #
    #   price_tick_observed  what we happened to see once. Diagnostic only;
    #                        it never authorises anything.
    #   price_tick_rule      WHICH rule applies (e.g. BYMA_EQUITY_PRICE_BANDS_V1).
    #                        The rule is the durable fact; the tick is not.
    #   effective tick       computed per price, at preview and again at
    #                        preflight. Never persisted as authority.
    #
    # A rule that merely appears here is not trusted: it must also carry a
    # verifying provenance in field_provenance["price_tick_rule"], exactly
    # like every other operative field.
    price_tick_rule: Mapped[str | None] = mapped_column(String(60), nullable=True)
    price_tick_rule_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    price_tick_observed: Mapped[float | None] = mapped_column(Float, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    buy_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    sell_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    quote_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    cancellation_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    # Fund-only metadata (NULL for securities). Present so an FCI entry is
    # never squeezed into the securities contract.
    fund_minimum_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    fund_cutoff_local_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    settlement_delay_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Provenance & freshness.
    source: Mapped[str] = mapped_column(String(50), default="")
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stale_after: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Hash of the raw identity fields the entry was built from. A change in
    # the broker's identity for this symbol changes the hash, which makes the
    # entry unverified again instead of silently drifting.
    raw_identity_hash: Mapped[str] = mapped_column(String(64), default="")
    raw_identity: Mapped[dict] = mapped_column(JSON, default=dict)

    # --- Verification lifecycle -------------------------------------------
    # candidate         resolved read-only, NOT yet executable
    # verified          every operative field has a trustworthy provenance
    # identity_changed  the broker now describes something different →
    #                   frozen until a human accepts or rejects the change
    # rejected          a human refused it
    # stale             verification expired
    # Only `verified` (plus active + fresh) may authorise an order.
    verification_status: Mapped[str] = mapped_column(String(20), default="candidate", index=True)
    # Per-field provenance, e.g. {"currency": "iol_portfolio",
    # "price_tick": "admin_verified_override"}. A class policy may supply a
    # LIMIT but may never claim it verified an instrument's identity.
    field_provenance: Mapped[dict] = mapped_column(JSON, default=dict)
    # When verification_status=identity_changed: what we had vs what the
    # broker now reports. Neither is discarded — a human decides.
    pending_identity: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    previous_identity: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    identity_changed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Append-only record of human verification decisions.
    verification_audit: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExecutionDailyNotional(Base):
    """Per-day, per-class, per-CURRENCY submitted notional.

    Backs max_daily_notional. Incremented ONLY at the moment an order is
    committed as 'submitting' (the point of no return), so a blocked or
    cancelled preflight never consumes daily budget.

    CONCURRENCY: the unique constraint is what makes the budget safe. Without
    it two processes can each insert their own row for the same key and each
    believe the whole daily allowance is theirs. With it, the losing INSERT
    raises IntegrityError and is forced through the conditional UPDATE, which
    carries the limit in its WHERE clause.

    Currency is part of the key: ARS and USD are different budgets and adding
    them together produces a number that means nothing.
    """

    __tablename__ = "execution_daily_notional"
    __table_args__ = (
        UniqueConstraint(
            "trade_date", "execution_class", "currency",
            name="uq_execution_daily_notional_scope",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # Local (America/Argentina/Buenos_Aires) operating day, YYYY-MM-DD.
    trade_date: Mapped[str] = mapped_column(String(10), index=True)
    execution_class: Mapped[str] = mapped_column(String(20), index=True)
    currency: Mapped[str] = mapped_column(String(10), default="")
    submitted_notional: Mapped[float] = mapped_column(Float, default=0.0)
    order_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ---------------------------------------------------------------------------
# FCI — Fondos Comunes de Inversión.
#
# A SEPARATE family with its own contract. A fund is subscribed or redeemed
# by AMOUNT (or cuotapartes) at a valuation that does not exist yet; it has no
# limit price, no order book and no tick. Reusing OrderExecution for it would
# mean carrying meaningless fields and, worse, letting fund state transitions
# ride on securities semantics.
# ---------------------------------------------------------------------------


class FundInstrument(Base):
    """A fund, as identified by IOL's own FCI catalog."""

    __tablename__ = "fund_instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    # Administradora / management company.
    manager: Mapped[str] = mapped_column(String(200), default="")
    currency: Mapped[str] = mapped_column(String(10), default="")
    # Per-FUND cutoff: never hardcoded, never shared between funds.
    cutoff_local_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    settlement_delay_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    minimum_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    minimum_redemption: Mapped[float | None] = mapped_column(Float, nullable=True)
    subscription_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    redemption_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    verification_status: Mapped[str] = mapped_column(String(20), default="candidate", index=True)
    field_provenance: Mapped[dict] = mapped_column(JSON, default=dict)
    source: Mapped[str] = mapped_column(String(50), default="")
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stale_after: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Hash of symbol + currency + manager: what makes this fund THIS fund.
    # An automatic refresh preserves an administrative verification only while
    # this stays the same; a change freezes the fund instead of inheriting an
    # approval that was given for something else.
    identity_hash: Mapped[str] = mapped_column(String(64), default="")
    previous_identity: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    pending_identity: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    raw_detail: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FundOperation(Base):
    """One subscription or redemption. NEVER an OrderExecution.

    States (asynchronous by nature — a fund confirms later, at a valuation
    that did not exist when we submitted):

        prepared → validation_requested → validated → approval_requested
        → submitting → submitted → pending_confirmation → confirmed
        | rejected | cancelled | submission_unknown | reconciliation_required
    """

    __tablename__ = "fund_operations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recommendation_id: Mapped[int | None] = mapped_column(ForeignKey("recommendations.id"), nullable=True)
    fund_symbol: Mapped[str] = mapped_column(String(30), index=True)
    # "subscribe" | "redeem" — never buy/sell.
    operation: Mapped[str] = mapped_column(String(20))
    currency: Mapped[str] = mapped_column(String(10), default="")
    # Amount OR cuotapartes, per the operation's contract. Exactly one is set.
    amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    quotaparts: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="prepared", index=True)
    # IOL's own request identifier, when it returns one.
    broker_operation_id: Mapped[str] = mapped_column(String(100), default="")
    # Result of the soloValidar pre-check, when the contract offers it.
    validation_result: Mapped[dict] = mapped_column(JSON, default=dict)
    # WHEN the validation succeeded, and a hash of exactly WHAT was validated
    # (symbol, amount, operation, currency). Both are required before a
    # submission: a validation is evidence about a specific request, so
    # changing the amount — or letting it go stale — voids it.
    validated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    validated_payload_hash: Mapped[str] = mapped_column(String(64), default="")
    # Hash of the FUND's verified state at validation time: status, identity,
    # cutoff, minimum, currency and the capability this operation needs. A
    # demotion, a withdrawn capability or an identity change moves this hash,
    # which voids the validation even though the request bytes are unchanged.
    validated_fund_hash: Mapped[str] = mapped_column(String(64), default="")
    broker_response: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str] = mapped_column(Text, default="")
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    preview_hash: Mapped[str] = mapped_column(String(64), default="")
    cutoff_local_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    settlement_delay_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FundBudgetReservation(Base):
    """The daily budget ONE fund operation reserved, with its own identity.

    Without this, releasing budget meant recomputing the key from
    `trade_date_for(settings)` at release time — so a release that happened
    after midnight, or after the timezone config changed, decremented a
    DIFFERENT day's row: today's allowance shrank while yesterday's stayed
    spent. The reservation now remembers exactly which row it incremented.

    It also makes release idempotent. `submitted_notional` is real spending
    authority; decrementing it twice would hand out allowance that was never
    given back.
    """

    __tablename__ = "fund_budget_reservations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    # One reservation per operation, enforced by the database: a second claim
    # of the same operation cannot create a second reservation.
    fund_operation_id: Mapped[int] = mapped_column(
        ForeignKey("fund_operations.id"), unique=True, index=True
    )
    trade_date: Mapped[str] = mapped_column(String(10), index=True)
    ledger_class: Mapped[str] = mapped_column(String(30))
    currency: Mapped[str] = mapped_column(String(10), default="")
    reserved_notional: Mapped[float] = mapped_column(Float, default=0.0)
    # reserved | released
    status: Mapped[str] = mapped_column(String(20), default="reserved", index=True)
    reserved_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    released_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    release_reason: Mapped[str] = mapped_column(String(200), default="")


class FundInstrumentVerification(Base):
    """Append-only audit of every administrative decision about a fund.

    A fund read from IOL's catalog is only ever a `candidate`: the operational
    field names are observed, not documented, so reading a plausible cutoff is
    not evidence enough to time real money. Promoting one to `verified` is a
    human act, and this table is the record of who asserted what.

    Rows are never updated or deleted. `data_hash` pins the exact parameter
    set that was verified, so a later change is visible instead of silently
    inheriting an old approval.
    """

    __tablename__ = "fund_instrument_verifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fund_symbol: Mapped[str] = mapped_column(String(30), index=True)
    # verify | reject | demote | identity_changed | auto_refresh_preserved
    action: Mapped[str] = mapped_column(String(30))
    previous_status: Mapped[str] = mapped_column(String(20), default="")
    new_status: Mapped[str] = mapped_column(String(20), default="")
    # The parameters as asserted at this moment. Kept even for a demotion, so
    # the record shows what was withdrawn.
    currency: Mapped[str] = mapped_column(String(10), default="")
    cutoff_local_time: Mapped[str | None] = mapped_column(String(5), nullable=True)
    minimum_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    settlement_delay_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    subscription_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    redemption_supported: Mapped[bool] = mapped_column(Boolean, default=False)
    # Where the assertion comes from (e.g. "official_docs", "fund_prospectus").
    source: Mapped[str] = mapped_column(String(100), default="")
    note: Mapped[str] = mapped_column(Text, default="")
    # Hash of the verified parameter set + the fund's identity.
    data_hash: Mapped[str] = mapped_column(String(64), default="", index=True)
    identity_hash: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class FundOperationDecision(Base):
    """The human decision that authorised a FundOperation."""

    __tablename__ = "fund_operation_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fund_operation_id: Mapped[int] = mapped_column(ForeignKey("fund_operations.id"))
    decision: Mapped[str] = mapped_column(String(20))  # approved | rejected
    note: Mapped[str] = mapped_column(Text, default="")
    preview_hash: Mapped[str] = mapped_column(String(64), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ---------------------------------------------------------------------------
# User Settings — persisted settings (P4)
# ---------------------------------------------------------------------------


class UserSettings(Base):
    __tablename__ = "user_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    value: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ---------------------------------------------------------------------------
# Analysis lease — cross-process mutual exclusion for the analysis cycle
# ---------------------------------------------------------------------------


class AnalysisLease(Base):
    """Single-row persistent lease guarding recommendation creation.

    APScheduler's max_instances=1 only prevents overlap inside ONE process;
    it does nothing against two replicas or overlapping deploys. This row
    lives in the same database as every other write, so a conditional UPDATE
    on it is atomic across processes.

    owner_id is an anonymized, non-sensitive token (never a hostname or a
    secret).
    """

    __tablename__ = "analysis_leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    owner_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    acquired_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
