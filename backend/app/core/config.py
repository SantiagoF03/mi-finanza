import math
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _finite_number(value) -> float | None:
    """Parse a setting into a finite float, rejecting NaN/Infinity/bools/junk.

    Booleans are rejected explicitly: True must never stand in for 1 in a
    financial threshold.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError, AttributeError):
        return None
    if not dec.is_finite():
        return None
    num = float(dec)
    if not math.isfinite(num):
        return None
    return num


class Settings(BaseSettings):
    app_name: str = "Mi Finanza MVP"
    environment: str = "dev"
    database_url: str = "sqlite:///./mi_finanza.db"
    broker_mode: str = "mock"
    iol_api_base: str = "https://api.invertironline.com"
    iol_username: str = ""
    iol_password: str = ""
    iol_portfolio_country: str = "argentina"
    iol_timeout_seconds: int = 15
    # DEPRECATED: iol_use_sandbox=true is mapped to broker_mode=sandbox by
    # app.broker.environment.resolve_execution_environment. Prefer
    # BROKER_MODE=sandbox explicitly.
    iol_use_sandbox: bool = False

    # --- Explicit broker environments (IOL Execution Contract V1) ---
    # Real: falls back to the legacy iol_api_base/iol_username/iol_password
    # so the current production deploy keeps working unchanged.
    iol_real_api_base: str = "https://api.invertironline.com"
    iol_real_username: str = ""
    iol_real_password: str = ""
    # Sandbox: fail closed — empty base/credentials make sandbox unusable.
    # Never shares tokens or credentials with real.
    iol_sandbox_api_base: str = ""
    iol_sandbox_username: str = ""
    iol_sandbox_password: str = ""
    # Separate lock for simulated (sandbox) order submission.
    sandbox_execution_enabled: bool = False

    # --- IOL order policy (fail closed: empty/zero blocks execution) ---
    # Market and settlement MUST be configured explicitly — never hardcoded.
    iol_order_market: str = ""
    iol_order_settlement: str = ""
    # Only limit orders are supported in this version. precioMercado is banned.
    iol_order_type: str = "precioLimite"
    iol_order_validity_minutes: int = 10
    execution_max_quote_age_seconds: int = 15
    # Tolerance for a quote timestamp slightly ahead of our clock (broker
    # clock skew). Anything further in the future is rejected.
    execution_quote_clock_skew_seconds: int = 2
    # 0 = NOT CONFIGURED → real/sandbox execution blocked (never "no limit").
    execution_max_price_deviation_pct: float = 0.0

    # --- Execution scope & instrument guard (fail closed) ---
    # Initial phase: sells only. Buys are blocked for sandbox/real.
    execution_sell_only: bool = True
    # Per-symbol allowlist, loaded from JSON. Empty = no instrument may be
    # traded (blocks sandbox and real). Never ship a productive policy here.
    execution_instrument_policies: dict = Field(default_factory=dict)
    # Re-verify the live position (read-only) right before submitting.
    execution_require_live_position_check: bool = True

    # Administrative creation of the BYMA execution-pilot recommendation.
    # Default false. It only allows PREPARING a pilot recommendation and can
    # never send an order: the sole execution path remains
    # POST /recommendations/{id}/approve, still gated by
    # ORDER_EXECUTION_ENABLED.
    execution_pilot_creation_enabled: bool = False

    @field_validator("execution_instrument_policies", mode="before")
    @classmethod
    def parse_instrument_policies(cls, v):
        if isinstance(v, str):
            raw = v.strip()
            if not raw:
                return {}
            import json as _json
            try:
                parsed = _json.loads(raw)
            except ValueError as exc:
                raise ValueError(f"EXECUTION_INSTRUMENT_POLICIES must be valid JSON: {exc}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("EXECUTION_INSTRUMENT_POLICIES must be a JSON object")
            return parsed
        return v

    news_provider: str = "mock"  # mock | rss
    news_rss_urls: List[str] = Field(
        default_factory=lambda: [
            # CNBC business news (company-specific, earnings, deals)
            "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
            # Investing.com stock market news (more company-focused than news_25)
            "https://www.investing.com/rss/news_301.rss",
            # MarketWatch top stories (requires follow_redirects=True in httpx)
            "https://feeds.marketwatch.com/marketwatch/topstories/",
            # Investing.com general market (kept as fallback)
            "https://www.investing.com/rss/news_25.rss",
        ]
    )
    news_timeout_seconds: int = 10
    news_max_items: int = 20

    scheduler_enabled: bool = True
    analysis_frequency_days: int = 4
    trigger_cooldown_seconds: int = 60
    investor_profile: str = "moderado"  # legacy alias, prefer investor_profile_target
    investor_profile_target: str = "moderate_aggressive"
    max_movement_per_cycle: float = 0.10
    min_liquidity_pct: float = 0.05
    max_single_asset_weight: float = 0.0  # 0 = use profile default
    max_equity_band: float = 0.0  # 0 = use profile default
    max_us_equity_concentration: float = 0.0  # 0 = use profile default
    llm_enabled: bool = False
    llm_provider: str = "openai"
    llm_api_key: str = ""
    llm_model: str = "gpt-4o-mini"
    llm_timeout_seconds: int = 15

    recommendation_unchanged_pct_threshold: float = 0.01
    recommendation_unchanged_risk_threshold: float = 0.03

    # Scheduler — market-hours aware (Part D)
    scheduler_market_open_hour: int = 11  # Argentina market open (UTC): 11:00 = 8:00 ART
    scheduler_market_close_hour: int = 20  # Argentina market close (UTC): 20:00 = 17:00 ART
    scheduler_premarket_minutes: List[int] = Field(default_factory=lambda: [60, 15])
    scheduler_open_interval_minutes: int = 30
    scheduler_postmarket_runs: int = 2
    scheduler_off_hours_enabled: bool = False
    scheduler_ingestion_only_off_hours: bool = True
    scheduler_postmarket_force_cycle: bool = False  # if True, post-market runs full cycle even without events

    # Notification (Part E)
    notification_enabled: bool = False
    notification_channel: str = "web_push"  # telegram | web_push
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    notification_min_severity: str = "medium"  # low | medium | high | critical
    notification_cooldown_seconds: int = 300
    notification_contradiction_threshold: int = 3  # >=N contradictions → high alert

    # Web push (VAPID keys — generate with: npx web-push generate-vapid-keys)
    vapid_public_key: str = ""
    vapid_private_key: str = ""
    vapid_contact_email: str = ""

    # Cluster-aware decision pipeline (additive — legacy flow unchanged when False)
    use_clusters: bool = False

    # Debug endpoints (POST /debug/simulate-alert). Disable in production.
    debug_endpoints_enabled: bool = True

    # API key — protects approve/reject/debug/settings endpoints. Empty = no auth.
    api_key: str = ""

    # Safety lock — with broker_mode=real, approve does NOT send orders unless
    # this is explicitly true. Fail closed: default false. Mock broker is not
    # gated by this lock (it never touches IOL), so tests/staging can rehearse
    # the approve flow safely.
    order_execution_enabled: bool = False

    # --- Execution Authorization V1 (all fail-closed defaults) ---
    # Secondary credential required to execute (X-Execution-Key header).
    # Empty = real execution impossible.
    execution_admin_key: str = ""
    # HMAC-SHA256 secret for signing execution previews.
    # Empty = previews unsigned = real execution impossible.
    execution_preview_secret: str = ""
    # How long a signed preview stays valid.
    execution_preview_ttl_seconds: int = 300
    # Recommendations older than this cannot be executed.
    execution_max_recommendation_age_minutes: int = 60
    # Hard limits. 0 means NOT CONFIGURED → real execution blocked (never "no limit").
    execution_max_order_value: float = 0.0
    execution_max_total_value: float = 0.0
    execution_max_portfolio_pct: float = 0.0

    # CORS — comma-separated origins, or "*" for dev.
    cors_origins: str = "*"

    whitelist_assets: List[str] = Field(
        default_factory=lambda: ["AAPL", "MSFT", "SPY", "QQQ", "AL30", "BND", "CASH"]
    )
    watchlist_assets: List[str] = Field(
        default_factory=list
    )
    market_universe_assets: List[str] = Field(
        default_factory=list
    )

    @field_validator("whitelist_assets", "news_rss_urls", "watchlist_assets", "market_universe_assets", "scheduler_premarket_minutes", mode="before")
    @classmethod
    def parse_csv_fields(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    @field_validator(
        "execution_preview_ttl_seconds",
        "execution_max_recommendation_age_minutes",
        "iol_order_validity_minutes",
        "execution_max_quote_age_seconds",
        mode="before",
    )
    @classmethod
    def validate_positive_execution_windows(cls, v, info):
        """Strictly positive finite windows — a zero/NaN window would make
        order timing comparisons ambiguous during an execution."""
        num = _finite_number(v)
        if num is None or num <= 0:
            raise ValueError(f"{info.field_name} must be a finite number > 0 (got {v!r})")
        return v

    @field_validator(
        "execution_max_price_deviation_pct",
        "execution_max_order_value",
        "execution_max_total_value",
        "execution_max_portfolio_pct",
        mode="before",
    )
    @classmethod
    def validate_fail_closed_limits(cls, v, info):
        """0 is allowed and means NOT CONFIGURED (blocks execution); negative
        or non-finite values are always rejected."""
        num = _finite_number(v)
        if num is None or num < 0:
            raise ValueError(f"{info.field_name} must be a finite number >= 0 (got {v!r})")
        return v

    @field_validator("execution_quote_clock_skew_seconds", mode="before")
    @classmethod
    def validate_clock_skew(cls, v, info):
        num = _finite_number(v)
        if num is None or num < 0 or num > 10:
            raise ValueError(f"{info.field_name} must be a finite number between 0 and 10 (got {v!r})")
        return v

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
