from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    iol_use_sandbox: bool = False

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
    notification_channel: str = "telegram"  # telegram | email
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

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
