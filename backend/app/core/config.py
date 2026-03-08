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
            "https://www.investing.com/rss/news_25.rss",
            "https://feeds.reuters.com/reuters/businessNews",
        ]
    )
    news_timeout_seconds: int = 10
    news_max_items: int = 20

    scheduler_enabled: bool = True
    analysis_frequency_days: int = 4
    trigger_cooldown_seconds: int = 60
    investor_profile: str = "moderado"
    max_movement_per_cycle: float = 0.10
    min_liquidity_pct: float = 0.05
    llm_enabled: bool = False
    whitelist_assets: List[str] = Field(
        default_factory=lambda: ["AAPL", "MSFT", "SPY", "QQQ", "AL30", "BND", "CASH"]
    )

    @field_validator("whitelist_assets", "news_rss_urls", mode="before")
    @classmethod
    def parse_csv_fields(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
