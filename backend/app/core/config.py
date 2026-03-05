from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Mi Finanza MVP"
    environment: str = "dev"
    database_url: str = "sqlite:///./mi_finanza.db"
    broker_mode: str = "mock"
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

    @field_validator("whitelist_assets", mode="before")
    @classmethod
    def parse_whitelist(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
