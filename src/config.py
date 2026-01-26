"""
Configuration for Polymarket Analytics Tool.
All thresholds and settings are centralized here.
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    # Database
    database_url: str = Field(
        default="sqlite+aiosqlite:///./polymarket_analytics.db",
        description="SQLite database URL"
    )

    # Polymarket API
    polymarket_api_base: str = Field(
        default="https://gamma-api.polymarket.com",
        description="Polymarket Gamma API base URL"
    )
    polymarket_clob_api: str = Field(
        default="https://clob.polymarket.com",
        description="Polymarket CLOB API base URL"
    )

    # Market filtering
    min_volume_usd: float = Field(
        default=100000.0,
        description="Minimum volume in USD to track a market"
    )
    max_days_to_resolution: int = Field(
        default=30,
        description="Maximum days until resolution to track"
    )

    # Data collection
    collection_interval_hours: int = Field(
        default=1,
        description="Hours between data snapshots"
    )

    # Analysis thresholds
    probability_buckets: list = Field(
        default=[0, 50, 60, 70, 80, 90, 95, 100],
        description="Probability bucket boundaries for analysis"
    )
    large_move_threshold_points: float = Field(
        default=15.0,
        description="Minimum probability point change to flag as large move"
    )
    large_move_window_hours: int = Field(
        default=24,
        description="Time window (hours) for detecting large moves"
    )
    black_swan_threshold: float = Field(
        default=80.0,
        description="Minimum probability (%) for black swan detection"
    )

    # Web server
    host: str = Field(default="127.0.0.1", description="Server host")
    port: int = Field(default=8000, description="Server port")

    class Config:
        env_file = ".env"
        env_prefix = "PM_"


settings = Settings()
