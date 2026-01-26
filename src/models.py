"""
SQLAlchemy models for Polymarket analytics.
"""
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean,
    ForeignKey, Text, Index, JSON
)
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base


class Market(Base):
    """
    Represents a Polymarket prediction market.
    """
    __tablename__ = "markets"

    id = Column(String, primary_key=True)  # Polymarket market ID
    condition_id = Column(String, nullable=True)  # CLOB condition ID
    question = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String, nullable=True)

    # Outcomes (typically Yes/No or multiple options)
    outcomes = Column(JSON, nullable=True)  # List of outcome names
    outcome_prices = Column(JSON, nullable=True)  # Current prices by outcome

    # Timing
    created_at = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)  # Expected resolution date
    resolved_at = Column(DateTime, nullable=True)

    # Resolution
    is_resolved = Column(Boolean, default=False)
    resolution_outcome = Column(String, nullable=True)  # Winning outcome

    # Liquidity/Volume
    liquidity = Column(Float, default=0.0)  # USD liquidity
    volume = Column(Float, default=0.0)  # Total volume traded
    volume_24h = Column(Float, default=0.0)  # 24h volume

    # Tracking metadata
    first_tracked_at = Column(DateTime, default=datetime.utcnow)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)  # Still being tracked

    # Relationships
    snapshots = relationship("MarketSnapshot", back_populates="market", cascade="all, delete-orphan")
    large_moves = relationship("LargeMove", back_populates="market", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_markets_end_date", "end_date"),
        Index("ix_markets_is_resolved", "is_resolved"),
        Index("ix_markets_liquidity", "liquidity"),
    )


class MarketSnapshot(Base):
    """
    Hourly snapshot of market state for time-series analysis.
    """
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(String, ForeignKey("markets.id"), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Probability/Price for the primary outcome (typically "Yes")
    probability = Column(Float, nullable=False)  # 0-100 scale

    # All outcome prices at this snapshot
    outcome_prices = Column(JSON, nullable=True)

    # Volume/Liquidity at snapshot time
    liquidity = Column(Float, default=0.0)
    volume = Column(Float, default=0.0)
    volume_24h = Column(Float, default=0.0)

    # Relationships
    market = relationship("Market", back_populates="snapshots")

    __table_args__ = (
        Index("ix_snapshots_market_timestamp", "market_id", "timestamp"),
        Index("ix_snapshots_timestamp", "timestamp"),
    )


class LargeMove(Base):
    """
    Records significant probability movements.
    """
    __tablename__ = "large_moves"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(String, ForeignKey("markets.id"), nullable=False)

    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    window_start = Column(DateTime, nullable=False)
    window_end = Column(DateTime, nullable=False)

    probability_start = Column(Float, nullable=False)
    probability_end = Column(Float, nullable=False)
    change_points = Column(Float, nullable=False)  # Absolute change in points

    # Relationships
    market = relationship("Market", back_populates="large_moves")

    __table_args__ = (
        Index("ix_large_moves_detected_at", "detected_at"),
        Index("ix_large_moves_change", "change_points"),
    )


class ResolutionAnalysis(Base):
    """
    Analysis of market resolutions for accuracy tracking.
    """
    __tablename__ = "resolution_analysis"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(String, nullable=False, unique=True)

    # Final state before resolution
    final_probability = Column(Float, nullable=False)
    probability_bucket = Column(String, nullable=False)  # e.g., "80-90%"

    # Resolution outcome
    resolved_at = Column(DateTime, nullable=False)
    outcome = Column(String, nullable=False)  # "yes", "no", or specific outcome

    # Was the high-probability outcome correct?
    predicted_correctly = Column(Boolean, nullable=False)

    # Black swan flag
    is_black_swan = Column(Boolean, default=False)

    __table_args__ = (
        Index("ix_resolution_bucket", "probability_bucket"),
        Index("ix_resolution_black_swan", "is_black_swan"),
    )
