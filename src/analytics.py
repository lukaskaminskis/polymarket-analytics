"""
Analytics engine for Polymarket data.
Handles probability tracking, black swan detection, and large move detection.
"""
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass
from sqlalchemy import select, and_, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from .database import async_session, init_db
from .models import Market, MarketSnapshot, LargeMove, ResolutionAnalysis
from .config import settings


@dataclass
class MarketWithSnapshot:
    """Market with its latest snapshot data."""
    id: str
    question: str
    category: Optional[str]
    end_date: Optional[datetime]
    probability: float
    liquidity: float
    volume: float
    is_resolved: bool
    resolution_outcome: Optional[str]
    last_updated: datetime


@dataclass
class ProbabilityBucketStats:
    """Statistics for a probability bucket."""
    bucket: str
    total_resolved: int
    correct_predictions: int
    incorrect_predictions: int
    accuracy_rate: float
    black_swan_count: int


@dataclass
class LargeMoveData:
    """Data for a large probability move."""
    market_id: str
    question: str
    detected_at: datetime
    probability_start: float
    probability_end: float
    change_points: float
    window_hours: int


@dataclass
class OverviewStats:
    """Overview statistics for the dashboard."""
    total_tracked: int
    active_markets: int
    resolved_markets: int
    total_snapshots: int
    bucket_stats: list[ProbabilityBucketStats]
    black_swan_count: int
    recent_large_moves: int


class AnalyticsEngine:
    """
    Engine for analyzing Polymarket data.
    """

    async def get_overview_stats(self) -> OverviewStats:
        """Get overview statistics for the dashboard."""
        async with async_session() as session:
            # Count markets
            total_result = await session.execute(select(func.count(Market.id)))
            total_tracked = total_result.scalar() or 0

            active_result = await session.execute(
                select(func.count(Market.id)).where(Market.is_active == True)
            )
            active_markets = active_result.scalar() or 0

            resolved_result = await session.execute(
                select(func.count(Market.id)).where(Market.is_resolved == True)
            )
            resolved_markets = resolved_result.scalar() or 0

            # Count snapshots
            snapshot_result = await session.execute(select(func.count(MarketSnapshot.id)))
            total_snapshots = snapshot_result.scalar() or 0

            # Get bucket statistics
            bucket_stats = await self._get_bucket_stats(session)

            # Count black swans
            black_swan_result = await session.execute(
                select(func.count(ResolutionAnalysis.id)).where(
                    ResolutionAnalysis.is_black_swan == True
                )
            )
            black_swan_count = black_swan_result.scalar() or 0

            # Count recent large moves (last 24h)
            cutoff = datetime.utcnow() - timedelta(hours=24)
            large_moves_result = await session.execute(
                select(func.count(LargeMove.id)).where(
                    LargeMove.detected_at >= cutoff
                )
            )
            recent_large_moves = large_moves_result.scalar() or 0

            return OverviewStats(
                total_tracked=total_tracked,
                active_markets=active_markets,
                resolved_markets=resolved_markets,
                total_snapshots=total_snapshots,
                bucket_stats=bucket_stats,
                black_swan_count=black_swan_count,
                recent_large_moves=recent_large_moves
            )

    async def _get_bucket_stats(self, session: AsyncSession) -> list[ProbabilityBucketStats]:
        """Get accuracy statistics by probability bucket."""
        buckets = settings.probability_buckets
        stats = []

        for i in range(len(buckets) - 1):
            bucket_name = f"{buckets[i]}-{buckets[i + 1]}%"

            # Get resolution analysis for this bucket
            result = await session.execute(
                select(ResolutionAnalysis).where(
                    ResolutionAnalysis.probability_bucket == bucket_name
                )
            )
            analyses = result.scalars().all()

            total = len(analyses)
            correct = sum(1 for a in analyses if a.predicted_correctly)
            incorrect = total - correct
            black_swans = sum(1 for a in analyses if a.is_black_swan)

            accuracy = (correct / total * 100) if total > 0 else 0

            stats.append(ProbabilityBucketStats(
                bucket=bucket_name,
                total_resolved=total,
                correct_predictions=correct,
                incorrect_predictions=incorrect,
                accuracy_rate=accuracy,
                black_swan_count=black_swans
            ))

        return stats

    async def get_active_markets(
        self,
        sort_by: str = "volume",
        limit: int = 100
    ) -> list[MarketWithSnapshot]:
        """Get active markets with their latest snapshot."""
        async with async_session() as session:
            # Get active markets
            result = await session.execute(
                select(Market).where(Market.is_active == True)
            )
            markets = result.scalars().all()

            market_data = []
            for market in markets:
                # Get latest snapshot
                snapshot_result = await session.execute(
                    select(MarketSnapshot)
                    .where(MarketSnapshot.market_id == market.id)
                    .order_by(MarketSnapshot.timestamp.desc())
                    .limit(1)
                )
                snapshot = snapshot_result.scalar_one_or_none()

                probability = snapshot.probability if snapshot else 0
                liquidity = snapshot.liquidity if snapshot else market.liquidity
                volume = snapshot.volume if snapshot else market.volume

                market_data.append(MarketWithSnapshot(
                    id=market.id,
                    question=market.question,
                    category=market.category,
                    end_date=market.end_date,
                    probability=probability,
                    liquidity=liquidity,
                    volume=volume,
                    is_resolved=market.is_resolved,
                    resolution_outcome=market.resolution_outcome,
                    last_updated=market.last_updated_at
                ))

            # Sort - default to volume
            if sort_by == "volume":
                market_data.sort(key=lambda m: m.volume, reverse=True)
            elif sort_by == "liquidity":
                market_data.sort(key=lambda m: m.liquidity, reverse=True)
            elif sort_by == "probability":
                market_data.sort(key=lambda m: m.probability, reverse=True)
            elif sort_by == "end_date":
                market_data.sort(key=lambda m: m.end_date or datetime.max)

            return market_data[:limit]

    async def get_market_history(self, market_id: str) -> dict:
        """Get full history for a market including snapshots."""
        async with async_session() as session:
            # Get market
            result = await session.execute(
                select(Market).where(Market.id == market_id)
            )
            market = result.scalar_one_or_none()

            if not market:
                return None

            # Get all snapshots
            snapshots_result = await session.execute(
                select(MarketSnapshot)
                .where(MarketSnapshot.market_id == market_id)
                .order_by(MarketSnapshot.timestamp.asc())
            )
            snapshots = snapshots_result.scalars().all()

            # Get large moves for this market
            moves_result = await session.execute(
                select(LargeMove)
                .where(LargeMove.market_id == market_id)
                .order_by(LargeMove.detected_at.desc())
            )
            large_moves = moves_result.scalars().all()

            return {
                "market": {
                    "id": market.id,
                    "question": market.question,
                    "description": market.description,
                    "category": market.category,
                    "outcomes": market.outcomes,
                    "end_date": market.end_date.isoformat() if market.end_date else None,
                    "is_resolved": market.is_resolved,
                    "resolution_outcome": market.resolution_outcome,
                    "liquidity": market.liquidity,
                    "volume": market.volume
                },
                "snapshots": [
                    {
                        "timestamp": s.timestamp.isoformat(),
                        "probability": s.probability,
                        "liquidity": s.liquidity,
                        "volume": s.volume
                    }
                    for s in snapshots
                ],
                "large_moves": [
                    {
                        "detected_at": m.detected_at.isoformat(),
                        "probability_start": m.probability_start,
                        "probability_end": m.probability_end,
                        "change_points": m.change_points
                    }
                    for m in large_moves
                ]
            }

    async def detect_large_moves(self) -> list[LargeMoveData]:
        """
        Detect and store large probability movements.
        """
        threshold = settings.large_move_threshold_points
        window_hours = settings.large_move_window_hours
        window_start = datetime.utcnow() - timedelta(hours=window_hours)

        detected_moves = []

        async with async_session() as session:
            # Get active markets
            result = await session.execute(
                select(Market).where(Market.is_active == True)
            )
            markets = result.scalars().all()

            for market in markets:
                # Get snapshots in the window
                snapshots_result = await session.execute(
                    select(MarketSnapshot)
                    .where(and_(
                        MarketSnapshot.market_id == market.id,
                        MarketSnapshot.timestamp >= window_start
                    ))
                    .order_by(MarketSnapshot.timestamp.asc())
                )
                snapshots = snapshots_result.scalars().all()

                if len(snapshots) < 2:
                    continue

                # Find max change in window
                min_prob = min(s.probability for s in snapshots)
                max_prob = max(s.probability for s in snapshots)
                change = abs(max_prob - min_prob)

                if change >= threshold:
                    # Check if this move was already recorded
                    existing = await session.execute(
                        select(LargeMove).where(and_(
                            LargeMove.market_id == market.id,
                            LargeMove.window_start >= window_start
                        ))
                    )

                    if existing.scalar_one_or_none():
                        continue

                    # Record the move
                    first_snapshot = snapshots[0]
                    last_snapshot = snapshots[-1]

                    move = LargeMove(
                        market_id=market.id,
                        detected_at=datetime.utcnow(),
                        window_start=first_snapshot.timestamp,
                        window_end=last_snapshot.timestamp,
                        probability_start=first_snapshot.probability,
                        probability_end=last_snapshot.probability,
                        change_points=change
                    )
                    session.add(move)

                    detected_moves.append(LargeMoveData(
                        market_id=market.id,
                        question=market.question,
                        detected_at=datetime.utcnow(),
                        probability_start=first_snapshot.probability,
                        probability_end=last_snapshot.probability,
                        change_points=change,
                        window_hours=window_hours
                    ))

            await session.commit()

        return detected_moves

    async def get_recent_movers(self, limit: int = 20) -> list[dict]:
        """Get markets with largest recent probability changes.

        Looks at max swing within the window (not just first vs last) to catch
        volatile markets that moved significantly at any point.
        """
        window_hours = settings.large_move_window_hours
        window_start = datetime.utcnow() - timedelta(hours=window_hours)

        movers = []

        async with async_session() as session:
            # Get active markets
            result = await session.execute(
                select(Market).where(Market.is_active == True)
            )
            markets = result.scalars().all()

            for market in markets:
                # Get ALL snapshots in window to find max swing
                snapshots_result = await session.execute(
                    select(MarketSnapshot)
                    .where(and_(
                        MarketSnapshot.market_id == market.id,
                        MarketSnapshot.timestamp >= window_start
                    ))
                    .order_by(MarketSnapshot.timestamp.asc())
                )
                snapshots = snapshots_result.scalars().all()

                if len(snapshots) < 2:
                    continue

                # Find the max swing in the window
                probs = [s.probability for s in snapshots]
                min_prob = min(probs)
                max_prob = max(probs)
                max_swing = max_prob - min_prob

                # Also get directional change (first to last)
                first = snapshots[0]
                last = snapshots[-1]
                directional_change = last.probability - first.probability

                if max_swing >= 1:  # At least 1 point move
                    movers.append({
                        "market_id": market.id,
                        "question": market.question,
                        "category": market.category,
                        "probability_start": first.probability,
                        "probability_end": last.probability,
                        "change_points": directional_change,
                        "max_swing": max_swing,
                        "abs_change": abs(directional_change),
                        "window_hours": window_hours,
                        "volume": market.volume
                    })

            # Sort by max swing (captures volatile markets better)
            movers.sort(key=lambda m: m["max_swing"], reverse=True)

        return movers[:limit]

    async def get_black_swans(self, limit: int = 50) -> list[dict]:
        """Get black swan events."""
        async with async_session() as session:
            result = await session.execute(
                select(ResolutionAnalysis, Market)
                .join(Market, ResolutionAnalysis.market_id == Market.id)
                .where(ResolutionAnalysis.is_black_swan == True)
                .order_by(ResolutionAnalysis.resolved_at.desc())
                .limit(limit)
            )
            rows = result.all()

            return [
                {
                    "market_id": analysis.market_id,
                    "question": market.question,
                    "final_probability": analysis.final_probability,
                    "outcome": analysis.outcome,
                    "resolved_at": analysis.resolved_at.isoformat()
                }
                for analysis, market in rows
            ]

    async def get_available_dates(self) -> list[str]:
        """Get list of dates that have snapshot data available."""
        async with async_session() as session:
            result = await session.execute(
                select(func.date(MarketSnapshot.timestamp))
                .distinct()
                .order_by(func.date(MarketSnapshot.timestamp).desc())
            )
            dates = result.scalars().all()
            return [str(d) for d in dates if d]

    async def get_simulation_markets(
        self,
        historical_date: str,
        limit: int = 50
    ) -> list[dict]:
        """
        Get markets for outcome simulation.

        Returns markets that:
        - Have snapshot data at the historical date
        - Would resolve within 30 days from that date
        - Are now resolved (so we know the outcome)

        Sorted by liquidity at the historical date.
        """
        from datetime import date as date_type

        # Parse the historical date
        try:
            hist_date = datetime.strptime(historical_date, "%Y-%m-%d")
        except ValueError:
            return []

        # Window: resolve within 30 days from historical date
        resolve_window_end = hist_date + timedelta(days=30)

        async with async_session() as session:
            # Get resolved markets that had end_date within 30 days of historical date
            result = await session.execute(
                select(Market).where(
                    and_(
                        Market.is_resolved == True,
                        Market.end_date != None,
                        Market.end_date > hist_date,
                        Market.end_date <= resolve_window_end
                    )
                )
            )
            markets = result.scalars().all()

            simulation_data = []

            for market in markets:
                # Get the snapshot closest to the historical date
                snapshot_result = await session.execute(
                    select(MarketSnapshot)
                    .where(and_(
                        MarketSnapshot.market_id == market.id,
                        func.date(MarketSnapshot.timestamp) <= historical_date
                    ))
                    .order_by(MarketSnapshot.timestamp.desc())
                    .limit(1)
                )
                snapshot = snapshot_result.scalar_one_or_none()

                if not snapshot:
                    continue

                # Get outcome prices at that date
                outcome_prices = snapshot.outcome_prices or {}
                if not outcome_prices and market.outcomes:
                    # Fallback: derive from probability
                    yes_prob = snapshot.probability / 100
                    outcome_prices = {
                        "Yes": yes_prob,
                        "No": 1 - yes_prob
                    }

                # Determine winning outcome
                resolution = market.resolution_outcome
                winning_outcome = None
                if resolution:
                    res_lower = resolution.lower()
                    if res_lower in ["yes", "true", "1"]:
                        winning_outcome = "Yes"
                    elif res_lower in ["no", "false", "0"]:
                        winning_outcome = "No"
                    else:
                        winning_outcome = resolution

                simulation_data.append({
                    "market_id": market.id,
                    "question": market.question,
                    "category": market.category,
                    "end_date": market.end_date.isoformat() if market.end_date else None,
                    "snapshot_date": snapshot.timestamp.isoformat(),
                    "liquidity_at_date": snapshot.liquidity,
                    "outcomes": market.outcomes or ["Yes", "No"],
                    "outcome_prices": outcome_prices,
                    "resolution_outcome": winning_outcome,
                    "is_resolved": True
                })

            # Sort by liquidity at historical date
            simulation_data.sort(key=lambda m: m["liquidity_at_date"], reverse=True)

            return simulation_data[:limit]
