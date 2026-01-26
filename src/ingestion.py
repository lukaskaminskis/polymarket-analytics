"""
Data ingestion service for collecting and storing Polymarket data.
"""
from datetime import datetime
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from .database import async_session, init_db
from .models import Market, MarketSnapshot, ResolutionAnalysis
from .polymarket_client import PolymarketClient, MarketData
from .config import settings


class IngestionService:
    """
    Handles data collection and storage from Polymarket.
    """

    def __init__(self):
        self.client = PolymarketClient()

    async def run_collection(self) -> dict:
        """
        Run a full data collection cycle.
        Returns statistics about the collection.
        """
        await init_db()

        stats = {
            "markets_fetched": 0,
            "markets_new": 0,
            "markets_updated": 0,
            "snapshots_created": 0,
            "resolutions_detected": 0,
            "errors": []
        }

        async with async_session() as session:
            # Fetch active markets
            try:
                markets = await self.client.get_active_markets()
                stats["markets_fetched"] = len(markets)
            except Exception as e:
                stats["errors"].append(f"Failed to fetch markets: {e}")
                return stats

            # Process each market
            for market_data in markets:
                try:
                    is_new = await self._upsert_market(session, market_data)
                    if is_new:
                        stats["markets_new"] += 1
                    else:
                        stats["markets_updated"] += 1

                    await self._create_snapshot(session, market_data)
                    stats["snapshots_created"] += 1
                except Exception as e:
                    stats["errors"].append(f"Error processing {market_data.id}: {e}")

            # Check for resolved markets that we were tracking
            try:
                resolved = await self._check_resolutions(session)
                stats["resolutions_detected"] = resolved
            except Exception as e:
                stats["errors"].append(f"Error checking resolutions: {e}")

            await session.commit()

        return stats

    async def _upsert_market(self, session: AsyncSession, data: MarketData) -> bool:
        """
        Insert or update a market record.
        Returns True if this is a new market.
        """
        result = await session.execute(
            select(Market).where(Market.id == data.id)
        )
        market = result.scalar_one_or_none()

        if market is None:
            # New market
            market = Market(
                id=data.id,
                condition_id=data.condition_id,
                question=data.question,
                description=data.description,
                category=data.category,
                outcomes=data.outcomes,
                outcome_prices=data.outcome_prices,
                created_at=data.created_at,
                end_date=data.end_date,
                is_resolved=data.is_resolved,
                resolution_outcome=data.resolution_outcome,
                liquidity=data.liquidity,
                volume=data.volume,
                volume_24h=data.volume_24h,
                first_tracked_at=datetime.utcnow(),
                last_updated_at=datetime.utcnow(),
                is_active=True
            )
            session.add(market)
            return True
        else:
            # Update existing market
            market.outcome_prices = data.outcome_prices
            market.end_date = data.end_date
            market.is_resolved = data.is_resolved
            market.resolution_outcome = data.resolution_outcome
            market.liquidity = data.liquidity
            market.volume = data.volume
            market.volume_24h = data.volume_24h
            market.last_updated_at = datetime.utcnow()
            return False

    async def _create_snapshot(self, session: AsyncSession, data: MarketData):
        """Create a market snapshot."""
        # Get primary probability (typically "Yes" outcome)
        probability = 0.0
        if data.outcome_prices:
            # Try to get "Yes" price, otherwise use first outcome
            if "Yes" in data.outcome_prices:
                probability = data.outcome_prices["Yes"] * 100
            elif data.outcomes:
                first_outcome = data.outcomes[0]
                probability = data.outcome_prices.get(first_outcome, 0) * 100

        snapshot = MarketSnapshot(
            market_id=data.id,
            timestamp=datetime.utcnow(),
            probability=probability,
            outcome_prices=data.outcome_prices,
            liquidity=data.liquidity,
            volume=data.volume,
            volume_24h=data.volume_24h
        )
        session.add(snapshot)

    async def _check_resolutions(self, session: AsyncSession) -> int:
        """
        Check for newly resolved markets and create resolution analysis.
        """
        resolved_count = 0

        # Get tracked markets that are resolved but not yet analyzed
        result = await session.execute(
            select(Market).where(
                and_(
                    Market.is_active == True,
                    Market.is_resolved == True
                )
            )
        )
        markets = result.scalars().all()

        for market in markets:
            # Check if already analyzed
            existing = await session.execute(
                select(ResolutionAnalysis).where(
                    ResolutionAnalysis.market_id == market.id
                )
            )
            if existing.scalar_one_or_none():
                continue

            # Get the last snapshot before resolution
            snapshot_result = await session.execute(
                select(MarketSnapshot)
                .where(MarketSnapshot.market_id == market.id)
                .order_by(MarketSnapshot.timestamp.desc())
                .limit(1)
            )
            last_snapshot = snapshot_result.scalar_one_or_none()

            if not last_snapshot:
                continue

            final_prob = last_snapshot.probability
            bucket = self._get_probability_bucket(final_prob)

            # Determine if prediction was correct
            # If final probability was > 50%, we predicted "Yes"
            predicted_yes = final_prob > 50
            resolved_yes = market.resolution_outcome and market.resolution_outcome.lower() in ["yes", "true", "1"]
            predicted_correctly = predicted_yes == resolved_yes

            # Detect black swan with new 14-day rule:
            # If 14 days before resolution certainty was >70% and then dropped to <50%
            is_black_swan = await self._detect_black_swan(session, market, resolved_yes)

            analysis = ResolutionAnalysis(
                market_id=market.id,
                final_probability=final_prob,
                probability_bucket=bucket,
                resolved_at=market.resolved_at or datetime.utcnow(),
                outcome=market.resolution_outcome or "unknown",
                predicted_correctly=predicted_correctly,
                is_black_swan=is_black_swan
            )
            session.add(analysis)

            # Mark market as no longer active
            market.is_active = False
            resolved_count += 1

        return resolved_count

    async def _detect_black_swan(
        self,
        session: AsyncSession,
        market: Market,
        resolved_yes: bool
    ) -> bool:
        """
        Detect black swan events using the 14-day rule:
        - 14 days before resolution, outcome certainty was >70% (yes or no)
        - Then at some point it dropped to less than 50%

        This captures dramatic reversals where the market was confident
        and then completely changed direction.
        """
        if not market.end_date:
            return False

        # Get snapshot from ~14 days before resolution
        fourteen_days_before = market.end_date - timedelta(days=14)
        snapshot_14d_result = await session.execute(
            select(MarketSnapshot)
            .where(and_(
                MarketSnapshot.market_id == market.id,
                MarketSnapshot.timestamp <= fourteen_days_before
            ))
            .order_by(MarketSnapshot.timestamp.desc())
            .limit(1)
        )
        snapshot_14d = snapshot_14d_result.scalar_one_or_none()

        if not snapshot_14d:
            # Fallback to old logic if no 14-day snapshot
            final_snapshot_result = await session.execute(
                select(MarketSnapshot)
                .where(MarketSnapshot.market_id == market.id)
                .order_by(MarketSnapshot.timestamp.desc())
                .limit(1)
            )
            final_snapshot = final_snapshot_result.scalar_one_or_none()
            if final_snapshot:
                final_prob = final_snapshot.probability
                return (
                    final_prob >= settings.black_swan_threshold and not resolved_yes
                ) or (
                    final_prob <= (100 - settings.black_swan_threshold) and resolved_yes
                )
            return False

        prob_14d = snapshot_14d.probability

        # Check if certainty was >70% at 14 days (either direction)
        was_high_certainty = prob_14d >= 70 or prob_14d <= 30

        if not was_high_certainty:
            return False

        # Check if it dropped below 50% at any point after
        # (i.e., the market lost confidence in the leading outcome)
        snapshots_after_result = await session.execute(
            select(MarketSnapshot)
            .where(and_(
                MarketSnapshot.market_id == market.id,
                MarketSnapshot.timestamp > fourteen_days_before
            ))
            .order_by(MarketSnapshot.timestamp.asc())
        )
        snapshots_after = snapshots_after_result.scalars().all()

        # Track if the leading outcome flipped
        if prob_14d >= 70:
            # Market was confident in "Yes" - check if it dropped below 50%
            dropped_confidence = any(s.probability < 50 for s in snapshots_after)
            # Black swan if it dropped AND "No" actually won
            return dropped_confidence and not resolved_yes
        else:
            # Market was confident in "No" (prob <= 30) - check if "Yes" rose above 50%
            gained_confidence = any(s.probability > 50 for s in snapshots_after)
            # Black swan if confidence shifted AND "Yes" actually won
            return gained_confidence and resolved_yes

    def _get_probability_bucket(self, probability: float) -> str:
        """Get the probability bucket label for a given probability."""
        buckets = settings.probability_buckets
        for i in range(len(buckets) - 1):
            if buckets[i] <= probability < buckets[i + 1]:
                return f"{buckets[i]}-{buckets[i + 1]}%"
        return f"{buckets[-2]}-{buckets[-1]}%"


async def run_ingestion():
    """Main entry point for data ingestion."""
    service = IngestionService()
    stats = await service.run_collection()
    return stats
