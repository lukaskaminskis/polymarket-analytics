#!/usr/bin/env python3
"""
Scheduler for automated hourly data collection.
Run this as a background process for continuous data collection.
"""
import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.ingestion import run_ingestion
from src.analytics import AnalyticsEngine
from src.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def collect_and_analyze():
    """Run data collection and analysis."""
    logger.info("Starting scheduled data collection...")

    try:
        # Run ingestion
        stats = await run_ingestion()
        logger.info(
            f"Collection complete: {stats['markets_fetched']} fetched, "
            f"{stats['markets_new']} new, {stats['snapshots_created']} snapshots"
        )

        if stats['errors']:
            for err in stats['errors'][:3]:
                logger.warning(f"Collection error: {err}")

        # Detect large moves
        engine = AnalyticsEngine()
        moves = await engine.detect_large_moves()

        if moves:
            logger.info(f"Detected {len(moves)} large moves:")
            for move in moves[:5]:
                logger.info(
                    f"  {move.question[:50]}... "
                    f"({move.probability_start:.1f}% -> {move.probability_end:.1f}%)"
                )

    except Exception as e:
        logger.error(f"Error during collection: {e}")


def run_scheduler():
    """Start the scheduler."""
    scheduler = AsyncIOScheduler()

    # Schedule hourly collection
    scheduler.add_job(
        collect_and_analyze,
        IntervalTrigger(hours=settings.collection_interval_hours),
        id='hourly_collection',
        name='Hourly data collection',
        next_run_time=datetime.now()  # Run immediately on start
    )

    logger.info(
        f"Starting scheduler (interval: {settings.collection_interval_hours}h)"
    )
    scheduler.start()

    # Keep the scheduler running
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown()


if __name__ == "__main__":
    run_scheduler()
