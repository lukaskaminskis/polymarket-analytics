#!/usr/bin/env python3
"""
CLI for Polymarket Analytics Tool.
Provides commands for data collection and analysis.
"""
import asyncio
import click
from datetime import datetime


@click.group()
def cli():
    """Polymarket Analytics - Local-first prediction market analytics."""
    pass


@cli.command()
def collect():
    """Run data collection from Polymarket API."""
    from src.ingestion import run_ingestion

    click.echo(f"[{datetime.now().isoformat()}] Starting data collection...")

    stats = asyncio.run(run_ingestion())

    click.echo(f"Collection complete:")
    click.echo(f"  - Markets fetched: {stats['markets_fetched']}")
    click.echo(f"  - New markets: {stats['markets_new']}")
    click.echo(f"  - Updated markets: {stats['markets_updated']}")
    click.echo(f"  - Snapshots created: {stats['snapshots_created']}")
    click.echo(f"  - Resolutions detected: {stats['resolutions_detected']}")

    if stats['errors']:
        click.echo(f"  - Errors: {len(stats['errors'])}")
        for err in stats['errors'][:5]:
            click.echo(f"    - {err}")


@cli.command()
def detect_moves():
    """Detect large probability movements."""
    from src.analytics import AnalyticsEngine

    click.echo(f"[{datetime.now().isoformat()}] Detecting large moves...")

    async def run():
        engine = AnalyticsEngine()
        return await engine.detect_large_moves()

    moves = asyncio.run(run())

    if moves:
        click.echo(f"Detected {len(moves)} large moves:")
        for move in moves:
            direction = "↑" if move.probability_end > move.probability_start else "↓"
            click.echo(
                f"  {direction} {move.change_points:.1f}pts: "
                f"{move.probability_start:.1f}% → {move.probability_end:.1f}%"
            )
            click.echo(f"    {move.question[:60]}...")
    else:
        click.echo("No large moves detected.")


@cli.command()
def stats():
    """Show overview statistics."""
    from src.analytics import AnalyticsEngine

    async def run():
        engine = AnalyticsEngine()
        return await engine.get_overview_stats()

    overview = asyncio.run(run())

    click.echo("\n=== Polymarket Analytics Overview ===\n")
    click.echo(f"Total markets tracked: {overview.total_tracked}")
    click.echo(f"Active markets: {overview.active_markets}")
    click.echo(f"Resolved markets: {overview.resolved_markets}")
    click.echo(f"Total snapshots: {overview.total_snapshots}")
    click.echo(f"Black swan events: {overview.black_swan_count}")
    click.echo(f"Recent large moves (24h): {overview.recent_large_moves}")

    if overview.bucket_stats:
        click.echo("\n--- Accuracy by Probability Bucket ---")
        for bucket in overview.bucket_stats:
            if bucket.total_resolved > 0:
                click.echo(
                    f"  {bucket.bucket}: {bucket.accuracy_rate:.1f}% accuracy "
                    f"({bucket.correct_predictions}/{bucket.total_resolved})"
                )


@cli.command()
def movers():
    """Show biggest recent movers."""
    from src.analytics import AnalyticsEngine

    async def run():
        engine = AnalyticsEngine()
        return await engine.get_recent_movers(limit=10)

    recent_movers = asyncio.run(run())

    click.echo("\n=== Biggest Movers ===\n")
    for mover in recent_movers:
        direction = "↑" if mover['change_points'] > 0 else "↓"
        click.echo(
            f"{direction} {abs(mover['change_points']):.1f}pts: "
            f"{mover['probability_start']:.1f}% → {mover['probability_end']:.1f}%"
        )
        click.echo(f"  {mover['question'][:70]}...")
        click.echo(f"  Liquidity: ${mover['liquidity']:,.0f}\n")


@cli.command()
def black_swans():
    """Show black swan events."""
    from src.analytics import AnalyticsEngine

    async def run():
        engine = AnalyticsEngine()
        return await engine.get_black_swans(limit=10)

    swans = asyncio.run(run())

    click.echo("\n=== Black Swan Events ===\n")
    if not swans:
        click.echo("No black swan events recorded yet.")
        return

    for swan in swans:
        click.echo(f"Final probability: {swan['final_probability']:.1f}%")
        click.echo(f"Outcome: {swan['outcome']}")
        click.echo(f"Question: {swan['question'][:70]}...")
        click.echo(f"Resolved: {swan['resolved_at']}\n")


@cli.command()
@click.option('--host', default='127.0.0.1', help='Server host')
@click.option('--port', default=8000, help='Server port')
def serve(host, port):
    """Start the web dashboard server."""
    import uvicorn

    click.echo(f"Starting dashboard at http://{host}:{port}")
    uvicorn.run("src.server:app", host=host, port=port, reload=True)


@cli.command()
def init_db():
    """Initialize the database."""
    from src.database import init_db as _init_db

    asyncio.run(_init_db())
    click.echo("Database initialized successfully.")


if __name__ == "__main__":
    cli()
