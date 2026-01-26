"""
FastAPI server for Polymarket Analytics Dashboard.
"""
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from datetime import datetime

from .database import init_db
from .analytics import AnalyticsEngine
from .polymarket_client import PolymarketClient
from .config import settings

# Initialize app
app = FastAPI(
    title="Polymarket Analytics",
    description="Local-first analytics tool for Polymarket prediction markets"
)

# Mount static files and templates
BASE_DIR = Path(__file__).resolve().parent.parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Analytics engine
engine = AnalyticsEngine()


@app.on_event("startup")
async def startup():
    """Initialize database on startup."""
    await init_db()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    overview = await engine.get_overview_stats()
    markets = await engine.get_active_markets(limit=20)
    movers = await engine.get_recent_movers(limit=10)

    # Convert bucket_stats to serializable dicts for template
    bucket_stats_json = [
        {
            "bucket": b.bucket,
            "total_resolved": b.total_resolved,
            "correct_predictions": b.correct_predictions,
            "accuracy_rate": b.accuracy_rate,
            "black_swan_count": b.black_swan_count
        }
        for b in overview.bucket_stats
    ]

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "overview": overview,
        "bucket_stats_json": bucket_stats_json,
        "markets": markets,
        "movers": movers,
        "config": settings
    })


@app.get("/markets", response_class=HTMLResponse)
async def markets_page(request: Request, sort: str = "volume"):
    """Markets listing page."""
    markets = await engine.get_active_markets(sort_by=sort, limit=100)

    return templates.TemplateResponse("markets.html", {
        "request": request,
        "markets": markets,
        "sort_by": sort
    })


@app.get("/market/{market_id}", response_class=HTMLResponse)
async def market_detail(request: Request, market_id: str):
    """Individual market detail page."""
    history = await engine.get_market_history(market_id)

    if not history:
        raise HTTPException(status_code=404, detail="Market not found")

    return templates.TemplateResponse("market_detail.html", {
        "request": request,
        "market": history["market"],
        "snapshots": history["snapshots"],
        "large_moves": history["large_moves"]
    })


@app.get("/movers", response_class=HTMLResponse)
async def movers_page(request: Request):
    """Large movers page."""
    movers = await engine.get_recent_movers(limit=50)

    return templates.TemplateResponse("movers.html", {
        "request": request,
        "movers": movers,
        "window_hours": settings.large_move_window_hours
    })


@app.get("/black-swans", response_class=HTMLResponse)
async def black_swans_page(request: Request, source: str = "api"):
    """Black swan events page.

    Args:
        source: "local" for tracked markets, "api" for Polymarket API search
    """
    if source == "api":
        # Fetch from Polymarket API (searches last 60 days)
        black_swans = await polymarket_client.find_black_swans_from_api(
            days_back=60,
            min_volume=100000,
            limit=50
        )
        source_label = "Polymarket API (last 60 days)"
    else:
        # Fetch from local database
        black_swans = await engine.get_black_swans(limit=50)
        source_label = "Local tracked markets"

    return templates.TemplateResponse("black_swans.html", {
        "request": request,
        "black_swans": black_swans,
        "threshold": settings.black_swan_threshold,
        "source": source,
        "source_label": source_label
    })


# API endpoints for AJAX/chart updates
@app.get("/api/overview")
async def api_overview():
    """API endpoint for overview stats."""
    overview = await engine.get_overview_stats()
    return {
        "total_tracked": overview.total_tracked,
        "active_markets": overview.active_markets,
        "resolved_markets": overview.resolved_markets,
        "total_snapshots": overview.total_snapshots,
        "black_swan_count": overview.black_swan_count,
        "recent_large_moves": overview.recent_large_moves,
        "bucket_stats": [
            {
                "bucket": b.bucket,
                "total_resolved": b.total_resolved,
                "correct_predictions": b.correct_predictions,
                "accuracy_rate": b.accuracy_rate,
                "black_swan_count": b.black_swan_count
            }
            for b in overview.bucket_stats
        ]
    }


@app.get("/api/markets")
async def api_markets(sort: str = "liquidity", limit: int = 100):
    """API endpoint for markets list."""
    markets = await engine.get_active_markets(sort_by=sort, limit=limit)
    return [
        {
            "id": m.id,
            "question": m.question,
            "category": m.category,
            "end_date": m.end_date.isoformat() if m.end_date else None,
            "probability": m.probability,
            "liquidity": m.liquidity,
            "volume": m.volume,
            "is_resolved": m.is_resolved
        }
        for m in markets
    ]


@app.get("/api/market/{market_id}")
async def api_market_history(market_id: str):
    """API endpoint for market history."""
    history = await engine.get_market_history(market_id)
    if not history:
        raise HTTPException(status_code=404, detail="Market not found")
    return history


@app.get("/api/movers")
async def api_movers(limit: int = 20):
    """API endpoint for recent movers."""
    return await engine.get_recent_movers(limit=limit)


@app.get("/api/black-swans")
async def api_black_swans(limit: int = 50):
    """API endpoint for black swan events."""
    return await engine.get_black_swans(limit=limit)


# Simulation endpoints
polymarket_client = PolymarketClient()


@app.get("/simulation", response_class=HTMLResponse)
async def simulation_page(request: Request):
    """Outcome simulation page - access any historical Polymarket data."""
    return templates.TemplateResponse("simulation.html", {
        "request": request
    })


@app.get("/api/simulation/markets")
async def api_simulation_markets(
    date: str,
    min_volume: float = 100000,
    limit: int = 50,
    any_resolved: bool = False
):
    """
    API endpoint for historical simulation data.
    Fetches directly from Polymarket API for any historical date.

    Args:
        date: Simulation date (YYYY-MM-DD format)
        min_volume: Minimum volume filter (resolved markets have $0 liquidity, so we use volume)
        limit: Maximum number of markets
        any_resolved: If true, ignore date filter and return top resolved markets by volume
    """
    try:
        simulation_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # Don't allow future dates (unless any_resolved mode)
    if simulation_date > datetime.utcnow() and not any_resolved:
        raise HTTPException(status_code=400, detail="Cannot simulate future dates")

    # Fetch historical data from Polymarket (filter by volume, not liquidity)
    markets = await polymarket_client.get_historical_simulation_data(
        simulation_date=simulation_date,
        min_volume=min_volume,
        limit=limit,
        any_resolved=any_resolved
    )

    return markets


@app.get("/api/simulation/local-markets")
async def api_simulation_local_markets(date: str, limit: int = 50):
    """
    API endpoint for simulation using locally stored data.
    Falls back to this if API data is unavailable.
    """
    return await engine.get_simulation_markets(date, limit=limit)
