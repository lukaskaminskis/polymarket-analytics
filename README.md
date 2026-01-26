# Polymarket Analytics

A local-first analytics tool for tracking Polymarket prediction markets, analyzing accuracy by probability buckets, detecting black swan events, and monitoring large odds movements.

## Features

- **Market Tracking**: Automatically tracks high-liquidity markets resolving within 30 days
- **Hourly Snapshots**: Collects time-series data every hour for probability path analysis
- **Accuracy Analysis**: Measures win/loss rates by probability buckets (50-60%, 60-70%, etc.)
- **Black Swan Detection**: Identifies high-probability markets that resolve unexpectedly
- **Large Move Detection**: Flags significant probability changes (configurable threshold)
- **Web Dashboard**: Modern UI for browsing markets, viewing charts, and analyzing data

## Quick Start

### 1. Install Dependencies

```bash
cd polymarket-analytics
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Initialize Database

```bash
python cli.py init-db
```

### 3. Collect Data

Run the data collector to fetch markets from Polymarket:

```bash
python cli.py collect
```

### 4. Start Dashboard

```bash
python cli.py serve
```

Open http://127.0.0.1:8000 in your browser.

## CLI Commands

| Command | Description |
|---------|-------------|
| `python cli.py collect` | Fetch and store market data from Polymarket |
| `python cli.py detect-moves` | Detect large probability movements |
| `python cli.py stats` | Show overview statistics |
| `python cli.py movers` | Display biggest recent movers |
| `python cli.py black-swans` | List black swan events |
| `python cli.py serve` | Start the web dashboard |
| `python cli.py init-db` | Initialize/reset the database |

## Automated Hourly Collection

Set up a cron job to collect data every hour:

```bash
# Edit crontab
crontab -e

# Add this line (adjust path as needed)
0 * * * * cd /path/to/polymarket-analytics && /path/to/venv/bin/python cli.py collect >> /var/log/polymarket.log 2>&1
```

Or use the built-in scheduler (runs in foreground):

```python
# scheduler.py - create this file if needed
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', hours=1)
def collect_data():
    subprocess.run(['python', 'cli.py', 'collect'])

scheduler.start()
```

## Configuration

All thresholds are configurable in `src/config.py` or via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PM_MIN_LIQUIDITY_USD` | 10000 | Minimum liquidity to track a market |
| `PM_MAX_DAYS_TO_RESOLUTION` | 30 | Maximum days until resolution |
| `PM_LARGE_MOVE_THRESHOLD_POINTS` | 15 | Points change to flag as large move |
| `PM_LARGE_MOVE_WINDOW_HOURS` | 24 | Time window for move detection |
| `PM_BLACK_SWAN_THRESHOLD` | 80 | Probability threshold for black swan |
| `PM_HOST` | 127.0.0.1 | Dashboard server host |
| `PM_PORT` | 8000 | Dashboard server port |

Create a `.env` file to override defaults:

```env
PM_MIN_LIQUIDITY_USD=50000
PM_LARGE_MOVE_THRESHOLD_POINTS=20
PM_BLACK_SWAN_THRESHOLD=85
```

## Dashboard Views

### Overview Dashboard
- Total tracked/active/resolved markets
- Accuracy statistics by probability bucket
- Top markets by liquidity
- Recent large movers

### Markets Table
- All active markets with current probability
- Sortable by liquidity, probability, or resolution date
- Clickable for detailed view

### Market Detail
- Full probability history chart
- Large move markers
- Resolution status
- Raw snapshot data

### Movers View
- Markets with largest recent probability changes
- Shows direction, magnitude, and current liquidity

### Black Swans
- Markets where high-probability outcomes failed
- Historical record of prediction failures

## Architecture

```
polymarket-analytics/
├── cli.py              # CLI entry point
├── requirements.txt    # Python dependencies
├── src/
│   ├── config.py       # Centralized configuration
│   ├── database.py     # SQLite setup with async SQLAlchemy
│   ├── models.py       # Database models
│   ├── polymarket_client.py  # Polymarket API client
│   ├── ingestion.py    # Data collection service
│   ├── analytics.py    # Analysis engine
│   └── server.py       # FastAPI web server
├── templates/          # Jinja2 HTML templates
│   ├── base.html
│   ├── dashboard.html
│   ├── markets.html
│   ├── market_detail.html
│   ├── movers.html
│   └── black_swans.html
└── static/
    └── styles.css      # Dashboard styling
```

## Data Storage

Data is stored in a local SQLite database (`polymarket_analytics.db`):

- **markets**: Market metadata and current state
- **market_snapshots**: Hourly probability/volume snapshots
- **large_moves**: Detected significant probability changes
- **resolution_analysis**: Accuracy tracking for resolved markets

## API Endpoints

The dashboard also exposes JSON API endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /api/overview` | Overview statistics |
| `GET /api/markets` | List of active markets |
| `GET /api/market/{id}` | Market history with snapshots |
| `GET /api/movers` | Recent large movers |
| `GET /api/black-swans` | Black swan events |

## Development

```bash
# Run with auto-reload
python cli.py serve --host 0.0.0.0 --port 8000

# Run tests
pytest

# Check database contents
sqlite3 polymarket_analytics.db ".tables"
sqlite3 polymarket_analytics.db "SELECT COUNT(*) FROM markets"
```

## License

MIT
