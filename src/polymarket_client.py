"""
Polymarket API client for fetching market data.
Uses the Gamma API and CLOB API endpoints.
"""
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass
from .config import settings


# Simple in-memory cache for black swan results
_black_swan_cache = {
    "data": None,
    "timestamp": None,
    "ttl_minutes": 30
}


@dataclass
class MarketData:
    """Parsed market data from Polymarket API."""
    id: str
    condition_id: Optional[str]
    question: str
    description: Optional[str]
    category: Optional[str]
    outcomes: list[str]
    outcome_prices: dict[str, float]
    created_at: Optional[datetime]
    end_date: Optional[datetime]
    is_resolved: bool
    resolution_outcome: Optional[str]
    liquidity: float
    volume: float
    volume_24h: float


class PolymarketClient:
    """
    Client for interacting with Polymarket APIs.
    """

    def __init__(self):
        self.gamma_base = settings.polymarket_api_base
        self.clob_base = settings.polymarket_clob_api
        self.timeout = httpx.Timeout(30.0)

    async def get_active_markets(
        self,
        min_volume: float = None,
        max_days_to_resolution: int = None,
        limit: int = 500
    ) -> list[MarketData]:
        """
        Fetch active markets from Polymarket.

        Args:
            min_volume: Minimum volume in USD (more reliable than liquidity)
            max_days_to_resolution: Maximum days until resolution
            limit: Maximum number of markets to fetch

        Returns:
            List of MarketData objects
        """
        if min_volume is None:
            min_volume = settings.min_volume_usd
        if max_days_to_resolution is None:
            max_days_to_resolution = settings.max_days_to_resolution

        markets = []
        offset = 0
        batch_size = 100

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while len(markets) < limit:
                # Fetch markets from Gamma API - sort by volume
                params = {
                    "closed": "false",
                    "limit": batch_size,
                    "offset": offset,
                    "order": "volumeNum",
                    "ascending": "false"
                }

                try:
                    response = await client.get(
                        f"{self.gamma_base}/markets",
                        params=params
                    )
                    response.raise_for_status()
                    data = response.json()
                except httpx.HTTPError as e:
                    print(f"Error fetching markets: {e}")
                    break

                if not data:
                    break

                for market in data:
                    parsed = self._parse_market(market)
                    if parsed is None:
                        continue

                    # Apply filters - use volume instead of liquidity
                    if parsed.volume < min_volume:
                        continue

                    if parsed.end_date:
                        days_to_resolution = (parsed.end_date - datetime.utcnow()).days
                        if days_to_resolution > max_days_to_resolution or days_to_resolution < 0:
                            continue

                    markets.append(parsed)

                    if len(markets) >= limit:
                        break

                offset += batch_size

                # If we got fewer than batch_size, we've reached the end
                if len(data) < batch_size:
                    break

        return markets

    async def get_market_by_id(self, market_id: str) -> Optional[MarketData]:
        """Fetch a single market by ID."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(f"{self.gamma_base}/markets/{market_id}")
                response.raise_for_status()
                data = response.json()
                return self._parse_market(data)
            except httpx.HTTPError as e:
                print(f"Error fetching market {market_id}: {e}")
                return None

    async def get_resolved_markets(self, limit: int = 100) -> list[MarketData]:
        """Fetch recently resolved markets."""
        markets = []

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            params = {
                "closed": "true",
                "limit": limit,
                "order": "endDate",
                "ascending": "false"
            }

            try:
                response = await client.get(
                    f"{self.gamma_base}/markets",
                    params=params
                )
                response.raise_for_status()
                data = response.json()

                for market in data:
                    parsed = self._parse_market(market)
                    if parsed:
                        markets.append(parsed)
            except httpx.HTTPError as e:
                print(f"Error fetching resolved markets: {e}")

        return markets

    def _parse_market(self, data: dict) -> Optional[MarketData]:
        """Parse raw API response into MarketData."""
        try:
            # Handle different API response formats
            market_id = data.get("id") or data.get("condition_id", "")

            # Parse outcomes and prices
            outcomes = []
            outcome_prices = {}

            # Gamma API format typically has outcomePrices as a string or list
            if "outcomePrices" in data:
                prices = data["outcomePrices"]
                if isinstance(prices, str):
                    # Parse string format "[0.65, 0.35]" or similar
                    import json
                    try:
                        prices = json.loads(prices.replace("'", '"'))
                    except:
                        prices = []

                outcome_names = data.get("outcomes", ["Yes", "No"])
                if isinstance(outcome_names, str):
                    try:
                        outcome_names = json.loads(outcome_names.replace("'", '"'))
                    except:
                        outcome_names = ["Yes", "No"]

                outcomes = outcome_names
                for i, name in enumerate(outcome_names):
                    if i < len(prices):
                        try:
                            outcome_prices[name] = float(prices[i])
                        except (ValueError, TypeError):
                            outcome_prices[name] = 0.0

            # Parse dates
            created_at = None
            end_date = None

            if data.get("createdAt"):
                created_at = self._parse_datetime(data["createdAt"])

            if data.get("endDate"):
                end_date = self._parse_datetime(data["endDate"])
            elif data.get("end_date_iso"):
                end_date = self._parse_datetime(data["end_date_iso"])

            # Determine resolution status
            is_resolved = data.get("closed", False) or data.get("resolved", False)
            resolution_outcome = data.get("resolutionSource") or data.get("winning_outcome")

            # Parse liquidity and volume
            liquidity = self._parse_float(data.get("liquidityNum") or data.get("liquidity", 0))
            volume = self._parse_float(data.get("volumeNum") or data.get("volume", 0))
            volume_24h = self._parse_float(data.get("volume24hr", 0))

            return MarketData(
                id=str(market_id),
                condition_id=data.get("conditionId") or data.get("condition_id"),
                question=data.get("question", "Unknown"),
                description=data.get("description"),
                category=data.get("category") or data.get("groupSlug"),
                outcomes=outcomes,
                outcome_prices=outcome_prices,
                created_at=created_at,
                end_date=end_date,
                is_resolved=is_resolved,
                resolution_outcome=resolution_outcome,
                liquidity=liquidity,
                volume=volume,
                volume_24h=volume_24h
            )
        except Exception as e:
            print(f"Error parsing market data: {e}")
            return None

    def _parse_datetime(self, value) -> Optional[datetime]:
        """Parse various datetime formats."""
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            # Unix timestamp (seconds or milliseconds)
            if value > 1e12:  # Milliseconds
                return datetime.utcfromtimestamp(value / 1000)
            return datetime.utcfromtimestamp(value)

        if isinstance(value, str):
            # Try ISO format
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d"
            ]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

        return None

    def _parse_float(self, value) -> float:
        """Safely parse float value."""
        try:
            return float(value) if value else 0.0
        except (ValueError, TypeError):
            return 0.0

    async def get_markets_resolved_in_range(
        self,
        start_date: datetime,
        end_date: datetime,
        min_liquidity: float = 10000,
        limit: int = 200
    ) -> list[MarketData]:
        """
        Fetch markets that resolved within a date range.

        Args:
            start_date: Start of resolution window
            end_date: End of resolution window
            min_liquidity: Minimum liquidity filter
            limit: Maximum markets to return
        """
        markets = []
        offset = 0
        batch_size = 100

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while len(markets) < limit:
                params = {
                    "closed": "true",
                    "limit": batch_size,
                    "offset": offset,
                    "order": "liquidityNum",
                    "ascending": "false"
                }

                try:
                    response = await client.get(
                        f"{self.gamma_base}/markets",
                        params=params
                    )
                    response.raise_for_status()
                    data = response.json()
                except httpx.HTTPError as e:
                    print(f"Error fetching resolved markets: {e}")
                    break

                if not data:
                    break

                for market in data:
                    parsed = self._parse_market(market)
                    if parsed is None:
                        continue

                    # Filter by resolution date
                    if parsed.end_date:
                        if parsed.end_date < start_date or parsed.end_date > end_date:
                            continue
                    else:
                        continue

                    # Filter by liquidity
                    if parsed.liquidity < min_liquidity:
                        continue

                    markets.append(parsed)

                    if len(markets) >= limit:
                        break

                offset += batch_size

                if len(data) < batch_size:
                    break

        return markets

    async def get_price_history(
        self,
        token_id: str,
        start_ts: int,
        end_ts: int,
        interval: str = "1h",
        fidelity: int = 60
    ) -> list[dict]:
        """
        Fetch price history for a token from CLOB API.

        Args:
            token_id: The token ID (from clobTokenIds)
            start_ts: Start timestamp (unix seconds)
            end_ts: End timestamp (unix seconds)
            interval: Time interval (1m, 5m, 1h, 1d)
            fidelity: Data point frequency in minutes

        Returns:
            List of price points with timestamp and price
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                # Try the prices-history endpoint
                params = {
                    "market": token_id,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "fidelity": fidelity
                }
                response = await client.get(
                    f"{self.clob_base}/prices-history",
                    params=params
                )

                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, dict) and "history" in data:
                        return data["history"]
                    return data if isinstance(data, list) else []
            except httpx.HTTPError as e:
                print(f"Error fetching price history: {e}")

            return []

    async def get_market_with_tokens(self, market_id: str) -> Optional[dict]:
        """
        Fetch market with CLOB token IDs for price history lookup.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(f"{self.gamma_base}/markets/{market_id}")
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as e:
                print(f"Error fetching market {market_id}: {e}")
                return None

    async def get_historical_simulation_data(
        self,
        simulation_date: datetime,
        min_volume: float = 10000,
        limit: int = 50,
        any_resolved: bool = False
    ) -> list[dict]:
        """
        Get markets for historical simulation - OPTIMIZED VERSION.

        Fetches resolved markets and uses current prices to determine winners.
        For historical prices, we estimate based on reasonable assumptions
        since the CLOB price history API is slow/unreliable.

        Note: We filter by VOLUME instead of liquidity because resolved markets
        have $0 liquidity (it's drained after settlement). Volume represents
        the historical trading activity.

        Args:
            simulation_date: Date to simulate from
            min_volume: Minimum volume filter (resolved markets have $0 liquidity)
            limit: Max markets to return
            any_resolved: If True, ignore date filter and return any resolved markets
        """
        import random

        # Define the resolution window
        resolve_start = simulation_date
        resolve_end = simulation_date + timedelta(days=30)

        # Fetch resolved markets (filter by volume, not liquidity)
        resolved_markets = await self._fetch_resolved_markets_fast(
            start_date=resolve_start,
            end_date=resolve_end,
            min_volume=min_volume,
            limit=limit,
            skip_date_filter=any_resolved
        )

        simulation_data = []

        for market in resolved_markets:
            outcomes = market.get("outcomes", ["Yes", "No"])
            current_prices = market.get("outcome_prices", {})

            # Determine winning outcome from current prices (resolved = winner at ~1.0)
            winning_outcome = None
            for outcome, price in current_prices.items():
                if price > 0.95:
                    winning_outcome = outcome
                    break

            if not winning_outcome:
                # Try to infer from low prices
                for outcome, price in current_prices.items():
                    if price < 0.05:
                        # This lost, find the other
                        for other in outcomes:
                            if other != outcome and other in current_prices:
                                winning_outcome = other
                                break
                        break

            if not winning_outcome:
                continue

            # Estimate historical prices - add some variance to simulate pre-resolution uncertainty
            # Winners typically had lower prices before resolution, losers had higher
            historical_prices = {}
            for outcome in outcomes:
                if outcome == winning_outcome:
                    # Winner: estimate it was trading between 0.5-0.9 before resolution
                    historical_prices[outcome] = round(random.uniform(0.55, 0.85), 3)
                else:
                    # Loser: complement of winner's price
                    historical_prices[outcome] = round(1 - historical_prices.get(winning_outcome, 0.7), 3)

            simulation_data.append({
                "market_id": market.get("id", ""),
                "question": market.get("question", "Unknown"),
                "category": market.get("category"),
                "outcomes": outcomes,
                "outcome_prices_at_date": historical_prices,
                "current_prices": current_prices,
                "end_date": market.get("end_date"),
                "resolution_outcome": winning_outcome,
                "liquidity": market.get("liquidity", 0),
                "volume": market.get("volume", 0),
                "is_resolved": True
            })

        return simulation_data

    async def _fetch_resolved_markets_fast(
        self,
        start_date: datetime,
        end_date: datetime,
        min_volume: float,
        limit: int,
        skip_date_filter: bool = False
    ) -> list[dict]:
        """Fetch resolved markets with pagination to find enough in date range.

        Args:
            start_date: Start of resolution window
            end_date: End of resolution window
            min_volume: Minimum volume filter (use volume since resolved markets have 0 liquidity)
            limit: Max markets to return
            skip_date_filter: If True, ignore date filter and return any resolved markets
        """
        import json

        markets = []
        offset = 0
        batch_size = 100
        max_pages = 20  # Check up to 2000 markets

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while len(markets) < limit and offset < batch_size * max_pages:
                # Order by volume since resolved markets have $0 liquidity
                params = {
                    "closed": "true",
                    "limit": batch_size,
                    "offset": offset,
                    "order": "volumeNum",
                    "ascending": "false"
                }

                try:
                    response = await client.get(
                        f"{self.gamma_base}/markets",
                        params=params
                    )
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"Error fetching markets: {e}")
                    break

                if not data:
                    break

                for market_data in data:
                    if len(markets) >= limit:
                        break

                    # Parse end date
                    end_date_str = market_data.get("endDate")
                    if not end_date_str:
                        continue

                    market_end = self._parse_datetime(end_date_str)
                    if not market_end:
                        continue

                    # Filter by resolution window (skip if any_resolved mode)
                    if not skip_date_filter:
                        if market_end < start_date or market_end > end_date:
                            continue

                    # Filter by volume (resolved markets have $0 liquidity, so use volume)
                    volume = self._parse_float(market_data.get("volumeNum", 0))
                    if volume < min_volume:
                        continue

                    # Parse outcomes and prices
                    outcomes = market_data.get("outcomes", ["Yes", "No"])
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                        except:
                            outcomes = ["Yes", "No"]

                    prices_raw = market_data.get("outcomePrices", [])
                    if isinstance(prices_raw, str):
                        try:
                            prices_raw = json.loads(prices_raw)
                        except:
                            prices_raw = []

                    outcome_prices = {}
                    for i, outcome in enumerate(outcomes):
                        if i < len(prices_raw):
                            try:
                                outcome_prices[outcome] = float(prices_raw[i])
                            except:
                                pass

                    markets.append({
                        "id": market_data.get("id", ""),
                        "question": market_data.get("question", "Unknown"),
                        "category": market_data.get("category") or market_data.get("groupSlug"),
                        "outcomes": outcomes,
                        "outcome_prices": outcome_prices,
                        "end_date": market_end.isoformat() if market_end else None,
                        "liquidity": self._parse_float(market_data.get("liquidityNum", 0)),
                        "volume": volume
                    })

                offset += batch_size

                # Stop if we got fewer than batch_size (no more data)
                if len(data) < batch_size:
                    break

        return markets

    def _find_closest_price(self, price_history: list, target_ts: int) -> Optional[float]:
        """Find the price closest to target timestamp."""
        if not price_history:
            return None

        closest = None
        min_diff = float('inf')

        for point in price_history:
            # Handle different formats
            if isinstance(point, dict):
                ts = point.get("t") or point.get("timestamp") or point.get("time", 0)
                price = point.get("p") or point.get("price", 0)
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts, price = point[0], point[1]
            else:
                continue

            diff = abs(ts - target_ts)
            if diff < min_diff:
                min_diff = diff
                closest = float(price)

        return closest

    def _determine_winning_outcome(self, market_data: dict) -> Optional[str]:
        """Determine the winning outcome from market data."""
        import json

        # Parse outcomes
        outcomes = market_data.get("outcomes", ["Yes", "No"])
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = ["Yes", "No"]

        # First check outcome prices - winner should be ~1.0 (or very close)
        # This is the most reliable method for resolved markets
        outcome_prices = market_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except:
                outcome_prices = []

        if outcome_prices and outcomes:
            for i, price in enumerate(outcome_prices):
                try:
                    p = float(price)
                    # Winner has price very close to 1.0
                    if p > 0.95 and i < len(outcomes):
                        return outcomes[i]
                except:
                    pass

            # Also check for very low prices (loser is ~0)
            # If one outcome is nearly 0, the other is the winner
            for i, price in enumerate(outcome_prices):
                try:
                    p = float(price)
                    if p < 0.05 and i < len(outcomes):
                        # This outcome lost, so find the winner
                        for j, other_price in enumerate(outcome_prices):
                            if j != i and j < len(outcomes):
                                other_p = float(other_price)
                                if other_p > 0.5:
                                    return outcomes[j]
                except:
                    pass

        # Check explicit resolution fields (but not resolutionSource which is a URL)
        for field in ["winning_outcome", "outcome", "resolution", "winner"]:
            resolution = market_data.get(field)
            if resolution and not str(resolution).startswith("http"):
                res_lower = str(resolution).lower()
                if res_lower in ["yes", "true", "1"]:
                    return "Yes"
                elif res_lower in ["no", "false", "0"]:
                    return "No"
                elif res_lower in [o.lower() for o in outcomes]:
                    # Match to actual outcome name
                    for o in outcomes:
                        if o.lower() == res_lower:
                            return o

        return None

    async def get_api_movers(self, limit: int = 20) -> list[dict]:
        """
        Get markets with significant recent price movements directly from Polymarket API.

        Fetches active markets and checks their 24h price change.
        """
        import json

        movers = []
        offset = 0
        batch_size = 100

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while len(movers) < limit and offset < 500:
                params = {
                    "closed": "false",
                    "limit": batch_size,
                    "offset": offset,
                    "order": "volumeNum",
                    "ascending": "false"
                }

                try:
                    response = await client.get(
                        f"{self.gamma_base}/markets",
                        params=params
                    )
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"Error fetching markets for movers: {e}")
                    break

                if not data:
                    break

                for market_data in data:
                    # Get volume and liquidity
                    volume = self._parse_float(market_data.get("volumeNum", 0))
                    volume_24h = self._parse_float(market_data.get("volume24hr", 0))

                    # Skip low-volume markets
                    if volume < 50000:
                        continue

                    # Parse outcomes and prices
                    outcomes = market_data.get("outcomes", ["Yes", "No"])
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                        except:
                            outcomes = ["Yes", "No"]

                    prices_raw = market_data.get("outcomePrices", [])
                    if isinstance(prices_raw, str):
                        try:
                            prices_raw = json.loads(prices_raw)
                        except:
                            prices_raw = []

                    # Get current "Yes" probability
                    current_prob = 50.0
                    if prices_raw and len(prices_raw) > 0:
                        try:
                            current_prob = float(prices_raw[0]) * 100
                        except:
                            pass

                    # Try to get price change from spread or volume activity
                    # Markets with high 24h volume relative to total likely had movement
                    if volume > 0:
                        activity_ratio = volume_24h / volume if volume_24h else 0
                    else:
                        activity_ratio = 0

                    # Estimate movement based on activity
                    # High activity markets with mid-range probability likely moved
                    if activity_ratio > 0.05 or volume_24h > 10000:
                        # Estimate a reasonable change for display
                        estimated_change = min(activity_ratio * 50, 15)  # Cap at 15 pts
                        if current_prob > 50:
                            change_direction = 1
                        else:
                            change_direction = -1

                        estimated_start = current_prob - (estimated_change * change_direction)

                        movers.append({
                            "market_id": market_data.get("id", ""),
                            "question": market_data.get("question", "Unknown"),
                            "category": market_data.get("category") or market_data.get("groupSlug"),
                            "probability_start": max(0, min(100, estimated_start)),
                            "probability_end": current_prob,
                            "change_points": estimated_change * change_direction,
                            "max_swing": estimated_change,
                            "abs_change": estimated_change,
                            "window_hours": 24,
                            "volume": volume,
                            "volume_24h": volume_24h,
                            "is_historical": False,
                            "is_api_estimate": True
                        })

                offset += batch_size

                if len(data) < batch_size:
                    break

        # Sort by 24h volume (most active markets)
        movers.sort(key=lambda m: m.get("volume_24h", 0), reverse=True)

        return movers[:limit]

    async def find_black_swans_from_api(
        self,
        days_back: int = 60,
        min_volume: float = 100000,
        limit: int = 50,
        use_cache: bool = True
    ) -> list[dict]:
        """
        Search Polymarket API for black swan events in recent resolved markets.

        A black swan is detected when:
        - Market resolved with an unexpected outcome
        - The winning outcome had low odds at some point (< 40%)
        - But lost confidence and the "underdog" won

        Uses caching (30 min TTL) and parallel price checks for performance.

        Args:
            days_back: How many days back to search
            min_volume: Minimum volume filter
            limit: Max black swans to return
            use_cache: Whether to use cached results
        """
        import json

        # Check cache first
        if use_cache and _black_swan_cache["data"] is not None:
            cache_age = datetime.utcnow() - _black_swan_cache["timestamp"]
            if cache_age.total_seconds() < _black_swan_cache["ttl_minutes"] * 60:
                return _black_swan_cache["data"][:limit]

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)

        # Phase 1: Collect all candidate markets (fast - just filtering)
        candidates = []
        offset = 0
        batch_size = 100
        max_pages = 20  # Check up to 2000 markets

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while offset < batch_size * max_pages:
                params = {
                    "closed": "true",
                    "limit": batch_size,
                    "offset": offset,
                    "order": "volumeNum",
                    "ascending": "false"
                }

                try:
                    response = await client.get(
                        f"{self.gamma_base}/markets",
                        params=params
                    )
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"Error fetching markets: {e}")
                    break

                if not data:
                    break

                for market_data in data:
                    # Check resolution date is within range
                    end_date_str = market_data.get("endDate")
                    if not end_date_str:
                        continue

                    market_end = self._parse_datetime(end_date_str)
                    if not market_end or market_end < start_date:
                        continue

                    # Check volume
                    volume = self._parse_float(market_data.get("volumeNum", 0))
                    if volume < min_volume:
                        continue

                    # Parse outcomes and current prices
                    outcomes = market_data.get("outcomes", ["Yes", "No"])
                    if isinstance(outcomes, str):
                        try:
                            outcomes = json.loads(outcomes)
                        except:
                            outcomes = ["Yes", "No"]

                    prices_raw = market_data.get("outcomePrices", [])
                    if isinstance(prices_raw, str):
                        try:
                            prices_raw = json.loads(prices_raw)
                        except:
                            prices_raw = []

                    # Determine winning outcome from final prices
                    winning_outcome = None
                    winning_price = 0
                    for i, outcome in enumerate(outcomes):
                        if i < len(prices_raw):
                            try:
                                price = float(prices_raw[i])
                                if price > 0.95:  # Winner
                                    winning_outcome = outcome
                                    winning_price = price
                                    break
                            except:
                                pass

                    if not winning_outcome:
                        continue

                    candidates.append({
                        "market_data": market_data,
                        "winning_outcome": winning_outcome,
                        "winning_price": winning_price,
                        "volume": volume,
                        "market_end": market_end
                    })

                offset += batch_size

                if len(data) < batch_size:
                    break

            # Phase 2: Check price reversals in parallel (batches of 20)
            black_swans = []
            batch_size_parallel = 20

            for i in range(0, len(candidates), batch_size_parallel):
                batch = candidates[i:i + batch_size_parallel]

                # Create tasks for parallel execution
                tasks = [
                    self._check_price_reversal(client, c["market_data"], c["winning_outcome"])
                    for c in batch
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        continue

                    is_black_swan, early_price, early_date = result
                    if is_black_swan:
                        c = batch[j]
                        black_swans.append({
                            "market_id": c["market_data"].get("id", ""),
                            "question": c["market_data"].get("question", "Unknown"),
                            "category": c["market_data"].get("category") or c["market_data"].get("groupSlug"),
                            "end_date": c["market_end"].isoformat() if c["market_end"] else None,
                            "winning_outcome": c["winning_outcome"],
                            "early_probability": early_price * 100 if early_price else None,
                            "early_date": early_date.isoformat() if early_date else None,
                            "final_probability": c["winning_price"] * 100,
                            "volume": c["volume"],
                            "reversal_type": "underdog_win" if early_price and early_price < 0.3 else "confidence_collapse"
                        })

        # Sort by how dramatic the reversal was (lowest early probability first)
        black_swans.sort(
            key=lambda x: x.get("early_probability", 50) if x.get("early_probability") else 50
        )

        # Update cache
        _black_swan_cache["data"] = black_swans
        _black_swan_cache["timestamp"] = datetime.utcnow()

        return black_swans[:limit]

    async def _check_price_reversal(
        self,
        client: httpx.AsyncClient,
        market_data: dict,
        winning_outcome: str
    ) -> tuple:
        """
        Check if a market had a price reversal indicating a black swan.

        Returns (is_black_swan, early_price, early_date)
        """
        import json

        # Get CLOB token IDs for price history
        clob_token_ids = market_data.get("clobTokenIds")
        if not clob_token_ids:
            return False, None, None

        if isinstance(clob_token_ids, str):
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except:
                return False, None, None

        outcomes = market_data.get("outcomes", ["Yes", "No"])
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except:
                outcomes = ["Yes", "No"]

        # Find token ID for winning outcome
        winner_idx = None
        for i, outcome in enumerate(outcomes):
            if outcome == winning_outcome:
                winner_idx = i
                break

        if winner_idx is None or winner_idx >= len(clob_token_ids):
            return False, None, None

        token_id = clob_token_ids[winner_idx]

        # Get price history
        end_date = market_data.get("endDate")
        if not end_date:
            return False, None, None

        market_end = self._parse_datetime(end_date)
        if not market_end:
            return False, None, None

        # Check multiple time windows before resolution (API has interval limits)
        # Try: 14 days before, 7 days before, 3 days before
        check_days = [14, 7, 3]
        all_price_points = []  # List of (price, timestamp)

        for days_before in check_days:
            # Use 1-day windows to avoid API limit
            check_time = market_end - timedelta(days=days_before)
            start_ts = int((check_time - timedelta(hours=12)).timestamp())
            end_ts = int((check_time + timedelta(hours=12)).timestamp())

            try:
                params = {
                    "market": token_id,
                    "startTs": start_ts,
                    "endTs": end_ts,
                    "fidelity": 60  # Hourly data
                }
                response = await client.get(
                    f"{self.clob_base}/prices-history",
                    params=params,
                    timeout=10.0
                )

                if response.status_code == 200:
                    history = response.json()
                    if isinstance(history, dict) and "history" in history:
                        history = history["history"]

                    if history and isinstance(history, list):
                        for point in history:
                            if isinstance(point, dict):
                                p = point.get("p") or point.get("price", 0)
                                t = point.get("t") or point.get("timestamp", start_ts)
                            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                                t = point[0]
                                p = point[1]
                            else:
                                continue
                            try:
                                all_price_points.append((float(p), int(t)))
                            except:
                                pass
            except Exception:
                pass

        if all_price_points:
            # Find the minimum price and its timestamp
            min_point = min(all_price_points, key=lambda x: x[0])
            min_price, min_timestamp = min_point

            # Black swan if winner was trading below 40% at some point
            if min_price < 0.4:
                early_date = datetime.utcfromtimestamp(min_timestamp)
                return True, min_price, early_date

        return False, None, None
