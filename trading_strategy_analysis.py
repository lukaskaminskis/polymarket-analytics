"""
Analyze trading strategy: Buy high-confidence outcomes 30 days before,
sell before resolution. No fees on trades.
"""

import asyncio
import httpx
import json
from datetime import datetime, timedelta
from collections import defaultdict

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


async def get_price_at_days_before(client, market, resolution_date, days_before):
    """Get price at specific days before resolution."""
    clob_token_ids = market.get("clobTokenIds")
    if not clob_token_ids:
        return None

    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except:
            return None

    outcomes = market.get("outcomes", ["Yes", "No"])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except:
            outcomes = ["Yes", "No"]

    # Get winning outcome
    prices_raw = market.get("outcomePrices", [])
    if isinstance(prices_raw, str):
        try:
            prices_raw = json.loads(prices_raw)
        except:
            prices_raw = []

    winner_idx = None
    for i, outcome in enumerate(outcomes):
        if i < len(prices_raw):
            try:
                if float(prices_raw[i]) > 0.95:
                    winner_idx = i
                    break
            except:
                pass

    if winner_idx is None or winner_idx >= len(clob_token_ids):
        return None

    token_id = clob_token_ids[winner_idx]

    check_date = resolution_date - timedelta(days=days_before)
    start_ts = int((check_date - timedelta(hours=12)).timestamp())
    end_ts = int((check_date + timedelta(hours=12)).timestamp())

    try:
        params = {
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "fidelity": 60
        }
        response = await client.get(
            f"{CLOB_API}/prices-history",
            params=params,
            timeout=10.0
        )

        if response.status_code == 200:
            history = response.json()
            if isinstance(history, dict) and "history" in history:
                history = history["history"]

            if history and isinstance(history, list):
                prices = []
                for point in history:
                    if isinstance(point, dict):
                        p = point.get("p") or point.get("price", 0)
                    elif isinstance(point, (list, tuple)) and len(point) >= 2:
                        p = point[1]
                    else:
                        continue
                    try:
                        prices.append(float(p))
                    except:
                        pass

                if prices:
                    return sum(prices) / len(prices)
    except:
        pass

    return None


async def analyze_trading_strategy():
    """Analyze buy-at-30-days, sell-at-3-days strategy."""

    print("Fetching resolved markets...")
    markets = []
    offset = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(markets) < 2000:
            params = {
                "closed": "true",
                "limit": 100,
                "offset": offset,
                "order": "volumeNum",
                "ascending": "false"
            }

            try:
                response = await client.get(f"{GAMMA_API}/markets", params=params)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"Error: {e}")
                break

            if not data:
                break

            markets.extend(data)
            offset += 100

            if len(data) < 100:
                break

        print(f"Fetched {len(markets)} markets")
        print("\nAnalyzing trading opportunities...")

        results = []
        batch_size = 15

        for i in range(0, len(markets), batch_size):
            batch = markets[i:i + batch_size]

            for market in batch:
                end_date_str = market.get("endDate")
                if not end_date_str:
                    continue

                try:
                    if 'T' in end_date_str:
                        resolution_date = datetime.fromisoformat(
                            end_date_str.replace('Z', '').replace('+00:00', '')
                        )
                    else:
                        resolution_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                except:
                    continue

                # Get price 30 days before
                price_30d = await get_price_at_days_before(client, market, resolution_date, 30)
                if price_30d is None:
                    continue

                # Only interested in high-confidence outcomes (85-98%)
                if price_30d < 0.85 or price_30d > 0.98:
                    continue

                # Get price 3 days before (our sell point)
                price_3d = await get_price_at_days_before(client, market, resolution_date, 3)
                if price_3d is None:
                    continue

                results.append({
                    "question": market.get("question", "")[:60],
                    "price_30d": price_30d,
                    "price_3d": price_3d,
                    "profit_pct": (price_3d - price_30d) / price_30d * 100,
                    "absolute_profit": price_3d - price_30d
                })

            if len(results) % 50 == 0 and len(results) > 0:
                print(f"Found {len(results)} tradeable markets...")

    return results


def print_results(results):
    print("\n" + "=" * 80)
    print("TRADING STRATEGY ANALYSIS: Buy at 30 days, Sell at 3 days before resolution")
    print("=" * 80)

    if not results:
        print("No results found")
        return

    # Filter by entry price buckets
    buckets = {
        "85-90%": [r for r in results if 0.85 <= r["price_30d"] < 0.90],
        "90-95%": [r for r in results if 0.90 <= r["price_30d"] < 0.95],
        "95-98%": [r for r in results if 0.95 <= r["price_30d"] <= 0.98],
    }

    print(f"\nTotal tradeable markets found: {len(results)}")

    print("\n" + "-" * 80)
    print("RESULTS BY ENTRY PRICE BUCKET")
    print("-" * 80)
    print(f"{'Entry Price':<12} {'Count':<8} {'Avg Profit':<12} {'Win Rate':<10} {'Avg Win':<10} {'Avg Loss':<10}")
    print("-" * 80)

    for bucket_name, bucket_results in buckets.items():
        if not bucket_results:
            continue

        profits = [r["profit_pct"] for r in bucket_results]
        wins = [p for p in profits if p > 0]
        losses = [p for p in profits if p <= 0]

        avg_profit = sum(profits) / len(profits)
        win_rate = len(wins) / len(profits) * 100
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0

        print(f"{bucket_name:<12} {len(bucket_results):<8} {avg_profit:<10.2f}%  {win_rate:<8.1f}%  {avg_win:<8.2f}%  {avg_loss:<8.2f}%")

    print("-" * 80)

    # Overall stats
    all_profits = [r["profit_pct"] for r in results]
    wins = [r for r in results if r["profit_pct"] > 0]
    losses = [r for r in results if r["profit_pct"] <= 0]

    print(f"\nOVERALL STATISTICS:")
    print(f"  Total trades: {len(results)}")
    print(f"  Win rate: {len(wins)/len(results)*100:.1f}%")
    print(f"  Average profit per trade: {sum(all_profits)/len(all_profits):.2f}%")
    print(f"  Median profit: {sorted(all_profits)[len(all_profits)//2]:.2f}%")

    if wins:
        print(f"  Average winning trade: +{sum(r['profit_pct'] for r in wins)/len(wins):.2f}%")
    if losses:
        print(f"  Average losing trade: {sum(r['profit_pct'] for r in losses)/len(losses):.2f}%")

    # Simulate $1000 across all trades equally
    capital_per_trade = 1000 / len(results)
    total_return = sum(capital_per_trade * (1 + r["profit_pct"]/100) for r in results)

    print(f"\n  Simulated return on $1000 spread equally: ${total_return:.2f} ({(total_return/1000-1)*100:.1f}%)")

    # Show worst losses
    print("\n" + "-" * 80)
    print("WORST LOSSES (when high-confidence prediction flipped)")
    print("-" * 80)

    worst = sorted(results, key=lambda x: x["profit_pct"])[:10]
    for r in worst:
        print(f"  {r['price_30d']*100:.0f}% -> {r['price_3d']*100:.0f}% ({r['profit_pct']:.1f}%): {r['question']}")

    # Show best wins
    print("\n" + "-" * 80)
    print("BEST WINS")
    print("-" * 80)

    best = sorted(results, key=lambda x: x["profit_pct"], reverse=True)[:10]
    for r in best:
        print(f"  {r['price_30d']*100:.0f}% -> {r['price_3d']*100:.0f}% ({r['profit_pct']:.1f}%): {r['question']}")


async def main():
    results = await analyze_trading_strategy()
    print_results(results)

    with open("trading_strategy_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to trading_strategy_results.json")


if __name__ == "__main__":
    asyncio.run(main())
