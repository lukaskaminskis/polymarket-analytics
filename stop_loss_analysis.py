"""
Stop-Loss Analysis for Polymarket Trading Strategy

Analyzes:
1. How do prices behave before they crash?
2. What's the optimal stop-loss level?
3. Can we detect early warning signs?
"""

import asyncio
import httpx
import json
from datetime import datetime, timedelta
from collections import defaultdict

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"


async def get_price_trajectory(client, market, resolution_date, days_list):
    """Get prices at multiple points before resolution."""
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

    # Get winning outcome index
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

    trajectory = {}

    for days in days_list:
        check_date = resolution_date - timedelta(days=days)
        start_ts = int((check_date - timedelta(hours=6)).timestamp())
        end_ts = int((check_date + timedelta(hours=6)).timestamp())

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
                        trajectory[days] = sum(prices) / len(prices)
        except:
            pass

    return trajectory if len(trajectory) >= 3 else None


async def analyze_stop_losses():
    """Analyze different stop-loss strategies."""

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
        print("\nAnalyzing price trajectories...")

        # Check prices at: 30, 25, 20, 15, 10, 7, 5, 3 days before resolution
        check_days = [30, 25, 20, 15, 10, 7, 5, 3]

        results = []

        for idx, market in enumerate(markets):
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

            trajectory = await get_price_trajectory(client, market, resolution_date, check_days)
            if not trajectory or 30 not in trajectory:
                continue

            entry_price = trajectory[30]

            # Only analyze 85-95% entries
            if entry_price < 0.85 or entry_price > 0.95:
                continue

            # Determine if this was ultimately a winner or loser
            final_price = trajectory.get(3, trajectory.get(5, trajectory.get(7)))
            if final_price is None:
                continue

            profit_pct = (final_price - entry_price) / entry_price * 100
            is_winner = profit_pct > 0

            results.append({
                "question": market.get("question", "")[:50],
                "entry_price": entry_price,
                "trajectory": trajectory,
                "final_price": final_price,
                "profit_pct": profit_pct,
                "is_winner": is_winner
            })

            if len(results) % 30 == 0:
                print(f"Analyzed {len(results)} trades...")

    return results


def simulate_stop_loss(results, stop_loss_pct):
    """Simulate strategy with a specific stop-loss percentage."""
    trades = []

    for r in results:
        entry = r["entry_price"]
        stop_price = entry * (1 - stop_loss_pct / 100)

        # Check if stop-loss would have been triggered
        stopped_out = False
        exit_price = r["final_price"]

        trajectory = r["trajectory"]
        check_days = sorted(trajectory.keys(), reverse=True)  # 30, 25, 20, ...

        for day in check_days:
            if day == 30:  # Entry day
                continue
            price = trajectory[day]
            if price <= stop_price:
                stopped_out = True
                exit_price = stop_price  # Assume we exit at stop price
                break

        actual_profit = (exit_price - entry) / entry * 100

        trades.append({
            "entry": entry,
            "exit": exit_price,
            "profit_pct": actual_profit,
            "stopped_out": stopped_out,
            "original_outcome": "win" if r["is_winner"] else "loss"
        })

    return trades


def print_analysis(results):
    print("\n" + "=" * 80)
    print("STOP-LOSS STRATEGY ANALYSIS")
    print("=" * 80)

    print(f"\nTotal trades analyzed: {len(results)}")

    # Baseline: No stop-loss
    winners = [r for r in results if r["is_winner"]]
    losers = [r for r in results if not r["is_winner"]]

    print(f"\nBASELINE (No Stop-Loss):")
    print(f"  Winners: {len(winners)} ({len(winners)/len(results)*100:.1f}%)")
    print(f"  Losers: {len(losers)} ({len(losers)/len(results)*100:.1f}%)")

    total_return = sum(r["profit_pct"] for r in results)
    avg_return = total_return / len(results)
    print(f"  Average return: {avg_return:.2f}%")
    print(f"  Total return (sum): {total_return:.1f}%")

    if losers:
        avg_loss = sum(r["profit_pct"] for r in losers) / len(losers)
        worst_loss = min(r["profit_pct"] for r in losers)
        print(f"  Average loss: {avg_loss:.2f}%")
        print(f"  Worst loss: {worst_loss:.2f}%")

    # Test different stop-loss levels
    print("\n" + "-" * 80)
    print("STOP-LOSS COMPARISON")
    print("-" * 80)
    print(f"{'Stop-Loss':<12} {'Stopped':<10} {'Wins':<10} {'Losses':<10} {'Avg Return':<12} {'Total Return':<12} {'Max Loss'}")
    print("-" * 80)

    stop_loss_levels = [3, 5, 7, 10, 15, 20, None]

    for sl in stop_loss_levels:
        if sl is None:
            # No stop loss
            trades = [{"profit_pct": r["profit_pct"], "stopped_out": False} for r in results]
            sl_label = "None"
        else:
            trades = simulate_stop_loss(results, sl)
            sl_label = f"{sl}%"

        stopped = sum(1 for t in trades if t["stopped_out"])
        wins = sum(1 for t in trades if t["profit_pct"] > 0)
        losses = sum(1 for t in trades if t["profit_pct"] <= 0)
        avg_ret = sum(t["profit_pct"] for t in trades) / len(trades)
        total_ret = sum(t["profit_pct"] for t in trades)
        max_loss = min(t["profit_pct"] for t in trades)

        print(f"{sl_label:<12} {stopped:<10} {wins:<10} {losses:<10} {avg_ret:<10.2f}%  {total_ret:<10.1f}%  {max_loss:.1f}%")

    print("-" * 80)

    # Analyze losing trades trajectory
    print("\n" + "-" * 80)
    print("LOSING TRADE TRAJECTORIES (How fast do they drop?)")
    print("-" * 80)

    for r in sorted(losers, key=lambda x: x["profit_pct"])[:10]:
        trajectory = r["trajectory"]
        entry = r["entry_price"]

        print(f"\n{r['question']}...")
        print(f"  Entry (30d): {entry*100:.1f}%")

        for day in sorted(trajectory.keys(), reverse=True):
            if day == 30:
                continue
            price = trajectory[day]
            change = (price - entry) / entry * 100
            marker = " *** WARNING ***" if change < -5 else ""
            print(f"  {day:2d}d before:  {price*100:.1f}% ({change:+.1f}%){marker}")

    # Early warning analysis
    print("\n" + "-" * 80)
    print("EARLY WARNING DETECTION")
    print("-" * 80)

    # Check if losing trades showed warning signs at 20d or 15d
    early_warnings = 0
    for r in losers:
        trajectory = r["trajectory"]
        entry = r["entry_price"]

        # Check 20 days before
        if 20 in trajectory:
            drop_at_20d = (trajectory[20] - entry) / entry * 100
            if drop_at_20d < -3:
                early_warnings += 1

    if losers:
        print(f"  Losing trades with >3% drop at 20 days: {early_warnings}/{len(losers)} ({early_warnings/len(losers)*100:.0f}%)")

    print("""
RECOMMENDATIONS:
----------------
1. Use a 5-7% stop-loss to limit catastrophic losses
2. Monitor positions daily - if price drops >3% in first 10 days, consider early exit
3. Avoid markets with unusual volatility or binary event risk
4. Diversify across 10+ markets to absorb occasional stop-outs
""")


async def main():
    results = await analyze_stop_losses()
    print_analysis(results)


if __name__ == "__main__":
    asyncio.run(main())
