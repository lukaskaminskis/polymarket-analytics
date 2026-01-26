"""
Polymarket 30-Day Prediction Accuracy Analysis

Analyzes how accurate Polymarket predictions are ~30 days before resolution.
Checks the leading position (highest probability outcome) 30 days before resolution
and compares it to the actual outcome.
"""

import asyncio
import httpx
import json
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

@dataclass
class MarketResult:
    market_id: str
    question: str
    leading_outcome_30d: str
    leading_prob_30d: float
    actual_winner: str
    was_correct: bool
    confidence_bucket: str
    resolution_date: datetime


async def fetch_resolved_markets(limit: int = 3000):
    """Fetch resolved markets sorted by volume."""
    markets = []
    offset = 0
    batch_size = 100

    async with httpx.AsyncClient(timeout=30.0) as client:
        while len(markets) < limit:
            params = {
                "closed": "true",
                "limit": batch_size,
                "offset": offset,
                "order": "volumeNum",
                "ascending": "false"
            }

            try:
                response = await client.get(f"{GAMMA_API}/markets", params=params)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"Error fetching markets at offset {offset}: {e}")
                break

            if not data:
                break

            markets.extend(data)
            offset += batch_size

            if len(data) < batch_size:
                break

            # Progress update
            if len(markets) % 500 == 0:
                print(f"Fetched {len(markets)} markets...")

    return markets[:limit]


def parse_datetime(dt_str: str):
    """Parse datetime string."""
    if not dt_str:
        return None
    try:
        # Try ISO format
        if 'T' in dt_str:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00').replace('+00:00', ''))
        return datetime.strptime(dt_str, "%Y-%m-%d")
    except:
        return None


def get_winning_outcome(market: dict):
    """Determine winning outcome from market data. Returns (outcome_name, outcome_index)."""
    outcomes = market.get("outcomes", ["Yes", "No"])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except:
            outcomes = ["Yes", "No"]

    prices = market.get("outcomePrices", [])
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except:
            prices = []

    # Winner has price > 0.95 (resolved to 1)
    for i, outcome in enumerate(outcomes):
        if i < len(prices):
            try:
                price = float(prices[i])
                if price > 0.95:
                    return outcome, i
            except:
                pass

    return None, None


async def get_price_30_days_before(
    client: httpx.AsyncClient,
    market: dict,
    resolution_date: datetime
):
    """
    Get the leading outcome and its probability 30 days before resolution.
    Returns (leading_outcome, probability) or (None, None) if unavailable.
    """
    clob_token_ids = market.get("clobTokenIds")
    if not clob_token_ids:
        return None, None

    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except:
            return None, None

    outcomes = market.get("outcomes", ["Yes", "No"])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except:
            outcomes = ["Yes", "No"]

    # Calculate 30 days before resolution
    check_date = resolution_date - timedelta(days=30)
    start_ts = int((check_date - timedelta(hours=12)).timestamp())
    end_ts = int((check_date + timedelta(hours=12)).timestamp())

    # Get prices for all outcomes
    outcome_prices = {}

    for i, token_id in enumerate(clob_token_ids):
        if i >= len(outcomes):
            break

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
                    # Get average price in the window
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
                        outcome_prices[outcomes[i]] = sum(prices) / len(prices)
        except Exception:
            pass

    if not outcome_prices:
        return None, None

    # Find the leading outcome (highest probability)
    leading = max(outcome_prices.items(), key=lambda x: x[1])
    return leading[0], leading[1]


def get_confidence_bucket(prob: float):
    """Assign probability to a confidence bucket."""
    prob_pct = prob * 100
    if prob_pct < 50:
        return "<50%"
    elif prob_pct < 60:
        return "50-60%"
    elif prob_pct < 70:
        return "60-70%"
    elif prob_pct < 80:
        return "70-80%"
    elif prob_pct < 90:
        return "80-90%"
    else:
        return "90-100%"


async def analyze_markets(markets, max_analyze: int = 3000):
    """Analyze markets for 30-day prediction accuracy."""
    results = []
    analyzed = 0
    skipped = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Process in batches for parallel requests
        batch_size = 20

        for i in range(0, min(len(markets), max_analyze), batch_size):
            batch = markets[i:i + batch_size]
            tasks = []
            valid_markets = []

            for market in batch:
                # Get resolution date
                end_date = parse_datetime(market.get("endDate"))
                if not end_date:
                    skipped += 1
                    continue

                # Get winning outcome
                winner, winner_idx = get_winning_outcome(market)
                if not winner:
                    skipped += 1
                    continue

                valid_markets.append((market, end_date, winner))
                tasks.append(get_price_30_days_before(client, market, end_date))

            if tasks:
                price_results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, result in enumerate(price_results):
                    if isinstance(result, Exception):
                        skipped += 1
                        continue

                    leading_outcome, leading_prob = result
                    if not leading_outcome or leading_prob is None:
                        skipped += 1
                        continue

                    market, end_date, winner = valid_markets[j]

                    # Determine if prediction was correct
                    was_correct = (leading_outcome == winner)
                    bucket = get_confidence_bucket(leading_prob)

                    results.append(MarketResult(
                        market_id=market.get("id", ""),
                        question=market.get("question", "Unknown")[:80],
                        leading_outcome_30d=leading_outcome,
                        leading_prob_30d=leading_prob,
                        actual_winner=winner,
                        was_correct=was_correct,
                        confidence_bucket=bucket,
                        resolution_date=end_date
                    ))
                    analyzed += 1

            # Progress update
            if (i + batch_size) % 200 == 0:
                print(f"Analyzed {analyzed} markets, skipped {skipped}...")

    return results


def print_analysis(results):
    """Print the analysis results."""
    print("\n" + "=" * 80)
    print("POLYMARKET 30-DAY PREDICTION ACCURACY ANALYSIS")
    print("=" * 80)

    # Overall stats
    total = len(results)
    correct = sum(1 for r in results if r.was_correct)

    print(f"\nTotal markets analyzed: {total}")
    print(f"Overall accuracy: {correct}/{total} ({100*correct/total:.1f}%)")

    # By confidence bucket
    buckets = defaultdict(lambda: {"total": 0, "correct": 0})
    bucket_order = ["<50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"]

    for r in results:
        buckets[r.confidence_bucket]["total"] += 1
        if r.was_correct:
            buckets[r.confidence_bucket]["correct"] += 1

    print("\n" + "-" * 80)
    print("ACCURACY BY CONFIDENCE BRACKET (30 days before resolution)")
    print("-" * 80)
    print(f"{'Confidence':<12} {'Markets':<10} {'Correct':<10} {'Accuracy':<12} {'Calibration'}")
    print("-" * 80)

    for bucket in bucket_order:
        if bucket in buckets:
            data = buckets[bucket]
            total_b = data["total"]
            correct_b = data["correct"]
            accuracy = 100 * correct_b / total_b if total_b > 0 else 0

            # Expected accuracy (midpoint of bucket)
            if bucket == "<50%":
                expected = 25  # Markets where leading was <50%
            elif bucket == "50-60%":
                expected = 55
            elif bucket == "60-70%":
                expected = 65
            elif bucket == "70-80%":
                expected = 75
            elif bucket == "80-90%":
                expected = 85
            else:
                expected = 95

            calibration = accuracy - expected
            cal_str = f"{calibration:+.1f}%" if bucket != "<50%" else "N/A"

            print(f"{bucket:<12} {total_b:<10} {correct_b:<10} {accuracy:<10.1f}%  {cal_str}")

    print("-" * 80)

    # Notable misses (high confidence wrong predictions)
    print("\n" + "-" * 80)
    print("NOTABLE MISSES (>80% confidence, wrong prediction)")
    print("-" * 80)

    high_conf_wrong = [r for r in results if r.leading_prob_30d >= 0.8 and not r.was_correct]
    high_conf_wrong.sort(key=lambda x: x.leading_prob_30d, reverse=True)

    for r in high_conf_wrong[:15]:
        print(f"{r.leading_prob_30d*100:.0f}% {r.leading_outcome_30d} -> Actually: {r.actual_winner}")
        print(f"   {r.question}")
        print()

    # Summary interpretation
    print("=" * 80)
    print("INTERPRETATION")
    print("=" * 80)
    print("""
A well-calibrated prediction market should have:
- 60-70% bracket: ~65% accuracy
- 70-80% bracket: ~75% accuracy
- 80-90% bracket: ~85% accuracy
- 90-100% bracket: ~95% accuracy

Positive calibration = market is UNDERCONFIDENT (predictions are better than odds suggest)
Negative calibration = market is OVERCONFIDENT (predictions are worse than odds suggest)
""")


async def main():
    print("Fetching resolved markets from Polymarket...")
    markets = await fetch_resolved_markets(limit=3000)
    print(f"Fetched {len(markets)} resolved markets")

    print("\nAnalyzing 30-day-before predictions...")
    print("(This will take a few minutes due to API rate limits)\n")

    results = await analyze_markets(markets, max_analyze=3000)

    print_analysis(results)

    # Save results to file
    output = []
    for r in results:
        output.append({
            "market_id": r.market_id,
            "question": r.question,
            "leading_outcome_30d": r.leading_outcome_30d,
            "leading_prob_30d": round(r.leading_prob_30d * 100, 1),
            "actual_winner": r.actual_winner,
            "was_correct": r.was_correct,
            "confidence_bucket": r.confidence_bucket,
            "resolution_date": r.resolution_date.isoformat() if r.resolution_date else None
        })

    with open("30day_accuracy_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to 30day_accuracy_results.json")


if __name__ == "__main__":
    asyncio.run(main())
