#!/usr/bin/env python3
"""
Benchmark Data Checker

检查Qlib数据目录中是否有可用的基准数据
Checks if benchmark data is available in Qlib data directory

Usage:
    uv run python scripts/check_benchmark.py
    uv run python scripts/check_benchmark.py --data-path /path/to/qlib/data
    uv run python scripts/check_benchmark.py --symbols SPY,QQQ,DIA
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Suppress gym warnings
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

import qlib
from qlib.data import D
import pandas as pd


# Common benchmark symbols
DEFAULT_BENCHMARKS = {
    "SPY": "S&P 500 ETF",
    "QQQ": "Nasdaq 100 ETF",
    "DIA": "Dow Jones ETF",
    "IWM": "Russell 2000 ETF",
    "VTI": "Total Stock Market ETF",
    "VOO": "Vanguard S&P 500 ETF",
}


def check_benchmark_exists(symbol: str, data_path: str, start_date: str, end_date: str) -> dict:
    """
    Check if benchmark data exists and return statistics

    Args:
        symbol: Benchmark symbol (e.g., "SPY")
        data_path: Path to Qlib data directory
        start_date: Start date for checking
        end_date: End date for checking

    Returns:
        dict with status and statistics
    """
    result = {
        "symbol": symbol,
        "exists": False,
        "data_points": 0,
        "date_range": None,
        "price_range": None,
        "error": None
    }

    try:
        # Try to read data
        data = D.features(
            [symbol],
            ["$close"],
            start_time=start_date,
            end_time=end_date
        )

        if data.empty:
            result["error"] = "Data is empty"
            return result

        # Data exists and is not empty
        result["exists"] = True
        result["data_points"] = len(data)

        # Get date range
        dates = data.index.get_level_values(0)
        date_min = pd.Timestamp(dates.min())
        date_max = pd.Timestamp(dates.max())
        result["date_range"] = (date_min, date_max)

        # Get price range
        prices = data["$close"]
        result["price_range"] = (prices.min(), prices.max())

    except Exception as e:
        result["error"] = str(e)

    return result


def print_benchmark_status(result: dict, description: str = ""):
    """Print formatted benchmark status"""
    symbol = result["symbol"]
    exists = result["exists"]

    # Status symbol
    status_symbol = "✓" if exists else "✗"
    status_color = "\033[92m" if exists else "\033[91m"  # Green or Red
    reset_color = "\033[0m"

    print(f"\n{status_color}{status_symbol}{reset_color} {symbol:6s} - {description}")

    if exists:
        print(f"  Data points: {result['data_points']:,}")
        if result['date_range']:
            start, end = result['date_range']
            print(f"  Date range:  {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
        if result['price_range']:
            min_price, max_price = result['price_range']
            print(f"  Price range: ${min_price:.2f} - ${max_price:.2f}")
    else:
        print(f"  Status: Not available")
        if result['error']:
            print(f"  Error: {result['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Check benchmark data availability in Qlib"
    )
    parser.add_argument(
        "--data-path",
        default="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        help="Path to Qlib data directory"
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated list of symbols to check (default: SPY,QQQ,DIA,IWM)"
    )
    parser.add_argument(
        "--start-date",
        help="Start date for checking (default: 2 months ago)"
    )
    parser.add_argument(
        "--end-date",
        help="End date for checking (default: today)"
    )
    parser.add_argument(
        "--region",
        default="us",
        choices=["us", "cn"],
        help="Market region"
    )

    args = parser.parse_args()

    # Set default date range
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    # Parse symbols
    if args.symbols:
        symbols_to_check = {}
        for symbol in args.symbols.split(","):
            symbol = symbol.strip()
            symbols_to_check[symbol] = f"Custom benchmark"
    else:
        symbols_to_check = DEFAULT_BENCHMARKS

    # Initialize Qlib
    print("=" * 80)
    print("BENCHMARK DATA CHECKER")
    print("=" * 80)
    print(f"\nData path: {args.data_path}")
    print(f"Region: {args.region}")
    print(f"Date range: {start_date} to {end_date}")

    try:
        qlib.init(provider_uri=args.data_path, region=args.region)
        print("✓ Qlib initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize Qlib: {e}")
        return 1

    # Check each benchmark
    print(f"\nChecking {len(symbols_to_check)} benchmark(s)...")
    print("=" * 80)

    results = []
    for symbol, description in symbols_to_check.items():
        result = check_benchmark_exists(symbol, args.data_path, start_date, end_date)
        results.append(result)
        print_benchmark_status(result, description)

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    available = [r for r in results if r["exists"]]
    unavailable = [r for r in results if not r["exists"]]

    print(f"\nAvailable benchmarks: {len(available)}/{len(results)}")
    if available:
        print("  " + ", ".join([r["symbol"] for r in available]))

    print(f"\nUnavailable benchmarks: {len(unavailable)}/{len(results)}")
    if unavailable:
        print("  " + ", ".join([r["symbol"] for r in unavailable]))

    # Recommendations
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)

    if available:
        print(f"\n✓ You can use these benchmarks in your strategies:")
        for result in available:
            print(f"  benchmark='{result['symbol']}'")

        print(f"\nExample:")
        print(f"```python")
        print(f"portfolio_metric, indicator_metric = backtest(")
        print(f"    strategy=strategy,")
        print(f"    benchmark='{available[0]['symbol']}',  # Recommended")
        print(f"    ...")
        print(f")")
        print(f"```")
    else:
        print("\n⚠️  No benchmark data available!")
        print("\nOption 1: Run without benchmark")
        print("```python")
        print("portfolio_metric, indicator_metric = backtest(")
        print("    strategy=strategy,")
        print("    benchmark=None,  # No benchmark")
        print("    ...")
        print(")")
        print("```")

        print("\nOption 2: Download benchmark data")
        print("See docs/BENCHMARK_DATA_GUIDE.md for instructions")

        print("\nQuick fix: Add benchmarks to your data collection")
        print("```bash")
        print("# Add these symbols to your data collection:")
        print("BENCHMARK_SYMBOLS = ['SPY', 'QQQ', 'DIA', 'IWM']")
        print("")
        print("# Then re-run data collection and conversion")
        print("uv run python scripts/ingest_polygon_stocks.py --symbols SPY,QQQ,DIA,IWM")
        print("uv run python scripts/convert_to_qlib.py --data-type stocks_daily")
        print("```")

    print()
    return 0 if available else 1


if __name__ == "__main__":
    sys.exit(main())
