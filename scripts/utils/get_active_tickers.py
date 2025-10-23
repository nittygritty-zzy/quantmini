#!/usr/bin/env python3
"""
Get Active Tickers from Reference Data

Queries the bronze/reference_data/tickers table and returns a list of active tickers.

Usage:
    # Get all active common stocks
    python scripts/utils/get_active_tickers.py

    # Get top N by market cap (requires ticker details)
    python scripts/utils/get_active_tickers.py --limit 100

    # Get specific ticker types
    python scripts/utils/get_active_tickers.py --types CS,ETF

    # Output as space-separated (for shell scripts)
    python scripts/utils/get_active_tickers.py --format space

    # Output as newline-separated
    python scripts/utils/get_active_tickers.py --format lines
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.paths import get_quantlake_root
import polars as pl


def get_active_tickers(
    ticker_types: list[str] = ['CS'],
    limit: int | None = None,
    active_only: bool = True
) -> list[str]:
    """
    Get list of active tickers from reference data.

    Args:
        ticker_types: List of ticker types to include (CS, ETF, etc.)
        limit: Maximum number of tickers to return
        active_only: Only return active tickers

    Returns:
        List of ticker symbols
    """
    quantlake_root = get_quantlake_root()
    tickers_dir = quantlake_root / 'bronze' / 'reference_data' / 'tickers'

    all_tickers = []

    for ticker_type in ticker_types:
        ticker_file = tickers_dir / f'locale=us/type={ticker_type}/data.parquet'

        if not ticker_file.exists():
            continue

        df = pl.read_parquet(ticker_file)

        # Filter active tickers
        if active_only and 'active' in df.columns:
            df = df.filter(pl.col('active') == True)

        # Get ticker symbols
        tickers = df.select('ticker').to_series().to_list()
        all_tickers.extend(tickers)

    # Apply limit if specified
    if limit is not None:
        all_tickers = all_tickers[:limit]

    return all_tickers


def main():
    parser = argparse.ArgumentParser(
        description='Get active tickers from reference data'
    )
    parser.add_argument(
        '--types',
        default='CS',
        help='Comma-separated ticker types (default: CS)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of tickers to return'
    )
    parser.add_argument(
        '--format',
        choices=['space', 'lines', 'json'],
        default='space',
        help='Output format (default: space)'
    )
    parser.add_argument(
        '--include-inactive',
        action='store_true',
        help='Include inactive tickers'
    )

    args = parser.parse_args()

    # Parse ticker types
    ticker_types = [t.strip() for t in args.types.split(',')]

    # Get tickers
    try:
        tickers = get_active_tickers(
            ticker_types=ticker_types,
            limit=args.limit,
            active_only=not args.include_inactive
        )

        # Output in requested format
        if args.format == 'space':
            print(' '.join(tickers))
        elif args.format == 'lines':
            for ticker in tickers:
                print(ticker)
        elif args.format == 'json':
            import json
            print(json.dumps(tickers))

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
