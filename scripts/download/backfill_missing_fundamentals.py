#!/usr/bin/env python3
"""
Backfill missing fundamental data for tickers

This script identifies tickers that are missing fundamental data and attempts
to download them from the Polygon API.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import argparse
import duckdb
from src.utils.paths import get_quantlake_root


def get_missing_tickers():
    """Get list of tickers missing fundamental data"""
    bronze = get_quantlake_root() / "bronze"

    conn = duckdb.connect(':memory:')

    query = f"""
    WITH active_stocks AS (
        SELECT DISTINCT ticker, name
        FROM read_parquet('{bronze}/reference_data/tickers/**/*.parquet')
        WHERE active = true
          AND type = 'CS'
          AND market = 'stocks'
          -- Exclude preferred shares and notes (usually have suffixes)
          AND ticker NOT LIKE '%-%'
          AND ticker NOT LIKE '%.%'
          AND LENGTH(ticker) <= 5
    ),
    has_fundamentals AS (
        SELECT DISTINCT tickers[1] as ticker
        FROM read_parquet('{bronze}/fundamentals/balance_sheets/**/*.parquet')
    )
    SELECT a.ticker, a.name
    FROM active_stocks a
    LEFT JOIN has_fundamentals h ON a.ticker = h.ticker
    WHERE h.ticker IS NULL
    ORDER BY a.ticker
    """

    result = conn.execute(query).fetchall()
    return [(row[0], row[1]) for row in result]


def main():
    parser = argparse.ArgumentParser(description='Backfill missing fundamental data')
    parser.add_argument('--limit', type=int, help='Limit number of tickers to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be downloaded without downloading')
    args = parser.parse_args()

    print("="*80)
    print("BACKFILL MISSING FUNDAMENTALS")
    print("="*80)

    # Get missing tickers
    missing = get_missing_tickers()

    if args.limit:
        missing = missing[:args.limit]

    print(f"\nFound {len(missing)} tickers missing fundamental data")

    if args.dry_run:
        print("\nDRY RUN - Would download fundamentals for:")
        for ticker, name in missing[:20]:
            print(f"  {ticker:8s} - {name}")
        if len(missing) > 20:
            print(f"  ... and {len(missing)-20} more")
        return

    # Import here to avoid loading if dry-run
    import subprocess

    print(f"\nDownloading fundamentals for {len(missing)} tickers...")
    print("This may take a while (est. 2-3 hours for 1,000 tickers)\n")

    success_count = 0
    fail_count = 0

    for i, (ticker, name) in enumerate(missing, 1):
        print(f"[{i}/{len(missing)}] {ticker:8s} - {name[:50]}")

        try:
            # Download fundamentals for this ticker
            cmd = [
                "quantmini", "polygon", "fundamentals", ticker,
                "--timeframe", "quarterly",
                "--filing-date-gte", "2010-01-01",
                "--output-dir", str(get_quantlake_root() / "bronze" / "fundamentals")
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                success_count += 1
                print(f"  ✅ Success")
            else:
                fail_count += 1
                # Many failures are expected (no fundamentals available)
                if "No data" in result.stderr or "404" in result.stderr:
                    print(f"  ⚠️  No fundamentals available")
                else:
                    print(f"  ❌ Failed: {result.stderr[:100]}")

        except subprocess.TimeoutExpired:
            fail_count += 1
            print(f"  ❌ Timeout")
        except Exception as e:
            fail_count += 1
            print(f"  ❌ Error: {str(e)[:100]}")

    print("\n" + "="*80)
    print("BACKFILL SUMMARY")
    print("="*80)
    print(f"Total tickers processed: {len(missing)}")
    print(f"✅ Successfully downloaded: {success_count}")
    print(f"❌ Failed/No data: {fail_count}")
    print("="*80)


if __name__ == "__main__":
    main()
