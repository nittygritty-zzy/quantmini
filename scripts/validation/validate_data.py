#!/usr/bin/env python3
"""
Fast data validation script - checks completeness and file integrity

Usage:
    python scripts/validation/validate_data.py                    # Validate all tables
    python scripts/validation/validate_data.py --quick            # Quick check only
    python scripts/validation/validate_data.py --table=balance_sheets  # Single table
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from src.utils.paths import get_quantlake_root


def validate_table(conn, table_name, table_path, expected_tickers=9900):
    """Validate a single table for completeness and integrity"""

    print(f"\n{'='*80}")
    print(f"VALIDATING: {table_name}")
    print(f"{'='*80}")

    results = {
        'table': table_name,
        'status': 'PASS',
        'issues': []
    }

    try:
        # Check 1: Can we read the files?
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT tickers[1]) as unique_tickers,
            MIN(filing_date) as earliest_date,
            MAX(filing_date) as latest_date
        FROM read_parquet('{table_path}/**/*.parquet')
        WHERE tickers IS NOT NULL AND array_length(tickers) > 0
        """

        result = conn.execute(query).fetchone()
        total_records = result[0]
        unique_tickers = result[1]
        earliest_date = result[2]
        latest_date = result[3]

        print(f"✅ Files readable")
        print(f"   Total Records:    {total_records:,}")
        print(f"   Unique Tickers:   {unique_tickers:,}")
        print(f"   Date Range:       {earliest_date} to {latest_date}")

        # Check 2: Do we have expected number of tickers?
        coverage_pct = (unique_tickers / expected_tickers) * 100
        print(f"   Coverage:         {coverage_pct:.1f}% of {expected_tickers:,} expected")

        if unique_tickers < expected_tickers * 0.95:  # Less than 95%
            results['status'] = 'WARN'
            missing = expected_tickers - unique_tickers
            results['issues'].append(f"Missing {missing:,} tickers ({100-coverage_pct:.1f}% incomplete)")
            print(f"⚠️  Missing {missing:,} tickers")

        # Check 3: Do we have recent data?
        if isinstance(latest_date, str):
            from datetime import date
            latest_date = date.fromisoformat(latest_date)
        days_old = (datetime.now().date() - latest_date).days
        if days_old > 90:
            results['status'] = 'WARN'
            results['issues'].append(f"Latest data is {days_old} days old")
            print(f"⚠️  Latest data is {days_old} days old")
        else:
            print(f"✅ Recent data (latest: {days_old} days ago)")

        # Check 4: Average records per ticker (should be ~60 for quarterly over 15 years)
        avg_records = total_records / unique_tickers if unique_tickers > 0 else 0
        print(f"   Avg Records/Ticker: {avg_records:.1f} (expected ~60)")

        if avg_records < 40:
            results['status'] = 'WARN'
            results['issues'].append(f"Low average records per ticker: {avg_records:.1f}")
            print(f"⚠️  Low average records per ticker")

        # Check 5: Any NULL or invalid data?
        null_query = f"""
        SELECT
            SUM(CASE WHEN tickers IS NULL OR array_length(tickers) = 0 THEN 1 ELSE 0 END) as null_tickers,
            SUM(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END) as null_dates,
            SUM(CASE WHEN fiscal_year IS NULL THEN 1 ELSE 0 END) as null_fiscal_year
        FROM read_parquet('{table_path}/**/*.parquet')
        """

        null_result = conn.execute(null_query).fetchone()
        null_tickers = null_result[0]
        null_dates = null_result[1]
        null_fiscal_year = null_result[2]

        if null_tickers > 0 or null_dates > 0 or null_fiscal_year > 0:
            results['status'] = 'FAIL'
            results['issues'].append(f"Found NULL values: tickers={null_tickers}, dates={null_dates}, fiscal_year={null_fiscal_year}")
            print(f"❌ NULL values found: tickers={null_tickers}, dates={null_dates}, fiscal_year={null_fiscal_year}")
        else:
            print(f"✅ No NULL values in critical fields")

        # Check 6: Duplicates?
        dup_query = f"""
        SELECT COUNT(*) as duplicate_groups
        FROM (
            SELECT
                tickers[1] as ticker,
                filing_date,
                fiscal_period,
                COUNT(*) as cnt
            FROM read_parquet('{table_path}/**/*.parquet')
            WHERE tickers IS NOT NULL
            GROUP BY ticker, filing_date, fiscal_period
            HAVING COUNT(*) > 1
        )
        """

        dup_result = conn.execute(dup_query).fetchone()
        duplicate_groups = dup_result[0]

        if duplicate_groups > 0:
            results['status'] = 'WARN'
            results['issues'].append(f"Found {duplicate_groups} duplicate records")
            print(f"⚠️  Found {duplicate_groups} duplicate record groups")
        else:
            print(f"✅ No duplicate records")

        # Summary
        if results['status'] == 'PASS':
            print(f"\n✅ {table_name}: PASS")
        elif results['status'] == 'WARN':
            print(f"\n⚠️  {table_name}: PASS with warnings")
        else:
            print(f"\n❌ {table_name}: FAIL")

    except Exception as e:
        results['status'] = 'FAIL'
        results['issues'].append(f"Error reading files: {str(e)}")
        print(f"❌ Error: {e}")

    return results


def quick_check(conn, table_name, table_path):
    """Quick check - just count records and tickers"""
    try:
        query = f"""
        SELECT
            COUNT(*) as records,
            COUNT(DISTINCT tickers[1]) as tickers
        FROM read_parquet('{table_path}/**/*.parquet')
        """
        result = conn.execute(query).fetchone()
        print(f"{table_name:20s} | {result[0]:8,} records | {result[1]:5,} tickers")
        return True
    except Exception as e:
        print(f"{table_name:20s} | ❌ ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Fast data validation')
    parser.add_argument('--quick', action='store_true', help='Quick check only (counts)')
    parser.add_argument('--table', type=str, help='Validate single table')
    args = parser.parse_args()

    print("="*80)
    print("DATA VALIDATION")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    data_root = get_quantlake_root()
    fundamentals_path = data_root / "bronze/fundamentals"

    # Define tables
    tables = {
        'balance_sheets': fundamentals_path / 'balance_sheets',
        'income_statements': fundamentals_path / 'income_statements',
        'cash_flow': fundamentals_path / 'cash_flow',
    }

    # Filter to single table if requested
    if args.table:
        if args.table not in tables:
            print(f"❌ Unknown table: {args.table}")
            print(f"Available tables: {', '.join(tables.keys())}")
            return 1
        tables = {args.table: tables[args.table]}

    conn = duckdb.connect(':memory:')

    # Quick mode
    if args.quick:
        print(f"\n{'Table':<20s} | {'Records':>8s} | {'Tickers':>7s}")
        print("-"*50)

        all_ok = True
        for table_name, table_path in tables.items():
            if not quick_check(conn, table_name, table_path):
                all_ok = False

        print("\n" + "="*80)
        if all_ok:
            print("✅ Quick check: All tables readable")
            return 0
        else:
            print("❌ Quick check: Some tables have errors")
            return 1

    # Full validation
    results = []
    for table_name, table_path in tables.items():
        result = validate_table(conn, table_name, table_path, expected_tickers=9900)
        results.append(result)

    # Overall summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    passed = sum(1 for r in results if r['status'] == 'PASS')
    warned = sum(1 for r in results if r['status'] == 'WARN')
    failed = sum(1 for r in results if r['status'] == 'FAIL')

    print(f"Total Tables:  {len(results)}")
    print(f"✅ Passed:     {passed}")
    print(f"⚠️  Warnings:   {warned}")
    print(f"❌ Failed:     {failed}")

    # List issues
    all_issues = []
    for r in results:
        if r['issues']:
            all_issues.extend([f"{r['table']}: {issue}" for issue in r['issues']])

    if all_issues:
        print(f"\nIssues Found:")
        for issue in all_issues:
            print(f"  - {issue}")

    print("="*80)

    # Exit code
    if failed > 0:
        return 1
    elif warned > 0:
        return 0  # Warnings are OK
    else:
        return 0


if __name__ == '__main__':
    sys.exit(main())
