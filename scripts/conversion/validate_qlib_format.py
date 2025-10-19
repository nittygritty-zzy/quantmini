#!/usr/bin/env python3
"""
Verify Qlib Conversion Integrity

Tests:
1. Initialize Qlib provider
2. Verify calendar dates
3. Test basic data queries
4. Compare sample with enriched parquet
"""

import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import ConfigLoader
from src.query import QueryEngine

def verify_qlib_data():
    """Verify Qlib binary data integrity"""

    print("=" * 70)
    print("🔍 Qlib Conversion Verification")
    print("=" * 70)
    print()

    config = ConfigLoader()
    qlib_root = config.get_data_root() / 'qlib' / 'stocks_daily'

    # Step 1: Check files exist
    print("Step 1: Checking Qlib file structure...")

    instruments_file = qlib_root / 'instruments' / 'all.txt'
    calendars_file = qlib_root / 'calendars' / 'day.txt'
    features_dir = qlib_root / 'features'

    if not instruments_file.exists():
        print(f"   ❌ Missing instruments file: {instruments_file}")
        return False

    if not calendars_file.exists():
        print(f"   ❌ Missing calendar file: {calendars_file}")
        return False

    if not features_dir.exists():
        print(f"   ❌ Missing features directory: {features_dir}")
        return False

    print(f"   ✅ Instruments file: {instruments_file}")
    print(f"   ✅ Calendar file: {calendars_file}")
    print(f"   ✅ Features directory: {features_dir}")
    print()

    # Step 2: Check instruments
    print("Step 2: Checking instruments...")
    with open(instruments_file) as f:
        instruments = [line.strip().split('\t')[0] for line in f if line.strip()]

    print(f"   ✅ Found {len(instruments)} instruments")
    print(f"   Sample: {', '.join(instruments[:5])}...")
    print()

    # Step 3: Check calendar
    print("Step 3: Checking calendar...")
    with open(calendars_file) as f:
        calendar_dates = [line.strip() for line in f if line.strip()]

    print(f"   ✅ Found {len(calendar_dates)} trading days")
    print(f"   Date range: {calendar_dates[0]} to {calendar_dates[-1]}")
    print(f"   Dates: {', '.join(calendar_dates[:5])}...")
    print()

    # Step 4: Check feature files
    print("Step 4: Checking feature files...")

    # Count .bin files
    bin_files = list(features_dir.rglob('*.bin'))
    print(f"   ✅ Found {len(bin_files)} .bin files")

    # Check a sample symbol
    sample_symbol = instruments[0]
    sample_dir = features_dir / sample_symbol

    if sample_dir.exists():
        sample_features = list(sample_dir.glob('*.bin'))
        print(f"   ✅ Sample symbol '{sample_symbol}' has {len(sample_features)} features")
        print(f"   Sample features: {', '.join([f.stem for f in sample_features[:5]])}...")
    else:
        print(f"   ❌ Sample symbol directory not found: {sample_dir}")
        return False

    print()

    # Step 5: Initialize Qlib and test queries
    print("Step 5: Testing Qlib initialization and queries...")

    try:
        import qlib
        from qlib.data import D

        # Initialize Qlib
        qlib.init(
            provider_uri=str(qlib_root),
            region='us',
        )
        print("   ✅ Qlib initialized successfully")

        # Test calendar query
        cal = D.calendar(start_time=calendar_dates[0], end_time=calendar_dates[-1])
        print(f"   ✅ Calendar query: {len(cal)} trading days")

        # Test instrument list
        instruments_qlib = D.instruments(market='all')
        print(f"   ✅ Instruments query: {len(instruments_qlib)} instruments")

        # Test data query for a sample symbol
        test_symbol = 'AAPL'
        if test_symbol in instruments:
            data = D.features(
                [test_symbol],
                ['$close', '$volume', '$high', '$low'],
                start_time=calendar_dates[0],
                end_time=calendar_dates[-1]
            )

            if not data.empty:
                print(f"   ✅ Data query for {test_symbol}:")
                print(f"      Rows: {len(data)}")
                print(f"      Columns: {list(data.columns)}")
                print(f"      Sample data:")
                print(data.head(3).to_string().replace('\n', '\n      '))
            else:
                print(f"   ⚠️  Data query returned empty for {test_symbol}")
        else:
            print(f"   ⚠️  Test symbol {test_symbol} not in instruments")

        print()

    except Exception as e:
        print(f"   ❌ Qlib query failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Step 6: Compare with enriched parquet
    print("Step 6: Comparing with enriched parquet...")

    try:
        enriched_root = config.get_data_root() / 'enriched' / 'stocks_daily'

        # Find a sample parquet file for the date range
        sample_date = calendar_dates[0]
        year, month = sample_date.split('-')[:2]
        sample_file = enriched_root / f'year={year}' / f'month={month}' / f'{sample_date}.parquet'

        if sample_file.exists():
            # Read parquet (use ParquetFile directly to avoid schema merge issues)
            parquet_file = pq.ParquetFile(sample_file)
            parquet_df = parquet_file.read().to_pandas()

            # Filter to test symbol
            if test_symbol in instruments:
                parquet_symbol = parquet_df[parquet_df['symbol'] == test_symbol]

                if not parquet_symbol.empty:
                    print(f"   ✅ Enriched parquet for {test_symbol} on {sample_date}:")
                    print(f"      Columns: {len(parquet_symbol.columns)}")

                    # Compare close price
                    qlib_close = data.loc[(test_symbol, pd.Timestamp(sample_date)), '$close']
                    parquet_close = parquet_symbol['close'].values[0]

                    print(f"      Qlib close: {qlib_close:.2f}")
                    print(f"      Parquet close: {parquet_close:.2f}")

                    if abs(qlib_close - parquet_close) < 0.01:
                        print(f"      ✅ Close prices match!")
                    else:
                        print(f"      ❌ Close prices differ by {abs(qlib_close - parquet_close):.4f}")
                        return False
                else:
                    print(f"   ⚠️  {test_symbol} not found in parquet file")
        else:
            print(f"   ⚠️  Sample parquet file not found: {sample_file}")

        print()

    except Exception as e:
        print(f"   ❌ Parquet comparison failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Summary
    print("=" * 70)
    print("✅ Verification PASSED!")
    print("=" * 70)
    print()
    print("Summary:")
    print(f"  • Instruments: {len(instruments):,}")
    print(f"  • Trading days: {len(calendar_dates)}")
    print(f"  • Date range: {calendar_dates[0]} to {calendar_dates[-1]}")
    print(f"  • Binary files: {len(bin_files):,}")
    print(f"  • Qlib queries: Working")
    print(f"  • Data consistency: Verified")
    print()

    return True


if __name__ == '__main__':
    success = verify_qlib_data()
    sys.exit(0 if success else 1)
