#!/usr/bin/env python3
"""
Batch Ingestion Script - Memory-Safe for 24GB Systems

Ingests Phase 2 (daily) and Phase 4 (minute) data from Landing to Bronze layer
using streaming mode to minimize memory usage.

Processes data in monthly batches to stay within memory constraints.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, timedelta, datetime as dt
import argparse


def ingest_month(data_type: str, year: int, month: int, log_file):
    """Ingest a single month of data using streaming mode"""
    start_date = f"{year}-{month:02d}-01"
    # Simple end-of-month: day 31 (CLI will handle invalid dates)
    end_date = f"{year}-{month:02d}-31"

    print(f"[{dt.now().strftime('%H:%M:%S')}] Ingesting {data_type}: {year}-{month:02d}", flush=True)
    log_file.write(f"[{dt.now().strftime('%H:%M:%S')}] Ingesting {data_type}: {year}-{month:02d}\n")
    log_file.flush()

    cmd = [
        "quantmini", "data", "ingest",
        "--data-type", data_type,
        "--start-date", start_date,
        "--end-date", end_date,
        "--mode", "streaming"  # Use streaming mode for low memory
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        if result.returncode == 0:
            print(f"  ✅ Success: {data_type} {year}-{month:02d}", flush=True)
            log_file.write(f"  ✅ Success\n")
        else:
            print(f"  ❌ Failed: {result.stderr[:200]}", flush=True)
            log_file.write(f"  ❌ Failed: {result.stderr[:200]}\n")
        log_file.flush()
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  Timeout: {data_type} {year}-{month:02d}", flush=True)
        log_file.write(f"  ⏱️  Timeout\n")
        log_file.flush()
        return False
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:200]}", flush=True)
        log_file.write(f"  ❌ Error: {str(e)[:200]}\n")
        log_file.flush()
        return False


def main():
    parser = argparse.ArgumentParser(description='Batch ingestion with streaming mode (24GB memory safe)')
    parser.add_argument('--log-file', type=str, help='Log file path')
    parser.add_argument('--daily-only', action='store_true', help='Ingest daily data only')
    parser.add_argument('--minute-only', action='store_true', help='Ingest minute data only')
    args = parser.parse_args()

    # Open log file
    if args.log_file:
        log_file = open(args.log_file, 'w', buffering=1)
    else:
        from src.utils.paths import get_quantlake_root
        quantlake_root = get_quantlake_root()
        log_path = quantlake_root / "logs" / f"batch_ingestion_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file = open(log_path, 'w', buffering=1)

    print("="*80)
    print("BATCH INGESTION - STREAMING MODE (24GB MEMORY SAFE)")
    print("="*80)
    print()

    log_file.write("="*80 + "\n")
    log_file.write("BATCH INGESTION - STREAMING MODE (24GB MEMORY SAFE)\n")
    log_file.write("="*80 + "\n\n")

    # Current date
    today = date.today()
    current_year = today.year
    current_month = today.month

    stocks_daily_success = 0
    stocks_daily_fail = 0
    options_daily_success = 0
    options_daily_fail = 0

    if not args.minute_only:
        # Ingest stocks_daily: 2020-10 to present
        print("📊 Ingesting stocks_daily (2020-10 to present)...")
        print(f"   Using streaming mode for low memory usage")
        print()

        log_file.write("📊 Ingesting stocks_daily (2020-10 to present)...\n")
        log_file.write(f"   Using streaming mode for low memory usage\n\n")

        # 2020: October onwards
        for month in range(10, 13):  # Oct, Nov, Dec
            if ingest_month("stocks_daily", 2020, month, log_file):
                stocks_daily_success += 1
            else:
                stocks_daily_fail += 1

        # 2021-2023: Full years
        for year in range(2021, 2024):
            for month in range(1, 13):
                if ingest_month("stocks_daily", year, month, log_file):
                    stocks_daily_success += 1
                else:
                    stocks_daily_fail += 1

        # 2024-current: Up to current month
        for year in range(2024, current_year + 1):
            end_month = 13 if year < current_year else current_month + 1
            for month in range(1, end_month):
                if ingest_month("stocks_daily", year, month, log_file):
                    stocks_daily_success += 1
                else:
                    stocks_daily_fail += 1

        print()
        print(f"Stocks Daily Summary: ✅ {stocks_daily_success} months, ❌ {stocks_daily_fail} failed")
        print()
        log_file.write(f"\nStocks Daily Summary: ✅ {stocks_daily_success} months, ❌ {stocks_daily_fail} failed\n\n")

        # Ingest options_daily: 2023-10 to present
        print("📈 Ingesting options_daily (2023-10 to present)...")
        print(f"   Using streaming mode for low memory usage")
        print()

        log_file.write("📈 Ingesting options_daily (2023-10 to present)...\n")
        log_file.write(f"   Using streaming mode for low memory usage\n\n")

        # 2023: October onwards
        for month in range(10, 13):
            if ingest_month("options_daily", 2023, month, log_file):
                options_daily_success += 1
            else:
                options_daily_fail += 1

        # 2024-current: Up to current month
        for year in range(2024, current_year + 1):
            end_month = 13 if year < current_year else current_month + 1
            for month in range(1, end_month):
                if ingest_month("options_daily", year, month, log_file):
                    options_daily_success += 1
                else:
                    options_daily_fail += 1

        print()
        print(f"Options Daily Summary: ✅ {options_daily_success} months, ❌ {options_daily_fail} failed")
        print()
        log_file.write(f"\nOptions Daily Summary: ✅ {options_daily_success} months, ❌ {options_daily_fail} failed\n\n")

    # Overall summary
    print("="*80)
    print("BATCH INGESTION SUMMARY")
    print("="*80)
    if not args.minute_only:
        print(f"Stocks Daily:  ✅ {stocks_daily_success}/{stocks_daily_success+stocks_daily_fail} months")
        print(f"Options Daily: ✅ {options_daily_success}/{options_daily_success+options_daily_fail} months")
    print(f"Total: ✅ {stocks_daily_success+options_daily_success} months ingested")
    print("="*80)

    log_file.write("="*80 + "\n")
    log_file.write("BATCH INGESTION SUMMARY\n")
    log_file.write("="*80 + "\n")
    if not args.minute_only:
        log_file.write(f"Stocks Daily:  ✅ {stocks_daily_success}/{stocks_daily_success+stocks_daily_fail} months\n")
        log_file.write(f"Options Daily: ✅ {options_daily_success}/{options_daily_success+options_daily_fail} months\n")
    log_file.write(f"Total: ✅ {stocks_daily_success+options_daily_success} months ingested\n")
    log_file.write("="*80 + "\n")

    log_file.close()

    return 0 if (stocks_daily_fail == 0 and options_daily_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
