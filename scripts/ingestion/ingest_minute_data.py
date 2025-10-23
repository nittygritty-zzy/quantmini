#!/usr/bin/env python3
"""
Phase 4: Ingest Minute Price Data from Landing to Bronze

Processes stocks_minute and options_minute data in 3-month chunks
using streaming mode for memory efficiency (24GB systems).

This is a LARGE ingestion (~22GB raw data, several hours).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, timedelta, datetime as dt
import argparse


def ingest_date_range(data_type: str, start_date: str, end_date: str, log_file):
    """Ingest a date range of minute data using streaming mode"""
    print(f"[{dt.now().strftime('%H:%M:%S')}] Ingesting {data_type}: {start_date} to {end_date}", flush=True)
    log_file.write(f"[{dt.now().strftime('%H:%M:%S')}] Ingesting {data_type}: {start_date} to {end_date}\n")
    log_file.flush()

    cmd = [
        "quantmini", "data", "ingest",
        "--data-type", data_type,
        "--start-date", start_date,
        "--end-date", end_date,
        "--mode", "streaming"  # Use streaming mode for low memory
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2 hour timeout
        if result.returncode == 0:
            print(f"  ✅ Success: {data_type} {start_date} to {end_date}", flush=True)
            log_file.write(f"  ✅ Success\n")
        else:
            print(f"  ❌ Failed: {result.stderr[:200]}", flush=True)
            log_file.write(f"  ❌ Failed: {result.stderr[:200]}\n")
        log_file.flush()
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  Timeout: {data_type} {start_date} to {end_date}", flush=True)
        log_file.write(f"  ⏱️  Timeout\n")
        log_file.flush()
        return False
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:200]}", flush=True)
        log_file.write(f"  ❌ Error: {str(e)[:200]}\n")
        log_file.flush()
        return False


def main():
    parser = argparse.ArgumentParser(description='Phase 4: Ingest minute data (streaming mode, 24GB safe)')
    parser.add_argument('--log-file', type=str, help='Log file path')
    parser.add_argument('--stocks-only', action='store_true', help='Ingest stocks_minute only')
    parser.add_argument('--options-only', action='store_true', help='Ingest options_minute only')
    args = parser.parse_args()

    # Open log file
    if args.log_file:
        log_file = open(args.log_file, 'w', buffering=1)
    else:
        from src.utils.paths import get_quantlake_root
        quantlake_root = get_quantlake_root()
        log_path = quantlake_root / "logs" / f"minute_ingestion_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file = open(log_path, 'w', buffering=1)

    print("="*80)
    print("PHASE 4: MINUTE DATA INGESTION - STREAMING MODE")
    print("="*80)
    print("⚠️  This will take several hours (~22GB data)")
    print()

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 4: MINUTE DATA INGESTION - STREAMING MODE\n")
    log_file.write("="*80 + "\n")
    log_file.write("⚠️  This will take several hours (~22GB data)\n\n")

    # Current date
    today = date.today()

    # Process in 3-month chunks to minimize memory usage
    stocks_success = 0
    stocks_fail = 0
    options_success = 0
    options_fail = 0

    if not args.options_only:
        print("📊 Ingesting stocks_minute (2020-10-17 to present)...")
        print(f"   Using 3-month chunks with streaming mode")
        print()

        log_file.write("📊 Ingesting stocks_minute (2020-10-17 to present)...\n")
        log_file.write(f"   Using 3-month chunks with streaming mode\n\n")

        # Start from 2020-10-17
        current_start = date(2020, 10, 17)

        while current_start < today:
            # Process in 3-month chunks (90 days)
            current_end = min(current_start + timedelta(days=90), today)

            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')

            if ingest_date_range("stocks_minute", start_str, end_str, log_file):
                stocks_success += 1
            else:
                stocks_fail += 1

            current_start = current_end + timedelta(days=1)

        print()
        print(f"Stocks Minute Summary: ✅ {stocks_success} chunks, ❌ {stocks_fail} failed")
        print()
        log_file.write(f"\nStocks Minute Summary: ✅ {stocks_success} chunks, ❌ {stocks_fail} failed\n\n")

    if not args.stocks_only:
        print("📈 Ingesting options_minute (2020-10-17 to present)...")
        print(f"   Using 3-month chunks with streaming mode")
        print()

        log_file.write("📈 Ingesting options_minute (2020-10-17 to present)...\n")
        log_file.write(f"   Using 3-month chunks with streaming mode\n\n")

        # Start from 2020-10-17
        current_start = date(2020, 10, 17)

        while current_start < today:
            # Process in 3-month chunks (90 days)
            current_end = min(current_start + timedelta(days=90), today)

            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')

            if ingest_date_range("options_minute", start_str, end_str, log_file):
                options_success += 1
            else:
                options_fail += 1

            current_start = current_end + timedelta(days=1)

        print()
        print(f"Options Minute Summary: ✅ {options_success} chunks, ❌ {options_fail} failed")
        print()
        log_file.write(f"\nOptions Minute Summary: ✅ {options_success} chunks, ❌ {options_fail} failed\n\n")

    # Overall summary
    print("="*80)
    print("PHASE 4 INGESTION SUMMARY")
    print("="*80)
    if not args.options_only:
        print(f"Stocks Minute:  ✅ {stocks_success}/{stocks_success+stocks_fail} chunks")
    if not args.stocks_only:
        print(f"Options Minute: ✅ {options_success}/{options_success+options_fail} chunks")
    print(f"Total: ✅ {stocks_success+options_success} chunks ingested")
    print("="*80)

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 4 INGESTION SUMMARY\n")
    log_file.write("="*80 + "\n")
    if not args.options_only:
        log_file.write(f"Stocks Minute:  ✅ {stocks_success}/{stocks_success+stocks_fail} chunks\n")
    if not args.stocks_only:
        log_file.write(f"Options Minute: ✅ {options_success}/{options_success+options_fail} chunks\n")
    log_file.write(f"Total: ✅ {stocks_success+options_success} chunks ingested\n")
    log_file.write("="*80 + "\n")

    log_file.close()

    return 0 if (stocks_fail == 0 and options_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
