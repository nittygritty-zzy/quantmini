#!/usr/bin/env python3
"""
Phase 4: Download Minute Price Data from S3

Downloads stocks_minute (2020-10-17+) and options_minute (2020-10-17+)
from Polygon S3 flat files.

This is a LARGE download (~1TB total, 20-30 hours)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, datetime, timedelta
import argparse


def download_date_range(data_type: str, start_date: str, end_date: str, log_file):
    """Download a date range of minute data"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading {data_type}: {start_date} to {end_date}", flush=True)
    log_file.write(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading {data_type}: {start_date} to {end_date}\n")
    log_file.flush()

    cmd = [
        "quantmini", "data", "download",
        "--data-type", data_type,
        "--start-date", start_date,
        "--end-date", end_date
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
    parser = argparse.ArgumentParser(description='Phase 4: Download minute price data from S3')
    parser.add_argument('--log-file', type=str, help='Log file path')
    parser.add_argument('--stocks-only', action='store_true', help='Download stocks_minute only')
    parser.add_argument('--options-only', action='store_true', help='Download options_minute only')
    args = parser.parse_args()

    # Open log file
    if args.log_file:
        log_file = open(args.log_file, 'w', buffering=1)
    else:
        from src.utils.paths import get_quantlake_root
        quantlake_root = get_quantlake_root()
        log_path = quantlake_root / "logs" / f"phase4_minute_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file = open(log_path, 'w', buffering=1)

    print("="*80)
    print("PHASE 4: MINUTE PRICE DATA DOWNLOAD FROM S3")
    print("="*80)
    print("⚠️  WARNING: This is a LARGE download (~1TB, 20-30 hours)")
    print()

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 4: MINUTE PRICE DATA DOWNLOAD FROM S3\n")
    log_file.write("="*80 + "\n")
    log_file.write("⚠️  WARNING: This is a LARGE download (~1TB, 20-30 hours)\n\n")

    # Current date
    today = date.today()

    # Download in 3-month chunks to avoid timeouts
    # Stocks minute: 2020-10-17 to present
    # Options minute: 2020-10-17 to present

    stocks_success = 0
    stocks_fail = 0
    options_success = 0
    options_fail = 0

    if not args.options_only:
        print("📊 Downloading stocks_minute (2020-10-17 to present)...")
        print(f"   Estimated: ~500 GB total, ~12-15 hours")
        print(f"   Strategy: 3-month chunks to avoid timeouts")
        print()

        log_file.write("📊 Downloading stocks_minute (2020-10-17 to present)...\n")
        log_file.write(f"   Estimated: ~500 GB total, ~12-15 hours\n\n")

        # Start from 2020-10-17
        current_start = date(2020, 10, 17)

        while current_start < today:
            # Download in 3-month chunks
            current_end = min(current_start + timedelta(days=90), today)

            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')

            if download_date_range("stocks_minute", start_str, end_str, log_file):
                stocks_success += 1
            else:
                stocks_fail += 1

            current_start = current_end + timedelta(days=1)

        print()
        print(f"Stocks Minute Summary: ✅ {stocks_success} chunks, ❌ {stocks_fail} failed")
        print()
        log_file.write(f"\nStocks Minute Summary: ✅ {stocks_success} chunks, ❌ {stocks_fail} failed\n\n")

    if not args.stocks_only:
        print("📈 Downloading options_minute (2020-10-17 to present)...")
        print(f"   Estimated: ~500 GB total, ~12-15 hours")
        print(f"   Strategy: 3-month chunks to avoid timeouts")
        print()

        log_file.write("📈 Downloading options_minute (2020-10-17 to present)...\n")
        log_file.write(f"   Estimated: ~500 GB total, ~12-15 hours\n\n")

        # Start from 2020-10-17
        current_start = date(2020, 10, 17)

        while current_start < today:
            # Download in 3-month chunks
            current_end = min(current_start + timedelta(days=90), today)

            start_str = current_start.strftime('%Y-%m-%d')
            end_str = current_end.strftime('%Y-%m-%d')

            if download_date_range("options_minute", start_str, end_str, log_file):
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
    print("PHASE 4 DOWNLOAD SUMMARY")
    print("="*80)
    if not args.options_only:
        print(f"Stocks Minute:  ✅ {stocks_success}/{stocks_success+stocks_fail} chunks")
    if not args.stocks_only:
        print(f"Options Minute: ✅ {options_success}/{options_success+options_fail} chunks")
    print(f"Total: ✅ {stocks_success+options_success} chunks downloaded")
    print("="*80)

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 4 DOWNLOAD SUMMARY\n")
    log_file.write("="*80 + "\n")
    if not args.options_only:
        log_file.write(f"Stocks Minute:  ✅ {stocks_success}/{stocks_success+stocks_fail} chunks\n")
    if not args.stocks_only:
        log_file.write(f"Options Minute: ✅ {options_success}/{options_success+options_fail} chunks\n")
    log_file.write(f"Total: ✅ {stocks_success+options_success} chunks downloaded\n")
    log_file.write("="*80 + "\n")

    log_file.close()

    return 0 if (stocks_fail == 0 and options_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
