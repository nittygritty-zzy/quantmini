#!/usr/bin/env python3
"""
Phase 2: Download Daily Price Data from S3

Downloads stocks_daily (2020-10-26+) and options_daily (2023-10-24+)
from Polygon S3 flat files.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, datetime
import argparse


def download_month(data_type: str, year: int, month: int, log_file):
    """Download a single month of data"""
    start_date = f"{year}-{month:02d}-01"
    # Simple end-of-month: just use day 31 (API will handle invalid dates)
    end_date = f"{year}-{month:02d}-31"

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading {data_type}: {year}-{month:02d}", flush=True)
    log_file.write(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading {data_type}: {year}-{month:02d}\n")
    log_file.flush()

    cmd = [
        "quantmini", "data", "download",
        "--data-type", data_type,
        "--start-date", start_date,
        "--end-date", end_date
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
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
    parser = argparse.ArgumentParser(description='Phase 2: Download daily price data from S3')
    parser.add_argument('--log-file', type=str, help='Log file path')
    args = parser.parse_args()

    # Open log file
    if args.log_file:
        log_file = open(args.log_file, 'w', buffering=1)  # Line buffering
    else:
        from src.utils.paths import get_quantlake_root
        quantlake_root = get_quantlake_root()
        log_path = quantlake_root / "logs" / f"phase2_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file = open(log_path, 'w', buffering=1)

    print("="*80)
    print("PHASE 2: DAILY PRICE DATA DOWNLOAD FROM S3")
    print("="*80)
    print()

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 2: DAILY PRICE DATA DOWNLOAD FROM S3\n")
    log_file.write("="*80 + "\n\n")

    # Current date
    today = date.today()
    current_year = today.year
    current_month = today.month

    # Download stocks_daily: 2020-10-26 to present
    print("📊 Downloading stocks_daily (2020-10 to present)...")
    print(f"   Estimated: ~50 months, ~10GB total")
    print()

    log_file.write("📊 Downloading stocks_daily (2020-10 to present)...\n")
    log_file.write(f"   Estimated: ~50 months, ~10GB total\n\n")

    stocks_success = 0
    stocks_fail = 0

    # 2020: October onwards
    for month in range(10, 13):  # Oct, Nov, Dec
        if download_month("stocks_daily", 2020, month, log_file):
            stocks_success += 1
        else:
            stocks_fail += 1

    # 2021-2023: Full years
    for year in range(2021, 2024):
        for month in range(1, 13):
            if download_month("stocks_daily", year, month, log_file):
                stocks_success += 1
            else:
                stocks_fail += 1

    # 2024-current: Up to current month
    for year in range(2024, current_year + 1):
        end_month = 13 if year < current_year else current_month + 1
        for month in range(1, end_month):
            if download_month("stocks_daily", year, month, log_file):
                stocks_success += 1
            else:
                stocks_fail += 1

    print()
    print(f"Stocks Daily Summary: ✅ {stocks_success} months, ❌ {stocks_fail} failed")
    print()
    log_file.write(f"\nStocks Daily Summary: ✅ {stocks_success} months, ❌ {stocks_fail} failed\n\n")

    # Download options_daily: 2023-10-24 to present
    print("📈 Downloading options_daily (2023-10 to present)...")
    print(f"   Estimated: ~25 months, ~5GB total")
    print()

    log_file.write("📈 Downloading options_daily (2023-10 to present)...\n")
    log_file.write(f"   Estimated: ~25 months, ~5GB total\n\n")

    options_success = 0
    options_fail = 0

    # 2023: October onwards
    for month in range(10, 13):
        if download_month("options_daily", 2023, month, log_file):
            options_success += 1
        else:
            options_fail += 1

    # 2024-current: Up to current month
    for year in range(2024, current_year + 1):
        end_month = 13 if year < current_year else current_month + 1
        for month in range(1, end_month):
            if download_month("options_daily", year, month, log_file):
                options_success += 1
            else:
                options_fail += 1

    print()
    print(f"Options Daily Summary: ✅ {options_success} months, ❌ {options_fail} failed")
    print()
    log_file.write(f"\nOptions Daily Summary: ✅ {options_success} months, ❌ {options_fail} failed\n\n")

    # Overall summary
    print("="*80)
    print("PHASE 2 DOWNLOAD SUMMARY")
    print("="*80)
    print(f"Stocks Daily:  ✅ {stocks_success}/{stocks_success+stocks_fail} months")
    print(f"Options Daily: ✅ {options_success}/{options_success+options_fail} months")
    print(f"Total: ✅ {stocks_success+options_success} months downloaded")
    print("="*80)

    log_file.write("="*80 + "\n")
    log_file.write("PHASE 2 DOWNLOAD SUMMARY\n")
    log_file.write("="*80 + "\n")
    log_file.write(f"Stocks Daily:  ✅ {stocks_success}/{stocks_success+stocks_fail} months\n")
    log_file.write(f"Options Daily: ✅ {options_success}/{options_success+options_fail} months\n")
    log_file.write(f"Total: ✅ {stocks_success+options_success} months downloaded\n")
    log_file.write("="*80 + "\n")

    log_file.close()

    return 0 if (stocks_fail == 0 and options_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
