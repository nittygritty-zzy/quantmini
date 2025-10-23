#!/usr/bin/env python3
"""
Download Options Daily and Minute Data

Downloads options_daily (2023-10 onwards) and options_minute (2020-10 onwards)
to the landing layer with proper partitioning.

Uses the fixed CLI which now saves to:
  landing/{data_type}/year={YYYY}/month={MM}/{YYYY-MM-DD}.csv.gz
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, timedelta, datetime as dt


def download_date_range(data_type: str, start_date: str, end_date: str, log_file):
    """Download a date range using the fixed CLI"""
    print(f"[{dt.now().strftime('%H:%M:%S')}] Downloading {data_type}: {start_date} to {end_date}", flush=True)
    log_file.write(f"[{dt.now().strftime('%H:%M:%S')}] Downloading {data_type}: {start_date} to {end_date}\n")
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
    from src.utils.paths import get_quantlake_root

    quantlake_root = get_quantlake_root()
    log_path = quantlake_root / "logs" / f"options_download_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_path, 'w', buffering=1)

    print("=" * 80)
    print("OPTIONS DATA DOWNLOAD - DAILY & MINUTE")
    print("=" * 80)
    print("⚠️  This will download:")
    print("   - options_daily: ~2 years of data (2023-10 to present)")
    print("   - options_minute: ~5 years of data (2020-10 to present)")
    print("   - Estimated size: ~500-600GB total")
    print("   - Estimated time: 12-20 hours")
    print()

    log_file.write("=" * 80 + "\n")
    log_file.write("OPTIONS DATA DOWNLOAD - DAILY & MINUTE\n")
    log_file.write("=" * 80 + "\n\n")

    today = date.today()

    # ========================================================================
    # OPTIONS DAILY (2023-10-01 to present)
    # ========================================================================
    print("📈 Downloading options_daily (2023-10-01 to present)...")
    print("   Processing in 3-month chunks")
    print()

    log_file.write("📈 Downloading options_daily (2023-10-01 to present)...\n")
    log_file.write("   Processing in 3-month chunks\n\n")

    options_daily_success = 0
    options_daily_fail = 0

    current_start = date(2023, 10, 1)

    while current_start < today:
        # Process in 3-month chunks (90 days)
        current_end = min(current_start + timedelta(days=90), today)

        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')

        if download_date_range("options_daily", start_str, end_str, log_file):
            options_daily_success += 1
        else:
            options_daily_fail += 1

        current_start = current_end + timedelta(days=1)

    print()
    print(f"Options Daily Summary: ✅ {options_daily_success} chunks, ❌ {options_daily_fail} failed")
    print()
    log_file.write(f"\nOptions Daily Summary: ✅ {options_daily_success} chunks, ❌ {options_daily_fail} failed\n\n")

    # ========================================================================
    # OPTIONS MINUTE (2020-10-17 to present)
    # ========================================================================
    print("📈 Downloading options_minute (2020-10-17 to present)...")
    print("   ⚠️  This is VERY LARGE (~500GB)")
    print("   Processing in 3-month chunks")
    print()

    log_file.write("📈 Downloading options_minute (2020-10-17 to present)...\n")
    log_file.write("   Processing in 3-month chunks\n\n")

    options_minute_success = 0
    options_minute_fail = 0

    current_start = date(2020, 10, 17)

    while current_start < today:
        # Process in 3-month chunks (90 days)
        current_end = min(current_start + timedelta(days=90), today)

        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')

        if download_date_range("options_minute", start_str, end_str, log_file):
            options_minute_success += 1
        else:
            options_minute_fail += 1

        current_start = current_end + timedelta(days=1)

    print()
    print(f"Options Minute Summary: ✅ {options_minute_success} chunks, ❌ {options_minute_fail} failed")
    print()
    log_file.write(f"\nOptions Minute Summary: ✅ {options_minute_success} chunks, ❌ {options_minute_fail} failed\n\n")

    # Overall summary
    print("=" * 80)
    print("OPTIONS DOWNLOAD SUMMARY")
    print("=" * 80)
    print(f"Options Daily:  ✅ {options_daily_success}/{options_daily_success+options_daily_fail} chunks")
    print(f"Options Minute: ✅ {options_minute_success}/{options_minute_success+options_minute_fail} chunks")
    print(f"Total: ✅ {options_daily_success+options_minute_success} chunks downloaded")
    print("=" * 80)

    log_file.write("=" * 80 + "\n")
    log_file.write("OPTIONS DOWNLOAD SUMMARY\n")
    log_file.write("=" * 80 + "\n")
    log_file.write(f"Options Daily:  ✅ {options_daily_success}/{options_daily_success+options_daily_fail} chunks\n")
    log_file.write(f"Options Minute: ✅ {options_minute_success}/{options_minute_success+options_minute_fail} chunks\n")
    log_file.write(f"Total: ✅ {options_daily_success+options_minute_success} chunks downloaded\n")
    log_file.write("=" * 80 + "\n")

    log_file.close()

    return 0 if (options_daily_fail == 0 and options_minute_fail == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
