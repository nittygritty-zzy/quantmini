#!/usr/bin/env python3
"""
Phase 3: News Data Download

Downloads news articles for all active stock tickers (10 years of data).
Uses parallel processing with rate limiting to avoid API throttling.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import subprocess
from datetime import date, timedelta, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def get_active_tickers():
    """Get list of active stock tickers"""
    try:
        result = subprocess.run(
            ["python", "scripts/utils/get_active_tickers.py"],
            capture_output=True,
            text=True,
            check=True
        )
        tickers = result.stdout.strip().split()
        return tickers
    except subprocess.CalledProcessError as e:
        print(f"Error getting active tickers: {e}")
        return []


def download_news_for_ticker(ticker: str, start_date: str, end_date: str, log_file) -> tuple:
    """Download news for a single ticker"""
    timestamp = dt.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] Downloading news for {ticker}", flush=True)
    log_file.write(f"[{timestamp}] Downloading news for {ticker}\n")
    log_file.flush()

    cmd = [
        "quantmini", "polygon", "news", ticker,
        "--start-date", start_date,
        "--end-date", end_date
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
        if result.returncode == 0:
            # Parse output to count articles
            articles_count = result.stdout.count("✓") if "✓" in result.stdout else 0
            print(f"  ✅ {ticker}: {articles_count} articles", flush=True)
            log_file.write(f"  ✅ Success: {articles_count} articles\n")
            log_file.flush()
            return (ticker, True, articles_count)
        else:
            error_msg = result.stderr[:100] if result.stderr else "Unknown error"
            print(f"  ❌ {ticker}: {error_msg}", flush=True)
            log_file.write(f"  ❌ Failed: {error_msg}\n")
            log_file.flush()
            return (ticker, False, 0)
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  {ticker}: Timeout", flush=True)
        log_file.write(f"  ⏱️  Timeout\n")
        log_file.flush()
        return (ticker, False, 0)
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"  ❌ {ticker}: {error_msg}", flush=True)
        log_file.write(f"  ❌ Error: {error_msg}\n")
        log_file.flush()
        return (ticker, False, 0)


def main():
    from src.utils.paths import get_quantlake_root

    quantlake_root = get_quantlake_root()
    log_path = quantlake_root / "logs" / f"news_download_{dt.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file = open(log_path, 'w', buffering=1)

    print("=" * 80)
    print("PHASE 3: NEWS DATA DOWNLOAD")
    print("=" * 80)
    print()

    log_file.write("=" * 80 + "\n")
    log_file.write("PHASE 3: NEWS DATA DOWNLOAD\n")
    log_file.write("=" * 80 + "\n\n")

    # Get active tickers
    print("Loading active stock tickers...")
    tickers = get_active_tickers()

    if not tickers:
        print("ERROR: No active tickers found")
        log_file.write("ERROR: No active tickers found\n")
        log_file.close()
        return 1

    print(f"Found {len(tickers)} active tickers")
    print()

    log_file.write(f"Found {len(tickers)} active tickers\n\n")

    # Calculate 10 years ago and today
    ten_years_ago = (date.today() - timedelta(days=10*365)).strftime('%Y-%m-%d')
    today = date.today().strftime('%Y-%m-%d')
    print(f"Downloading news from {ten_years_ago} to {today}")
    print(f"Using 8 parallel workers with rate limiting")
    print()

    log_file.write(f"Date range: {ten_years_ago} to {today}\n")
    log_file.write(f"Parallel workers: 8\n\n")

    # Download news in parallel (8 workers as per script spec)
    success_count = 0
    fail_count = 0
    total_articles = 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all tasks
        futures = {
            executor.submit(download_news_for_ticker, ticker, ten_years_ago, today, log_file): ticker
            for ticker in tickers
        }

        # Process completed tasks
        for i, future in enumerate(as_completed(futures), 1):
            ticker, success, article_count = future.result()

            if success:
                success_count += 1
                total_articles += article_count
            else:
                fail_count += 1

            # Progress update every 100 tickers
            if i % 100 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                remaining = len(tickers) - i
                eta = remaining / rate if rate > 0 else 0

                print(f"\n📊 Progress: {i}/{len(tickers)} tickers ({i/len(tickers)*100:.1f}%)")
                print(f"   Success: {success_count} | Failed: {fail_count}")
                print(f"   Articles downloaded: {total_articles:,}")
                print(f"   ETA: {eta/60:.1f} minutes\n")

            # Small delay to avoid overwhelming the API
            time.sleep(0.1)

    # Final summary
    elapsed = time.time() - start_time

    print()
    print("=" * 80)
    print("PHASE 3 SUMMARY")
    print("=" * 80)
    print(f"Total tickers processed: {len(tickers)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Success rate: {success_count/len(tickers)*100:.1f}%")
    print(f"Total articles downloaded: {total_articles:,}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Average rate: {len(tickers)/elapsed*60:.1f} tickers/min")
    print("=" * 80)

    log_file.write("\n" + "=" * 80 + "\n")
    log_file.write("PHASE 3 SUMMARY\n")
    log_file.write("=" * 80 + "\n")
    log_file.write(f"Total tickers: {len(tickers)}\n")
    log_file.write(f"Successful: {success_count}\n")
    log_file.write(f"Failed: {fail_count}\n")
    log_file.write(f"Success rate: {success_count/len(tickers)*100:.1f}%\n")
    log_file.write(f"Total articles: {total_articles:,}\n")
    log_file.write(f"Time elapsed: {elapsed/60:.1f} minutes\n")
    log_file.write("=" * 80 + "\n")

    log_file.close()

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
