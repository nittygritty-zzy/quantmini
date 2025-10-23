#!/usr/bin/env python3
"""
Batch Download Short Interest & Short Volume Data

Downloads short data for all common stocks with intelligent date-range parallelization.

Strategy:
- Short interest: Downloaded by quarter (settlement dates are typically bi-weekly)
- Short volume: Downloaded by month
- Parallel workers process different date ranges concurrently
- Data automatically partitioned by ticker/date for efficient storage

Features:
- Parallel date-range processing
- Incremental downloads (skip already processed date ranges)
- Progress tracking with checkpointing
- Automatic retry with exponential backoff
- Real-time progress monitoring

Usage:
    # Download all historical data (recommended - first run)
    python scripts/download/batch_load_short_data.py --start-date 2020-01-01

    # Download recent data (daily updates)
    python scripts/download/batch_load_short_data.py --days-back 30

    # Download specific date range
    python scripts/download/batch_load_short_data.py --start-date 2024-01-01 --end-date 2024-12-31

    # Customize workers
    python scripts/download/batch_load_short_data.py --workers 4 --days-back 90

    # Force re-download all
    python scripts/download/batch_load_short_data.py --force --start-date 2020-01-01
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.download import FundamentalsDownloader, PolygonRESTClient
from src.core.config_loader import ConfigLoader
from src.utils.paths import get_quantlake_root

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ProgressTracker:
    """Track and persist download progress by date ranges"""

    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.data = self._load()

    def _load(self) -> Dict:
        """Load progress from file"""
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                return json.load(f)
        return {
            'short_interest_completed': [],
            'short_volume_completed': [],
            'failed_ranges': {},
            'started_at': None,
            'last_updated': None,
            'total_interest_records': 0,
            'total_volume_records': 0
        }

    def save(self):
        """Save progress to file"""
        self.data['last_updated'] = datetime.now().isoformat()
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)

    def mark_interest_completed(self, date_range: str, record_count: int):
        """Mark a short interest date range as completed"""
        if date_range not in self.data['short_interest_completed']:
            self.data['short_interest_completed'].append(date_range)
        self.data['total_interest_records'] += record_count
        self.save()

    def mark_volume_completed(self, date_range: str, record_count: int):
        """Mark a short volume date range as completed"""
        if date_range not in self.data['short_volume_completed']:
            self.data['short_volume_completed'].append(date_range)
        self.data['total_volume_records'] += record_count
        self.save()

    def mark_failed(self, date_range: str, error: str, data_type: str):
        """Mark a date range as failed"""
        key = f"{data_type}:{date_range}"
        self.data['failed_ranges'][key] = {
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
        self.save()

    def is_interest_completed(self, date_range: str) -> bool:
        """Check if short interest date range is completed"""
        return date_range in self.data['short_interest_completed']

    def is_volume_completed(self, date_range: str) -> bool:
        """Check if short volume date range is completed"""
        return date_range in self.data['short_volume_completed']


def generate_date_ranges(start_date: str, end_date: str, chunk_months: int = 3) -> List[Tuple[str, str]]:
    """
    Generate list of date ranges for parallel processing

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        chunk_months: Number of months per chunk

    Returns:
        List of (start_date, end_date) tuples
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    ranges = []
    current = start

    while current < end:
        # Calculate chunk end (3 months later or end_date, whichever is earlier)
        chunk_end = min(
            current + timedelta(days=chunk_months * 30),
            end
        )

        ranges.append((
            current.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        ))

        current = chunk_end + timedelta(days=1)

    return ranges


async def download_short_interest_range(
    start_date: str,
    end_date: str,
    output_dir: Path,
    api_key: str,
    progress: ProgressTracker
) -> Dict[str, int]:
    """
    Download short interest data for a date range

    Returns:
        Dictionary with 'records' count
    """
    range_key = f"{start_date}_to_{end_date}"

    try:
        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:

            downloader = FundamentalsDownloader(
                client=client,
                output_dir=output_dir,
                use_partitioned_structure=True
            )

            logger.info(f"📥 Downloading short interest: {start_date} to {end_date}")

            df = await downloader.download_short_interest(
                ticker=None,  # All tickers
                settlement_date_gte=start_date,
                settlement_date_lte=end_date,
                limit=100
            )

            if df is None or len(df) == 0:
                logger.warning(f"No short interest data for {range_key}")
                progress.mark_interest_completed(range_key, 0)
                return {'records': 0}

            record_count = len(df)
            logger.info(f"✅ Short interest {range_key}: {record_count:,} records")

            progress.mark_interest_completed(range_key, record_count)

            return {'records': record_count}

    except Exception as e:
        logger.error(f"❌ Failed short interest {range_key}: {e}")
        progress.mark_failed(range_key, str(e), 'short_interest')
        raise


async def download_short_volume_range(
    start_date: str,
    end_date: str,
    output_dir: Path,
    api_key: str,
    progress: ProgressTracker
) -> Dict[str, int]:
    """
    Download short volume data for a date range

    Returns:
        Dictionary with 'records' count
    """
    range_key = f"{start_date}_to_{end_date}"

    try:
        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:

            downloader = FundamentalsDownloader(
                client=client,
                output_dir=output_dir,
                use_partitioned_structure=True
            )

            logger.info(f"📥 Downloading short volume: {start_date} to {end_date}")

            df = await downloader.download_short_volume(
                ticker=None,  # All tickers
                date_gte=start_date,
                date_lte=end_date,
                limit=100
            )

            if df is None or len(df) == 0:
                logger.warning(f"No short volume data for {range_key}")
                progress.mark_volume_completed(range_key, 0)
                return {'records': 0}

            record_count = len(df)
            logger.info(f"✅ Short volume {range_key}: {record_count:,} records")

            progress.mark_volume_completed(range_key, record_count)

            return {'records': record_count}

    except Exception as e:
        logger.error(f"❌ Failed short volume {range_key}: {e}")
        progress.mark_failed(range_key, str(e), 'short_volume')
        raise


def process_range_wrapper(args):
    """Wrapper for multiprocessing"""
    range_info, output_dir, api_key, progress_file, data_type = args
    start_date, end_date = range_info

    # Recreate progress tracker in subprocess
    progress = ProgressTracker(progress_file)

    try:
        if data_type == 'short_interest':
            result = asyncio.run(download_short_interest_range(
                start_date, end_date, output_dir, api_key, progress
            ))
        else:  # short_volume
            result = asyncio.run(download_short_volume_range(
                start_date, end_date, output_dir, api_key, progress
            ))

        return {
            'success': True,
            'range': f"{start_date}_to_{end_date}",
            'data_type': data_type,
            'records': result['records']
        }

    except Exception as e:
        return {
            'success': False,
            'range': f"{start_date}_to_{end_date}",
            'data_type': data_type,
            'error': str(e)
        }


def main():
    parser = argparse.ArgumentParser(
        description='Batch download short interest and short volume data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD). Default: 2020-01-01'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD). Default: today'
    )

    parser.add_argument(
        '--days-back',
        type=int,
        help='Download last N days (alternative to start-date/end-date)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )

    parser.add_argument(
        '--chunk-months',
        type=int,
        default=3,
        help='Months per date chunk (default: 3)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download all date ranges'
    )

    parser.add_argument(
        '--interest-only',
        action='store_true',
        help='Download only short interest data'
    )

    parser.add_argument(
        '--volume-only',
        action='store_true',
        help='Download only short volume data'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get credentials
    credentials = config.get_credentials('polygon')
    if not credentials or 'api' not in credentials or 'key' not in credentials['api']:
        logger.error("Polygon API key not found in config/credentials.yaml")
        logger.error("Expected structure: polygon.api.key")
        sys.exit(1)

    api_key = credentials['api']['key']

    # Get output directory
    quantlake_root = get_quantlake_root()
    output_dir = quantlake_root / 'fundamentals'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Progress tracking
    progress_file = Path('data/short_data_progress.json')
    progress = ProgressTracker(progress_file)

    if progress.data['started_at'] is None:
        progress.data['started_at'] = datetime.now().isoformat()
        progress.save()

    # Determine date range
    if args.days_back:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.days_back)).strftime('%Y-%m-%d')
    else:
        start_date = args.start_date or '2020-01-01'
        end_date = args.end_date or datetime.now().strftime('%Y-%m-%d')

    logger.info("=" * 80)
    logger.info("📊 SHORT INTEREST & VOLUME DATA DOWNLOAD")
    logger.info("=" * 80)
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Chunk size: {args.chunk_months} months")
    logger.info(f"Progress file: {progress_file}")
    logger.info("=" * 80)

    # Generate date ranges
    date_ranges = generate_date_ranges(start_date, end_date, args.chunk_months)
    logger.info(f"\n📅 Generated {len(date_ranges)} date range chunks")

    # Determine which data types to download
    download_interest = not args.volume_only
    download_volume = not args.interest_only

    # Filter out already completed ranges (unless --force)
    if not args.force:
        if download_interest:
            pending_interest = [r for r in date_ranges if not progress.is_interest_completed(f"{r[0]}_to_{r[1]}")]
            logger.info(f"Short Interest: {len(pending_interest)} pending, {len(date_ranges) - len(pending_interest)} completed")
        else:
            pending_interest = []

        if download_volume:
            pending_volume = [r for r in date_ranges if not progress.is_volume_completed(f"{r[0]}_to_{r[1]}")]
            logger.info(f"Short Volume: {len(pending_volume)} pending, {len(date_ranges) - len(pending_volume)} completed")
        else:
            pending_volume = []
    else:
        pending_interest = date_ranges if download_interest else []
        pending_volume = date_ranges if download_volume else []
        logger.info(f"--force enabled: Re-downloading all ranges")

    if not pending_interest and not pending_volume:
        logger.info("\n✅ All date ranges already downloaded!")
        logger.info(f"Total short interest records: {progress.data['total_interest_records']:,}")
        logger.info(f"Total short volume records: {progress.data['total_volume_records']:,}")
        sys.exit(0)

    # Prepare work items
    work_items = []

    if download_interest:
        for range_info in pending_interest:
            work_items.append((range_info, output_dir, api_key, progress_file, 'short_interest'))

    if download_volume:
        for range_info in pending_volume:
            work_items.append((range_info, output_dir, api_key, progress_file, 'short_volume'))

    logger.info(f"\n🚀 Starting {len(work_items)} download tasks with {args.workers} workers...")

    # Process in parallel
    start_time = time.time()
    completed = 0
    failed = 0
    total_interest_records = progress.data['total_interest_records']
    total_volume_records = progress.data['total_volume_records']

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_range_wrapper, item) for item in work_items]

        for future in as_completed(futures):
            result = future.result()
            completed += 1

            if result['success']:
                if result['data_type'] == 'short_interest':
                    total_interest_records += result['records']
                else:
                    total_volume_records += result['records']

                elapsed = time.time() - start_time
                rate = completed / (elapsed / 60) if elapsed > 0 else 0
                remaining = len(work_items) - completed
                eta_minutes = remaining / rate if rate > 0 else 0

                logger.info(f"Progress: {completed}/{len(work_items)} ({completed/len(work_items)*100:.1f}%) | "
                           f"Rate: {rate:.1f} ranges/min | ETA: {eta_minutes:.0f}m")
            else:
                failed += 1
                logger.error(f"Failed: {result['range']} ({result['data_type']}): {result['error']}")

    # Final summary
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 80)
    logger.info("✅ DOWNLOAD COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total duration: {elapsed/60:.1f} minutes")
    logger.info(f"Completed: {completed - failed}/{len(work_items)} date ranges")
    logger.info(f"Failed: {failed}/{len(work_items)} date ranges")
    logger.info(f"\n📊 Data Summary:")
    logger.info(f"   Short Interest: {total_interest_records:,} total records")
    logger.info(f"   Short Volume: {total_volume_records:,} total records")
    logger.info(f"\n💾 Data saved to: {output_dir}")
    logger.info(f"   {output_dir}/short_interest/")
    logger.info(f"   {output_dir}/short_volume/")

    if failed > 0:
        logger.warning(f"\n⚠️  {failed} date ranges failed. Check progress file for details:")
        logger.warning(f"   {progress_file}")
        logger.warning(f"   Re-run the script to retry failed ranges")


if __name__ == '__main__':
    main()
