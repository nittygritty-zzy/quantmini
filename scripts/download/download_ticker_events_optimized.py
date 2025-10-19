#!/usr/bin/env python3
"""
Download Ticker Events (Optimized)

Optimized version for downloading ticker events for all CS tickers.

Improvements over standard approach:
- Uses batch_request() for parallel API calls
- Processes in chunks (default: 1000 tickers/chunk)
- Saves incrementally every N records to avoid data loss
- Better memory management
- Progress tracking

Usage:
    # Download ticker events for all CS tickers
    python scripts/download/download_ticker_events_optimized.py

    # Custom chunk size and save interval
    python scripts/download/download_ticker_events_optimized.py --chunk-size 500 --save-interval 250

Performance:
    - Chunk size: 1000 tickers processed in parallel
    - Save interval: 500 records saved at a time
    - Expected: ~11,464 tickers in ~12-15 chunks
    - Time estimate: ~2-5 minutes total (vs 20+ minutes with old method)
"""

import argparse
import asyncio
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader
from src.download.polygon_rest_client import PolygonRESTClient
from src.download.corporate_actions_optimized import OptimizedTickerEventsDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description='Download ticker events (optimized)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--tickers-file',
        type=Path,
        default=Path('tickers_cs.txt'),
        help='File with tickers (one per line, default: tickers_cs.txt)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Number of tickers to process per chunk (default: 1000)'
    )

    parser.add_argument(
        '--save-interval',
        type=int,
        default=500,
        help='Save to disk every N records (default: 500)'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get credentials
    credentials = config.get_credentials('polygon')
    if not credentials or 'api_key' not in credentials:
        logger.error("Polygon API key not found in config/credentials.yaml")
        sys.exit(1)

    # Get bronze path
    bronze_path = config.get_bronze_path() / 'corporate_actions'
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Corporate actions output: {bronze_path}")

    # Read tickers
    if not args.tickers_file.exists():
        logger.error(f"Tickers file not found: {args.tickers_file}")
        sys.exit(1)

    with open(args.tickers_file) as f:
        tickers = [line.strip() for line in f if line.strip()]

    if not tickers:
        logger.error("No tickers found in file")
        sys.exit(1)

    logger.info(f"Loaded {len(tickers):,} tickers from {args.tickers_file}")

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = OptimizedTickerEventsDownloader(
            client=client,
            output_dir=bronze_path,
            use_partitioned_structure=True
        )

        logger.info("✅ OptimizedTickerEventsDownloader initialized")
        logger.info(f"\n{'='*70}")
        logger.info(f"📥 Downloading ticker events (OPTIMIZED)")
        logger.info(f"{'='*70}")
        logger.info(f"Tickers: {len(tickers):,}")
        logger.info(f"Chunk size: {args.chunk_size}")
        logger.info(f"Save interval: {args.save_interval}")
        logger.info(f"{'='*70}\n")

        try:
            # Download using optimized method
            await downloader.download_ticker_events_optimized(
                tickers=tickers,
                chunk_size=args.chunk_size,
                save_interval=args.save_interval
            )

        except Exception as e:
            logger.error(f"❌ Failed to download ticker events: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 DOWNLOAD COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"API requests: {stats['total_requests']:,}")
        logger.info(f"API retries: {stats['total_retries']:,}")
        logger.info(f"Success rate: {stats['success_rate']:.1%}")
        logger.info(f"\n✅ Ticker events saved to: {bronze_path / 'ticker_events'}")


if __name__ == '__main__':
    asyncio.run(main())
