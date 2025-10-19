#!/usr/bin/env python3
"""
Download News for All CS Tickers - 1 Year Backfill

Downloads news articles for all CS tickers from the past year
and stores them in the bronze layer with date-based partitioning.

Usage:
    # Download 1 year of news for all CS tickers
    python scripts/download/download_news_1year.py

    # Custom batch size
    python scripts/download/download_news_1year.py --batch-size 100

    # Custom date range
    python scripts/download/download_news_1year.py --start-date 2024-01-01 --end-date 2024-12-31
"""

import asyncio
import sys
import logging
import argparse
from pathlib import Path
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader
from src.download.polygon_rest_client import PolygonRESTClient
from src.download.news import NewsDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description='Download 1 year of news for CS tickers',
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
        '--batch-size',
        type=int,
        default=50,
        help='Number of tickers to process per batch (default: 50)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD), default: 1 year ago'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD), default: today'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Max articles per ticker (default: 1000)'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get credentials
    credentials = config.get_credentials('polygon')
    if not credentials or 'api_key' not in credentials:
        logger.error("Polygon API key not found in config/credentials.yaml")
        sys.exit(1)

    # Get bronze path for news
    bronze_path = config.get_bronze_path() / 'news'
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"News output: {bronze_path}")

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

    # Calculate date range
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        end_date = date.today()

    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = end_date - timedelta(days=365)

    logger.info(f"Date range: {start_date} to {end_date}")

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = NewsDownloader(
            client=client,
            output_dir=bronze_path,
            use_partitioned_structure=True
        )

        logger.info("✅ NewsDownloader initialized")
        logger.info(f"\n{'='*70}")
        logger.info(f"📰 Downloading news articles")
        logger.info(f"{'='*70}")
        logger.info(f"Tickers: {len(tickers):,}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Batch size: {args.batch_size}")
        logger.info(f"Max articles/ticker: {args.limit}")
        logger.info(f"{'='*70}\n")

        try:
            # Process in batches
            total_articles = 0
            failed_tickers = []

            for i in range(0, len(tickers), args.batch_size):
                batch = tickers[i:i+args.batch_size]
                batch_num = (i // args.batch_size) + 1
                total_batches = (len(tickers) + args.batch_size - 1) // args.batch_size

                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")

                try:
                    result = await downloader.download_news_batch(
                        tickers=batch,
                        published_utc_gte=start_date.strftime('%Y-%m-%d'),
                        published_utc_lte=end_date.strftime('%Y-%m-%d'),
                        limit=args.limit
                    )

                    batch_articles = result['total_articles']
                    total_articles += batch_articles

                    logger.info(
                        f"  Batch {batch_num} complete: "
                        f"{batch_articles} articles (Total: {total_articles:,})"
                    )

                except Exception as e:
                    logger.error(f"Failed to download batch {batch_num}: {e}")
                    failed_tickers.extend(batch)

        except Exception as e:
            logger.error(f"❌ Failed to download news: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 NEWS DOWNLOAD COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"Tickers processed: {len(tickers):,}")
        logger.info(f"Total articles: {total_articles:,}")
        logger.info(f"Failed tickers: {len(failed_tickers)}")
        logger.info(f"API requests: {stats['total_requests']:,}")
        logger.info(f"API retries: {stats['total_retries']:,}")
        logger.info(f"Success rate: {stats['success_rate']:.1%}")
        logger.info(f"\n✅ News saved to: {bronze_path}")

        if failed_tickers:
            logger.warning(f"\n⚠️  Failed tickers ({len(failed_tickers)}):")
            for ticker in failed_tickers[:20]:
                logger.warning(f"   {ticker}")
            if len(failed_tickers) > 20:
                logger.warning(f"   ... and {len(failed_tickers) - 20} more")


if __name__ == '__main__':
    asyncio.run(main())
