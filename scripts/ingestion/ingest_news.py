#!/usr/bin/env python3
"""
Backfill news for all active US common stocks for 1 year

This script downloads news articles for all active US common stocks
from the past year and stores them in partitioned structure.
"""

import asyncio
import polars as pl
from pathlib import Path
from datetime import date, timedelta
import logging
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.download import PolygonRESTClient, NewsDownloader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Backfill news for all active US common stocks"""

    # Load active stocks
    logger.info("Loading active US common stocks...")
    df = pl.read_parquet('data/reference/locale=us/type=CS/data.parquet')
    active_stocks = df.filter(pl.col('active') == True)
    tickers = active_stocks.select('ticker').to_series().to_list()

    logger.info(f"Found {len(tickers)} active US common stocks")

    # Calculate date range (1 year back from today)
    end_date = date.today()
    start_date = end_date - timedelta(days=365)

    logger.info(f"Date range: {start_date} to {end_date}")

    # Load credentials
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')

    # Extract API key (supports multiple formats)
    api_key = None
    if credentials:
        if 'api_key' in credentials:
            api_key = credentials['api_key']
        elif 'api' in credentials and isinstance(credentials['api'], dict):
            api_key = credentials['api'].get('key')

    if not api_key:
        logger.error("API key not found. Please configure config/credentials.yaml")
        return 1

    # Create client
    async with PolygonRESTClient(
        api_key=api_key,
        max_concurrent=100,  # High concurrency for unlimited tier
        max_connections=200
    ) as client:

        # Create downloader
        downloader = NewsDownloader(
            client=client,
            output_dir=Path('data/partitioned_screener'),
            use_partitioned_structure=True
        )

        logger.info("Starting news backfill...")

        # Process in batches of 50 tickers to avoid overwhelming the system
        batch_size = 50
        total_articles = 0
        failed_tickers = []

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(tickers) + batch_size - 1) // batch_size

            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")

            try:
                result = await downloader.download_news_batch(
                    tickers=batch,
                    published_utc_gte=start_date.strftime('%Y-%m-%d'),
                    published_utc_lte=end_date.strftime('%Y-%m-%d'),
                    limit=1000  # Max articles per ticker
                )

                batch_articles = result['total_articles']
                total_articles += batch_articles

                logger.info(
                    f"Batch {batch_num}/{total_batches} complete: "
                    f"{batch_articles} articles downloaded "
                    f"(Total so far: {total_articles})"
                )

            except Exception as e:
                logger.error(f"Failed to download batch {batch_num}: {e}")
                failed_tickers.extend(batch)

        # Show statistics
        stats = client.get_statistics()
        logger.info(f"\n{'='*60}")
        logger.info(f"News backfill complete!")
        logger.info(f"{'='*60}")
        logger.info(f"Total tickers processed: {len(tickers)}")
        logger.info(f"Total articles downloaded: {total_articles}")
        logger.info(f"Failed tickers: {len(failed_tickers)}")
        logger.info(f"\nAPI Statistics:")
        logger.info(f"  Total requests: {stats['total_requests']}")
        logger.info(f"  Success rate: {stats['success_rate']:.1%}")
        logger.info(f"  Failed requests: {stats['failed_requests']}")
        logger.info(f"{'='*60}")

        if failed_tickers:
            logger.warning(f"\nFailed tickers ({len(failed_tickers)}):")
            for ticker in failed_tickers[:20]:  # Show first 20
                logger.warning(f"  {ticker}")
            if len(failed_tickers) > 20:
                logger.warning(f"  ... and {len(failed_tickers) - 20} more")

        return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
