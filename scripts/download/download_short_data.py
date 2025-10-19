#!/usr/bin/env python3
"""
Download Short Interest and Short Volume Data

Downloads short interest and short volume data from Polygon's fundamentals endpoint.

Usage:
    # Download short data for all CS tickers
    python scripts/download/download_short_data.py --tickers-file tickers_cs.txt

    # Download for specific tickers
    python scripts/download/download_short_data.py --tickers AAPL,MSFT,GOOGL
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
from src.download.fundamentals import FundamentalsDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description='Download short interest and short volume data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--tickers',
        type=str,
        help='Comma-separated list of tickers'
    )

    parser.add_argument(
        '--tickers-file',
        type=Path,
        help='File with tickers (one per line)'
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
    bronze_path = config.get_bronze_path() / 'fundamentals'
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Fundamentals output: {bronze_path}")

    # Get tickers list
    tickers = []
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(',')]
    elif args.tickers_file:
        if args.tickers_file.exists():
            with open(args.tickers_file) as f:
                tickers = [line.strip() for line in f if line.strip()]
        else:
            logger.error(f"Tickers file not found: {args.tickers_file}")
            sys.exit(1)

    if not tickers:
        logger.error("No tickers specified. Use --tickers or --tickers-file")
        sys.exit(1)

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = FundamentalsDownloader(
            client=client,
            output_dir=bronze_path,
            use_partitioned_structure=True
        )

        logger.info("✅ FundamentalsDownloader initialized")
        logger.info(f"\n{'='*70}")
        logger.info(f"📥 Downloading short data for {len(tickers):,} tickers")
        logger.info(f"{'='*70}\n")

        try:
            # Download short data for all tickers in batch
            short_data = await downloader.download_short_data_batch(tickers)

            logger.info(f"\n{'='*70}")
            logger.info(f"✅ Short Data Download Complete")
            logger.info(f"{'='*70}")
            logger.info(f"Short interest records: {len(short_data['short_interest']):,}")
            logger.info(f"Short volume records: {len(short_data['short_volume']):,}")

        except Exception as e:
            logger.error(f"❌ Failed to download short data: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Total requests: {stats['total_requests']:,}")
        logger.info(f"   Total retries: {stats['total_retries']:,}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")

        logger.info(f"\n✅ Short data saved to: {bronze_path}")


if __name__ == '__main__':
    asyncio.run(main())
