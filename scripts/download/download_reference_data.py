#!/usr/bin/env python3
"""
Download Reference Data to Bronze Layer

Downloads ticker metadata, related tickers, and ticker details
directly to bronze layer (REST API → Bronze).

Usage:
    # Download all ticker types and details for specific tickers
    python scripts/download/download_reference_data.py \
        --tickers AAPL,MSFT,GOOGL,AMZN,TSLA

    # Download related tickers for a list of tickers
    python scripts/download/download_reference_data.py \
        --tickers-file tickers.txt \
        --related-tickers-only
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
from src.download.reference_data import ReferenceDataDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description='Download reference data to bronze layer',
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

    parser.add_argument(
        '--ticker-types',
        action='store_true',
        help='Download ticker types'
    )

    parser.add_argument(
        '--ticker-details',
        action='store_true',
        help='Download ticker details'
    )

    parser.add_argument(
        '--related-tickers',
        action='store_true',
        help='Download related tickers'
    )

    parser.add_argument(
        '--related-tickers-only',
        action='store_true',
        help='Download ONLY related tickers (skip types and details)'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download all reference data types'
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
    bronze_path = config.get_bronze_path() / 'reference_data'
    bronze_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reference data output: {bronze_path}")

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

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = ReferenceDataDownloader(
            client=client,
            output_dir=bronze_path,
            use_partitioned_structure=True
        )

        logger.info("✅ ReferenceDataDownloader initialized")

        # Download ticker types if requested
        if args.ticker_types or args.all:
            logger.info("\n📥 Downloading ticker types...")
            try:
                ticker_types_df = await downloader.download_ticker_types()
                logger.info(f"✅ Downloaded {len(ticker_types_df)} ticker types")
            except Exception as e:
                logger.error(f"❌ Failed to download ticker types: {e}")

        # Download ticker details if requested
        if (args.ticker_details or args.all) and tickers and not args.related_tickers_only:
            logger.info(f"\n📥 Downloading ticker details for {len(tickers)} tickers...")
            try:
                details_df = await downloader.download_ticker_details_batch(tickers)
                logger.info(f"✅ Downloaded details for {len(details_df)} tickers")
            except Exception as e:
                logger.error(f"❌ Failed to download ticker details: {e}")

        # Download related tickers if requested
        if args.related_tickers or args.all or args.related_tickers_only:
            if not tickers:
                logger.warning("⚠️ No tickers specified for related tickers download")
            else:
                logger.info(f"\n📥 Downloading related tickers for {len(tickers)} tickers...")
                try:
                    related_df = await downloader.download_related_tickers_batch(tickers)
                    logger.info(f"✅ Downloaded {len(related_df)} related ticker relationships")
                except Exception as e:
                    logger.error(f"❌ Failed to download related tickers: {e}")

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Total requests: {stats['total_requests']}")
        logger.info(f"   Total retries: {stats['total_retries']}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")

        logger.info(f"\n✅ Reference data saved to: {bronze_path}")


if __name__ == '__main__':
    asyncio.run(main())
