#!/usr/bin/env python3
"""
Download Fundamentals Data to Bronze Layer

Downloads financial statements and metrics directly to bronze layer
(REST API → Bronze).

Usage:
    # Download all financials for specific tickers
    python scripts/download/download_fundamentals.py \
        --tickers AAPL,MSFT,GOOGL \
        --timeframe quarterly

    # Download from tickers file
    python scripts/download/download_fundamentals.py \
        --tickers-file tickers.txt \
        --timeframe annual \
        --include-short-data

    # Download specific statement types
    python scripts/download/download_fundamentals.py \
        --tickers AAPL \
        --balance-sheets \
        --income-statements
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
        description='Download fundamentals data to bronze layer',
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
        '--timeframe',
        choices=['annual', 'quarterly', 'ttm'],
        default='quarterly',
        help='Timeframe for financial statements'
    )

    parser.add_argument(
        '--balance-sheets',
        action='store_true',
        help='Download balance sheets only'
    )

    parser.add_argument(
        '--cash-flow',
        action='store_true',
        help='Download cash flow statements only'
    )

    parser.add_argument(
        '--income-statements',
        action='store_true',
        help='Download income statements only'
    )

    parser.add_argument(
        '--include-short-data',
        action='store_true',
        help='Include short interest and short volume'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download all fundamental data types'
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
        logger.info(f"📥 Downloading fundamentals for {len(tickers)} tickers ({args.timeframe})")

        # Determine what to download
        download_all = args.all or (
            not args.balance_sheets and
            not args.cash_flow and
            not args.income_statements
        )

        if download_all:
            # Download all financials in batch
            try:
                logger.info("\n📥 Downloading all financials (batch mode)...")
                counts = await downloader.download_financials_batch(
                    tickers=tickers,
                    timeframe=args.timeframe
                )

                logger.info(f"\n✅ Downloaded all financials:")
                logger.info(f"   Balance sheets: {counts['balance_sheets']:,}")
                logger.info(f"   Cash flow: {counts['cash_flow']:,}")
                logger.info(f"   Income statements: {counts['income_statements']:,}")

            except Exception as e:
                logger.error(f"❌ Failed to download financials: {e}")
        else:
            # Download specific types
            for ticker in tickers:
                logger.info(f"\n📥 Processing {ticker}...")

                try:
                    if args.balance_sheets:
                        df = await downloader.download_balance_sheets(
                            ticker=ticker,
                            timeframe=args.timeframe
                        )
                        logger.info(f"   ✓ Balance sheets: {len(df)} records")

                    if args.cash_flow:
                        df = await downloader.download_cash_flow_statements(
                            ticker=ticker,
                            timeframe=args.timeframe
                        )
                        logger.info(f"   ✓ Cash flow: {len(df)} records")

                    if args.income_statements:
                        df = await downloader.download_income_statements(
                            ticker=ticker,
                            timeframe=args.timeframe
                        )
                        logger.info(f"   ✓ Income statements: {len(df)} records")

                except Exception as e:
                    logger.error(f"   ✗ Failed: {e}")

        # Download short data if requested
        if args.include_short_data:
            logger.info(f"\n📥 Downloading short data for {len(tickers)} tickers...")
            try:
                short_data = await downloader.download_short_data_batch(tickers)
                logger.info(f"✅ Downloaded short data:")
                logger.info(f"   Short interest: {len(short_data['short_interest']):,} records")
                logger.info(f"   Short volume: {len(short_data['short_volume']):,} records")
            except Exception as e:
                logger.error(f"❌ Failed to download short data: {e}")

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Total requests: {stats['total_requests']}")
        logger.info(f"   Total retries: {stats['total_retries']}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")

        logger.info(f"\n✅ Fundamentals data saved to: {bronze_path}")


if __name__ == '__main__':
    asyncio.run(main())
