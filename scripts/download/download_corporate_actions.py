#!/usr/bin/env python3
"""
Download Corporate Actions to Bronze Layer

Downloads dividends, splits, and IPOs directly to bronze layer
(REST API → Bronze).

Usage:
    # Download all corporate actions for specific tickers
    python scripts/download/download_corporate_actions.py \
        --tickers AAPL,MSFT,GOOGL \
        --start-date 2020-01-01 \
        --end-date 2025-12-31

    # Download from tickers file
    python scripts/download/download_corporate_actions.py \
        --tickers-file tickers.txt \
        --start-date 2024-01-01

    # Download specific types
    python scripts/download/download_corporate_actions.py \
        --tickers AAPL \
        --dividends-only
"""

import argparse
import asyncio
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader
from src.download.polygon_rest_client import PolygonRESTClient
from src.download.corporate_actions import CorporateActionsDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description='Download corporate actions data to bronze layer',
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
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD, defaults to today)'
    )

    parser.add_argument(
        '--dividends',
        action='store_true',
        help='Download dividends'
    )

    parser.add_argument(
        '--dividends-only',
        action='store_true',
        help='Download ONLY dividends'
    )

    parser.add_argument(
        '--splits',
        action='store_true',
        help='Download stock splits'
    )

    parser.add_argument(
        '--splits-only',
        action='store_true',
        help='Download ONLY stock splits'
    )

    parser.add_argument(
        '--ipos',
        action='store_true',
        help='Download IPOs'
    )

    parser.add_argument(
        '--ticker-events',
        action='store_true',
        help='Download ticker events (requires tickers)'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download all corporate action types'
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

    # Set end date to today if not specified
    end_date = args.end_date or datetime.now().strftime('%Y-%m-%d')

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = CorporateActionsDownloader(
            client=client,
            output_dir=bronze_path,
            use_partitioned_structure=True
        )

        logger.info("✅ CorporateActionsDownloader initialized")

        # Determine what to download
        download_all = args.all or (
            not args.dividends and
            not args.dividends_only and
            not args.splits and
            not args.splits_only and
            not args.ipos and
            not args.ticker_events
        )

        # Download for each ticker
        if tickers:
            for ticker in tickers:
                logger.info(f"\n📥 Downloading corporate actions for {ticker}...")

                try:
                    if download_all:
                        # Download all corporate actions
                        data = await downloader.download_all_corporate_actions(
                            ticker=ticker,
                            start_date=args.start_date,
                            end_date=end_date,
                            include_ipos=args.ipos or args.all
                        )

                        logger.info(f"✅ {ticker} Complete:")
                        logger.info(f"   Dividends: {len(data['dividends']):,}")
                        logger.info(f"   Splits: {len(data['splits']):,}")
                        if args.ipos or args.all:
                            logger.info(f"   IPOs: {len(data['ipos']):,}")

                    else:
                        # Download specific types
                        if args.dividends or args.dividends_only:
                            df = await downloader.download_dividends_with_params(
                                ticker=ticker,
                                **{'ex_dividend_date.gte': args.start_date} if args.start_date else {},
                                **{'ex_dividend_date.lte': end_date} if end_date else {}
                            )
                            logger.info(f"   ✓ Dividends: {len(df):,} records")

                        if (args.splits or args.splits_only) and not args.dividends_only:
                            df = await downloader.download_stock_splits_with_params(
                                ticker=ticker,
                                **{'execution_date.gte': args.start_date} if args.start_date else {},
                                **{'execution_date.lte': end_date} if end_date else {}
                            )
                            logger.info(f"   ✓ Splits: {len(df):,} records")

                    # Ticker events (separate endpoint, requires specific ticker)
                    if args.ticker_events:
                        df = await downloader.download_ticker_events(ticker)
                        logger.info(f"   ✓ Ticker events: {len(df):,} records")

                except Exception as e:
                    logger.error(f"   ✗ Failed: {e}")

        else:
            # No tickers specified - download all corporate actions
            logger.info(f"\n📥 Downloading all corporate actions...")
            logger.info(f"   Date range: {args.start_date or 'all'} to {end_date}")

            try:
                data = await downloader.download_all_corporate_actions(
                    ticker=None,
                    start_date=args.start_date,
                    end_date=end_date,
                    include_ipos=args.ipos or args.all
                )

                logger.info(f"✅ Downloaded all corporate actions:")
                logger.info(f"   Dividends: {len(data['dividends']):,}")
                logger.info(f"   Splits: {len(data['splits']):,}")
                if args.ipos or args.all:
                    logger.info(f"   IPOs: {len(data['ipos']):,}")

            except Exception as e:
                logger.error(f"❌ Failed: {e}")

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Total requests: {stats['total_requests']}")
        logger.info(f"   Total retries: {stats['total_retries']}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")

        logger.info(f"\n✅ Corporate actions data saved to: {bronze_path}")


if __name__ == '__main__':
    asyncio.run(main())
