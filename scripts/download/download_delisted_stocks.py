#!/usr/bin/env python3
"""
Download Delisted Stocks - Fix Survivorship Bias

This script downloads historical data for stocks that delisted during
the backtest period to address survivorship bias.

Usage:
    python scripts/download_delisted_stocks.py --start-date 2024-01-01 --end-date 2025-10-06

Requirements:
    - POLYGON_API_KEY environment variable set
    - polygon-api-client installed
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.download.delisted_stocks import DelistedStocksDownloader
from src.core.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Download delisted stocks for backtest period"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download delisted stocks to fix survivorship bias"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Backtest start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Backtest end date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output-dir",
        default="data/parquet",
        help="Output directory for parquet files (default: data/parquet)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of delisted stocks to check (default: 10000)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Only query delisted stocks, don't download data"
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("DELISTED STOCKS DOWNLOADER")
    logger.info("=" * 80)
    logger.info(f"Start date: {args.start_date}")
    logger.info(f"End date: {args.end_date}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Limit: {args.limit}")
    logger.info("=" * 80)

    # Initialize downloader
    try:
        downloader = DelistedStocksDownloader(output_dir=args.output_dir)
        logger.info("✓ Delisted stocks downloader initialized")
    except Exception as e:
        logger.error(f"Failed to initialize downloader: {e}")
        logger.error("Make sure POLYGON_API_KEY environment variable is set")
        return 1

    # Step 1: Query delisted stocks
    logger.info("\nStep 1: Querying Polygon API for delisted stocks...")
    try:
        delisted_stocks = downloader.get_delisted_stocks(
            args.start_date,
            args.end_date,
            limit=args.limit
        )
        logger.info(f"✓ Found {len(delisted_stocks)} delisted stocks")
    except Exception as e:
        logger.error(f"Failed to query delisted stocks: {e}")
        return 1

    if len(delisted_stocks) == 0:
        logger.warning("No delisted stocks found in the specified period")
        logger.info("This could mean:")
        logger.info("  1. Very few delistings occurred in this period")
        logger.info("  2. The query limit is too low")
        logger.info("  3. The date range is too narrow")
        return 0

    # Step 2: Save delisted list
    logger.info("\nStep 2: Saving delisted stocks list...")
    try:
        list_file = downloader.save_delisted_list(delisted_stocks)
        logger.info(f"✓ Saved to: {list_file}")
    except Exception as e:
        logger.error(f"Failed to save delisted list: {e}")
        return 1

    # Step 3: Download historical data (if not skipped)
    if args.skip_download:
        logger.info("\nSkipping data download (--skip-download flag set)")
        logger.info("You can download data later using the saved list")
    else:
        logger.info("\nStep 3: Downloading historical OHLCV data...")
        logger.info(f"This will download {len(delisted_stocks)} stocks...")
        logger.info(f"Estimated time: ~{len(delisted_stocks) / 4 / 60:.1f} minutes")

        try:
            stats = downloader.download_historical_data(
                delisted_stocks,
                args.start_date
            )
            logger.info(f"✓ Download complete")
            logger.info(f"  Success: {stats['success']}")
            logger.info(f"  No data: {stats['no_data']}")
            logger.info(f"  Errors: {stats['errors']}")
        except Exception as e:
            logger.error(f"Failed to download historical data: {e}")
            return 1

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Delisted stocks found: {len(delisted_stocks)}")

    if not args.skip_download:
        logger.info(f"Successfully downloaded: {stats['success']}/{len(delisted_stocks)}")
        logger.info(f"Success rate: {stats['success']/len(delisted_stocks)*100:.1f}%")

    logger.info("\nNext steps:")
    logger.info("  1. Convert parquet files to qlib binary format:")
    logger.info("     python scripts/convert_to_qlib.py")
    logger.info("  2. Re-run backtests with updated universe")
    logger.info("  3. Compare results (expect -15-30% drop in returns)")
    logger.info("=" * 80)

    return 0


if __name__ == '__main__':
    sys.exit(main())
