#!/usr/bin/env python3
"""
Download All Tickers for All Types

Downloads all tickers from Polygon's all-tickers endpoint with proper partitioning
by locale and type. This populates the reference_data/tickers/ directory with
complete ticker metadata for all ticker types.

Usage:
    # Download all tickers (all types, all locales)
    python scripts/download/download_all_tickers.py --all

    # Download specific types
    python scripts/download/download_all_tickers.py --types ADRW,BOND,BASKET

    # Download only missing types
    python scripts/download/download_all_tickers.py --missing-only
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
import polars as pl

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_missing_types(bronze_path: Path) -> list[str]:
    """Get ticker types that don't have data yet"""
    # Get all types from metadata
    types_file = bronze_path / 'reference_data' / 'types' / 'types.parquet'
    if not types_file.exists():
        logger.warning("types/types.parquet not found")
        return []

    types_df = pl.read_parquet(types_file)
    all_types = set(types_df['code'].to_list())

    # Get types from directories
    tickers_path = bronze_path / 'reference_data' / 'tickers' / 'locale=us'
    if not tickers_path.exists():
        return list(all_types)

    existing_types = set([d.name.replace('type=', '') for d in tickers_path.iterdir() if d.is_dir()])

    missing = sorted(all_types - existing_types)
    logger.info(f"Found {len(existing_types)} existing types, {len(missing)} missing types")
    return missing


async def main():
    parser = argparse.ArgumentParser(
        description='Download all tickers to bronze layer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download all tickers (all types)'
    )

    parser.add_argument(
        '--types',
        type=str,
        help='Comma-separated list of ticker types to download'
    )

    parser.add_argument(
        '--missing-only',
        action='store_true',
        help='Download only missing ticker types'
    )

    parser.add_argument(
        '--locale',
        default='us',
        help='Locale filter (us, global, default: us)'
    )

    parser.add_argument(
        '--active-only',
        action='store_true',
        default=True,
        help='Download only active tickers (default: True)'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get credentials
    credentials = config.get_credentials('polygon')
    if not credentials or 'api' not in credentials or 'key' not in credentials['api']:
        logger.error("Polygon API key not found in config/credentials.yaml")
        sys.exit(1)

    api_key = credentials['api']['key']

    # Get bronze path
    bronze_path = config.get_bronze_path()
    ref_data_path = bronze_path / 'reference_data'
    ref_data_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Reference data path: {ref_data_path}")

    # Determine which types to download
    types_to_download = []

    if args.all:
        logger.info("Downloading ALL ticker types")
        types_to_download = [None]  # None means all types
    elif args.missing_only:
        missing_types = get_missing_types(bronze_path)
        if not missing_types:
            logger.info("No missing types found. All ticker types already downloaded!")
            sys.exit(0)
        logger.info(f"Missing types: {missing_types}")
        types_to_download = missing_types
    elif args.types:
        types_to_download = [t.strip() for t in args.types.split(',')]
        logger.info(f"Downloading types: {types_to_download}")
    else:
        logger.error("Must specify --all, --missing-only, or --types")
        sys.exit(1)

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=api_key,
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = ReferenceDataDownloader(
            client=client,
            output_dir=ref_data_path,
            use_partitioned_structure=True
        )

        logger.info("✅ ReferenceDataDownloader initialized")

        total_tickers = 0

        # Download each type
        for ticker_type in types_to_download:
            try:
                type_label = ticker_type or "ALL"
                logger.info(f"\n{'='*70}")
                logger.info(f"📥 Downloading tickers: type={type_label}, locale={args.locale}")
                logger.info(f"{'='*70}")

                df = await downloader.download_all_tickers(
                    ticker_type=ticker_type,
                    locale=args.locale,
                    active=args.active_only
                )

                total_tickers += len(df)
                logger.info(f"✅ Downloaded {len(df)} tickers for type={type_label}")

            except Exception as e:
                logger.error(f"❌ Failed to download type {ticker_type}: {e}")
                continue

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 DOWNLOAD COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"Total tickers downloaded: {total_tickers:,}")
        logger.info(f"API requests: {stats['total_requests']}")
        logger.info(f"API retries: {stats['total_retries']}")
        logger.info(f"Success rate: {stats['success_rate']:.1%}")
        logger.info(f"\n✅ Tickers saved to: {ref_data_path / 'tickers'}")


if __name__ == '__main__':
    asyncio.run(main())
