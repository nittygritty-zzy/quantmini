#!/usr/bin/env python3
"""
Download Ticker Relationships for All Tickers

Downloads related company data from Polygon's related-companies endpoint
for all tickers in the tickers/ directory. Focuses on ticker types where
relationships are meaningful (CS, ETF, ADR, PFD).

Usage:
    # Download relationships for all CS and ETF tickers
    python scripts/download/download_all_relationships.py --types CS,ETF

    # Download for all supported types
    python scripts/download/download_all_relationships.py --all

    # Download only for tickers without relationships yet
    python scripts/download/download_all_relationships.py --all --incremental
"""

import argparse
import asyncio
import sys
import logging
from pathlib import Path
import polars as pl

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

# Ticker types where relationships make sense
RELATIONSHIP_TYPES = ['CS', 'ETF', 'ADRC', 'ADRP', 'ADRR', 'PFD']


def get_tickers_for_types(bronze_path: Path, ticker_types: list[str]) -> list[str]:
    """
    Extract all ticker symbols for given types from tickers/ directory

    Args:
        bronze_path: Path to bronze layer
        ticker_types: List of ticker types to extract

    Returns:
        List of ticker symbols
    """
    tickers = []
    tickers_path = bronze_path / 'reference_data' / 'tickers' / 'locale=us'

    for ticker_type in ticker_types:
        type_dir = tickers_path / f'type={ticker_type}'
        data_file = type_dir / 'data.parquet'

        if not data_file.exists():
            logger.warning(f"No data found for type {ticker_type}")
            continue

        df = pl.read_parquet(data_file)

        if 'ticker' in df.columns:
            type_tickers = df['ticker'].to_list()
            tickers.extend(type_tickers)
            logger.info(f"Found {len(type_tickers):,} tickers for type {ticker_type}")

    return tickers


def get_existing_relationships(bronze_path: Path) -> set[str]:
    """Get set of tickers that already have relationship data"""
    relationships_path = bronze_path / 'reference_data' / 'relationships'
    if not relationships_path.exists():
        return set()

    existing = set()
    for file in relationships_path.glob('ticker=*.parquet'):
        # Extract ticker from filename: ticker=AAPL.parquet -> AAPL
        ticker = file.stem.replace('ticker=', '')
        existing.add(ticker)

    return existing


async def main():
    parser = argparse.ArgumentParser(
        description='Download ticker relationships to bronze layer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download relationships for all supported ticker types'
    )

    parser.add_argument(
        '--types',
        type=str,
        help='Comma-separated list of ticker types (CS,ETF,PFD,etc.)'
    )

    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Skip tickers that already have relationship data'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of tickers to process (for testing)'
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
    bronze_path = config.get_bronze_path()
    ref_data_path = bronze_path / 'reference_data'

    # Determine which types to process
    if args.all:
        types_to_process = RELATIONSHIP_TYPES
    elif args.types:
        types_to_process = [t.strip() for t in args.types.split(',')]
    else:
        logger.error("Must specify --all or --types")
        sys.exit(1)

    logger.info(f"Processing ticker types: {types_to_process}")

    # Get all tickers for these types
    logger.info("\nExtracting tickers from reference data...")
    all_tickers = get_tickers_for_types(bronze_path, types_to_process)

    if not all_tickers:
        logger.error("No tickers found!")
        sys.exit(1)

    logger.info(f"\nTotal tickers found: {len(all_tickers):,}")

    # Filter out tickers that already have relationships (if incremental)
    if args.incremental:
        existing = get_existing_relationships(bronze_path)
        original_count = len(all_tickers)
        all_tickers = [t for t in all_tickers if t not in existing]
        logger.info(f"Incremental mode: {len(existing):,} already have relationships")
        logger.info(f"Tickers to download: {len(all_tickers):,} (skipped {original_count - len(all_tickers):,})")

    # Apply limit if specified
    if args.limit:
        all_tickers = all_tickers[:args.limit]
        logger.info(f"Limiting to first {args.limit} tickers")

    if not all_tickers:
        logger.info("No tickers to process!")
        sys.exit(0)

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,  # High parallelism for speed
        max_connections=200
    ) as client:

        downloader = ReferenceDataDownloader(
            client=client,
            output_dir=ref_data_path,
            use_partitioned_structure=True
        )

        logger.info("✅ ReferenceDataDownloader initialized")
        logger.info(f"\n{'='*70}")
        logger.info(f"📥 Downloading relationships for {len(all_tickers):,} tickers")
        logger.info(f"{'='*70}")

        try:
            # Download relationships in batch (uses massive parallelization)
            df = await downloader.download_related_tickers_batch(
                tickers=all_tickers,
                save_intermediate=True
            )

            logger.info(f"\n✅ Successfully downloaded {len(df):,} total relationships")

        except Exception as e:
            logger.error(f"❌ Failed to download relationships: {e}")
            sys.exit(1)

        # Print statistics
        stats = client.get_statistics()
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 DOWNLOAD COMPLETE")
        logger.info(f"{'='*70}")
        logger.info(f"Tickers processed: {len(all_tickers):,}")
        logger.info(f"Relationships found: {len(df):,}")
        logger.info(f"API requests: {stats['total_requests']:,}")
        logger.info(f"API retries: {stats['total_retries']:,}")
        logger.info(f"Success rate: {stats['success_rate']:.1%}")
        logger.info(f"\n✅ Relationships saved to: {ref_data_path / 'relationships'}")


if __name__ == '__main__':
    asyncio.run(main())
