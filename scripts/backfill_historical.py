#!/usr/bin/env python3
"""
Backfill Historical Data

Backfills historical market data for the last 2 months across all data types.

Usage:
    python scripts/backfill_historical.py --start 2025-08-01 --end 2025-09-30
    python scripts/backfill_historical.py --start 2025-08-01 --end 2025-09-30 --data-type stocks_daily
    python scripts/backfill_historical.py --days 60  # Last 60 days
"""

import asyncio
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import sys
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.orchestration.ingestion_orchestrator import IngestionOrchestrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def backfill_data(
    start_date: str,
    end_date: str,
    data_types: list[str],
    incremental: bool = True,
    use_polars: bool = False
):
    """
    Backfill historical data for date range

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        data_types: List of data types to backfill
        incremental: Skip already ingested dates
        use_polars: Use high-performance Polars ingestor
    """
    # Initialize
    config = ConfigLoader()

    # Use data_root from config (supports external drives)
    data_root = Path(config.get('data_root', 'data'))
    parquet_root = data_root / "data" / "parquet"
    metadata_root = data_root / "data" / "metadata"

    orchestrator = IngestionOrchestrator(
        config=config,
        parquet_root=parquet_root,
        metadata_root=metadata_root
    )

    logger.info(f"Starting backfill: {start_date} to {end_date}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Incremental: {incremental}")
    logger.info(f"Use Polars: {use_polars}")

    # Backfill each data type
    results = {}

    for data_type in data_types:
        logger.info(f"\n{'='*70}")
        logger.info(f"Backfilling: {data_type}")
        logger.info(f"{'='*70}")

        try:
            result = await orchestrator.ingest_date_range(
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                symbols=None,  # All symbols
                incremental=incremental,
                use_polars=use_polars
            )

            results[data_type] = result

            # Print summary
            logger.info(f"\n✅ {data_type} Complete:")
            logger.info(f"   Status: {result['status']}")
            logger.info(f"   Ingested: {result.get('ingested', 0)} files")
            logger.info(f"   Failed: {result.get('failed', 0)} files")
            logger.info(f"   Records: {result.get('records_processed', 0):,}")

        except Exception as e:
            logger.error(f"❌ {data_type} failed: {e}")
            results[data_type] = {'status': 'error', 'error': str(e)}

    # Final summary
    logger.info(f"\n{'='*70}")
    logger.info(f"BACKFILL SUMMARY")
    logger.info(f"{'='*70}")

    total_records = 0
    total_ingested = 0
    total_failed = 0

    for data_type, result in results.items():
        status_icon = "✅" if result.get('status') == 'completed' else "❌"
        records = result.get('records_processed', 0)
        ingested = result.get('ingested', 0)
        failed = result.get('failed', 0)

        logger.info(f"{status_icon} {data_type:20} {records:>12,} records  ({ingested} ingested, {failed} failed)")

        total_records += records
        total_ingested += ingested
        total_failed += failed

    logger.info(f"\n{'─'*70}")
    logger.info(f"TOTAL:               {total_records:>12,} records  ({total_ingested} ingested, {total_failed} failed)")

    # Pipeline statistics
    stats = orchestrator.get_statistics()
    logger.info(f"\nPipeline Statistics:")
    logger.info(f"   Downloads: {stats['downloads']}")
    logger.info(f"   Errors: {stats['errors']}")
    logger.info(f"   Data downloaded: {stats['bytes_downloaded'] / 1024**2:.2f} MB")

    return results


def main():
    parser = argparse.ArgumentParser(description='Backfill historical market data')

    # Date range options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--start', help='Start date (YYYY-MM-DD)')
    date_group.add_argument('--days', type=int, help='Number of days to backfill from today')

    parser.add_argument('--end', help='End date (YYYY-MM-DD, default: today)')

    # Data type selection
    parser.add_argument(
        '--data-type',
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        default='all',
        help='Data type to backfill (default: all)'
    )

    # Processing options
    parser.add_argument(
        '--no-incremental',
        action='store_true',
        help='Reprocess all dates (skip incremental check)'
    )

    parser.add_argument(
        '--use-polars',
        action='store_true',
        help='Use high-performance Polars ingestor'
    )

    args = parser.parse_args()

    # Calculate dates
    if args.days:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    else:
        start_date = args.start
        end_date = args.end or datetime.now().strftime('%Y-%m-%d')

    # Determine data types
    if args.data_type == 'all':
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']
    else:
        data_types = [args.data_type]

    # Run backfill
    asyncio.run(backfill_data(
        start_date=start_date,
        end_date=end_date,
        data_types=data_types,
        incremental=not args.no_incremental,
        use_polars=args.use_polars
    ))


if __name__ == '__main__':
    main()
