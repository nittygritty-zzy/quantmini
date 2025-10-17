#!/usr/bin/env python3
"""
Re-ingest Production Data with Fixed Schema

This script re-ingests data using the corrected schema (explicit types, no dictionary encoding).

Usage:
    python scripts/reingest_production.py --data-type stocks_daily
    python scripts/reingest_production.py --data-type all --start 2020-10-16 --end 2025-10-16
"""

import asyncio
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.orchestration.ingestion_orchestrator import IngestionOrchestrator


async def reingest_date_range(
    data_type: str,
    start_date: str,
    end_date: str,
    data_root: Path
):
    """Re-ingest data for a date range"""

    # Load config and override data_root
    config_loader = ConfigLoader()
    config_loader.config['data_root'] = str(data_root)

    # Create orchestrator with production data root
    orchestrator = IngestionOrchestrator(config=config_loader)

    # Parse dates
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    # Generate date list
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    print(f"🔄 Re-ingesting {data_type}")
    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Total dates: {len(dates)}")
    print(f"   Data root: {data_root}")
    print()

    # Ingest in batches
    batch_size = 30
    for i in range(0, len(dates), batch_size):
        batch = dates[i:i+batch_size]

        print(f"📥 Batch {i//batch_size + 1}/{(len(dates) + batch_size - 1)//batch_size}")
        print(f"   Dates: {batch[0]} to {batch[-1]}")

        # Ingest batch (use date range)
        result = await orchestrator.ingest_date_range(
            data_type=data_type,
            start_date=batch[0],
            end_date=batch[-1],
            incremental=False,  # Force re-ingestion
            use_polars=True  # Use fixed Polars ingestor
        )

        # Stats
        success = result.get('ingested', 0)
        total = result.get('total_files', len(batch))
        print(f"   ✅ {success}/{total} files ingested successfully")
        print()


async def main():
    parser = argparse.ArgumentParser(description='Re-ingest production data with fixed schema')
    parser.add_argument(
        '--data-type',
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        required=True,
        help='Data type to re-ingest'
    )
    parser.add_argument(
        '--start',
        default='2020-10-16',
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        default='2025-10-16',
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--data-root',
        default='/Volumes/sandisk/quantmini-data',
        help='Production data root directory'
    )

    args = parser.parse_args()
    data_root = Path(args.data_root)

    if not data_root.exists():
        print(f"❌ Data root not found: {data_root}")
        sys.exit(1)

    # Re-ingest
    if args.data_type == 'all':
        for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            await reingest_date_range(data_type, args.start, args.end, data_root)
    else:
        await reingest_date_range(args.data_type, args.start, args.end, data_root)

    print("✅ Re-ingestion complete!")


if __name__ == '__main__':
    asyncio.run(main())
