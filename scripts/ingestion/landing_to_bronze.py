#!/usr/bin/env python3
"""
Process Landing → Bronze Layer

Reads raw CSV.GZ files from landing layer and ingests them to bronze layer
(validated Parquet format). This is the proper Medallion Architecture flow.

Usage:
    # Process stocks_daily from landing to bronze
    python scripts/ingestion/landing_to_bronze.py \
        --data-type stocks_daily \
        --start-date 2020-10-18 \
        --end-date 2025-10-18

    # Process all data types
    python scripts/ingestion/landing_to_bronze.py \
        --data-type all \
        --start-date 2020-10-18 \
        --end-date 2025-10-18
"""

import argparse
import sys
import logging
import gzip
from pathlib import Path
from datetime import datetime, timedelta
from io import BytesIO

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader
from src.ingest.streaming_ingestor import StreamingIngestor
from src.ingest.polars_ingestor import PolarsIngestor
from src.storage.parquet_manager import ParquetManager
from src.storage.metadata_manager import MetadataManager
from src.core.system_profiler import SystemProfiler
from src.utils.market_calendar import get_default_calendar
from src.utils.paths import get_quantlake_root

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_landing_files(
    landing_root: Path,
    data_type: str,
    start_date: str,
    end_date: str
) -> list[Path]:
    """
    Get list of landing files for date range

    Args:
        landing_root: Landing layer root directory
        data_type: Data type (stocks_daily, etc.)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of landing file paths
    """
    landing_path = landing_root / 'polygon-s3' / data_type

    if not landing_path.exists():
        logger.warning(f"Landing path does not exist: {landing_path}")
        return []

    # Generate date range
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    files = []
    current = start
    while current <= end:
        # Check year/month directory
        year_month_dir = landing_path / current.strftime('%Y') / current.strftime('%m')

        if year_month_dir.exists():
            # Look for file matching this date
            filename = f"{current.strftime('%Y-%m-%d')}.csv.gz"
            file_path = year_month_dir / filename

            if file_path.exists():
                files.append(file_path)

        current += timedelta(days=1)

    return sorted(files)


def process_landing_to_bronze(
    data_type: str,
    start_date: str,
    end_date: str,
    config: ConfigLoader,
    incremental: bool = True
):
    """
    Process landing files to bronze layer

    Args:
        data_type: Data type to process
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        config: Config loader
        incremental: Skip already processed files
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"Processing Landing → Bronze: {data_type}")
    logger.info(f"{'='*70}")
    logger.info(f"Date range: {start_date} to {end_date}")

    # Get paths
    # Use configured data_lake_root if available, otherwise fall back to environment-based path
    data_lake_root = config.get('data_lake_root')
    if not data_lake_root:
        data_lake_root = get_quantlake_root()

    landing_root = Path(data_lake_root) / 'landing'
    bronze_root = config.get_bronze_path()
    metadata_root = config.get_metadata_path()

    logger.info(f"Landing: {landing_root}")
    logger.info(f"Bronze: {bronze_root}")

    # Initialize managers
    metadata_manager = MetadataManager(metadata_root)

    # Get processing mode
    profiler = SystemProfiler()
    processing_mode = profiler.profile.get('recommended_mode', 'streaming')

    logger.info(f"Processing mode: {processing_mode}")

    # Select ingestor based on mode
    if processing_mode == 'batch':
        ingestor = PolarsIngestor(
            data_type=data_type,
            output_root=bronze_root,
            config=config.config
        )
    else:
        ingestor = StreamingIngestor(
            data_type=data_type,
            output_root=bronze_root,
            config=config.config
        )

    # Get landing files
    landing_files = get_landing_files(landing_root, data_type, start_date, end_date)

    if not landing_files:
        logger.warning(f"No landing files found for {data_type} from {start_date} to {end_date}")
        return {
            'status': 'no_data',
            'files_processed': 0,
            'rows_ingested': 0
        }

    logger.info(f"Found {len(landing_files)} landing files")

    # Get watermark if incremental
    last_watermark = None
    if incremental:
        last_watermark = metadata_manager.get_watermark(data_type)
        if last_watermark:
            logger.info(f"Last watermark: {last_watermark}")

    # Process each file
    files_processed = 0
    total_rows = 0

    for i, landing_file in enumerate(landing_files):
        try:
            # Extract date from filename
            file_date = landing_file.stem.replace('.csv', '')  # YYYY-MM-DD

            # Skip if before watermark
            if incremental and last_watermark and file_date <= last_watermark:
                logger.debug(f"Skipping {file_date} (before watermark)")
                continue

            logger.info(f"Processing {i+1}/{len(landing_files)}: {file_date}")

            # Read and decompress landing file
            with gzip.open(landing_file, 'rb') as f:
                csv_data = f.read()

            # Ingest to bronze
            result = ingestor.ingest_date(
                date=file_date,
                data=BytesIO(csv_data)
            )

            if result and result.get('status') in ['success', 'skipped']:
                files_processed += 1
                rows_written = result.get('records', 0)
                total_rows += rows_written

                # Record ingestion metadata
                metadata_manager.record_ingestion(
                    data_type=data_type,
                    date=file_date,
                    status=result.get('status'),
                    statistics={
                        'records': rows_written,
                        'file_size_mb': result.get('file_size_mb', 0),
                        'processing_time_sec': result.get('processing_time_sec', 0),
                        'reason': result.get('reason', '')
                    }
                )

                # Update watermark
                metadata_manager.set_watermark(
                    data_type=data_type,
                    date=file_date
                )

                if result.get('status') == 'skipped':
                    logger.info(f"  ⊙ Skipped {file_date} ({result.get('reason', 'unknown')})")
                else:
                    logger.info(f"  ✓ Ingested {rows_written:,} rows to bronze")
            else:
                logger.error(f"  ✗ Failed to ingest {file_date}")

                # Record failure
                metadata_manager.record_ingestion(
                    data_type=data_type,
                    date=file_date,
                    status='failed',
                    statistics={},
                    error='Ingestion returned non-success status'
                )

        except Exception as e:
            logger.error(f"Error processing {landing_file}: {e}")

            # Record error
            try:
                file_date = landing_file.stem.replace('.csv', '')
                metadata_manager.record_ingestion(
                    data_type=data_type,
                    date=file_date,
                    status='failed',
                    statistics={},
                    error=str(e)
                )
            except:
                pass  # Don't let metadata errors block the pipeline

            continue

    # Summary
    summary = {
        'status': 'completed',
        'data_type': data_type,
        'date_range': {'start': start_date, 'end': end_date},
        'files_available': len(landing_files),
        'files_processed': files_processed,
        'total_rows': total_rows,
    }

    logger.info(f"\n{'='*70}")
    logger.info(f"Landing → Bronze Complete: {data_type}")
    logger.info(f"{'='*70}")
    logger.info(f"Files processed: {files_processed}/{len(landing_files)}")
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Bronze path: {bronze_root / data_type}")

    return summary


def main():
    parser = argparse.ArgumentParser(
        description='Process landing files to bronze layer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--data-type',
        required=True,
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        help='Data type to process'
    )

    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        required=True,
        help='End date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--no-incremental',
        action='store_true',
        help='Reprocess all files (ignore watermarks)'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Determine data types
    if args.data_type == 'all':
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']
    else:
        data_types = [args.data_type]

    # Process each data type
    results = {}
    for data_type in data_types:
        try:
            result = process_landing_to_bronze(
                data_type,
                args.start_date,
                args.end_date,
                config,
                incremental=not args.no_incremental
            )
            results[data_type] = result
        except Exception as e:
            logger.error(f"Failed to process {data_type}: {e}")
            results[data_type] = {'status': 'error', 'error': str(e)}

    # Overall summary
    logger.info(f"\n{'='*70}")
    logger.info("OVERALL SUMMARY")
    logger.info(f"{'='*70}")

    total_files = 0
    total_rows = 0

    for data_type, result in results.items():
        if result['status'] == 'completed':
            files = result['files_processed']
            rows = result['total_rows']
            total_files += files
            total_rows += rows
            logger.info(f"✅ {data_type:20} {files:6} files, {rows:12,} rows")
        else:
            logger.info(f"❌ {data_type:20} {result.get('status', 'error')}")

    logger.info(f"\n{'─'*70}")
    logger.info(f"TOTAL: {total_files} files, {total_rows:,} rows")
    logger.info(f"Bronze: {config.get_bronze_path()}")


if __name__ == '__main__':
    main()
