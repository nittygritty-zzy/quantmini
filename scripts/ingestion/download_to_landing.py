#!/usr/bin/env python3
"""
Download Raw Files to Landing Layer

Downloads raw CSV.GZ files from Polygon S3 to the landing layer
for archival and audit purposes before processing to Bronze.

Usage:
    # Download stocks_daily to landing
    python scripts/ingestion/download_to_landing.py \\
        --data-type stocks_daily \\
        --start-date 2020-10-18 \\
        --end-date 2025-10-18

    # Download all data types
    python scripts/ingestion/download_to_landing.py \\
        --data-type all \\
        --start-date 2020-10-18 \\
        --end-date 2025-10-18
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
from src.download.s3_catalog import S3Catalog
from src.download.async_downloader import AsyncS3Downloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def download_to_landing(
    data_type: str,
    start_date: str,
    end_date: str,
    config: ConfigLoader,
    credentials: dict
):
    """
    Download raw files to landing layer

    Args:
        data_type: Data type to download
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        config: Config loader
        credentials: S3 credentials
    """
    logger.info(f"\n{'='*70}")
    logger.info(f"Downloading {data_type} to Landing Layer")
    logger.info(f"{'='*70}")
    logger.info(f"Date range: {start_date} to {end_date}")

    # Get landing path (organized by source)
    data_lake_root = config.get('data_lake_root')
    if not data_lake_root:
        raise ValueError("data_lake_root not configured")

    landing_path = Path(data_lake_root) / 'landing' / 'polygon-s3' / data_type
    landing_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Landing path: {landing_path}")

    # Initialize components
    catalog = S3Catalog()
    downloader = AsyncS3Downloader(
        credentials=credentials,
        endpoint_url=config.get('source.s3.endpoint', 'https://files.polygon.io'),
        max_concurrent=config.get('ingestion.max_concurrent_downloads', 4)
    )

    # Get S3 keys for date range
    keys = catalog.get_date_range_keys(
        data_type,
        start_date,
        end_date
    )

    if not keys:
        logger.warning(f"No keys found for {data_type} from {start_date} to {end_date}")
        return {
            'status': 'no_data',
            'files_downloaded': 0,
            'bytes_downloaded': 0
        }

    logger.info(f"Found {len(keys)} files to download")

    # Download files
    bucket = config.get('source.s3.bucket', 'flatfiles')
    files_downloaded = 0
    bytes_downloaded = 0

    for i, key in enumerate(keys):
        try:
            # Extract date from key for organizing landing files
            metadata = catalog.parse_key_metadata(key)
            date = metadata.get('date', 'unknown')

            # Create date-based subdirectory in landing
            date_path = landing_path / date[:4] / date[5:7]  # year/month
            date_path.mkdir(parents=True, exist_ok=True)

            # Output filename
            output_file = date_path / Path(key).name

            # Skip if already exists
            if output_file.exists():
                logger.debug(f"Skipping {output_file.name} (already exists)")
                continue

            # Download single file (keep compressed .gz format)
            file_data = await downloader.download_one(bucket, key, decompress=False)

            if file_data:
                # Write raw file to landing
                with open(output_file, 'wb') as f:
                    f.write(file_data.getvalue())

                file_size = len(file_data.getvalue())
                files_downloaded += 1
                bytes_downloaded += file_size

                logger.info(
                    f"Downloaded {i+1}/{len(keys)}: {output_file.name} "
                    f"({file_size / 1024 / 1024:.1f} MB)"
                )
            else:
                logger.error(f"Failed to download: {key}")

        except Exception as e:
            logger.error(f"Error downloading {key}: {e}")
            continue

    # Summary
    summary = {
        'status': 'completed',
        'data_type': data_type,
        'date_range': {'start': start_date, 'end': end_date},
        'files_downloaded': files_downloaded,
        'bytes_downloaded': bytes_downloaded,
        'mb_downloaded': bytes_downloaded / 1024 / 1024,
        'gb_downloaded': bytes_downloaded / 1024 / 1024 / 1024,
    }

    logger.info(f"\n{'='*70}")
    logger.info(f"Download Complete: {data_type}")
    logger.info(f"{'='*70}")
    logger.info(f"Files downloaded: {files_downloaded}/{len(keys)}")
    logger.info(f"Data downloaded: {summary['gb_downloaded']:.2f} GB")
    logger.info(f"Landing path: {landing_path}")

    return summary


async def main():
    parser = argparse.ArgumentParser(
        description='Download raw files to landing layer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--data-type',
        required=True,
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        help='Data type to download'
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

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get credentials
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 's3' not in polygon_creds:
        logger.error("S3 credentials not found in config/credentials.yaml")
        sys.exit(1)

    credentials = {
        'access_key_id': polygon_creds['s3']['access_key_id'],
        'secret_access_key': polygon_creds['s3']['secret_access_key'],
    }

    # Determine data types
    if args.data_type == 'all':
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']
    else:
        data_types = [args.data_type]

    # Download each data type
    results = {}
    for data_type in data_types:
        try:
            result = await download_to_landing(
                data_type,
                args.start_date,
                args.end_date,
                config,
                credentials
            )
            results[data_type] = result
        except Exception as e:
            logger.error(f"Failed to download {data_type}: {e}")
            results[data_type] = {'status': 'error', 'error': str(e)}

    # Overall summary
    logger.info(f"\n{'='*70}")
    logger.info("OVERALL SUMMARY")
    logger.info(f"{'='*70}")

    total_files = 0
    total_gb = 0

    for data_type, result in results.items():
        if result['status'] == 'completed':
            files = result['files_downloaded']
            gb = result['gb_downloaded']
            total_files += files
            total_gb += gb
            logger.info(f"✅ {data_type:20} {files:6} files, {gb:8.2f} GB")
        else:
            logger.info(f"❌ {data_type:20} {result.get('status', 'error')}")

    logger.info(f"\n{'─'*70}")
    logger.info(f"TOTAL: {total_files} files, {total_gb:.2f} GB")
    logger.info(f"Landing: {config.get('data_lake_root')}/landing/polygon-s3/")


if __name__ == '__main__':
    asyncio.run(main())
