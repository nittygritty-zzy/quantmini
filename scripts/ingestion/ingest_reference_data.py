#!/usr/bin/env python3
"""
Download comprehensive ticker metadata from Polygon API

This script downloads metadata for all ~100K tickers from Polygon and creates
a lookup table with detailed company information and fundamentals status.

Uses httpx with HTTP/2 for maximum performance with unlimited API rate.

Output: data/reference/ticker_metadata.parquet

Usage:
    python scripts/download_ticker_metadata.py
    python scripts/download_ticker_metadata.py --bulk-concurrency 50 --detail-concurrency 100
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config_loader import ConfigLoader

# Import the TickerMetadataDownloader class from batch script
import importlib.util
spec = importlib.util.spec_from_file_location(
    "batch_module",
    project_root / "scripts/batch_load_fundamentals_all.py"
)
batch_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(batch_module)

TickerMetadataDownloader = batch_module.TickerMetadataDownloader


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Download ticker metadata for all tickers"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Download ticker metadata from Polygon API'
    )
    parser.add_argument(
        '--bulk-concurrency',
        type=int,
        default=100,
        help='Concurrent requests for bulk ticker download (default: 100)'
    )
    parser.add_argument(
        '--detail-concurrency',
        type=int,
        default=200,
        help='Concurrent requests for ticker enrichment (default: 200)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('data/reference/ticker_metadata.parquet'),
        help='Output file path (default: data/reference/ticker_metadata.parquet)'
    )
    parser.add_argument(
        '--fundamentals-dir',
        type=Path,
        default=Path('data/partitioned_screener'),
        help='Fundamentals directory for status computation (default: data/partitioned_screener)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("TICKER METADATA DOWNLOAD")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load credentials
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')
    api_key = credentials['api']['key']

    # Create output directory
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Initialize downloader
    downloader = TickerMetadataDownloader(
        api_key=api_key,
        output_dir=args.output.parent,
        bulk_concurrency=args.bulk_concurrency,
        detail_concurrency=args.detail_concurrency,
    )

    # Override output file
    downloader.output_file = args.output

    try:
        # Run all phases using the simplified implementation
        # This will only download from Polygon API and partition by locale/type
        df_final = await downloader.run_all_phases()

        print()
        print("=" * 80)
        print("✅ TICKER METADATA DOWNLOAD COMPLETE!")
        print("=" * 80)
        print(f"Total tickers: {len(df_final):,}")
        print(f"Output directory: {args.output.parent}")
        print("=" * 80)

    except Exception as e:
        logger.error(f"Download failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
