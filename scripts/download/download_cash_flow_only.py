#!/usr/bin/env python3
"""
Download Cash Flow Statements ONLY for all 9,900 common stock tickers

This script focuses on completing cash flow statement downloads with:
- Progress tracking and resume capability
- Controlled parallelism to avoid connection errors
- Date range filtering (2010-2025)
- Clear status reporting
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Set

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config_loader import ConfigLoader
from src.download import PolygonRESTClient, FundamentalsDownloader
from src.utils.paths import get_quantlake_root
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/cash_flow_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_active_tickers() -> List[str]:
    """Load 9,900 active common stocks from reference data"""
    data_root = get_quantlake_root()
    tickers_path = data_root / "bronze/reference_data/tickers"

    logger.info(f"Loading tickers from: {tickers_path}")

    conn = duckdb.connect(':memory:')
    query = f"""
    SELECT DISTINCT ticker
    FROM read_parquet('{str(tickers_path)}/**/*.parquet')
    WHERE type = 'CS' AND locale = 'us'
    ORDER BY ticker
    """

    result = conn.execute(query).fetchall()
    tickers = [row[0] for row in result]
    logger.info(f"Loaded {len(tickers)} active common stock tickers")
    return tickers


def get_completed_tickers() -> Set[str]:
    """Check which tickers already have cash flow statement data"""
    data_root = get_quantlake_root()
    # Note: cash flow data is currently in 'cash_flow' folder, not 'cash_flow_statements'
    cf_path = data_root / "bronze/fundamentals/cash_flow"

    logger.info(f"Checking for existing cash flow statements at: {cf_path}")

    if not cf_path.exists():
        logger.info("No existing cash flow statements found")
        return set()

    conn = duckdb.connect(':memory:')
    try:
        query = f"""
        SELECT DISTINCT tickers[1] as ticker
        FROM read_parquet('{str(cf_path)}/**/*.parquet')
        WHERE tickers IS NOT NULL AND array_length(tickers) > 0
        """
        result = conn.execute(query).fetchall()
        completed = {row[0] for row in result if row[0]}
        logger.info(f"Found {len(completed)} tickers with existing cash flow statements")
        return completed
    except Exception as e:
        logger.warning(f"Could not check existing data: {e}")
        return set()


async def download_cash_flow_for_ticker(
    ticker: str,
    client: PolygonRESTClient,
    output_dir: Path,
    start_date: str = "2010-01-01",
    end_date: str = "2026-01-01"
) -> bool:
    """
    Download cash flow statements for a single ticker

    Returns:
        True if successful, False if failed
    """
    try:
        downloader = FundamentalsDownloader(
            client=client,
            output_dir=output_dir,
            use_partitioned_structure=True
        )

        # Download only cash flow statements
        df = await downloader.download_cash_flow_statements(
            ticker=ticker,
            filing_date_gte=start_date,
            filing_date_lt=end_date,
            timeframe='quarterly',
            limit=100
        )

        record_count = len(df) if df is not None else 0
        logger.info(f"✅ {ticker}: Downloaded {record_count} cash flow records")
        return True

    except Exception as e:
        logger.error(f"❌ {ticker}: Failed - {str(e)}")
        return False


async def download_batch(
    tickers: List[str],
    api_key: str,
    output_dir: Path,
    batch_size: int = 10
) -> tuple:
    """
    Download cash flow statements for a batch of tickers in parallel

    Args:
        tickers: List of ticker symbols
        api_key: Polygon API key
        output_dir: Output directory
        batch_size: Number of concurrent downloads

    Returns:
        Tuple of (successful_count, failed_count)
    """
    async with PolygonRESTClient(
        api_key=api_key,
        max_concurrent=50,  # Reduced from 100 to avoid connection errors
        max_connections=100  # Reduced from 200
    ) as client:

        # Process in batches to avoid overwhelming the API
        successful = 0
        failed = 0

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} tickers")

            tasks = [
                download_cash_flow_for_ticker(
                    ticker=ticker,
                    client=client,
                    output_dir=output_dir
                )
                for ticker in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=False)

            successful += sum(1 for r in results if r)
            failed += sum(1 for r in results if not r)

            logger.info(f"Batch complete: {successful} successful, {failed} failed so far")

            # Small delay between batches
            await asyncio.sleep(2)

    return successful, failed


async def main():
    """Main download orchestrator"""
    print("=" * 80)
    print("CASH FLOW STATEMENTS DOWNLOAD - FOCUSED EXECUTION")
    print("=" * 80)
    print()

    # Load configuration
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')

    # Extract API key (supports multiple formats)
    api_key = None
    if credentials:
        if 'api_key' in credentials:
            api_key = credentials['api_key']
        elif 'api' in credentials and isinstance(credentials['api'], dict):
            api_key = credentials['api'].get('key')
        elif 'key' in credentials:
            api_key = credentials['key']

    if not api_key:
        logger.error(f"❌ Polygon API key not found in credentials: {credentials}")
        return 1

    data_root = get_quantlake_root()
    output_dir = data_root / "bronze/fundamentals"

    # Get tickers
    logger.info("Loading ticker list...")
    all_tickers = get_active_tickers()

    # Check what's already completed
    logger.info("Checking existing cash flow statements...")
    completed_tickers = get_completed_tickers()

    # Filter to only tickers that need downloading
    pending_tickers = [t for t in all_tickers if t not in completed_tickers]

    print()
    print(f"📊 Status:")
    print(f"  Total Tickers:     {len(all_tickers):,}")
    print(f"  Already Complete:  {len(completed_tickers):,}")
    print(f"  Pending Download:  {len(pending_tickers):,}")
    print()

    if not pending_tickers:
        print("✅ All tickers already have cash flow statements!")
        return 0

    # Download
    print(f"🚀 Starting download for {len(pending_tickers):,} tickers...")
    print(f"   Date Range: 2010-01-01 to 2026-01-01")
    print(f"   Batch Size: 10 tickers at a time")
    print(f"   Output: {output_dir}")
    print()

    start_time = datetime.now()

    successful, failed = await download_batch(
        tickers=pending_tickers,
        api_key=api_key,
        output_dir=output_dir,
        batch_size=10  # Conservative to avoid connection errors
    )

    elapsed = datetime.now() - start_time

    # Final report
    print()
    print("=" * 80)
    print("DOWNLOAD COMPLETE")
    print("=" * 80)
    print(f"✅ Successful:  {successful:,} tickers")
    print(f"❌ Failed:      {failed:,} tickers")
    print(f"⏱️  Duration:    {elapsed}")
    print(f"📊 Success Rate: {successful/(successful+failed)*100:.1f}%")
    print()
    print(f"📁 Data Location: {output_dir}/cash_flow/")
    print(f"📄 Log File: /tmp/cash_flow_download.log")
    print("=" * 80)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
