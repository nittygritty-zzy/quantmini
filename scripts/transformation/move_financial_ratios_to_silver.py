#!/usr/bin/env python3
"""
Move Financial Ratios to Silver Layer

This script consolidates financial ratios from the bronze layer
and moves them to the silver layer with minimal transformation.

Usage:
    python scripts/transformation/move_financial_ratios_to_silver.py
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
from src.utils.paths import get_quantlake_root

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point"""

    logger.info("="*80)
    logger.info("MOVING FINANCIAL RATIOS TO SILVER LAYER")
    logger.info("="*80)
    logger.info("")

    # Paths (using centralized configuration)
    quantlake_root = get_quantlake_root()
    bronze_path = quantlake_root / 'fundamentals' / 'financial_ratios'
    silver_path = quantlake_root / 'silver' / 'financial_ratios'

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info("")

    # Find all parquet files
    all_files = list(bronze_path.rglob("*.parquet"))
    logger.info(f"Found {len(all_files):,} files")

    # Load all files
    logger.info("Loading and consolidating files...")
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            continue

    # Combine all data (using vertical_relaxed to handle schema differences)
    logger.info(f"Combining {len(dfs)} dataframes...")
    # Collect all unique columns across all dataframes
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)

    # Ensure all dataframes have the same columns (fill missing with nulls)
    aligned_dfs = []
    for df in dfs:
        missing_cols = all_columns - set(df.columns)
        for col in missing_cols:
            df = df.with_columns(pl.lit(None).alias(col))
        aligned_dfs.append(df.select(sorted(all_columns)))

    combined_df = pl.concat(aligned_dfs, how="vertical_relaxed")

    logger.info(f"Total records: {len(combined_df):,}")
    logger.info(f"Total columns: {len(combined_df.columns)}")
    logger.info(f"Unique tickers: {combined_df['ticker'].n_unique()}")

    # Add processed_at timestamp
    combined_df = combined_df.with_columns(
        pl.lit(datetime.now()).alias('processed_at')
    )

    # Save to silver layer partitioned by fiscal_year and fiscal_period
    logger.info("")
    logger.info("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    for (year, quarter), group_df in combined_df.group_by(['fiscal_year', 'fiscal_period']):
        partition_dir = silver_path / f"year={year}" / f"quarter={quarter}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"
        group_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3
        )

        logger.info(f"  Saved: year={year}, quarter={quarter} ({len(group_df):,} records)")

    logger.info("")
    logger.info("✓ Financial ratios moved to silver layer")
    logger.info(f"  Location: {silver_path}")
    logger.info(f"  Total records: {len(combined_df):,}")
    logger.info(f"  Total columns: {len(combined_df.columns)}")
    logger.info(f"  Partitioning: fiscal_year / fiscal_period")
    logger.info("")


if __name__ == '__main__':
    main()
