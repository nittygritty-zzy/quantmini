#!/usr/bin/env python3
"""
Phase 3: Consolidate Corporate Actions to Silver Layer

This script consolidates dividends, stock splits, and IPOs from the bronze layer
into a unified corporate_actions table in the silver layer.

Usage:
    python scripts/transformation/consolidate_corporate_actions.py
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


def process_dividends(bronze_path: Path) -> pl.DataFrame:
    """Process dividend files from bronze layer"""

    logger.info("Processing DIVIDENDS...")
    dividends_path = bronze_path / "dividends"

    if not dividends_path.exists():
        logger.warning(f"Dividends path not found: {dividends_path}")
        return None

    # Use glob pattern to read all parquet files efficiently
    pattern = str(dividends_path / "**/*.parquet")
    logger.info(f"  Reading from: {pattern}")

    try:
        # Read all files at once using glob pattern (much faster than individual reads)
        # Use hive_partitioning=False to avoid schema conflicts from extra/missing columns
        combined_df = pl.scan_parquet(pattern, hive_partitioning=False).collect()
        logger.info(f"  Loaded {len(combined_df):,} dividend records")
    except Exception as e:
        logger.error(f"Failed to read dividends: {e}")
        return None

    # Transform to unified schema
    unified_df = combined_df.select([
        pl.col('ticker'),
        pl.lit('dividend').alias('action_type'),
        pl.col('ex_dividend_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Dividend-specific columns
        pl.col('cash_amount').alias('div_cash_amount'),
        pl.col('currency').alias('div_currency'),
        pl.col('declaration_date').str.to_date().alias('div_declaration_date'),
        pl.col('dividend_type').alias('div_dividend_type'),
        pl.col('ex_dividend_date').str.to_date().alias('div_ex_dividend_date'),
        pl.col('frequency').alias('div_frequency'),
        pl.col('pay_date').str.to_date().alias('div_pay_date'),
        pl.col('record_date').str.to_date().alias('div_record_date'),

        # Null columns for splits
        pl.lit(None).cast(pl.Date).alias('split_execution_date'),
        pl.lit(None).cast(pl.Float64).alias('split_from'),
        pl.lit(None).cast(pl.Float64).alias('split_to'),
        pl.lit(None).cast(pl.Float64).alias('split_ratio'),

        # Null columns for IPOs
        pl.lit(None).cast(pl.Date).alias('ipo_last_updated'),
        pl.lit(None).cast(pl.Date).alias('ipo_announced_date'),
        pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
        pl.lit(None).cast(pl.String).alias('ipo_issuer_name'),
        pl.lit(None).cast(pl.String).alias('ipo_currency_code'),
        pl.lit(None).cast(pl.String).alias('ipo_us_code'),
        pl.lit(None).cast(pl.String).alias('ipo_isin'),
        pl.lit(None).cast(pl.Float64).alias('ipo_final_issue_price'),
        pl.lit(None).cast(pl.Int64).alias('ipo_max_shares_offered'),
        pl.lit(None).cast(pl.Float64).alias('ipo_lowest_offer_price'),
        pl.lit(None).cast(pl.Float64).alias('ipo_highest_offer_price'),
        pl.lit(None).cast(pl.Float64).alias('ipo_total_offer_size'),
        pl.lit(None).cast(pl.String).alias('ipo_primary_exchange'),
        pl.lit(None).cast(pl.Int64).alias('ipo_shares_outstanding'),
        pl.lit(None).cast(pl.String).alias('ipo_security_type'),
        pl.lit(None).cast(pl.Int64).alias('ipo_lot_size'),
        pl.lit(None).cast(pl.String).alias('ipo_security_description'),
        pl.lit(None).cast(pl.String).alias('ipo_status'),
    ])

    logger.info(f"  Processed {len(unified_df):,} dividend records")
    return unified_df


def process_splits(bronze_path: Path) -> pl.DataFrame:
    """Process stock split files from bronze layer"""

    logger.info("Processing SPLITS...")
    splits_path = bronze_path / "splits"

    if not splits_path.exists():
        logger.warning(f"Splits path not found: {splits_path}")
        return None

    # Use glob pattern to read all parquet files efficiently
    pattern = str(splits_path / "**/*.parquet")
    logger.info(f"  Reading from: {pattern}")

    try:
        # Read all files at once using glob pattern
        # Use hive_partitioning=False to avoid schema conflicts
        combined_df = pl.scan_parquet(pattern, hive_partitioning=False).collect()
        logger.info(f"  Loaded {len(combined_df):,} split records")
    except Exception as e:
        logger.error(f"Failed to read splits: {e}")
        return None

    # Transform to unified schema
    unified_df = combined_df.select([
        pl.col('ticker'),
        pl.lit('split').alias('action_type'),
        pl.col('execution_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Null columns for dividends
        pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
        pl.lit(None).cast(pl.String).alias('div_currency'),
        pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
        pl.lit(None).cast(pl.String).alias('div_dividend_type'),
        pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
        pl.lit(None).cast(pl.Int64).alias('div_frequency'),
        pl.lit(None).cast(pl.Date).alias('div_pay_date'),
        pl.lit(None).cast(pl.Date).alias('div_record_date'),

        # Split-specific columns
        pl.col('execution_date').str.to_date().alias('split_execution_date'),
        pl.col('split_from').alias('split_from'),
        pl.col('split_to').alias('split_to'),
        (pl.col('split_to') / pl.col('split_from')).alias('split_ratio'),

        # Null columns for IPOs
        pl.lit(None).cast(pl.Date).alias('ipo_last_updated'),
        pl.lit(None).cast(pl.Date).alias('ipo_announced_date'),
        pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
        pl.lit(None).cast(pl.String).alias('ipo_issuer_name'),
        pl.lit(None).cast(pl.String).alias('ipo_currency_code'),
        pl.lit(None).cast(pl.String).alias('ipo_us_code'),
        pl.lit(None).cast(pl.String).alias('ipo_isin'),
        pl.lit(None).cast(pl.Float64).alias('ipo_final_issue_price'),
        pl.lit(None).cast(pl.Int64).alias('ipo_max_shares_offered'),
        pl.lit(None).cast(pl.Float64).alias('ipo_lowest_offer_price'),
        pl.lit(None).cast(pl.Float64).alias('ipo_highest_offer_price'),
        pl.lit(None).cast(pl.Float64).alias('ipo_total_offer_size'),
        pl.lit(None).cast(pl.String).alias('ipo_primary_exchange'),
        pl.lit(None).cast(pl.Int64).alias('ipo_shares_outstanding'),
        pl.lit(None).cast(pl.String).alias('ipo_security_type'),
        pl.lit(None).cast(pl.Int64).alias('ipo_lot_size'),
        pl.lit(None).cast(pl.String).alias('ipo_security_description'),
        pl.lit(None).cast(pl.String).alias('ipo_status'),
    ])

    logger.info(f"  Processed {len(unified_df):,} split records")
    return unified_df


def process_ipos(bronze_path: Path) -> pl.DataFrame:
    """Process IPO files from bronze layer"""

    logger.info("Processing IPOS...")
    ipos_path = bronze_path / "ipos"

    if not ipos_path.exists():
        logger.warning(f"IPOs path not found: {ipos_path}")
        return None

    # Use glob pattern to read all parquet files efficiently
    pattern = str(ipos_path / "**/*.parquet")
    logger.info(f"  Reading from: {pattern}")

    try:
        # Read all files at once using glob pattern
        # Use hive_partitioning=False to avoid schema conflicts
        combined_df = pl.scan_parquet(pattern, hive_partitioning=False).collect()
        logger.info(f"  Loaded {len(combined_df):,} IPO records")
    except Exception as e:
        logger.error(f"Failed to read IPOs: {e}")
        return None

    # Transform to unified schema
    # Generate ID if not present
    if 'id' not in combined_df.columns:
        combined_df = combined_df.with_columns(
            (pl.col('ticker') + '_' + pl.col('listing_date')).alias('id')
        )

    unified_df = combined_df.select([
        pl.col('ticker'),
        pl.lit('ipo').alias('action_type'),
        pl.col('listing_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Null columns for dividends
        pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
        pl.lit(None).cast(pl.String).alias('div_currency'),
        pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
        pl.lit(None).cast(pl.String).alias('div_dividend_type'),
        pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
        pl.lit(None).cast(pl.Int64).alias('div_frequency'),
        pl.lit(None).cast(pl.Date).alias('div_pay_date'),
        pl.lit(None).cast(pl.Date).alias('div_record_date'),

        # Null columns for splits
        pl.lit(None).cast(pl.Date).alias('split_execution_date'),
        pl.lit(None).cast(pl.Float64).alias('split_from'),
        pl.lit(None).cast(pl.Float64).alias('split_to'),
        pl.lit(None).cast(pl.Float64).alias('split_ratio'),

        # IPO-specific columns
        pl.col('last_updated').str.to_date().alias('ipo_last_updated'),
        pl.col('announced_date').str.to_date().alias('ipo_announced_date'),
        pl.col('listing_date').str.to_date().alias('ipo_listing_date'),
        pl.col('issuer_name').alias('ipo_issuer_name'),
        pl.col('currency_code').alias('ipo_currency_code'),
        pl.col('us_code').alias('ipo_us_code'),
        pl.col('isin').alias('ipo_isin'),
        pl.col('final_issue_price').alias('ipo_final_issue_price'),
        pl.col('max_shares_offered').alias('ipo_max_shares_offered'),
        pl.col('lowest_offer_price').alias('ipo_lowest_offer_price'),
        pl.col('highest_offer_price').alias('ipo_highest_offer_price'),
        pl.col('total_offer_size').alias('ipo_total_offer_size'),
        pl.col('primary_exchange').alias('ipo_primary_exchange'),
        pl.col('shares_outstanding').alias('ipo_shares_outstanding'),
        pl.col('security_type').alias('ipo_security_type'),
        pl.col('lot_size').alias('ipo_lot_size'),
        pl.col('security_description').alias('ipo_security_description'),
        pl.col('ipo_status').alias('ipo_status'),
    ])

    logger.info(f"  Processed {len(unified_df):,} IPO records")
    return unified_df


def main():
    """Main entry point"""

    logger.info("="*80)
    logger.info("PHASE 3: CORPORATE ACTIONS CONSOLIDATION")
    logger.info("="*80)
    logger.info("")

    # Paths (using centralized configuration)
    quantlake_root = get_quantlake_root()
    bronze_path = quantlake_root / 'bronze' / 'corporate_actions'
    silver_path = quantlake_root / 'silver' / 'corporate_actions'

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info("")

    # Process each corporate action type
    dividends_df = process_dividends(bronze_path)
    splits_df = process_splits(bronze_path)
    ipos_df = process_ipos(bronze_path)

    # Combine all corporate actions
    logger.info("")
    logger.info("Combining all corporate actions...")

    all_dfs = []
    if dividends_df is not None:
        all_dfs.append(dividends_df)
    if splits_df is not None:
        all_dfs.append(splits_df)
    if ipos_df is not None:
        all_dfs.append(ipos_df)

    if not all_dfs:
        logger.error("No corporate actions found!")
        return

    combined_df = pl.concat(all_dfs, how="vertical_relaxed")

    # Add metadata columns
    combined_df = combined_df.with_columns([
        pl.lit(datetime.now()).alias('processed_at'),
        pl.col('event_date').dt.year().alias('year'),
        pl.col('event_date').dt.month().alias('month'),
    ])

    # Summary statistics
    logger.info(f"Total records: {len(combined_df):,}")
    logger.info(f"Total columns: {len(combined_df.columns)}")
    logger.info("")
    logger.info("Records by action type:")
    for action_type, count in combined_df.group_by('action_type').agg(pl.count()).iter_rows():
        logger.info(f"  {action_type}: {count:,}")

    logger.info("")
    logger.info(f"Unique tickers: {combined_df['ticker'].n_unique()}")
    logger.info(f"Date range: {combined_df['event_date'].min()} to {combined_df['event_date'].max()}")

    # Save to silver layer partitioned by year and month
    logger.info("")
    logger.info("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    for (year, month), group_df in combined_df.group_by(['year', 'month']):
        partition_dir = silver_path / f"year={year}" / f"month={month:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"
        group_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3
        )

        logger.info(f"  Saved: year={year}, month={month:02d} ({len(group_df):,} records)")

    logger.info("")
    logger.info("✓ Corporate actions consolidated to silver layer")
    logger.info(f"  Location: {silver_path}")
    logger.info(f"  Total records: {len(combined_df):,}")
    logger.info(f"  Total columns: {len(combined_df.columns)}")
    logger.info(f"  Partitioning: year / month")
    logger.info(f"  Action types: {', '.join(combined_df['action_type'].unique().sort())}")
    logger.info("")


if __name__ == '__main__':
    main()
