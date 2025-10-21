#!/usr/bin/env python3
"""
Optimized Corporate Actions Silver Layer Transformation

This script creates an optimized silver layer for corporate actions data with:
- Ticker-first partitioning for fast stock screening
- Event-type sub-partitioning for efficient filtering
- Derived features for analysis
- Data quality validation

Partitioning structure:
    silver/corporate_actions/
    ├── ticker=AAPL/
    │   ├── event_type=dividend/
    │   │   └── data.parquet
    │   ├── event_type=split/
    │   │   └── data.parquet
    └── ticker=MSFT/
        └── event_type=dividend/
            └── data.parquet

Usage:
    python scripts/transformation/corporate_actions_silver_optimized.py
    python scripts/transformation/corporate_actions_silver_optimized.py --tickers AAPL MSFT GOOGL
"""

import sys
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
from src.utils.paths import get_quantlake_root
from src.storage.metadata_manager import MetadataManager
from src.core.config_loader import ConfigLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_dividends(bronze_path: Path, tickers: Optional[List[str]] = None) -> pl.DataFrame:
    """Process dividend files from bronze layer"""

    logger.info("Processing DIVIDENDS...")
    dividends_path = bronze_path / "dividends"

    if not dividends_path.exists():
        logger.warning(f"Dividends path not found: {dividends_path}")
        return None

    # Find all parquet files
    all_files = list(dividends_path.rglob("*.parquet"))

    # Filter by tickers if specified
    if tickers:
        ticker_set = set(t.upper() for t in tickers)
        all_files = [f for f in all_files if f.stem.replace('ticker=', '') in ticker_set]

    logger.info(f"  Found {len(all_files):,} dividend files")

    if not all_files:
        return None

    # Load all files
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            continue

    if not dfs:
        return None

    # Combine all dividends
    combined_df = pl.concat(dfs, how="vertical_relaxed")

    # Transform to unified schema with derived features
    unified_df = combined_df.select([
        # Base fields
        pl.col('ticker'),
        pl.lit('dividend').alias('event_type'),
        pl.col('ex_dividend_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Dividend-specific fields
        pl.col('cash_amount').alias('div_cash_amount'),
        pl.col('currency').alias('div_currency'),
        pl.col('declaration_date').str.to_date().alias('div_declaration_date'),
        pl.col('dividend_type').alias('div_type'),
        pl.col('ex_dividend_date').str.to_date().alias('div_ex_dividend_date'),
        pl.col('frequency').alias('div_frequency'),
        pl.col('pay_date').str.to_date().alias('div_pay_date'),
        pl.col('record_date').str.to_date().alias('div_record_date'),

        # Null columns for splits
        pl.lit(None).cast(pl.Date).alias('split_execution_date'),
        pl.lit(None).cast(pl.Float64).alias('split_from'),
        pl.lit(None).cast(pl.Float64).alias('split_to'),
        pl.lit(None).cast(pl.Float64).alias('split_ratio'),
        pl.lit(None).cast(pl.Boolean).alias('split_is_reverse'),

        # Null columns for IPOs
        pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
        pl.lit(None).cast(pl.Float64).alias('ipo_issue_price'),
        pl.lit(None).cast(pl.Int64).alias('ipo_shares_offered'),
        pl.lit(None).cast(pl.String).alias('ipo_exchange'),
        pl.lit(None).cast(pl.String).alias('ipo_status'),

        # Null columns for ticker changes
        pl.lit(None).cast(pl.String).alias('new_ticker'),
    ])

    # Add derived features for dividends
    unified_df = unified_df.with_columns([
        # Annualized amount based on frequency
        pl.when(pl.col('div_frequency') == 12).then(pl.col('div_cash_amount') * 12)
          .when(pl.col('div_frequency') == 4).then(pl.col('div_cash_amount') * 4)
          .when(pl.col('div_frequency') == 2).then(pl.col('div_cash_amount') * 2)
          .when(pl.col('div_frequency') == 1).then(pl.col('div_cash_amount'))
          .otherwise(None)
          .alias('div_annualized_amount'),

        # Special dividend flag (one-time)
        (pl.col('div_frequency') == 0).alias('div_is_special'),

        # Quarter from ex-dividend date
        pl.col('event_date').dt.quarter().cast(pl.Int8).alias('div_quarter'),
    ])

    logger.info(f"  Processed {len(unified_df):,} dividend records")
    return unified_df


def process_splits(bronze_path: Path, tickers: Optional[List[str]] = None) -> pl.DataFrame:
    """Process stock split files from bronze layer"""

    logger.info("Processing SPLITS...")
    splits_path = bronze_path / "splits"

    if not splits_path.exists():
        logger.warning(f"Splits path not found: {splits_path}")
        return None

    # Find all parquet files
    all_files = list(splits_path.rglob("*.parquet"))

    # Filter by tickers if specified
    if tickers:
        ticker_set = set(t.upper() for t in tickers)
        all_files = [f for f in all_files if f.stem.replace('ticker=', '') in ticker_set]

    logger.info(f"  Found {len(all_files):,} split files")

    if not all_files:
        return None

    # Load all files
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            continue

    if not dfs:
        return None

    # Combine all splits
    combined_df = pl.concat(dfs, how="vertical_relaxed")

    # Transform to unified schema
    unified_df = combined_df.select([
        # Base fields
        pl.col('ticker'),
        pl.lit('split').alias('event_type'),
        pl.col('execution_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Null columns for dividends
        pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
        pl.lit(None).cast(pl.String).alias('div_currency'),
        pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
        pl.lit(None).cast(pl.String).alias('div_type'),
        pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
        pl.lit(None).cast(pl.Int64).alias('div_frequency'),
        pl.lit(None).cast(pl.Date).alias('div_pay_date'),
        pl.lit(None).cast(pl.Date).alias('div_record_date'),
        pl.lit(None).cast(pl.Float64).alias('div_annualized_amount'),
        pl.lit(None).cast(pl.Boolean).alias('div_is_special'),
        pl.lit(None).cast(pl.Int8).alias('div_quarter'),

        # Split-specific fields
        pl.col('execution_date').str.to_date().alias('split_execution_date'),
        pl.col('split_from').cast(pl.Float64).alias('split_from'),
        pl.col('split_to').cast(pl.Float64).alias('split_to'),
        (pl.col('split_to').cast(pl.Float64) / pl.col('split_from').cast(pl.Float64)).alias('split_ratio'),

        # Null columns for IPOs
        pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
        pl.lit(None).cast(pl.Float64).alias('ipo_issue_price'),
        pl.lit(None).cast(pl.Int64).alias('ipo_shares_offered'),
        pl.lit(None).cast(pl.String).alias('ipo_exchange'),
        pl.lit(None).cast(pl.String).alias('ipo_status'),

        # Null columns for ticker changes
        pl.lit(None).cast(pl.String).alias('new_ticker'),
    ])

    # Add derived features for splits
    unified_df = unified_df.with_columns([
        # Reverse split flag (ratio < 1)
        (pl.col('split_ratio') < 1.0).alias('split_is_reverse'),
    ])

    logger.info(f"  Processed {len(unified_df):,} split records")
    return unified_df


def process_ipos(bronze_path: Path, tickers: Optional[List[str]] = None) -> pl.DataFrame:
    """Process IPO files from bronze layer"""

    logger.info("Processing IPOS...")
    ipos_path = bronze_path / "ipos"

    if not ipos_path.exists():
        logger.warning(f"IPOs path not found: {ipos_path}")
        return None

    # Find all parquet files
    all_files = list(ipos_path.rglob("*.parquet"))

    # Filter by tickers if specified
    if tickers:
        ticker_set = set(t.upper() for t in tickers)
        all_files = [f for f in all_files if f.stem.replace('ticker=', '') in ticker_set]

    logger.info(f"  Found {len(all_files):,} IPO files")

    if not all_files:
        return None

    # Load all files
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            continue

    if not dfs:
        return None

    # Combine all IPOs
    combined_df = pl.concat(dfs, how="vertical_relaxed")

    # Generate ID if not present
    if 'id' not in combined_df.columns:
        combined_df = combined_df.with_columns(
            (pl.col('ticker') + '_' + pl.col('listing_date')).alias('id')
        )

    # Transform to unified schema
    unified_df = combined_df.select([
        # Base fields
        pl.col('ticker'),
        pl.lit('ipo').alias('event_type'),
        pl.col('listing_date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Null columns for dividends
        pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
        pl.lit(None).cast(pl.String).alias('div_currency'),
        pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
        pl.lit(None).cast(pl.String).alias('div_type'),
        pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
        pl.lit(None).cast(pl.Int64).alias('div_frequency'),
        pl.lit(None).cast(pl.Date).alias('div_pay_date'),
        pl.lit(None).cast(pl.Date).alias('div_record_date'),
        pl.lit(None).cast(pl.Float64).alias('div_annualized_amount'),
        pl.lit(None).cast(pl.Boolean).alias('div_is_special'),
        pl.lit(None).cast(pl.Int8).alias('div_quarter'),

        # Null columns for splits
        pl.lit(None).cast(pl.Date).alias('split_execution_date'),
        pl.lit(None).cast(pl.Float64).alias('split_from'),
        pl.lit(None).cast(pl.Float64).alias('split_to'),
        pl.lit(None).cast(pl.Float64).alias('split_ratio'),
        pl.lit(None).cast(pl.Boolean).alias('split_is_reverse'),

        # IPO-specific fields
        pl.col('listing_date').str.to_date().alias('ipo_listing_date'),
        pl.col('final_issue_price').alias('ipo_issue_price'),
        pl.col('max_shares_offered').alias('ipo_shares_offered'),
        pl.col('primary_exchange').alias('ipo_exchange'),
        pl.col('ipo_status').alias('ipo_status'),

        # Null columns for ticker changes
        pl.lit(None).cast(pl.String).alias('new_ticker'),
    ])

    logger.info(f"  Processed {len(unified_df):,} IPO records")
    return unified_df


def process_ticker_events(bronze_path: Path, tickers: Optional[List[str]] = None) -> pl.DataFrame:
    """Process ticker change events from bronze layer"""

    logger.info("Processing TICKER EVENTS...")
    ticker_events_path = bronze_path / "ticker_events"

    if not ticker_events_path.exists():
        logger.warning(f"Ticker events path not found: {ticker_events_path}")
        return None

    # Find all parquet files
    all_files = list(ticker_events_path.rglob("*.parquet"))

    # Filter by tickers if specified
    if tickers:
        ticker_set = set(t.upper() for t in tickers)
        all_files = [f for f in all_files if f.stem.replace('ticker=', '') in ticker_set]

    logger.info(f"  Found {len(all_files):,} ticker event files")

    if not all_files:
        return None

    # Load all files
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            continue

    if not dfs:
        return None

    # Combine all ticker events
    combined_df = pl.concat(dfs, how="vertical_relaxed")

    # Generate ID if not present
    if 'id' not in combined_df.columns:
        combined_df = combined_df.with_columns(
            (pl.col('ticker') + '_' + pl.col('date')).alias('id')
        )

    # Transform to unified schema
    unified_df = combined_df.select([
        # Base fields
        pl.col('ticker'),
        pl.lit('ticker_change').alias('event_type'),
        pl.col('date').str.to_date().alias('event_date'),
        pl.col('id'),
        pl.col('downloaded_at'),

        # Null columns for dividends
        pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
        pl.lit(None).cast(pl.String).alias('div_currency'),
        pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
        pl.lit(None).cast(pl.String).alias('div_type'),
        pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
        pl.lit(None).cast(pl.Int64).alias('div_frequency'),
        pl.lit(None).cast(pl.Date).alias('div_pay_date'),
        pl.lit(None).cast(pl.Date).alias('div_record_date'),
        pl.lit(None).cast(pl.Float64).alias('div_annualized_amount'),
        pl.lit(None).cast(pl.Boolean).alias('div_is_special'),
        pl.lit(None).cast(pl.Int8).alias('div_quarter'),

        # Null columns for splits
        pl.lit(None).cast(pl.Date).alias('split_execution_date'),
        pl.lit(None).cast(pl.Float64).alias('split_from'),
        pl.lit(None).cast(pl.Float64).alias('split_to'),
        pl.lit(None).cast(pl.Float64).alias('split_ratio'),
        pl.lit(None).cast(pl.Boolean).alias('split_is_reverse'),

        # Null columns for IPOs
        pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
        pl.lit(None).cast(pl.Float64).alias('ipo_issue_price'),
        pl.lit(None).cast(pl.Int64).alias('ipo_shares_offered'),
        pl.lit(None).cast(pl.String).alias('ipo_exchange'),
        pl.lit(None).cast(pl.String).alias('ipo_status'),

        # Ticker change specific fields
        pl.col('new_ticker') if 'new_ticker' in combined_df.columns else pl.lit(None).cast(pl.String).alias('new_ticker'),
    ])

    logger.info(f"  Processed {len(unified_df):,} ticker change records")
    return unified_df


def write_partitioned_silver(df: pl.DataFrame, silver_path: Path) -> dict:
    """
    Write data to silver layer with ticker + event_type partitioning

    Args:
        df: DataFrame with all corporate actions
        silver_path: Root path for silver layer

    Returns:
        Dictionary with write statistics
    """
    silver_path.mkdir(parents=True, exist_ok=True)

    stats = {
        'tickers_written': 0,
        'files_written': 0,
        'total_records': len(df)
    }

    # Get unique ticker/event_type combinations
    partitions = df.select(['ticker', 'event_type']).unique()

    logger.info(f"Writing {len(partitions)} partitions...")

    for row in partitions.iter_rows(named=True):
        ticker = row['ticker']
        event_type = row['event_type']

        # Filter data for this partition
        partition_df = df.filter(
            (pl.col('ticker') == ticker) &
            (pl.col('event_type') == event_type)
        )

        # Sort by event_date descending (most recent first)
        partition_df = partition_df.sort('event_date', descending=True)

        # Add processing metadata
        partition_df = partition_df.with_columns([
            pl.lit(datetime.now()).alias('processed_at'),
            pl.col('event_date').dt.year().cast(pl.Int32).alias('year'),
            pl.col('event_date').dt.quarter().cast(pl.Int8).alias('quarter'),
            pl.col('event_date').dt.month().cast(pl.Int8).alias('month'),
        ])

        # Create partition directory
        partition_dir = silver_path / f"ticker={ticker}" / f"event_type={event_type}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"

        # Write with optimizations
        partition_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3,
            statistics=True,  # Write column statistics for predicate pushdown
            row_group_size=50000  # Optimize for query performance
        )

        stats['files_written'] += 1

        if stats['files_written'] % 100 == 0:
            logger.info(f"  Written {stats['files_written']} partitions...")

    stats['tickers_written'] = df.select('ticker').n_unique()

    return stats


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Transform corporate actions to optimized silver layer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--tickers',
        nargs='+',
        help='Specific tickers to process (default: all)'
    )

    parser.add_argument(
        '--bronze-dir',
        type=Path,
        help='Bronze layer path (default: $QUANTLAKE_ROOT/bronze/corporate_actions)'
    )

    parser.add_argument(
        '--silver-dir',
        type=Path,
        help='Silver layer path (default: $QUANTLAKE_ROOT/silver/corporate_actions)'
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("OPTIMIZED CORPORATE ACTIONS SILVER LAYER TRANSFORMATION")
    logger.info("="*80)
    logger.info("")

    # Paths (using centralized configuration)
    quantlake_root = get_quantlake_root()
    bronze_path = args.bronze_dir or quantlake_root / 'bronze' / 'corporate_actions'
    silver_path = args.silver_dir or quantlake_root / 'silver' / 'corporate_actions'

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")

    if args.tickers:
        logger.info(f"Processing tickers: {', '.join(args.tickers)}")
    else:
        logger.info("Processing ALL tickers")
    logger.info("")

    # Process each corporate action type
    dividends_df = process_dividends(bronze_path, args.tickers)
    splits_df = process_splits(bronze_path, args.tickers)
    ipos_df = process_ipos(bronze_path, args.tickers)
    ticker_events_df = process_ticker_events(bronze_path, args.tickers)

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
    if ticker_events_df is not None:
        all_dfs.append(ticker_events_df)

    if not all_dfs:
        logger.error("No corporate actions found!")
        return

    # Define consistent column order
    column_order = [
        # Base fields
        'ticker', 'event_type', 'event_date', 'id', 'downloaded_at',
        # Dividend fields
        'div_cash_amount', 'div_currency', 'div_declaration_date', 'div_type',
        'div_ex_dividend_date', 'div_frequency', 'div_pay_date', 'div_record_date',
        'div_annualized_amount', 'div_is_special', 'div_quarter',
        # Split fields
        'split_execution_date', 'split_from', 'split_to', 'split_ratio', 'split_is_reverse',
        # IPO fields
        'ipo_listing_date', 'ipo_issue_price', 'ipo_shares_offered', 'ipo_exchange', 'ipo_status',
        # Ticker change fields
        'new_ticker'
    ]

    # Ensure all dataframes have the same columns in the same order
    aligned_dfs = [df.select(column_order) for df in all_dfs]

    combined_df = pl.concat(aligned_dfs, how="vertical")

    # Summary statistics
    logger.info(f"Total records: {len(combined_df):,}")
    logger.info(f"Total columns: {len(combined_df.columns)}")
    logger.info("")
    logger.info("Records by event type:")
    for event_type, count in combined_df.group_by('event_type').agg(pl.len()).iter_rows():
        logger.info(f"  {event_type}: {count:,}")

    logger.info("")
    logger.info(f"Unique tickers: {combined_df['ticker'].n_unique()}")
    logger.info(f"Date range: {combined_df['event_date'].min()} to {combined_df['event_date'].max()}")

    # Write to silver layer with optimized partitioning
    logger.info("")
    logger.info("Writing to silver layer with ticker + event_type partitioning...")

    stats = write_partitioned_silver(combined_df, silver_path)

    logger.info("")
    logger.info("✓ Corporate actions silver layer created")
    logger.info(f"  Location: {silver_path}")
    logger.info(f"  Tickers: {stats['tickers_written']:,}")
    logger.info(f"  Files written: {stats['files_written']:,}")
    logger.info(f"  Total records: {stats['total_records']:,}")
    logger.info(f"  Partitioning: ticker / event_type")
    logger.info(f"  Optimization: Sorted by event_date DESC, no dictionary encoding")
    logger.info("")

    # Record metadata for silver layer
    try:
        config = ConfigLoader()
        metadata_root = config.get_metadata_path()
        metadata_manager = MetadataManager(metadata_root)

        # Get date range from the combined data
        min_date = str(combined_df['event_date'].min())
        max_date = str(combined_df['event_date'].max())

        # Record metadata for each date in the range
        # For corporate actions, we record a single entry for the transformation
        metadata_manager.record_ingestion(
            data_type='corporate_actions',
            date=max_date,  # Use max date as the watermark
            status='success',
            statistics={
                'records': stats['total_records'],
                'tickers': stats['tickers_written'],
                'files_written': stats['files_written'],
                'min_date': min_date,
                'max_date': max_date,
            },
            layer='silver'
        )

        # Update watermark
        metadata_manager.set_watermark(
            data_type='corporate_actions',
            date=max_date,
            layer='silver'
        )

        logger.info("✓ Metadata recorded for silver layer")

    except Exception as e:
        logger.warning(f"Failed to record metadata: {e}")


if __name__ == '__main__':
    main()
