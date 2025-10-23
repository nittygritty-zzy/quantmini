#!/usr/bin/env python3
"""
Phase 3: Consolidate Corporate Actions to Silver Layer (DuckDB version)

This script consolidates dividends, stock splits, IPOs, and ticker events from the bronze layer
into unified corporate_actions tables in the silver layer using DuckDB for schema flexibility.

Usage:
    python scripts/transformation/consolidate_corporate_actions_duckdb.py
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from src.utils.paths import get_quantlake_root

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point"""

    logger.info("="*80)
    logger.info("CORPORATE ACTIONS CONSOLIDATION (DuckDB)")
    logger.info("="*80)
    logger.info("")

    # Paths
    quantlake_root = get_quantlake_root()
    bronze_path = quantlake_root / 'bronze' / 'corporate_actions'
    silver_path = quantlake_root / 'silver' / 'corporate_actions'

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info("")

    # Create connection
    conn = duckdb.connect(':memory:')

    # Process Dividends
    logger.info("Processing DIVIDENDS...")
    dividends_path = bronze_path / "dividends"
    if dividends_path.exists():
        query = f"""
        SELECT
            ticker,
            'dividend' as action_type,
            CAST(ex_dividend_date AS DATE) as event_date,
            id,
            downloaded_at,

            -- Dividend-specific columns
            cash_amount as div_cash_amount,
            currency as div_currency,
            TRY_CAST(declaration_date AS DATE) as div_declaration_date,
            dividend_type as div_dividend_type,
            CAST(ex_dividend_date AS DATE) as div_ex_dividend_date,
            frequency as div_frequency,
            TRY_CAST(pay_date AS DATE) as div_pay_date,
            TRY_CAST(record_date AS DATE) as div_record_date,

            -- Null columns for splits
            CAST(NULL AS DATE) as split_execution_date,
            CAST(NULL AS DOUBLE) as split_from,
            CAST(NULL AS DOUBLE) as split_to,
            CAST(NULL AS DOUBLE) as split_ratio,

            -- Null columns for IPOs
            CAST(NULL AS DATE) as ipo_last_updated,
            CAST(NULL AS DATE) as ipo_announced_date,
            CAST(NULL AS DATE) as ipo_listing_date,
            CAST(NULL AS VARCHAR) as ipo_issuer_name,
            CAST(NULL AS VARCHAR) as ipo_currency_code,
            CAST(NULL AS VARCHAR) as ipo_us_code,
            CAST(NULL AS VARCHAR) as ipo_isin,
            CAST(NULL AS DOUBLE) as ipo_final_issue_price,
            CAST(NULL AS BIGINT) as ipo_max_shares_offered,
            CAST(NULL AS DOUBLE) as ipo_lowest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_highest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_total_offer_size,
            CAST(NULL AS VARCHAR) as ipo_primary_exchange,
            CAST(NULL AS BIGINT) as ipo_shares_outstanding,
            CAST(NULL AS VARCHAR) as ipo_security_type,
            CAST(NULL AS BIGINT) as ipo_lot_size,
            CAST(NULL AS VARCHAR) as ipo_security_description,
            CAST(NULL AS VARCHAR) as ipo_status,

            -- Null columns for ticker events
            CAST(NULL AS VARCHAR) as event_type,
            CAST(NULL AS DATE) as event_announcement_date
        FROM read_parquet('{str(dividends_path)}/**/*.parquet', union_by_name=true, filename=false)
        """
        dividends_df = conn.execute(query).fetchdf()
        logger.info(f"  Loaded {len(dividends_df):,} dividend records")
    else:
        logger.warning(f"Dividends path not found: {dividends_path}")
        dividends_df = None

    # Process Splits
    logger.info("Processing SPLITS...")
    splits_path = bronze_path / "splits"
    if splits_path.exists():
        query = f"""
        SELECT
            ticker,
            'split' as action_type,
            CAST(execution_date AS DATE) as event_date,
            id,
            downloaded_at,

            -- Null columns for dividends
            CAST(NULL AS DOUBLE) as div_cash_amount,
            CAST(NULL AS VARCHAR) as div_currency,
            CAST(NULL AS DATE) as div_declaration_date,
            CAST(NULL AS VARCHAR) as div_dividend_type,
            CAST(NULL AS DATE) as div_ex_dividend_date,
            CAST(NULL AS BIGINT) as div_frequency,
            CAST(NULL AS DATE) as div_pay_date,
            CAST(NULL AS DATE) as div_record_date,

            -- Split-specific columns
            CAST(execution_date AS DATE) as split_execution_date,
            split_from as split_from,
            split_to as split_to,
            (split_to / split_from) as split_ratio,

            -- Null columns for IPOs
            CAST(NULL AS DATE) as ipo_last_updated,
            CAST(NULL AS DATE) as ipo_announced_date,
            CAST(NULL AS DATE) as ipo_listing_date,
            CAST(NULL AS VARCHAR) as ipo_issuer_name,
            CAST(NULL AS VARCHAR) as ipo_currency_code,
            CAST(NULL AS VARCHAR) as ipo_us_code,
            CAST(NULL AS VARCHAR) as ipo_isin,
            CAST(NULL AS DOUBLE) as ipo_final_issue_price,
            CAST(NULL AS BIGINT) as ipo_max_shares_offered,
            CAST(NULL AS DOUBLE) as ipo_lowest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_highest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_total_offer_size,
            CAST(NULL AS VARCHAR) as ipo_primary_exchange,
            CAST(NULL AS BIGINT) as ipo_shares_outstanding,
            CAST(NULL AS VARCHAR) as ipo_security_type,
            CAST(NULL AS BIGINT) as ipo_lot_size,
            CAST(NULL AS VARCHAR) as ipo_security_description,
            CAST(NULL AS VARCHAR) as ipo_status,

            -- Null columns for ticker events
            CAST(NULL AS VARCHAR) as event_type,
            CAST(NULL AS DATE) as event_announcement_date
        FROM read_parquet('{str(splits_path)}/**/*.parquet', union_by_name=true, filename=false)
        """
        splits_df = conn.execute(query).fetchdf()
        logger.info(f"  Loaded {len(splits_df):,} split records")
    else:
        logger.warning(f"Splits path not found: {splits_path}")
        splits_df = None

    # Process IPOs
    logger.info("Processing IPOS...")
    ipos_path = bronze_path / "ipos"
    if ipos_path.exists():
        query = f"""
        SELECT
            ticker,
            'ipo' as action_type,
            CAST(listing_date AS DATE) as event_date,
            ticker || '_' || listing_date as id,
            downloaded_at,

            -- Null columns for dividends
            CAST(NULL AS DOUBLE) as div_cash_amount,
            CAST(NULL AS VARCHAR) as div_currency,
            CAST(NULL AS DATE) as div_declaration_date,
            CAST(NULL AS VARCHAR) as div_dividend_type,
            CAST(NULL AS DATE) as div_ex_dividend_date,
            CAST(NULL AS BIGINT) as div_frequency,
            CAST(NULL AS DATE) as div_pay_date,
            CAST(NULL AS DATE) as div_record_date,

            -- Null columns for splits
            CAST(NULL AS DATE) as split_execution_date,
            CAST(NULL AS DOUBLE) as split_from,
            CAST(NULL AS DOUBLE) as split_to,
            CAST(NULL AS DOUBLE) as split_ratio,

            -- IPO-specific columns
            TRY_CAST(last_updated AS DATE) as ipo_last_updated,
            TRY_CAST(announced_date AS DATE) as ipo_announced_date,
            CAST(listing_date AS DATE) as ipo_listing_date,
            issuer_name as ipo_issuer_name,
            currency_code as ipo_currency_code,
            us_code as ipo_us_code,
            isin as ipo_isin,
            final_issue_price as ipo_final_issue_price,
            max_shares_offered as ipo_max_shares_offered,
            lowest_offer_price as ipo_lowest_offer_price,
            highest_offer_price as ipo_highest_offer_price,
            total_offer_size as ipo_total_offer_size,
            primary_exchange as ipo_primary_exchange,
            shares_outstanding as ipo_shares_outstanding,
            security_type as ipo_security_type,
            lot_size as ipo_lot_size,
            security_description as ipo_security_description,
            ipo_status as ipo_status,

            -- Null columns for ticker events
            CAST(NULL AS VARCHAR) as event_type,
            CAST(NULL AS DATE) as event_announcement_date
        FROM read_parquet('{str(ipos_path)}/**/*.parquet', union_by_name=true, filename=false)
        """
        ipos_df = conn.execute(query).fetchdf()
        logger.info(f"  Loaded {len(ipos_df):,} IPO records")
    else:
        logger.warning(f"IPOs path not found: {ipos_path}")
        ipos_df = None

    # Process Ticker Events
    logger.info("Processing TICKER EVENTS...")
    events_path = bronze_path / "ticker_events"
    if events_path.exists():
        query = f"""
        SELECT
            ticker,
            'ticker_event' as action_type,
            CAST(date AS DATE) as event_date,
            ticker || '_' || event_type || '_' || date as id,
            downloaded_at,

            -- Null columns for dividends
            CAST(NULL AS DOUBLE) as div_cash_amount,
            CAST(NULL AS VARCHAR) as div_currency,
            CAST(NULL AS DATE) as div_declaration_date,
            CAST(NULL AS VARCHAR) as div_dividend_type,
            CAST(NULL AS DATE) as div_ex_dividend_date,
            CAST(NULL AS BIGINT) as div_frequency,
            CAST(NULL AS DATE) as div_pay_date,
            CAST(NULL AS DATE) as div_record_date,

            -- Null columns for splits
            CAST(NULL AS DATE) as split_execution_date,
            CAST(NULL AS DOUBLE) as split_from,
            CAST(NULL AS DOUBLE) as split_to,
            CAST(NULL AS DOUBLE) as split_ratio,

            -- Null columns for IPOs
            CAST(NULL AS DATE) as ipo_last_updated,
            CAST(NULL AS DATE) as ipo_announced_date,
            CAST(NULL AS DATE) as ipo_listing_date,
            CAST(NULL AS VARCHAR) as ipo_issuer_name,
            CAST(NULL AS VARCHAR) as ipo_currency_code,
            CAST(NULL AS VARCHAR) as ipo_us_code,
            CAST(NULL AS VARCHAR) as ipo_isin,
            CAST(NULL AS DOUBLE) as ipo_final_issue_price,
            CAST(NULL AS BIGINT) as ipo_max_shares_offered,
            CAST(NULL AS DOUBLE) as ipo_lowest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_highest_offer_price,
            CAST(NULL AS DOUBLE) as ipo_total_offer_size,
            CAST(NULL AS VARCHAR) as ipo_primary_exchange,
            CAST(NULL AS BIGINT) as ipo_shares_outstanding,
            CAST(NULL AS VARCHAR) as ipo_security_type,
            CAST(NULL AS BIGINT) as ipo_lot_size,
            CAST(NULL AS VARCHAR) as ipo_security_description,
            CAST(NULL AS VARCHAR) as ipo_status,

            -- Ticker event specific
            event_type,
            CAST(date AS DATE) as event_announcement_date
        FROM read_parquet('{str(events_path)}/**/*.parquet', union_by_name=true, filename=false)
        """
        events_df = conn.execute(query).fetchdf()
        logger.info(f"  Loaded {len(events_df):,} ticker event records")
    else:
        logger.warning(f"Ticker events path not found: {events_path}")
        events_df = None

    # Combine all
    logger.info("")
    logger.info("Combining all corporate actions...")

    import pandas as pd
    all_dfs = []
    if dividends_df is not None:
        all_dfs.append(dividends_df)
    if splits_df is not None:
        all_dfs.append(splits_df)
    if ipos_df is not None:
        all_dfs.append(ipos_df)
    if events_df is not None:
        all_dfs.append(events_df)

    if not all_dfs:
        logger.error("No corporate actions found!")
        return 1

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Add metadata columns
    combined_df['processed_at'] = datetime.now()
    combined_df['year'] = pd.to_datetime(combined_df['event_date']).dt.year
    combined_df['month'] = pd.to_datetime(combined_df['event_date']).dt.month

    # Summary
    logger.info(f"Total records: {len(combined_df):,}")
    logger.info(f"Total columns: {len(combined_df.columns)}")
    logger.info("")
    logger.info("Records by action type:")
    for action_type, count in combined_df['action_type'].value_counts().items():
        logger.info(f"  {action_type}: {count:,}")

    logger.info("")
    logger.info(f"Unique tickers: {combined_df['ticker'].nunique()}")
    logger.info(f"Date range: {combined_df['event_date'].min()} to {combined_df['event_date'].max()}")

    # Save to silver layer partitioned by year and month
    logger.info("")
    logger.info("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    import pyarrow as pa
    import pyarrow.parquet as pq

    # Group by year/month and save
    for (year, month), group_df in combined_df.groupby(['year', 'month']):
        partition_dir = silver_path / f"year={int(year)}" / f"month={int(month):02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"

        # Convert to PyArrow table and write
        table = pa.Table.from_pandas(group_df, preserve_index=False)
        pq.write_table(table, output_file, compression='zstd', compression_level=3)

        logger.info(f"  Saved: year={int(year)}, month={int(month):02d} ({len(group_df):,} records)")

    logger.info("")
    logger.info("✓ Corporate actions consolidated to silver layer")
    logger.info(f"  Location: {silver_path}")
    logger.info(f"  Total records: {len(combined_df):,}")
    logger.info(f"  Total columns: {len(combined_df.columns)}")
    logger.info(f"  Partitioning: year / month")
    logger.info(f"  Action types: {', '.join(sorted(combined_df['action_type'].unique()))}")
    logger.info("")

    return 0


if __name__ == '__main__':
    sys.exit(main())
