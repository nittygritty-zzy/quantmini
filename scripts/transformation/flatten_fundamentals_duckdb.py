#!/usr/bin/env python3
"""
Phase 2: Flatten Fundamentals from Bronze to Silver Layer (DuckDB version)

Memory-efficient transformation using DuckDB to process data incrementally by partition.
Avoids loading all 4,585 tickers into memory at once.

Strategy:
1. Use DuckDB to read Bronze parquet files directly
2. Process one year/quarter partition at a time
3. Flatten nested structs using SQL
4. Calculate financial ratios and metrics
5. Write each partition to Silver immediately
6. Release memory before processing next partition

Usage:
    python scripts/transformation/flatten_fundamentals_duckdb.py

Output:
    $QUANTLAKE_ROOT/silver/fundamentals_wide/
    Partitioned by: year={fiscal_year}/quarter={fiscal_period}
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.paths import get_quantlake_root

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point"""

    logger.info("="*80)
    logger.info("FUNDAMENTALS FLATTENING - DuckDB Memory-Efficient Version")
    logger.info("="*80)
    logger.info("")

    # Paths
    quantlake_root = get_quantlake_root()
    bronze_path = quantlake_root / 'bronze' / 'fundamentals'
    silver_path = quantlake_root / 'silver' / 'fundamentals_wide'

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info("")

    # Create DuckDB connection
    conn = duckdb.connect(':memory:')

    # Step 1: Discover all year/quarter partitions in Bronze
    logger.info("Discovering available partitions in Bronze layer...")

    # Get unique year/month combinations from balance sheets
    bs_path = bronze_path / 'balance_sheets'

    query = f"""
    SELECT DISTINCT
        CAST(fiscal_year AS INTEGER) as fiscal_year,
        fiscal_period
    FROM read_parquet('{str(bs_path)}/**/*.parquet', union_by_name=true)
    WHERE fiscal_year IS NOT NULL
      AND fiscal_period IS NOT NULL
    ORDER BY fiscal_year, fiscal_period
    """

    partitions_df = conn.execute(query).fetchdf()
    logger.info(f"Found {len(partitions_df)} unique year/quarter partitions")
    logger.info("")

    # Step 2: Process each partition incrementally
    silver_path.mkdir(parents=True, exist_ok=True)

    total_records = 0

    for idx, row in partitions_df.iterrows():
        fiscal_year = int(row['fiscal_year'])
        fiscal_period = row['fiscal_period']

        logger.info(f"Processing partition [{idx+1}/{len(partitions_df)}]: year={fiscal_year}, quarter={fiscal_period}")

        try:
            # Process this partition
            partition_df = process_partition(conn, bronze_path, fiscal_year, fiscal_period)

            if partition_df is None or len(partition_df) == 0:
                logger.warning(f"  No data for year={fiscal_year}, quarter={fiscal_period}")
                continue

            # Save to Silver layer
            partition_dir = silver_path / f"year={fiscal_year}" / f"quarter={fiscal_period}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / "data.parquet"

            # Convert to PyArrow table and write
            table = pa.Table.from_pandas(partition_df, preserve_index=False)
            pq.write_table(table, output_file, compression='zstd', compression_level=3)

            total_records += len(partition_df)
            logger.info(f"  ✓ Saved {len(partition_df):,} records, {len(partition_df.columns)} columns")

        except Exception as e:
            logger.error(f"  ✗ Failed to process partition: {e}")
            continue

    logger.info("")
    logger.info("="*80)
    logger.info("✓ FUNDAMENTALS FLATTENING COMPLETE")
    logger.info("="*80)
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Total partitions: {len(partitions_df)}")
    logger.info(f"Output location: {silver_path}")
    logger.info("="*80)
    logger.info("")

    conn.close()
    return 0


def process_partition(conn, bronze_path, fiscal_year, fiscal_period):
    """
    Process a single year/quarter partition

    Args:
        conn: DuckDB connection
        bronze_path: Path to bronze fundamentals directory
        fiscal_year: Fiscal year to process
        fiscal_period: Fiscal period (Q1, Q2, Q3, Q4) to process

    Returns:
        Pandas DataFrame with flattened fundamentals for this partition
    """

    # Paths to three statement types
    bs_path = bronze_path / 'balance_sheets'
    is_path = bronze_path / 'income_statements'
    cf_path = bronze_path / 'cash_flow'

    # Build query to flatten and join all three statements for this partition
    # Using DuckDB's struct access syntax: financials.balance_sheet.assets.value

    query = f"""
    WITH balance_sheet_flat AS (
        SELECT
            ticker,
            filing_date,
            fiscal_year,
            fiscal_period,

            -- Extract balance sheet metrics (use TRY to handle missing fields)
            TRY_CAST(financials.balance_sheet.assets.value AS DOUBLE) as bs_assets,
            TRY_CAST(financials.balance_sheet.current_assets.value AS DOUBLE) as bs_current_assets,
            TRY_CAST(financials.balance_sheet.noncurrent_assets.value AS DOUBLE) as bs_noncurrent_assets,
            TRY_CAST(financials.balance_sheet.liabilities.value AS DOUBLE) as bs_liabilities,
            TRY_CAST(financials.balance_sheet.current_liabilities.value AS DOUBLE) as bs_current_liabilities,
            TRY_CAST(financials.balance_sheet.noncurrent_liabilities.value AS DOUBLE) as bs_noncurrent_liabilities,
            TRY_CAST(financials.balance_sheet.equity.value AS DOUBLE) as bs_equity,
            TRY_CAST(financials.balance_sheet.equity_attributable_to_parent.value AS DOUBLE) as bs_equity_attributable_to_parent,
            TRY_CAST(financials.balance_sheet.liabilities_and_equity.value AS DOUBLE) as bs_liabilities_and_equity,
            TRY_CAST(financials.balance_sheet.inventory.value AS DOUBLE) as bs_inventory,
            TRY_CAST(financials.balance_sheet.accounts_receivable.value AS DOUBLE) as bs_accounts_receivable,
            TRY_CAST(financials.balance_sheet.cash_and_cash_equivalents.value AS DOUBLE) as bs_cash,
            TRY_CAST(financials.balance_sheet.intangible_assets.value AS DOUBLE) as bs_intangible_assets,
            TRY_CAST(financials.balance_sheet.goodwill.value AS DOUBLE) as bs_goodwill,
            TRY_CAST(financials.balance_sheet.long_term_debt.value AS DOUBLE) as bs_long_term_debt,
            TRY_CAST(financials.balance_sheet.short_term_debt.value AS DOUBLE) as bs_short_term_debt
        FROM read_parquet('{str(bs_path)}/**/*.parquet', union_by_name=true)
        WHERE CAST(fiscal_year AS INTEGER) = {fiscal_year}
          AND fiscal_period = '{fiscal_period}'
    ),

    income_statement_flat AS (
        SELECT
            ticker,
            filing_date,
            fiscal_year,
            fiscal_period,

            -- Extract income statement metrics
            TRY_CAST(financials.income_statement.revenues.value AS DOUBLE) as is_revenues,
            TRY_CAST(financials.income_statement.cost_of_revenue.value AS DOUBLE) as is_cost_of_revenue,
            TRY_CAST(financials.income_statement.gross_profit.value AS DOUBLE) as is_gross_profit,
            TRY_CAST(financials.income_statement.operating_expenses.value AS DOUBLE) as is_operating_expenses,
            TRY_CAST(financials.income_statement.operating_income_loss.value AS DOUBLE) as is_operating_income_loss,
            TRY_CAST(financials.income_statement.net_income_loss.value AS DOUBLE) as is_net_income_loss,
            TRY_CAST(financials.income_statement.net_income_loss_attributable_to_parent.value AS DOUBLE) as is_net_income_loss_attributable_to_parent,
            TRY_CAST(financials.income_statement.income_loss_before_equity_method_investments.value AS DOUBLE) as is_income_before_tax,
            TRY_CAST(financials.income_statement.income_tax_expense_benefit.value AS DOUBLE) as is_income_tax_expense,
            TRY_CAST(financials.income_statement.interest_expense.value AS DOUBLE) as is_interest_expense,
            TRY_CAST(financials.income_statement.basic_earnings_per_share.value AS DOUBLE) as is_basic_eps,
            TRY_CAST(financials.income_statement.diluted_earnings_per_share.value AS DOUBLE) as is_diluted_eps,
            TRY_CAST(financials.income_statement.preferred_stock_dividends_and_other_adjustments.value AS DOUBLE) as is_preferred_dividends
        FROM read_parquet('{str(is_path)}/**/*.parquet', union_by_name=true)
        WHERE CAST(fiscal_year AS INTEGER) = {fiscal_year}
          AND fiscal_period = '{fiscal_period}'
    ),

    cash_flow_flat AS (
        SELECT
            ticker,
            filing_date,
            fiscal_year,
            fiscal_period,

            -- Extract cash flow metrics
            TRY_CAST(financials.cash_flow_statement.net_cash_flow.value AS DOUBLE) as cf_net_cash_flow,
            TRY_CAST(financials.cash_flow_statement.net_cash_flow_from_operating_activities.value AS DOUBLE) as cf_net_cash_flow_from_operating_activities,
            TRY_CAST(financials.cash_flow_statement.net_cash_flow_from_investing_activities.value AS DOUBLE) as cf_net_cash_flow_from_investing_activities,
            TRY_CAST(financials.cash_flow_statement.net_cash_flow_from_financing_activities.value AS DOUBLE) as cf_net_cash_flow_from_financing_activities,
            TRY_CAST(financials.cash_flow_statement.net_cash_flow_continuing.value AS DOUBLE) as cf_net_cash_flow_continuing,
            TRY_CAST(financials.cash_flow_statement.depreciation_and_amortization.value AS DOUBLE) as cf_depreciation_and_amortization
        FROM read_parquet('{str(cf_path)}/**/*.parquet', union_by_name=true)
        WHERE CAST(fiscal_year AS INTEGER) = {fiscal_year}
          AND fiscal_period = '{fiscal_period}'
    ),

    merged AS (
        SELECT
            COALESCE(bs.ticker, is_stmt.ticker, cf.ticker) as ticker,
            COALESCE(bs.filing_date, is_stmt.filing_date, cf.filing_date) as filing_date,
            COALESCE(bs.fiscal_year, is_stmt.fiscal_year, cf.fiscal_year) as fiscal_year,
            COALESCE(bs.fiscal_period, is_stmt.fiscal_period, cf.fiscal_period) as fiscal_period,

            -- Balance sheet columns
            bs.*,

            -- Income statement columns
            is_stmt.*,

            -- Cash flow columns
            cf.*

        FROM balance_sheet_flat bs
        FULL OUTER JOIN income_statement_flat is_stmt
            ON bs.ticker = is_stmt.ticker
            AND bs.filing_date = is_stmt.filing_date
            AND bs.fiscal_year = is_stmt.fiscal_year
            AND bs.fiscal_period = is_stmt.fiscal_period
        FULL OUTER JOIN cash_flow_flat cf
            ON COALESCE(bs.ticker, is_stmt.ticker) = cf.ticker
            AND COALESCE(bs.filing_date, is_stmt.filing_date) = cf.filing_date
            AND COALESCE(bs.fiscal_year, is_stmt.fiscal_year) = cf.fiscal_year
            AND COALESCE(bs.fiscal_period, is_stmt.fiscal_period) = cf.fiscal_period
    )

    SELECT
        *,

        -- Calculate financial ratios
        CASE WHEN bs_equity IS NOT NULL AND bs_equity != 0
             THEN is_net_income_loss / bs_equity
             ELSE NULL END as ratio_roe,

        CASE WHEN bs_assets IS NOT NULL AND bs_assets != 0
             THEN is_net_income_loss / bs_assets
             ELSE NULL END as ratio_roa,

        CASE WHEN is_revenues IS NOT NULL AND is_revenues != 0
             THEN is_net_income_loss / is_revenues
             ELSE NULL END as ratio_profit_margin,

        CASE WHEN is_revenues IS NOT NULL AND is_revenues != 0
             THEN is_operating_income_loss / is_revenues
             ELSE NULL END as ratio_operating_margin,

        CASE WHEN bs_current_liabilities IS NOT NULL AND bs_current_liabilities != 0
             THEN bs_current_assets / bs_current_liabilities
             ELSE NULL END as ratio_current,

        CASE WHEN bs_current_liabilities IS NOT NULL AND bs_current_liabilities != 0
             THEN (bs_current_assets - COALESCE(bs_inventory, 0)) / bs_current_liabilities
             ELSE NULL END as ratio_quick,

        CASE WHEN bs_equity IS NOT NULL AND bs_equity != 0
             THEN bs_liabilities / bs_equity
             ELSE NULL END as ratio_debt_to_equity,

        CASE WHEN bs_assets IS NOT NULL AND bs_assets != 0
             THEN bs_liabilities / bs_assets
             ELSE NULL END as ratio_debt_to_assets,

        -- Add metadata
        CAST('{datetime.now()}' AS TIMESTAMP) as processed_at

    FROM merged
    WHERE ticker IS NOT NULL
    ORDER BY ticker, filing_date
    """

    # Execute query and get result
    try:
        result_df = conn.execute(query).fetchdf()

        # Remove duplicate columns (ticker_1, filing_date_1, etc. from joins)
        cols_to_drop = [c for c in result_df.columns if c.endswith('_1') or c.endswith('_2')]
        if cols_to_drop:
            result_df = result_df.drop(columns=cols_to_drop)

        return result_df

    except Exception as e:
        logger.error(f"Error processing partition year={fiscal_year}, quarter={fiscal_period}: {e}")
        return None


if __name__ == '__main__':
    sys.exit(main())
