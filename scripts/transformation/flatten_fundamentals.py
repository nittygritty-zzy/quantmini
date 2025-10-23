#!/usr/bin/env python3
"""
Phase 2: Flatten Fundamentals from Bronze to Silver Layer

This script processes nested bronze layer fundamentals and creates a denormalized
silver layer table optimized for screening and analysis.

Steps:
1. Load nested fundamentals (balance_sheets, income_statements, cash_flow)
2. Unnest Polars Struct format (extract .value from each metric)
3. Merge all three statement types per ticker-quarter
4. Add prefixes (bs_, is_, cf_) to avoid column name collisions
5. Calculate financial ratios (profitability, liquidity, leverage, efficiency)
6. Calculate growth rates (YoY, QoQ)
7. Calculate quality scores (Piotroski F-Score, Altman Z-Score)
8. Save to silver layer partitioned by fiscal_year/fiscal_quarter

Usage:
    python scripts/transformation/flatten_fundamentals.py

    # For testing with sample:
    python scripts/transformation/flatten_fundamentals.py --sample-tickers 10

Output:
    $QUANTLAKE_ROOT/silver/fundamentals_wide/
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging
import argparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
from src.utils.paths import get_quantlake_root

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalsFlattener:
    """Flatten nested fundamentals from bronze to silver layer"""

    def __init__(
        self,
        bronze_path: Path,
        silver_path: Path,
        schema_path: Path
    ):
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.schema_path = Path(schema_path)

        # Load schema configuration
        with open(self.schema_path, 'r') as f:
            self.schema_config = json.load(f)

        logger.info(f"Bronze path: {self.bronze_path}")
        logger.info(f"Silver path: {self.silver_path}")
        logger.info(f"Schema config: {self.schema_path}")

    def process_all_fundamentals(self, sample_tickers: Optional[int] = None):
        """
        Process all fundamentals from bronze to silver

        Args:
            sample_tickers: If set, only process this many tickers (for testing)
        """
        logger.info("="*80)
        logger.info("FUNDAMENTALS FLATTENING - Phase 2")
        logger.info("="*80)
        logger.info("")

        # Get all unique tickers from balance sheets directory
        bs_dir = self.bronze_path / "balance_sheets"
        all_files = list(bs_dir.rglob("*.parquet"))

        # Extract unique tickers
        tickers = sorted(set(f.stem.replace('ticker=', '') for f in all_files))

        if sample_tickers:
            tickers = tickers[:sample_tickers]
            logger.info(f"Processing sample of {len(tickers)} tickers (testing mode)")
        else:
            logger.info(f"Processing all {len(tickers)} tickers (production mode)")

        logger.info("")

        # Process each ticker
        all_flattened_data = []

        for i, ticker in enumerate(tickers, 1):
            try:
                logger.info(f"[{i}/{len(tickers)}] Processing {ticker}...")

                # Flatten this ticker's fundamentals
                ticker_data = self._flatten_ticker(ticker)

                if ticker_data is not None and len(ticker_data) > 0:
                    all_flattened_data.append(ticker_data)
                    logger.info(f"  ✓ {ticker}: {len(ticker_data)} quarters processed")
                else:
                    logger.warning(f"  ⚠ {ticker}: No data found")

            except Exception as e:
                logger.error(f"  ✗ {ticker}: Failed - {e}")
                continue

        # Combine all ticker data
        if not all_flattened_data:
            logger.error("No data processed!")
            return

        logger.info("")
        logger.info("="*80)
        logger.info("COMBINING ALL TICKER DATA")
        logger.info("="*80)

        # Standardize dtypes before concatenating
        # Convert all Int64 columns to Float64 for consistency
        standardized_data = []
        for df in all_flattened_data:
            # Cast all integer columns to Float64
            cast_exprs = []
            for col, dtype in zip(df.columns, df.dtypes):
                if dtype in [pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8]:
                    cast_exprs.append(pl.col(col).cast(pl.Float64))
                else:
                    cast_exprs.append(pl.col(col))

            if cast_exprs:
                df = df.select(cast_exprs)

            standardized_data.append(df)

        # Use diagonal concat to handle different schemas across tickers
        combined_df = pl.concat(standardized_data, how="diagonal")

        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Total columns: {len(combined_df.columns)}")
        logger.info(f"Date range: {combined_df['filing_date'].min()} to {combined_df['filing_date'].max()}")

        # Calculate derived metrics
        logger.info("")
        logger.info("Calculating financial ratios...")
        combined_df = self._calculate_financial_ratios(combined_df)

        logger.info("Calculating growth rates...")
        combined_df = self._calculate_growth_rates(combined_df)

        logger.info("Calculating quality scores...")
        combined_df = self._calculate_quality_scores(combined_df)

        # Save to silver layer
        self._save_to_silver(combined_df)

        logger.info("")
        logger.info("="*80)
        logger.info("✓ PHASE 2 COMPLETE - Fundamentals Flattening Successful")
        logger.info("="*80)

    def _flatten_ticker(self, ticker: str) -> Optional[pl.DataFrame]:
        """
        Flatten all fundamentals for a single ticker

        Returns:
            DataFrame with one row per quarter, all metrics flattened
        """
        # Load all three statement types
        balance_sheets = self._load_statements(ticker, 'balance_sheets')
        income_statements = self._load_statements(ticker, 'income_statements')
        cash_flow = self._load_statements(ticker, 'cash_flow')

        # If no data found, return None
        if balance_sheets is None and income_statements is None and cash_flow is None:
            return None

        # Unnest each statement type
        bs_flat = self._unnest_financials(balance_sheets, 'bs') if balance_sheets is not None else None
        is_flat = self._unnest_financials(income_statements, 'is') if income_statements is not None else None
        cf_flat = self._unnest_financials(cash_flow, 'cf') if cash_flow is not None else None

        # DEBUG: Log what columns we have
        if bs_flat is not None:
            logger.debug(f"  BS columns ({len(bs_flat.columns)}): {bs_flat.columns[:10]}")
        if is_flat is not None:
            logger.debug(f"  IS columns ({len(is_flat.columns)}): {is_flat.columns[:10]}")
        if cf_flat is not None:
            logger.debug(f"  CF columns ({len(cf_flat.columns)}): {cf_flat.columns[:10]}")

        # Merge on ticker + filing_date + fiscal_year + fiscal_period
        key_cols = ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period']

        # Start with the first available statement
        merged = bs_flat if bs_flat is not None else (is_flat if is_flat is not None else cf_flat)

        # Helper function to get non-key metric columns
        def get_metric_cols(df, key_cols):
            """Get all columns that are not key columns"""
            all_key_cols = [c for c in key_cols if c in df.columns]
            return [c for c in df.columns if c not in all_key_cols]

        # Join income statement if available
        if is_flat is not None and merged is not is_flat:
            # Join and then drop duplicate '_right' columns
            metric_cols = get_metric_cols(is_flat, key_cols)
            is_to_join = is_flat.select(key_cols + metric_cols)
            merged = merged.join(is_to_join, on=key_cols, how='full', suffix='_is_dup')

            # Drop duplicate columns with '_is_dup' suffix
            dup_cols = [c for c in merged.columns if c.endswith('_is_dup')]
            if dup_cols:
                merged = merged.drop(dup_cols)

        # Join cash flow if available
        if cf_flat is not None and merged is not cf_flat:
            # Join and then drop duplicate '_right' columns
            metric_cols = get_metric_cols(cf_flat, key_cols)
            cf_to_join = cf_flat.select(key_cols + metric_cols)
            merged = merged.join(cf_to_join, on=key_cols, how='full', suffix='_cf_dup')

            # Drop duplicate columns with '_cf_dup' suffix
            dup_cols = [c for c in merged.columns if c.endswith('_cf_dup')]
            if dup_cols:
                merged = merged.drop(dup_cols)

        return merged

    def _load_statements(self, ticker: str, statement_type: str) -> Optional[pl.DataFrame]:
        """
        Load all statements of a given type for a ticker

        Args:
            ticker: Stock ticker symbol
            statement_type: One of 'balance_sheets', 'income_statements', 'cash_flow'

        Returns:
            Combined DataFrame or None if no files found
        """
        stmt_dir = self.bronze_path / statement_type

        # Find all files for this ticker (across all year/month partitions)
        ticker_files = list(stmt_dir.rglob(f"ticker={ticker}.parquet"))

        if not ticker_files:
            return None

        # Load and combine all files
        dfs = []
        for file_path in ticker_files:
            try:
                df = pl.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"  Failed to read {file_path}: {e}")
                continue

        if not dfs:
            return None

        combined = pl.concat(dfs, how="vertical_relaxed")

        # Handle ticker column - bronze files have 'tickers' (List) not 'ticker' (String)
        if 'ticker' not in combined.columns:
            if 'tickers' in combined.columns:
                # Extract first ticker from the list
                combined = combined.with_columns(
                    pl.col('tickers').list.first().alias('ticker')
                )
            else:
                # Fallback: use the ticker parameter
                combined = combined.with_columns(pl.lit(ticker).alias('ticker'))

        return combined

    def _unnest_financials(self, df: pl.DataFrame, prefix: str) -> pl.DataFrame:
        """
        Unnest the nested financials struct and extract .value fields

        Args:
            df: DataFrame with nested 'financials' column
            prefix: Prefix for column names (bs, is, cf)

        Returns:
            Flattened DataFrame with extracted metrics
        """
        # Check if financials column exists
        if 'financials' not in df.columns:
            logger.warning(f"No 'financials' column found for prefix {prefix}")
            return df

        # Start with ONLY the join key columns - nothing else
        # This ensures we don't get duplicate '_right' columns during joins
        key_cols = ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period']

        # Select only the key columns to start
        result_df = df.select(key_cols)

        # Determine which financial statement to extract based on prefix
        statement_map = {
            'bs': 'balance_sheet',
            'is': 'income_statement',
            'cf': 'cash_flow_statement'
        }

        statement_key = statement_map.get(prefix)
        if not statement_key:
            logger.warning(f"Unknown prefix: {prefix}")
            return result_df

        # Extract nested struct fields
        # The structure is: financials.balance_sheet.assets.value
        try:
            # Access the nested struct
            financials_col = df['financials']

            # Check if this is a struct type
            if not isinstance(financials_col.dtype, pl.Struct):
                logger.warning(f"'financials' is not a Struct type: {financials_col.dtype}")
                return result_df

            # Get the statement-level struct (balance_sheet, income_statement, etc.)
            if statement_key not in financials_col.struct.fields:
                logger.warning(f"'{statement_key}' not found in financials struct")
                return result_df

            statement_struct = financials_col.struct.field(statement_key)

            # Check if statement_struct is also a Struct
            if not isinstance(statement_struct.dtype, pl.Struct):
                logger.warning(f"'{statement_key}' is not a Struct type")
                return result_df

            # Get all metric fields in this statement
            metric_fields = statement_struct.struct.fields

            # Extract .value from each metric
            for metric_name in metric_fields:
                try:
                    # Access metric.value
                    metric_struct = statement_struct.struct.field(metric_name)

                    # Check if this metric has a 'value' field
                    if isinstance(metric_struct.dtype, pl.Struct) and 'value' in metric_struct.struct.fields:
                        value_col = metric_struct.struct.field('value')

                        # Add to result with prefix
                        col_name = f"{prefix}_{metric_name}"
                        result_df = result_df.with_columns(value_col.alias(col_name))

                except Exception as e:
                    logger.debug(f"  Could not extract {metric_name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to unnest financials for {prefix}: {e}")
            return result_df

        return result_df

    def _calculate_financial_ratios(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate key financial ratios

        Ratios to calculate:
        - Profitability: ROE, ROA, Profit Margin, Operating Margin
        - Liquidity: Current Ratio, Quick Ratio, Cash Ratio
        - Leverage: Debt-to-Equity, Debt-to-Assets, Interest Coverage
        - Efficiency: Asset Turnover, Inventory Turnover
        """
        result = df

        # Profitability Ratios
        # ROE = Net Income / Shareholders Equity
        if 'is_net_income_loss' in df.columns and 'bs_equity' in df.columns:
            result = result.with_columns(
                (pl.col('is_net_income_loss') / pl.col('bs_equity')).alias('ratio_roe')
            )

        # ROA = Net Income / Total Assets
        if 'is_net_income_loss' in df.columns and 'bs_assets' in df.columns:
            result = result.with_columns(
                (pl.col('is_net_income_loss') / pl.col('bs_assets')).alias('ratio_roa')
            )

        # Profit Margin = Net Income / Revenues
        if 'is_net_income_loss' in df.columns and 'is_revenues' in df.columns:
            result = result.with_columns(
                (pl.col('is_net_income_loss') / pl.col('is_revenues')).alias('ratio_profit_margin')
            )

        # Operating Margin = Operating Income / Revenues
        if 'is_operating_income_loss' in df.columns and 'is_revenues' in df.columns:
            result = result.with_columns(
                (pl.col('is_operating_income_loss') / pl.col('is_revenues')).alias('ratio_operating_margin')
            )

        # Liquidity Ratios
        # Current Ratio = Current Assets / Current Liabilities
        if 'bs_current_assets' in df.columns and 'bs_current_liabilities' in df.columns:
            result = result.with_columns(
                (pl.col('bs_current_assets') / pl.col('bs_current_liabilities')).alias('ratio_current')
            )

        # Quick Ratio = (Current Assets - Inventory) / Current Liabilities
        if all(col in df.columns for col in ['bs_current_assets', 'bs_inventory', 'bs_current_liabilities']):
            result = result.with_columns(
                ((pl.col('bs_current_assets') - pl.col('bs_inventory')) / pl.col('bs_current_liabilities')).alias('ratio_quick')
            )

        # Leverage Ratios
        # Debt-to-Equity = Total Liabilities / Shareholders Equity
        if 'bs_liabilities' in df.columns and 'bs_equity' in df.columns:
            result = result.with_columns(
                (pl.col('bs_liabilities') / pl.col('bs_equity')).alias('ratio_debt_to_equity')
            )

        # Debt-to-Assets = Total Liabilities / Total Assets
        if 'bs_liabilities' in df.columns and 'bs_assets' in df.columns:
            result = result.with_columns(
                (pl.col('bs_liabilities') / pl.col('bs_assets')).alias('ratio_debt_to_assets')
            )

        logger.info(f"  Added {len([c for c in result.columns if c.startswith('ratio_')])} financial ratios")

        return result

    def _calculate_growth_rates(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate YoY and QoQ growth rates for key metrics

        Growth metrics:
        - Revenue growth (YoY, QoQ)
        - Net income growth (YoY, QoQ)
        - EPS growth (YoY, QoQ)
        - Asset growth (YoY)
        """
        result = df.sort(['ticker', 'filing_date'])

        # YoY Revenue Growth (compare to 4 quarters ago)
        if 'is_revenues' in df.columns:
            result = result.with_columns(
                ((pl.col('is_revenues') - pl.col('is_revenues').shift(4).over('ticker')) /
                 pl.col('is_revenues').shift(4).over('ticker')).alias('growth_revenue_yoy')
            )

        # QoQ Revenue Growth
        if 'is_revenues' in df.columns:
            result = result.with_columns(
                ((pl.col('is_revenues') - pl.col('is_revenues').shift(1).over('ticker')) /
                 pl.col('is_revenues').shift(1).over('ticker')).alias('growth_revenue_qoq')
            )

        # YoY Net Income Growth
        if 'is_net_income_loss' in df.columns:
            result = result.with_columns(
                ((pl.col('is_net_income_loss') - pl.col('is_net_income_loss').shift(4).over('ticker')) /
                 pl.col('is_net_income_loss').shift(4).over('ticker').abs()).alias('growth_net_income_yoy')
            )

        # YoY Asset Growth
        if 'bs_assets' in df.columns:
            result = result.with_columns(
                ((pl.col('bs_assets') - pl.col('bs_assets').shift(4).over('ticker')) /
                 pl.col('bs_assets').shift(4).over('ticker')).alias('growth_assets_yoy')
            )

        logger.info(f"  Added {len([c for c in result.columns if c.startswith('growth_')])} growth metrics")

        return result

    def _calculate_quality_scores(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate quality scores (simplified versions)

        Quality metrics:
        - Piotroski F-Score (0-9)
        - Altman Z-Score
        - Has complete data flag
        """
        result = df

        # Piotroski F-Score (simplified - checking for positive values)
        f_score_components = []

        # 1. Positive net income
        if 'is_net_income_loss' in df.columns:
            f_score_components.append((pl.col('is_net_income_loss') > 0).cast(pl.Int32))

        # 2. Positive operating cash flow
        if 'cf_net_cash_flow_from_operating_activities' in df.columns:
            f_score_components.append((pl.col('cf_net_cash_flow_from_operating_activities') > 0).cast(pl.Int32))

        # 3. Increasing ROA
        if 'ratio_roa' in df.columns:
            f_score_components.append(
                (pl.col('ratio_roa') > pl.col('ratio_roa').shift(1).over('ticker')).cast(pl.Int32)
            )

        # 4. Decreasing leverage
        if 'ratio_debt_to_assets' in df.columns:
            f_score_components.append(
                (pl.col('ratio_debt_to_assets') < pl.col('ratio_debt_to_assets').shift(1).over('ticker')).cast(pl.Int32)
            )

        # 5. Increasing current ratio
        if 'ratio_current' in df.columns:
            f_score_components.append(
                (pl.col('ratio_current') > pl.col('ratio_current').shift(1).over('ticker')).cast(pl.Int32)
            )

        if f_score_components:
            result = result.with_columns(
                pl.sum_horizontal(f_score_components).alias('quality_piotroski_f_score')
            )

        # Has complete data (all key fields non-null)
        key_fields = ['is_revenues', 'is_net_income_loss', 'bs_assets', 'bs_liabilities', 'bs_equity']
        available_key_fields = [f for f in key_fields if f in df.columns]

        if available_key_fields:
            null_checks = [pl.col(f).is_not_null() for f in available_key_fields]
            result = result.with_columns(
                pl.all_horizontal(null_checks).alias('has_complete_data')
            )

        logger.info(f"  Added {len([c for c in result.columns if c.startswith('quality_')])} quality scores")

        return result

    def _save_to_silver(self, df: pl.DataFrame):
        """
        Save flattened fundamentals to silver layer
        Partitioned by fiscal_year and fiscal_quarter
        """
        logger.info("")
        logger.info("="*80)
        logger.info("SAVING TO SILVER LAYER")
        logger.info("="*80)

        output_dir = self.silver_path / "fundamentals_wide"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Add processed_at timestamp
        df = df.with_columns(
            pl.lit(datetime.now()).alias('processed_at')
        )

        # Create fiscal_quarter column if not present
        if 'fiscal_quarter' not in df.columns:
            # Derive from fiscal_period (Q1, Q2, Q3, Q4)
            df = df.with_columns(
                pl.col('fiscal_period').alias('fiscal_quarter')
            )

        # Group by year/quarter and save partitioned files
        for (year, quarter), group_df in df.group_by(['fiscal_year', 'fiscal_quarter']):
            partition_dir = output_dir / f"year={year}" / f"quarter={quarter}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / "data.parquet"
            group_df.write_parquet(
                output_file,
                compression='zstd',
                compression_level=3
            )

            logger.info(f"  Saved: year={year}, quarter={quarter} ({len(group_df):,} records)")

        logger.info("")
        logger.info(f"✓ Silver layer saved to: {output_dir}")
        logger.info(f"  Total records: {len(df):,}")
        logger.info(f"  Total columns: {len(df.columns)}")
        logger.info(f"  File format: Parquet (ZSTD compression)")
        logger.info(f"  Partitioning: fiscal_year / fiscal_quarter")


def main():
    """Main entry point"""

    parser = argparse.ArgumentParser(description='Flatten fundamentals from bronze to silver layer')
    parser.add_argument(
        '--sample-tickers',
        type=int,
        default=None,
        help='Number of tickers to process (for testing). Omit for all tickers.'
    )

    args = parser.parse_args()

    # Configuration (using centralized path function)
    quantlake_root = get_quantlake_root()
    bronze_path = quantlake_root / 'fundamentals'
    silver_path = quantlake_root / 'silver'
    schema_path = Path(__file__).parent.parent.parent / 'config' / 'fundamentals_schema.json'

    # Initialize flattener
    flattener = FundamentalsFlattener(
        bronze_path=bronze_path,
        silver_path=silver_path,
        schema_path=schema_path
    )

    # Process fundamentals
    flattener.process_all_fundamentals(sample_tickers=args.sample_tickers)


if __name__ == '__main__':
    main()
