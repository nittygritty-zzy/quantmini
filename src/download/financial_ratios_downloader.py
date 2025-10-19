"""
Financial Ratios Downloader - Calculate ratios from fundamentals data

Reads balance sheets, income statements, and cash flow from partitioned structure,
calculates financial ratios using FinancialRatiosCalculator, and saves to partitioned structure.

Ratios include:
- Profitability (ROE, ROA, ROIC, margins)
- Liquidity (current ratio, quick ratio, cash ratio)
- Leverage (debt to equity, debt to assets)
- Efficiency (asset turnover, inventory turnover)
- Cash flow (free cash flow, OCF ratio)
- Growth (YoY/QoQ revenue/earnings growth)
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import logging

from ..features.financial_ratios import FinancialRatiosCalculator

logger = logging.getLogger(__name__)


class FinancialRatiosDownloader:
    """
    Calculate financial ratios from partitioned fundamentals data

    Reads from:
    - data/partitioned_screener/balance_sheets/
    - data/partitioned_screener/income_statements/
    - data/partitioned_screener/cash_flow/

    Saves to:
    - data/partitioned_screener/financial_ratios/year=YYYY/month=MM/ticker=SYMBOL.parquet
    """

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize financial ratios downloader

        Args:
            input_dir: Directory containing partitioned fundamentals data
            output_dir: Directory to save calculated ratios
            use_partitioned_structure: If True, save in partitioned structure
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.calculator = FinancialRatiosCalculator()

        logger.info(f"FinancialRatiosDownloader initialized (input: {input_dir}, output: {output_dir})")

    def _flatten_polygon_financials(self, df: pl.DataFrame, statement_type: str) -> pl.DataFrame:
        """
        Flatten Polygon's nested financials structure

        Polygon returns financials as nested structs like:
        financials.balance_sheet.assets.value

        We need to flatten this to just: assets

        Args:
            df: DataFrame with nested financials
            statement_type: balance_sheets, income_statements, or cash_flow

        Returns:
            Flattened DataFrame
        """
        if len(df) == 0:
            return df

        # Map statement type to Polygon's nested structure name
        statement_map = {
            'balance_sheets': 'balance_sheet',
            'income_statements': 'income_statement',
            'cash_flow': 'cash_flow_statement'
        }

        nested_name = statement_map.get(statement_type)
        if not nested_name:
            logger.warning(f"Unknown statement type: {statement_type}")
            return df

        # Extract ticker from tickers list
        if 'tickers' in df.columns:
            df = df.with_columns([
                pl.col('tickers').list.first().alias('ticker')
            ])

        # Keep metadata columns
        metadata_cols = ['ticker', 'fiscal_year', 'fiscal_period', 'filing_date', 'timeframe']
        metadata = df.select([col for col in metadata_cols if col in df.columns])

        # Unnest the financials struct
        if 'financials' not in df.columns:
            logger.warning("No 'financials' column found")
            return df

        # First level: unnest financials
        financials_df = df.select('financials').unnest('financials')

        # Second level: get the specific statement (balance_sheet, income_statement, etc.)
        if nested_name not in financials_df.columns:
            logger.warning(f"No '{nested_name}' found in financials")
            return df

        statement_df = financials_df.select(nested_name).unnest(nested_name)

        # Third level: extract .value from each field
        # Each field is a struct with {value, unit, label, order}
        flattened_cols = []
        for col_name in statement_df.columns:
            try:
                # Extract the .value field from the struct
                flattened_cols.append(
                    pl.col(col_name).struct.field('value').alias(col_name)
                )
            except Exception as e:
                logger.debug(f"Could not extract value from {col_name}: {e}")

        if flattened_cols:
            statement_df = statement_df.select(flattened_cols)

        # Combine metadata with flattened statement data
        result = pl.concat([metadata, statement_df], how="horizontal")

        logger.debug(f"Flattened {statement_type}: {len(result)} rows, {len(result.columns)} columns")

        return result

    def _read_partitioned_fundamentals(
        self,
        ticker: str,
        statement_type: str
    ) -> pl.DataFrame:
        """
        Read partitioned fundamentals data for a ticker

        Args:
            ticker: Ticker symbol
            statement_type: Type of statement (balance_sheets, income_statements, cash_flow)

        Returns:
            Polars DataFrame with statement data
        """
        statement_dir = self.input_dir / statement_type

        if not statement_dir.exists():
            logger.warning(f"Statement directory does not exist: {statement_dir}")
            return pl.DataFrame()

        # Find all parquet files for this ticker across all year/month partitions
        pattern = f"**/ticker={ticker.upper()}.parquet"
        files = list(statement_dir.glob(pattern))

        if not files:
            logger.warning(f"No {statement_type} data found for {ticker}")
            return pl.DataFrame()

        # Read and combine all files
        dfs = []
        for file in files:
            try:
                df = pl.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file}: {e}")

        if not dfs:
            return pl.DataFrame()

        # Combine all dataframes using diagonal_relaxed to handle schema variations
        # Polygon's financials struct can have different field counts across time periods
        combined = pl.concat(dfs, how="diagonal_relaxed")
        logger.info(f"Read {len(combined)} {statement_type} records for {ticker} from {len(files)} files")

        # Flatten Polygon's nested structure
        flattened = self._flatten_polygon_financials(combined, statement_type)

        return flattened

    def _save_partitioned_ratios(
        self,
        df: pl.DataFrame,
        ticker: str
    ) -> None:
        """
        Save ratios DataFrame in date-first partitioned structure

        Structure: output_dir/financial_ratios/year=YYYY/month=MM/ticker=SYMBOL.parquet

        Args:
            df: DataFrame with calculated ratios
            ticker: Ticker symbol
        """
        if len(df) == 0:
            return

        # Ensure ticker column exists
        if 'ticker' not in df.columns:
            df = df.with_columns(pl.lit(ticker.upper()).alias('ticker'))

        # Parse filing_date to extract year/month for partitioning
        if 'filing_date' not in df.columns:
            logger.warning(f"No filing_date column in ratios for {ticker}, cannot partition")
            # Save without partitioning
            output_file = self.output_dir / 'financial_ratios' / f'{ticker.upper()}.parquet'
            output_file.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(df)} ratio records to {output_file} (no partitioning)")
            return

        # Parse filing date and extract year/month
        if df.schema['filing_date'] == pl.String:
            df = df.with_columns([
                pl.col('filing_date').str.to_date("%Y-%m-%d").alias('_date_parsed')
            ])
        else:
            df = df.with_columns([
                pl.col('filing_date').cast(pl.Date).alias('_date_parsed')
            ])

        df = df.with_columns([
            pl.col('_date_parsed').dt.year().cast(pl.Int32).alias('year'),
            pl.col('_date_parsed').dt.month().cast(pl.Int32).alias('month'),
        ]).drop('_date_parsed')

        # Get unique year/month combinations
        partitions = df.select(['year', 'month']).unique()

        for row in partitions.iter_rows(named=True):
            year = row['year']
            month = row['month']

            # Filter for this partition
            partition_df = df.filter(
                (pl.col('year') == year) &
                (pl.col('month') == month)
            ).drop(['year', 'month'])

            # Create partition directory: year=2024/month=10/ticker=AAPL.parquet
            partition_dir = self.output_dir / 'financial_ratios' / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker.upper()}.parquet'

            # If file exists, append to it (diagonal concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal")

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} ratio records to {output_file}")

    async def calculate_ratios_for_ticker(
        self,
        ticker: str,
        include_growth: bool = True
    ) -> pl.DataFrame:
        """
        Calculate financial ratios for a single ticker

        Args:
            ticker: Ticker symbol
            include_growth: Include growth rate calculations

        Returns:
            Polars DataFrame with calculated ratios
        """
        logger.info(f"Calculating ratios for {ticker}")

        # Read fundamentals data
        balance_sheet = self._read_partitioned_fundamentals(ticker, 'balance_sheets')
        income_statement = self._read_partitioned_fundamentals(ticker, 'income_statements')
        cash_flow = self._read_partitioned_fundamentals(ticker, 'cash_flow')

        if len(balance_sheet) == 0 and len(income_statement) == 0 and len(cash_flow) == 0:
            logger.warning(f"No fundamentals data found for {ticker}")
            return pl.DataFrame()

        # Calculate ratios
        try:
            ratios_df = self.calculator.calculate_all_ratios(
                balance_sheet=balance_sheet,
                income_statement=income_statement,
                cash_flow=cash_flow
            )

            if len(ratios_df) == 0:
                logger.warning(f"No ratios calculated for {ticker}")
                return pl.DataFrame()

            # Add ticker column
            ratios_df = ratios_df.with_columns(pl.lit(ticker.upper()).alias('ticker'))

            # Add calculated_at timestamp
            ratios_df = ratios_df.with_columns(pl.lit(datetime.now()).alias('calculated_at'))

            # Calculate growth rates if requested
            # Note: Growth rates are calculated on the ratios themselves, not raw fundamentals
            # We skip this for now as it requires the merged dataset which is already in ratios_df
            # if include_growth:
            #     # Calculate YoY growth (4 quarters back)
            #     growth_df = self.calculator.calculate_growth_rates(ratios_df, periods=4)
            #     if len(growth_df) > 0:
            #         ratios_df = growth_df

            logger.info(f"Calculated {len(ratios_df)} ratio records for {ticker}")

            # Save to partitioned structure
            if self.use_partitioned_structure:
                self._save_partitioned_ratios(ratios_df, ticker)

            return ratios_df

        except Exception as e:
            logger.error(f"Failed to calculate ratios for {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return pl.DataFrame()

    async def calculate_ratios_batch(
        self,
        tickers: List[str],
        include_growth: bool = True
    ) -> Dict[str, pl.DataFrame]:
        """
        Calculate financial ratios for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            include_growth: Include growth rate calculations

        Returns:
            Dictionary mapping tickers to their ratio DataFrames
        """
        logger.info(f"Calculating ratios for {len(tickers)} tickers in parallel")

        # Calculate all in parallel
        tasks = [
            self.calculate_ratios_for_ticker(ticker, include_growth)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        ratios_by_ticker = {}
        total_ratios = 0

        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to calculate ratios for {ticker}: {result}")
                ratios_by_ticker[ticker] = pl.DataFrame()
            elif isinstance(result, pl.DataFrame):
                ratios_by_ticker[ticker] = result
                total_ratios += len(result)

        logger.info(f"Calculated {total_ratios} total ratio records for {len(tickers)} tickers")

        return ratios_by_ticker


async def main():
    """Example usage"""
    import sys

    try:
        # Create downloader
        downloader = FinancialRatiosDownloader(
            input_dir=Path('data/partitioned_screener'),
            output_dir=Path('data/partitioned_screener')
        )

        print("✅ FinancialRatiosDownloader initialized\n")

        # Test: Calculate ratios for AAPL
        print("📊 Calculating ratios for AAPL...")
        ratios = await downloader.calculate_ratios_for_ticker('AAPL', include_growth=True)

        if len(ratios) > 0:
            print(f"\n✅ Calculated {len(ratios)} ratio records")
            print("\nSample ratios:")
            print(ratios.select([
                'filing_date', 'ticker',
                'roe', 'roa', 'current_ratio', 'debt_to_equity',
                'gross_profit_margin', 'net_profit_margin'
            ]).head(5))
        else:
            print("❌ No ratios calculated")

        # Test: Batch calculation
        print("\n📊 Calculating ratios for multiple tickers...")
        batch_ratios = await downloader.calculate_ratios_batch(
            ['AAPL', 'MSFT', 'GOOGL'],
            include_growth=True
        )

        for ticker, df in batch_ratios.items():
            print(f"{ticker}: {len(df)} ratio records")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
