"""
Data Loader - Single entry point for loading all partitioned screener tables

Provides a unified interface to load:
- Fundamentals (balance_sheets, income_statements, cash_flow)
- Financial ratios
- Corporate actions
- Ticker events
- Reference data
- Economy data
- Any other partitioned data

All data is stored in Hive-style partitioned structure:
data/partitioned_screener/{table_name}/year=YYYY/month=MM/ticker=SYMBOL.parquet
"""

import polars as pl
from pathlib import Path
from typing import List, Optional, Dict, Union
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Unified data loader for all partitioned screener tables

    Usage:
        loader = DataLoader()

        # Load balance sheets for specific tickers
        bs = loader.load('balance_sheets', tickers=['AAPL', 'MSFT'])

        # Load financial ratios for a date range
        ratios = loader.load('financial_ratios',
                            start_date='2024-01-01',
                            end_date='2024-12-31')

        # Load all corporate actions
        actions = loader.load('corporate_actions')

        # List available tables
        tables = loader.list_tables()
    """

    def __init__(self, base_dir: Union[str, Path] = 'data/partitioned_screener'):
        """
        Initialize data loader

        Args:
            base_dir: Base directory containing partitioned tables
        """
        self.base_dir = Path(base_dir)
        if not self.base_dir.exists():
            logger.warning(f"Base directory does not exist: {self.base_dir}")

    def list_tables(self) -> List[str]:
        """
        List all available tables in the partitioned screener

        Returns:
            List of table names
        """
        if not self.base_dir.exists():
            return []

        tables = [
            d.name for d in self.base_dir.iterdir()
            if d.is_dir() and not d.name.startswith('.')
        ]

        return sorted(tables)

    def _parse_date(self, date_str: Union[str, date, datetime]) -> date:
        """Parse date string to date object"""
        if isinstance(date_str, date):
            return date_str
        if isinstance(date_str, datetime):
            return date_str.date()
        if isinstance(date_str, str):
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        raise ValueError(f"Invalid date format: {date_str}")

    def load(
        self,
        table_name: str,
        tickers: Optional[List[str]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[pl.Expr] = None
    ) -> pl.DataFrame:
        """
        Load data from a partitioned table

        Args:
            table_name: Name of the table (e.g., 'balance_sheets', 'financial_ratios')
            tickers: Optional list of ticker symbols to filter by
            start_date: Optional start date for filtering (format: 'YYYY-MM-DD')
            end_date: Optional end date for filtering (format: 'YYYY-MM-DD')
            columns: Optional list of columns to select
            filter_expr: Optional Polars expression for additional filtering

        Returns:
            Polars DataFrame with requested data

        Examples:
            # Load all balance sheets for AAPL
            bs = loader.load('balance_sheets', tickers=['AAPL'])

            # Load financial ratios for 2024
            ratios = loader.load('financial_ratios',
                                start_date='2024-01-01',
                                end_date='2024-12-31')

            # Load with custom filter
            data = loader.load('income_statements',
                             tickers=['AAPL', 'MSFT'],
                             filter_expr=pl.col('revenues') > 1e11)
        """
        table_dir = self.base_dir / table_name

        if not table_dir.exists():
            logger.error(f"Table directory does not exist: {table_dir}")
            return pl.DataFrame()

        # Build file pattern based on filters
        if tickers:
            # If specific tickers requested, find their files
            files = []
            for ticker in tickers:
                pattern = f"**/ticker={ticker.upper()}.parquet"
                ticker_files = list(table_dir.glob(pattern))
                files.extend(ticker_files)
        else:
            # Load all parquet files
            files = list(table_dir.glob("**/*.parquet"))

        if not files:
            logger.warning(f"No data found for table '{table_name}' with given filters")
            return pl.DataFrame()

        logger.info(f"Loading {len(files)} files from '{table_name}'")

        # Read all files
        dfs = []
        for file in files:
            try:
                df = pl.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file}: {e}")

        if not dfs:
            return pl.DataFrame()

        # Combine all dataframes using diagonal concat to handle schema differences
        # This is especially important for Polygon data which has inconsistent schemas
        try:
            combined = pl.concat(dfs, how="diagonal_relaxed")
        except Exception as e:
            logger.warning(f"Failed to concat with diagonal_relaxed, trying diagonal: {e}")
            try:
                combined = pl.concat(dfs, how="diagonal")
            except Exception as e2:
                logger.error(f"Failed to concat dataframes: {e2}")
                # Fall back to reading just the first file if concat fails
                if dfs:
                    combined = dfs[0]
                    logger.warning(f"Using only first file due to concat errors")
                else:
                    return pl.DataFrame()

        # Apply date filters if provided
        if start_date or end_date:
            # Determine date column name (try common variants)
            date_col = None
            for col in ['filing_date', 'date', 'timestamp', 'start_date']:
                if col in combined.columns:
                    date_col = col
                    break

            if date_col:
                # Ensure date column is Date type
                if combined.schema[date_col] == pl.String:
                    combined = combined.with_columns([
                        pl.col(date_col).str.to_date("%Y-%m-%d").alias(date_col)
                    ])

                if start_date:
                    start = self._parse_date(start_date)
                    combined = combined.filter(pl.col(date_col) >= start)

                if end_date:
                    end = self._parse_date(end_date)
                    combined = combined.filter(pl.col(date_col) <= end)
            else:
                logger.warning(f"No date column found for date filtering in table '{table_name}'")

        # Apply custom filter expression
        if filter_expr is not None:
            combined = combined.filter(filter_expr)

        # Select specific columns if requested
        if columns:
            available_cols = [col for col in columns if col in combined.columns]
            if len(available_cols) < len(columns):
                missing = set(columns) - set(available_cols)
                logger.warning(f"Columns not found: {missing}")
            if available_cols:
                combined = combined.select(available_cols)

        logger.info(f"Loaded {len(combined)} records from '{table_name}'")

        return combined

    def load_fundamentals(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        include_balance_sheet: bool = True,
        include_income_statement: bool = True,
        include_cash_flow: bool = True,
    ) -> Dict[str, pl.DataFrame]:
        """
        Load fundamentals data (balance sheet, income statement, cash flow)

        Args:
            tickers: Optional list of ticker symbols
            start_date: Optional start date
            end_date: Optional end date
            include_balance_sheet: Include balance sheet data
            include_income_statement: Include income statement data
            include_cash_flow: Include cash flow data

        Returns:
            Dictionary with keys 'balance_sheets', 'income_statements', 'cash_flow'
        """
        result = {}

        if include_balance_sheet:
            result['balance_sheets'] = self.load(
                'balance_sheets',
                tickers=tickers,
                start_date=start_date,
                end_date=end_date
            )

        if include_income_statement:
            result['income_statements'] = self.load(
                'income_statements',
                tickers=tickers,
                start_date=start_date,
                end_date=end_date
            )

        if include_cash_flow:
            result['cash_flow'] = self.load(
                'cash_flow',
                tickers=tickers,
                start_date=start_date,
                end_date=end_date
            )

        return result

    def load_financial_ratios(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        ratio_columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Load financial ratios

        Args:
            tickers: Optional list of ticker symbols
            start_date: Optional start date
            end_date: Optional end date
            ratio_columns: Optional list of specific ratio columns to load

        Returns:
            DataFrame with financial ratios
        """
        return self.load(
            'financial_ratios',
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            columns=ratio_columns
        )

    def load_corporate_actions(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        action_types: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Load corporate actions

        Args:
            tickers: Optional list of ticker symbols
            start_date: Optional start date
            end_date: Optional end date
            action_types: Optional list of action types (e.g., ['dividend', 'split'])

        Returns:
            DataFrame with corporate actions
        """
        df = self.load(
            'corporate_actions',
            tickers=tickers,
            start_date=start_date,
            end_date=end_date
        )

        # Filter by action types if specified
        if action_types and len(df) > 0 and 'type' in df.columns:
            df = df.filter(pl.col('type').is_in(action_types))

        return df

    def load_ticker_events(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        event_types: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Load ticker events (IPO, ticker changes, delistings)

        Args:
            tickers: Optional list of ticker symbols
            start_date: Optional start date
            end_date: Optional end date
            event_types: Optional list of event types

        Returns:
            DataFrame with ticker events
        """
        df = self.load(
            'ticker_events',
            tickers=tickers,
            start_date=start_date,
            end_date=end_date
        )

        # Filter by event types if specified
        if event_types and len(df) > 0 and 'event_type' in df.columns:
            df = df.filter(pl.col('event_type').is_in(event_types))

        return df

    def get_table_info(self, table_name: str) -> Dict:
        """
        Get information about a table

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table info (file count, size, date range, tickers)
        """
        table_dir = self.base_dir / table_name

        if not table_dir.exists():
            return {'error': f"Table '{table_name}' does not exist"}

        files = list(table_dir.glob("**/*.parquet"))

        if not files:
            return {
                'table_name': table_name,
                'file_count': 0,
                'total_size_mb': 0,
            }

        # Calculate total size
        total_size = sum(f.stat().st_size for f in files)

        # Extract tickers from file paths
        tickers = set()
        for f in files:
            if 'ticker=' in str(f):
                ticker = f.stem.replace('ticker=', '')
                tickers.add(ticker)

        # Get date range from partition directories
        years = set()
        months = set()
        for f in files:
            parts = f.parts
            for part in parts:
                if part.startswith('year='):
                    years.add(int(part.replace('year=', '')))
                if part.startswith('month='):
                    months.add(int(part.replace('month=', '')))

        return {
            'table_name': table_name,
            'file_count': len(files),
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'ticker_count': len(tickers),
            'tickers': sorted(tickers),
            'year_range': (min(years), max(years)) if years else None,
        }


# Convenience function for quick loading
def load_table(
    table_name: str,
    tickers: Optional[List[str]] = None,
    start_date: Optional[Union[str, date, datetime]] = None,
    end_date: Optional[Union[str, date, datetime]] = None,
    base_dir: Union[str, Path] = 'data/partitioned_screener'
) -> pl.DataFrame:
    """
    Convenience function to quickly load a table

    Args:
        table_name: Name of the table
        tickers: Optional list of tickers
        start_date: Optional start date
        end_date: Optional end date
        base_dir: Base directory

    Returns:
        Polars DataFrame

    Example:
        # Quick load balance sheets for AAPL
        bs = load_table('balance_sheets', tickers=['AAPL'])
    """
    loader = DataLoader(base_dir)
    return loader.load(table_name, tickers=tickers, start_date=start_date, end_date=end_date)


def main():
    """Example usage"""
    loader = DataLoader()

    print("📊 QuantMini Data Loader\n")

    # List available tables
    tables = loader.list_tables()
    print(f"Available tables ({len(tables)}):")
    for table in tables:
        info = loader.get_table_info(table)
        print(f"  - {table}: {info.get('file_count', 0)} files, "
              f"{info.get('ticker_count', 0)} tickers, "
              f"{info.get('total_size_mb', 0)} MB")

    print("\n" + "="*60 + "\n")

    # Example: Load financial ratios for AAPL
    print("Example 1: Load financial ratios for AAPL")
    ratios = loader.load_financial_ratios(tickers=['AAPL'])
    if len(ratios) > 0:
        print(f"Loaded {len(ratios)} ratio records")
        print("\nSample ratios:")
        print(ratios.select([
            'ticker', 'filing_date', 'fiscal_period',
            'return_on_equity', 'return_on_assets',
            'current_ratio', 'debt_to_equity'
        ]).head(5))

    print("\n" + "="*60 + "\n")

    # Example: Load fundamentals
    print("Example 2: Load fundamentals for AAPL and MSFT")
    fundamentals = loader.load_fundamentals(tickers=['AAPL', 'MSFT'])
    for name, df in fundamentals.items():
        print(f"{name}: {len(df)} records")

    print("\n" + "="*60 + "\n")

    # Example: Load corporate actions
    print("Example 3: Load corporate actions for 2024")
    actions = loader.load_corporate_actions(
        start_date='2024-01-01',
        end_date='2024-12-31'
    )
    print(f"Loaded {len(actions)} corporate actions for 2024")

    print("\n" + "="*60 + "\n")

    # Example: Custom filter
    print("Example 4: Load high-ROE companies from financial ratios")
    ratios = loader.load(
        'financial_ratios',
        filter_expr=pl.col('return_on_equity') > 50  # ROE > 50%
    )
    if len(ratios) > 0:
        print(f"Found {len(ratios)} quarters with ROE > 50%")
        print("\nTop ROE quarters:")
        print(ratios.select(['ticker', 'filing_date', 'fiscal_period', 'return_on_equity', 'return_on_assets'])
              .sort('return_on_equity', descending=True)
              .head(5))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
