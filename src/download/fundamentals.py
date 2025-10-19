"""
Fundamentals Downloader - Financial statements and metrics

High-performance downloader for Polygon fundamentals data.

Downloads:
- Balance sheets
- Cash flow statements
- Income statements
- Short interest
- Short volume

Note: Financial ratios are computed separately using FinancialRatiosCalculator
in src/features/financial_ratios.py
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient, format_date

logger = logging.getLogger(__name__)


class FundamentalsDownloader:
    """
    High-performance fundamentals downloader

    Optimized for unlimited API rate with parallel requests
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize fundamentals downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
            use_partitioned_structure: If True, save in date-first partitioned structure
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"FundamentalsDownloader initialized (output: {output_dir}, partitioned: {use_partitioned_structure})")

    def _save_partitioned(
        self,
        df: pl.DataFrame,
        statement_type: str,
        ticker: str
    ) -> None:
        """
        Save DataFrame in date-first partitioned structure.

        Structure: output_dir/{statement_type}/year=YYYY/month=MM/ticker=SYMBOL.parquet

        Args:
            df: DataFrame to save
            statement_type: Type of statement (balance_sheets, cash_flow, income_statements)
            ticker: Ticker symbol
        """
        if len(df) == 0:
            return

        # Extract ticker from tickers list column
        if 'tickers' in df.columns:
            if df.schema['tickers'] == pl.List(pl.String):
                df = df.with_columns([
                    pl.col('tickers').list.first().alias('ticker_extracted')
                ])
            else:
                df = df.with_columns([
                    pl.col('tickers').alias('ticker_extracted')
                ])
        else:
            df = df.with_columns([
                pl.lit(ticker.upper()).alias('ticker_extracted')
            ])

        # Filter out null tickers and dates
        df = df.filter(
            pl.col('ticker_extracted').is_not_null() &
            pl.col('filing_date').is_not_null()
        )

        if len(df) == 0:
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

        # Get unique year/month/ticker combinations
        partitions = df.select(['year', 'month', 'ticker_extracted']).unique()

        for row in partitions.iter_rows(named=True):
            year = row['year']
            month = row['month']
            ticker_name = row['ticker_extracted']

            # Filter for this partition
            partition_df = df.filter(
                (pl.col('year') == year) &
                (pl.col('month') == month) &
                (pl.col('ticker_extracted') == ticker_name)
            ).drop(['ticker_extracted', 'year', 'month'])

            # Create partition directory: year=2024/month=10/ticker=AAPL.parquet
            partition_dir = self.output_dir / statement_type / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker_name}.parquet'

            # If file exists, append to it (diagonal_relaxed concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} records to {output_file}")

    def _save_partitioned_short_data(
        self,
        df: pl.DataFrame,
        data_type: str,
        date_column: str
    ) -> None:
        """
        Save short data DataFrame in date-first partitioned structure.

        Structure: output_dir/{data_type}/year=YYYY/month=MM/ticker=SYMBOL.parquet

        Args:
            df: DataFrame to save
            data_type: Type of data (short_interest, short_volume)
            date_column: Column name for date partitioning (settlement_date or date)
        """
        if len(df) == 0:
            return

        # Ensure ticker column exists
        if 'ticker' not in df.columns:
            logger.warning(f"No 'ticker' column in {data_type}, skipping partitioned save")
            return

        # Filter out null tickers and dates
        df = df.filter(
            pl.col('ticker').is_not_null() &
            pl.col(date_column).is_not_null()
        )

        if len(df) == 0:
            return

        # Parse date and extract year/month
        if df.schema[date_column] == pl.String:
            df = df.with_columns([
                pl.col(date_column).str.to_date("%Y-%m-%d").alias('_date_parsed')
            ])
        else:
            df = df.with_columns([
                pl.col(date_column).cast(pl.Date).alias('_date_parsed')
            ])

        df = df.with_columns([
            pl.col('_date_parsed').dt.year().cast(pl.Int32).alias('year'),
            pl.col('_date_parsed').dt.month().cast(pl.Int32).alias('month'),
        ]).drop('_date_parsed')

        # Get unique year/month/ticker combinations
        partitions = df.select(['year', 'month', 'ticker']).unique()

        for row in partitions.iter_rows(named=True):
            year = row['year']
            month = row['month']
            ticker = row['ticker']

            # Filter for this partition
            partition_df = df.filter(
                (pl.col('year') == year) &
                (pl.col('month') == month) &
                (pl.col('ticker') == ticker)
            ).drop(['year', 'month'])

            # Create partition directory: year=2024/month=10/ticker=AAPL.parquet
            partition_dir = self.output_dir / data_type / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker}.parquet'

            # If file exists, append to it (diagonal_relaxed concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} records to {output_file}")

    async def download_balance_sheets(
        self,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        company_name: Optional[str] = None,
        filing_date: Optional[str] = None,
        period_of_report_date: Optional[str] = None,
        timeframe: Optional[str] = None,
        include_sources: bool = False,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download balance sheets

        Args:
            ticker: Ticker symbol
            cik: CIK number
            company_name: Company name
            filing_date: Filing date (YYYY-MM-DD)
            period_of_report_date: Period of report date
            timeframe: annual or quarterly
            include_sources: Include source data
            limit: Results per page

        Returns:
            Polars DataFrame with balance sheet data
        """
        logger.info(f"Downloading balance sheets (ticker={ticker})")

        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker.upper()
        if cik:
            params['cik'] = cik
        if company_name:
            params['company_name'] = company_name
        if filing_date:
            params['filing_date'] = filing_date
        if period_of_report_date:
            params['period_of_report_date'] = period_of_report_date
        if timeframe:
            params['timeframe'] = timeframe
        if include_sources:
            params['include_sources'] = 'true'

        # Fetch all pages
        results = await self.client.paginate_all('/vX/reference/financials', params)

        if not results:
            logger.warning("No balance sheets found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} balance sheet records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'balance_sheets', ticker or 'UNKNOWN')
        else:
            output_file = self.output_dir / f"balance_sheets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_cash_flow_statements(
        self,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        filing_date: Optional[str] = None,
        period_of_report_date: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download cash flow statements

        Args:
            ticker: Ticker symbol
            cik: CIK number
            filing_date: Filing date
            period_of_report_date: Period of report date
            timeframe: annual or quarterly
            limit: Results per page

        Returns:
            Polars DataFrame with cash flow data
        """
        logger.info(f"Downloading cash flow statements (ticker={ticker})")

        params = {'limit': limit, 'financial_statement_type': 'cash_flow_statement'}
        if ticker:
            params['ticker'] = ticker.upper()
        if cik:
            params['cik'] = cik
        if filing_date:
            params['filing_date'] = filing_date
        if period_of_report_date:
            params['period_of_report_date'] = period_of_report_date
        if timeframe:
            params['timeframe'] = timeframe

        # Fetch all pages
        results = await self.client.paginate_all('/vX/reference/financials', params)

        if not results:
            logger.warning("No cash flow statements found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} cash flow records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'cash_flow', ticker or 'UNKNOWN')
        else:
            output_file = self.output_dir / f"cash_flow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_income_statements(
        self,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        filing_date: Optional[str] = None,
        period_of_report_date: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download income statements

        Args:
            ticker: Ticker symbol
            cik: CIK number
            filing_date: Filing date
            period_of_report_date: Period of report date
            timeframe: annual or quarterly
            limit: Results per page

        Returns:
            Polars DataFrame with income statement data
        """
        logger.info(f"Downloading income statements (ticker={ticker})")

        params = {'limit': limit, 'financial_statement_type': 'income_statement'}
        if ticker:
            params['ticker'] = ticker.upper()
        if cik:
            params['cik'] = cik
        if filing_date:
            params['filing_date'] = filing_date
        if period_of_report_date:
            params['period_of_report_date'] = period_of_report_date
        if timeframe:
            params['timeframe'] = timeframe

        # Fetch all pages
        results = await self.client.paginate_all('/vX/reference/financials', params)

        if not results:
            logger.warning("No income statements found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} income statement records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'income_statements', ticker or 'UNKNOWN')
        else:
            output_file = self.output_dir / f"income_statements_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_all_financials(
        self,
        ticker: str,
        timeframe: str = 'quarterly',
        limit: int = 100
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all financial statements in parallel

        Args:
            ticker: Ticker symbol
            timeframe: annual or quarterly
            limit: Results per page

        Returns:
            Dictionary with DataFrames for each statement type
        """
        logger.info(f"Downloading all financials for {ticker} ({timeframe})")

        # Download all in parallel
        results = await asyncio.gather(
            self.download_balance_sheets(ticker=ticker, timeframe=timeframe, limit=limit),
            self.download_cash_flow_statements(ticker=ticker, timeframe=timeframe, limit=limit),
            self.download_income_statements(ticker=ticker, timeframe=timeframe, limit=limit),
            return_exceptions=True
        )

        # Process results
        data = {}
        statement_types = ['balance_sheets', 'cash_flow', 'income_statements']

        for stmt_type, result in zip(statement_types, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {stmt_type}: {result}")
                data[stmt_type] = pl.DataFrame()
            else:
                data[stmt_type] = result

        logger.info(
            f"Downloaded all financials for {ticker}: "
            f"{len(data['balance_sheets'])} balance sheets, "
            f"{len(data['cash_flow'])} cash flow statements, "
            f"{len(data['income_statements'])} income statements"
        )

        return data

    async def download_financials_batch(
        self,
        tickers: List[str],
        timeframe: str = 'quarterly'
    ) -> Dict[str, int]:
        """
        Download financials for multiple tickers in parallel

        Note: Files are saved separately per ticker to preserve nested struct schemas.
        This avoids the issue of combining structs with different field counts.

        Args:
            tickers: List of ticker symbols
            timeframe: annual or quarterly

        Returns:
            Dictionary with counts for each statement type
        """
        logger.info(f"Downloading financials for {len(tickers)} tickers in parallel")

        # Download all tickers in parallel (files are saved automatically per ticker)
        tasks = [
            self.download_all_financials(ticker, timeframe)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count total records
        total_counts = {
            'balance_sheets': 0,
            'cash_flow': 0,
            'income_statements': 0
        }

        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download financials for {ticker}: {result}")
                continue

            for stmt_type in total_counts.keys():
                df = result.get(stmt_type, pl.DataFrame())
                total_counts[stmt_type] += len(df)

        logger.info(
            f"Downloaded financials for {len(tickers)} tickers: "
            f"{total_counts['balance_sheets']} balance sheets, "
            f"{total_counts['cash_flow']} cash flow statements, "
            f"{total_counts['income_statements']} income statements"
        )

        return total_counts

    async def download_stock_financials_vx(
        self,
        ticker: str,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download stock financials using vX endpoint (all in one)

        Args:
            ticker: Ticker symbol
            timeframe: annual, quarterly, or ttm
            limit: Results per page

        Returns:
            Polars DataFrame with all financial data
        """
        logger.info(f"Downloading stock financials for {ticker}")

        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker.upper()
        if timeframe:
            params['timeframe'] = timeframe

        # Fetch all pages
        results = await self.client.paginate_all('/vX/reference/financials', params)

        if not results:
            logger.warning(f"No financials found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} financial records for {ticker}")

        return df

    async def download_short_interest(
        self,
        ticker: str,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download short interest data for a ticker

        Args:
            ticker: Ticker symbol
            limit: Results per page

        Returns:
            Polars DataFrame with short interest data
        """
        logger.info(f"Downloading short interest for {ticker}")

        params = {
            'ticker': ticker.upper(),
            'limit': limit
        }

        # Fetch all pages
        results = await self.client.paginate_all(
            '/stocks/v1/short-interest',
            params
        )

        if not results:
            logger.warning(f"No short interest found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        logger.info(f"Downloaded {len(df)} short interest records for {ticker}")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned_short_data(df, 'short_interest', 'settlement_date')
        else:
            output_file = self.output_dir / f"short_interest_{ticker.upper()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_short_volume(
        self,
        ticker: str,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download short volume data for a ticker

        Args:
            ticker: Ticker symbol
            limit: Results per page

        Returns:
            Polars DataFrame with short volume data
        """
        logger.info(f"Downloading short volume for {ticker}")

        params = {
            'ticker': ticker.upper(),
            'limit': limit
        }

        # Fetch all pages
        results = await self.client.paginate_all(
            '/stocks/v1/short-volume',
            params
        )

        if not results:
            logger.warning(f"No short volume found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        logger.info(f"Downloaded {len(df)} short volume records for {ticker}")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned_short_data(df, 'short_volume', 'date')
        else:
            output_file = self.output_dir / f"short_volume_{ticker.upper()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_short_data_batch(
        self,
        tickers: List[str],
        limit: int = 100
    ) -> Dict[str, pl.DataFrame]:
        """
        Download short interest and short volume for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            limit: Results per page

        Returns:
            Dictionary with 'short_interest' and 'short_volume' DataFrames
        """
        logger.info(f"Downloading short data for {len(tickers)} tickers in parallel")

        # Download all in parallel
        tasks = []
        for ticker in tickers:
            tasks.append(self.download_short_interest(ticker, limit))
            tasks.append(self.download_short_volume(ticker, limit))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate short interest and short volume results
        short_interest_dfs = []
        short_volume_dfs = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                ticker_idx = i // 2
                data_type = 'short_interest' if i % 2 == 0 else 'short_volume'
                logger.error(f"Failed to download {data_type} for {tickers[ticker_idx]}: {result}")
                continue

            if len(result) > 0:
                if i % 2 == 0:  # Short interest
                    short_interest_dfs.append(result)
                else:  # Short volume
                    short_volume_dfs.append(result)

        # Combine results (use diagonal concat to handle schema differences)
        combined = {
            'short_interest': pl.concat(short_interest_dfs, how="diagonal") if short_interest_dfs else pl.DataFrame(),
            'short_volume': pl.concat(short_volume_dfs, how="diagonal") if short_volume_dfs else pl.DataFrame()
        }

        logger.info(
            f"Downloaded short data for {len(tickers)} tickers: "
            f"{len(combined['short_interest'])} short interest records, "
            f"{len(combined['short_volume'])} short volume records"
        )

        return combined

    async def download_all_fundamentals_extended(
        self,
        ticker: str,
        timeframe: str = 'quarterly',
        include_short_data: bool = True,
        limit: int = 100
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all fundamentals including short interest and short volume

        Args:
            ticker: Ticker symbol
            timeframe: annual or quarterly
            include_short_data: Include short interest and volume
            limit: Results per page

        Returns:
            Dictionary with all fundamental data
        """
        logger.info(f"Downloading all fundamentals (extended) for {ticker}")

        # Build task list
        tasks = [
            self.download_balance_sheets(ticker=ticker, timeframe=timeframe, limit=limit),
            self.download_cash_flow_statements(ticker=ticker, timeframe=timeframe, limit=limit),
            self.download_income_statements(ticker=ticker, timeframe=timeframe, limit=limit),
        ]

        if include_short_data:
            tasks.append(self.download_short_interest(ticker=ticker, limit=limit))
            tasks.append(self.download_short_volume(ticker=ticker, limit=limit))

        # Download all in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        data = {}
        keys = ['balance_sheets', 'cash_flow', 'income_statements']
        if include_short_data:
            keys.extend(['short_interest', 'short_volume'])

        for key, result in zip(keys, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {key}: {result}")
                data[key] = pl.DataFrame()
            else:
                data[key] = result

        logger.info(
            f"Downloaded all fundamentals for {ticker}: "
            f"{len(data['balance_sheets'])} balance sheets, "
            f"{len(data['cash_flow'])} cash flow statements, "
            f"{len(data['income_statements'])} income statements"
        )

        if include_short_data:
            logger.info(
                f"Short data: {len(data.get('short_interest', pl.DataFrame()))} short interest, "
                f"{len(data.get('short_volume', pl.DataFrame()))} short volume"
            )

        return data


async def main():
    """Example usage"""
    import sys
    from ..core.config_loader import ConfigLoader

    try:
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')

        if not credentials or 'api_key' not in credentials:
            print("❌ API key not found. Please configure config/credentials.yaml")
            sys.exit(1)

        # Create client
        async with PolygonRESTClient(
            api_key=credentials['api_key'],
            max_concurrent=100,
            max_connections=200
        ) as client:

            # Create downloader
            downloader = FundamentalsDownloader(
                client=client,
                output_dir=Path('data/fundamentals')
            )

            print("✅ FundamentalsDownloader initialized\n")

            # Test: Download all financials for AAPL (including short data)
            print("📥 Downloading all financials for AAPL (quarterly, with short data)...")
            data = await downloader.download_all_fundamentals_extended('AAPL', timeframe='quarterly')

            for stmt_type, df in data.items():
                print(f"\n{stmt_type.upper()}: {len(df)} records")
                if len(df) > 0:
                    print(df.head())

            # Test: Download short data for multiple tickers
            print("\n📥 Downloading short data for multiple tickers...")
            short_data = await downloader.download_short_data_batch(['AAPL', 'MSFT', 'GOOGL'])

            print(f"Short Interest: {len(short_data['short_interest'])} total records")
            print(f"Short Volume: {len(short_data['short_volume'])} total records")

            # Test: Batch download for multiple tickers (traditional)
            print("\n📥 Downloading financials for multiple tickers...")
            batch_data = await downloader.download_financials_batch(
                ['AAPL', 'MSFT', 'GOOGL'],
                timeframe='annual'
            )

            for stmt_type, df in batch_data.items():
                print(f"{stmt_type}: {len(df)} total records")

            # Statistics
            stats = client.get_statistics()
            print(f"\n📊 Statistics:")
            print(f"   Total requests: {stats['total_requests']}")
            print(f"   Success rate: {stats['success_rate']:.1%}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
