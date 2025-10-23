"""
Corporate Actions Downloader - IPOs, splits, dividends, ticker events

High-performance downloader for Polygon corporate actions data.

Downloads:
- IPOs
- Stock splits
- Dividends
- Ticker events
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging

from .polygon_rest_client import PolygonRESTClient, format_date

logger = logging.getLogger(__name__)


class CorporateActionsDownloader:
    """
    High-performance corporate actions downloader

    Optimized for unlimited API rate with parallel pagination
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize corporate actions downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
            use_partitioned_structure: If True, save in date-first partitioned structure
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"CorporateActionsDownloader initialized (output: {output_dir}, partitioned: {use_partitioned_structure})")

    def _save_partitioned(
        self,
        df: pl.DataFrame,
        data_type: str,
        date_column: str
    ) -> None:
        """
        Save DataFrame in date-first partitioned structure.

        Structure: output_dir/{data_type}/year=YYYY/month=MM/ticker=SYMBOL.parquet

        Args:
            df: DataFrame to save
            data_type: Type of data (dividends, splits, ipos, ticker_events)
            date_column: Column name for date partitioning
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

            # Create partition directory: {data_type}/year=2024/month=10/ticker=AAPL.parquet
            partition_dir = self.output_dir / data_type / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker}.parquet'

            # If file exists, append to it (diagonal concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")

            partition_df.write_parquet(
                str(output_file),
                compression='zstd',
                use_pyarrow=True,
                pyarrow_options={'use_dictionary': False}  # Disable dictionary encoding to prevent schema conflicts
            )
            logger.info(f"Saved {len(partition_df)} records to {output_file}")

    async def download_dividends(
        self,
        ticker: Optional[str] = None,
        ex_dividend_date: Optional[str] = None,
        record_date: Optional[str] = None,
        declaration_date: Optional[str] = None,
        pay_date: Optional[str] = None,
        frequency: Optional[int] = None,
        cash_amount: Optional[float] = None,
        dividend_type: Optional[str] = None,
        limit: int = 1000
    ) -> pl.DataFrame:
        """
        Download dividends data with full pagination

        Args:
            ticker: Ticker symbol
            ex_dividend_date: Ex-dividend date (YYYY-MM-DD or YYYY-MM-DD.gte/lte)
            record_date: Record date
            declaration_date: Declaration date
            pay_date: Payment date
            frequency: Frequency (0=One-Time, 1=Annual, 2=Bi-Annual, 4=Quarterly, 12=Monthly)
            cash_amount: Cash amount
            dividend_type: Dividend type
            limit: Results per page

        Returns:
            Polars DataFrame with dividend data
        """
        logger.info(f"Downloading dividends (ticker={ticker})")

        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker.upper()
        if ex_dividend_date:
            params['ex_dividend_date'] = ex_dividend_date
        if record_date:
            params['record_date'] = record_date
        if declaration_date:
            params['declaration_date'] = declaration_date
        if pay_date:
            params['pay_date'] = pay_date
        if frequency is not None:
            params['frequency'] = frequency
        if cash_amount is not None:
            params['cash_amount'] = cash_amount
        if dividend_type:
            params['dividend_type'] = dividend_type

        # Fetch all pages in parallel
        results = await self.client.paginate_all('/v3/reference/dividends', params)

        if not results:
            logger.warning("No dividends found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} dividend records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'dividends', 'ex_dividend_date')
        else:
            output_file = self.output_dir / f"dividends_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_stock_splits(
        self,
        ticker: Optional[str] = None,
        execution_date: Optional[str] = None,
        reverse_split: Optional[bool] = None,
        limit: int = 1000
    ) -> pl.DataFrame:
        """
        Download stock splits data with full pagination

        Args:
            ticker: Ticker symbol
            execution_date: Execution date (YYYY-MM-DD or YYYY-MM-DD.gte/lte)
            reverse_split: Filter for reverse splits
            limit: Results per page

        Returns:
            Polars DataFrame with stock split data
        """
        logger.info(f"Downloading stock splits (ticker={ticker})")

        params = {'limit': limit}
        if ticker:
            params['ticker'] = ticker.upper()
        if execution_date:
            params['execution_date'] = execution_date
        if reverse_split is not None:
            params['reverse_split'] = reverse_split

        # Fetch all pages in parallel
        results = await self.client.paginate_all('/v3/reference/splits', params)

        if not results:
            logger.warning("No stock splits found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} stock split records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'splits', 'execution_date')
        else:
            output_file = self.output_dir / f"stock_splits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_ipos(
        self,
        ticker: Optional[str] = None,
        ipo_status: Optional[str] = None,
        listing_date: Optional[str] = None,
        limit: int = 1000,
        **date_params
    ) -> pl.DataFrame:
        """
        Download IPO (Initial Public Offering) data

        Args:
            ticker: Ticker symbol (case-sensitive)
            ipo_status: Filter by status (direct_listing_process, history, new, pending, postponed, rumor, withdrawn)
            listing_date: First trading date for newly listed entity (YYYY-MM-DD)
            limit: Results per page
            **date_params: Additional date filter parameters (e.g., listing_date.gte, listing_date.lte)

        Returns:
            Polars DataFrame with IPO data
        """
        logger.info(f"Downloading IPOs (ticker={ticker}, status={ipo_status})")

        params = {'limit': limit, **date_params}
        if ticker:
            params['ticker'] = ticker.upper()
        if ipo_status:
            params['ipo_status'] = ipo_status
        if listing_date:
            params['listing_date'] = listing_date

        # Fetch all pages in parallel
        results = await self.client.paginate_all('/vX/reference/ipos', params)

        if not results:
            logger.warning("No IPOs found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} IPO records")

        # Save to parquet
        if self.use_partitioned_structure and 'ticker' in df.columns and 'listing_date' in df.columns:
            self._save_partitioned(df, 'ipos', 'listing_date')
        else:
            output_file = self.output_dir / f"ipos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_ticker_events(
        self,
        ticker: str,
        types: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download ticker events (ticker symbol changes, rebranding) for a specific ticker

        Note: This endpoint is experimental and currently only supports ticker_change events.
        For IPO data, use download_ipos() instead.
        This endpoint requires a specific ticker and returns a single result (not paginated).

        Args:
            ticker: Ticker symbol (required)
            types: Event types filter (comma-separated, currently only 'ticker_change' supported)

        Returns:
            Polars DataFrame with ticker events (flattened from events array)
        """
        if not ticker:
            raise ValueError("Ticker is required for ticker events endpoint")

        logger.info(f"Downloading ticker events for {ticker}")

        # Build endpoint with ticker in path
        endpoint = f'/vX/reference/tickers/{ticker.upper()}/events'
        params = {}
        if types:
            params['types'] = types

        # Fetch single result (not paginated)
        response = await self.client.make_request(endpoint, params)

        if not response or 'results' not in response:
            logger.warning(f"No ticker events found for {ticker}")
            return pl.DataFrame()

        result = response['results']
        events = result.get('events', [])

        if not events:
            logger.warning(f"No events in results for {ticker}")
            return pl.DataFrame()

        # Flatten events array and add company metadata
        records = []
        for event in events:
            record = {
                'ticker': ticker.upper(),
                'name': result.get('name'),
                'composite_figi': result.get('composite_figi'),
                'cik': result.get('cik'),
                'event_type': event.get('type'),
                'date': event.get('date'),
                'downloaded_at': datetime.now()
            }

            # Add event-specific fields
            if 'ticker_change' in event:
                record['new_ticker'] = event['ticker_change'].get('ticker')

            records.append(record)

        # Convert to DataFrame
        df = pl.DataFrame(records)
        logger.info(f"Downloaded {len(df)} ticker event records for {ticker}")

        # Save to parquet
        if self.use_partitioned_structure and len(df) > 0:
            self._save_partitioned(df, 'ticker_events', 'date')
        else:
            output_file = self.output_dir / f"ticker_events_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_ticker_events_batch(
        self,
        tickers: List[str],
        types: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download ticker events for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            types: Event types filter (comma-separated)

        Returns:
            Polars DataFrame with all ticker events combined
        """
        logger.info(f"Downloading ticker events for {len(tickers)} tickers")

        # Download all tickers in parallel
        tasks = [self.download_ticker_events(ticker, types) for ticker in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        dfs = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download ticker events for {ticker}: {result}")
            elif isinstance(result, pl.DataFrame) and len(result) > 0:
                dfs.append(result)

        if not dfs:
            logger.warning("No ticker events found for any ticker")
            return pl.DataFrame()

        # Combine all DataFrames with relaxed schema alignment to handle Null vs String mismatches
        combined_df = pl.concat(dfs, how="diagonal_relaxed")
        logger.info(f"Downloaded {len(combined_df)} total ticker event records")

        return combined_df

    async def download_dividends_with_params(
        self,
        ticker: Optional[str] = None,
        limit: int = 1000,
        **date_params
    ) -> pl.DataFrame:
        """
        Download dividends with flexible date parameters

        Args:
            ticker: Ticker symbol
            limit: Results per page
            **date_params: Additional date filter parameters (e.g., ex_dividend_date.gte, ex_dividend_date.lte)

        Returns:
            Polars DataFrame with dividend data
        """
        logger.info(f"Downloading dividends (ticker={ticker}, params={date_params})")

        params = {'limit': limit, **date_params}
        if ticker:
            params['ticker'] = ticker.upper()

        # Fetch all pages in parallel
        results = await self.client.paginate_all('/v3/reference/dividends', params)

        if not results:
            logger.warning("No dividends found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} dividend records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'dividends', 'ex_dividend_date')
        else:
            output_file = self.output_dir / f"dividends_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_stock_splits_with_params(
        self,
        ticker: Optional[str] = None,
        limit: int = 1000,
        **date_params
    ) -> pl.DataFrame:
        """
        Download stock splits with flexible date parameters

        Args:
            ticker: Ticker symbol
            limit: Results per page
            **date_params: Additional date filter parameters (e.g., execution_date.gte, execution_date.lte)

        Returns:
            Polars DataFrame with stock split data
        """
        logger.info(f"Downloading stock splits (ticker={ticker}, params={date_params})")

        params = {'limit': limit, **date_params}
        if ticker:
            params['ticker'] = ticker.upper()

        # Fetch all pages in parallel
        results = await self.client.paginate_all('/v3/reference/splits', params)

        if not results:
            logger.warning("No stock splits found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} stock split records")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, 'splits', 'execution_date')
        else:
            output_file = self.output_dir / f"stock_splits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_all_corporate_actions(
        self,
        ticker: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_ipos: bool = False
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all corporate actions in parallel

        Args:
            ticker: Ticker symbol (optional, for all tickers if None)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            include_ipos: Include IPO data

        Returns:
            Dictionary with DataFrames for each action type (dividends, splits, ipos)
        """
        logger.info(f"Downloading all corporate actions for {ticker or 'all tickers'}")

        # Build date filters using separate gte/lte parameters
        div_params = {}
        split_params = {}
        ipo_params = {}

        if start_date:
            div_params['ex_dividend_date.gte'] = start_date
            split_params['execution_date.gte'] = start_date
            ipo_params['listing_date.gte'] = start_date
        if end_date:
            div_params['ex_dividend_date.lte'] = end_date
            split_params['execution_date.lte'] = end_date
            ipo_params['listing_date.lte'] = end_date

        # Build task list
        tasks = [
            self.download_dividends_with_params(ticker=ticker, **div_params),
            self.download_stock_splits_with_params(ticker=ticker, **split_params),
        ]

        # Add IPOs if requested
        if include_ipos:
            tasks.append(self.download_ipos(ticker=ticker, **ipo_params))

        # Download all in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        data = {}
        action_types = ['dividends', 'splits']
        if include_ipos:
            action_types.append('ipos')

        for action_type, result in zip(action_types, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {action_type}: {result}")
                data[action_type] = pl.DataFrame()
            else:
                data[action_type] = result

        # Add empty IPOs for backward compatibility if not included
        if not include_ipos:
            data['ipos'] = pl.DataFrame()

        logger.info(
            f"Downloaded all corporate actions: "
            f"{len(data['dividends'])} dividends, "
            f"{len(data['splits'])} splits"
            + (f", {len(data['ipos'])} IPOs" if include_ipos else "")
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
            downloader = CorporateActionsDownloader(
                client=client,
                output_dir=Path('data/corporate_actions')
            )

            print("✅ CorporateActionsDownloader initialized\n")

            # Test: Download all corporate actions for AAPL
            print("📥 Downloading all corporate actions for AAPL...")
            data = await downloader.download_all_corporate_actions(
                ticker='AAPL',
                start_date='2024-01-01'
            )

            for action_type, df in data.items():
                print(f"\n{action_type.upper()}: {len(df)} records")
                if len(df) > 0:
                    print(df.head())

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
