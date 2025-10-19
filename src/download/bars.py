"""
Aggregate Bars Downloader - Historical OHLCV price data

High-performance downloader for Polygon aggregate bars (OHLCV).

Downloads:
- Custom time range bars (minute, hour, day, week, month, quarter, year)
- Previous day bars
- Daily open/close
- Group by ticker or date
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
import logging

from .polygon_rest_client import PolygonRESTClient, format_date

logger = logging.getLogger(__name__)


class AggregatesDownloader:
    """
    High-performance aggregate bars downloader

    Downloads OHLCV data for stocks with parallel pagination
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path
    ):
        """
        Initialize aggregates downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"AggregatesDownloader initialized (output: {output_dir})")

    async def download_bars(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = 'day',
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
        sort: str = 'asc',
        limit: int = 50000
    ) -> pl.DataFrame:
        """
        Download aggregate bars for a single ticker

        Args:
            ticker: Stock ticker symbol
            multiplier: Size of timespan (e.g., 1, 5, 15)
            timespan: Size of time window (minute, hour, day, week, month, quarter, year)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            adjusted: Adjust for splits/dividends
            sort: Sort order (asc or desc)
            limit: Results per page

        Returns:
            Polars DataFrame with OHLCV data
        """
        logger.info(f"Downloading {timespan} bars for {ticker}")

        # Default date range: last 2 years
        if not to_date:
            to_date = date.today().strftime('%Y-%m-%d')
        if not from_date:
            from_date = (date.today() - timedelta(days=730)).strftime('%Y-%m-%d')

        # Build endpoint
        endpoint = f'/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}'

        params = {
            'adjusted': str(adjusted).lower(),
            'sort': sort,
            'limit': limit
        }

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No bars found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)

        # Add metadata columns
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns to standard names
        column_mapping = {
            'v': 'volume',
            'vw': 'vwap',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'timestamp',
            'n': 'transactions'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})

        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='ms').alias('datetime')
            )

        logger.info(f"Downloaded {len(df)} bars for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_{timespan}_{from_date}_{to_date}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df

    async def download_bars_batch(
        self,
        tickers: List[str],
        multiplier: int = 1,
        timespan: str = 'day',
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        adjusted: bool = True,
        sort: str = 'asc',
        limit: int = 50000
    ) -> pl.DataFrame:
        """
        Download bars for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            multiplier: Size of timespan
            timespan: Size of time window
            from_date: Start date
            to_date: End date
            adjusted: Adjust for splits/dividends
            sort: Sort order
            limit: Results per page

        Returns:
            Polars DataFrame with all bars
        """
        logger.info(f"Downloading {timespan} bars for {len(tickers)} tickers")

        # Download all in parallel
        tasks = [
            self.download_bars(
                ticker, multiplier, timespan, from_date, to_date, adjusted, sort, limit
            )
            for ticker in tickers
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine successful results
        dfs = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {ticker}: {result}")
                continue
            if len(result) > 0:
                dfs.append(result)

        if not dfs:
            logger.warning("No bars downloaded for any ticker")
            return pl.DataFrame()

        # Concatenate all DataFrames
        combined_df = pl.concat(dfs)
        logger.info(f"Downloaded {len(combined_df)} total bars for {len(dfs)} tickers")

        # Save combined file
        output_file = self.output_dir / f"batch_{timespan}_{from_date}_{to_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        combined_df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved combined file to {output_file}")

        return combined_df

    async def download_previous_close(
        self,
        ticker: str,
        adjusted: bool = True
    ) -> pl.DataFrame:
        """
        Download previous day's close for a ticker

        Args:
            ticker: Stock ticker symbol
            adjusted: Adjust for splits/dividends

        Returns:
            Polars DataFrame with previous close data
        """
        logger.info(f"Downloading previous close for {ticker}")

        endpoint = f'/v2/aggs/ticker/{ticker.upper()}/prev'
        params = {'adjusted': str(adjusted).lower()}

        response = await self.client.make_request(endpoint, params)
        results = response.get('results', [])

        if not results:
            logger.warning(f"No previous close found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        column_mapping = {
            'v': 'volume',
            'vw': 'vwap',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'timestamp',
            'n': 'transactions'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})

        logger.info(f"Downloaded previous close for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_prev_close_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_previous_close_batch(
        self,
        tickers: List[str],
        adjusted: bool = True
    ) -> pl.DataFrame:
        """
        Download previous close for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            adjusted: Adjust for splits/dividends

        Returns:
            Polars DataFrame with all previous close data
        """
        logger.info(f"Downloading previous close for {len(tickers)} tickers")

        tasks = [self.download_previous_close(ticker, adjusted) for ticker in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        dfs = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {ticker}: {result}")
                continue
            if len(result) > 0:
                dfs.append(result)

        if not dfs:
            logger.warning("No previous close data for any ticker")
            return pl.DataFrame()

        combined_df = pl.concat(dfs)
        logger.info(f"Downloaded previous close for {len(dfs)} tickers")

        return combined_df

    async def download_daily_open_close(
        self,
        ticker: str,
        date_str: str,
        adjusted: bool = True
    ) -> pl.DataFrame:
        """
        Download daily open/close for a specific date

        Args:
            ticker: Stock ticker symbol
            date_str: Date (YYYY-MM-DD)
            adjusted: Adjust for splits/dividends

        Returns:
            Polars DataFrame with open/close data
        """
        logger.info(f"Downloading daily open/close for {ticker} on {date_str}")

        endpoint = f'/v1/open-close/{ticker.upper()}/{date_str}'
        params = {'adjusted': str(adjusted).lower()}

        response = await self.client.make_request(endpoint, params)

        if not response:
            logger.warning(f"No open/close data for {ticker} on {date_str}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame([response])
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded daily open/close for {ticker} on {date_str}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_daily_{date_str}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_grouped_daily(
        self,
        date_str: str,
        adjusted: bool = True,
        include_otc: bool = False
    ) -> pl.DataFrame:
        """
        Download all tickers' daily data for a specific date

        Args:
            date_str: Date (YYYY-MM-DD)
            adjusted: Adjust for splits/dividends
            include_otc: Include OTC securities

        Returns:
            Polars DataFrame with all tickers' data
        """
        logger.info(f"Downloading grouped daily bars for {date_str}")

        endpoint = f'/v2/aggs/grouped/locale/us/market/stocks/{date_str}'
        params = {
            'adjusted': str(adjusted).lower(),
            'include_otc': str(include_otc).lower()
        }

        # This endpoint doesn't support pagination, returns all results
        response = await self.client.make_request(endpoint, params)
        results = response.get('results', [])

        if not results:
            logger.warning(f"No grouped daily data for {date_str}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(date_str).alias('date'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        column_mapping = {
            'T': 'ticker',
            'v': 'volume',
            'vw': 'vwap',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            't': 'timestamp',
            'n': 'transactions'
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})

        logger.info(f"Downloaded {len(df)} tickers for {date_str}")

        # Save to parquet
        output_file = self.output_dir / f"grouped_daily_{date_str}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df


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
            downloader = AggregatesDownloader(
                client=client,
                output_dir=Path('data/bars')
            )

            print("✅ AggregatesDownloader initialized\n")

            # Test: Download daily bars for AAPL
            print("📥 Downloading daily bars for AAPL (last 30 days)...")
            from_date = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
            to_date = date.today().strftime('%Y-%m-%d')

            df = await downloader.download_bars('AAPL', timespan='day', from_date=from_date, to_date=to_date)
            print(f"   Downloaded {len(df)} bars")
            if len(df) > 0:
                print(df.head())

            # Test: Download previous close
            print(f"\n📥 Downloading previous close for AAPL...")
            prev_df = await downloader.download_previous_close('AAPL')
            if len(prev_df) > 0:
                print(prev_df)

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
