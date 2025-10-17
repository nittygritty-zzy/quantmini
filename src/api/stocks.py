"""
Stocks API Data Fetcher

Fetches stock market data from Polygon.io REST API and converts to pipeline format.
"""

import polars as pl
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
import logging

from .client import PolygonAPIClient
from ..core.exceptions import APIError

logger = logging.getLogger(__name__)


class StocksAPIFetcher:
    """
    Fetches stock data from Polygon API

    Supports:
    - Daily aggregates (OHLCV)
    - Minute aggregates (OHLCV)
    - Historical and incremental updates
    """

    def __init__(self, client: PolygonAPIClient):
        """
        Initialize stocks fetcher

        Args:
            client: Initialized PolygonAPIClient
        """
        self.client = client

    def _parse_aggregates_to_dataframe(
        self,
        ticker: str,
        results: List[Dict[str, Any]],
        timespan: str
    ) -> pl.DataFrame:
        """
        Convert API aggregate results to Polars DataFrame

        Args:
            ticker: Ticker symbol
            results: List of aggregate bars from API
            timespan: Timespan (day or minute)

        Returns:
            Polars DataFrame with standardized schema
        """
        if not results:
            logger.warning(f"No results for {ticker}")
            return pl.DataFrame()

        # Parse results
        data = []
        for bar in results:
            record = {
                'ticker': ticker,
                'timestamp': datetime.fromtimestamp(bar['t'] / 1000),  # Convert from milliseconds
                'open': float(bar['o']),
                'high': float(bar['h']),
                'low': float(bar['l']),
                'close': float(bar['c']),
                'volume': int(bar['v']),
                'vwap': float(bar.get('vw', 0.0)),  # Volume weighted average price
                'transactions': int(bar.get('n', 0))  # Number of transactions
            }
            data.append(record)

        # Create DataFrame
        df = pl.DataFrame(data)

        # Add date column for daily data
        if timespan == 'day':
            df = df.with_columns([
                pl.col('timestamp').dt.date().alias('date')
            ])

        return df

    async def fetch_daily_bars(
        self,
        tickers: List[str],
        from_date: date,
        to_date: date,
        adjusted: bool = True
    ) -> pl.DataFrame:
        """
        Fetch daily bars for multiple tickers

        Args:
            tickers: List of ticker symbols
            from_date: Start date
            to_date: End date
            adjusted: Whether to adjust for splits

        Returns:
            Combined Polars DataFrame with all tickers
        """
        logger.info(f"Fetching daily bars for {len(tickers)} tickers from {from_date} to {to_date}")

        # Fetch data for each ticker
        tasks = []
        for ticker in tickers:
            task = self._fetch_ticker_daily(ticker, from_date, to_date, adjusted)
            tasks.append(task)

        # Execute in parallel with some concurrency control
        results = []
        batch_size = 10  # Process 10 tickers at a time to respect rate limits
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error fetching ticker: {result}")
                elif not result.is_empty():
                    results.append(result)

        if not results:
            logger.warning("No data fetched for any tickers")
            return pl.DataFrame()

        # Combine all results
        combined = pl.concat(results)
        logger.info(f"Fetched {len(combined)} daily bars for {len(results)} tickers")

        return combined

    async def _fetch_ticker_daily(
        self,
        ticker: str,
        from_date: date,
        to_date: date,
        adjusted: bool
    ) -> pl.DataFrame:
        """Fetch daily bars for a single ticker"""
        try:
            response = await self.client.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan='day',
                from_date=from_date,
                to_date=to_date,
                adjusted=adjusted,
                limit=50000
            )

            results = response.get('results', [])
            if not results:
                logger.debug(f"No results for {ticker}")
                return pl.DataFrame()

            df = self._parse_aggregates_to_dataframe(ticker, results, 'day')
            logger.debug(f"Fetched {len(df)} daily bars for {ticker}")

            return df

        except Exception as e:
            logger.error(f"Error fetching daily bars for {ticker}: {e}")
            raise

    async def fetch_minute_bars(
        self,
        tickers: List[str],
        from_date: date,
        to_date: date,
        adjusted: bool = True
    ) -> pl.DataFrame:
        """
        Fetch minute bars for multiple tickers

        Args:
            tickers: List of ticker symbols
            from_date: Start date
            to_date: End date
            adjusted: Whether to adjust for splits

        Returns:
            Combined Polars DataFrame with all tickers
        """
        logger.info(f"Fetching minute bars for {len(tickers)} tickers from {from_date} to {to_date}")

        # For minute data, we need to fetch day by day to respect API limits
        # Minute data can be very large, so we process in smaller chunks
        all_results = []

        for ticker in tickers:
            try:
                df = await self._fetch_ticker_minute(ticker, from_date, to_date, adjusted)
                if not df.is_empty():
                    all_results.append(df)
            except Exception as e:
                logger.error(f"Error fetching minute bars for {ticker}: {e}")

        if not all_results:
            logger.warning("No minute data fetched")
            return pl.DataFrame()

        combined = pl.concat(all_results)
        logger.info(f"Fetched {len(combined)} minute bars total")

        return combined

    async def _fetch_ticker_minute(
        self,
        ticker: str,
        from_date: date,
        to_date: date,
        adjusted: bool
    ) -> pl.DataFrame:
        """Fetch minute bars for a single ticker"""
        try:
            response = await self.client.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan='minute',
                from_date=from_date,
                to_date=to_date,
                adjusted=adjusted,
                limit=50000
            )

            results = response.get('results', [])
            if not results:
                logger.debug(f"No minute results for {ticker}")
                return pl.DataFrame()

            df = self._parse_aggregates_to_dataframe(ticker, results, 'minute')
            logger.debug(f"Fetched {len(df)} minute bars for {ticker}")

            return df

        except Exception as e:
            logger.error(f"Error fetching minute bars for {ticker}: {e}")
            raise

    async def get_latest_trading_day(self) -> date:
        """
        Get the latest trading day from Polygon API

        Returns:
            Latest trading date
        """
        # Use a major ticker (SPY) to get latest trading day
        today = date.today()
        yesterday = today - timedelta(days=7)  # Look back 7 days to ensure we find a trading day

        try:
            response = await self.client.get_aggregates(
                ticker='SPY',
                multiplier=1,
                timespan='day',
                from_date=yesterday,
                to_date=today,
                adjusted=True,
                limit=10
            )

            results = response.get('results', [])
            if results:
                # Get the most recent bar
                latest_timestamp = results[-1]['t']
                latest_date = datetime.fromtimestamp(latest_timestamp / 1000).date()
                logger.info(f"Latest trading day: {latest_date}")
                return latest_date
            else:
                logger.warning("Could not determine latest trading day, using today")
                return today

        except Exception as e:
            logger.error(f"Error getting latest trading day: {e}")
            return today

    def save_to_parquet(
        self,
        df: pl.DataFrame,
        output_path: Path,
        compression: str = 'zstd'
    ):
        """
        Save DataFrame to parquet file

        Args:
            df: Polars DataFrame
            output_path: Output file path
            compression: Compression algorithm (zstd, snappy, etc.)
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.write_parquet(
            output_path,
            compression=compression,
            use_pyarrow=True,
            statistics=True
        )

        logger.info(f"Saved {len(df)} records to {output_path}")
