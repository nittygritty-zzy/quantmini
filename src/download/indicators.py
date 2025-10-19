"""
Technical Indicators Downloader - Pre-computed technical indicators

High-performance downloader for Polygon technical indicators.

Downloads:
- SMA (Simple Moving Average)
- EMA (Exponential Moving Average)
- MACD (Moving Average Convergence Divergence)
- RSI (Relative Strength Index)
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
import logging

from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class TechnicalIndicatorsDownloader:
    """
    High-performance technical indicators downloader

    Downloads pre-computed indicators from Polygon
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path
    ):
        """
        Initialize technical indicators downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"TechnicalIndicatorsDownloader initialized (output: {output_dir})")

    async def download_sma(
        self,
        ticker: str,
        timespan: str = 'day',
        window: int = 50,
        series_type: str = 'close',
        adjusted: bool = True,
        limit: int = 5000,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download Simple Moving Average

        Args:
            ticker: Stock ticker symbol
            timespan: Size of aggregate window (minute, hour, day, week, month, quarter, year)
            window: Window size for SMA
            series_type: Price type (close, open, high, low)
            adjusted: Adjust for splits
            limit: Results per page
            timestamp_gte: Filter >= timestamp (milliseconds)
            timestamp_lte: Filter <= timestamp (milliseconds)

        Returns:
            Polars DataFrame with SMA data
        """
        logger.info(f"Downloading SMA({window}) for {ticker}")

        endpoint = f'/v1/indicators/sma/{ticker.upper()}'

        params = {
            'timespan': timespan,
            'window': window,
            'series_type': series_type,
            'adjusted': str(adjusted).lower(),
            'limit': limit
        }

        if timestamp_gte:
            params['timestamp.gte'] = timestamp_gte
        if timestamp_lte:
            params['timestamp.lte'] = timestamp_lte

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No SMA data for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(window).alias('window'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        if 'timestamp' in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='ms').alias('datetime')
            )

        logger.info(f"Downloaded {len(df)} SMA records for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_sma_{window}_{timespan}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_ema(
        self,
        ticker: str,
        timespan: str = 'day',
        window: int = 50,
        series_type: str = 'close',
        adjusted: bool = True,
        limit: int = 5000,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download Exponential Moving Average

        Args:
            ticker: Stock ticker symbol
            timespan: Size of aggregate window
            window: Window size for EMA
            series_type: Price type
            adjusted: Adjust for splits
            limit: Results per page
            timestamp_gte: Filter >= timestamp
            timestamp_lte: Filter <= timestamp

        Returns:
            Polars DataFrame with EMA data
        """
        logger.info(f"Downloading EMA({window}) for {ticker}")

        endpoint = f'/v1/indicators/ema/{ticker.upper()}'

        params = {
            'timespan': timespan,
            'window': window,
            'series_type': series_type,
            'adjusted': str(adjusted).lower(),
            'limit': limit
        }

        if timestamp_gte:
            params['timestamp.gte'] = timestamp_gte
        if timestamp_lte:
            params['timestamp.lte'] = timestamp_lte

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No EMA data for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(window).alias('window'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        if 'timestamp' in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='ms').alias('datetime')
            )

        logger.info(f"Downloaded {len(df)} EMA records for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_ema_{window}_{timespan}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_macd(
        self,
        ticker: str,
        timespan: str = 'day',
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        series_type: str = 'close',
        adjusted: bool = True,
        limit: int = 5000,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download MACD indicator

        Args:
            ticker: Stock ticker symbol
            timespan: Size of aggregate window
            short_window: Short EMA window
            long_window: Long EMA window
            signal_window: Signal line window
            series_type: Price type
            adjusted: Adjust for splits
            limit: Results per page
            timestamp_gte: Filter >= timestamp
            timestamp_lte: Filter <= timestamp

        Returns:
            Polars DataFrame with MACD data
        """
        logger.info(f"Downloading MACD({short_window},{long_window},{signal_window}) for {ticker}")

        endpoint = f'/v1/indicators/macd/{ticker.upper()}'

        params = {
            'timespan': timespan,
            'short_window': short_window,
            'long_window': long_window,
            'signal_window': signal_window,
            'series_type': series_type,
            'adjusted': str(adjusted).lower(),
            'limit': limit
        }

        if timestamp_gte:
            params['timestamp.gte'] = timestamp_gte
        if timestamp_lte:
            params['timestamp.lte'] = timestamp_lte

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No MACD data for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        if 'timestamp' in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='ms').alias('datetime')
            )

        logger.info(f"Downloaded {len(df)} MACD records for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_macd_{timespan}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_rsi(
        self,
        ticker: str,
        timespan: str = 'day',
        window: int = 14,
        series_type: str = 'close',
        adjusted: bool = True,
        limit: int = 5000,
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download RSI indicator

        Args:
            ticker: Stock ticker symbol
            timespan: Size of aggregate window
            window: Window size for RSI
            series_type: Price type
            adjusted: Adjust for splits
            limit: Results per page
            timestamp_gte: Filter >= timestamp
            timestamp_lte: Filter <= timestamp

        Returns:
            Polars DataFrame with RSI data
        """
        logger.info(f"Downloading RSI({window}) for {ticker}")

        endpoint = f'/v1/indicators/rsi/{ticker.upper()}'

        params = {
            'timespan': timespan,
            'window': window,
            'series_type': series_type,
            'adjusted': str(adjusted).lower(),
            'limit': limit
        }

        if timestamp_gte:
            params['timestamp.gte'] = timestamp_gte
        if timestamp_lte:
            params['timestamp.lte'] = timestamp_lte

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No RSI data for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(window).alias('window'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        # Rename columns
        if 'timestamp' in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col('timestamp'), time_unit='ms').alias('datetime')
            )

        logger.info(f"Downloaded {len(df)} RSI records for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_rsi_{window}_{timespan}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_all_indicators(
        self,
        ticker: str,
        timespan: str = 'day',
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all indicators for a ticker in parallel

        Args:
            ticker: Stock ticker symbol
            timespan: Size of aggregate window
            timestamp_gte: Filter >= timestamp
            timestamp_lte: Filter <= timestamp

        Returns:
            Dictionary with DataFrames for each indicator
        """
        logger.info(f"Downloading all indicators for {ticker}")

        # Download all in parallel
        results = await asyncio.gather(
            self.download_sma(ticker, timespan=timespan, window=20, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            self.download_sma(ticker, timespan=timespan, window=50, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            self.download_ema(ticker, timespan=timespan, window=12, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            self.download_ema(ticker, timespan=timespan, window=26, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            self.download_macd(ticker, timespan=timespan, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            self.download_rsi(ticker, timespan=timespan, timestamp_gte=timestamp_gte, timestamp_lte=timestamp_lte),
            return_exceptions=True
        )

        # Process results
        data = {}
        indicator_names = ['sma_20', 'sma_50', 'ema_12', 'ema_26', 'macd', 'rsi']

        for indicator_name, result in zip(indicator_names, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {indicator_name}: {result}")
                data[indicator_name] = pl.DataFrame()
            else:
                data[indicator_name] = result

        logger.info(f"Downloaded all indicators for {ticker}")

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
            downloader = TechnicalIndicatorsDownloader(
                client=client,
                output_dir=Path('data/indicators')
            )

            print("✅ TechnicalIndicatorsDownloader initialized\n")

            # Test: Download SMA for AAPL
            print("📥 Downloading SMA(50) for AAPL...")
            sma_df = await downloader.download_sma('AAPL', window=50, limit=100)
            print(f"   Downloaded {len(sma_df)} SMA records")
            if len(sma_df) > 0:
                print(sma_df.head())

            # Test: Download RSI for AAPL
            print("\n📥 Downloading RSI(14) for AAPL...")
            rsi_df = await downloader.download_rsi('AAPL', window=14, limit=100)
            print(f"   Downloaded {len(rsi_df)} RSI records")
            if len(rsi_df) > 0:
                print(rsi_df.head())

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
