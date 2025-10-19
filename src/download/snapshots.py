"""
Snapshots Downloader - Real-time market snapshots

High-performance downloader for Polygon snapshot data.

Downloads:
- Single ticker snapshot (current price, day stats, prev day)
- All tickers snapshot (full market state)
- Gainers/Losers (top movers)
- Most active (highest volume)
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class SnapshotsDownloader:
    """
    High-performance snapshots downloader

    Downloads real-time market state from Polygon
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path
    ):
        """
        Initialize snapshots downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"SnapshotsDownloader initialized (output: {output_dir})")

    async def download_ticker_snapshot(
        self,
        ticker: str
    ) -> pl.DataFrame:
        """
        Download snapshot for a single ticker

        Args:
            ticker: Stock ticker symbol

        Returns:
            Polars DataFrame with snapshot data
        """
        logger.info(f"Downloading snapshot for {ticker}")

        endpoint = f'/v2/snapshot/locale/us/markets/stocks/tickers/{ticker.upper()}'

        response = await self.client.make_request(endpoint, {})
        ticker_data = response.get('ticker', {})

        if not ticker_data:
            logger.warning(f"No snapshot found for {ticker}")
            return pl.DataFrame()

        # Flatten nested structure
        flattened = {
            'ticker': ticker_data.get('ticker'),
            'updated': ticker_data.get('updated'),
            'downloaded_at': datetime.now()
        }

        # Day data
        day = ticker_data.get('day', {})
        for key, value in day.items():
            flattened[f'day_{key}'] = value

        # Prev day data
        prev_day = ticker_data.get('prevDay', {})
        for key, value in prev_day.items():
            flattened[f'prev_{key}'] = value

        # Min data (minute aggregate)
        min_data = ticker_data.get('min', {})
        for key, value in min_data.items():
            flattened[f'min_{key}'] = value

        # Convert to DataFrame
        df = pl.DataFrame([flattened])

        logger.info(f"Downloaded snapshot for {ticker}")

        # Save to parquet
        output_file = self.output_dir / f"{ticker.lower()}_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_ticker_snapshots_batch(
        self,
        tickers: List[str]
    ) -> pl.DataFrame:
        """
        Download snapshots for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols

        Returns:
            Polars DataFrame with all snapshots
        """
        logger.info(f"Downloading snapshots for {len(tickers)} tickers")

        tasks = [self.download_ticker_snapshot(ticker) for ticker in tickers]
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
            logger.warning("No snapshots for any ticker")
            return pl.DataFrame()

        combined_df = pl.concat(dfs)
        logger.info(f"Downloaded snapshots for {len(dfs)} tickers")

        # Save combined file
        output_file = self.output_dir / f"snapshots_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        combined_df.write_parquet(output_file, compression='zstd')

        return combined_df

    async def download_all_tickers_snapshot(
        self,
        include_otc: bool = False
    ) -> pl.DataFrame:
        """
        Download snapshot for all tickers

        Args:
            include_otc: Include OTC securities

        Returns:
            Polars DataFrame with all tickers' snapshots
        """
        logger.info("Downloading all tickers snapshot")

        endpoint = '/v2/snapshot/locale/us/markets/stocks/tickers'
        params = {'include_otc': str(include_otc).lower()}

        response = await self.client.make_request(endpoint, params)
        tickers_data = response.get('tickers', [])

        if not tickers_data:
            logger.warning("No snapshots found")
            return pl.DataFrame()

        # Flatten all ticker data
        flattened_list = []
        for ticker_data in tickers_data:
            flattened = {
                'ticker': ticker_data.get('ticker'),
                'updated': ticker_data.get('updated'),
                'downloaded_at': datetime.now()
            }

            # Day data
            day = ticker_data.get('day', {})
            for key, value in day.items():
                flattened[f'day_{key}'] = value

            # Prev day data
            prev_day = ticker_data.get('prevDay', {})
            for key, value in prev_day.items():
                flattened[f'prev_{key}'] = value

            # Min data
            min_data = ticker_data.get('min', {})
            for key, value in min_data.items():
                flattened[f'min_{key}'] = value

            flattened_list.append(flattened)

        # Convert to DataFrame
        df = pl.DataFrame(flattened_list)

        logger.info(f"Downloaded {len(df)} ticker snapshots")

        # Save to parquet
        output_file = self.output_dir / f"all_tickers_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_gainers(
        self,
        direction: str = 'gainers',
        include_otc: bool = False
    ) -> pl.DataFrame:
        """
        Download top gainers or losers

        Args:
            direction: 'gainers' or 'losers'
            include_otc: Include OTC securities

        Returns:
            Polars DataFrame with top movers
        """
        logger.info(f"Downloading {direction}")

        endpoint = f'/v2/snapshot/locale/us/markets/stocks/{direction}'
        params = {'include_otc': str(include_otc).lower()}

        response = await self.client.make_request(endpoint, params)
        tickers_data = response.get('tickers', [])

        if not tickers_data:
            logger.warning(f"No {direction} found")
            return pl.DataFrame()

        # Flatten data
        flattened_list = []
        for ticker_data in tickers_data:
            flattened = {
                'ticker': ticker_data.get('ticker'),
                'updated': ticker_data.get('updated'),
                'downloaded_at': datetime.now()
            }

            # Day data
            day = ticker_data.get('day', {})
            for key, value in day.items():
                flattened[f'day_{key}'] = value

            # Prev day data
            prev_day = ticker_data.get('prevDay', {})
            for key, value in prev_day.items():
                flattened[f'prev_{key}'] = value

            # Calculate change percentage if available
            if 'day_c' in flattened and 'prev_c' in flattened and flattened['prev_c']:
                flattened['change_pct'] = ((flattened['day_c'] - flattened['prev_c']) / flattened['prev_c']) * 100

            flattened_list.append(flattened)

        # Convert to DataFrame
        df = pl.DataFrame(flattened_list)

        logger.info(f"Downloaded {len(df)} {direction}")

        # Save to parquet
        output_file = self.output_dir / f"{direction}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

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
            downloader = SnapshotsDownloader(
                client=client,
                output_dir=Path('data/snapshots')
            )

            print("✅ SnapshotsDownloader initialized\n")

            # Test: Download snapshot for AAPL
            print("📥 Downloading snapshot for AAPL...")
            df = await downloader.download_ticker_snapshot('AAPL')
            print(f"   Downloaded snapshot with {len(df.columns)} columns")
            if len(df) > 0:
                print(df)

            # Test: Download gainers
            print("\n📥 Downloading top gainers...")
            gainers_df = await downloader.download_gainers('gainers')
            print(f"   Downloaded {len(gainers_df)} gainers")
            if len(gainers_df) > 0:
                print(gainers_df.select(['ticker', 'change_pct', 'day_c', 'day_v']).head())

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
