"""
Market Status Downloader - Trading hours and market metadata

High-performance downloader for Polygon market status data.

Downloads:
- Market status (open/closed)
- Market holidays
- Exchanges list
- Condition codes (trade/quote conditions)
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class MarketStatusDownloader:
    """
    High-performance market status downloader

    Downloads market metadata and status information
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path
    ):
        """
        Initialize market status downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"MarketStatusDownloader initialized (output: {output_dir})")

    async def download_market_status(self) -> pl.DataFrame:
        """
        Download current market status

        Returns:
            Polars DataFrame with market status
        """
        logger.info("Downloading market status")

        endpoint = '/v1/marketstatus/now'

        response = await self.client.make_request(endpoint, {})

        if not response:
            logger.warning("No market status found")
            return pl.DataFrame()

        # Flatten structure
        flattened = {
            'market': response.get('market'),
            'server_time': response.get('serverTime'),
            'downloaded_at': datetime.now()
        }

        # Add exchanges info
        exchanges = response.get('exchanges', {})
        for exchange, status in exchanges.items():
            flattened[f'exchange_{exchange}'] = status

        # Early hours
        early_hours = response.get('earlyHours', False)
        flattened['early_hours'] = early_hours

        # After hours
        after_hours = response.get('afterHours', False)
        flattened['after_hours'] = after_hours

        # Convert to DataFrame
        df = pl.DataFrame([flattened])

        logger.info("Downloaded market status")

        # Save to parquet
        output_file = self.output_dir / f"market_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_market_holidays(self) -> pl.DataFrame:
        """
        Download market holidays

        Returns:
            Polars DataFrame with market holidays
        """
        logger.info("Downloading market holidays")

        endpoint = '/v1/marketstatus/upcoming'

        response = await self.client.make_request(endpoint, {})

        if not response:
            logger.warning("No market holidays found")
            return pl.DataFrame()

        # Extract holidays from response
        holidays_list = []

        # Check different response formats
        if isinstance(response, list):
            holidays_list = response
        elif isinstance(response, dict):
            # Try to find holidays in nested structure
            if 'results' in response:
                holidays_list = response['results']
            elif 'holidays' in response:
                holidays_list = response['holidays']
            else:
                # Response itself might be the holiday data
                holidays_list = [response]

        if not holidays_list:
            logger.warning("No holidays data in response")
            return pl.DataFrame()

        # Add download timestamp
        for holiday in holidays_list:
            if isinstance(holiday, dict):
                holiday['downloaded_at'] = datetime.now()

        # Convert to DataFrame
        df = pl.DataFrame(holidays_list)

        logger.info(f"Downloaded {len(df)} market holidays")

        # Save to parquet
        output_file = self.output_dir / f"market_holidays_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_exchanges(self) -> pl.DataFrame:
        """
        Download list of exchanges

        Returns:
            Polars DataFrame with exchanges
        """
        logger.info("Downloading exchanges")

        endpoint = '/v3/reference/exchanges'

        # Use pagination
        results = await self.client.paginate_all(endpoint, {'limit': 1000})

        if not results:
            logger.warning("No exchanges found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} exchanges")

        # Save to parquet
        output_file = self.output_dir / f"exchanges_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_conditions(
        self,
        asset_class: str = 'stocks',
        data_type: str = 'trades'
    ) -> pl.DataFrame:
        """
        Download condition codes

        Args:
            asset_class: Asset class (stocks, options, crypto)
            data_type: Data type (trades, quotes)

        Returns:
            Polars DataFrame with condition codes
        """
        logger.info(f"Downloading {data_type} conditions for {asset_class}")

        endpoint = f'/v3/reference/conditions'

        params = {
            'asset_class': asset_class,
            'data_type': data_type,
            'limit': 1000
        }

        # Use pagination
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No {data_type} conditions found for {asset_class}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(asset_class).alias('asset_class'),
            pl.lit(data_type).alias('data_type'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        logger.info(f"Downloaded {len(df)} {data_type} conditions for {asset_class}")

        # Save to parquet
        output_file = self.output_dir / f"conditions_{asset_class}_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')

        return df

    async def download_all_market_metadata(self) -> Dict[str, pl.DataFrame]:
        """
        Download all market metadata in parallel

        Returns:
            Dictionary with DataFrames for each metadata type
        """
        logger.info("Downloading all market metadata")

        # Download all in parallel
        results = await asyncio.gather(
            self.download_market_status(),
            self.download_market_holidays(),
            self.download_exchanges(),
            self.download_conditions('stocks', 'trades'),
            self.download_conditions('stocks', 'quotes'),
            return_exceptions=True
        )

        # Process results
        data = {}
        metadata_types = ['status', 'holidays', 'exchanges', 'trade_conditions', 'quote_conditions']

        for metadata_type, result in zip(metadata_types, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {metadata_type}: {result}")
                data[metadata_type] = pl.DataFrame()
            else:
                data[metadata_type] = result

        logger.info(
            f"Downloaded market metadata: "
            f"{len(data['status'])} status, "
            f"{len(data['holidays'])} holidays, "
            f"{len(data['exchanges'])} exchanges, "
            f"{len(data['trade_conditions'])} trade conditions, "
            f"{len(data['quote_conditions'])} quote conditions"
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
            downloader = MarketStatusDownloader(
                client=client,
                output_dir=Path('data/market_status')
            )

            print("✅ MarketStatusDownloader initialized\n")

            # Test: Download market status
            print("📥 Downloading market status...")
            status_df = await downloader.download_market_status()
            if len(status_df) > 0:
                print(status_df)

            # Test: Download exchanges
            print("\n📥 Downloading exchanges...")
            exchanges_df = await downloader.download_exchanges()
            print(f"   Downloaded {len(exchanges_df)} exchanges")
            if len(exchanges_df) > 0:
                print(exchanges_df.head())

            # Test: Download all metadata
            print("\n📥 Downloading all market metadata...")
            data = await downloader.download_all_market_metadata()
            for metadata_type, df in data.items():
                print(f"   {metadata_type}: {len(df)} records")

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
