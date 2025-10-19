"""
Economy Data Downloader - Macroeconomic indicators

High-performance downloader for Polygon economy data.

Downloads:
- Treasury yields
- Inflation data
- Inflation expectations
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient, format_date

logger = logging.getLogger(__name__)


class EconomyDataDownloader:
    """
    High-performance economy data downloader

    Downloads macroeconomic indicators from Polygon
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path
    ):
        """
        Initialize economy data downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"EconomyDataDownloader initialized (output: {output_dir})")

    async def download_treasury_yields(
        self,
        ticker: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 1000,
        order: str = 'desc',
        **date_params
    ) -> pl.DataFrame:
        """
        Download treasury yields data

        Args:
            ticker: Treasury ticker (e.g., DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS10, DGS30)
            date: Date or date range (YYYY-MM-DD or YYYY-MM-DD.gte/lte)
            limit: Results per page
            order: Sort order (asc or desc)
            **date_params: Additional date filter parameters (e.g., date.gte, date.lte)

        Returns:
            Polars DataFrame with treasury yields
        """
        logger.info(f"Downloading treasury yields (ticker={ticker})")

        params = {'limit': limit, 'order': order, **date_params}
        if ticker:
            params['ticker'] = ticker
        if date:
            params['date'] = date

        # Fetch all pages
        results = await self.client.paginate_all('/fed/v1/treasury-yields', params)

        if not results:
            logger.warning("No treasury yields found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} treasury yield records")

        # Save to parquet
        output_file = self.output_dir / f"treasury_yields_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df

    async def download_inflation(
        self,
        ticker: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 1000,
        order: str = 'desc',
        **date_params
    ) -> pl.DataFrame:
        """
        Download inflation data (CPI, PCE, etc.)

        Args:
            ticker: Inflation ticker (e.g., CPIAUCSL, CPILFESL, PCEPI, PCEPILFE)
            date: Date or date range
            limit: Results per page
            order: Sort order (asc or desc)
            **date_params: Additional date filter parameters (e.g., date.gte, date.lte)

        Returns:
            Polars DataFrame with inflation data
        """
        logger.info(f"Downloading inflation data (ticker={ticker})")

        params = {'limit': limit, 'order': order, **date_params}
        if ticker:
            params['ticker'] = ticker
        if date:
            params['date'] = date

        # Fetch all pages
        results = await self.client.paginate_all('/fed/v1/inflation', params)

        if not results:
            logger.warning("No inflation data found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} inflation records")

        # Save to parquet
        output_file = self.output_dir / f"inflation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df

    async def download_inflation_expectations(
        self,
        ticker: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 1000,
        order: str = 'desc',
        **date_params
    ) -> pl.DataFrame:
        """
        Download inflation expectations data

        Args:
            ticker: Inflation expectations ticker
            date: Date or date range
            limit: Results per page
            order: Sort order (asc or desc)
            **date_params: Additional date filter parameters (e.g., date.gte, date.lte)

        Returns:
            Polars DataFrame with inflation expectations
        """
        logger.info(f"Downloading inflation expectations (ticker={ticker})")

        params = {'limit': limit, 'order': order, **date_params}
        if ticker:
            params['ticker'] = ticker
        if date:
            params['date'] = date

        # Fetch all pages
        results = await self.client.paginate_all('/fed/v1/inflation-expectations', params)

        if not results:
            logger.warning("No inflation expectations found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} inflation expectation records")

        # Save to parquet
        output_file = self.output_dir / f"inflation_expectations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df

    async def download_all_economy_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all economy data in parallel

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dictionary with DataFrames for each indicator type
        """
        logger.info("Downloading all economy data in parallel")

        # Build date filters using separate date.gte and date.lte parameters
        date_params = {}
        if start_date:
            date_params['date.gte'] = start_date
        if end_date:
            date_params['date.lte'] = end_date

        # Download all in parallel with date params
        tasks = []
        for method in [self.download_treasury_yields, self.download_inflation, self.download_inflation_expectations]:
            task = method(**date_params) if date_params else method()
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        data = {}
        indicator_types = ['treasury_yields', 'inflation', 'inflation_expectations']

        for indicator_type, result in zip(indicator_types, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {indicator_type}: {result}")
                data[indicator_type] = pl.DataFrame()
            else:
                data[indicator_type] = result

        logger.info(
            f"Downloaded all economy data: "
            f"{len(data['treasury_yields'])} treasury yields, "
            f"{len(data['inflation'])} inflation records, "
            f"{len(data['inflation_expectations'])} expectations records"
        )

        return data

    async def download_treasury_curve(
        self,
        date: str
    ) -> pl.DataFrame:
        """
        Download full treasury yield curve for a specific date

        Args:
            date: Date (YYYY-MM-DD)

        Returns:
            Polars DataFrame with yield curve data (all maturities)
        """
        logger.info(f"Downloading treasury yield curve for {date}")

        # All treasury tickers (maturities)
        tickers = [
            'DGS1MO',   # 1-Month
            'DGS3MO',   # 3-Month
            'DGS6MO',   # 6-Month
            'DGS1',     # 1-Year
            'DGS2',     # 2-Year
            'DGS3',     # 3-Year
            'DGS5',     # 5-Year
            'DGS7',     # 7-Year
            'DGS10',    # 10-Year
            'DGS20',    # 20-Year
            'DGS30'     # 30-Year
        ]

        # Download all maturities in parallel
        tasks = [
            self.download_treasury_yields(ticker=ticker, date=date, limit=100)
            for ticker in tickers
        ]
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
            logger.warning(f"No yield curve data found for {date}")
            return pl.DataFrame()

        # Concatenate all maturities
        curve_df = pl.concat(dfs)
        logger.info(f"Downloaded yield curve with {len(curve_df)} data points for {date}")

        return curve_df


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
            downloader = EconomyDataDownloader(
                client=client,
                output_dir=Path('data/economy')
            )

            print("✅ EconomyDataDownloader initialized\n")

            # Test: Download all economy data
            print("📥 Downloading all economy data (last 90 days)...")
            from datetime import date, timedelta
            end_date = date.today()
            start_date = end_date - timedelta(days=90)

            data = await downloader.download_all_economy_data(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )

            for indicator_type, df in data.items():
                print(f"\n{indicator_type.upper()}: {len(df)} records")
                if len(df) > 0:
                    print(df.head())

            # Test: Download yield curve for today
            print(f"\n📥 Downloading yield curve for {end_date}...")
            curve_df = await downloader.download_treasury_curve(end_date.strftime('%Y-%m-%d'))
            print(f"   Downloaded {len(curve_df)} yield curve points")
            if len(curve_df) > 0:
                print(curve_df)

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
