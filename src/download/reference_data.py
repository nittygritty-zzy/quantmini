"""
Reference Data Downloader - Ticker metadata and relationships

High-performance downloader for Polygon reference data with massive parallelization.

Downloads:
- Ticker types
- Related tickers (for multiple tickers in parallel)
- Ticker details
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient, format_date

logger = logging.getLogger(__name__)


class ReferenceDataDownloader:
    """
    High-performance reference data downloader

    Optimized for unlimited API rate with massive parallelization
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize reference data downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
            use_partitioned_structure: If True, save in ticker-partitioned structure
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"ReferenceDataDownloader initialized (output: {output_dir}, partitioned: {use_partitioned_structure})")

    def _save_partitioned_related_tickers(
        self,
        df: pl.DataFrame,
        data_type: str = 'relationships'
    ) -> None:
        """
        Save related tickers DataFrame in ticker-partitioned structure.

        Structure: output_dir/relationships/ticker=SYMBOL.parquet

        Args:
            df: DataFrame to save
            data_type: Type of data (relationships)
        """
        if len(df) == 0:
            return

        # Ensure source_ticker column exists
        if 'source_ticker' not in df.columns:
            logger.warning(f"No 'source_ticker' column in {data_type}, skipping partitioned save")
            return

        # Filter out null source_tickers
        df = df.filter(pl.col('source_ticker').is_not_null())

        if len(df) == 0:
            return

        # Get unique source tickers
        unique_tickers = df.select('source_ticker').unique()

        for row in unique_tickers.iter_rows(named=True):
            ticker = row['source_ticker']

            # Filter for this ticker
            ticker_df = df.filter(pl.col('source_ticker') == ticker)

            # Create partition directory: related_tickers/ticker=AAPL.parquet
            partition_dir = self.output_dir / data_type
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker}.parquet'

            # If file exists, append to it (diagonal concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                ticker_df = pl.concat([existing_df, ticker_df], how="diagonal")

            ticker_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(ticker_df)} records to {output_file}")

    async def download_ticker_types(
        self,
        asset_class: Optional[str] = None,
        locale: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download ticker types

        Args:
            asset_class: Filter by asset class (stocks, options, crypto, fx, indices)
            locale: Filter by locale (us, global)

        Returns:
            Polars DataFrame with ticker types
        """
        logger.info(f"Downloading ticker types (asset_class={asset_class}, locale={locale})")

        params = {}
        if asset_class:
            params['asset_class'] = asset_class
        if locale:
            params['locale'] = locale

        # Make API request
        response = await self.client._make_request(
            '/v3/reference/tickers/types',
            params
        )

        # Convert to DataFrame
        results = response.get('results', [])
        if not results:
            logger.warning("No ticker types found")
            return pl.DataFrame()

        df = pl.DataFrame(results)

        # Add metadata
        df = df.with_columns([
            pl.lit(datetime.now()).alias('downloaded_at'),
            pl.lit(response.get('request_id')).alias('request_id')
        ])

        logger.info(f"Downloaded {len(df)} ticker types")

        # Save to parquet
        output_file = self.output_dir / f"ticker_types_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df

    async def download_related_tickers_batch(
        self,
        tickers: List[str],
        save_intermediate: bool = True
    ) -> pl.DataFrame:
        """
        Download related tickers for multiple tickers in parallel (FAST!)

        Args:
            tickers: List of ticker symbols
            save_intermediate: Save results immediately after download

        Returns:
            Combined Polars DataFrame with all related tickers
        """
        logger.info(f"Downloading related tickers for {len(tickers)} tickers in parallel")

        # Build all requests
        requests = [
            {'endpoint': f'/v1/related-companies/{ticker.upper()}'}
            for ticker in tickers
        ]

        # Execute all requests in parallel
        responses = await self.client.batch_request(requests)

        # Process responses into DataFrame
        all_data = []
        for ticker, response in zip(tickers, responses):
            if 'error' in response:
                logger.warning(f"Failed to get related tickers for {ticker}: {response['error']}")
                continue

            results = response.get('results', [])
            if not results:
                continue

            # Add source ticker to each result
            for item in results:
                item['source_ticker'] = ticker.upper()
                item['downloaded_at'] = datetime.now().isoformat()
                item['request_id'] = response.get('request_id')

            all_data.extend(results)

        if not all_data:
            logger.warning("No related tickers found for any ticker")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(all_data)
        logger.info(f"Downloaded {len(df)} total related ticker relationships")

        # Save to parquet
        if save_intermediate:
            if self.use_partitioned_structure:
                self._save_partitioned_related_tickers(df)
            else:
                output_file = self.output_dir / f"related_tickers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                df.write_parquet(output_file, compression='zstd')
                logger.info(f"Saved to {output_file}")

        return df

    async def download_all_tickers(
        self,
        market: Optional[str] = None,
        ticker_type: Optional[str] = None,
        locale: Optional[str] = 'us',
        active: bool = True,
        limit: int = 1000
    ) -> pl.DataFrame:
        """
        Download all tickers using pagination

        Args:
            market: Filter by market (stocks, crypto, fx, otc, indices)
            ticker_type: Filter by ticker type (CS, ETF, ADRC, etc.)
            locale: Filter by locale (us, global)
            active: Only active tickers
            limit: Results per page (max 1000)

        Returns:
            Polars DataFrame with all tickers
        """
        logger.info(f"Downloading all tickers (market={market}, type={ticker_type}, locale={locale}, active={active})")

        params = {
            'limit': min(limit, 1000),
            'active': str(active).lower()
        }
        if market:
            params['market'] = market
        if ticker_type:
            params['type'] = ticker_type
        if locale:
            params['locale'] = locale

        all_results = []
        page_count = 0
        next_url = None

        while True:
            page_count += 1

            # Make request
            if next_url:
                # Use next_url for pagination
                response = await self.client._make_request_raw_url(next_url)
            else:
                # First request
                response = await self.client._make_request('/v3/reference/tickers', params)

            results = response.get('results', [])
            if not results:
                logger.info(f"No more results after {page_count} pages")
                break

            all_results.extend(results)
            logger.info(f"Page {page_count}: Downloaded {len(results)} tickers (total: {len(all_results)})")

            # Check for next page
            next_url = response.get('next_url')
            if not next_url:
                logger.info(f"Completed pagination after {page_count} pages")
                break

        if not all_results:
            logger.warning("No tickers found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(all_results)

        # Add metadata
        df = df.with_columns([
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        logger.info(f"Downloaded {len(df)} total tickers")

        # Save with partitioning if enabled
        if self.use_partitioned_structure and 'locale' in df.columns and 'type' in df.columns:
            self._save_partitioned_tickers(df)
        else:
            output_file = self.output_dir / f"all_tickers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    def _save_partitioned_tickers(self, df: pl.DataFrame) -> None:
        """
        Save tickers DataFrame in locale/type partitioned structure

        Structure: output_dir/tickers/locale=us/type=CS/data.parquet
        """
        if len(df) == 0:
            return

        # Get unique locale/type combinations
        partitions = df.select(['locale', 'type']).unique()

        for row in partitions.iter_rows(named=True):
            locale = row['locale']
            ticker_type = row['type']

            # Filter for this partition
            partition_df = df.filter(
                (pl.col('locale') == locale) & (pl.col('type') == ticker_type)
            )

            # Create partition directory
            partition_dir = self.output_dir / 'tickers' / f'locale={locale}' / f'type={ticker_type}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / 'data.parquet'

            # If file exists, merge with existing data
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                # Merge based on ticker symbol (deduplicate)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal")
                if 'ticker' in partition_df.columns:
                    partition_df = partition_df.unique(subset=['ticker'], keep='last')

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} records to {output_file}")

    async def download_ticker_details_batch(
        self,
        tickers: List[str],
        date: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Download ticker details for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            date: Optional date for historical details (YYYY-MM-DD)

        Returns:
            Polars DataFrame with ticker details
        """
        logger.info(f"Downloading ticker details for {len(tickers)} tickers")

        params = {}
        if date:
            params['date'] = format_date(date)

        # Build all requests
        requests = [
            {'endpoint': f'/v3/reference/tickers/{ticker.upper()}', 'params': params}
            for ticker in tickers
        ]

        # Execute all requests in parallel
        responses = await self.client.batch_request(requests)

        # Process responses
        all_data = []
        for ticker, response in zip(tickers, responses):
            if 'error' in response:
                logger.warning(f"Failed to get details for {ticker}: {response['error']}")
                continue

            results = response.get('results', {})
            if not results:
                continue

            # Add metadata
            results['downloaded_at'] = datetime.now().isoformat()
            results['request_id'] = response.get('request_id')

            all_data.append(results)

        if not all_data:
            logger.warning("No ticker details found")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(all_data)
        logger.info(f"Downloaded details for {len(df)} tickers")

        # Save to parquet
        output_file = self.output_dir / f"ticker_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Saved to {output_file}")

        return df


async def main():
    """Example usage"""
    import sys
    from ..core.config_loader import ConfigLoader

    # Load configuration
    try:
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')

        if not credentials or 'api_key' not in credentials:
            print("❌ API key not found. Please configure config/credentials.yaml")
            sys.exit(1)

        # Create client with unlimited rate settings
        async with PolygonRESTClient(
            api_key=credentials['api_key'],
            max_concurrent=100,  # High parallelism
            max_connections=200
        ) as client:

            # Create downloader
            downloader = ReferenceDataDownloader(
                client=client,
                output_dir=Path('data/reference')
            )

            print("✅ ReferenceDataDownloader initialized\n")

            # Test: Download ticker types
            print("📥 Downloading ticker types...")
            ticker_types_df = await downloader.download_ticker_types()
            print(f"   Found {len(ticker_types_df)} ticker types")
            if len(ticker_types_df) > 0:
                print(ticker_types_df.head())

            # Test: Download related tickers for multiple stocks
            print("\n📥 Downloading related tickers (batch)...")
            test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
            related_df = await downloader.download_related_tickers_batch(test_tickers)
            print(f"   Found {len(related_df)} related ticker relationships")
            if len(related_df) > 0:
                print(related_df.head())

            # Statistics
            stats = client.get_statistics()
            print(f"\n📊 Statistics:")
            print(f"   Total requests: {stats['total_requests']}")
            print(f"   Total retries: {stats['total_retries']}")
            print(f"   Success rate: {stats['success_rate']:.1%}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
