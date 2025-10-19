"""
News Downloader - Financial news and sentiment data

High-performance downloader for Polygon news data with partitioning support.

Downloads:
- Ticker news articles
- Market news
- Publisher information
- Sentiment insights
- Keywords and related tickers

Partition structure: news/year=YYYY/month=MM/ticker=SYMBOL.parquet
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class NewsDownloader:
    """
    High-performance news downloader with date-first partitioning

    Optimized for unlimited API rate with parallel requests.
    Partitions news by published date and ticker for efficient querying.
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize news downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
            use_partitioned_structure: If True, save in date-first partitioned structure
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"NewsDownloader initialized (output: {output_dir}, partitioned: {use_partitioned_structure})")

    def _save_partitioned(
        self,
        df: pl.DataFrame,
        ticker: Optional[str] = None
    ) -> None:
        """
        Save DataFrame in date-first partitioned structure.

        Structure: output_dir/news/year=YYYY/month=MM/ticker=SYMBOL.parquet

        News articles can have multiple tickers, so we create a separate row for each ticker.

        Args:
            df: DataFrame to save (must have 'published_utc' and 'tickers' columns)
            ticker: Optional ticker filter (if provided, only save news for this ticker)
        """
        if len(df) == 0:
            return

        # Ensure required columns exist
        if 'published_utc' not in df.columns:
            logger.error("DataFrame missing 'published_utc' column")
            return

        # Handle tickers column - explode list to create one row per ticker
        if 'tickers' in df.columns:
            # Explode tickers list to create one row per ticker
            df = df.explode('tickers')
            df = df.rename({'tickers': 'ticker'})
        else:
            # If no tickers column but ticker was specified, add it
            if ticker:
                df = df.with_columns([
                    pl.lit(ticker.upper()).alias('ticker')
                ])
            else:
                logger.warning("No tickers column and no ticker specified, using 'UNKNOWN'")
                df = df.with_columns([
                    pl.lit('UNKNOWN').alias('ticker')
                ])

        # Filter by ticker if specified
        if ticker:
            df = df.filter(pl.col('ticker') == ticker.upper())

        # Filter out null tickers and dates
        df = df.filter(
            pl.col('ticker').is_not_null() &
            pl.col('published_utc').is_not_null()
        )

        if len(df) == 0:
            logger.warning("No valid data to save after filtering")
            return

        # Parse published_utc and extract year/month
        # published_utc format: "2024-06-24T18:33:53Z"
        if df.schema['published_utc'] == pl.String:
            df = df.with_columns([
                pl.col('published_utc').str.to_datetime("%Y-%m-%dT%H:%M:%SZ").alias('_datetime_parsed')
            ])
        else:
            df = df.with_columns([
                pl.col('published_utc').cast(pl.Datetime).alias('_datetime_parsed')
            ])

        df = df.with_columns([
            pl.col('_datetime_parsed').dt.year().cast(pl.Int32).alias('year'),
            pl.col('_datetime_parsed').dt.month().cast(pl.Int32).alias('month'),
        ]).drop('_datetime_parsed')

        # Get unique year/month/ticker combinations
        partitions = df.select(['year', 'month', 'ticker']).unique()

        for row in partitions.iter_rows(named=True):
            year = row['year']
            month = row['month']
            ticker_name = row['ticker']

            # Filter for this partition
            partition_df = df.filter(
                (pl.col('year') == year) &
                (pl.col('month') == month) &
                (pl.col('ticker') == ticker_name)
            ).drop(['year', 'month'])

            # Create partition directory: news/year=2024/month=10/ticker=AAPL.parquet
            partition_dir = self.output_dir / 'news' / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker_name}.parquet'

            # If file exists, append to it (diagonal_relaxed concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")
                # Deduplicate by article ID if present
                if 'id' in partition_df.columns:
                    partition_df = partition_df.unique(subset=['id'], keep='last')

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} news articles to {output_file}")

    async def download_ticker_news(
        self,
        ticker: Optional[str] = None,
        limit: int = 1000,
        published_utc_gte: Optional[str] = None,
        published_utc_lte: Optional[str] = None,
        order: str = 'desc',
        sort: str = 'published_utc'
    ) -> pl.DataFrame:
        """
        Download news for a ticker or all tickers

        Args:
            ticker: Ticker symbol (optional, downloads all news if not specified)
            limit: Results per page (max 1000)
            published_utc_gte: Filter news published on or after this date (YYYY-MM-DD)
            published_utc_lte: Filter news published on or before this date (YYYY-MM-DD)
            order: Sort order ('asc' or 'desc')
            sort: Sort field (default: 'published_utc')

        Returns:
            Polars DataFrame with news articles
        """
        logger.info(f"Downloading news for {ticker or 'all tickers'}")

        endpoint = '/v2/reference/news'
        params = {
            'limit': limit,
            'order': order,
            'sort': sort
        }

        if ticker:
            params['ticker'] = ticker.upper()
        if published_utc_gte:
            params['published_utc.gte'] = published_utc_gte
        if published_utc_lte:
            params['published_utc.lte'] = published_utc_lte

        # Fetch all pages
        results = await self.client.paginate_all(endpoint, params)

        if not results:
            logger.warning(f"No news found for {ticker or 'all tickers'}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} news articles")

        # Save to parquet
        if self.use_partitioned_structure:
            self._save_partitioned(df, ticker)
        else:
            output_file = self.output_dir / f"news_{ticker or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_news_batch(
        self,
        tickers: List[str],
        published_utc_gte: Optional[str] = None,
        published_utc_lte: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, int]:
        """
        Download news for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            published_utc_gte: Filter news published on or after this date
            published_utc_lte: Filter news published on or before this date
            limit: Results per page

        Returns:
            Dictionary with total count of articles downloaded
        """
        logger.info(f"Downloading news for {len(tickers)} tickers in parallel")

        # Download all tickers in parallel
        tasks = [
            self.download_ticker_news(
                ticker=ticker,
                published_utc_gte=published_utc_gte,
                published_utc_lte=published_utc_lte,
                limit=limit
            )
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count total records
        total_count = 0
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download news for {ticker}: {result}")
                continue
            total_count += len(result)

        logger.info(f"Downloaded {total_count} total news articles for {len(tickers)} tickers")

        return {'total_articles': total_count}


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
            downloader = NewsDownloader(
                client=client,
                output_dir=Path('data/news')
            )

            print("✅ NewsDownloader initialized\n")

            # Test: Download news for AAPL
            print("📰 Downloading news for AAPL (last 30 days)...")
            from datetime import date, timedelta

            end_date = date.today()
            start_date = end_date - timedelta(days=30)

            df = await downloader.download_ticker_news(
                ticker='AAPL',
                published_utc_gte=start_date.strftime('%Y-%m-%d'),
                published_utc_lte=end_date.strftime('%Y-%m-%d'),
                limit=100
            )

            if len(df) > 0:
                print(f"\n📊 Downloaded {len(df)} articles")
                print("\nSample articles:")
                print(df.select(['published_utc', 'title', 'author']).head(5))

                # Show sentiment insights if available
                if 'insights' in df.columns:
                    print("\n💡 Sentiment insights available")

            # Test: Batch download for multiple tickers
            print("\n📰 Downloading news for multiple tickers...")
            batch_results = await downloader.download_news_batch(
                tickers=['AAPL', 'MSFT', 'GOOGL'],
                published_utc_gte=start_date.strftime('%Y-%m-%d'),
                published_utc_lte=end_date.strftime('%Y-%m-%d')
            )

            print(f"\n📊 Total articles: {batch_results['total_articles']}")

            # Statistics
            stats = client.get_statistics()
            print(f"\n📊 API Statistics:")
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
