"""
Financial Ratios Downloader - Download pre-calculated ratios from Polygon API

Downloads financial ratios directly from Polygon's /vX/reference/financials endpoint
with financial_statement_type='financial_ratios'

Ratios include:
- Profitability (ROE, ROA, ROIC, margins)
- Liquidity (current ratio, quick ratio, cash ratio)
- Leverage (debt to equity, debt to assets, interest coverage)
- Efficiency (asset turnover, inventory turnover, receivables turnover)
- Market valuation (P/E, P/B, EV/EBITDA)
- Growth rates (revenue growth, earnings growth)

Reference: https://polygon.io/docs/rest/stocks/fundamentals/ratios
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class FinancialRatiosAPIDownloader:
    """
    Download pre-calculated financial ratios from Polygon API

    Reads from: Polygon REST API /vX/reference/financials (financial_statement_type='financial_ratios')

    Saves to: {output_dir}/financial_ratios/year=YYYY/month=MM/ticker=SYMBOL.parquet
    """

    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        """
        Initialize financial ratios downloader

        Args:
            client: Polygon REST API client
            output_dir: Directory to save parquet files
            use_partitioned_structure: If True, save in date-first partitioned structure
        """
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"FinancialRatiosAPIDownloader initialized (output: {output_dir}, partitioned: {use_partitioned_structure})")

    def _save_partitioned(
        self,
        df: pl.DataFrame,
        ticker: str
    ) -> None:
        """
        Save DataFrame in date-first partitioned structure.

        Structure: output_dir/financial_ratios/year=YYYY/month=MM/ticker=SYMBOL.parquet

        Args:
            df: DataFrame to save
            ticker: Ticker symbol
        """
        if len(df) == 0:
            return

        # Extract ticker from tickers list column if present
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

        # Determine date column (try 'date' first, then 'filing_date')
        date_col = 'date' if 'date' in df.columns else 'filing_date'

        # Filter out null tickers and dates
        df = df.filter(
            pl.col('ticker_extracted').is_not_null() &
            pl.col(date_col).is_not_null()
        )

        if len(df) == 0:
            return

        # Parse date and extract year/month
        if df.schema[date_col] == pl.String:
            df = df.with_columns([
                pl.col(date_col).str.to_date("%Y-%m-%d").alias('_date_parsed')
            ])
        else:
            df = df.with_columns([
                pl.col(date_col).cast(pl.Date).alias('_date_parsed')
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
            partition_dir = self.output_dir / 'financial_ratios' / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)

            output_file = partition_dir / f'ticker={ticker_name}.parquet'

            # If file exists, append to it (diagonal_relaxed concat to handle schema differences)
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")

            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} records to {output_file}")

    async def download_ratios(
        self,
        ticker: Optional[str] = None,
        cik: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download financial ratios from Polygon API

        Uses the /stocks/financials/v1/ratios endpoint which returns
        pre-calculated ratios like ROE, ROA, P/E, etc.

        Args:
            ticker: Ticker symbol
            cik: CIK number
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Results per page

        Returns:
            Polars DataFrame with financial ratios data
        """
        logger.info(f"Downloading financial ratios (ticker={ticker})")

        # Build query parameters
        params = {'limit': limit}

        if ticker:
            params['ticker'] = ticker.upper()
        if cik:
            params['cik'] = cik
        if start_date:
            params['date.gte'] = start_date
        if end_date:
            params['date.lt'] = end_date

        # Fetch all pages from the ratios endpoint
        results = await self.client.paginate_all('/stocks/financials/v1/ratios', params)

        if not results:
            logger.warning(f"No financial ratios found for {ticker}")
            return pl.DataFrame()

        # Convert to DataFrame
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        logger.info(f"Downloaded {len(df)} financial ratio records")

        # Save to parquet
        if self.use_partitioned_structure and ticker:
            self._save_partitioned(df, ticker)
        elif not self.use_partitioned_structure:
            output_file = self.output_dir / f"financial_ratios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.write_parquet(output_file, compression='zstd')
            logger.info(f"Saved to {output_file}")

        return df

    async def download_ratios_batch(
        self,
        tickers: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Download financial ratios for multiple tickers in parallel

        Args:
            tickers: List of ticker symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dictionary with count of downloaded ratios
        """
        date_info = ""
        if start_date or end_date:
            date_info = f" (date: {start_date or 'beginning'} to {end_date or 'today'})"

        logger.info(f"Downloading financial ratios for {len(tickers)} tickers in parallel{date_info}")

        # Download all tickers in parallel
        tasks = [
            self.download_ratios(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date
            )
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count total records
        total_count = 0
        success_count = 0

        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download ratios for {ticker}: {result}")
                continue

            total_count += len(result)
            success_count += 1

        logger.info(
            f"Downloaded financial ratios for {success_count}/{len(tickers)} tickers: "
            f"{total_count} total records"
        )

        return {
            'financial_ratios': total_count,
            'successful_tickers': success_count,
            'total_tickers': len(tickers)
        }


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
            downloader = FinancialRatiosAPIDownloader(
                client=client,
                output_dir=Path('data/bronze/fundamentals')
            )

            print("✅ FinancialRatiosAPIDownloader initialized\n")

            # Test: Download ratios for AAPL
            print("📥 Downloading financial ratios for AAPL...")
            ratios = await downloader.download_ratios('AAPL', timeframe='quarterly')

            if len(ratios) > 0:
                print(f"\n✅ Downloaded {len(ratios)} ratio records")
                print(f"Columns: {ratios.columns[:10]}...")  # Show first 10 columns
            else:
                print("❌ No ratios downloaded")

            # Test: Batch download
            print("\n📥 Downloading ratios for multiple tickers...")
            batch_result = await downloader.download_ratios_batch(
                ['AAPL', 'MSFT', 'GOOGL'],
                timeframe='quarterly',
                filing_date_gte='2020-01-01'
            )

            print(f"Downloaded {batch_result['financial_ratios']} total records")
            print(f"Success rate: {batch_result['successful_tickers']}/{batch_result['total_tickers']}")

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
