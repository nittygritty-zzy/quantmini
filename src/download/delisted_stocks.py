"""
Delisted Stocks Downloader - Fix Survivorship Bias

This module downloads historical data for stocks that delisted during the
backtest period. This addresses survivorship bias where only active stocks
are included in the S3 flat files.

Usage:
    from src.download.delisted_stocks import DelistedStocksDownloader

    downloader = DelistedStocksDownloader()
    delisted = downloader.get_delisted_stocks("2024-01-01", "2025-10-06")
    downloader.download_historical_data(delisted)
"""

import os
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from polygon import RESTClient
import logging

logger = logging.getLogger(__name__)


class DelistedStocksDownloader:
    """
    Download historical data for delisted stocks using Polygon API

    This fixes survivorship bias by including stocks that failed during
    the backtest period (bankruptcy, acquisition, regulatory issues, etc.)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        output_dir: Path = None,
        rate_limit_delay: float = 0.25
    ):
        """
        Initialize delisted stocks downloader

        Args:
            api_key: Polygon API key (default: from POLYGON_API_KEY env var)
            output_dir: Output directory for parquet files (default: data/parquet)
            rate_limit_delay: Delay between API calls in seconds (default: 0.25 = 4/sec)
        """
        # Initialize Polygon client
        if api_key:
            self.client = RESTClient(api_key)
        else:
            self.client = RESTClient()  # Uses POLYGON_API_KEY env var

        # Set output directory
        if output_dir is None:
            self.output_dir = Path("data/parquet")
        else:
            self.output_dir = Path(output_dir)

        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Rate limiting
        self.rate_limit_delay = rate_limit_delay
        self._last_api_call = 0

        logger.info(f"DelistedStocksDownloader initialized")
        logger.info(f"Output directory: {self.output_dir}")

    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self._last_api_call
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_api_call = time.time()

    def get_delisted_stocks(
        self,
        start_date: str,
        end_date: str,
        limit: int = 10000
    ) -> List[Dict[str, str]]:
        """
        Query Polygon API for stocks delisted during date range

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of delisted stocks to check (default: 10000)

        Returns:
            List of delisted stock dictionaries with keys:
                - ticker: Stock symbol
                - name: Company name
                - delisted_date: Delisting date (YYYY-MM-DD)
                - exchange: Primary exchange

        Example:
            >>> downloader.get_delisted_stocks("2024-01-01", "2025-10-06")
            [
                {
                    'ticker': 'DISH',
                    'name': 'DISH Network Corp.',
                    'delisted_date': '2024-01-02',
                    'exchange': 'XNAS'
                },
                ...
            ]
        """
        logger.info(f"Querying delisted stocks from {start_date} to {end_date}")

        # Parse date range
        start_dt = datetime.fromisoformat(start_date + 'T00:00:00+00:00')
        end_dt = datetime.fromisoformat(end_date + 'T23:59:59+00:00')

        delisted_stocks = []
        total_checked = 0

        try:
            # Query delisted tickers (active=False)
            for ticker_obj in self.client.list_tickers(
                market="stocks",
                type="CS",  # Common stock only
                active=False,  # Delisted stocks
                limit=limit
            ):
                total_checked += 1

                # Progress logging every 100 stocks
                if total_checked % 100 == 0:
                    logger.info(f"Checked {total_checked} delisted stocks, found {len(delisted_stocks)} in period")

                # Check if has delisting date
                if not hasattr(ticker_obj, 'delisted_utc') or not ticker_obj.delisted_utc:
                    continue

                # Parse delisting date
                try:
                    delisted_date_str = ticker_obj.delisted_utc.replace('Z', '+00:00')
                    delisted_date = datetime.fromisoformat(delisted_date_str)

                    # Check if delisted during our period
                    if start_dt <= delisted_date <= end_dt:
                        delisted_stocks.append({
                            'ticker': ticker_obj.ticker,
                            'name': ticker_obj.name if hasattr(ticker_obj, 'name') else ticker_obj.ticker,
                            'delisted_date': delisted_date.strftime('%Y-%m-%d'),
                            'exchange': ticker_obj.primary_exchange if hasattr(ticker_obj, 'primary_exchange') else 'N/A',
                        })

                except Exception as e:
                    logger.debug(f"Error parsing delisting date for {ticker_obj.ticker}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error querying delisted stocks: {e}")
            raise

        logger.info(f"Found {len(delisted_stocks)} stocks delisted during {start_date} to {end_date}")
        logger.info(f"Total delisted stocks checked: {total_checked}")

        return delisted_stocks

    def download_historical_data(
        self,
        delisted_stocks: List[Dict[str, str]],
        start_date: str
    ) -> Dict[str, any]:
        """
        Download historical OHLCV data for delisted stocks

        Args:
            delisted_stocks: List of delisted stock dicts from get_delisted_stocks()
            start_date: Start date for historical data (YYYY-MM-DD)

        Returns:
            Dictionary with download statistics:
                - success: Number of successful downloads
                - no_data: Number of stocks with no data
                - errors: Number of errors
                - total: Total stocks processed

        Example:
            >>> delisted = downloader.get_delisted_stocks("2024-01-01", "2025-10-06")
            >>> stats = downloader.download_historical_data(delisted, "2024-01-01")
            >>> print(f"Downloaded {stats['success']} stocks")
        """
        logger.info(f"Downloading historical data for {len(delisted_stocks)} delisted stocks")

        stats = {
            'success': 0,
            'no_data': 0,
            'errors': 0,
            'total': len(delisted_stocks)
        }

        for i, stock in enumerate(delisted_stocks, 1):
            ticker = stock['ticker']
            delisted_date = stock['delisted_date']

            # Progress logging every 10 stocks
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(delisted_stocks)} - {ticker}")

            try:
                # Rate limiting
                self._rate_limit()

                # Download daily bars from start_date to delisting date
                bars = []
                for bar in self.client.list_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan="day",
                    from_=start_date,
                    to=delisted_date,
                    limit=50000
                ):
                    bars.append({
                        'date': datetime.fromtimestamp(bar.timestamp / 1000).strftime('%Y-%m-%d'),
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vwap': bar.vwap if hasattr(bar, 'vwap') else (bar.high + bar.low + bar.close) / 3,
                    })

                if len(bars) == 0:
                    logger.debug(f"No data available for {ticker}")
                    stats['no_data'] += 1
                    continue

                # Convert to DataFrame
                df = pd.DataFrame(bars)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')

                # Save to parquet
                parquet_file = self.output_dir / f"{ticker}.parquet"
                df.to_parquet(parquet_file, index=False)

                logger.debug(f"Saved {len(bars)} days of data for {ticker} to {parquet_file}")
                stats['success'] += 1

            except Exception as e:
                logger.error(f"Error downloading {ticker}: {e}")
                stats['errors'] += 1
                continue

        logger.info(f"Download complete: {stats['success']}/{stats['total']} successful")
        logger.info(f"No data: {stats['no_data']}, Errors: {stats['errors']}")

        return stats

    def save_delisted_list(
        self,
        delisted_stocks: List[Dict[str, str]],
        output_file: Path = None
    ):
        """
        Save delisted stocks list to CSV for reference

        Args:
            delisted_stocks: List of delisted stock dicts
            output_file: Output CSV file path (default: data/delisted_stocks.csv)
        """
        if output_file is None:
            output_file = Path("data/delisted_stocks.csv")

        df = pd.DataFrame(delisted_stocks).sort_values('delisted_date')
        df.to_csv(output_file, index=False)

        logger.info(f"Saved delisted stocks list to {output_file}")
        logger.info(f"Total stocks: {len(delisted_stocks)}")

        return output_file


def main():
    """Command-line interface for delisted stocks downloader"""
    import argparse

    parser = argparse.ArgumentParser(description="Download delisted stocks data")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="data/parquet", help="Output directory")
    parser.add_argument("--limit", type=int, default=10000, help="Max stocks to check")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Initialize downloader
    downloader = DelistedStocksDownloader(output_dir=args.output_dir)

    # Get delisted stocks
    delisted = downloader.get_delisted_stocks(
        args.start_date,
        args.end_date,
        limit=args.limit
    )

    # Save list
    downloader.save_delisted_list(delisted)

    # Download historical data
    stats = downloader.download_historical_data(delisted, args.start_date)

    print("\n" + "=" * 70)
    print("DOWNLOAD COMPLETE")
    print("=" * 70)
    print(f"Delisted stocks found: {len(delisted)}")
    print(f"Successfully downloaded: {stats['success']}")
    print(f"No data: {stats['no_data']}")
    print(f"Errors: {stats['errors']}")
    print("=" * 70)


if __name__ == '__main__':
    main()
