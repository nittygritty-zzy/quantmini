"""Trades & Quotes Downloader - Tick-level market data"""

import polars as pl
import asyncio
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging
from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class TradesQuotesDownloader:
    """High-performance trades and quotes downloader"""

    def __init__(self, client: PolygonRESTClient, output_dir: Path):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"TradesQuotesDownloader initialized (output: {output_dir})")

    async def download_trades(
        self, ticker: str, date: str, limit: int = 50000
    ) -> pl.DataFrame:
        """Download trades for a ticker on a specific date"""
        logger.info(f"Downloading trades for {ticker} on {date}")

        endpoint = f'/v3/trades/{ticker.upper()}'
        params = {'timestamp': date, 'limit': limit}

        results = await self.client.paginate_all(endpoint, params)
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"trades_{ticker.lower()}_{date}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} trades")
        return df

    async def download_quotes(
        self, ticker: str, date: str, limit: int = 50000
    ) -> pl.DataFrame:
        """Download quotes for a ticker on a specific date"""
        logger.info(f"Downloading quotes for {ticker} on {date}")

        endpoint = f'/v3/quotes/{ticker.upper()}'
        params = {'timestamp': date, 'limit': limit}

        results = await self.client.paginate_all(endpoint, params)
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"quotes_{ticker.lower()}_{date}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} quotes")
        return df

    async def download_last_trade(self, ticker: str) -> pl.DataFrame:
        """Download last trade for a ticker"""
        logger.info(f"Downloading last trade for {ticker}")

        endpoint = f'/v2/last/trade/{ticker.upper()}'
        response = await self.client.make_request(endpoint, {})
        results = response.get('results', {})
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame([results])
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"last_trade_{ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        return df

    async def download_last_quote(self, ticker: str) -> pl.DataFrame:
        """Download last quote for a ticker"""
        logger.info(f"Downloading last quote for {ticker}")

        endpoint = f'/v2/last/nbbo/{ticker.upper()}'
        response = await self.client.make_request(endpoint, {})
        results = response.get('results', {})
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame([results])
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"last_quote_{ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
