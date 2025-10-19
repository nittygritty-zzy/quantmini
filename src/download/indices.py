"""Indices Downloader - Stock market indices data"""

import polars as pl
from pathlib import Path
from typing import Optional
from datetime import datetime, date, timedelta
import logging
from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class IndicesDownloader:
    """High-performance indices downloader"""

    def __init__(self, client: PolygonRESTClient, output_dir: Path):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"IndicesDownloader initialized (output: {output_dir})")

    async def download_index_snapshot(self, ticker: str) -> pl.DataFrame:
        """Download snapshot for an index"""
        logger.info(f"Downloading index snapshot for {ticker}")

        endpoint = f'/v3/snapshot/indices/{ticker.upper()}'
        response = await self.client.make_request(endpoint, {})
        results = response.get('results', {})
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame([results])
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"snapshot_{ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded index snapshot for {ticker}")
        return df

    async def download_index_bars(
        self, ticker: str, multiplier: int = 1, timespan: str = 'day',
        from_date: Optional[str] = None, to_date: Optional[str] = None, limit: int = 5000
    ) -> pl.DataFrame:
        """Download aggregate bars for an index"""
        logger.info(f"Downloading {timespan} bars for index {ticker}")

        if not to_date:
            to_date = date.today().strftime('%Y-%m-%d')
        if not from_date:
            from_date = (date.today() - timedelta(days=365)).strftime('%Y-%m-%d')

        endpoint = f'/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}'
        params = {'limit': limit}

        results = await self.client.paginate_all(endpoint, params)
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"bars_{ticker.lower()}_{timespan}_{from_date}_{to_date}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} bars for index {ticker}")
        return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
