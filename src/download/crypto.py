"""Crypto Downloader - Cryptocurrency market data"""

import polars as pl
from pathlib import Path
from typing import Optional
from datetime import datetime, date, timedelta
import logging
from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class CryptoDownloader:
    """High-performance crypto downloader"""

    def __init__(self, client: PolygonRESTClient, output_dir: Path):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CryptoDownloader initialized (output: {output_dir})")

    async def download_crypto_bars(
        self, from_crypto: str, to_currency: str = 'USD',
        multiplier: int = 1, timespan: str = 'day',
        from_date: Optional[str] = None, to_date: Optional[str] = None, limit: int = 5000
    ) -> pl.DataFrame:
        """Download crypto aggregate bars"""
        ticker = f"X:{from_crypto.upper()}{to_currency.upper()}"
        logger.info(f"Downloading crypto bars for {ticker}")

        if not to_date:
            to_date = date.today().strftime('%Y-%m-%d')
        if not from_date:
            from_date = (date.today() - timedelta(days=365)).strftime('%Y-%m-%d')

        endpoint = f'/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}'
        params = {'limit': limit}

        results = await self.client.paginate_all(endpoint, params)
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(ticker).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"bars_{from_crypto}_{to_currency}_{timespan}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} crypto bars")
        return df

    async def download_crypto_snapshot(self, ticker: str) -> pl.DataFrame:
        """Download crypto snapshot"""
        logger.info(f"Downloading crypto snapshot for {ticker}")

        endpoint = f'/v2/snapshot/locale/global/markets/crypto/tickers/{ticker.upper()}'
        response = await self.client.make_request(endpoint, {})
        results = response.get('ticker', {})
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame([results])
        df = df.with_columns([
            pl.lit(ticker.upper()).alias('ticker'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"snapshot_{ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
