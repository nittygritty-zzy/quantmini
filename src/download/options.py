"""Options Downloader - Options contracts and market data"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from .polygon_rest_client import PolygonRESTClient

logger = logging.getLogger(__name__)


class OptionsDownloader:
    """High-performance options data downloader"""

    def __init__(self, client: PolygonRESTClient, output_dir: Path):
        self.client = client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"OptionsDownloader initialized (output: {output_dir})")

    async def download_contracts(
        self, underlying_ticker: Optional[str] = None,
        contract_type: Optional[str] = None,
        expiration_date: Optional[str] = None,
        limit: int = 1000
    ) -> pl.DataFrame:
        """Download options contracts"""
        logger.info(f"Downloading options contracts for {underlying_ticker or 'all'}")

        endpoint = '/v3/reference/options/contracts'
        params = {'limit': limit}
        if underlying_ticker:
            params['underlying_ticker'] = underlying_ticker.upper()
        if contract_type:
            params['contract_type'] = contract_type
        if expiration_date:
            params['expiration_date'] = expiration_date

        results = await self.client.paginate_all(endpoint, params)
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        output_file = self.output_dir / f"contracts_{underlying_ticker or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} contracts")
        return df

    async def download_options_chain(
        self, underlying_ticker: str,
        expiration_date: Optional[str] = None,
        strike_price: Optional[float] = None
    ) -> pl.DataFrame:
        """Download options chain snapshot"""
        logger.info(f"Downloading options chain for {underlying_ticker}")

        endpoint = f'/v3/snapshot/options/{underlying_ticker.upper()}'
        params = {}
        if expiration_date:
            params['expiration_date'] = expiration_date
        if strike_price:
            params['strike_price'] = strike_price

        response = await self.client.make_request(endpoint, params)
        results = response.get('results', [])
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame(results)
        df = df.with_columns([
            pl.lit(underlying_ticker.upper()).alias('underlying'),
            pl.lit(datetime.now()).alias('downloaded_at')
        ])

        output_file = self.output_dir / f"chain_{underlying_ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        logger.info(f"Downloaded {len(df)} options in chain")
        return df

    async def download_contract_details(self, options_ticker: str) -> pl.DataFrame:
        """Download single options contract details"""
        logger.info(f"Downloading contract details for {options_ticker}")

        endpoint = f'/v3/reference/options/contracts/{options_ticker.upper()}'
        response = await self.client.make_request(endpoint, {})
        results = response.get('results', {})
        if not results:
            return pl.DataFrame()

        df = pl.DataFrame([results])
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))

        output_file = self.output_dir / f"contract_{options_ticker.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.write_parquet(output_file, compression='zstd')
        return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
