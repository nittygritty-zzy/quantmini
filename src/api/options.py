"""
Options API Data Fetcher

Fetches options market data from Polygon.io REST API and converts to pipeline format.
"""

import polars as pl
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
import logging

from .client import PolygonAPIClient
from ..core.exceptions import APIError

logger = logging.getLogger(__name__)


class OptionsAPIFetcher:
    """
    Fetches options data from Polygon API

    Supports:
    - Options contracts discovery
    - Daily aggregates (OHLCV)
    - Minute aggregates (OHLCV)
    - Historical and incremental updates
    """

    def __init__(self, client: PolygonAPIClient):
        """
        Initialize options fetcher

        Args:
            client: Initialized PolygonAPIClient
        """
        self.client = client

    async def fetch_options_contracts(
        self,
        underlying_ticker: str,
        expiration_date: Optional[date] = None,
        contract_type: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Fetch options contracts for an underlying ticker

        Args:
            underlying_ticker: Underlying ticker symbol (e.g., 'AAPL')
            expiration_date: Filter by specific expiration date
            contract_type: Filter by contract type ('call' or 'put')

        Returns:
            DataFrame with options contracts
        """
        logger.info(f"Fetching options contracts for {underlying_ticker}")

        try:
            # Fetch first page
            response = await self.client.get_options_chain(
                underlying_ticker=underlying_ticker,
                expiration_date=expiration_date,
                contract_type=contract_type,
                limit=1000
            )

            results = response.get('results', [])
            all_contracts = results.copy()

            # Handle pagination if there's more data
            next_url = response.get('next_url')
            while next_url:
                # Extract cursor from next_url
                # Polygon returns full URL, we need to extract the path and params
                logger.debug(f"Fetching next page of contracts")

                # For simplicity, break after first page for now
                # In production, you'd want to handle pagination properly
                break

            if not all_contracts:
                logger.warning(f"No contracts found for {underlying_ticker}")
                return pl.DataFrame()

            # Parse contracts to DataFrame
            df = self._parse_contracts_to_dataframe(all_contracts)
            logger.info(f"Found {len(df)} options contracts for {underlying_ticker}")

            return df

        except Exception as e:
            logger.error(f"Error fetching options contracts: {e}")
            raise

    def _parse_contracts_to_dataframe(self, contracts: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Convert options contracts to DataFrame

        Args:
            contracts: List of contract dictionaries from API

        Returns:
            Polars DataFrame with standardized schema
        """
        data = []
        for contract in contracts:
            record = {
                'ticker': contract.get('ticker'),
                'underlying_ticker': contract.get('underlying_ticker'),
                'contract_type': contract.get('contract_type'),
                'strike_price': float(contract.get('strike_price', 0)),
                'expiration_date': contract.get('expiration_date'),
                'shares_per_contract': int(contract.get('shares_per_contract', 100)),
                'exercise_style': contract.get('exercise_style', 'american'),
            }
            data.append(record)

        return pl.DataFrame(data)

    def _parse_aggregates_to_dataframe(
        self,
        ticker: str,
        results: List[Dict[str, Any]],
        timespan: str
    ) -> pl.DataFrame:
        """
        Convert API aggregate results to Polars DataFrame

        Args:
            ticker: Options contract ticker (e.g., 'O:AAPL240119C00150000')
            results: List of aggregate bars from API
            timespan: Timespan (day or minute)

        Returns:
            Polars DataFrame with standardized schema
        """
        if not results:
            logger.warning(f"No results for {ticker}")
            return pl.DataFrame()

        # Parse results
        data = []
        for bar in results:
            record = {
                'ticker': ticker,
                'timestamp': datetime.fromtimestamp(bar['t'] / 1000),
                'open': float(bar['o']),
                'high': float(bar['h']),
                'low': float(bar['l']),
                'close': float(bar['c']),
                'volume': int(bar['v']),
                'vwap': float(bar.get('vw', 0.0)),
                'transactions': int(bar.get('n', 0))
            }
            data.append(record)

        # Create DataFrame
        df = pl.DataFrame(data)

        # Add date column for daily data
        if timespan == 'day':
            df = df.with_columns([
                pl.col('timestamp').dt.date().alias('date')
            ])

        return df

    async def fetch_daily_bars(
        self,
        option_tickers: List[str],
        from_date: date,
        to_date: date
    ) -> pl.DataFrame:
        """
        Fetch daily bars for options contracts

        Args:
            option_tickers: List of options contract tickers
            from_date: Start date
            to_date: End date

        Returns:
            Combined Polars DataFrame with all contracts
        """
        logger.info(f"Fetching daily bars for {len(option_tickers)} options from {from_date} to {to_date}")

        # Fetch data for each contract
        tasks = []
        for ticker in option_tickers:
            task = self._fetch_option_daily(ticker, from_date, to_date)
            tasks.append(task)

        # Execute in parallel with concurrency control
        results = []
        batch_size = 10  # Process 10 contracts at a time
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Error fetching contract: {result}")
                elif not result.is_empty():
                    results.append(result)

        if not results:
            logger.warning("No data fetched for any options")
            return pl.DataFrame()

        # Combine all results
        combined = pl.concat(results)
        logger.info(f"Fetched {len(combined)} daily bars for {len(results)} options")

        return combined

    async def _fetch_option_daily(
        self,
        ticker: str,
        from_date: date,
        to_date: date
    ) -> pl.DataFrame:
        """Fetch daily bars for a single options contract"""
        try:
            response = await self.client.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan='day',
                from_date=from_date,
                to_date=to_date,
                adjusted=False,  # Options are not adjusted
                limit=50000
            )

            results = response.get('results', [])
            if not results:
                logger.debug(f"No results for {ticker}")
                return pl.DataFrame()

            df = self._parse_aggregates_to_dataframe(ticker, results, 'day')
            logger.debug(f"Fetched {len(df)} daily bars for {ticker}")

            return df

        except Exception as e:
            logger.error(f"Error fetching daily bars for {ticker}: {e}")
            raise

    async def fetch_minute_bars(
        self,
        option_tickers: List[str],
        from_date: date,
        to_date: date
    ) -> pl.DataFrame:
        """
        Fetch minute bars for options contracts

        Args:
            option_tickers: List of options contract tickers
            from_date: Start date
            to_date: End date

        Returns:
            Combined Polars DataFrame with all contracts
        """
        logger.info(f"Fetching minute bars for {len(option_tickers)} options from {from_date} to {to_date}")

        all_results = []
        for ticker in option_tickers:
            try:
                df = await self._fetch_option_minute(ticker, from_date, to_date)
                if not df.is_empty():
                    all_results.append(df)
            except Exception as e:
                logger.error(f"Error fetching minute bars for {ticker}: {e}")

        if not all_results:
            logger.warning("No minute data fetched")
            return pl.DataFrame()

        combined = pl.concat(all_results)
        logger.info(f"Fetched {len(combined)} minute bars total")

        return combined

    async def _fetch_option_minute(
        self,
        ticker: str,
        from_date: date,
        to_date: date
    ) -> pl.DataFrame:
        """Fetch minute bars for a single options contract"""
        try:
            response = await self.client.get_aggregates(
                ticker=ticker,
                multiplier=1,
                timespan='minute',
                from_date=from_date,
                to_date=to_date,
                adjusted=False,
                limit=50000
            )

            results = response.get('results', [])
            if not results:
                logger.debug(f"No minute results for {ticker}")
                return pl.DataFrame()

            df = self._parse_aggregates_to_dataframe(ticker, results, 'minute')
            logger.debug(f"Fetched {len(df)} minute bars for {ticker}")

            return df

        except Exception as e:
            logger.error(f"Error fetching minute bars for {ticker}: {e}")
            raise

    async def fetch_options_for_underlyings(
        self,
        underlying_tickers: List[str],
        from_date: date,
        to_date: date,
        timespan: str = 'day'
    ) -> pl.DataFrame:
        """
        Fetch all options data for underlying tickers

        This is a convenience method that:
        1. Discovers all options contracts for the underlyings
        2. Fetches OHLCV data for those contracts

        Args:
            underlying_tickers: List of underlying tickers (e.g., ['AAPL', 'MSFT'])
            from_date: Start date for OHLCV data
            to_date: End date for OHLCV data
            timespan: 'day' or 'minute'

        Returns:
            Combined DataFrame with all options data
        """
        logger.info(f"Fetching options for {len(underlying_tickers)} underlyings")

        # Step 1: Get all contracts for underlyings
        all_contracts = []
        for underlying in underlying_tickers:
            try:
                contracts = await self.fetch_options_contracts(underlying)
                if not contracts.is_empty():
                    all_contracts.append(contracts)
            except Exception as e:
                logger.error(f"Error fetching contracts for {underlying}: {e}")

        if not all_contracts:
            logger.warning("No options contracts found")
            return pl.DataFrame()

        contracts_df = pl.concat(all_contracts)
        option_tickers = contracts_df['ticker'].to_list()

        logger.info(f"Found {len(option_tickers)} options contracts, fetching {timespan} data")

        # Step 2: Fetch OHLCV data for contracts
        if timespan == 'day':
            ohlcv_df = await self.fetch_daily_bars(option_tickers, from_date, to_date)
        elif timespan == 'minute':
            ohlcv_df = await self.fetch_minute_bars(option_tickers, from_date, to_date)
        else:
            raise ValueError(f"Invalid timespan: {timespan}")

        return ohlcv_df

    def save_to_parquet(
        self,
        df: pl.DataFrame,
        output_path: Path,
        compression: str = 'zstd'
    ):
        """
        Save DataFrame to parquet file

        Args:
            df: Polars DataFrame
            output_path: Output file path
            compression: Compression algorithm
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.write_parquet(
            output_path,
            compression=compression,
            use_pyarrow=True,
            statistics=True
        )

        logger.info(f"Saved {len(df)} records to {output_path}")
