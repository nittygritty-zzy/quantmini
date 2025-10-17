"""
Base Polygon API Client

Provides authenticated HTTP client for Polygon.io REST API endpoints.
"""

import httpx
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, date
import logging
from pathlib import Path

from ..core.exceptions import APIError

logger = logging.getLogger(__name__)


class PolygonAPIClient:
    """
    Base client for Polygon.io REST API

    Features:
    - Async HTTP requests with connection pooling
    - Rate limiting and retry logic
    - Authentication handling
    - Response validation

    API Documentation: https://polygon.io/docs/stocks/getting-started
    """

    BASE_URL = "https://api.polygon.io"

    def __init__(
        self,
        api_key: str,
        max_retries: int = 3,
        timeout: int = 30,
        rate_limit_calls: int = 5,  # calls per second for free tier
        rate_limit_period: float = 1.0
    ):
        """
        Initialize Polygon API client

        Args:
            api_key: Polygon.io API key
            max_retries: Maximum retry attempts for failed requests
            timeout: Request timeout in seconds
            rate_limit_calls: Maximum API calls per period
            rate_limit_period: Rate limit period in seconds
        """
        self.api_key = api_key
        self.max_retries = max_retries
        self.timeout = timeout
        self.rate_limit_calls = rate_limit_calls
        self.rate_limit_period = rate_limit_period

        # Rate limiting
        self._call_times: List[float] = []
        self._rate_limit_lock = asyncio.Lock()

        # HTTP client (created in async context)
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=self.timeout,
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=50
            )
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._client:
            await self._client.aclose()

    async def _wait_for_rate_limit(self):
        """Enforce rate limiting"""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()

            # Remove old call times
            self._call_times = [
                t for t in self._call_times
                if now - t < self.rate_limit_period
            ]

            # Wait if we've hit the limit
            if len(self._call_times) >= self.rate_limit_calls:
                sleep_time = self.rate_limit_period - (now - self._call_times[0])
                if sleep_time > 0:
                    logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                    await asyncio.sleep(sleep_time)
                    # Recheck after sleeping
                    now = asyncio.get_event_loop().time()
                    self._call_times = [
                        t for t in self._call_times
                        if now - t < self.rate_limit_period
                    ]

            # Record this call
            self._call_times.append(now)

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make authenticated API request with retry logic

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            **kwargs: Additional httpx request arguments

        Returns:
            API response as dictionary

        Raises:
            APIError: If request fails after retries
        """
        if not self._client:
            raise APIError("Client not initialized. Use async context manager.")

        # Add API key to params
        if params is None:
            params = {}
        params['apiKey'] = self.api_key

        # Retry loop
        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                await self._wait_for_rate_limit()

                # Make request
                response = await self._client.request(
                    method=method,
                    url=endpoint,
                    params=params,
                    **kwargs
                )

                # Check for errors
                response.raise_for_status()

                # Parse response
                data = response.json()

                # Check for API errors
                if 'status' in data and data['status'] == 'ERROR':
                    error_msg = data.get('error', 'Unknown API error')
                    raise APIError(f"Polygon API error: {error_msg}")

                return data

            except httpx.HTTPStatusError as e:
                logger.warning(
                    f"HTTP error {e.response.status_code} on attempt {attempt + 1}/{self.max_retries}: {e}"
                )
                if attempt == self.max_retries - 1:
                    raise APIError(f"Request failed after {self.max_retries} attempts: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

            except httpx.RequestError as e:
                logger.warning(
                    f"Request error on attempt {attempt + 1}/{self.max_retries}: {e}"
                )
                if attempt == self.max_retries - 1:
                    raise APIError(f"Request failed after {self.max_retries} attempts: {e}")
                await asyncio.sleep(2 ** attempt)

        raise APIError("Request failed for unknown reason")

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make GET request"""
        return await self._request('GET', endpoint, params=params)

    async def get_aggregates(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        from_date: date,
        to_date: date,
        adjusted: bool = True,
        sort: str = 'asc',
        limit: int = 50000
    ) -> Dict[str, Any]:
        """
        Get aggregate bars for a ticker over a date range

        API: https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to

        Args:
            ticker: Ticker symbol
            multiplier: Size of timespan multiplier (e.g., 1 for 1 day, 5 for 5 minutes)
            timespan: Size of time window (minute, hour, day, week, month, quarter, year)
            from_date: Start date
            to_date: End date
            adjusted: Whether results are adjusted for splits
            sort: Sort order (asc or desc)
            limit: Limit results (max 50000)

        Returns:
            API response with results array
        """
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"

        params = {
            'adjusted': str(adjusted).lower(),
            'sort': sort,
            'limit': limit
        }

        return await self.get(endpoint, params=params)

    async def get_ticker_details(self, ticker: str, date: Optional[date] = None) -> Dict[str, Any]:
        """
        Get details about a ticker symbol

        API: https://polygon.io/docs/stocks/get_v3_reference_tickers__ticker

        Args:
            ticker: Ticker symbol
            date: Optional date for historical details

        Returns:
            Ticker details
        """
        endpoint = f"/v3/reference/tickers/{ticker}"

        params = {}
        if date:
            params['date'] = str(date)

        return await self.get(endpoint, params=params)

    async def get_options_chain(
        self,
        underlying_ticker: str,
        expiration_date: Optional[date] = None,
        strike_price: Optional[float] = None,
        contract_type: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Get options contracts for an underlying ticker

        API: https://polygon.io/docs/options/get_v3_reference_options_contracts

        Args:
            underlying_ticker: Underlying ticker symbol
            expiration_date: Filter by expiration date
            strike_price: Filter by strike price
            contract_type: Filter by contract type (call or put)
            limit: Limit results

        Returns:
            Options contracts
        """
        endpoint = "/v3/reference/options/contracts"

        params = {
            'underlying_ticker': underlying_ticker,
            'limit': limit
        }

        if expiration_date:
            params['expiration_date'] = str(expiration_date)
        if strike_price:
            params['strike_price'] = strike_price
        if contract_type:
            params['contract_type'] = contract_type

        return await self.get(endpoint, params=params)
