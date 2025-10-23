"""
Polygon REST API Client - High-performance HTTP client using httpx

This module provides a high-performance HTTP client for Polygon.io REST API
optimized for unlimited API rate with parallel requests.

Features:
- HTTP/2 connection pooling via httpx
- Massive parallel requests (configurable concurrency)
- Automatic pagination with parallel fetching
- Retry logic with exponential backoff
- Response validation
"""

import httpx
import asyncio
from typing import Dict, List, Any, Optional, AsyncIterator, Callable
from datetime import datetime, date
import logging
from pathlib import Path
import json

from ..core.exceptions import PolygonAPIError

logger = logging.getLogger(__name__)


class PolygonRESTClient:
    """
    High-performance async client for Polygon.io REST API

    Optimized for unlimited API rate with:
    - HTTP/2 connection pooling
    - Massive parallel requests (100+ concurrent)
    - Connection reuse
    - Automatic retry with backoff
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        max_concurrent: int = 100,
        max_connections: int = 200,
        max_retries: int = 3,
        timeout: int = 30,
        enable_http2: bool = True,
        enable_adaptive_throttling: bool = True
    ):
        """
        Initialize Polygon REST API client

        Args:
            api_key: Polygon.io API key
            base_url: Base URL for API
            max_concurrent: Max concurrent requests (unlimited rate = high value)
            max_connections: Max HTTP connections in pool
            max_retries: Maximum retry attempts
            timeout: Request timeout in seconds
            enable_http2: Enable HTTP/2 for better performance
            enable_adaptive_throttling: Enable automatic concurrency reduction on errors
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.enable_adaptive_throttling = enable_adaptive_throttling

        # Create async HTTP client with connection pooling
        limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections // 2
        )

        self.client = httpx.AsyncClient(
            base_url=base_url,
            limits=limits,
            timeout=httpx.Timeout(timeout),
            http2=enable_http2,
            follow_redirects=True
        )

        # Semaphore for concurrency control
        self._current_concurrent = max_concurrent
        self._min_concurrent = max(1, max_concurrent // 10)  # Never go below 10% of max
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Adaptive throttling tracking
        self._connection_errors = 0
        self._error_threshold = 5  # Throttle after 5 connection errors
        self._throttle_cooldown = 0  # Seconds since last throttle
        self._recovery_interval = 30  # Seconds between recovery attempts
        self._last_adjustment = 0  # Timestamp of last adjustment

        # Statistics
        self.total_requests = 0
        self.total_retries = 0
        self.total_errors = 0
        self.total_connection_errors = 0
        self.total_throttles = 0
        self._stats_lock = asyncio.Lock()

        logger.info(
            f"PolygonRESTClient initialized "
            f"(max_concurrent={max_concurrent}, http2={enable_http2}, "
            f"adaptive_throttling={enable_adaptive_throttling})"
        )

    async def close(self):
        """Close HTTP client and cleanup"""
        await self.client.aclose()

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def _throttle_down(self):
        """Reduce concurrency due to connection errors"""
        if not self.enable_adaptive_throttling:
            return

        async with self._stats_lock:
            import time
            current_time = time.time()

            # Avoid throttling too frequently
            if current_time - self._last_adjustment < 10:
                return

            old_concurrent = self._current_concurrent
            # Reduce by 50%, but not below minimum
            new_concurrent = max(self._min_concurrent, self._current_concurrent // 2)

            if new_concurrent < old_concurrent:
                self._current_concurrent = new_concurrent
                self._last_adjustment = current_time
                self.total_throttles += 1

                logger.warning(
                    f"⚠️  Adaptive throttling: reducing concurrency "
                    f"{old_concurrent} → {new_concurrent} due to connection errors"
                )

                # Create new semaphore with reduced limit
                # Note: Can't modify existing semaphore, but new requests will use current limit
                self.semaphore = asyncio.Semaphore(new_concurrent)

                # Reset error counter after throttling
                self._connection_errors = 0

    async def _try_recover(self):
        """Gradually increase concurrency if things are stable"""
        if not self.enable_adaptive_throttling:
            return

        async with self._stats_lock:
            import time
            current_time = time.time()

            # Only try recovery if we're throttled and enough time has passed
            if self._current_concurrent >= self.max_concurrent:
                return

            if current_time - self._last_adjustment < self._recovery_interval:
                return

            old_concurrent = self._current_concurrent
            # Increase by 25%, but not above maximum
            new_concurrent = min(self.max_concurrent, int(self._current_concurrent * 1.25))

            if new_concurrent > old_concurrent:
                self._current_concurrent = new_concurrent
                self._last_adjustment = current_time

                logger.info(
                    f"✨ Adaptive recovery: increasing concurrency "
                    f"{old_concurrent} → {new_concurrent}"
                )

                # Create new semaphore with increased limit
                self.semaphore = asyncio.Semaphore(new_concurrent)

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> Dict[str, Any]:
        """
        Make HTTP request to Polygon API with retry logic

        Args:
            endpoint: API endpoint path (e.g., '/v3/reference/tickers/types')
            params: Query parameters
            method: HTTP method (GET, POST, etc.)

        Returns:
            JSON response as dictionary

        Raises:
            PolygonAPIError: If request fails after retries
        """
        if params is None:
            params = {}

        # Add API key to params
        params['apiKey'] = self.api_key

        async with self.semaphore:
            for attempt in range(1, self.max_retries + 1):
                try:
                    async with self._stats_lock:
                        self.total_requests += 1

                    response = await self.client.request(
                        method=method,
                        url=endpoint,
                        params=params
                    )

                    # Check HTTP status
                    if response.status_code == 429:
                        # Rate limited (shouldn't happen with unlimited, but handle it)
                        retry_after = int(response.headers.get('Retry-After', 10))
                        logger.warning(
                            f"Rate limited (429), waiting {retry_after}s before retry"
                        )
                        await asyncio.sleep(retry_after)
                        async with self._stats_lock:
                            self.total_retries += 1
                        continue

                    elif response.status_code >= 500:
                        # Server error, retry with backoff
                        if attempt < self.max_retries:
                            wait_time = min(2 ** attempt, 30)  # Cap at 30s
                            logger.warning(
                                f"Server error ({response.status_code}), "
                                f"retrying in {wait_time}s (attempt {attempt}/{self.max_retries})"
                            )
                            await asyncio.sleep(wait_time)
                            async with self._stats_lock:
                                self.total_retries += 1
                            continue
                        else:
                            async with self._stats_lock:
                                self.total_errors += 1
                            raise PolygonAPIError(
                                f"Server error {response.status_code} after {self.max_retries} retries"
                            )

                    elif response.status_code >= 400:
                        # Client error, don't retry
                        async with self._stats_lock:
                            self.total_errors += 1
                        error_text = response.text
                        raise PolygonAPIError(
                            f"Client error {response.status_code}: {error_text}"
                        )

                    # Success - parse JSON
                    data = response.json()

                    # Check API response status
                    if data.get('status') == 'ERROR':
                        error_msg = data.get('error', 'Unknown error')
                        async with self._stats_lock:
                            self.total_errors += 1
                        raise PolygonAPIError(f"API error: {error_msg}")

                    logger.debug(f"Request successful: {endpoint}")

                    # Try to recover concurrency if we're throttled and stable
                    await self._try_recover()

                    return data

                except httpx.HTTPError as e:
                    # Check if it's a connection error that should trigger throttling
                    is_connection_error = (
                        "ConnectionTerminated" in str(type(e).__name__) or
                        "ConnectionTerminated" in str(e) or
                        "RemoteProtocolError" in str(type(e).__name__) or
                        "Connection" in str(e)
                    )

                    if is_connection_error:
                        async with self._stats_lock:
                            self.total_connection_errors += 1
                            self._connection_errors += 1

                        # Trigger throttling if too many connection errors
                        if self._connection_errors >= self._error_threshold:
                            await self._throttle_down()

                    if attempt < self.max_retries:
                        wait_time = min(2 ** attempt, 30)
                        logger.warning(
                            f"HTTP error: {e}, retrying in {wait_time}s "
                            f"(attempt {attempt}/{self.max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        async with self._stats_lock:
                            self.total_retries += 1
                        continue
                    else:
                        async with self._stats_lock:
                            self.total_errors += 1
                        raise PolygonAPIError(f"Request failed after {self.max_retries} retries: {e}")

                except Exception as e:
                    async with self._stats_lock:
                        self.total_errors += 1
                    raise PolygonAPIError(f"Unexpected error: {e}")

            # Should not reach here
            raise PolygonAPIError("Request failed")

    async def make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> Dict[str, Any]:
        """
        Public wrapper for making requests (used by downloaders)

        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method

        Returns:
            JSON response as dictionary
        """
        return await self._make_request(endpoint, params, method)

    async def _make_request_raw_url(self, url: str) -> Dict[str, Any]:
        """
        Make request with full URL (for pagination next_url)

        Args:
            url: Full URL with params (will add API key if missing)

        Returns:
            JSON response as dictionary
        """
        # Add API key if not already in URL
        if 'apiKey=' not in url and 'api_key=' not in url:
            separator = '&' if '?' in url else '?'
            url = f"{url}{separator}apiKey={self.api_key}"

        async with self.semaphore:
            async with self._stats_lock:
                self.total_requests += 1

            response = await self.client.get(url)

            if response.status_code != 200:
                async with self._stats_lock:
                    self.total_errors += 1
                raise PolygonAPIError(f"Request failed with status {response.status_code}")

            return response.json()

    async def paginate_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
        parallel_pages: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages in parallel with aggressive optimization

        Args:
            endpoint: API endpoint path
            params: Query parameters
            max_pages: Maximum pages to fetch (None = all)
            parallel_pages: Number of pages to fetch in parallel

        Returns:
            List of all result items across all pages
        """
        if params is None:
            params = {}

        all_results = []

        # Fetch first page to get total and next_url
        first_response = await self._make_request(endpoint, params)
        results = first_response.get('results', [])
        all_results.extend(results)

        logger.info(f"Fetched page 1: {len(results)} items")

        next_url = first_response.get('next_url')
        if not next_url:
            logger.info("No more pages, returning results")
            return all_results

        # Parallel pagination: fetch multiple pages at once
        pages_fetched = 1
        while next_url and (max_pages is None or pages_fetched < max_pages):
            # Collect next_urls for parallel fetch
            next_urls = [next_url]

            # Fetch more next_urls by making requests in parallel
            # This pre-fetches page metadata to get more next_urls
            for _ in range(parallel_pages - 1):
                if max_pages and pages_fetched + len(next_urls) >= max_pages:
                    break

                # Fetch next page to get its next_url
                try:
                    temp_response = await self._make_request_raw_url(next_urls[-1])
                    next_url = temp_response.get('next_url')
                    if next_url:
                        next_urls.append(next_url)
                    else:
                        break
                except Exception as e:
                    logger.error(f"Error pre-fetching next_url: {e}")
                    break

            # Now fetch all these pages in parallel
            tasks = [self._make_request_raw_url(url) for url in next_urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            # Process responses
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    logger.error(f"Failed to fetch page: {response}")
                    continue

                results = response.get('results', [])
                all_results.extend(results)
                pages_fetched += 1

                logger.info(
                    f"Fetched page {pages_fetched}: {len(results)} items "
                    f"(total: {len(all_results)})"
                )

                # Get next_url from last response
                if i == len(responses) - 1:
                    next_url = response.get('next_url')

        logger.info(f"Pagination complete: {pages_fetched} pages, {len(all_results)} total items")
        return all_results

    async def batch_request(
        self,
        requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple requests in parallel

        Args:
            requests: List of request configs, each with 'endpoint' and optional 'params'

        Returns:
            List of responses (in same order as requests)

        Example:
            requests = [
                {'endpoint': '/v1/related-companies/AAPL'},
                {'endpoint': '/v1/related-companies/MSFT'},
                {'endpoint': '/v1/related-companies/GOOGL'},
            ]
            responses = await client.batch_request(requests)
        """
        logger.info(f"Executing {len(requests)} requests in parallel")

        async def execute_request(req: Dict[str, Any]) -> Dict[str, Any]:
            try:
                return await self._make_request(
                    req['endpoint'],
                    req.get('params')
                )
            except Exception as e:
                logger.error(f"Request failed for {req['endpoint']}: {e}")
                return {'error': str(e), 'request': req}

        # Execute all in parallel
        responses = await asyncio.gather(*[execute_request(req) for req in requests])

        success_count = sum(1 for r in responses if 'error' not in r)
        logger.info(f"Completed {success_count}/{len(requests)} requests successfully")

        return responses

    def get_statistics(self) -> Dict[str, Any]:
        """Get client statistics including adaptive throttling metrics"""
        return {
            'total_requests': self.total_requests,
            'total_retries': self.total_retries,
            'total_errors': self.total_errors,
            'total_connection_errors': self.total_connection_errors,
            'total_throttles': self.total_throttles,
            'current_concurrency': self._current_concurrent,
            'max_concurrency': self.max_concurrent,
            'success_rate': (
                (self.total_requests - self.total_errors) / self.total_requests
                if self.total_requests > 0 else 0.0
            ),
            'throttled': self._current_concurrent < self.max_concurrent
        }

    def reset_statistics(self):
        """Reset statistics counters"""
        self.total_requests = 0
        self.total_retries = 0
        self.total_errors = 0
        self.total_connection_errors = 0
        self.total_throttles = 0
        self._connection_errors = 0


# Convenience function to format dates
def format_date(d: Optional[date]) -> Optional[str]:
    """Format date as YYYY-MM-DD string"""
    if d is None:
        return None
    if isinstance(d, str):
        return d
    return d.strftime('%Y-%m-%d')
