#!/usr/bin/env python3
"""
Batch download ALL screener data for common stocks (10,070 tickers)

Downloads 8 data types:
- Fundamentals: balance_sheets, income_statements, cash_flow
- Calculated: financial_ratios (121 ratios from fundamentals)
- Corporate Actions: dividends, splits, ticker_events
- Reference Data: related_tickers

PLUS (Optional Phase 4):
- Ticker Metadata: Comprehensive lookup table with ~100K tickers
  - Bulk download all ticker info (active + inactive)
  - Enrich universe tickers with detailed company data
  - Compute fundamentals status (is_operating_company, has_fundamentals)
  - Output: data/reference/ticker_metadata.parquet

Features:
- Parallel processing with 8 workers (configurable)
- Idempotent: safe to re-run, skips already downloaded tickers
- Progress tracking with checkpointing
- Automatic retry with exponential backoff
- Support for refilling individual stocks
- Real-time progress monitoring
- Comprehensive data coverage (all 8 tables + metadata)
- Aggressive parallelization for metadata (100-200 concurrent requests)

Usage:
    # Download all remaining tickers (skips processed)
    python scripts/batch_load_fundamentals_all.py

    # Download fundamentals + ticker metadata (Phase 4)
    python scripts/batch_load_fundamentals_all.py --download-metadata

    # Force re-download specific tickers
    python scripts/batch_load_fundamentals_all.py --refill AAPL MSFT GOOGL

    # Download with different worker count
    python scripts/batch_load_fundamentals_all.py --workers 4

    # Start from specific batch
    python scripts/batch_load_fundamentals_all.py --start-batch 50

    # Force re-download all (NOT recommended)
    python scripts/batch_load_fundamentals_all.py --force

    # Customize metadata download concurrency
    python scripts/batch_load_fundamentals_all.py --download-metadata --metadata-bulk-concurrency 50 --metadata-detail-concurrency 100
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import List, Set, Dict, Optional
from datetime import datetime
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import httpx
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.download import FundamentalsDownloader, PolygonRESTClient
from src.download.financial_ratios_downloader import FinancialRatiosDownloader
from src.download.corporate_actions import CorporateActionsDownloader
from src.download.reference_data import ReferenceDataDownloader
from src.core.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ProgressTracker:
    """Track and persist download progress"""

    def __init__(self, progress_file: Path):
        self.progress_file = progress_file
        self.data = self._load()

    def _load(self) -> Dict:
        """Load progress from file"""
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                return json.load(f)
        return {
            'completed': [],
            'failed': {},
            'started_at': None,
            'last_updated': None,
            'total_tickers': 0,
            'workers': 0
        }

    def save(self):
        """Save progress to file"""
        self.data['last_updated'] = datetime.now().isoformat()
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)

    def mark_completed(self, ticker: str):
        """Mark ticker as completed"""
        if ticker not in self.data['completed']:
            self.data['completed'].append(ticker)
        # Remove from failed if it was there
        if ticker in self.data['failed']:
            del self.data['failed'][ticker]
        self.save()

    def mark_failed(self, ticker: str, error: str):
        """Mark ticker as failed"""
        self.data['failed'][ticker] = {
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        }
        self.save()

    def get_completed(self) -> Set[str]:
        """Get set of completed tickers"""
        return set(self.data['completed'])

    def get_failed(self) -> Dict[str, Dict]:
        """Get failed tickers with error info"""
        return self.data['failed']


def get_already_downloaded_tickers(data_dir: Path) -> Set[str]:
    """
    Get set of tickers that already have fundamentals downloaded

    Checks for existence of balance_sheets files as indicator
    """
    downloaded = set()
    bs_dir = data_dir / 'balance_sheets'

    if bs_dir.exists():
        for file in bs_dir.rglob('ticker=*.parquet'):
            ticker = file.stem.replace('ticker=', '')
            downloaded.add(ticker.upper())

    return downloaded


async def download_ticker_fundamentals(
    ticker: str,
    api_key: str,
    output_dir: Path,
    max_retries: int = 3
) -> Dict[str, any]:
    """
    Download ALL screener data for a single ticker with retry logic

    Downloads 8 data types:
    - Fundamentals: balance_sheets, income_statements, cash_flow
    - Calculated: financial_ratios
    - Corporate Actions: dividends, splits, ticker_events
    - Reference Data: related_tickers

    Returns:
        Dict with status, ticker, and error info
    """
    result = {
        'ticker': ticker,
        'success': False,
        'fundamentals_downloaded': False,
        'ratios_calculated': False,
        'corporate_actions_downloaded': False,
        'reference_data_downloaded': False,
        'error': None,
        'records': {
            'bs': 0, 'is': 0, 'cf': 0, 'ratios': 0,
            'dividends': 0, 'splits': 0, 'ticker_events': 0, 'related_tickers': 0
        }
    }

    for attempt in range(max_retries):
        try:
            # Download fundamentals
            async with PolygonRESTClient(api_key=api_key) as client:
                downloader = FundamentalsDownloader(
                    client=client,
                    output_dir=output_dir,
                    use_partitioned_structure=True
                )

                # Download all three statement types
                bs = await downloader.download_balance_sheets(ticker)
                income = await downloader.download_income_statements(ticker)
                cf = await downloader.download_cash_flow_statements(ticker)

                result['records']['bs'] = len(bs) if bs is not None else 0
                result['records']['is'] = len(income) if income is not None else 0
                result['records']['cf'] = len(cf) if cf is not None else 0

                # Check if we got any data
                if result['records']['bs'] > 0 or result['records']['is'] > 0 or result['records']['cf'] > 0:
                    result['fundamentals_downloaded'] = True
                else:
                    result['error'] = 'No fundamentals data returned by API'
                    continue

            # Calculate ratios
            if result['fundamentals_downloaded']:
                ratio_downloader = FinancialRatiosDownloader(
                    input_dir=output_dir,
                    output_dir=output_dir,
                    use_partitioned_structure=True
                )

                ratios = await ratio_downloader.calculate_ratios_for_ticker(ticker)
                result['records']['ratios'] = len(ratios) if ratios is not None else 0

                if result['records']['ratios'] > 0:
                    result['ratios_calculated'] = True

            # Download corporate actions (dividends, splits, ticker_events)
            async with PolygonRESTClient(api_key=api_key) as client:
                corp_actions_downloader = CorporateActionsDownloader(
                    client=client,
                    output_dir=output_dir,
                    use_partitioned_structure=True
                )

                # Dividends
                try:
                    dividends = await corp_actions_downloader.download_dividends(ticker=ticker)
                    result['records']['dividends'] = len(dividends) if dividends is not None else 0
                except Exception as e:
                    logger.debug(f"{ticker}: Dividends download failed: {e}")
                    result['records']['dividends'] = 0

                # Splits
                try:
                    splits = await corp_actions_downloader.download_stock_splits(ticker=ticker)
                    result['records']['splits'] = len(splits) if splits is not None else 0
                except Exception as e:
                    logger.debug(f"{ticker}: Splits download failed: {e}")
                    result['records']['splits'] = 0

                # Ticker events
                try:
                    events = await corp_actions_downloader.download_ticker_events(ticker)
                    result['records']['ticker_events'] = len(events) if events is not None else 0
                except Exception as e:
                    logger.debug(f"{ticker}: Ticker events download failed: {e}")
                    result['records']['ticker_events'] = 0

                # Mark corporate actions as downloaded if we got any data
                if (result['records']['dividends'] > 0 or
                    result['records']['splits'] > 0 or
                    result['records']['ticker_events'] > 0):
                    result['corporate_actions_downloaded'] = True

            # Download related tickers
            async with PolygonRESTClient(api_key=api_key) as client:
                ref_data_downloader = ReferenceDataDownloader(
                    client=client,
                    output_dir=output_dir,
                    use_partitioned_structure=True
                )

                try:
                    related = await ref_data_downloader.download_related_tickers_batch([ticker])
                    result['records']['related_tickers'] = len(related) if related is not None else 0
                    if result['records']['related_tickers'] > 0:
                        result['reference_data_downloaded'] = True
                except Exception as e:
                    logger.debug(f"{ticker}: Related tickers download failed: {e}")
                    result['records']['related_tickers'] = 0

            # Success if we got fundamentals (corporate actions and reference data are optional)
            result['success'] = result['fundamentals_downloaded']
            break

        except Exception as e:
            result['error'] = str(e)
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = 2 ** attempt
                logger.warning(f"{ticker}: Attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"{ticker}: All {max_retries} attempts failed: {e}")

    return result


def process_ticker_sync(ticker: str, api_key: str, output_dir: Path) -> Dict[str, any]:
    """Synchronous wrapper for process pool executor"""
    return asyncio.run(download_ticker_fundamentals(ticker, api_key, output_dir))


async def process_batch(
    tickers: List[str],
    api_key: str,
    output_dir: Path,
    progress: ProgressTracker,
    batch_num: int,
    total_batches: int
) -> Dict[str, int]:
    """
    Process a batch of tickers in parallel using asyncio

    Returns:
        Dict with batch statistics
    """
    logger.info(f"{'='*80}")
    logger.info(f"BATCH {batch_num}/{total_batches}: Processing {len(tickers)} tickers")
    logger.info(f"{'='*80}")

    start_time = time.time()

    # Process all tickers in batch concurrently
    tasks = [
        download_ticker_fundamentals(ticker, api_key, output_dir)
        for ticker in tickers
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    stats = {
        'success': 0,
        'failed': 0,
        'fundamentals_only': 0,
        'with_ratios': 0,
        'no_data': 0
    }

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Task exception: {result}")
            stats['failed'] += 1
            continue

        ticker = result['ticker']

        if result['success']:
            progress.mark_completed(ticker)
            stats['success'] += 1

            # Build concise summary
            r = result['records']
            summary = f"BS={r['bs']}, IS={r['is']}, CF={r['cf']}, Ratios={r['ratios']}"

            # Add corporate actions if present
            corp_parts = []
            if r['dividends'] > 0:
                corp_parts.append(f"Div={r['dividends']}")
            if r['splits'] > 0:
                corp_parts.append(f"Spl={r['splits']}")
            if r['ticker_events'] > 0:
                corp_parts.append(f"Evt={r['ticker_events']}")
            if corp_parts:
                summary += f", {', '.join(corp_parts)}"

            # Add related tickers if present
            if r['related_tickers'] > 0:
                summary += f", Related={r['related_tickers']}"

            if result['ratios_calculated']:
                stats['with_ratios'] += 1
                logger.info(f"  ✅ {ticker}: {summary}")
            else:
                stats['fundamentals_only'] += 1
                logger.info(f"  🟡 {ticker}: {summary} (no ratios)")
        else:
            progress.mark_failed(ticker, result['error'] or 'Unknown error')
            if 'No fundamentals data' in str(result['error']):
                stats['no_data'] += 1
                logger.warning(f"  ⚪ {ticker}: No data available")
            else:
                stats['failed'] += 1
                logger.error(f"  ❌ {ticker}: {result['error']}")

    elapsed = time.time() - start_time

    logger.info(f"\nBatch {batch_num} Summary:")
    logger.info(f"  Duration: {elapsed:.1f}s")
    logger.info(f"  Success: {stats['success']}/{len(tickers)} ({stats['success']/len(tickers)*100:.1f}%)")
    logger.info(f"  With ratios: {stats['with_ratios']}")
    logger.info(f"  Fundamentals only: {stats['fundamentals_only']}")
    logger.info(f"  No data: {stats['no_data']}")
    logger.info(f"  Failed: {stats['failed']}")

    return stats


class TickerMetadataDownloader:
    """
    Download comprehensive ticker metadata from Polygon API

    Uses aggressive parallelization with httpx for unlimited API rate
    """

    def __init__(
        self,
        api_key: str,
        output_dir: Path,
        bulk_concurrency: int = 100,
        detail_concurrency: int = 200,
    ):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / 'ticker_metadata.parquet'
        self.bulk_concurrency = bulk_concurrency
        self.detail_concurrency = detail_concurrency

        # Base URL
        self.base_url = 'https://api.polygon.io'

    async def download_all_tickers_bulk(self) -> pl.DataFrame:
        """
        Phase 1: Download basic metadata for ALL tickers using cursor-based pagination

        API: /v3/reference/tickers
        Returns: ~100K ticker records
        Time: ~30 seconds with unlimited rate
        """
        logger.info("Phase 1: Downloading bulk ticker metadata...")

        all_tickers = []

        # HTTP/2 with connection pooling
        limits = httpx.Limits(
            max_connections=self.bulk_concurrency,
            max_keepalive_connections=50
        )

        async with httpx.AsyncClient(
            limits=limits,
            http2=True,
            timeout=30.0
        ) as client:
            # Download for both active and inactive tickers
            for active in ['true', 'false']:
                logger.info(f"  Downloading {active} tickers...")

                url = f"{self.base_url}/v3/reference/tickers"
                params = {
                    'market': 'stocks',
                    'active': active,
                    'limit': 1000,
                    'apiKey': self.api_key
                }

                page_count = 0
                next_url = None

                while True:
                    # Use next_url if available, otherwise use base params
                    if next_url:
                        # Extract cursor from next_url and use it
                        request_url = next_url
                        # Add API key to the next_url
                        if '?' in request_url:
                            request_url += f"&apiKey={self.api_key}"
                        else:
                            request_url += f"?apiKey={self.api_key}"
                        response = await client.get(request_url)
                    else:
                        response = await client.get(url, params=params)

                    response.raise_for_status()
                    data = response.json()

                    results = data.get('results', [])
                    all_tickers.extend(results)
                    page_count += 1

                    # Check for more pages
                    next_url = data.get('next_url')
                    if not next_url:
                        break

                    # Show progress every 10 pages
                    if page_count % 10 == 0:
                        logger.info(f"    Progress: {len(all_tickers):,} tickers ({page_count} pages)")

                logger.info(f"  Downloaded {page_count} pages for {active} tickers")

        logger.info(f"  Total downloaded: {len(all_tickers):,} tickers")

        # Convert to DataFrame
        df = pl.DataFrame(all_tickers)

        # Add download timestamp
        df = df.with_columns(
            pl.lit(datetime.now()).alias('downloaded_at')
        )

        return df

    async def enrich_tickers(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Phase 2: Enrich specific tickers with detailed information

        API: /v3/reference/tickers/{ticker}
        Input: List of tickers to enrich
        Returns: Dict of ticker -> detailed info
        Time: ~2-3 minutes for 10K tickers with unlimited rate
        """
        logger.info(f"Phase 2: Enriching {len(tickers):,} tickers with detailed info...")

        enriched_data = {}

        # HTTP/2 with aggressive connection pooling
        limits = httpx.Limits(
            max_connections=self.detail_concurrency,
            max_keepalive_connections=100
        )

        async with httpx.AsyncClient(
            limits=limits,
            http2=True,
            timeout=30.0
        ) as client:
            # Create tasks for all tickers
            async def fetch_ticker_details(ticker: str):
                url = f"{self.base_url}/v3/reference/tickers/{ticker}"
                params = {'apiKey': self.api_key}

                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    return ticker, data.get('results', {})
                except Exception as e:
                    logger.debug(f"  {ticker}: Failed to enrich: {e}")
                    return ticker, {}

            # Process in batches to show progress
            batch_size = 1000
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i + batch_size]
                tasks = [fetch_ticker_details(ticker) for ticker in batch]
                results = await asyncio.gather(*tasks)

                for ticker, details in results:
                    if details:
                        enriched_data[ticker] = details

                logger.info(f"  Progress: {min(i + batch_size, len(tickers)):,}/{len(tickers):,} tickers")

        logger.info(f"  Enriched {len(enriched_data):,} tickers")
        return enriched_data

    async def fetch_ticker_types(self, tickers: List[str]) -> Dict[str, List[Dict]]:
        """
        Phase 3: Fetch global ticker types reference data

        API: /v3/reference/tickers/types (GLOBAL endpoint, not per-ticker)

        NOTE: This is a GLOBAL reference endpoint that returns all available
        ticker types (CS, ETF, ADRC, etc.) for the asset_class and locale.
        It does NOT return types for individual tickers.

        Returns: Dict mapping each ticker to the global types list (same for all)
        Time: < 1 second (single API call)
        """
        logger.info(f"Phase 3: Fetching global ticker types reference...")

        ticker_types_data = {}

        # Single HTTP request to get global ticker types
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.base_url}/v3/reference/tickers/types"
            params = {
                'asset_class': 'stocks',
                'locale': 'us',
                'apiKey': self.api_key
            }

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                global_types = data.get('results', [])

                logger.info(f"  Fetched {len(global_types)} global ticker types")

                # NOTE: Since this is global reference data, we're mapping it to all tickers
                # In practice, you might want to filter based on the ticker's 'type' field
                # For now, we'll store the global types list for reference
                for ticker in tickers:
                    ticker_types_data[ticker] = global_types

            except Exception as e:
                logger.error(f"  Failed to fetch global ticker types: {e}")

        logger.info(f"  Mapped types to {len(ticker_types_data):,} tickers")
        return ticker_types_data

    async def fetch_related_companies(self, tickers: List[str]) -> Dict[str, List[Dict]]:
        """
        Phase 4: Fetch related companies for specific tickers

        API: /v1/related-companies/{ticker} (CORRECT endpoint is v1, not v3)
        Input: List of tickers
        Returns: Dict of ticker -> list of related companies
        Time: ~1-2 minutes for 10K tickers with unlimited rate
        """
        logger.info(f"Phase 4: Fetching related companies for {len(tickers):,} tickers...")

        related_companies_data = {}

        # HTTP/2 with aggressive connection pooling
        limits = httpx.Limits(
            max_connections=self.detail_concurrency,
            max_keepalive_connections=100
        )

        async with httpx.AsyncClient(
            limits=limits,
            http2=True,
            timeout=30.0
        ) as client:
            # Create tasks for all tickers
            async def fetch_related(ticker: str):
                # FIXED: Use v1 endpoint, not v3
                url = f"{self.base_url}/v1/related-companies/{ticker}"
                params = {'apiKey': self.api_key}

                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    return ticker, data.get('results', [])
                except Exception as e:
                    logger.debug(f"  {ticker}: Failed to fetch related companies: {e}")
                    return ticker, []

            # Process in batches to show progress
            batch_size = 1000
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i + batch_size]
                tasks = [fetch_related(ticker) for ticker in batch]
                results = await asyncio.gather(*tasks)

                for ticker, related in results:
                    if related:
                        related_companies_data[ticker] = related

                logger.info(f"  Progress: {min(i + batch_size, len(tickers)):,}/{len(tickers):,} tickers")

        logger.info(f"  Fetched related companies for {len(related_companies_data):,} tickers")
        return related_companies_data

    async def run_all_phases(self) -> pl.DataFrame:
        """
        Run all phases of ticker metadata download using ONLY Polygon APIs

        Uses 4 Polygon endpoints:
        1. /v3/reference/tickers - bulk list (download_all_tickers_bulk)
        2. /v3/reference/tickers/{ticker} - detailed info (enrich_tickers)
        3. /v3/reference/tickers/{ticker}/types - ticker types (fetch_ticker_types)
        4. /v3/reference/related-companies/{ticker} - related companies (fetch_related_companies)

        Returns:
            Complete ticker metadata DataFrame partitioned by locale/type with all enrichments merged
        """
        phase_start = time.time()

        logger.info(f"{'='*80}")
        logger.info("TICKER METADATA DOWNLOAD - ALL 4 POLYGON ENDPOINTS")
        logger.info(f"{'='*80}")

        # Phase 1: Bulk download all tickers
        df_bulk = await self.download_all_tickers_bulk()

        # Filter to only active US common stocks for enrichment
        # User requirement: only enrich active stocks, ignore delisted, only US CS
        df_to_enrich = df_bulk.filter(
            (pl.col('active') == True) &
            (pl.col('locale') == 'us') &
            (pl.col('type') == 'CS')
        )
        enrichment_tickers = df_to_enrich.get_column('ticker').to_list()

        logger.info(f"\nTotal tickers downloaded: {len(df_bulk):,}")
        logger.info(f"Active US common stocks to enrich: {len(enrichment_tickers):,}")
        logger.info(f"Enriching {len(enrichment_tickers):,} tickers with detailed data...")

        # Phase 2: Enrich with detailed ticker info (only active US CS)
        enriched_data = await self.enrich_tickers(enrichment_tickers)

        # Phase 3: Fetch ticker types (only active US CS)
        ticker_types_data = await self.fetch_ticker_types(enrichment_tickers)

        # Phase 4: Fetch related companies (only active US CS)
        related_companies_data = await self.fetch_related_companies(enrichment_tickers)

        # Merge all data into bulk DataFrame
        logger.info("\nMerging enriched data into main DataFrame...")

        # Convert enriched_data dict to DataFrame and merge
        if enriched_data:
            enriched_rows = []
            for ticker, data in enriched_data.items():
                # Flatten nested data from ticker details
                row = {'ticker': ticker}
                # Add all fields from enriched data
                if isinstance(data, dict):
                    row.update(data)
                enriched_rows.append(row)

            df_enriched = pl.DataFrame(enriched_rows)
            # Merge with bulk data (left join to keep all bulk tickers)
            df_bulk = df_bulk.join(df_enriched, on='ticker', how='left', suffix='_detail')
            logger.info(f"  Merged detailed info for {len(enriched_data):,} tickers")

        # Add ticker types as JSON column
        if ticker_types_data:
            ticker_types_rows = [
                {'ticker': ticker, 'ticker_types': types}
                for ticker, types in ticker_types_data.items()
            ]
            df_types = pl.DataFrame(ticker_types_rows)
            df_bulk = df_bulk.join(df_types, on='ticker', how='left')
            logger.info(f"  Merged ticker types for {len(ticker_types_data):,} tickers")

        # Add related companies as JSON column
        if related_companies_data:
            related_rows = [
                {'ticker': ticker, 'related_companies': companies}
                for ticker, companies in related_companies_data.items()
            ]
            df_related = pl.DataFrame(related_rows)
            df_bulk = df_bulk.join(df_related, on='ticker', how='left')
            logger.info(f"  Merged related companies for {len(related_companies_data):,} tickers")

        # Save partitioned by locale then type
        logger.info("\nSaving enriched ticker metadata partitioned by locale then type...")

        if 'type' in df_bulk.columns and 'locale' in df_bulk.columns:
            # Filter out rows with null locale or type
            df_valid = df_bulk.filter(
                pl.col('locale').is_not_null() & pl.col('type').is_not_null()
            )

            # Check if we filtered out any rows
            null_count = len(df_bulk) - len(df_valid)
            if null_count > 0:
                logger.warning(f"  Filtered out {null_count} tickers with null locale or type")

            # Get unique combinations (sorted by locale first, then type)
            type_locale_combos = df_valid.select(['locale', 'type']).unique().sort(['locale', 'type'])
            logger.info(f"  Found {len(type_locale_combos)} locale/type combinations")

            # Save each combination separately
            for row in type_locale_combos.iter_rows(named=True):
                locale = row['locale']
                ticker_type = row['type']

                # Filter data for this combination (using proper null-safe comparison)
                combo_df = df_valid.filter(
                    (pl.col('locale') == locale) & (pl.col('type') == ticker_type)
                )

                # Create nested directory structure: locale=us/type=CS/data.parquet
                combo_output_dir = self.output_dir / f'locale={locale}' / f'type={ticker_type}'
                combo_output_dir.mkdir(parents=True, exist_ok=True)
                combo_output_file = combo_output_dir / 'data.parquet'

                combo_df.write_parquet(
                    combo_output_file,
                    compression='zstd',
                    compression_level=3
                )

                # Safe string formatting (locale and ticker_type are guaranteed non-null here)
                locale_str = str(locale) if locale else 'null'
                type_str = str(ticker_type) if ticker_type else 'null'
                logger.info(f"  {locale_str:5s} / {type_str:15s}: {len(combo_df):,} tickers -> {combo_output_file}")
        else:
            # Fallback: save single file if columns are missing
            logger.warning("  Missing 'type' or 'locale' columns, saving as single file")
            df_bulk.write_parquet(
                self.output_file,
                compression='zstd',
                compression_level=3
            )

        phase_elapsed = time.time() - phase_start
        logger.info(f"{'='*80}")
        logger.info(f"Ticker metadata download complete!")
        logger.info(f"  Total time: {phase_elapsed:.1f}s ({phase_elapsed/60:.1f} minutes)")
        logger.info(f"  Total tickers: {len(df_bulk):,}")
        logger.info(f"  Output directory: {self.output_dir}")
        logger.info(f"{'='*80}")

        return df_bulk


async def main_async(args):
    """Main async function"""

    # Load configuration
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')
    api_key = credentials['api']['key']
    output_dir = Path('data/partitioned_screener')

    # Initialize progress tracker
    progress_file = Path('data/download_progress.json')
    progress = ProgressTracker(progress_file)

    # Load target tickers
    if args.refill:
        # Refill mode: process specific tickers
        tickers = [t.upper() for t in args.refill]
        logger.info(f"REFILL MODE: Processing {len(tickers)} specific tickers")
    else:
        # Normal mode: process all from common_stocks.json
        common_stocks_file = Path('data/common_stocks.json')
        if not common_stocks_file.exists():
            logger.error(f"Common stocks file not found: {common_stocks_file}")
            return 1

        with open(common_stocks_file) as f:
            all_tickers = [t.upper() for t in json.load(f)]

        logger.info(f"Loaded {len(all_tickers)} tickers from {common_stocks_file}")

        # Filter out already processed tickers (unless --force)
        if args.force:
            tickers = all_tickers
            logger.info("FORCE MODE: Re-downloading all tickers")
        else:
            # Get already downloaded tickers
            already_downloaded = get_already_downloaded_tickers(output_dir)
            completed = progress.get_completed()
            skip_tickers = already_downloaded | completed

            tickers = [t for t in all_tickers if t not in skip_tickers]

            logger.info(f"Already downloaded: {len(already_downloaded)} tickers")
            logger.info(f"Already completed: {len(completed)} tickers")
            logger.info(f"To download: {len(tickers)} tickers")

    if not tickers:
        logger.info("✅ All tickers already downloaded!")
        return 0

    # Initialize progress tracking
    if progress.data['started_at'] is None:
        progress.data['started_at'] = datetime.now().isoformat()
        progress.data['total_tickers'] = len(tickers)
        progress.data['workers'] = args.workers
        progress.save()

    logger.info(f"{'='*80}")
    logger.info(f"LARGE-SCALE FUNDAMENTALS DOWNLOAD")
    logger.info(f"{'='*80}")
    logger.info(f"Total tickers: {len(tickers)}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Progress file: {progress_file}")
    logger.info(f"{'='*80}")

    # Create batches
    batches = [tickers[i:i + args.batch_size] for i in range(0, len(tickers), args.batch_size)]
    total_batches = len(batches)

    # Start from specific batch if requested
    start_idx = max(0, args.start_batch - 1)
    if start_idx > 0:
        logger.info(f"Starting from batch {args.start_batch} (skipping {start_idx} batches)")
        batches = batches[start_idx:]

    # Process batches
    overall_start = time.time()
    total_stats = {
        'success': 0,
        'failed': 0,
        'fundamentals_only': 0,
        'with_ratios': 0,
        'no_data': 0
    }

    for batch_idx, batch in enumerate(batches, start=start_idx + 1):
        batch_stats = await process_batch(
            batch,
            api_key,
            output_dir,
            progress,
            batch_idx,
            total_batches
        )

        # Update totals
        for key in total_stats:
            total_stats[key] += batch_stats[key]

        # Show progress
        completed_tickers = len(progress.get_completed())
        total_processed = completed_tickers + len(progress.get_failed())

        logger.info(f"\n{'='*80}")
        logger.info(f"OVERALL PROGRESS: {total_processed}/{len(tickers)} tickers ({total_processed/len(tickers)*100:.1f}%)")
        logger.info(f"Completed: {completed_tickers} | Failed: {len(progress.get_failed())}")
        logger.info(f"{'='*80}\n")

    # Phase 4: Download ticker metadata (if enabled)
    if args.download_metadata:
        try:
            logger.info(f"\n{'='*80}")
            logger.info("PHASE 4: TICKER METADATA DOWNLOAD")
            logger.info(f"{'='*80}")

            # Get list of tickers with fundamentals for enrichment
            completed_tickers = list(progress.get_completed())

            # Initialize metadata downloader
            metadata_output_dir = Path('data/reference')
            metadata_downloader = TickerMetadataDownloader(
                api_key=api_key,
                output_dir=metadata_output_dir,
                bulk_concurrency=args.metadata_bulk_concurrency,
                detail_concurrency=args.metadata_detail_concurrency,
            )

            # Run all phases (ONLY uses Polygon APIs - no fundamentals computation)
            await metadata_downloader.run_all_phases()

            logger.info("Ticker metadata download complete!")

        except Exception as e:
            logger.error(f"Ticker metadata download failed: {e}")
            logger.warning("Continuing with fundamentals summary...")

    # Final summary
    overall_elapsed = time.time() - overall_start

    logger.info(f"\n{'='*80}")
    logger.info(f"FINAL SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Total time: {overall_elapsed/3600:.2f} hours ({overall_elapsed/60:.1f} minutes)")
    logger.info(f"Total tickers processed: {len(tickers)}")
    logger.info(f"Success: {total_stats['success']} ({total_stats['success']/len(tickers)*100:.1f}%)")
    logger.info(f"  - With ratios: {total_stats['with_ratios']}")
    logger.info(f"  - Fundamentals only: {total_stats['fundamentals_only']}")
    logger.info(f"No data: {total_stats['no_data']}")
    logger.info(f"Failed: {total_stats['failed']}")
    logger.info(f"Average time per ticker: {overall_elapsed/len(tickers):.2f}s")
    logger.info(f"{'='*80}")

    # Show failed tickers
    failed = progress.get_failed()
    if failed:
        logger.info(f"\n❌ FAILED TICKERS ({len(failed)}):")
        for ticker, info in sorted(failed.items())[:20]:
            logger.info(f"  {ticker}: {info['error']}")
        if len(failed) > 20:
            logger.info(f"  ... and {len(failed) - 20} more")

    return 0 if total_stats['failed'] == 0 else 1


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Batch download fundamentals for all common stocks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all remaining tickers (default: 8 workers)
  python scripts/batch_load_fundamentals_all.py

  # Use 4 workers instead
  python scripts/batch_load_fundamentals_all.py --workers 4

  # Refill specific tickers
  python scripts/batch_load_fundamentals_all.py --refill AAPL MSFT GOOGL

  # Force re-download all
  python scripts/batch_load_fundamentals_all.py --force

  # Start from batch 50
  python scripts/batch_load_fundamentals_all.py --start-batch 50
        """
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=8,
        help='Number of parallel workers (default: 8)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Number of tickers per batch (default: 50)'
    )

    parser.add_argument(
        '--start-batch',
        type=int,
        default=1,
        help='Start from this batch number (default: 1)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download all tickers (ignore progress)'
    )

    parser.add_argument(
        '--refill',
        nargs='+',
        metavar='TICKER',
        help='Refill specific tickers (e.g., --refill AAPL MSFT GOOGL)'
    )

    parser.add_argument(
        '--download-metadata',
        action='store_true',
        help='Download ticker metadata after fundamentals (Phase 4)'
    )

    parser.add_argument(
        '--metadata-bulk-concurrency',
        type=int,
        default=100,
        help='Concurrent requests for bulk ticker metadata download (default: 100)'
    )

    parser.add_argument(
        '--metadata-detail-concurrency',
        type=int,
        default=200,
        help='Concurrent requests for detailed ticker enrichment (default: 200)'
    )

    args = parser.parse_args()

    # Validate workers
    max_workers = mp.cpu_count() * 2
    if args.workers > max_workers:
        logger.warning(f"Workers ({args.workers}) exceeds recommended max ({max_workers}), reducing to {max_workers}")
        args.workers = max_workers

    try:
        exit_code = asyncio.run(main_async(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user. Progress has been saved.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
