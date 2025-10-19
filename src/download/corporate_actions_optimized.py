"""
Optimized Corporate Actions Downloader - Ticker Events

Improvements over standard implementation:
1. Uses batch_request() for parallel API calls (no individual file I/O)
2. Processes in chunks to avoid memory issues
3. Saves incrementally to avoid data loss
4. Better error handling and progress tracking
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient
from .corporate_actions import CorporateActionsDownloader

logger = logging.getLogger(__name__)


class OptimizedTickerEventsDownloader(CorporateActionsDownloader):
    """
    Optimized ticker events downloader using batch_request pattern
    """

    async def download_ticker_events_optimized(
        self,
        tickers: List[str],
        types: Optional[str] = None,
        chunk_size: int = 1000,
        save_interval: int = 500
    ) -> pl.DataFrame:
        """
        Download ticker events for multiple tickers with optimizations

        Improvements:
        - Uses batch_request() instead of individual downloads
        - Processes in chunks to manage memory
        - Saves incrementally every N tickers
        - Better progress tracking

        Args:
            tickers: List of ticker symbols
            types: Event types filter (currently only 'ticker_change' supported)
            chunk_size: Number of tickers to process per chunk
            save_interval: Save to disk every N tickers

        Returns:
            Combined DataFrame with all ticker events
        """
        logger.info(f"Optimized download: {len(tickers):,} tickers (chunks={chunk_size}, save_interval={save_interval})")

        all_records = []
        total_events = 0
        tickers_processed = 0
        tickers_with_events = 0

        # Process in chunks
        for chunk_idx in range(0, len(tickers), chunk_size):
            chunk = tickers[chunk_idx:chunk_idx + chunk_size]
            chunk_num = (chunk_idx // chunk_size) + 1
            total_chunks = (len(tickers) + chunk_size - 1) // chunk_size

            logger.info(f"Processing chunk {chunk_num}/{total_chunks}: {len(chunk)} tickers")

            # Build batch requests for this chunk
            requests = []
            for ticker in chunk:
                endpoint = f'/vX/reference/tickers/{ticker.upper()}/events'
                params = {'types': types} if types else {}
                requests.append({'endpoint': endpoint, 'params': params})

            # Execute all requests in parallel using batch_request
            responses = await self.client.batch_request(requests)

            # Process responses
            chunk_records = []
            for ticker, response in zip(chunk, responses):
                tickers_processed += 1

                # Handle errors
                if 'error' in response:
                    logger.debug(f"No events for {ticker}: {response.get('error', 'unknown')}")
                    continue

                # Extract events
                result = response.get('results', {})
                events = result.get('events', [])

                if not events:
                    continue

                tickers_with_events += 1

                # Flatten events
                for event in events:
                    record = {
                        'ticker': ticker.upper(),
                        'name': result.get('name'),
                        'composite_figi': result.get('composite_figi'),
                        'cik': result.get('cik'),
                        'event_type': event.get('type'),
                        'date': event.get('date'),
                        'downloaded_at': datetime.now().isoformat()
                    }

                    # Add event-specific fields
                    if 'ticker_change' in event:
                        record['new_ticker'] = event['ticker_change'].get('ticker')

                    chunk_records.append(record)
                    total_events += 1

            all_records.extend(chunk_records)

            # Save incrementally
            if len(all_records) >= save_interval and len(all_records) > 0:
                self._save_incremental(all_records)
                all_records = []  # Clear memory

            # Progress logging
            logger.info(
                f"  Chunk {chunk_num} complete: "
                f"{tickers_with_events}/{tickers_processed} tickers with events, "
                f"{total_events:,} total events"
            )

        # Save any remaining records
        if len(all_records) > 0:
            self._save_incremental(all_records)

        logger.info(
            f"\n✅ Optimized download complete:\n"
            f"   Tickers processed: {tickers_processed:,}\n"
            f"   Tickers with events: {tickers_with_events:,}\n"
            f"   Total events: {total_events:,}"
        )

        # Return summary DataFrame (optional - data already saved)
        return pl.DataFrame({
            'tickers_processed': [tickers_processed],
            'tickers_with_events': [tickers_with_events],
            'total_events': [total_events]
        })

    def _save_incremental(self, records: List[dict]) -> None:
        """
        Save records incrementally to avoid data loss

        Args:
            records: List of event records to save
        """
        if not records:
            return

        df = pl.DataFrame(records)

        if self.use_partitioned_structure:
            self._save_partitioned(df, 'ticker_events', 'date')
        else:
            # Append to single file
            output_file = self.output_dir / 'ticker_events.parquet'
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                df = pl.concat([existing_df, df], how="diagonal")

            df.write_parquet(str(output_file), compression='zstd')

        logger.info(f"  💾 Saved {len(records)} events incrementally")


async def download_ticker_events_for_all_cs_tickers(
    api_key: str,
    tickers_file: Path,
    output_dir: Path,
    chunk_size: int = 1000,
    save_interval: int = 500
) -> None:
    """
    Convenience function to download ticker events for all CS tickers

    Args:
        api_key: Polygon API key
        tickers_file: Path to file with ticker list (one per line)
        output_dir: Output directory for parquet files
        chunk_size: Tickers per chunk (default: 1000)
        save_interval: Save every N records (default: 500)
    """
    # Read tickers
    with open(tickers_file) as f:
        tickers = [line.strip() for line in f if line.strip()]

    logger.info(f"Loaded {len(tickers):,} tickers from {tickers_file}")

    # Initialize client and downloader
    async with PolygonRESTClient(
        api_key=api_key,
        max_concurrent=100,
        max_connections=200
    ) as client:

        downloader = OptimizedTickerEventsDownloader(
            client=client,
            output_dir=output_dir,
            use_partitioned_structure=True
        )

        # Download
        await downloader.download_ticker_events_optimized(
            tickers=tickers,
            chunk_size=chunk_size,
            save_interval=save_interval
        )

        # Print stats
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Total requests: {stats['total_requests']:,}")
        logger.info(f"   Total retries: {stats['total_retries']:,}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")
