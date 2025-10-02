"""
Ingestion Orchestrator - Coordinate S3 downloads and Parquet ingestion

This module orchestrates the complete ingestion pipeline from S3 downloads
to Parquet storage with metadata tracking and error handling.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from ..download.s3_catalog import S3Catalog
from ..download.async_downloader import AsyncS3Downloader
from ..ingest.streaming_ingestor import StreamingIngestor
from ..ingest.polars_ingestor import PolarsIngestor
from ..storage.parquet_manager import ParquetManager
from ..storage.metadata_manager import MetadataManager
from ..core.config_loader import ConfigLoader
from ..core.system_profiler import SystemProfiler
from ..core.exceptions import PipelineException
from ..utils.market_calendar import get_default_calendar

logger = logging.getLogger(__name__)


class IngestionOrchestratorError(PipelineException):
    """Raised when orchestration fails"""
    pass


class IngestionOrchestrator:
    """
    Orchestrate S3 download → Parquet ingestion pipeline

    Features:
    - Coordinate downloads and ingestion
    - Incremental processing with watermarks
    - Parallel downloads with sequential ingestion
    - Metadata tracking
    - Error handling and retry
    - Statistics reporting
    """

    def __init__(
        self,
        config: Optional[ConfigLoader] = None,
        credentials: Optional[Dict[str, str]] = None,
        parquet_root: Optional[Path] = None,
        metadata_root: Optional[Path] = None
    ):
        """
        Initialize ingestion orchestrator

        Args:
            config: Optional config loader (auto-created if None)
            credentials: Optional S3 credentials
            parquet_root: Optional Parquet root directory
            metadata_root: Optional metadata root directory
        """
        # Load configuration
        self.config = config or ConfigLoader()

        # Get credentials
        if credentials is None:
            polygon_creds = self.config.get_credentials('polygon')
            if not polygon_creds or 's3' not in polygon_creds:
                raise IngestionOrchestratorError(
                    "S3 credentials not found. Please configure config/credentials.yaml"
                )
            credentials = {
                'access_key_id': polygon_creds['s3']['access_key_id'],
                'secret_access_key': polygon_creds['s3']['secret_access_key'],
            }

        # Initialize components
        self.catalog = S3Catalog()

        self.downloader = AsyncS3Downloader(
            credentials=credentials,
            endpoint_url=self.config.get('storage.s3_endpoint', 'https://files.polygon.io'),
            max_concurrent=self.config.get('ingestion.max_concurrent_downloads', 4)
        )

        # Parquet and metadata managers
        self.parquet_root = parquet_root or Path(
            self.config.get('storage.parquet_root', 'data/parquet')
        )
        self.metadata_root = metadata_root or Path(
            self.config.get('storage.metadata_root', 'data/metadata')
        )

        self.metadata_manager = MetadataManager(self.metadata_root)

        # System profiler for mode selection
        self.profiler = SystemProfiler()
        self.processing_mode = self.profiler.profile['recommended_mode']

        # Market calendar for trading day validation
        self.market_calendar = get_default_calendar()

        # Statistics
        self.statistics = {
            'downloads': 0,
            'ingestions': 0,
            'errors': 0,
            'bytes_downloaded': 0,
            'records_processed': 0,
            'skipped_non_trading_days': 0,
        }

        logger.info(
            f"IngestionOrchestrator initialized "
            f"(mode: {self.processing_mode}, "
            f"parquet: {self.parquet_root})"
        )

    async def ingest_date_range(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
        incremental: bool = True,
        use_polars: bool = False
    ) -> Dict[str, Any]:
        """
        Ingest data for a date range

        Args:
            data_type: Data type ('stocks_daily', etc.)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            symbols: Optional symbol filter (for minute data)
            incremental: Skip already ingested dates
            use_polars: Use PolarsIngestor instead of StreamingIngestor

        Returns:
            Dictionary with ingestion summary

        Example:
            >>> orchestrator = IngestionOrchestrator()
            >>> result = await orchestrator.ingest_date_range(
            ...     'stocks_daily',
            ...     '2025-09-01',
            ...     '2025-09-30'
            ... )
        """
        logger.info(
            f"Starting ingestion: {data_type} from {start_date} to {end_date}"
        )

        try:
            # Filter to trading days only for daily data
            from datetime import datetime as dt, timedelta

            if 'daily' in data_type:
                start_dt = dt.strptime(start_date, '%Y-%m-%d').date()
                end_dt = dt.strptime(end_date, '%Y-%m-%d').date()

                # Get trading days in range
                trading_days = self.market_calendar.get_trading_days(start_dt, end_dt)

                # Count skipped days
                total_days = (end_dt - start_dt).days + 1
                skipped = total_days - len(trading_days)

                if skipped > 0:
                    logger.info(
                        f"Filtered to {len(trading_days)} trading days "
                        f"(skipped {skipped} weekends/holidays)"
                    )
                    self.statistics['skipped_non_trading_days'] += skipped

                # Reconstruct date range from trading days only
                if not trading_days:
                    logger.warning("No trading days in date range")
                    return {'status': 'no_trading_days', 'ingested': 0}

                start_date = trading_days[0].isoformat()
                end_date = trading_days[-1].isoformat()

            # Get S3 keys
            keys = self.catalog.get_date_range_keys(
                data_type,
                start_date,
                end_date,
                symbols=symbols
            )

            if not keys:
                logger.warning("No keys found for date range")
                return {'status': 'no_data', 'ingested': 0}

            # Filter for incremental processing
            if incremental:
                keys = await self._filter_incremental(data_type, keys)

            if not keys:
                logger.info("All dates already ingested (incremental mode)")
                return {'status': 'up_to_date', 'ingested': 0}

            logger.info(f"Downloading {len(keys)} files...")

            # Download files
            bucket = self.config.get('storage.s3_bucket', 'flatfiles')
            files = await self.downloader.download_batch(bucket, keys)

            # Track download statistics
            self.statistics['downloads'] += len(files)
            self.statistics['bytes_downloaded'] += sum(
                len(f.getvalue()) if f else 0 for f in files
            )

            # Create ingestor
            ingestor = self._create_ingestor(data_type, use_polars)

            # Ingest files
            logger.info(f"Ingesting {len(files)} files...")
            results = []

            for i, (key, file_data) in enumerate(zip(keys, files)):
                if file_data is None:
                    logger.error(f"Download failed: {key}")
                    self.statistics['errors'] += 1
                    continue

                try:
                    # Extract date from key
                    metadata = self.catalog.parse_key_metadata(key)
                    date = metadata.get('date')
                    symbol = metadata.get('symbol')

                    if not date:
                        logger.error(f"Could not parse date from key: {key}")
                        continue

                    # Ingest
                    result = ingestor.ingest_date(date, file_data, symbols=symbols)

                    # Record metadata
                    self.metadata_manager.record_ingestion(
                        data_type=data_type,
                        date=date,
                        status=result['status'],
                        statistics=result,
                        symbol=symbol
                    )

                    results.append(result)
                    self.statistics['ingestions'] += 1

                    if result['status'] == 'success':
                        self.statistics['records_processed'] += result.get('records', 0)

                    logger.info(
                        f"Ingested {i+1}/{len(files)}: {date} "
                        f"({result.get('records', 0):,} records)"
                    )

                except Exception as e:
                    logger.error(f"Ingestion failed for {key}: {e}")
                    self.statistics['errors'] += 1

                    # Record error in metadata
                    self.metadata_manager.record_ingestion(
                        data_type=data_type,
                        date=date,
                        status='failed',
                        statistics={},
                        symbol=symbol,
                        error=str(e)
                    )

            # Summary
            success = sum(1 for r in results if r['status'] == 'success')

            summary = {
                'status': 'completed',
                'data_type': data_type,
                'date_range': {'start': start_date, 'end': end_date},
                'total_files': len(keys),
                'ingested': success,
                'failed': len(results) - success,
                'records_processed': sum(r.get('records', 0) for r in results),
                'statistics': self.statistics.copy(),
            }

            logger.info(
                f"Ingestion complete: {success}/{len(keys)} files "
                f"({summary['records_processed']:,} records)"
            )

            return summary

        except Exception as e:
            logger.error(f"Orchestration failed: {e}")
            raise IngestionOrchestratorError(f"Ingestion failed: {e}")

    async def ingest_date(
        self,
        data_type: str,
        date: str,
        symbols: Optional[List[str]] = None,
        use_polars: bool = False
    ) -> Dict[str, Any]:
        """
        Ingest data for a single date

        Args:
            data_type: Data type
            date: Date string (YYYY-MM-DD)
            symbols: Optional symbol filter
            use_polars: Use PolarsIngestor

        Returns:
            Ingestion result
        """
        return await self.ingest_date_range(
            data_type,
            date,
            date,
            symbols=symbols,
            incremental=False,
            use_polars=use_polars
        )

    async def backfill(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Backfill missing dates in a range

        Args:
            data_type: Data type
            start_date: Start date
            end_date: End date
            symbols: Optional symbol filter

        Returns:
            Backfill summary
        """
        logger.info(f"Backfilling {data_type} from {start_date} to {end_date}")

        # Get expected dates
        expected_dates = self.catalog.get_business_days(start_date, end_date)

        # Get missing dates
        missing_dates = self.metadata_manager.get_missing_dates(
            data_type,
            start_date,
            end_date,
            expected_dates
        )

        if not missing_dates:
            logger.info("No missing dates found")
            return {'status': 'up_to_date', 'missing': 0}

        logger.info(f"Found {len(missing_dates)} missing dates")

        # Ingest missing dates
        results = []
        for date in missing_dates:
            result = await self.ingest_date(data_type, date, symbols=symbols)
            results.append(result)

        success = sum(1 for r in results if r.get('status') == 'completed')

        return {
            'status': 'completed',
            'data_type': data_type,
            'missing_dates': len(missing_dates),
            'ingested': success,
            'failed': len(results) - success,
        }

    def _create_ingestor(self, data_type: str, use_polars: bool = False):
        """
        Create appropriate ingestor based on mode and preferences

        Args:
            data_type: Data type
            use_polars: Force Polars ingestor

        Returns:
            Ingestor instance
        """
        if use_polars:
            logger.info("Using PolarsIngestor (high-performance mode)")
            return PolarsIngestor(
                data_type=data_type,
                output_root=self.parquet_root,
                config=self.config.config,
                streaming=True
            )
        else:
            logger.info("Using StreamingIngestor (memory-safe mode)")
            chunk_size = self.config.get('ingestion.chunk_size', 100000)
            return StreamingIngestor(
                data_type=data_type,
                output_root=self.parquet_root,
                config=self.config.config,
                chunk_size=chunk_size
            )

    async def _filter_incremental(
        self,
        data_type: str,
        keys: List[str]
    ) -> List[str]:
        """
        Filter keys for incremental processing

        Args:
            data_type: Data type
            keys: List of S3 keys

        Returns:
            Filtered list of keys
        """
        filtered = []

        for key in keys:
            metadata = self.catalog.parse_key_metadata(key)
            date = metadata.get('date')
            symbol = metadata.get('symbol')

            if not date:
                filtered.append(key)
                continue

            # Check if already ingested
            status = self.metadata_manager.get_ingestion_status(
                data_type,
                date,
                symbol
            )

            if status is None or status['status'] != 'success':
                filtered.append(key)

        logger.debug(f"Incremental filter: {len(filtered)}/{len(keys)} keys remaining")

        return filtered

    def get_statistics(self) -> Dict[str, Any]:
        """Get orchestration statistics"""
        return self.statistics.copy()

    def reset_statistics(self):
        """Reset statistics counters"""
        self.statistics = {
            'downloads': 0,
            'ingestions': 0,
            'errors': 0,
            'bytes_downloaded': 0,
            'records_processed': 0,
        }


async def main():
    """Command-line interface for ingestion orchestrator"""
    import sys

    try:
        orchestrator = IngestionOrchestrator()

        print("✅ IngestionOrchestrator initialized")
        print(f"   Processing mode: {orchestrator.processing_mode}")
        print(f"   Parquet root: {orchestrator.parquet_root}")
        print(f"   Metadata root: {orchestrator.metadata_root}")

        # Test ingestion for most recent trading day (yesterday or Friday if weekend)
        from datetime import datetime, timedelta
        today = datetime.now()
        # If weekend, use Friday; otherwise use yesterday
        if today.weekday() == 6:  # Sunday
            test_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')
        elif today.weekday() == 0:  # Monday
            test_date = (today - timedelta(days=3)).strftime('%Y-%m-%d')
        else:
            test_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"\n⬇️  Testing ingestion for {test_date}...")

        result = await orchestrator.ingest_date(
            data_type='stocks_daily',
            date=test_date,
        )

        print(f"\n📊 Result:")
        print(f"   Status: {result.get('status')}")
        print(f"   Ingested: {result.get('ingested', 0)} files")
        print(f"   Records: {result.get('records_processed', 0):,}")

        # Statistics
        stats = orchestrator.get_statistics()
        print(f"\n📈 Statistics:")
        print(f"   Downloads: {stats['downloads']}")
        print(f"   Ingestions: {stats['ingestions']}")
        print(f"   Errors: {stats['errors']}")
        print(f"   Records: {stats['records_processed']:,}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
