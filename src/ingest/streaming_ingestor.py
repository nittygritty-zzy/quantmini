"""
Streaming Ingestor - Memory-efficient chunked processing for <32GB systems

This module provides streaming CSV→Parquet conversion using pandas chunking
for memory-constrained systems. Ideal for 24GB Macs.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, Optional, List
from io import BytesIO
import gc
import logging

from .base_ingestor import BaseIngestor, IngestionError

logger = logging.getLogger(__name__)


class StreamingIngestor(BaseIngestor):
    """
    Streaming ingestor for memory-constrained systems (<32GB RAM)

    Features:
    - Chunked CSV reading (process 100K rows at a time)
    - Incremental Parquet writing
    - Garbage collection after each chunk
    - Memory pressure monitoring
    - 50-70% memory reduction via dtype optimization

    Recommended for:
    - 24GB Macs (your system)
    - Systems with <32GB RAM
    - Large files that don't fit in memory
    """

    def __init__(
        self,
        data_type: str,
        output_root: Path,
        config: Dict[str, Any],
        chunk_size: int = 100000
    ):
        """
        Initialize streaming ingestor

        Args:
            data_type: Data type ('stocks_daily', 'stocks_minute', etc.)
            output_root: Root directory for Parquet output
            config: Configuration dictionary
            chunk_size: Rows per chunk (default: 100K)
        """
        super().__init__(data_type, output_root, config)

        self.chunk_size = chunk_size

        logger.info(
            f"StreamingIngestor initialized "
            f"(chunk_size: {chunk_size:,} rows)"
        )

    def ingest_date(
        self,
        date: str,
        data: BytesIO,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process single date file with streaming

        Args:
            date: Date string (YYYY-MM-DD)
            data: CSV data as BytesIO
            symbols: Optional symbol filter

        Returns:
            Dictionary with ingestion statistics
        """
        try:
            logger.info(f"Streaming ingestion: {date}")

            # Check memory before starting
            mem_status = self.memory_monitor.check_and_wait()
            if mem_status['action'] == 'critical':
                logger.warning("Memory pressure is critical before ingestion")

            # Read CSV in chunks
            chunks_processed = 0
            total_records = 0

            output_path = self._get_output_path(date)

            # Check if output already exists
            if output_path.exists():
                logger.warning(f"Output exists, skipping: {output_path}")
                return {
                    'date': date,
                    'records': 0,
                    'chunks': 0,
                    'status': 'skipped',
                    'reason': 'output_exists'
                }

            # Stream processing
            reader = pd.read_csv(
                data,
                compression=None,
                chunksize=self.chunk_size,
                low_memory=False,
                na_values=['', 'NA', 'NULL', 'NaN'],
            )

            # Process first chunk to initialize writer
            first_chunk = True
            writer = None

            for chunk in reader:
                chunks_processed += 1

                logger.debug(
                    f"Processing chunk {chunks_processed} "
                    f"({len(chunk):,} rows)"
                )

                # Normalize columns (rename, add date)
                chunk = self._normalize_columns(chunk, date)

                # Optimize dtypes
                chunk = self._optimize_dtypes(chunk)

                # Filter symbols if provided
                if symbols and 'symbol' in chunk.columns:
                    chunk = chunk[chunk['symbol'].isin(symbols)]
                    if len(chunk) == 0:
                        logger.debug("Chunk filtered to 0 rows, skipping")
                        continue

                # Add partition columns
                chunk = self._add_partition_columns(chunk, date)

                # Convert to Arrow
                table = self._convert_to_arrow(chunk)

                # Write Parquet incrementally
                if first_chunk:
                    # Create writer on first chunk
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    writer = pq.ParquetWriter(
                        output_path,
                        schema=table.schema,
                        compression='snappy',
                        use_dictionary=False,  # Disable for schema consistency
                        write_statistics=True,
                    )
                    first_chunk = False

                # Write chunk
                writer.write_table(table)

                total_records += len(chunk)

                # Memory cleanup
                del chunk, table
                gc.collect(generation=0)  # Quick GC

                # Check memory pressure
                mem_status = self.memory_monitor.check_and_wait()
                if mem_status['action'] == 'critical':
                    logger.warning(
                        f"Memory critical after chunk {chunks_processed}, "
                        f"waiting for GC..."
                    )
                    gc.collect()  # Full GC

            # Close writer
            if writer:
                writer.close()

            # Update statistics
            self.records_processed += total_records
            self.files_processed += 1
            self.bytes_processed += data.getbuffer().nbytes

            # Final memory check
            mem_status = self.memory_monitor.check_and_wait()

            file_size = output_path.stat().st_size / 1024**2 if output_path.exists() else 0

            logger.info(
                f"Streaming ingestion complete: {date} "
                f"({total_records:,} records, {chunks_processed} chunks, "
                f"{file_size:.1f} MB)"
            )

            return {
                'date': date,
                'records': total_records,
                'chunks': chunks_processed,
                'file_size_mb': file_size,
                'status': 'success',
                'memory_peak_percent': mem_status['system_percent'],
            }

        except Exception as e:
            self.errors += 1
            logger.error(f"Streaming ingestion failed for {date}: {e}")
            raise IngestionError(f"Streaming ingestion failed: {e}")

    def ingest_batch(
        self,
        dates: List[str],
        data_map: Dict[str, BytesIO],
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process multiple dates sequentially (not parallel)

        Args:
            dates: List of date strings
            data_map: Dictionary mapping dates to CSV data
            symbols: Optional symbol filter

        Returns:
            List of ingestion results
        """
        results = []

        logger.info(f"Batch streaming ingestion: {len(dates)} files")

        for date in dates:
            if date not in data_map:
                logger.warning(f"No data for {date}, skipping")
                continue

            try:
                result = self.ingest_date(date, data_map[date], symbols=symbols)
                results.append(result)

                # Garbage collection between files
                gc.collect()

            except Exception as e:
                logger.error(f"Failed to ingest {date}: {e}")
                results.append({
                    'date': date,
                    'status': 'error',
                    'error': str(e)
                })

        logger.info(
            f"Batch streaming complete: "
            f"{sum(1 for r in results if r['status'] == 'success')}/{len(dates)} "
            f"files succeeded"
        )

        return results


async def main():
    """Command-line interface for streaming ingestor"""
    import sys
    from ..core.config_loader import ConfigLoader
    from ..download.async_downloader import AsyncS3Downloader
    from ..download.s3_catalog import S3Catalog

    try:
        # Load configuration
        config_loader = ConfigLoader()
        credentials = config_loader.get_credentials('polygon')

        if not credentials or 's3' not in credentials:
            print("❌ Credentials not found. Please configure config/credentials.yaml")
            sys.exit(1)

        s3_creds = credentials['s3']

        # Create components
        catalog = S3Catalog()
        downloader = AsyncS3Downloader(
            credentials={
                'access_key_id': s3_creds['access_key_id'],
                'secret_access_key': s3_creds['secret_access_key'],
            },
            endpoint_url=s3_creds.get('endpoint_url', 'https://files.polygon.io'),
            max_concurrent=4  # Lower for streaming mode
        )

        print("✅ StreamingIngestor initialized")
        print(f"   Data type: stocks_daily")
        print(f"   Chunk size: 100,000 rows")

        # Test: Download and ingest one file
        print("\n📥 Testing download + ingestion...")
        bucket = s3_creds.get('bucket', 'flatfiles')

        # Get key for recent date
        test_date = '2025-09-29'
        key = catalog.get_stocks_daily_key(test_date)

        print(f"   Downloading: {key}")

        # Download file
        files = await downloader.download_batch(bucket, [key])

        if files[0] is None:
            print(f"❌ Download failed")
            sys.exit(1)

        print(f"   Downloaded: {len(files[0].getvalue()) / 1024**2:.1f} MB")

        # Create ingestor
        output_root = Path('data/parquet_test')
        ingestor = StreamingIngestor(
            data_type='stocks_daily',
            output_root=output_root,
            config=config_loader.config,
            chunk_size=100000
        )

        # Ingest
        print(f"   Ingesting...")
        result = ingestor.ingest_date(test_date, files[0])

        print(f"\n📊 Ingestion Result:")
        print(f"   Records: {result['records']:,}")
        print(f"   Chunks: {result['chunks']}")
        print(f"   File size: {result['file_size_mb']:.1f} MB")
        print(f"   Status: {result['status']}")

        # Statistics
        stats = ingestor.get_statistics()
        print(f"\n📈 Statistics:")
        print(f"   Records processed: {stats['records_processed']:,}")
        print(f"   Files processed: {stats['files_processed']}")
        print(f"   Errors: {stats['errors']}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
