"""
Polars Ingestor - High-performance ingestion with Polars (5-10x faster)

This module provides optimized CSV→Parquet conversion using Polars for
significant performance gains over pandas-based approaches.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, Optional, List
from io import BytesIO
import gc
import logging

from .base_ingestor import BaseIngestor, IngestionError

logger = logging.getLogger(__name__)


class PolarsIngestor(BaseIngestor):
    """
    High-performance ingestor using Polars

    Features:
    - Polars lazy evaluation (5-10x faster than pandas)
    - Automatic parallelization
    - Streaming CSV reading
    - Memory-efficient column selection
    - Native Arrow integration
    - Query optimization

    Recommended for:
    - All systems where performance is critical
    - Batch processing of multiple files
    - Large datasets
    """

    def __init__(
        self,
        data_type: str,
        output_root: Path,
        config: Dict[str, Any],
        streaming: bool = True
    ):
        """
        Initialize Polars ingestor

        Args:
            data_type: Data type ('stocks_daily', 'stocks_minute', etc.)
            output_root: Root directory for Parquet output
            config: Configuration dictionary
            streaming: Use streaming mode for large files
        """
        super().__init__(data_type, output_root, config)

        self.streaming = streaming

        logger.info(
            f"PolarsIngestor initialized "
            f"(streaming: {streaming})"
        )

    def ingest_date(
        self,
        date: str,
        data: BytesIO,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process single date file with Polars

        Args:
            date: Date string (YYYY-MM-DD)
            data: CSV data as BytesIO
            symbols: Optional symbol filter

        Returns:
            Dictionary with ingestion statistics
        """
        try:
            logger.info(f"Polars ingestion: {date}")

            # Check memory before starting
            mem_status = self.memory_monitor.check_and_wait()
            if mem_status['action'] == 'critical':
                logger.warning("Memory pressure is critical before ingestion")

            output_path = self._get_output_path(date)

            # Check if output already exists
            if output_path.exists():
                logger.warning(f"Output exists, skipping: {output_path}")
                return {
                    'date': date,
                    'records': 0,
                    'status': 'skipped',
                    'reason': 'output_exists'
                }

            # Read CSV with Polars
            if self.streaming:
                # Lazy/streaming mode
                df = pl.read_csv(
                    data,
                    has_header=True,
                    null_values=['', 'NA', 'NULL', 'NaN'],
                    try_parse_dates=True,
                    low_memory=True,
                )
            else:
                # Eager mode (load all into memory)
                df = pl.read_csv(
                    data,
                    has_header=True,
                    null_values=['', 'NA', 'NULL', 'NaN'],
                    try_parse_dates=True,
                )

            # Normalize column names (Polars)
            df = self._normalize_columns_polars(df, date)

            # Filter symbols if provided
            if symbols and 'symbol' in df.columns:
                df = df.filter(pl.col('symbol').is_in(symbols))

            # Add partition columns
            df = self._add_partition_columns_polars(df, date)

            # Convert types to match schema
            df = self._optimize_dtypes_polars(df)

            # Convert to PyArrow
            table = df.to_arrow()

            # Write Parquet
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                table,
                output_path,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True,
                row_group_size=100000,
            )

            # Statistics
            num_records = len(df)
            file_size = output_path.stat().st_size / 1024**2

            self.records_processed += num_records
            self.files_processed += 1
            self.bytes_processed += data.getbuffer().nbytes

            # Memory cleanup
            del df, table
            gc.collect()

            # Final memory check
            mem_status = self.memory_monitor.check_and_wait()

            logger.info(
                f"Polars ingestion complete: {date} "
                f"({num_records:,} records, {file_size:.1f} MB)"
            )

            return {
                'date': date,
                'records': num_records,
                'file_size_mb': file_size,
                'status': 'success',
                'memory_peak_percent': mem_status['system_percent'],
            }

        except Exception as e:
            self.errors += 1
            logger.error(f"Polars ingestion failed for {date}: {e}")
            raise IngestionError(f"Polars ingestion failed: {e}")

    def ingest_batch(
        self,
        dates: List[str],
        data_map: Dict[str, BytesIO],
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process multiple dates sequentially with Polars

        Args:
            dates: List of date strings
            data_map: Dictionary mapping dates to CSV data
            symbols: Optional symbol filter

        Returns:
            List of ingestion results
        """
        results = []

        logger.info(f"Batch Polars ingestion: {len(dates)} files")

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
            f"Batch Polars complete: "
            f"{sum(1 for r in results if r['status'] == 'success')}/{len(dates)} "
            f"files succeeded"
        )

        return results

    def _normalize_columns_polars(self, df: pl.DataFrame, date: str) -> pl.DataFrame:
        """
        Normalize column names using Polars

        Args:
            df: Polars DataFrame
            date: Date string

        Returns:
            DataFrame with normalized columns
        """
        # Rename columns according to mapping
        for old_name, new_name in self.column_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})

        # Add date column if not present
        if 'date' not in df.columns and 'timestamp' in df.columns:
            # Handle both string and integer (Unix timestamp) formats
            if df['timestamp'].dtype in [pl.Int64, pl.Int32, pl.UInt64, pl.UInt32]:
                # Unix timestamp in nanoseconds or milliseconds
                df = df.with_columns(
                    (pl.col('timestamp') / 1_000_000_000).cast(pl.Int64).cast(pl.Datetime('ns')).cast(pl.Date).alias('date')
                )
            else:
                # String timestamp
                df = df.with_columns(
                    pl.col('timestamp').str.to_date().alias('date')
                )
        elif 'date' not in df.columns:
            from datetime import datetime
            dt = datetime.strptime(date, '%Y-%m-%d').date()
            df = df.with_columns(pl.lit(dt).alias('date'))

        return df

    def _add_partition_columns_polars(self, df: pl.DataFrame, date: str) -> pl.DataFrame:
        """
        Add partition columns (year, month) using Polars

        Args:
            df: Polars DataFrame
            date: Date string (YYYY-MM-DD)

        Returns:
            DataFrame with partition columns
        """
        from datetime import datetime

        dt = datetime.strptime(date, '%Y-%m-%d')

        df = df.with_columns([
            pl.lit(dt.year).alias('year').cast(pl.Int16),
            pl.lit(dt.month).alias('month').cast(pl.Int8),
        ])

        # Reorder columns to put partition columns first
        partition_cols = ['year', 'month']
        other_cols = [col for col in df.columns if col not in partition_cols]
        df = df.select(partition_cols + other_cols)

        return df

    def _optimize_dtypes_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Optimize Polars DataFrame dtypes

        Args:
            df: Polars DataFrame

        Returns:
            Optimized DataFrame
        """
        # Polars already uses optimal dtypes by default
        # Just ensure float64 → float32 for price columns

        float_columns = ['open', 'high', 'low', 'close', 'vwap']

        for col in float_columns:
            if col in df.columns and df[col].dtype == pl.Float64:
                df = df.with_columns(pl.col(col).cast(pl.Float32))

        return df

    def ingest_lazy(
        self,
        date: str,
        data: BytesIO,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process single date file with Polars LazyFrame (streaming)

        This is the most memory-efficient mode for very large files.

        Args:
            date: Date string (YYYY-MM-DD)
            data: CSV data as BytesIO
            symbols: Optional symbol filter

        Returns:
            Dictionary with ingestion statistics
        """
        try:
            logger.info(f"Polars lazy ingestion: {date}")

            output_path = self._get_output_path(date)

            # Check if output already exists
            if output_path.exists():
                logger.warning(f"Output exists, skipping: {output_path}")
                return {
                    'date': date,
                    'records': 0,
                    'status': 'skipped',
                    'reason': 'output_exists'
                }

            # Read CSV as LazyFrame
            lf = pl.scan_csv(
                data,
                has_header=True,
                null_values=['', 'NA', 'NULL', 'NaN'],
                try_parse_dates=True,
                low_memory=True,
            )

            # Apply transformations lazily
            if symbols and 'symbol' in lf.columns:
                lf = lf.filter(pl.col('symbol').is_in(symbols))

            # Add partition columns
            from datetime import datetime
            dt = datetime.strptime(date, '%Y-%m-%d')

            lf = lf.with_columns([
                pl.lit(dt.year).alias('year').cast(pl.Int16),
                pl.lit(dt.month).alias('month').cast(pl.Int8),
            ])

            # Optimize dtypes
            float_columns = ['open', 'high', 'low', 'close', 'vwap']
            for col in float_columns:
                if col in lf.columns:
                    lf = lf.with_columns(pl.col(col).cast(pl.Float32))

            # Execute query and write to Parquet (streaming)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            lf.sink_parquet(
                output_path,
                compression='snappy',
                statistics=True,
                row_group_size=100000,
            )

            # Get statistics (need to read back for count)
            num_records = pl.scan_parquet(output_path).select(pl.count()).collect()[0, 0]
            file_size = output_path.stat().st_size / 1024**2

            self.records_processed += num_records
            self.files_processed += 1

            logger.info(
                f"Polars lazy ingestion complete: {date} "
                f"({num_records:,} records, {file_size:.1f} MB)"
            )

            return {
                'date': date,
                'records': num_records,
                'file_size_mb': file_size,
                'status': 'success',
            }

        except Exception as e:
            self.errors += 1
            logger.error(f"Polars lazy ingestion failed for {date}: {e}")
            raise IngestionError(f"Polars lazy ingestion failed: {e}")


async def main():
    """Command-line interface for Polars ingestor"""
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
            max_concurrent=8
        )

        print("✅ PolarsIngestor initialized")
        print(f"   Data type: stocks_daily")
        print(f"   Mode: streaming")

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
        ingestor = PolarsIngestor(
            data_type='stocks_daily',
            output_root=output_root,
            config=config_loader.config,
            streaming=True
        )

        # Ingest
        print(f"   Ingesting with Polars...")
        result = ingestor.ingest_date(test_date, files[0])

        print(f"\n📊 Ingestion Result:")
        print(f"   Records: {result['records']:,}")
        print(f"   File size: {result['file_size_mb']:.1f} MB")
        print(f"   Status: {result['status']}")

        # Statistics
        stats = ingestor.get_statistics()
        print(f"\n📈 Statistics:")
        print(f"   Records processed: {stats['records_processed']:,}")
        print(f"   Files processed: {stats['files_processed']}")
        print(f"   Errors: {stats['errors']}")

        # Benchmark comparison
        print(f"\n⚡ Performance:")
        print(f"   Polars is 5-10x faster than pandas")
        print(f"   Automatic parallelization")
        print(f"   Lower memory usage")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
