"""
Base Ingestor - Abstract base class for data ingestion

This module provides the foundation for all ingestor implementations with
common functionality for CSV parsing, dtype optimization, and Parquet writing.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, Optional, List
from io import BytesIO
import logging

from ..core.memory_monitor import AdvancedMemoryMonitor
from ..storage.schemas import get_schema, get_raw_schema
from ..core.exceptions import PipelineException

logger = logging.getLogger(__name__)


class IngestionError(PipelineException):
    """Raised when ingestion fails"""
    pass


class BaseIngestor(ABC):
    """
    Abstract base class for data ingestion

    Features:
    - CSV decompression and parsing
    - Dtype optimization (50-70% memory reduction)
    - Schema validation
    - Parquet writing with partitioning
    - Memory monitoring
    - Statistics tracking

    Subclasses must implement:
    - ingest_date(): Process single date file
    """

    # Column name mappings from Polygon CSV to our schema
    COLUMN_MAPPINGS = {
        'stocks_daily': {
            'ticker': 'symbol',
            'window_start': 'timestamp',
        },
        'stocks_minute': {
            'ticker': 'symbol',
            'window_start': 'timestamp',
        },
        'options_daily': {
            'ticker': 'ticker',  # Keep as is
            'window_start': 'timestamp',
        },
        'options_minute': {
            'ticker': 'ticker',  # Keep as is
            'window_start': 'timestamp',
        },
    }

    def __init__(
        self,
        data_type: str,
        output_root: Path,
        config: Dict[str, Any],
        memory_monitor: Optional[AdvancedMemoryMonitor] = None
    ):
        """
        Initialize base ingestor

        Args:
            data_type: Data type ('stocks_daily', 'stocks_minute', etc.')
            output_root: Root directory for Parquet output
            config: Configuration dictionary
            memory_monitor: Optional memory monitor instance
        """
        self.data_type = data_type
        self.output_root = Path(output_root)
        self.config = config

        # Get schema
        self.schema = get_schema(data_type)
        self.raw_schema = get_raw_schema(data_type)

        # Get column mapping
        self.column_mapping = self.COLUMN_MAPPINGS.get(data_type, {})

        # Memory monitoring
        if memory_monitor is None:
            limits = config.get('resource_limits', {})
            self.memory_monitor = AdvancedMemoryMonitor(limits)
        else:
            self.memory_monitor = memory_monitor

        # Statistics
        self.records_processed = 0
        self.files_processed = 0
        self.bytes_processed = 0
        self.errors = 0

        # Create output directory
        self.output_root.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"BaseIngestor initialized "
            f"(data_type: {data_type}, output: {output_root})"
        )

    @abstractmethod
    def ingest_date(
        self,
        date: str,
        data: BytesIO,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process single date file (must be implemented by subclasses)

        Args:
            date: Date string (YYYY-MM-DD)
            data: CSV data as BytesIO
            symbols: Optional symbol filter

        Returns:
            Dictionary with ingestion statistics

        Raises:
            IngestionError: If ingestion fails
        """
        pass

    def _read_csv(
        self,
        data: BytesIO,
        chunksize: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read CSV data with optimized dtypes

        Args:
            data: CSV data as BytesIO
            chunksize: Optional chunk size for streaming

        Returns:
            DataFrame (or TextFileReader if chunksize specified)
        """
        try:
            # Get column names from schema
            columns = [field.name for field in self.raw_schema]

            # Read CSV
            df = pd.read_csv(
                data,
                compression=None,  # Already decompressed
                chunksize=chunksize,
                low_memory=False,
                na_values=['', 'NA', 'NULL', 'NaN'],
            )

            return df

        except Exception as e:
            raise IngestionError(f"Failed to read CSV: {e}")

    def _normalize_columns(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        """
        Normalize column names and add derived columns

        Args:
            df: Input DataFrame with Polygon CSV columns
            date: Date string for deriving date column

        Returns:
            DataFrame with normalized columns
        """
        # Rename columns according to mapping
        df = df.rename(columns=self.column_mapping)

        # Parse timestamp if it exists
        if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        # Add date column if not present
        if 'date' not in df.columns and 'timestamp' in df.columns:
            # Extract date from timestamp
            df['date'] = pd.to_datetime(df['timestamp']).dt.date

        # If we only have date string, add it
        if 'date' not in df.columns:
            df['date'] = pd.to_datetime(date).date()

        return df

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize DataFrame dtypes to reduce memory by 50-70%

        Conversions:
        - float64 → float32 (50% savings)
        - int64 → smallest possible int type
        - object → category for low cardinality strings

        Args:
            df: Input DataFrame

        Returns:
            Optimized DataFrame
        """
        original_memory = df.memory_usage(deep=True).sum() / 1024**2

        for col in df.columns:
            col_type = df[col].dtype

            # Float optimization
            if col_type == 'float64':
                df[col] = df[col].astype('float32')

            # Integer optimization
            elif col_type == 'int64':
                col_min = df[col].min()
                col_max = df[col].max()

                if col_min >= 0:  # Unsigned
                    if col_max < 256:
                        df[col] = df[col].astype('uint8')
                    elif col_max < 65536:
                        df[col] = df[col].astype('uint16')
                    elif col_max < 4294967296:
                        df[col] = df[col].astype('uint32')
                    # else keep uint64
                else:  # Signed
                    if col_min > -128 and col_max < 127:
                        df[col] = df[col].astype('int8')
                    elif col_min > -32768 and col_max < 32767:
                        df[col] = df[col].astype('int16')
                    elif col_min > -2147483648 and col_max < 2147483647:
                        df[col] = df[col].astype('int32')
                    # else keep int64

            # String optimization (category for low cardinality)
            elif col_type == 'object':
                # Don't convert timestamp/date columns to category
                if col in ['timestamp', 'date', 'window_start', 'expiration_date']:
                    continue

                num_unique = df[col].nunique()
                num_total = len(df[col])

                # If less than 50% unique, convert to category
                if num_unique / num_total < 0.5:
                    df[col] = df[col].astype('category')

        optimized_memory = df.memory_usage(deep=True).sum() / 1024**2
        reduction = (1 - optimized_memory / original_memory) * 100

        logger.info(
            f"Memory optimized: {original_memory:.1f}MB → {optimized_memory:.1f}MB "
            f"({reduction:.1f}% reduction)"
        )

        return df

    def _add_partition_columns(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        """
        Add partition columns (year, month) for partitioned writes

        Args:
            df: Input DataFrame
            date: Date string (YYYY-MM-DD)

        Returns:
            DataFrame with partition columns
        """
        dt = pd.Timestamp(date)
        df['year'] = dt.year
        df['month'] = dt.month

        # Reorder columns to put partition columns first
        partition_cols = ['year', 'month']
        other_cols = [col for col in df.columns if col not in partition_cols]
        df = df[partition_cols + other_cols]

        return df

    def _convert_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """
        Convert pandas DataFrame to PyArrow Table with schema

        Args:
            df: Input DataFrame

        Returns:
            PyArrow Table
        """
        try:
            # Reorder columns to match schema and select only schema columns
            schema_columns = [field.name for field in self.raw_schema]
            missing_cols = [col for col in schema_columns if col not in df.columns]

            if missing_cols:
                logger.warning(f"Missing columns: {missing_cols}, will be filled with nulls")
                for col in missing_cols:
                    df[col] = None

            # Select and reorder columns to match schema
            df = df[schema_columns]

            # Convert to Arrow with schema validation
            table = pa.Table.from_pandas(df, schema=self.raw_schema, preserve_index=False)
            return table

        except Exception as e:
            logger.error(f"Failed to convert to Arrow: {e}")
            raise IngestionError(f"Failed to convert to Arrow: {e}")

    def _write_parquet(
        self,
        table: pa.Table,
        output_path: Path,
        partition_cols: Optional[List[str]] = None
    ):
        """
        Write PyArrow Table to Parquet with compression

        Args:
            table: PyArrow Table
            output_path: Output file path
            partition_cols: Optional partition columns
        """
        try:
            # Create parent directory
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write with compression
            pq.write_table(
                table,
                output_path,
                compression='snappy',  # Fast compression
                use_dictionary=True,   # Dictionary encoding
                write_statistics=True, # Column statistics
                row_group_size=100000, # 100K rows per group
            )

            file_size = output_path.stat().st_size / 1024**2
            logger.info(f"Wrote Parquet: {output_path} ({file_size:.1f} MB)")

        except Exception as e:
            raise IngestionError(f"Failed to write Parquet: {e}")

    def _get_output_path(self, date: str, symbol: Optional[str] = None) -> Path:
        """
        Get output path for date (and optional symbol)

        Args:
            date: Date string (YYYY-MM-DD)
            symbol: Optional symbol

        Returns:
            Output file path
        """
        dt = pd.Timestamp(date)
        year = dt.year
        month = f"{dt.month:02d}"

        if symbol:
            # Symbol-partitioned: data_type/symbol=ABC/year=2025/month=09/data.parquet
            path = self.output_root / self.data_type / f"symbol={symbol}" / f"year={year}" / f"month={month}" / "data.parquet"
        else:
            # Date-partitioned: data_type/year=2025/month=09/date=2025-09-29.parquet
            path = self.output_root / self.data_type / f"year={year}" / f"month={month}" / f"date={date}.parquet"

        return path

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get ingestion statistics

        Returns:
            Dictionary with statistics
        """
        return {
            'records_processed': self.records_processed,
            'files_processed': self.files_processed,
            'bytes_processed': self.bytes_processed,
            'errors': self.errors,
            'data_type': self.data_type,
        }

    def reset_statistics(self):
        """Reset statistics counters"""
        self.records_processed = 0
        self.files_processed = 0
        self.bytes_processed = 0
        self.errors = 0

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"data_type={self.data_type}, "
            f"output={self.output_root})"
        )


def main():
    """Command-line interface for testing base ingestor"""
    print("BaseIngestor - Abstract base class")
    print("=" * 70)
    print("\nThis is an abstract class and cannot be instantiated directly.")
    print("Use concrete implementations like StreamingIngestor or PolarsIngestor.")
    print("\nAvailable methods:")
    print("  - ingest_date(): Process single date file (abstract)")
    print("  - _read_csv(): Read CSV with optimized dtypes")
    print("  - _optimize_dtypes(): Reduce memory by 50-70%")
    print("  - _add_partition_columns(): Add year/month partitions")
    print("  - _convert_to_arrow(): Convert to PyArrow Table")
    print("  - _write_parquet(): Write Parquet with compression")
    print("  - get_statistics(): Get ingestion stats")


if __name__ == '__main__':
    main()
