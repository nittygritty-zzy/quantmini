"""
Parquet Manager - Manage Parquet datasets with partitioning

This module provides high-level management of Parquet datasets including
reading, writing, querying, and dataset maintenance.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging

from .schemas import get_schema
from ..core.exceptions import PipelineException

logger = logging.getLogger(__name__)


class ParquetManagerError(PipelineException):
    """Raised when Parquet operations fail"""
    pass


class ParquetManager:
    """
    Manage Parquet datasets with partitioning and querying

    Features:
    - Read/write partitioned Parquet datasets
    - Query with filters (date ranges, symbols)
    - Dataset statistics and metadata
    - Partition management
    - Incremental updates
    """

    def __init__(
        self,
        root_path: Path,
        data_type: str
    ):
        """
        Initialize Parquet manager

        Args:
            root_path: Root directory for Parquet data
            data_type: Data type ('stocks_daily', 'stocks_minute', etc.)
        """
        self.root_path = Path(root_path)
        self.data_type = data_type
        self.dataset_path = self.root_path / data_type

        # Get schema
        self.schema = get_schema(data_type)

        # Create directory
        self.dataset_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"ParquetManager initialized (path: {self.dataset_path})")

    def write_partition(
        self,
        table: pa.Table,
        partition_values: Dict[str, Any]
    ):
        """
        Write data to a specific partition

        Args:
            table: PyArrow table to write
            partition_values: Partition values (e.g., {'year': 2025, 'month': 9})

        Raises:
            ParquetManagerError: If write fails
        """
        try:
            # Build partition path
            partition_path = self._build_partition_path(partition_values)
            partition_path.parent.mkdir(parents=True, exist_ok=True)

            # Write Parquet file
            pq.write_table(
                table,
                partition_path,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True,
                row_group_size=100000,
            )

            logger.info(
                f"Wrote partition: {partition_path} "
                f"({len(table):,} rows, {partition_path.stat().st_size / 1024**2:.1f} MB)"
            )

        except Exception as e:
            raise ParquetManagerError(f"Failed to write partition: {e}")

    def read_partition(
        self,
        partition_values: Dict[str, Any],
        columns: Optional[List[str]] = None
    ) -> pa.Table:
        """
        Read data from a specific partition

        Args:
            partition_values: Partition values
            columns: Optional column subset to read

        Returns:
            PyArrow table

        Raises:
            ParquetManagerError: If read fails
        """
        try:
            partition_path = self._build_partition_path(partition_values)

            if not partition_path.exists():
                raise ParquetManagerError(f"Partition not found: {partition_path}")

            # Read the partition - use ParquetFile to avoid dataset discovery issues
            parquet_file = pq.ParquetFile(partition_path)
            table = parquet_file.read(columns=columns)

            logger.debug(f"Read partition: {partition_path} ({len(table):,} rows)")

            return table

        except Exception as e:
            raise ParquetManagerError(f"Failed to read partition: {e}")

    def read_date_range(
        self,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
        columns: Optional[List[str]] = None
    ) -> pa.Table:
        """
        Read data for a date range with optional symbol filtering

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            symbols: Optional symbol filter
            columns: Optional column subset

        Returns:
            PyArrow table with filtered data
        """
        try:
            # Build filters
            filters = [
                ('date', '>=', start_date),
                ('date', '<=', end_date),
            ]

            if symbols:
                filters.append(('symbol', 'in', symbols))

            # Read dataset with filters
            dataset = ds.dataset(
                self.dataset_path,
                format='parquet',
                partitioning='hive'
            )

            table = dataset.to_table(
                columns=columns,
                filter=(ds.field('date') >= start_date) & (ds.field('date') <= end_date)
            )

            # Apply symbol filter if needed (post-read filtering)
            if symbols and 'symbol' in table.column_names:
                mask = pa.compute.is_in(table['symbol'], value_set=pa.array(symbols))
                table = table.filter(mask)

            logger.info(
                f"Read date range: {start_date} to {end_date} "
                f"({len(table):,} rows)"
            )

            return table

        except Exception as e:
            raise ParquetManagerError(f"Failed to read date range: {e}")

    def query(
        self,
        filters: Optional[List[Tuple]] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> pa.Table:
        """
        Query dataset with flexible filters

        Args:
            filters: PyArrow filter expressions (e.g., [('symbol', '=', 'AAPL')])
            columns: Column subset to read
            limit: Maximum rows to return

        Returns:
            Filtered PyArrow table

        Example:
            >>> manager = ParquetManager(Path('data/parquet'), 'stocks_daily')
            >>> table = manager.query(
            ...     filters=[('symbol', '=', 'AAPL'), ('date', '>=', '2025-09-01')],
            ...     columns=['date', 'close', 'volume']
            ... )
        """
        try:
            dataset = ds.dataset(
                self.dataset_path,
                format='parquet',
                partitioning='hive'
            )

            # Convert filters to PyArrow expression
            filter_expr = None
            if filters:
                filter_expr = self._build_filter_expression(filters)

            table = dataset.to_table(
                columns=columns,
                filter=filter_expr
            )

            # Apply limit
            if limit and len(table) > limit:
                table = table.slice(0, limit)

            logger.info(f"Query returned {len(table):,} rows")

            return table

        except Exception as e:
            raise ParquetManagerError(f"Query failed: {e}")

    def list_partitions(self) -> List[Dict[str, Any]]:
        """
        List all partitions in the dataset

        Returns:
            List of partition metadata dictionaries
        """
        try:
            partitions = []

            # Walk directory tree to find partitions
            if not self.dataset_path.exists():
                return partitions

            for partition_dir in self.dataset_path.rglob('*.parquet'):
                # Parse partition values from path
                partition_values = self._parse_partition_path(partition_dir)

                # Get file stats
                stats = partition_dir.stat()

                partitions.append({
                    'path': partition_dir,
                    'partition': partition_values,
                    'size_mb': stats.st_size / 1024**2,
                    'modified': datetime.fromtimestamp(stats.st_mtime),
                })

            logger.debug(f"Found {len(partitions)} partitions")

            return partitions

        except Exception as e:
            raise ParquetManagerError(f"Failed to list partitions: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get dataset statistics

        Returns:
            Dictionary with dataset statistics
        """
        try:
            partitions = self.list_partitions()

            if not partitions:
                return {
                    'total_partitions': 0,
                    'total_size_mb': 0,
                    'data_type': self.data_type,
                }

            total_size = sum(p['size_mb'] for p in partitions)

            # Get date range if available
            date_min = None
            date_max = None
            if partitions:
                dates = [p['partition'].get('date') for p in partitions if 'date' in p['partition']]
                if dates:
                    date_min = min(dates)
                    date_max = max(dates)

            return {
                'data_type': self.data_type,
                'total_partitions': len(partitions),
                'total_size_mb': total_size,
                'total_size_gb': total_size / 1024,
                'date_range': {
                    'start': date_min,
                    'end': date_max,
                } if date_min else None,
            }

        except Exception as e:
            raise ParquetManagerError(f"Failed to get statistics: {e}")

    def delete_partition(self, partition_values: Dict[str, Any]):
        """
        Delete a specific partition

        Args:
            partition_values: Partition to delete
        """
        try:
            partition_path = self._build_partition_path(partition_values)

            if partition_path.exists():
                partition_path.unlink()
                logger.info(f"Deleted partition: {partition_path}")
            else:
                logger.warning(f"Partition not found: {partition_path}")

        except Exception as e:
            raise ParquetManagerError(f"Failed to delete partition: {e}")

    def optimize_dataset(self):
        """
        Optimize dataset by compacting small files

        This is a placeholder for future optimization logic
        """
        logger.info("Dataset optimization not yet implemented")
        pass

    def _build_partition_path(self, partition_values: Dict[str, Any]) -> Path:
        """
        Build partition path from values

        Args:
            partition_values: Dict with partition keys/values

        Returns:
            Path to partition file
        """
        path = self.dataset_path

        # Add partition directories
        if 'symbol' in partition_values:
            path = path / f"symbol={partition_values['symbol']}"
        if 'underlying' in partition_values:
            path = path / f"underlying={partition_values['underlying']}"
        if 'year' in partition_values:
            path = path / f"year={partition_values['year']}"
        if 'month' in partition_values:
            path = path / f"month={partition_values['month']:02d}"

        # Add filename
        if 'date' in partition_values:
            path = path / f"date={partition_values['date']}.parquet"
        else:
            path = path / "data.parquet"

        return path

    def _parse_partition_path(self, path: Path) -> Dict[str, Any]:
        """
        Parse partition values from path

        Args:
            path: Partition file path

        Returns:
            Dictionary with partition values
        """
        values = {}

        # Parse parent directories
        for part in path.parents:
            if '=' in part.name:
                key, value = part.name.split('=', 1)
                values[key] = value

        # Parse filename
        if path.stem.startswith('date='):
            date_str = path.stem.replace('date=', '')
            values['date'] = date_str

        return values

    def _build_filter_expression(self, filters: List[Tuple]):
        """
        Build PyArrow filter expression from list of tuples

        Args:
            filters: List of (column, operator, value) tuples

        Returns:
            PyArrow filter expression
        """
        if not filters:
            return None

        expressions = []
        for column, op, value in filters:
            field = ds.field(column)

            if op == '=':
                expr = field == value
            elif op == '!=':
                expr = field != value
            elif op == '>':
                expr = field > value
            elif op == '>=':
                expr = field >= value
            elif op == '<':
                expr = field < value
            elif op == '<=':
                expr = field <= value
            elif op == 'in':
                expr = field.isin(value)
            else:
                raise ValueError(f"Unknown operator: {op}")

            expressions.append(expr)

        # Combine with AND
        result = expressions[0]
        for expr in expressions[1:]:
            result = result & expr

        return result

    def __repr__(self) -> str:
        return f"ParquetManager(data_type={self.data_type}, path={self.dataset_path})"


def main():
    """Command-line interface for Parquet manager"""
    import sys
    from ..core.config_loader import ConfigLoader

    try:
        config = ConfigLoader()
        root_path = Path(config.get('storage.parquet_root', 'data/parquet'))

        print("✅ ParquetManager initialized")
        print(f"   Root: {root_path}")

        # List all data types
        for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            manager = ParquetManager(root_path, data_type)
            stats = manager.get_statistics()

            print(f"\n📊 {data_type}:")
            print(f"   Partitions: {stats['total_partitions']}")
            print(f"   Size: {stats['total_size_gb']:.2f} GB")
            if stats['date_range']:
                print(f"   Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
