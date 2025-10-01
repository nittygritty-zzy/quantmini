"""
Unit tests for ParquetManager

Run with: pytest tests/unit/test_parquet_manager.py
"""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

from src.storage.parquet_manager import ParquetManager, ParquetManagerError


@pytest.fixture
def test_parquet_root(tmp_path):
    """Create temporary Parquet root directory"""
    return tmp_path / 'parquet'


@pytest.fixture
def parquet_manager(test_parquet_root):
    """Create test Parquet manager"""
    return ParquetManager(test_parquet_root, 'stocks_daily')


@pytest.fixture
def sample_table():
    """Create sample PyArrow table matching stocks_daily schema"""
    from src.storage.schemas import get_raw_schema

    # Create data matching raw schema types
    import pandas as pd

    timestamps = pd.to_datetime(['2025-09-29T00:00:00Z'] * 3, utc=True)

    # Get schema first to ensure type consistency
    schema = get_raw_schema('stocks_daily')

    data = {
        'year': pa.array([2025, 2025, 2025], type=pa.int16()),
        'month': pa.array([9, 9, 9], type=pa.int8()),
        'symbol': pa.array(['AAPL', 'AAPL', 'AAPL'], type=pa.string()),
        'date': pa.array([datetime(2025, 9, 29).date()] * 3, type=pa.date32()),
        'timestamp': pa.array(timestamps, type=pa.timestamp('ns', tz='UTC')),
        'open': pa.array([100.0, 101.0, 102.0], type=pa.float32()),
        'high': pa.array([101.0, 102.0, 103.0], type=pa.float32()),
        'low': pa.array([99.0, 100.0, 101.0], type=pa.float32()),
        'close': pa.array([100.5, 101.5, 102.5], type=pa.float32()),
        'volume': pa.array([1000000, 2000000, 3000000], type=pa.uint64()),
        'transactions': pa.array([1000, 2000, 3000], type=pa.uint32()),
    }

    return pa.table(data, schema=schema)


def test_parquet_manager_initialization(parquet_manager, test_parquet_root):
    """Test ParquetManager initialization"""
    assert parquet_manager.data_type == 'stocks_daily'
    assert parquet_manager.root_path == test_parquet_root
    assert parquet_manager.dataset_path == test_parquet_root / 'stocks_daily'
    assert parquet_manager.dataset_path.exists()


def test_write_partition(parquet_manager, sample_table):
    """Test writing a partition"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}

    parquet_manager.write_partition(sample_table, partition_values)

    # Check file exists
    expected_path = parquet_manager._build_partition_path(partition_values)
    assert expected_path.exists()


def test_read_partition(parquet_manager, sample_table):
    """Test reading a partition"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}

    # Write then read
    parquet_manager.write_partition(sample_table, partition_values)
    result = parquet_manager.read_partition(partition_values)

    assert len(result) == len(sample_table)
    # Check that core data columns are present (partition columns may have different types after read)
    data_columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'transactions']
    for col in data_columns:
        assert col in result.schema.names

    # Verify data values match
    assert result.column('symbol').to_pylist() == sample_table.column('symbol').to_pylist()


def test_read_partition_not_found(parquet_manager):
    """Test reading non-existent partition raises error"""
    partition_values = {'year': 2025, 'month': 1, 'date': '2025-01-01'}

    with pytest.raises(ParquetManagerError, match="Partition not found"):
        parquet_manager.read_partition(partition_values)


def test_read_partition_with_columns(parquet_manager, sample_table):
    """Test reading partition with column subset"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}

    parquet_manager.write_partition(sample_table, partition_values)
    result = parquet_manager.read_partition(partition_values, columns=['symbol', 'close'])

    # Check requested columns are present
    assert 'symbol' in result.schema.names
    assert 'close' in result.schema.names
    assert len(result) == len(sample_table)


def test_list_partitions(parquet_manager, sample_table):
    """Test listing partitions"""
    # Write multiple partitions
    dates = ['2025-09-26', '2025-09-27', '2025-09-29']

    for date in dates:
        partition_values = {'year': 2025, 'month': 9, 'date': date}
        parquet_manager.write_partition(sample_table, partition_values)

    # List partitions
    partitions = parquet_manager.list_partitions()

    assert len(partitions) == 3
    assert all('partition' in p for p in partitions)
    assert all('size_mb' in p for p in partitions)


def test_get_statistics(parquet_manager, sample_table):
    """Test getting dataset statistics"""
    # Initially empty
    stats = parquet_manager.get_statistics()
    assert stats['total_partitions'] == 0

    # Write partition
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}
    parquet_manager.write_partition(sample_table, partition_values)

    # Check statistics
    stats = parquet_manager.get_statistics()
    assert stats['total_partitions'] == 1
    assert stats['total_size_mb'] > 0
    assert stats['data_type'] == 'stocks_daily'


def test_delete_partition(parquet_manager, sample_table):
    """Test deleting a partition"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}

    # Write partition
    parquet_manager.write_partition(sample_table, partition_values)
    assert len(parquet_manager.list_partitions()) == 1

    # Delete partition
    parquet_manager.delete_partition(partition_values)
    assert len(parquet_manager.list_partitions()) == 0


def test_build_partition_path(parquet_manager):
    """Test partition path building"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}
    path = parquet_manager._build_partition_path(partition_values)

    assert 'year=2025' in str(path)
    assert 'month=09' in str(path)
    assert 'date=2025-09-29.parquet' in str(path)


def test_build_partition_path_with_symbol(test_parquet_root):
    """Test partition path building for minute data with symbol"""
    manager = ParquetManager(test_parquet_root, 'stocks_minute')
    partition_values = {'symbol': 'AAPL', 'year': 2025, 'month': 9}
    path = manager._build_partition_path(partition_values)

    assert 'symbol=AAPL' in str(path)
    assert 'year=2025' in str(path)


def test_parse_partition_path(parquet_manager):
    """Test parsing partition values from path"""
    partition_values = {'year': 2025, 'month': 9, 'date': '2025-09-29'}
    path = parquet_manager._build_partition_path(partition_values)

    parsed = parquet_manager._parse_partition_path(path)

    assert parsed['year'] == '2025'
    assert parsed['month'] == '09'
    assert parsed['date'] == '2025-09-29'


def test_repr(parquet_manager):
    """Test string representation"""
    repr_str = repr(parquet_manager)
    assert 'ParquetManager' in repr_str
    assert 'stocks_daily' in repr_str


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
