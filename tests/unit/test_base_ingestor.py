"""
Unit tests for BaseIngestor

Run with: pytest tests/unit/test_base_ingestor.py
"""

import pytest
import pandas as pd
import pyarrow as pa
from pathlib import Path
from io import BytesIO

from src.ingest.base_ingestor import BaseIngestor, IngestionError


class TestIngestor(BaseIngestor):
    """Concrete implementation for testing"""

    def ingest_date(self, date, data, symbols=None):
        """Minimal implementation for testing"""
        return {'date': date, 'status': 'success'}


@pytest.fixture
def test_config():
    """Configuration for testing"""
    return {
        'resource_limits': {
            'max_memory_gb': 14.0,
            'max_memory_percent': 70,
            'chunk_size_mb': 100,
        }
    }


@pytest.fixture
def test_ingestor(tmp_path, test_config):
    """Create test ingestor instance"""
    return TestIngestor(
        data_type='stocks_daily',
        output_root=tmp_path / 'parquet',
        config=test_config
    )


def test_base_ingestor_initialization(test_ingestor, tmp_path):
    """Test BaseIngestor initialization"""
    assert test_ingestor.data_type == 'stocks_daily'
    assert test_ingestor.output_root == tmp_path / 'parquet'
    assert test_ingestor.records_processed == 0
    assert test_ingestor.files_processed == 0


def test_base_ingestor_schema_loading(test_ingestor):
    """Test schema loading"""
    assert test_ingestor.schema is not None
    assert test_ingestor.raw_schema is not None
    assert isinstance(test_ingestor.schema, pa.Schema)


def test_optimize_dtypes(test_ingestor):
    """Test dtype optimization"""
    # Create DataFrame with suboptimal types
    df = pd.DataFrame({
        'open': [100.5, 101.2, 102.1],
        'close': [101.0, 102.0, 103.0],
        'volume': [1000000, 2000000, 3000000],
        'symbol': ['AAPL', 'AAPL', 'AAPL'],
    })

    # Convert to float64/int64 (worst case)
    df['open'] = df['open'].astype('float64')
    df['close'] = df['close'].astype('float64')
    df['volume'] = df['volume'].astype('int64')

    original_memory = df.memory_usage(deep=True).sum()

    # Optimize
    df_optimized = test_ingestor._optimize_dtypes(df)

    optimized_memory = df_optimized.memory_usage(deep=True).sum()

    # Should use less memory
    assert optimized_memory < original_memory

    # Check types
    assert df_optimized['open'].dtype == 'float32'
    assert df_optimized['close'].dtype == 'float32'


def test_add_partition_columns(test_ingestor):
    """Test adding partition columns"""
    df = pd.DataFrame({
        'symbol': ['AAPL', 'TSLA'],
        'close': [100.0, 200.0],
    })

    df_with_partitions = test_ingestor._add_partition_columns(df, '2025-09-29')

    assert 'year' in df_with_partitions.columns
    assert 'month' in df_with_partitions.columns
    assert df_with_partitions['year'].iloc[0] == 2025
    assert df_with_partitions['month'].iloc[0] == 9

    # Partition columns should be first
    assert df_with_partitions.columns[0] == 'year'
    assert df_with_partitions.columns[1] == 'month'


def test_get_output_path_date_partitioned(test_ingestor, tmp_path):
    """Test output path for date-partitioned data"""
    path = test_ingestor._get_output_path('2025-09-29')

    assert 'stocks_daily' in str(path)
    assert 'year=2025' in str(path)
    assert 'month=09' in str(path)
    assert 'date=2025-09-29.parquet' in str(path)


def test_get_output_path_symbol_partitioned(test_ingestor, tmp_path):
    """Test output path for symbol-partitioned data"""
    path = test_ingestor._get_output_path('2025-09-29', symbol='AAPL')

    assert 'symbol=AAPL' in str(path)
    assert 'year=2025' in str(path)
    assert 'month=09' in str(path)


def test_statistics_tracking(test_ingestor):
    """Test statistics tracking"""
    # Initial state
    stats = test_ingestor.get_statistics()
    assert stats['records_processed'] == 0
    assert stats['files_processed'] == 0

    # Update statistics
    test_ingestor.records_processed = 1000
    test_ingestor.files_processed = 1
    test_ingestor.errors = 0

    stats = test_ingestor.get_statistics()
    assert stats['records_processed'] == 1000
    assert stats['files_processed'] == 1

    # Reset statistics
    test_ingestor.reset_statistics()
    stats = test_ingestor.get_statistics()
    assert stats['records_processed'] == 0
    assert stats['files_processed'] == 0


def test_read_csv(test_ingestor):
    """Test CSV reading"""
    # Create sample CSV
    csv_data = "symbol,open,close,volume\nAAPL,100.0,101.0,1000000\nTSLA,200.0,201.0,2000000"
    csv_bytes = BytesIO(csv_data.encode('utf-8'))

    # Read CSV
    df = test_ingestor._read_csv(csv_bytes)

    assert len(df) == 2
    assert 'symbol' in df.columns
    assert 'open' in df.columns
    assert df['symbol'].iloc[0] == 'AAPL'


def test_output_directory_creation(test_ingestor, tmp_path):
    """Test that output directory is created"""
    assert (tmp_path / 'parquet').exists()


def test_memory_monitor_integration(test_ingestor):
    """Test memory monitor is available"""
    assert test_ingestor.memory_monitor is not None

    mem_status = test_ingestor.memory_monitor.check_and_wait()
    assert 'action' in mem_status
    assert 'system_percent' in mem_status
    assert 'process_gb' in mem_status


def test_repr(test_ingestor):
    """Test string representation"""
    repr_str = repr(test_ingestor)
    assert 'TestIngestor' in repr_str
    assert 'stocks_daily' in repr_str


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
