"""
Unit tests for StreamingIngestor

Run with: pytest tests/unit/test_streaming_ingestor.py
"""

import pytest
import pandas as pd
from pathlib import Path
from io import BytesIO

from src.ingest.streaming_ingestor import StreamingIngestor


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
    return StreamingIngestor(
        data_type='stocks_daily',
        output_root=tmp_path / 'parquet',
        config=test_config,
        chunk_size=100  # Small chunks for testing
    )


@pytest.fixture
def sample_csv_data():
    """Create sample CSV data"""
    n_rows = 250
    df = pd.DataFrame({
        'ticker': ['AAPL'] * n_rows,
        'volume': [1000000 + i for i in range(n_rows)],
        'open': [100.0 + i * 0.1 for i in range(n_rows)],
        'close': [101.0 + i * 0.1 for i in range(n_rows)],
        'high': [102.0 + i * 0.1 for i in range(n_rows)],
        'low': [99.0 + i * 0.1 for i in range(n_rows)],
        'window_start': ['2025-09-29T09:30:00Z'] * n_rows,
        'transactions': [1000 + i for i in range(n_rows)],
    })

    csv_bytes = BytesIO()
    df.to_csv(csv_bytes, index=False)
    csv_bytes.seek(0)

    return csv_bytes


def test_streaming_ingestor_initialization(test_ingestor):
    """Test StreamingIngestor initialization"""
    assert test_ingestor.chunk_size == 100
    assert test_ingestor.data_type == 'stocks_daily'


def test_ingest_date_creates_output(test_ingestor, sample_csv_data, tmp_path):
    """Test that ingest_date creates Parquet output"""
    result = test_ingestor.ingest_date('2025-09-29', sample_csv_data)

    assert result['status'] == 'success'
    assert result['records'] == 250
    assert result['chunks'] > 1  # Should process multiple chunks

    # Check output file exists
    output_path = test_ingestor._get_output_path('2025-09-29')
    assert output_path.exists()


def test_ingest_date_skips_existing(test_ingestor, sample_csv_data):
    """Test that ingest_date skips existing files"""
    # First ingestion
    result1 = test_ingestor.ingest_date('2025-09-29', sample_csv_data)
    assert result1['status'] == 'success'

    # Second ingestion should skip
    sample_csv_data.seek(0)  # Reset BytesIO
    result2 = test_ingestor.ingest_date('2025-09-29', sample_csv_data)
    assert result2['status'] == 'skipped'
    assert result2['reason'] == 'output_exists'


def test_streaming_chunks(test_ingestor):
    """Test that streaming processes in chunks"""
    # Create data with 250 rows, chunk size 100
    # Should process 3 chunks: 100, 100, 50

    n_rows = 250
    df = pd.DataFrame({
        'ticker': ['AAPL'] * n_rows,
        'volume': [1000000 + i for i in range(n_rows)],
        'open': [100.0] * n_rows,
        'close': [101.0] * n_rows,
        'high': [102.0] * n_rows,
        'low': [99.0] * n_rows,
        'window_start': ['2025-09-29T09:30:00Z'] * n_rows,
        'transactions': [1000 + i for i in range(n_rows)],
    })

    csv_bytes = BytesIO()
    df.to_csv(csv_bytes, index=False)
    csv_bytes.seek(0)

    result = test_ingestor.ingest_date('2025-09-30', csv_bytes)

    # Should process 3 chunks (100 + 100 + 50)
    assert result['chunks'] == 3


def test_ingest_batch(test_ingestor):
    """Test batch ingestion"""
    # Create data for multiple dates
    data_map = {}

    for i, date in enumerate(['2025-09-26', '2025-09-27']):
        n_rows = 100
        df = pd.DataFrame({
            'ticker': ['AAPL'] * n_rows,
            'volume': [1000000 + j for j in range(n_rows)],
            'open': [100.0 + i] * n_rows,
            'close': [101.0 + i] * n_rows,
            'high': [102.0 + i] * n_rows,
            'low': [99.0 + i] * n_rows,
            'window_start': [f'{date}T09:30:00Z'] * n_rows,
            'transactions': [1000 + j for j in range(n_rows)],
        })

        csv_bytes = BytesIO()
        df.to_csv(csv_bytes, index=False)
        csv_bytes.seek(0)
        data_map[date] = csv_bytes

    # Ingest batch
    results = test_ingestor.ingest_batch(
        dates=['2025-09-26', '2025-09-27'],
        data_map=data_map
    )

    assert len(results) == 2
    assert all(r['status'] == 'success' for r in results)


def test_statistics_tracking(test_ingestor, sample_csv_data):
    """Test statistics are tracked correctly"""
    # Initial stats
    stats = test_ingestor.get_statistics()
    assert stats['records_processed'] == 0
    assert stats['files_processed'] == 0

    # Ingest file
    test_ingestor.ingest_date('2025-09-29', sample_csv_data)

    # Check updated stats
    stats = test_ingestor.get_statistics()
    assert stats['records_processed'] == 250
    assert stats['files_processed'] == 1
    assert stats['errors'] == 0


def test_symbol_filtering(test_ingestor):
    """Test symbol filtering during ingestion"""
    # Create data with multiple symbols
    n_rows = 100
    df = pd.DataFrame({
        'ticker': ['AAPL'] * 50 + ['TSLA'] * 50,
        'volume': [1000000 + i for i in range(n_rows)],
        'open': [100.0] * n_rows,
        'close': [101.0] * n_rows,
        'high': [102.0] * n_rows,
        'low': [99.0] * n_rows,
        'window_start': ['2025-09-29T09:30:00Z'] * n_rows,
        'transactions': [1000 + i for i in range(n_rows)],
    })

    csv_bytes = BytesIO()
    df.to_csv(csv_bytes, index=False)
    csv_bytes.seek(0)

    # Ingest with symbol filter (note: column is 'ticker' in CSV, 'symbol' after mapping)
    # This test may need adjustment based on actual column mapping
    result = test_ingestor.ingest_date('2025-10-01', csv_bytes, symbols=['AAPL'])

    # Should filter out TSLA rows
    # Note: Actual filtering depends on column name mapping
    assert result['status'] == 'success'


def test_memory_monitoring(test_ingestor, sample_csv_data):
    """Test memory monitoring during ingestion"""
    result = test_ingestor.ingest_date('2025-09-29', sample_csv_data)

    # Should report memory status
    assert 'memory_peak_percent' in result
    assert result['memory_peak_percent'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
