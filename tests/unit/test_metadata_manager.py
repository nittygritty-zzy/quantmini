"""
Unit tests for MetadataManager

Run with: pytest tests/unit/test_metadata_manager.py
"""

import pytest
from pathlib import Path
from datetime import datetime

from src.storage.metadata_manager import MetadataManager, MetadataManagerError


@pytest.fixture
def metadata_root(tmp_path):
    """Create temporary metadata root directory"""
    return tmp_path / 'metadata'


@pytest.fixture
def metadata_manager(metadata_root):
    """Create test metadata manager"""
    return MetadataManager(metadata_root)


def test_metadata_manager_initialization(metadata_manager, metadata_root):
    """Test MetadataManager initialization"""
    assert metadata_manager.metadata_root == metadata_root
    assert metadata_root.exists()


def test_record_ingestion(metadata_manager):
    """Test recording ingestion result"""
    metadata_manager.record_ingestion(
        data_type='stocks_daily',
        date='2025-09-29',
        status='success',
        statistics={'records': 1000, 'file_size_mb': 10.5}
    )

    # Check metadata file exists
    status = metadata_manager.get_ingestion_status('stocks_daily', '2025-09-29')
    assert status is not None
    assert status['status'] == 'success'
    assert status['statistics']['records'] == 1000


def test_record_ingestion_with_error(metadata_manager):
    """Test recording failed ingestion with error"""
    metadata_manager.record_ingestion(
        data_type='stocks_daily',
        date='2025-09-28',
        status='failed',
        statistics={},
        error='Download failed'
    )

    status = metadata_manager.get_ingestion_status('stocks_daily', '2025-09-28')
    assert status['status'] == 'failed'
    assert status['error'] == 'Download failed'


def test_record_ingestion_with_symbol(metadata_manager):
    """Test recording ingestion for minute data with symbol"""
    metadata_manager.record_ingestion(
        data_type='stocks_minute',
        date='2025-09-29',
        status='success',
        statistics={'records': 500},
        symbol='AAPL'
    )

    status = metadata_manager.get_ingestion_status(
        'stocks_minute',
        '2025-09-29',
        symbol='AAPL'
    )
    assert status is not None
    assert status['symbol'] == 'AAPL'


def test_get_ingestion_status_not_found(metadata_manager):
    """Test getting status for non-existent ingestion"""
    status = metadata_manager.get_ingestion_status('stocks_daily', '2020-01-01')
    assert status is None


def test_list_ingestions(metadata_manager):
    """Test listing ingestion records"""
    # Record multiple ingestions
    dates = ['2025-09-26', '2025-09-27', '2025-09-29']

    for date in dates:
        metadata_manager.record_ingestion(
            data_type='stocks_daily',
            date=date,
            status='success',
            statistics={'records': 1000}
        )

    # List all
    records = metadata_manager.list_ingestions('stocks_daily')
    assert len(records) == 3


def test_list_ingestions_with_date_filter(metadata_manager):
    """Test listing with date range filter"""
    dates = ['2025-09-26', '2025-09-27', '2025-09-28', '2025-09-29']

    for date in dates:
        metadata_manager.record_ingestion(
            data_type='stocks_daily',
            date=date,
            status='success',
            statistics={}
        )

    # Filter by date range
    records = metadata_manager.list_ingestions(
        'stocks_daily',
        start_date='2025-09-27',
        end_date='2025-09-28'
    )

    assert len(records) == 2
    assert all('2025-09-27' <= r['date'] <= '2025-09-28' for r in records)


def test_list_ingestions_with_status_filter(metadata_manager):
    """Test listing with status filter"""
    # Record with different statuses
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-26', 'success', {}
    )
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-27', 'failed', {}
    )
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-28', 'success', {}
    )

    # Filter by status
    records = metadata_manager.list_ingestions('stocks_daily', status='success')
    assert len(records) == 2
    assert all(r['status'] == 'success' for r in records)


def test_get_watermark(metadata_manager):
    """Test getting watermark"""
    # No records yet
    assert metadata_manager.get_watermark('stocks_daily') is None

    # Record ingestions
    metadata_manager.record_ingestion('stocks_daily', '2025-09-26', 'success', {})
    metadata_manager.record_ingestion('stocks_daily', '2025-09-29', 'success', {})

    # Get watermark (latest date)
    watermark = metadata_manager.get_watermark('stocks_daily')
    assert watermark == '2025-09-29'


def test_set_watermark(metadata_manager):
    """Test setting watermark"""
    metadata_manager.set_watermark('stocks_daily', '2025-09-30')

    # Verify watermark file exists
    watermark_file = metadata_manager._get_watermark_file('stocks_daily')
    assert watermark_file.exists()


def test_get_missing_dates(metadata_manager):
    """Test finding missing dates"""
    expected_dates = ['2025-09-26', '2025-09-27', '2025-09-29', '2025-09-30']

    # Record some dates
    metadata_manager.record_ingestion('stocks_daily', '2025-09-26', 'success', {})
    metadata_manager.record_ingestion('stocks_daily', '2025-09-29', 'success', {})

    # Find missing
    missing = metadata_manager.get_missing_dates(
        'stocks_daily',
        '2025-09-26',
        '2025-09-30',
        expected_dates
    )

    assert set(missing) == {'2025-09-27', '2025-09-30'}


def test_get_statistics_summary(metadata_manager):
    """Test getting aggregated statistics"""
    # Record multiple ingestions
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-26', 'success',
        {'records': 1000, 'file_size_mb': 10.0}
    )
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-27', 'success',
        {'records': 2000, 'file_size_mb': 20.0}
    )
    metadata_manager.record_ingestion(
        'stocks_daily', '2025-09-28', 'failed',
        {}
    )

    # Get summary
    summary = metadata_manager.get_statistics_summary('stocks_daily')

    assert summary['total_jobs'] == 3
    assert summary['success'] == 2
    assert summary['failed'] == 1
    assert summary['total_records'] == 3000
    assert summary['total_size_mb'] == 30.0
    assert summary['success_rate'] == 2/3


def test_delete_metadata(metadata_manager):
    """Test deleting metadata"""
    metadata_manager.record_ingestion('stocks_daily', '2025-09-29', 'success', {})

    # Verify exists
    assert metadata_manager.get_ingestion_status('stocks_daily', '2025-09-29') is not None

    # Delete
    metadata_manager.delete_metadata('stocks_daily', '2025-09-29')

    # Verify deleted
    assert metadata_manager.get_ingestion_status('stocks_daily', '2025-09-29') is None


def test_get_metadata_file_path(metadata_manager):
    """Test metadata file path generation"""
    path = metadata_manager._get_metadata_file('stocks_daily', '2025-09-29')

    assert '2025' in str(path)
    assert '09' in str(path)
    assert '2025-09-29.json' in str(path)


def test_get_metadata_file_path_with_symbol(metadata_manager):
    """Test metadata file path with symbol"""
    path = metadata_manager._get_metadata_file('stocks_minute', '2025-09-29', 'AAPL')

    assert '2025-09-29_AAPL.json' in str(path)


def test_repr(metadata_manager):
    """Test string representation"""
    repr_str = repr(metadata_manager)
    assert 'MetadataManager' in repr_str


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
