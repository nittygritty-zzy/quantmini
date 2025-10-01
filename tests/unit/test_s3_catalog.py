"""
Unit tests for S3Catalog

Run with: pytest tests/unit/test_s3_catalog.py
"""

import pytest
from src.download.s3_catalog import S3Catalog


def test_s3_catalog_creation():
    """Test S3Catalog initialization"""
    catalog = S3Catalog()
    assert catalog.bucket == 'flatfiles'

    catalog = S3Catalog(bucket='test-bucket')
    assert catalog.bucket == 'test-bucket'


def test_get_stocks_daily_key():
    """Test stock daily key generation"""
    catalog = S3Catalog()
    key = catalog.get_stocks_daily_key('2025-09-29')

    assert key == 'us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz'
    assert key.startswith(catalog.STOCKS_DAILY_PREFIX)
    assert key.endswith('.csv.gz')


def test_get_stocks_minute_key():
    """Test stock minute key generation"""
    catalog = S3Catalog()
    key = catalog.get_stocks_minute_key('2025-09-29')

    assert key == 'us_stocks_sip/minute_aggs_v1/2025/09/2025-09-29.csv.gz'
    assert key.startswith(catalog.STOCKS_MINUTE_PREFIX)
    assert key.endswith('.csv.gz')


def test_get_options_daily_key():
    """Test options daily key generation"""
    catalog = S3Catalog()
    key = catalog.get_options_daily_key('2025-09-29')

    assert key == 'us_options_opra/day_aggs_v1/2025/09/2025-09-29.csv.gz'
    assert key.startswith(catalog.OPTIONS_DAILY_PREFIX)
    assert key.endswith('.csv.gz')


def test_get_options_minute_key():
    """Test options minute key generation"""
    catalog = S3Catalog()
    key = catalog.get_options_minute_key('2025-09-29')

    assert key == 'us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz'


def test_get_date_range_keys_stocks_daily():
    """Test date range key generation for stocks daily"""
    catalog = S3Catalog()
    keys = catalog.get_date_range_keys('stocks_daily', '2025-09-26', '2025-09-30')

    # Should include only business days (excludes weekend)
    assert len(keys) == 3  # 26th (Thu), 29th (Sun->Mon), 30th (Mon)
    assert all(k.startswith(catalog.STOCKS_DAILY_PREFIX) for k in keys)


def test_get_date_range_keys_stocks_minute():
    """Test date range key generation for stocks minute"""
    catalog = S3Catalog()
    keys = catalog.get_date_range_keys(
        'stocks_minute',
        '2025-09-26',
        '2025-09-30'
    )

    # 3 business days (Thu 26, Fri 27, Mon 30)
    assert len(keys) == 3
    assert all('minute_aggs_v1' in key for key in keys)


def test_get_date_range_keys_invalid_type():
    """Test invalid data type raises error"""
    catalog = S3Catalog()

    with pytest.raises(ValueError, match="Invalid data_type"):
        catalog.get_date_range_keys('invalid_type', '2025-09-01', '2025-09-30')


def test_get_date_range_keys_with_symbols():
    """Test that symbols parameter is accepted (but optional)"""
    catalog = S3Catalog()

    # Symbols parameter should be optional now
    keys = catalog.get_date_range_keys('stocks_minute', '2025-09-26', '2025-09-30', symbols=['AAPL'])
    assert len(keys) >= 0  # Should not raise error


def test_parse_key_metadata_stocks_daily():
    """Test metadata parsing for stocks daily"""
    catalog = S3Catalog()
    key = 'us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz'

    metadata = catalog.parse_key_metadata(key)

    assert metadata['data_type'] == 'stocks_daily'
    assert metadata['year'] == '2025'
    assert metadata['month'] == '09'
    assert metadata['date'] == '2025-09-29'


def test_parse_key_metadata_stocks_minute():
    """Test metadata parsing for stocks minute"""
    catalog = S3Catalog()
    key = 'us_stocks_sip/minute_aggs_v1/2025/09/2025-09-29.csv.gz'

    metadata = catalog.parse_key_metadata(key)

    assert metadata['data_type'] == 'stocks_minute'
    assert metadata['year'] == '2025'
    assert metadata['month'] == '09'
    assert metadata['date'] == '2025-09-29'


def test_parse_key_metadata_invalid():
    """Test parsing invalid key raises error"""
    catalog = S3Catalog()

    with pytest.raises(ValueError, match="Unknown key pattern"):
        catalog.parse_key_metadata('invalid/key/pattern.csv.gz')


def test_get_business_days():
    """Test business days generation"""
    days = S3Catalog.get_business_days('2025-09-26', '2025-09-30')

    # Should exclude weekend (27th, 28th)
    assert len(days) == 3
    assert '2025-09-26' in days  # Thursday
    assert '2025-09-29' in days  # Monday
    assert '2025-09-30' in days  # Tuesday
    assert '2025-09-27' not in days  # Saturday
    assert '2025-09-28' not in days  # Sunday


def test_get_missing_dates():
    """Test missing dates calculation"""
    existing = ['2025-09-26']
    missing = S3Catalog.get_missing_dates(existing, '2025-09-26', '2025-09-30')

    assert len(missing) == 2
    assert '2025-09-29' in missing
    assert '2025-09-30' in missing
    assert '2025-09-26' not in missing


def test_validate_key():
    """Test key validation"""
    catalog = S3Catalog()

    # Valid keys
    assert catalog.validate_key('us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz') is True
    assert catalog.validate_key('us_stocks_sip/minute_aggs_v1/2025/09/AAPL.csv.gz') is True

    # Invalid keys
    assert catalog.validate_key('invalid/key.csv.gz') is False
    assert catalog.validate_key('not_a_key') is False


def test_get_summary():
    """Test summary statistics"""
    catalog = S3Catalog()

    keys = [
        catalog.get_stocks_daily_key('2025-09-29'),
        catalog.get_stocks_daily_key('2025-09-30'),
        catalog.get_stocks_minute_key('2025-09-29'),
        'invalid/key.csv.gz',
    ]

    summary = catalog.get_summary(keys)

    assert summary['total'] == 4
    assert summary['stocks_daily'] == 2
    assert summary['stocks_minute'] == 1
    assert summary['invalid'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
