"""
Unit tests for Parquet schemas

Run with: pytest tests/unit/test_schemas.py
"""

import pytest
import pyarrow as pa
from src.storage.schemas import (
    get_schema,
    get_raw_schema,
    get_stocks_daily_schema,
    get_stocks_minute_schema,
    get_options_daily_schema,
    get_options_minute_schema,
    SCHEMAS
)


def test_schema_registry():
    """Test schema registry contains all data types"""
    assert 'stocks_daily' in SCHEMAS
    assert 'stocks_minute' in SCHEMAS
    assert 'options_daily' in SCHEMAS
    assert 'options_minute' in SCHEMAS


def test_get_schema_stocks_daily():
    """Test getting stocks daily schema"""
    schema = get_schema('stocks_daily')

    assert isinstance(schema, pa.Schema)
    assert 'symbol' in schema.names
    assert 'open' in schema.names
    assert 'close' in schema.names
    assert 'volume' in schema.names
    assert 'year' in schema.names
    assert 'month' in schema.names


def test_get_schema_stocks_minute():
    """Test getting stocks minute schema"""
    schema = get_schema('stocks_minute')

    assert isinstance(schema, pa.Schema)
    assert 'symbol' in schema.names
    assert 'timestamp' in schema.names
    assert 'volume' in schema.names


def test_get_schema_options_daily():
    """Test getting options daily schema"""
    schema = get_schema('options_daily')

    assert isinstance(schema, pa.Schema)
    assert 'underlying' in schema.names
    assert 'ticker' in schema.names
    assert 'strike_price' in schema.names
    assert 'contract_type' in schema.names
    assert 'expiration_date' in schema.names


def test_get_schema_options_minute():
    """Test getting options minute schema"""
    schema = get_schema('options_minute')

    assert isinstance(schema, pa.Schema)
    assert 'underlying' in schema.names
    assert 'ticker' in schema.names
    assert 'timestamp' in schema.names


def test_get_schema_invalid():
    """Test getting invalid schema raises error"""
    with pytest.raises(ValueError, match="Unknown data_type"):
        get_schema('invalid_type')


def test_stocks_daily_schema_types():
    """Test stocks daily schema has correct types"""
    schema = get_stocks_daily_schema()

    # Check partition column types
    assert schema.field('year').type == pa.int16()
    assert schema.field('month').type == pa.int8()

    # Check data column types
    assert isinstance(schema.field('symbol').type, pa.DictionaryType)
    assert schema.field('date').type == pa.date32()
    assert schema.field('open').type == pa.float32()
    assert schema.field('high').type == pa.float32()
    assert schema.field('low').type == pa.float32()
    assert schema.field('close').type == pa.float32()
    assert schema.field('volume').type == pa.uint64()
    assert schema.field('transactions').type == pa.uint32()

    # Check enriched feature columns (nullable)
    assert schema.field('alpha_daily').nullable is True
    assert schema.field('price_range').nullable is True


def test_stocks_minute_schema_types():
    """Test stocks minute schema has correct types"""
    schema = get_stocks_minute_schema()

    assert schema.field('symbol').type == pa.dictionary(pa.int16(), pa.string())
    assert schema.field('year').type == pa.int16()
    assert schema.field('month').type == pa.int8()
    assert schema.field('volume').type == pa.uint32()
    assert schema.field('transactions').type == pa.uint32()


def test_options_daily_schema_types():
    """Test options daily schema has correct types"""
    schema = get_options_daily_schema()

    assert schema.field('underlying').type == pa.dictionary(pa.int16(), pa.string())
    assert schema.field('contract_type').type == pa.dictionary(pa.int8(), pa.string())
    assert schema.field('strike_price').type == pa.float32()
    assert schema.field('expiration_date').type == pa.date32()
    assert schema.field('moneyness').nullable is True


def test_options_minute_schema_types():
    """Test options minute schema has correct types"""
    schema = get_options_minute_schema()

    assert schema.field('underlying').type == pa.dictionary(pa.int16(), pa.string())
    assert schema.field('date').type == pa.date32()
    assert schema.field('strike_price').type == pa.float32()


def test_get_raw_schema():
    """Test getting raw schema without enriched features"""
    raw_schema = get_raw_schema('stocks_daily')

    # Should have core columns
    assert 'symbol' in raw_schema.names
    assert 'open' in raw_schema.names
    assert 'volume' in raw_schema.names

    # Should NOT have enriched features
    assert 'alpha_daily' not in raw_schema.names
    assert 'price_range' not in raw_schema.names
    assert 'daily_return' not in raw_schema.names


def test_schema_field_count():
    """Test schema field counts"""
    stocks_daily = get_stocks_daily_schema()
    stocks_minute = get_stocks_minute_schema()
    options_daily = get_options_daily_schema()
    options_minute = get_options_minute_schema()

    # Stocks daily should have most fields (with enriched features)
    assert len(stocks_daily) > len(stocks_minute)

    # Options schemas should have contract-specific fields
    assert len(options_daily) > 10
    assert len(options_minute) > 8


def test_schema_memory_optimization():
    """Test that schemas use memory-optimized types"""
    schema = get_stocks_daily_schema()

    # Check that we use float32 instead of float64
    assert schema.field('open').type == pa.float32()
    assert schema.field('close').type == pa.float32()

    # Check that we use appropriate integer types
    assert schema.field('year').type == pa.int16()  # 2-byte int
    assert schema.field('month').type == pa.int8()  # 1-byte int

    # Check dictionary encoding for categorical columns
    assert isinstance(schema.field('symbol').type, pa.DictionaryType)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
