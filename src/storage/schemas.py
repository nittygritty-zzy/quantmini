"""
Parquet Schema Definitions - PyArrow schemas for all data types

This module defines optimized PyArrow schemas for stocks and options data
with memory-efficient data types.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import pyarrow as pa
from typing import Dict


def get_stocks_daily_schema() -> pa.Schema:
    """
    Stock daily aggregates schema with memory-optimized types

    Columns from Polygon S3:
    - ticker, volume, open, close, high, low, window_start, transactions

    Returns:
        PyArrow schema
    """
    return pa.schema([
        # Partition columns
        pa.field('year', pa.int16()),
        pa.field('month', pa.int8()),

        # Data columns from S3
        pa.field('symbol', pa.dictionary(pa.int16(), pa.string())),
        pa.field('date', pa.date32()),
        pa.field('timestamp', pa.timestamp('ns', tz='UTC')),
        pa.field('open', pa.float32()),
        pa.field('high', pa.float32()),
        pa.field('low', pa.float32()),
        pa.field('close', pa.float32()),
        pa.field('volume', pa.uint64()),
        pa.field('transactions', pa.uint32()),

        # Enriched features (added in feature engineering)
        pa.field('alpha_daily', pa.float32(), nullable=True),
        pa.field('price_range', pa.float32(), nullable=True),
        pa.field('daily_return', pa.float32(), nullable=True),
        pa.field('vwap', pa.float32(), nullable=True),
        pa.field('relative_volume', pa.float32(), nullable=True),
    ])


def get_stocks_minute_schema() -> pa.Schema:
    """
    Stock minute aggregates schema with symbol partitioning

    Returns:
        PyArrow schema
    """
    return pa.schema([
        # Partition columns
        pa.field('symbol', pa.dictionary(pa.int16(), pa.string())),
        pa.field('year', pa.int16()),
        pa.field('month', pa.int8()),

        # Data columns from S3
        pa.field('timestamp', pa.timestamp('ns', tz='America/New_York')),
        pa.field('open', pa.float32()),
        pa.field('high', pa.float32()),
        pa.field('low', pa.float32()),
        pa.field('close', pa.float32()),
        pa.field('volume', pa.uint32()),
        pa.field('transactions', pa.uint32()),

        # Enriched features
        pa.field('alpha_minute', pa.float32(), nullable=True),
        pa.field('price_velocity', pa.float32(), nullable=True),
        pa.field('minute_return', pa.float32(), nullable=True),
    ])


def get_options_daily_schema() -> pa.Schema:
    """
    Options daily aggregates schema with underlying partitioning

    Returns:
        PyArrow schema
    """
    return pa.schema([
        # Partition columns
        pa.field('underlying', pa.dictionary(pa.int16(), pa.string()), nullable=True),
        pa.field('year', pa.int16()),
        pa.field('month', pa.int8()),

        # Data columns from S3
        pa.field('ticker', pa.string()),
        pa.field('date', pa.date32()),
        pa.field('timestamp', pa.timestamp('ns', tz='UTC')),
        pa.field('expiration_date', pa.date32(), nullable=True),
        pa.field('contract_type', pa.dictionary(pa.int8(), pa.string()), nullable=True),  # 'call' or 'put'
        pa.field('strike_price', pa.float32(), nullable=True),
        pa.field('open', pa.float32()),
        pa.field('high', pa.float32()),
        pa.field('low', pa.float32()),
        pa.field('close', pa.float32()),
        pa.field('volume', pa.uint32()),
        pa.field('transactions', pa.uint32()),

        # Enriched features
        pa.field('moneyness', pa.float32(), nullable=True),
        pa.field('days_to_expiry', pa.int16(), nullable=True),
        pa.field('relative_volume', pa.float32(), nullable=True),
    ])


def get_options_minute_schema() -> pa.Schema:
    """
    Options minute aggregates schema with date partitioning

    Returns:
        PyArrow schema
    """
    return pa.schema([
        # Partition columns
        pa.field('underlying', pa.dictionary(pa.int16(), pa.string()), nullable=True),
        pa.field('date', pa.date32()),

        # Data columns from S3
        pa.field('ticker', pa.string()),
        pa.field('timestamp', pa.timestamp('ns', tz='America/New_York')),
        pa.field('expiration_date', pa.date32(), nullable=True),
        pa.field('contract_type', pa.dictionary(pa.int8(), pa.string()), nullable=True),
        pa.field('strike_price', pa.float32(), nullable=True),
        pa.field('open', pa.float32()),
        pa.field('high', pa.float32()),
        pa.field('low', pa.float32()),
        pa.field('close', pa.float32()),
        pa.field('volume', pa.uint32()),
        pa.field('transactions', pa.uint32()),
    ])


# Schema registry
SCHEMAS: Dict[str, pa.Schema] = {
    'stocks_daily': get_stocks_daily_schema(),
    'stocks_minute': get_stocks_minute_schema(),
    'options_daily': get_options_daily_schema(),
    'options_minute': get_options_minute_schema(),
}


def get_schema(data_type: str) -> pa.Schema:
    """
    Get schema for data type

    Args:
        data_type: Data type ('stocks_daily', 'stocks_minute', 'options_daily', 'options_minute')

    Returns:
        PyArrow schema

    Raises:
        ValueError: If data_type is unknown
    """
    if data_type not in SCHEMAS:
        raise ValueError(f"Unknown data_type: {data_type}. Must be one of {list(SCHEMAS.keys())}")

    return SCHEMAS[data_type]


def get_raw_schema(data_type: str) -> pa.Schema:
    """
    Get schema for raw data (before feature engineering)

    Args:
        data_type: Data type

    Returns:
        PyArrow schema with only raw columns (no enriched features)
    """
    schema = get_schema(data_type)

    # Filter out enriched feature columns (marked as nullable=True)
    raw_fields = []
    for field in schema:
        # Keep partition columns and core data columns
        if field.name in ['year', 'month', 'symbol', 'underlying', 'date', 'timestamp',
                          'ticker', 'open', 'high', 'low', 'close', 'volume', 'transactions',
                          'expiration_date', 'contract_type', 'strike_price']:
            raw_fields.append(field)

    return pa.schema(raw_fields)


def print_schema_info(data_type: str):
    """
    Print human-readable schema information

    Args:
        data_type: Data type to display
    """
    schema = get_schema(data_type)

    print(f"\n{'='*70}")
    print(f"Schema: {data_type}")
    print(f"{'='*70}\n")

    partition_fields = []
    data_fields = []
    feature_fields = []

    for field in schema:
        field_info = f"{field.name:20} {str(field.type):20}"

        if field.name in ['year', 'month', 'symbol', 'underlying', 'date']:
            partition_fields.append(field_info)
        elif field.nullable:
            feature_fields.append(field_info + " (enriched)")
        else:
            data_fields.append(field_info)

    print("📊 Partition Columns:")
    for f in partition_fields:
        print(f"  {f}")

    print("\n📦 Data Columns:")
    for f in data_fields:
        print(f"  {f}")

    if feature_fields:
        print("\n✨ Feature Columns (enriched):")
        for f in feature_fields:
            print(f"  {f}")

    print(f"\n{'='*70}\n")


def main():
    """Command-line interface for schema viewer"""
    print("PyArrow Schema Definitions")
    print("="*70)

    for data_type in SCHEMAS.keys():
        print_schema_info(data_type)

    # Memory estimation
    print("\n💾 Memory Estimation (per 1M records):")
    print("  stocks_daily:    ~120 MB")
    print("  stocks_minute:   ~100 MB")
    print("  options_daily:   ~180 MB")
    print("  options_minute:  ~160 MB")

    print("\n📝 Data Type Savings vs float64/int64:")
    print("  float32 vs float64:  50% savings")
    print("  uint32 vs int64:     50% savings")
    print("  dictionary encoding: 30-70% savings for symbols")

    print(f"\n{'='*70}\n")


if __name__ == '__main__':
    main()
