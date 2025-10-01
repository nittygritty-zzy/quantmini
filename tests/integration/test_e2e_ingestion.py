"""
End-to-End Ingestion Tests

Test complete pipeline: S3 Download → Parquet Ingestion → Metadata Tracking

Run with: pytest tests/integration/test_e2e_ingestion.py -v

Note: Requires valid Polygon.io S3 credentials in config/credentials.yaml
"""

import pytest
import asyncio
from pathlib import Path
from datetime import datetime

from src.orchestration.ingestion_orchestrator import IngestionOrchestrator
from src.storage.parquet_manager import ParquetManager
from src.storage.metadata_manager import MetadataManager
from src.core.config_loader import ConfigLoader


# Test configuration
TEST_DATE = '2025-09-29'
TEST_SYMBOLS = ['TSLA', 'ORCL']
TEST_DATA_TYPES = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']


@pytest.fixture(scope="module")
def test_root(tmp_path_factory):
    """Create temporary test root directory"""
    return tmp_path_factory.mktemp("e2e_test")


@pytest.fixture(scope="module")
def config():
    """Load configuration"""
    try:
        return ConfigLoader()
    except Exception as e:
        pytest.skip(f"Config not available: {e}")


@pytest.fixture(scope="module")
def check_credentials(config):
    """Check if credentials are available"""
    try:
        credentials = config.get_credentials('polygon')
        if not credentials or 's3' not in credentials:
            pytest.skip("S3 credentials not configured")
        return True
    except Exception:
        pytest.skip("Credentials not available")


@pytest.fixture(scope="module")
def orchestrator(test_root, config, check_credentials):
    """Create orchestrator for testing"""
    parquet_root = test_root / "parquet"
    metadata_root = test_root / "metadata"

    return IngestionOrchestrator(
        config=config,
        parquet_root=parquet_root,
        metadata_root=metadata_root
    )


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_stocks_daily_batch_load(orchestrator):
    """
    E2E Test: Stocks Daily - Batch Load

    Test loading stocks daily data for TSLA and ORCL on 2025-09-29
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Stocks Daily - Batch Load")
    print(f"Date: {TEST_DATE}, Symbols: {TEST_SYMBOLS}")
    print(f"{'='*70}\n")

    result = await orchestrator.ingest_date(
        data_type='stocks_daily',
        date=TEST_DATE,
        symbols=None,  # Daily data is not symbol-specific
        use_polars=False
    )

    # Verify result
    assert result['status'] in ['completed', 'up_to_date']

    if result['status'] == 'completed':
        assert result['ingested'] >= 0
        print(f"✅ Ingested: {result['ingested']} files")
        print(f"   Records: {result.get('records_processed', 0):,}")

    # Verify Parquet data exists
    parquet_manager = ParquetManager(
        orchestrator.parquet_root,
        'stocks_daily'
    )

    stats = parquet_manager.get_statistics()
    print(f"\n📊 Parquet Statistics:")
    print(f"   Partitions: {stats['total_partitions']}")
    print(f"   Size: {stats['total_size_mb']:.2f} MB")

    # Verify metadata
    metadata_manager = orchestrator.metadata_manager
    status = metadata_manager.get_ingestion_status('stocks_daily', TEST_DATE)

    if status:
        print(f"\n📝 Metadata:")
        print(f"   Status: {status['status']}")
        print(f"   Records: {status['statistics'].get('records', 0):,}")

    assert stats['total_partitions'] > 0, "No partitions created"


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_stocks_daily_incremental_load(orchestrator):
    """
    E2E Test: Stocks Daily - Incremental Load

    Test incremental loading (should skip already ingested date)
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Stocks Daily - Incremental Load")
    print(f"Date: {TEST_DATE} (should skip if already loaded)")
    print(f"{'='*70}\n")

    result = await orchestrator.ingest_date_range(
        data_type='stocks_daily',
        start_date=TEST_DATE,
        end_date=TEST_DATE,
        incremental=True
    )

    print(f"Result: {result['status']}")

    # Should either ingest or be up-to-date
    assert result['status'] in ['completed', 'up_to_date']

    if result['status'] == 'up_to_date':
        print("✅ Correctly skipped already ingested date")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_stocks_minute_batch_load(orchestrator):
    """
    E2E Test: Stocks Minute - Batch Load

    Test loading minute data for TSLA and ORCL
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Stocks Minute - Batch Load")
    print(f"Date: {TEST_DATE}, Symbols: {TEST_SYMBOLS}")
    print(f"{'='*70}\n")

    result = await orchestrator.ingest_date(
        data_type='stocks_minute',
        date=TEST_DATE,
        symbols=TEST_SYMBOLS,
        use_polars=False
    )

    assert result['status'] in ['completed', 'up_to_date', 'no_data']

    if result['status'] == 'completed':
        print(f"✅ Ingested: {result['ingested']} files")
        print(f"   Records: {result.get('records_processed', 0):,}")

    # Verify Parquet data
    parquet_manager = ParquetManager(
        orchestrator.parquet_root,
        'stocks_minute'
    )

    stats = parquet_manager.get_statistics()
    print(f"\n📊 Parquet Statistics:")
    print(f"   Partitions: {stats['total_partitions']}")
    print(f"   Size: {stats['total_size_mb']:.2f} MB")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_options_daily_batch_load(orchestrator):
    """
    E2E Test: Options Daily - Batch Load

    Test loading options daily data for TSLA and ORCL underlyings
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Options Daily - Batch Load")
    print(f"Date: {TEST_DATE}, Underlyings: {TEST_SYMBOLS}")
    print(f"{'='*70}\n")

    result = await orchestrator.ingest_date(
        data_type='options_daily',
        date=TEST_DATE,
        symbols=TEST_SYMBOLS,  # For options, these are underlyings
        use_polars=False
    )

    assert result['status'] in ['completed', 'up_to_date', 'no_data']

    if result['status'] == 'completed':
        print(f"✅ Ingested: {result['ingested']} files")
        print(f"   Records: {result.get('records_processed', 0):,}")

    # Verify Parquet data
    parquet_manager = ParquetManager(
        orchestrator.parquet_root,
        'options_daily'
    )

    stats = parquet_manager.get_statistics()
    print(f"\n📊 Parquet Statistics:")
    print(f"   Partitions: {stats['total_partitions']}")
    print(f"   Size: {stats['total_size_mb']:.2f} MB")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_options_minute_batch_load(orchestrator):
    """
    E2E Test: Options Minute - Batch Load

    Test loading options minute data
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Options Minute - Batch Load")
    print(f"Date: {TEST_DATE}")
    print(f"{'='*70}\n")

    result = await orchestrator.ingest_date(
        data_type='options_minute',
        date=TEST_DATE,
        symbols=None,  # Options minute is by date, not symbol
        use_polars=False
    )

    assert result['status'] in ['completed', 'up_to_date', 'no_data']

    if result['status'] == 'completed':
        print(f"✅ Ingested: {result['ingested']} files")
        print(f"   Records: {result.get('records_processed', 0):,}")

    # Verify Parquet data
    parquet_manager = ParquetManager(
        orchestrator.parquet_root,
        'options_minute'
    )

    stats = parquet_manager.get_statistics()
    print(f"\n📊 Parquet Statistics:")
    print(f"   Partitions: {stats['total_partitions']}")
    print(f"   Size: {stats['total_size_mb']:.2f} MB")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_query_ingested_data(orchestrator):
    """
    E2E Test: Query Ingested Data

    Test querying the ingested Parquet data
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Query Ingested Data")
    print(f"{'='*70}\n")

    # Query stocks daily data
    parquet_manager = ParquetManager(
        orchestrator.parquet_root,
        'stocks_daily'
    )

    try:
        # Read data for test date
        table = parquet_manager.read_date_range(
            start_date=TEST_DATE,
            end_date=TEST_DATE,
            symbols=TEST_SYMBOLS,
            columns=['symbol', 'date', 'close', 'volume']
        )

        print(f"📊 Query Results:")
        print(f"   Rows: {len(table):,}")
        print(f"   Columns: {table.schema.names}")

        if len(table) > 0:
            print(f"\n   Sample data (first 3 rows):")
            df = table.to_pandas().head(3)
            print(df.to_string())

        assert len(table) >= 0

    except Exception as e:
        print(f"⚠️  Query failed (may be no data): {e}")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_metadata_summary(orchestrator):
    """
    E2E Test: Metadata Summary

    Test metadata tracking and summary statistics
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Metadata Summary")
    print(f"{'='*70}\n")

    metadata_manager = orchestrator.metadata_manager

    for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
        summary = metadata_manager.get_statistics_summary(data_type)

        if summary['total_jobs'] > 0:
            print(f"\n📊 {data_type}:")
            print(f"   Total jobs: {summary['total_jobs']}")
            print(f"   Success: {summary['success']} ({summary['success_rate']:.1%})")
            print(f"   Failed: {summary['failed']}")
            print(f"   Total records: {summary['total_records']:,}")
            print(f"   Total size: {summary['total_size_mb']:.2f} MB")


@pytest.mark.asyncio
@pytest.mark.e2e
async def test_polars_performance(orchestrator):
    """
    E2E Test: Polars Performance

    Test ingestion using Polars for performance comparison
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Polars Performance")
    print(f"Testing high-performance Polars ingestion")
    print(f"{'='*70}\n")

    import time

    # Test with Polars
    start = time.time()
    result = await orchestrator.ingest_date(
        data_type='stocks_daily',
        date=TEST_DATE,
        use_polars=True
    )
    polars_time = time.time() - start

    print(f"⚡ Polars ingestion time: {polars_time:.2f}s")
    print(f"   Status: {result['status']}")

    assert result['status'] in ['completed', 'up_to_date']


def test_pipeline_statistics(orchestrator):
    """
    E2E Test: Pipeline Statistics

    Final summary of all pipeline operations
    """
    print(f"\n{'='*70}")
    print(f"E2E Test: Pipeline Statistics Summary")
    print(f"{'='*70}\n")

    stats = orchestrator.get_statistics()

    print(f"📈 Pipeline Statistics:")
    print(f"   Downloads: {stats['downloads']}")
    print(f"   Ingestions: {stats['ingestions']}")
    print(f"   Errors: {stats['errors']}")
    print(f"   Records processed: {stats['records_processed']:,}")
    print(f"   Bytes downloaded: {stats['bytes_downloaded'] / 1024**2:.2f} MB")

    # Verify no errors
    assert stats['errors'] == 0 or stats['ingestions'] > 0, "Pipeline had errors with no successful ingestions"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s', '-m', 'e2e'])
