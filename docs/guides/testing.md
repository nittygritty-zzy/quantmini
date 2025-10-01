# End-to-End Test Instructions

## Prerequisites

### 1. Configure Credentials

Copy the example credentials file and add your Polygon.io credentials:

```bash
cp config/credentials.yaml.example config/credentials.yaml
```

Edit `config/credentials.yaml` and add your credentials:
```yaml
polygon:
  s3:
    access_key_id: "YOUR_POLYGON_S3_ACCESS_KEY"
    secret_access_key: "YOUR_POLYGON_S3_SECRET_KEY"
    endpoint_url: "https://files.polygon.io"
    bucket: "flatfiles"
```

**Note**: Never commit `config/credentials.yaml` to git (it's already in `.gitignore`)

### 2. Verify Setup

Check that your credentials are configured:
```bash
python -c "from src.core.config_loader import ConfigLoader; c = ConfigLoader(); print('✅ Credentials OK' if c.get_credentials('polygon') else '❌ No credentials')"
```

## Running End-to-End Tests

### Test Coverage

The e2e tests cover all combinations:
- **Data Types**: stocks_daily, stocks_minute, options_daily, options_minute
- **Loading Modes**: batch, incremental
- **Symbols**: TSLA, ORCL
- **Date**: 2025-09-29

### Run All E2E Tests

```bash
# Activate virtual environment
source .venv/bin/activate

# Run all e2e tests (requires credentials)
pytest tests/integration/test_e2e_ingestion.py -v -s -m e2e

# Run specific test
pytest tests/integration/test_e2e_ingestion.py::test_stocks_daily_batch_load -v -s

# Run with detailed output
pytest tests/integration/test_e2e_ingestion.py -v -s -m e2e --tb=short
```

### Test Scenarios

#### 1. Stocks Daily - Batch Load
```bash
pytest tests/integration/test_e2e_ingestion.py::test_stocks_daily_batch_load -v -s
```
- Downloads: `us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz`
- Ingests: All stocks for 2025-09-29
- Verifies: Parquet files created, metadata tracked

#### 2. Stocks Daily - Incremental Load
```bash
pytest tests/integration/test_e2e_ingestion.py::test_stocks_daily_incremental_load -v -s
```
- Tests: Incremental processing (should skip already loaded date)
- Verifies: Watermark tracking works correctly

#### 3. Stocks Minute - Batch Load
```bash
pytest tests/integration/test_e2e_ingestion.py::test_stocks_minute_batch_load -v -s
```
- Downloads: `us_stocks_sip/minute_aggs_v1/2025/09/TSLA.csv.gz`
- Downloads: `us_stocks_sip/minute_aggs_v1/2025/09/ORCL.csv.gz`
- Ingests: Minute data for TSLA and ORCL
- Verifies: Symbol-partitioned Parquet files

#### 4. Options Daily - Batch Load
```bash
pytest tests/integration/test_e2e_ingestion.py::test_options_daily_batch_load -v -s
```
- Downloads: `us_options_opra/day_aggs_v1/2025/09/TSLA.csv.gz`
- Downloads: `us_options_opra/day_aggs_v1/2025/09/ORCL.csv.gz`
- Ingests: Options chains for TSLA and ORCL
- Verifies: Underlying-partitioned Parquet files

#### 5. Options Minute - Batch Load
```bash
pytest tests/integration/test_e2e_ingestion.py::test_options_minute_batch_load -v -s
```
- Downloads: `us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz`
- Ingests: Options minute data for all contracts
- Verifies: Date-partitioned Parquet files

#### 6. Query Ingested Data
```bash
pytest tests/integration/test_e2e_ingestion.py::test_query_ingested_data -v -s
```
- Queries: Stocks daily data for TSLA and ORCL
- Filters: By date range and symbols
- Verifies: Data can be read back correctly

#### 7. Metadata Summary
```bash
pytest tests/integration/test_e2e_ingestion.py::test_metadata_summary -v -s
```
- Reports: Ingestion statistics for all data types
- Shows: Success rates, record counts, file sizes

#### 8. Polars Performance
```bash
pytest tests/integration/test_e2e_ingestion.py::test_polars_performance -v -s
```
- Tests: High-performance Polars ingestion
- Compares: Performance vs StreamingIngestor
- Expected: 5-10x faster

#### 9. Pipeline Statistics
```bash
pytest tests/integration/test_e2e_ingestion.py::test_pipeline_statistics -v -s
```
- Summary: Overall pipeline statistics
- Reports: Downloads, ingestions, errors, bytes processed

## Expected Results

### Successful Test Output

```
E2E Test: Stocks Daily - Batch Load
Date: 2025-09-29, Symbols: ['TSLA', 'ORCL']
======================================================================

✅ Ingested: 1 files
   Records: 12,543

📊 Parquet Statistics:
   Partitions: 1
   Size: 1.23 MB

📝 Metadata:
   Status: success
   Records: 12,543
```

### Test Data Directory Structure

After successful tests, you'll have:

```
data/
├── parquet/
│   ├── stocks_daily/
│   │   └── year=2025/
│   │       └── month=09/
│   │           └── date=2025-09-29.parquet
│   ├── stocks_minute/
│   │   ├── symbol=TSLA/
│   │   │   └── year=2025/
│   │   │       └── month=09/
│   │   │           └── data.parquet
│   │   └── symbol=ORCL/
│   │       └── year=2025/
│   │           └── month=09/
│   │               └── data.parquet
│   ├── options_daily/
│   │   ├── underlying=TSLA/
│   │   │   └── year=2025/
│   │   │       └── month=09/
│   │   │           └── data.parquet
│   │   └── underlying=ORCL/
│   │       └── year=2025/
│   │           └── month=09/
│   │               └── data.parquet
│   └── options_minute/
│       └── year=2025/
│           └── month=09/
│               └── date=2025-09-29.parquet
└── metadata/
    ├── stocks_daily/
    │   └── 2025/
    │       └── 09/
    │           └── 2025-09-29.json
    ├── stocks_minute/
    │   └── 2025/
    │       └── 09/
    │           ├── 2025-09-29_TSLA.json
    │           └── 2025-09-29_ORCL.json
    ├── options_daily/
    │   └── 2025/
    │       └── 09/
    │           ├── 2025-09-29_TSLA.json
    │           └── 2025-09-29_ORCL.json
    └── options_minute/
        └── 2025/
            └── 09/
                └── 2025-09-29.json
```

## Troubleshooting

### No Credentials Error
```
SKIPPED [1] tests/integration/test_e2e_ingestion.py:44: S3 credentials not configured
```
**Solution**: Configure `config/credentials.yaml` with your Polygon.io credentials

### Download Failures
```
ERROR: Download failed: File not found
```
**Solution**:
- Check date is valid (not weekend, not future)
- Check symbols exist on Polygon
- Verify credentials have access to flatfiles bucket

### No Data for Date
```
Result: no_data
```
**Solution**: Some data types may not have files for specific dates. This is normal.

### Memory Issues
```
MemoryError: Cannot allocate memory
```
**Solution**:
- Tests use StreamingIngestor by default (safe for 24GB Mac)
- Reduce concurrent downloads in config if needed

## Performance Benchmarks

Expected performance on 24GB Mac (M-series):

| Data Type       | File Size | Ingestion Time | Records/sec |
|----------------|-----------|----------------|-------------|
| Stocks Daily   | ~50 MB    | 10-15 sec      | 1-2M        |
| Stocks Minute  | ~100 MB   | 20-30 sec      | 500K-1M     |
| Options Daily  | ~200 MB   | 30-45 sec      | 500K        |
| Options Minute | ~300 MB   | 60-90 sec      | 300K        |

Polars should be 5-10x faster than these benchmarks.

## Next Steps After Tests Pass

1. **Review Parquet Data**:
   ```python
   from src.storage.parquet_manager import ParquetManager
   from pathlib import Path

   manager = ParquetManager(Path('data/parquet'), 'stocks_daily')
   stats = manager.get_statistics()
   print(stats)
   ```

2. **Query Data**:
   ```python
   table = manager.read_date_range(
       '2025-09-29', '2025-09-29',
       symbols=['TSLA', 'ORCL'],
       columns=['symbol', 'close', 'volume']
   )
   df = table.to_pandas()
   print(df)
   ```

3. **Check Metadata**:
   ```python
   from src.storage.metadata_manager import MetadataManager
   from src.core.config_loader import ConfigLoader

   config = ConfigLoader()
   metadata = MetadataManager(config.get_data_root() / 'metadata')
   summary = metadata.get_statistics_summary('stocks_daily')
   print(summary)
   ```

4. **Set Up Scheduled Ingestion**: See `IMPLEMENTATION_PLAN.md` for Phase 5-14

---

**Ready to run tests?**

```bash
# 1. Configure credentials
cp config/credentials.yaml.example config/credentials.yaml
# Edit config/credentials.yaml with your Polygon credentials

# 2. Run all e2e tests
source .venv/bin/activate
pytest tests/integration/test_e2e_ingestion.py -v -s -m e2e
```
