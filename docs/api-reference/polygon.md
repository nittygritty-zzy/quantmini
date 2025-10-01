# Polygon.io API Reference

## Official Documentation

**IMPORTANT**: Always refer to the official Polygon.io documentation before making changes:
- **Library Interface**: https://polygon.readthedocs.io/en/latest/Library-Interface-Documentation.html

## Overview

This project integrates with Polygon.io's **Flat Files (S3)** data service, not the REST API or WebSocket streaming.

## Flat Files Integration

### Data Access Method

We use Polygon.io's S3-based flat files for bulk historical data:

```python
from src.download.s3_catalog import S3Catalog
from src.download.async_downloader import AsyncS3Downloader

# S3 credentials configuration
credentials = {
    'access_key_id': 'YOUR_POLYGON_S3_KEY',
    'secret_access_key': 'YOUR_POLYGON_S3_SECRET'
}

# Initialize catalog and downloader
catalog = S3Catalog()
downloader = AsyncS3Downloader(
    credentials=credentials,
    endpoint_url='https://files.polygon.io',
    max_concurrent=4
)
```

### S3 Path Structure

```
s3://flatfiles/
├── us_stocks_sip/
│   ├── day_aggs_v1/{year}/{month}/{date}.csv.gz
│   └── minute_aggs_v1/{year}/{month}/{date}.csv.gz
└── us_options_opra/
    ├── day_aggs_v1/{year}/{month}/{date}.csv.gz
    └── minute_aggs_v1/{year}/{month}/{date}.csv.gz
```

## Data Types Supported

### 1. Stocks Daily Aggregates
- **Path**: `us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz`
- **Frequency**: Daily OHLCV
- **Coverage**: All US stocks

### 2. Stocks Minute Aggregates
- **Path**: `us_stocks_sip/minute_aggs_v1/{year}/{month}/{date}.csv.gz`
- **Frequency**: 1-minute OHLCV
- **Coverage**: All US stocks

### 3. Options Daily Aggregates
- **Path**: `us_options_opra/day_aggs_v1/{year}/{month}/{date}.csv.gz`
- **Frequency**: Daily OHLCV
- **Coverage**: All US options contracts

### 4. Options Minute Aggregates
- **Path**: `us_options_opra/minute_aggs_v1/{year}/{month}/{date}.csv.gz`
- **Frequency**: 1-minute OHLCV
- **Coverage**: All US options contracts

## Key Differences from REST API

| Feature | Flat Files (Our Approach) | REST API |
|---------|--------------------------|----------|
| Data Access | Bulk S3 downloads | Individual API calls |
| Rate Limiting | None (S3 throughput) | Yes (API limits) |
| Cost | Subscription-based | Per-request |
| Latency | Minutes (bulk) | Seconds (real-time) |
| Use Case | Historical backtesting | Real-time trading |

## Authentication

### S3 Credentials

Configure in `config/credentials.yaml`:

```yaml
polygon:
  s3:
    access_key_id: "YOUR_POLYGON_S3_ACCESS_KEY"
    secret_access_key: "YOUR_POLYGON_S3_SECRET_KEY"
    endpoint_url: "https://files.polygon.io"
    bucket: "flatfiles"
```

### Security Notes

- **Never commit credentials** to version control
- Use environment variables for CI/CD: `POLYGON_S3_ACCESS_KEY_ID`
- Rotate keys periodically
- Credentials file is in `.gitignore`

## Usage Patterns

### Download Single Date

```python
import asyncio
from src.orchestration.ingestion_orchestrator import IngestionOrchestrator

async def download_data():
    orchestrator = IngestionOrchestrator()

    result = await orchestrator.ingest_date(
        data_type='stocks_daily',
        date='2025-09-29'
    )

    print(f"Downloaded {result['records_processed']:,} records")

asyncio.run(download_data())
```

### Download Date Range

```python
async def download_range():
    orchestrator = IngestionOrchestrator()

    result = await orchestrator.ingest_date_range(
        data_type='stocks_daily',
        start_date='2025-08-01',
        end_date='2025-09-30',
        incremental=True  # Skip already downloaded dates
    )

    print(f"Downloaded {result['ingested']} files")

asyncio.run(download_range())
```

## Data Schema

See [Data Schemas Reference](../reference/data-schemas.md) for detailed field definitions.

## Best Practices

### 1. Use Incremental Processing
```python
# Good: Skip already downloaded data
result = await orchestrator.ingest_date_range(
    data_type='stocks_daily',
    start_date='2025-08-01',
    end_date='2025-09-30',
    incremental=True  # ✓ Efficient
)
```

### 2. Parallel Downloads
```python
# Configure max concurrent downloads
downloader = AsyncS3Downloader(
    credentials=credentials,
    max_concurrent=8  # Adjust based on bandwidth
)
```

### 3. Error Handling
```python
try:
    result = await orchestrator.ingest_date(
        data_type='stocks_daily',
        date='2025-09-29'
    )
except IngestionOrchestratorError as e:
    logger.error(f"Ingestion failed: {e}")
    # Implement retry logic or alerting
```

### 4. Memory Management
```python
# Use streaming ingestor for large files
result = await orchestrator.ingest_date(
    data_type='stocks_minute',  # Large file
    date='2025-09-29',
    use_polars=False  # Streaming mode for memory safety
)
```

## Monitoring

### Download Statistics

```python
# Get orchestration statistics
stats = orchestrator.get_statistics()

print(f"Downloads: {stats['downloads']}")
print(f"Errors: {stats['errors']}")
print(f"Data: {stats['bytes_downloaded'] / 1024**3:.2f} GB")
print(f"Records: {stats['records_processed']:,}")
```

### Metadata Tracking

```python
from src.storage.metadata_manager import MetadataManager

metadata_manager = MetadataManager('data/metadata')

# Get ingestion status
status = metadata_manager.get_ingestion_status(
    data_type='stocks_daily',
    date='2025-09-29'
)

print(f"Status: {status['status']}")
print(f"Records: {status['statistics']['records']}")
```

## Troubleshooting

### Common Issues

1. **Invalid Credentials**
   - Verify S3 keys in `config/credentials.yaml`
   - Check key permissions in Polygon.io dashboard

2. **Download Failures**
   - Check network connectivity
   - Verify S3 endpoint URL
   - Check available disk space

3. **Missing Dates**
   - Polygon.io only provides trading days
   - Weekends/holidays will have no data
   - Use `S3Catalog.get_business_days()` to get valid dates

## Related Documentation

- [Polygon.io Official Library Docs](https://polygon.readthedocs.io/en/latest/Library-Interface-Documentation.html)
- [Polygon S3 Flat Files Guide](polygon-s3-flatfiles.md)
- [Data Schemas Reference](../reference/data-schemas.md)
- [Data Ingestion Guide](../guides/data-ingestion.md)
