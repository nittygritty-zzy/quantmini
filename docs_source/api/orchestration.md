# Orchestration Module (`src.orchestration`)

Pipeline coordination and workflow management.

## Ingestion Orchestrator

**Module**: `src.orchestration.ingestion_orchestrator`

Coordinate S3 downloads and Parquet ingestion with error handling and incremental processing.

### Class: `IngestionOrchestrator`

```python
IngestionOrchestrator(
    config: Optional[ConfigLoader] = None,
    credentials: Optional[Dict[str, str]] = None,
    parquet_root: Optional[Path] = None,
    metadata_root: Optional[Path] = None
)
```

**Parameters:**
- `config`: Configuration loader (auto-created if None)
- `credentials`: Polygon credentials (loaded from config if None)
- `parquet_root`: Parquet data root (from config if None)
- `metadata_root`: Metadata root (from config if None)

**Key Methods:**

#### `ingest_date_range(data_type: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None, incremental: bool = True, use_polars: bool = False) -> Dict[str, Any]`
Ingest data for date range (async).

```python
from src.orchestration import IngestionOrchestrator
import asyncio

async def main():
    orchestrator = IngestionOrchestrator()
    
    result = await orchestrator.ingest_date_range(
        data_type='stocks_daily',
        start_date='2024-01-01',
        end_date='2024-01-31',
        incremental=True,
        use_polars=True
    )
    
    print(f"Ingested {result['dates_processed']} dates")
    print(f"Total records: {result['total_records']}")
    print(f"Success rate: {result['success_rate']:.1%}")

asyncio.run(main())
```

**Returns:**
```python
{
    'dates_processed': int,
    'dates_succeeded': int,
    'dates_failed': int,
    'total_records': int,
    'total_size_mb': float,
    'elapsed_time': float,
    'success_rate': float,
    'failed_dates': List[str]
}
```

#### `ingest_date(data_type: str, date: str, symbols: Optional[List[str]] = None, use_polars: bool = False) -> Dict[str, Any]`
Ingest single date (async).

```python
result = await orchestrator.ingest_date(
    data_type='stocks_daily',
    date='2024-01-02',
    use_polars=True
)
```

#### `backfill(data_type: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None) -> Dict[str, Any]`
Backfill missing dates (async).

```python
# Automatically detects and ingests missing dates
result = await orchestrator.backfill(
    data_type='stocks_daily',
    start_date='2024-01-01',
    end_date='2024-12-31'
)

print(f"Backfilled {result['dates_processed']} missing dates")
```

#### `get_statistics() -> Dict[str, Any]`
Get orchestration statistics.

```python
stats = orchestrator.get_statistics()
print(f"Total downloads: {stats['downloads']['total']}")
print(f"Total ingestions: {stats['ingestions']['total']}")
```

#### `reset_statistics()`
Reset statistics counters.

---

## Complete Pipeline Example

### Daily Update Pipeline

```python
from src.orchestration import IngestionOrchestrator
from datetime import datetime, timedelta
import asyncio

async def daily_update():
    """Run daily update pipeline."""
    orchestrator = IngestionOrchestrator()
    
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Ingest yesterday's data
    result = await orchestrator.ingest_date(
        data_type='stocks_daily',
        date=yesterday,
        use_polars=True
    )
    
    if result['status'] == 'success':
        print(f"✓ Daily update complete: {result['records']} records")
    else:
        print(f"✗ Daily update failed: {result.get('error', 'Unknown error')}")
    
    return result

if __name__ == '__main__':
    asyncio.run(daily_update())
```

### Backfill Pipeline

```python
async def backfill_historical():
    """Backfill historical data."""
    orchestrator = IngestionOrchestrator()
    
    # Backfill 2024 data
    result = await orchestrator.ingest_date_range(
        data_type='stocks_daily',
        start_date='2024-01-01',
        end_date='2024-12-31',
        incremental=True,
        use_polars=True
    )
    
    print(f"Backfill complete:")
    print(f"  Dates processed: {result['dates_processed']}")
    print(f"  Success rate: {result['success_rate']:.1%}")
    print(f"  Total records: {result['total_records']:,}")
    print(f"  Total size: {result['total_size_mb']:.2f} MB")
    print(f"  Time elapsed: {result['elapsed_time']:.2f}s")
    
    if result['failed_dates']:
        print(f"  Failed dates: {', '.join(result['failed_dates'])}")

asyncio.run(backfill_historical())
```

### Multi-Type Pipeline

```python
async def ingest_all_types():
    """Ingest all data types."""
    orchestrator = IngestionOrchestrator()
    
    data_types = ['stocks_daily', 'stocks_minute', 'options_daily']
    date_range = ('2024-01-01', '2024-01-31')
    
    for data_type in data_types:
        print(f"\nIngesting {data_type}...")
        
        result = await orchestrator.ingest_date_range(
            data_type=data_type,
            start_date=date_range[0],
            end_date=date_range[1],
            incremental=True,
            use_polars=True
        )
        
        print(f"  ✓ {result['dates_processed']} dates, {result['total_records']:,} records")

asyncio.run(ingest_all_types())
```

---

## Orchestration Features

### Incremental Processing
- Automatically skips already-ingested dates
- Uses watermarks to track progress
- Efficient for daily updates

```python
# First run: ingests all dates
await orchestrator.ingest_date_range(
    'stocks_daily',
    '2024-01-01',
    '2024-01-31',
    incremental=True
)

# Second run: skips already-ingested dates
await orchestrator.ingest_date_range(
    'stocks_daily',
    '2024-01-01',
    '2024-02-29',
    incremental=True
)  # Only ingests February
```

### Error Handling
- Retries failed downloads (exponential backoff)
- Continues on individual date failures
- Returns list of failed dates
- Detailed error logging

### Memory Management
- Automatic memory monitoring
- Adaptive processing mode selection
- Garbage collection between batches

### Progress Tracking
- Metadata tracking for each ingestion
- Statistics for downloads and ingestions
- Success rate monitoring

---

## Configuration

The orchestrator uses configuration from `config/pipeline_config.yaml`:

```yaml
pipeline:
  mode: adaptive  # streaming, batch, parallel, or adaptive
  data_root: data
  
s3_source:
  bucket: flatfiles
  endpoint_url: https://files.polygon.io
  
processing:
  use_polars: true
  chunk_size: 100000
  
parquet:
  compression: snappy
  row_group_size: 1000000
```

Override with environment variables:
```bash
export PIPELINE_MODE=streaming
export MAX_MEMORY_GB=16
python scripts/run_pipeline.py
```

---

## Integration with Other Modules

```python
from src.orchestration import IngestionOrchestrator
from src.features import FeatureEngineer
from src.transform import QlibBinaryWriter
from src.core import ConfigLoader
from pathlib import Path
import asyncio

async def full_pipeline():
    """Complete pipeline: ingest → enrich → convert."""
    config = ConfigLoader()
    
    # 1. Ingest raw data
    orchestrator = IngestionOrchestrator()
    ingest_result = await orchestrator.ingest_date_range(
        'stocks_daily',
        '2024-01-01',
        '2024-01-31',
        incremental=True,
        use_polars=True
    )
    print(f"✓ Ingestion: {ingest_result['total_records']} records")
    
    # 2. Enrich with features
    engineer = FeatureEngineer(
        parquet_root=Path('data/lake'),
        enriched_root=Path('data/enriched'),
        config=config
    )
    enrich_result = engineer.enrich_date_range(
        'stocks_daily',
        '2024-01-01',
        '2024-01-31',
        incremental=True
    )
    print(f"✓ Enrichment: {enrich_result['records_processed']} records")
    
    # 3. Convert to Qlib binary
    writer = QlibBinaryWriter(
        enriched_root=Path('data/enriched'),
        qlib_root=Path('data/binary'),
        config=config
    )
    convert_result = writer.convert_data_type(
        'stocks_daily',
        '2024-01-01',
        '2024-01-31',
        incremental=True
    )
    print(f"✓ Conversion: {convert_result['symbols_converted']} symbols")

asyncio.run(full_pipeline())
```
