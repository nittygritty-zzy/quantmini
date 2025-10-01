# Ingest Module (`src.ingest`)

Data ingestion with Polars and streaming support for different memory constraints.

## Base Ingestor

**Module**: `src.ingest.base_ingestor`

Abstract base class with common ingestion functionality.

### Class: `BaseIngestor`

```python
BaseIngestor(
    data_type: str,
    output_root: Path,
    config: Dict[str, Any],
    memory_monitor: Optional[AdvancedMemoryMonitor] = None
)
```

**Parameters:**
- `data_type`: Data type (`'stocks_daily'`, `'stocks_minute'`, etc.)
- `output_root`: Output directory for Parquet files
- `config`: Pipeline configuration dict
- `memory_monitor`: Optional memory monitor

**Abstract Methods:**

#### `ingest_date(date: str, data: BytesIO, symbols: Optional[List[str]] = None) -> Dict[str, Any]`
Process single date file (must be implemented by subclasses).

**Common Methods:**

#### `get_statistics() -> Dict[str, Any]`
Get ingestion statistics.

Returns:
```python
{
    'records_processed': int,
    'files_processed': int,
    'bytes_processed': int,
    'errors': int
}
```

#### `reset_statistics()`
Reset statistics counters.

**Key Features:**
- CSV parsing with automatic dtype detection
- 50-70% memory reduction through dtype optimization
- PyArrow schema validation
- Parquet writing with Snappy compression

---

## Polars Ingestor

**Module**: `src.ingest.polars_ingestor`

High-performance ingestion using Polars (5-10x faster than pandas).

### Class: `PolarsIngestor(BaseIngestor)`

```python
PolarsIngestor(
    data_type: str,
    output_root: Path,
    config: Dict[str, Any],
    streaming: bool = True
)
```

**Parameters:**
- `data_type`: Data type
- `output_root`: Output directory
- `config`: Configuration
- `streaming`: Enable streaming mode (default: True)

**Key Methods:**

#### `ingest_date(date: str, data: BytesIO, symbols: Optional[List[str]] = None) -> Dict[str, Any]`
Process single date with Polars.

```python
from src.ingest import PolarsIngestor

ingestor = PolarsIngestor('stocks_daily', Path('data/lake'), config)
result = ingestor.ingest_date('2024-01-02', csv_data)
print(f"Processed {result['records']} records in {result['elapsed_time']:.2f}s")
```

#### `ingest_batch(dates: List[str], data_map: Dict[str, BytesIO], symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]`
Process multiple dates sequentially.

#### `ingest_lazy(date: str, data: BytesIO, symbols: Optional[List[str]] = None) -> Dict[str, Any]`
Most memory-efficient mode using LazyFrame.

**Performance:**
- 5-10x faster than pandas
- Automatic parallelization
- Native Arrow integration
- Lazy evaluation

**When to Use:**
- All systems where performance is critical
- Batch processing
- Large datasets

**Recommended for:** Primary ingestor for most use cases

---

## Streaming Ingestor

**Module**: `src.ingest.streaming_ingestor`

Memory-efficient chunked processing for systems with <32GB RAM.

### Class: `StreamingIngestor(BaseIngestor)`

```python
StreamingIngestor(
    data_type: str,
    output_root: Path,
    config: Dict[str, Any],
    chunk_size: int = 100000
)
```

**Parameters:**
- `data_type`: Data type
- `output_root`: Output directory
- `config`: Configuration
- `chunk_size`: Rows per chunk (default: 100K)

**Key Methods:**

#### `ingest_date(date: str, data: BytesIO, symbols: Optional[List[str]] = None) -> Dict[str, Any]`
Process single date with streaming.

```python
from src.ingest import StreamingIngestor

ingestor = StreamingIngestor('stocks_daily', Path('data/lake'), config, chunk_size=50000)
result = ingestor.ingest_date('2024-01-02', csv_data)
```

#### `ingest_batch(dates: List[str], data_map: Dict[str, BytesIO], symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]`
Process multiple dates sequentially.

**Memory Management:**
- Processes data in chunks (default: 100K rows)
- Garbage collection after each chunk
- Incremental Parquet writing
- 50-70% memory reduction through dtype optimization

**When to Use:**
- 24GB Macs
- Systems with <32GB RAM
- Large files that don't fit in memory
- Memory-constrained environments

**Trade-off:** Slower than Polars but uses minimal memory

---

## Choosing an Ingestor

| Ingestor | RAM Requirement | Speed | Best For |
|----------|----------------|-------|----------|
| **PolarsIngestor** | Any | 5-10x faster | Production, performance-critical |
| **StreamingIngestor** | <32GB | Slower | Memory-constrained systems |

**Recommendation:** Use `PolarsIngestor` unless memory-constrained.

---

## Common Usage Pattern

```python
from src.core import ConfigLoader
from src.ingest import PolarsIngestor
from src.download import AsyncS3Downloader, S3Catalog
from pathlib import Path

# Setup
config = ConfigLoader()
catalog = S3Catalog()
downloader = AsyncS3Downloader(config.get_credentials('polygon'))
ingestor = PolarsIngestor('stocks_daily', Path('data/lake'), config)

# Download and ingest
date = '2024-01-02'
key = catalog.get_stocks_daily_key(date)
data = await downloader.download_one('flatfiles', key)
result = ingestor.ingest_date(date, data)

print(f"Ingested {result['records']} records")
```

---

## Output Structure

Parquet files are written with this structure:

```
data/lake/
└── stocks_daily/
    └── year=2024/
        └── month=01/
            └── date=2024-01-02.parquet
```

**Partitioning:**
- Daily data: Partitioned by `year`, `month`
- Minute data: Partitioned by `year`, `month`, `symbol`

**Schema:** See `src.storage.schemas` for complete schema definitions
