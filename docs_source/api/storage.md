# Storage Module (`src.storage`)

Parquet management, metadata tracking, and schema definitions.

## Parquet Manager

**Module**: `src.storage.parquet_manager`

Manage Parquet datasets with partitioning and querying.

### Class: `ParquetManager`

```python
ParquetManager(root_path: Path, data_type: str)
```

**Parameters:**
- `root_path`: Root directory for Parquet data
- `data_type`: Data type (`'stocks_daily'`, etc.)

**Key Methods:**

#### `write_partition(table: pa.Table, partition_values: Dict[str, Any])`
Write data to specific partition.

```python
from src.storage import ParquetManager
import pyarrow as pa

manager = ParquetManager(Path('data/lake'), 'stocks_daily')
manager.write_partition(
    table=pa_table,
    partition_values={'year': 2024, 'month': 1}
)
```

#### `read_partition(partition_values: Dict[str, Any], columns: Optional[List[str]] = None) -> pa.Table`
Read from specific partition.

```python
table = manager.read_partition(
    partition_values={'year': 2024, 'month': 1},
    columns=['symbol', 'close', 'volume']
)
```

#### `read_date_range(start_date: str, end_date: str, symbols: Optional[List[str]] = None, columns: Optional[List[str]] = None) -> pa.Table`
Read date range with optional symbol filtering.

```python
table = manager.read_date_range(
    start_date='2024-01-01',
    end_date='2024-01-31',
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    columns=['symbol', 'date', 'close']
)
```

#### `query(filters: Optional[List[Tuple]] = None, columns: Optional[List[str]] = None, limit: Optional[int] = None) -> pa.Table`
Query with flexible filters.

```python
table = manager.query(
    filters=[('symbol', 'in', ['AAPL', 'MSFT'])],
    columns=['symbol', 'date', 'close', 'volume'],
    limit=1000
)
```

#### `list_partitions() -> List[Dict[str, Any]]`
List all partitions with metadata.

#### `get_statistics() -> Dict[str, Any]`
Get dataset statistics.

Returns:
```python
{
    'total_partitions': int,
    'total_size_mb': float,
    'date_range': {'start': str, 'end': str},
    'symbols': List[str]
}
```

#### `delete_partition(partition_values: Dict[str, Any])`
Delete specific partition.

---

## Metadata Manager

**Module**: `src.storage.metadata_manager`

Track ingestion status and dataset metadata.

### Class: `MetadataManager`

```python
MetadataManager(metadata_root: Path)
```

**Key Methods:**

#### `record_ingestion(data_type: str, date: str, status: str, statistics: Dict[str, Any], symbol: Optional[str] = None, error: Optional[str] = None)`
Record ingestion result.

```python
from src.storage import MetadataManager

metadata = MetadataManager(Path('data/metadata'))
metadata.record_ingestion(
    data_type='stocks_daily',
    date='2024-01-02',
    status='success',
    statistics={'records': 15000, 'size_mb': 2.5}
)
```

#### `get_ingestion_status(data_type: str, date: str, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]`
Get ingestion status for specific date.

```python
status = metadata.get_ingestion_status('stocks_daily', '2024-01-02')
if status and status['status'] == 'success':
    print(f"Already ingested {status['statistics']['records']} records")
```

#### `list_ingestions(data_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]`
List ingestion records with filtering.

```python
# Get all successful ingestions in January 2024
ingestions = metadata.list_ingestions(
    data_type='stocks_daily',
    start_date='2024-01-01',
    end_date='2024-01-31',
    status='success'
)
```

#### `get_watermark(data_type: str, symbol: Optional[str] = None) -> Optional[str]`
Get latest successfully ingested date (for incremental processing).

```python
watermark = metadata.get_watermark('stocks_daily')
print(f"Latest data: {watermark}")
```

#### `set_watermark(data_type: str, date: str, symbol: Optional[str] = None)`
Set watermark for incremental processing.

#### `get_missing_dates(data_type: str, start_date: str, end_date: str, expected_dates: List[str]) -> List[str]`
Get dates not successfully ingested.

```python
from src.download import S3Catalog

catalog = S3Catalog()
expected = catalog.get_business_days('2024-01-01', '2024-01-31')
missing = metadata.get_missing_dates('stocks_daily', '2024-01-01', '2024-01-31', expected)
print(f"Missing {len(missing)} days")
```

#### `get_statistics_summary(data_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]`
Get aggregated statistics.

#### `is_symbol_converted(symbol: str, data_type: str) -> bool`
Check if symbol converted to binary format.

#### `mark_symbol_converted(symbol: str, data_type: str)`
Mark symbol as converted to binary.

#### `clear_conversion_status(data_type: Optional[str] = None)`
Clear binary conversion status.

---

## Schemas

**Module**: `src.storage.schemas`

PyArrow schema definitions for all data types.

**Functions:**

#### `get_stocks_daily_schema() -> pa.Schema`
Get stock daily aggregates schema.

#### `get_stocks_minute_schema() -> pa.Schema`
Get stock minute aggregates schema.

#### `get_options_daily_schema() -> pa.Schema`
Get options daily aggregates schema.

#### `get_options_minute_schema() -> pa.Schema`
Get options minute aggregates schema.

#### `get_schema(data_type: str) -> pa.Schema`
Get schema for data type.

```python
from src.storage.schemas import get_schema

schema = get_schema('stocks_daily')
print(f"Schema has {len(schema)} fields")
```

#### `get_raw_schema(data_type: str) -> pa.Schema`
Get schema for raw data (before feature engineering).

#### `print_schema_info(data_type: str)`
Print human-readable schema information.

```python
from src.storage.schemas import print_schema_info

print_schema_info('stocks_daily')
# Prints field names, types, and descriptions
```

**Schema Features:**
- Memory-optimized types (float32 instead of float64)
- Dictionary encoding for symbols
- Partition columns (year, month, symbol/underlying)
- Data columns from S3
- Enriched feature columns (added during feature engineering)

**Example Schema (stocks_daily):**
```
symbol: dictionary<values=string, indices=int32>
date: date32[day]
open: float32
high: float32
low: float32
close: float32
volume: float64
transactions: int64
year: int32
month: int32
# Enriched features (nullable):
return_1d: float32
return_5d: float32
return_20d: float32
alpha_daily: float32
...
```
