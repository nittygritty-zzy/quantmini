# Download Module (`src.download`)

S3 downloaders (async/sync) and catalog management for Polygon.io flat files.

## AsyncS3Downloader

**Module**: `src.download.async_downloader`

High-performance async S3 downloads using aioboto3.

### Class: `AsyncS3Downloader`

```python
AsyncS3Downloader(
    credentials: Dict[str, str],
    endpoint_url: str = 'https://files.polygon.io',
    max_retries: int = 5,
    timeout: int = 60,
    max_pool_connections: int = 50,
    max_concurrent: int = 8
)
```

**Parameters:**
- `credentials`: Dict with `'access_key_id'` and `'secret_access_key'`
- `endpoint_url`: S3 endpoint URL
- `max_retries`: Maximum retry attempts
- `timeout`: Request timeout in seconds
- `max_pool_connections`: Connection pool size
- `max_concurrent`: Maximum concurrent downloads

**Key Methods:**

#### `download_one(bucket: str, key: str, decompress: bool = True) -> BytesIO`
Download single file (async).

```python
downloader = AsyncS3Downloader(credentials)
data = await downloader.download_one(
    bucket='flatfiles',
    key='us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz',
    decompress=True
)
```

#### `download_batch(bucket: str, keys: List[str], decompress: bool = True) -> List[Optional[BytesIO]]`
Download multiple files in parallel.

```python
keys = ['file1.csv.gz', 'file2.csv.gz', 'file3.csv.gz']
results = await downloader.download_batch('flatfiles', keys)
```

#### `download_to_file(bucket: str, key: str, local_path: Path, decompress: bool = True)`
Download and save to disk (async).

#### `list_objects(bucket: str, prefix: str, max_keys: int = 1000) -> List[str]`
List S3 objects with prefix.

```python
keys = await downloader.list_objects(
    bucket='flatfiles',
    prefix='us_stocks_sip/day_aggs_v1/2024/01/'
)
```

#### `get_statistics() -> Dict[str, Any]`
Get download statistics.

Returns:
```python
{
    'total_downloads': int,
    'successful_downloads': int,
    'failed_downloads': int,
    'total_retries': int,
    'success_rate': float
}
```

#### `reset_statistics()`
Reset statistics counters.

**Performance:**
- 3-5x faster than sync downloader
- Parallel processing with connection pooling
- Exponential backoff retry logic
- Automatic decompression of `.gz` files

---

## SyncS3Downloader

**Module**: `src.download.sync_downloader`

Synchronous S3 downloads using boto3.

### Class: `SyncS3Downloader`

```python
SyncS3Downloader(
    credentials: Dict[str, str],
    endpoint_url: str = 'https://files.polygon.io',
    max_retries: int = 5,
    timeout: int = 60,
    max_pool_connections: int = 10
)
```

**Parameters:** Same as AsyncS3Downloader (except no `max_concurrent`)

**Key Methods:**

#### `download(bucket: str, key: str, decompress: bool = True) -> BytesIO`
Download single file (synchronous).

```python
downloader = SyncS3Downloader(credentials)
data = downloader.download('flatfiles', 'path/to/file.csv.gz')
```

#### `download_to_file(bucket: str, key: str, local_path: Path, decompress: bool = True)`
Download and save to disk.

#### `list_objects(bucket: str, prefix: str, max_keys: int = 1000) -> list`
List S3 objects.

#### `check_exists(bucket: str, key: str) -> bool`
Check if object exists in S3.

```python
if downloader.check_exists('flatfiles', 'path/to/file.csv.gz'):
    data = downloader.download('flatfiles', 'path/to/file.csv.gz')
```

#### `get_statistics() -> Dict[str, int]`
Get download statistics.

#### `reset_statistics()`
Reset statistics.

**When to Use:**
- Simple scripts without async support
- Sequential processing
- Debugging

**Recommendation:** Use AsyncS3Downloader for production (3-5x faster)

---

## S3Catalog

**Module**: `src.download.s3_catalog`

Manage S3 file paths for Polygon.io flat files.

### Class: `S3Catalog`

```python
S3Catalog(bucket: str = 'flatfiles')
```

**Key Methods:**

#### `get_stocks_daily_key(date: str) -> str`
Get S3 key for stock daily aggregates.

```python
catalog = S3Catalog()
key = catalog.get_stocks_daily_key('2024-01-02')
# Returns: 'us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz'
```

#### `get_stocks_minute_key(date: str) -> str`
Get S3 key for stock minute aggregates.

#### `get_options_daily_key(date: str) -> str`
Get S3 key for options daily aggregates.

#### `get_options_minute_key(date: str) -> str`
Get S3 key for options minute aggregates.

#### `get_date_range_keys(data_type: str, start_date: str, end_date: str, symbols: Optional[List[str]] = None) -> List[str]`
Get S3 keys for date range.

```python
keys = catalog.get_date_range_keys(
    data_type='stocks_daily',
    start_date='2024-01-01',
    end_date='2024-01-31'
)
print(f"Found {len(keys)} trading days in January 2024")
```

**Supported Data Types:**
- `stocks_daily`: Stock daily aggregates
- `stocks_minute`: Stock minute aggregates
- `options_daily`: Options daily aggregates
- `options_minute`: Options minute aggregates

#### `parse_key_metadata(key: str) -> Dict[str, str]`
Parse metadata from S3 key.

```python
metadata = catalog.parse_key_metadata(
    'us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz'
)
# Returns: {'data_type': 'stocks_daily', 'date': '2024-01-02', 'year': '2024', 'month': '01'}
```

#### `validate_key(key: str) -> bool`
Validate if key matches expected pattern.

#### `get_summary(keys: List[str]) -> Dict[str, int]`
Get summary statistics for keys.

**Static Methods:**

#### `get_business_days(start_date: str, end_date: str) -> List[str]`
Get US stock market business days between dates.

```python
days = S3Catalog.get_business_days('2024-01-01', '2024-01-31')
print(f"Trading days: {len(days)}")
```

#### `get_missing_dates(existing_dates: List[str], start_date: str, end_date: str) -> List[str]`
Get missing business days.

```python
existing = ['2024-01-02', '2024-01-03']
missing = S3Catalog.get_missing_dates(existing, '2024-01-01', '2024-01-05')
# Returns dates that are business days but not in existing
```

**S3 Path Patterns:**
- Stocks Daily: `us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz`
- Stocks Minute: `us_stocks_sip/minute_aggs_v1/{year}/{month}/{date}.csv.gz`
- Options Daily: `us_options/day_aggs_v1/{year}/{month}/{date}.csv.gz`
- Options Minute: `us_options/minute_aggs_v1/{year}/{month}/{date}.csv.gz`
