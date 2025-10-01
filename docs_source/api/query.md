# Query Module (`src.query`)

Query enriched Parquet data using DuckDB with caching.

## Query Engine

**Module**: `src.query.query_engine`

High-performance query engine with DuckDB backend.

### Class: `QueryEngine`

```python
QueryEngine(
    data_root: Path,
    config: ConfigLoader,
    enable_cache: bool = True
)
```

**Parameters:**
- `data_root`: Root directory for enriched Parquet data
- `config`: Configuration loader
- `enable_cache`: Enable query result caching (default: True)

**Key Methods:**

#### `query_parquet(data_type: str, symbols: List[str], fields: List[str], start_date: str, end_date: str, use_cache: bool = True)`
Query Parquet files.

```python
from src.query import QueryEngine
from src.core import ConfigLoader
from pathlib import Path

config = ConfigLoader()
engine = QueryEngine(
    data_root=Path('data/enriched'),
    config=config,
    enable_cache=True
)

result = engine.query_parquet(
    data_type='stocks_daily',
    symbols=['AAPL', 'MSFT', 'GOOGL'],
    fields=['date', 'close', 'volume', 'return_1d', 'alpha_daily'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    use_cache=True
)

print(f"Query returned {len(result)} rows")
print(result.head())
```

**Return Type:** `pandas.DataFrame` with requested fields

#### `get_cache_stats()`
Get cache statistics.

```python
stats = engine.get_cache_stats()
print(f"Cache hits: {stats['hits']}")
print(f"Cache misses: {stats['misses']}")
print(f"Hit rate: {stats['hit_rate']:.1%}")
print(f"Cache size: {stats['size_mb']:.2f} MB")
```

#### `clear_cache()`
Clear query cache.

```python
engine.clear_cache()
print("Cache cleared")
```

#### `close()`
Close DuckDB connection.

**Context Manager:**
```python
with QueryEngine(data_root, config) as engine:
    result = engine.query_parquet(
        'stocks_daily',
        ['AAPL'],
        ['close'],
        '2024-01-01',
        '2024-01-31'
    )
```

**Performance Features:**
- DuckDB for fast analytical queries
- Predicate pushdown (filters applied during read)
- Column selection (read only requested fields)
- Query result caching (LRU eviction)
- Lazy evaluation

**Query Optimizations:**
- Partition pruning: Only reads relevant partitions
- Column pruning: Only reads requested columns
- Filter pushdown: Filters applied at storage level
- Caching: Repeated queries served from cache

---

## Query Cache

**Module**: `src.query.query_cache`

LRU cache for query results.

### Class: `QueryCache`

```python
QueryCache(
    cache_root: Path,
    max_size_gb: float = 2.0
)
```

**Parameters:**
- `cache_root`: Cache directory
- `max_size_gb`: Maximum cache size in GB (default: 2.0)

**Key Methods:**

#### `make_key(**kwargs) -> str`
Create cache key from query parameters.

```python
cache = QueryCache(Path('data/cache'))
key = cache.make_key(
    data_type='stocks_daily',
    symbols=['AAPL', 'MSFT'],
    fields=['close', 'volume'],
    start_date='2024-01-01',
    end_date='2024-01-31'
)
print(f"Cache key: {key}")  # MD5 hash
```

#### `get(key: str) -> Optional[pd.DataFrame]`
Get cached result.

```python
result = cache.get(key)
if result is not None:
    print("Cache hit!")
else:
    print("Cache miss")
```

#### `put(key: str, data: pd.DataFrame)`
Cache result.

```python
cache.put(key, dataframe)
print("Result cached")
```

#### `get_stats() -> dict`
Get cache statistics.

```python
stats = cache.get_stats()
# Returns: {
#     'hits': int,
#     'misses': int,
#     'hit_rate': float,
#     'entries': int,
#     'size_mb': float
# }
```

#### `clear()`
Clear all cached data.

**Cache Features:**
- Pickle-based serialization
- LRU eviction when size limit reached
- MD5 hash keys for efficient lookup
- Statistics tracking (hits, misses, size)

**Cache Management:**
- Old entries evicted when size exceeds limit
- Cache survives across sessions
- Thread-safe operations

---

## Example Queries

### Basic Query

```python
from src.query import QueryEngine
from src.core import ConfigLoader
from pathlib import Path

config = ConfigLoader()
engine = QueryEngine(Path('data/enriched'), config)

# Get close prices for AAPL
df = engine.query_parquet(
    data_type='stocks_daily',
    symbols=['AAPL'],
    fields=['date', 'close'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

print(df.head())
```

### Multiple Symbols and Features

```python
# Get multiple features for multiple symbols
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
fields = ['date', 'close', 'volume', 'return_1d', 'alpha_daily', 'volatility_20d']

df = engine.query_parquet(
    data_type='stocks_daily',
    symbols=symbols,
    fields=fields,
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Pivot for analysis
pivot = df.pivot(index='date', columns='symbol', values='return_1d')
print(pivot.corr())  # Correlation matrix
```

### Filtering After Query

```python
# Query returns DataFrame, can filter with pandas
df = engine.query_parquet(
    data_type='stocks_daily',
    symbols=['AAPL', 'MSFT'],
    fields=['date', 'close', 'volume'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Filter high-volume days
high_volume = df[df['volume'] > df['volume'].quantile(0.9)]
print(f"High-volume days: {len(high_volume)}")
```

### Backtesting Example

```python
# Get historical data for backtesting
symbols = ['AAPL', 'MSFT', 'GOOGL']
fields = ['date', 'close', 'return_1d', 'alpha_daily', 'volatility_20d']

df = engine.query_parquet(
    data_type='stocks_daily',
    symbols=symbols,
    fields=fields,
    start_date='2020-01-01',
    end_date='2024-12-31'
)

# Simple strategy: buy if alpha > 0
df['signal'] = (df['alpha_daily'] > 0).astype(int)
df['strategy_return'] = df['signal'].shift(1) * df['return_1d']

# Calculate cumulative returns
df['cum_return'] = (1 + df['strategy_return']).cumprod()
print(f"Strategy return: {df['cum_return'].iloc[-1] - 1:.2%}")
```

---

## Performance Tips

1. **Use Caching**: Enable caching for repeated queries
   ```python
   engine = QueryEngine(data_root, config, enable_cache=True)
   ```

2. **Select Only Needed Columns**: DuckDB only reads requested columns
   ```python
   # Good: Only reads 'close'
   df = engine.query_parquet(..., fields=['close'], ...)
   
   # Bad: Reads all columns
   df = engine.query_parquet(..., fields=['*'], ...)
   ```

3. **Use Appropriate Date Ranges**: Smaller ranges are faster
   ```python
   # Good: Query specific month
   df = engine.query_parquet(..., start_date='2024-01-01', end_date='2024-01-31')
   
   # Slower: Query entire year
   df = engine.query_parquet(..., start_date='2024-01-01', end_date='2024-12-31')
   ```

4. **Filter Symbols**: Query only needed symbols
   ```python
   # Good: Query specific symbols
   df = engine.query_parquet(..., symbols=['AAPL', 'MSFT'], ...)
   
   # Slower: Query all symbols
   df = engine.query_parquet(..., symbols=all_symbols, ...)
   ```

5. **Clear Cache Periodically**: Free up disk space
   ```python
   engine.clear_cache()
   ```
