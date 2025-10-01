# Features Module (`src.features`)

Feature engineering and technical indicators calculation.

## Feature Engineer

**Module**: `src.features.feature_engineer`

Compute features with adaptive memory usage using DuckDB.

### Class: `FeatureEngineer`

```python
FeatureEngineer(
    parquet_root: Path,
    enriched_root: Path,
    config: ConfigLoader,
    metadata_root: Optional[Path] = None
)
```

**Parameters:**
- `parquet_root`: Input directory (raw Parquet data)
- `enriched_root`: Output directory (enriched Parquet data)
- `config`: Configuration loader
- `metadata_root`: Optional metadata directory

**Key Methods:**

#### `enrich_date_range(data_type: str, start_date: str, end_date: str, incremental: bool = True) -> Dict`
Enrich date range with features.

```python
from src.features import FeatureEngineer
from src.core import ConfigLoader
from pathlib import Path

config = ConfigLoader()
engineer = FeatureEngineer(
    parquet_root=Path('data/lake'),
    enriched_root=Path('data/enriched'),
    config=config
)

result = engineer.enrich_date_range(
    data_type='stocks_daily',
    start_date='2024-01-01',
    end_date='2024-01-31',
    incremental=True
)
print(f"Enriched {result['records_processed']} records")
```

#### `get_enrichment_status(data_type: str) -> Dict`
Get enrichment status.

Returns:
```python
{
    'completed': List[str],  # Dates completed
    'pending': List[str],    # Dates pending
    'failed': List[str]      # Dates failed
}
```

#### `close()`
Close DuckDB connection.

**Context Manager:**
```python
with FeatureEngineer(parquet_root, enriched_root, config) as engineer:
    result = engineer.enrich_date_range('stocks_daily', '2024-01-01', '2024-01-31')
```

**Processing Modes:**
- Streaming: <32GB RAM (one symbol at a time)
- Batch: 32-64GB RAM (multiple symbols)
- Parallel: >64GB RAM (all symbols in parallel, future)

---

## Feature Definitions

**Module**: `src.features.definitions`

Define features to calculate for each data type.

**Functions:**

#### `get_feature_definitions(data_type: str) -> Dict[str, str]`
Get feature definitions (name -> SQL expression).

```python
from src.features.definitions import get_feature_definitions

features = get_feature_definitions('stocks_daily')
for name, expr in features.items():
    print(f"{name}: {expr}")
```

#### `get_feature_list(data_type: str) -> List[str]`
Get list of feature names.

```python
features = get_feature_list('stocks_daily')
print(f"Total features: {len(features)}")
```

#### `build_feature_sql(data_type: str, base_view: str = 'raw_data') -> str`
Build complete SQL query with all features.

**Feature Sets:**

**Stock Daily Features:**
- Returns: `return_1d`, `return_5d`, `return_20d`
- Alpha: `alpha_daily` (daily return - market return)
- Price features: `price_range`, `daily_return_pct`
- Volume features: `vwap`, `avg_trade_size`, `volume_change_pct`
- Volatility: `volatility_20d`

**Stock Minute Features:**
- Returns: `minute_return`, `minute_return_pct`
- Intraday: `intraday_vwap`, `intraday_high`, `intraday_low`
- Volume: `volume_change`, `spread`, `typical_price`

**Options Daily Features:**
- Ticker parsing: `underlying`, `expiration_date`, `contract_type`, `strike_price`
- Moneyness: `moneyness` (strike/spot ratio)
- Returns: `return_1d`
- Volume: `volume_change_pct`

**Options Minute Features:**
- Ticker parsing: `underlying`, `expiration_date`, `contract_type`, `strike_price`
- Returns: `minute_return`
- Volume: `volume_change`

---

## Simple Definitions

**Module**: `src.features.definitions_simple`

Simplified features for testing (no nested window functions).

**Functions:**

#### `build_simple_stock_daily_sql(base_view: str = 'raw_data') -> str`
Build simple SQL for stock daily features.

**Features:**
- `return_1d`: Daily return
- `alpha_daily`: Price change vs previous day
- `price_range`: (high - low) / close
- `daily_return_pct`: Daily return percentage
- `vwap`: Volume-weighted average price

**When to Use:**
- Testing and development
- DuckDB compatibility issues
- Simpler feature requirements

---

## Options Parser

**Module**: `src.features.options_parser`

Parse Polygon.io options ticker format.

### Class: `OptionsTickerParser`

**Class Methods:**

#### `parse(ticker: str) -> Optional[Dict[str, Any]]`
Parse options ticker.

```python
from src.features import OptionsTickerParser

result = OptionsTickerParser.parse('O:SPY230327P00390000')
print(result)
# {
#     'underlying': 'SPY',
#     'expiration_date': datetime.date(2023, 3, 27),
#     'contract_type': 'P',
#     'strike_price': 390.0
# }
```

#### `parse_batch(tickers: List[str]) -> Dict[str, Dict]`
Parse multiple tickers efficiently.

```python
tickers = ['O:SPY230327P00390000', 'O:AAPL230317C00150000']
results = OptionsTickerParser.parse_batch(tickers)
for ticker, parsed in results.items():
    if parsed:
        print(f"{ticker}: {parsed['underlying']} {parsed['strike_price']}")
```

#### `is_valid_ticker(ticker: str) -> bool`
Check if ticker matches options format.

```python
if OptionsTickerParser.is_valid_ticker('O:SPY230327P00390000'):
    print("Valid options ticker")
```

#### `extract_underlying(ticker: str) -> Optional[str]`
Quick extraction of underlying symbol.

```python
underlying = OptionsTickerParser.extract_underlying('O:SPY230327P00390000')
# Returns: 'SPY'
```

#### `extract_expiration(ticker: str) -> Optional[date]`
Quick extraction of expiration date.

**Ticker Format:**
```
O:UNDERLYING[YY]MMDD[C/P]STRIKE

Examples:
O:SPY230327P00390000  -> SPY put, exp 2023-03-27, strike $390
O:AAPL230317C00150000 -> AAPL call, exp 2023-03-17, strike $150
```

**Components:**
- Prefix: `O:`
- Underlying: Stock symbol
- Expiration: YYMMDD (6 digits)
- Type: `C` (call) or `P` (put)
- Strike: 8 digits (dollars * 1000)

---

## Custom Features

To add custom features, edit `src/features/definitions.py`:

```python
STOCK_DAILY_FEATURES = {
    'return_1d': '(close / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) - 1',
    
    # Add your custom feature:
    'my_custom_feature': 'YOUR SQL EXPRESSION HERE',
}
```

Features are computed using DuckDB SQL, supporting:
- Window functions: `LAG`, `LEAD`, `ROW_NUMBER`, etc.
- Aggregations: `SUM`, `AVG`, `MAX`, `MIN`, etc.
- Date functions: `DATE_DIFF`, `EXTRACT`, etc.
- Math functions: `LOG`, `SQRT`, `POWER`, etc.

**Best Practices:**
1. Use memory-efficient types (CAST to FLOAT if needed)
2. Partition by symbol for stock-level features
3. Order by date for time-series features
4. Handle NULLs explicitly
5. Test with `definitions_simple` first
