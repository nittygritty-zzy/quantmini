# Transform Module (`src.transform`)

Convert enriched Parquet to Qlib binary format with validation.

## Qlib Binary Writer

**Module**: `src.transform.qlib_binary_writer`

Convert enriched Parquet to Qlib binary format for ML training.

### Class: `QlibBinaryWriter`

```python
QlibBinaryWriter(
    enriched_root: Path,
    qlib_root: Path,
    config: ConfigLoader
)
```

**Parameters:**
- `enriched_root`: Input directory (enriched Parquet data)
- `qlib_root`: Output directory (Qlib binary format)
- `config`: Configuration loader

**Key Methods:**

#### `convert_data_type(data_type: str, start_date: str, end_date: str, incremental: bool = True, metadata_manager: Optional[object] = None) -> Dict[str, Any]`
Convert entire data type to Qlib binary.

```python
from src.transform import QlibBinaryWriter
from src.core import ConfigLoader
from pathlib import Path

config = ConfigLoader()
writer = QlibBinaryWriter(
    enriched_root=Path('data/enriched'),
    qlib_root=Path('data/binary'),
    config=config
)

result = writer.convert_data_type(
    data_type='stocks_daily',
    start_date='2024-01-01',
    end_date='2024-01-31',
    incremental=True
)

print(f"Converted {result['symbols_converted']} symbols")
print(f"Total features: {result['features_written']}")
```

#### `close()`
Close DuckDB connection.

**Context Manager:**
```python
with QlibBinaryWriter(enriched_root, qlib_root, config) as writer:
    result = writer.convert_data_type('stocks_daily', '2024-01-01', '2024-01-31')
```

**Binary Format:**

Qlib uses a custom binary format for fast data access:

```
qlib_root/
├── instruments/
│   └── all.txt              # Tab-separated: symbol start_date end_date
├── calendars/
│   └── day.txt              # One date per line: YYYY-MM-DD
└── features/
    └── {symbol}/
        ├── close.bin        # Binary feature data
        ├── volume.bin
        ├── return_1d.bin
        └── ...
```

**Binary File Format:**
```
4 bytes: count (uint32, little-endian)
N * 4 bytes: float32 values (little-endian)
```

**Processing Modes:**
- Streaming: One symbol at a time (memory-efficient)
- Batch: Multiple symbols (future)
- Parallel: All symbols in parallel (future)

**Critical Fixes Applied:**
See `docs/changelog/QLIB_BINARY_WRITER_UPDATES.md` for details on 6 critical fixes:
1. Filter NULL symbols in SQL query
2. Tab-separated instruments file
3. Create `.qlib/dataset_info.json` with frequency
4. Clean macOS metadata files (`._*`)
5. Proper date ranges in instruments file
6. Validated binary format

---

## Qlib Binary Validator

**Module**: `src.transform.qlib_binary_validator`

Validate Qlib binary format conversions.

### Class: `QlibBinaryValidator`

```python
QlibBinaryValidator(qlib_root: Path)
```

**Key Methods:**

#### `validate_conversion(data_type: str) -> Dict[str, Any]`
Run all validation checks.

```python
from src.transform import QlibBinaryValidator
from pathlib import Path

validator = QlibBinaryValidator(Path('data/binary'))
results = validator.validate_conversion('stocks_daily')

if results['all_passed']:
    print("✓ All validation checks passed")
else:
    print("✗ Validation failed:")
    for check, passed in results['checks'].items():
        print(f"  {check}: {'✓' if passed else '✗'}")
```

**Validation Checks:**
1. **Instruments file**: Exists, correct format, valid date ranges
2. **Calendar file**: Exists, correct format, business days only
3. **Binary files**: Exist for each symbol, correct format
4. **File structure**: Proper directory structure
5. **Metadata**: `.qlib/dataset_info.json` exists and valid

#### `read_binary_feature(data_type: str, symbol: str, feature: str) -> np.ndarray`
Read binary feature for testing.

```python
feature_data = validator.read_binary_feature('stocks_daily', 'AAPL', 'close')
print(f"AAPL close prices: shape={feature_data.shape}")
```

#### `get_feature_list(data_type: str, symbol: str) -> List[str]`
Get list of features for symbol.

```python
features = validator.get_feature_list('stocks_daily', 'AAPL')
print(f"AAPL features: {', '.join(features)}")
```

#### `compare_with_parquet(data_type: str, symbol: str, feature: str, parquet_df, tolerance: float = 1e-6) -> Dict[str, Any]`
Compare binary with original Parquet (roundtrip test).

```python
import pandas as pd

# Load original data
df = pd.read_parquet('data/enriched/stocks_daily/AAPL.parquet')

# Compare
comparison = validator.compare_with_parquet(
    data_type='stocks_daily',
    symbol='AAPL',
    feature='close',
    parquet_df=df,
    tolerance=1e-6
)

if comparison['match']:
    print(f"✓ Binary matches Parquet within tolerance")
else:
    print(f"✗ Mismatch: {comparison['max_diff']} max difference")
```

---

## Using Qlib Binary Data

After conversion, initialize Qlib with the binary data:

```python
import qlib
from qlib.data import D

# Initialize Qlib
qlib.init(
    provider_uri='data/binary/stocks_daily',
    region='us'
)

# Query data
symbols = ['AAPL', 'MSFT', 'GOOGL']
fields = ['$close', '$volume', '$return_1d', '$alpha_daily']

data = D.features(
    symbols,
    fields,
    start_time='2024-01-01',
    end_time='2024-01-31'
)

print(data.head())
```

**Field Naming:**
- Prefix with `$` when querying: `$close`, `$volume`, `$return_1d`
- Binary files have no prefix: `close.bin`, `volume.bin`, `return_1d.bin`

---

## Incremental Updates

The binary writer supports incremental updates:

```python
from src.storage import MetadataManager

metadata = MetadataManager(Path('data/metadata'))

# First conversion
writer.convert_data_type(
    'stocks_daily',
    start_date='2024-01-01',
    end_date='2024-01-31',
    incremental=True,
    metadata_manager=metadata
)

# Later, add new data
writer.convert_data_type(
    'stocks_daily',
    start_date='2024-02-01',
    end_date='2024-02-29',
    incremental=True,
    metadata_manager=metadata
)
# Only converts symbols not already converted
```

**Benefits:**
- Skip already-converted symbols
- Faster updates
- Avoid reprocessing

---

## Troubleshooting

**Issue**: `qlib.init()` fails with "Provider not found"

**Solution**: Check binary format structure:
```bash
ls data/binary/stocks_daily/
# Should see: instruments/, calendars/, features/
```

**Issue**: Validation fails on instruments file

**Solution**: Check tab-separated format:
```bash
head data/binary/stocks_daily/instruments/all.txt
# Format: SYMBOL\tSTART_DATE\tEND_DATE
```

**Issue**: Binary files have wrong size

**Solution**: Verify count matches calendar:
```python
import struct

# Read first 4 bytes (count)
with open('data/binary/stocks_daily/features/AAPL/close.bin', 'rb') as f:
    count = struct.unpack('<I', f.read(4))[0]
    print(f"Binary has {count} values")

# Compare with calendar
with open('data/binary/stocks_daily/calendars/day.txt') as f:
    calendar_days = len(f.readlines())
    print(f"Calendar has {calendar_days} days")
```

See `docs/changelog/QLIB_BINARY_WRITER_UPDATES.md` for more troubleshooting.
