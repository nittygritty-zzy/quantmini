# Qlib Workflow Example - Setup & Fixes

## Overview

This directory contains a complete example of using Qlib with our converted US stocks data. The workflow demonstrates:
- Data loading from Qlib binary format
- Feature engineering with Alpha158 (158 technical indicators)
- Model training with LightGBM
- Predictions and signal generation
- Experiment tracking

## Files

- `qlib_workflow_config.yaml` - Workflow configuration adapted for US stocks
- `run_qlib_workflow.py` - Complete workflow script
- `QLIB_WORKFLOW_README.md` - This file

## Data Requirements

### Required Fixes for Qlib Binary Data

When converting data to Qlib format using `QlibBinaryWriter`, the following issues were discovered and must be fixed:

#### 1. **Frequency Metadata File** ⚠️ **CRITICAL**

**Problem**: Qlib requires a `.qlib/dataset_info.json` file specifying the data frequency.

**Solution**: Create this file in the Qlib data directory (gold layer):

```bash
mkdir -p /path/to/gold/qlib/stocks_daily/.qlib
cat > /path/to/gold/qlib/stocks_daily/.qlib/dataset_info.json << 'EOF'
{
  "freq": ["day"]
}
EOF
```

**Code Fix Needed**: Update `QlibBinaryWriter.convert_data_type()` to automatically create this file.

#### 2. **Instruments File Format** ⚠️ **CRITICAL**

**Problem**: Qlib expects TAB-separated values, not space-separated. Format must be: `SYMBOL\tSTART_DATE\tEND_DATE`

**Current Output** (WRONG):
```
AAPL 2025-08-01 2025-09-29
```

**Required Format** (CORRECT):
```
AAPL	2025-08-01	2025-09-29
```

**Code Fix Needed**: Update `QlibBinaryWriter._generate_instruments()` around line 185-187:

```python
# Before:
f.write(f"{symbol}\n")

# After:
f.write(f"{symbol}\t{start_date}\t{end_date}\n")
```

####  3. **Null Symbol Handling** ⚠️ **CRITICAL**

**Problem**: Null/NaN symbols in the source data create:
- `nan` entry in instruments file (parsed as float, not string)
- `nan/` directory in features folder

**Solution**: Filter out null symbols before writing.

**Code Fix Needed**: Update `QlibBinaryWriter._generate_instruments()`:

```python
# Query unique symbols/tickers
if data_type.startswith('stocks'):
    symbol_col = 'symbol'
else:
    symbol_col = 'ticker'

df = conn.execute(f"""
    SELECT DISTINCT {symbol_col}
    FROM read_parquet('{source_dir}/**/*.parquet')
    WHERE {symbol_col} IS NOT NULL  -- ADD THIS LINE
    ORDER BY {symbol_col}
""").fetchdf()

symbols = df[symbol_col].tolist()
# Filter out any remaining null/NaN values
symbols = [s for s in symbols if s and str(s).lower() != 'nan']
```

#### 4. **macOS Metadata Files** ⚠️ **MODERATE**

**Problem**: macOS creates `._*` metadata files that interfere with Qlib's frequency detection.

**Solution**: Clean up after conversion:

```bash
find /path/to/qlib/stocks_daily -name "._*" -type f -delete
```

**Code Fix Needed**: Add cleanup step to `QlibBinaryWriter.convert_data_type()`:

```python
# After conversion completes
import subprocess
subprocess.run([
    'find', str(output_dir), '-name', '._*', '-type', 'f', '-delete'
], check=False)
```

## Running the Example

### 1. Ensure Data Fixes Are Applied

Before running, verify:

```bash
# 1. Check frequency metadata exists
cat /Volumes/sandisk/quantmini-lake/gold/qlib/stocks_daily/.qlib/dataset_info.json
# Should show: {"freq": ["day"]}

# 2. Check instruments file format (should be TAB-separated)
head -5 /Volumes/sandisk/quantmini-lake/gold/qlib/stocks_daily/instruments/all.txt
# Should show: SYMBOL<TAB>START_DATE<TAB>END_DATE

# 3. Check for null symbols
grep -i "nan" /Volumes/sandisk/quantmini-lake/gold/qlib/stocks_daily/instruments/all.txt
# Should return nothing

# 4. Clean macOS metadata files
find /Volumes/sandisk/quantmini-lake/gold/qlib/stocks_daily -name "._*" -delete
```

### 2. Run the Workflow

```bash
uv run python examples/run_qlib_workflow.py
```

### 3. Expected Output

```
================================================================================
QLIB WORKFLOW EXAMPLE - US Stocks
================================================================================

Configuration loaded from: examples/qlib_workflow_config.yaml
Data source: /Volumes/sandisk/quantmini-lake/gold/qlib/stocks_daily
Date range: 2025-08-01 to 2025-09-29
Market: all

================================================================================
STEP 1: Initializing Qlib
================================================================================
✓ Qlib initialized successfully

================================================================================
STEP 2: Creating Dataset with Alpha158 Features
================================================================================
Handler: Alpha158
Train: [2025-08-01, 2025-08-22]
Valid: [2025-08-23, 2025-09-06]
Test: [2025-09-09, 2025-09-29]
✓ Dataset created successfully

================================================================================
STEP 3: Creating LightGBM Model
================================================================================
✓ Model created successfully

================================================================================
STEP 4: Training Model
================================================================================
Training on segment: [2025-08-01, 2025-08-22]
[20]    train's l2: 0.991353    valid's l2: 0.999148
...
[92]    train's l2: 0.968197    valid's l2: 0.99843
✓ Model training complete

================================================================================
STEP 5: Making Predictions
================================================================================
✓ Validation predictions: ~108K samples
✓ Test predictions: ~180K samples

================================================================================
STEP 6: Recording Predictions
================================================================================
✓ Signal record generated

✅ WORKFLOW COMPLETED SUCCESSFULLY!
```

## Data Statistics

- **Symbols**: 11,993 US stocks
- **Date Range**: 2025-08-01 to 2025-09-29 (42 trading days)
- **Features**: Alpha158 (158 technical indicators)
- **Training Samples**: ~108,000
- **Test Samples**: ~180,000

## Key Learnings

1. **Qlib requires strict data format compliance** - Tab-separated instruments, specific file naming
2. **Frequency metadata is mandatory** - Must exist in `.qlib/dataset_info.json`
3. **Null handling is critical** - NaN symbols break the system
4. **macOS metadata cleanup needed** - `._*` files interfere with glob patterns

## Next Steps

1. **Fix `QlibBinaryWriter`** - Apply the 4 fixes above to prevent these issues
2. **Add validation** - Create a validator that checks for these issues post-conversion
3. **Automated cleanup** - Add cleanup steps to the conversion pipeline
4. **Documentation** - Update `docs/api-reference/qlib.md` with these requirements

## Troubleshooting

### Error: "freq format is not supported"

**Cause**: Missing or incorrect `.qlib/dataset_info.json` file, or macOS metadata files

**Solution**:
```bash
# Create frequency metadata in gold layer
echo '{"freq": ["day"]}' > /path/to/gold/qlib/stocks_daily/.qlib/dataset_info.json

# Remove macOS metadata
find /path/to/gold/qlib/stocks_daily -name "._*" -delete
```

### Error: "'float' object has no attribute 'lower'"

**Cause**: NaN/null symbol in instruments file or features directory

**Solution**:
```bash
# Check for null symbols
grep -i "nan" /path/to/instruments/all.txt

# Remove nan features directory if exists
rm -rf /path/to/features/nan
```

### Error: "Too many columns specified: expected 3 and found 1"

**Cause**: Instruments file is not TAB-separated

**Solution**: Convert to tab-separated format (see Fix #2 above)

## References

- [Qlib Official Workflow Documentation](https://qlib.readthedocs.io/en/latest/component/workflow.html)
- [Qlib Data Format Specification](https://qlib.readthedocs.io/en/latest/component/data.html)
- [Project Documentation](../docs/api-reference/qlib.md)
