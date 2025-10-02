# Data Integrity Checker

Automated tool to check data completeness and generate backfill commands for missing dates.

## Features

- ✅ Scan parquet files for existing dates
- ✅ Scan qlib binary calendars for existing dates
- ✅ Detect date gaps within data ranges
- ✅ Identify dates in parquet but not in qlib (need conversion)
- ✅ Identify dates in qlib but not in parquet (data loss)
- ✅ Generate backfill commands
- ✅ Optionally run backfill directly

## Supported Data Types

- `stocks_daily` - Daily stock data
- `stocks_minute` - Minute-level stock data
- `options_daily` - Daily options data

## Usage

### 1. Check Only (No Backfill)

```bash
python -m src.maintenance.data_integrity_checker --check-only
```

This will:
- Scan all data types
- Report date ranges and gaps
- Exit without generating commands

### 2. Generate Backfill Commands

```bash
python -m src.maintenance.data_integrity_checker
```

This will:
- Check all data types
- Generate backfill commands
- Save to `backfill_commands.sh`

Custom output file:
```bash
python -m src.maintenance.data_integrity_checker --output my_backfill.sh
```

### 3. Run Backfill Directly

```bash
python -m src.maintenance.data_integrity_checker --run-backfill
```

This will:
- Check all data types
- Automatically download missing parquet data
- Automatically convert missing qlib data
- Report results

Options:
```bash
# Only backfill parquet (download from Polygon.io)
python -m src.maintenance.data_integrity_checker --run-backfill --parquet-only

# Only backfill qlib (convert from parquet)
python -m src.maintenance.data_integrity_checker --run-backfill --qlib-only
```

### 4. Custom Directories

```bash
python -m src.maintenance.data_integrity_checker \
  --parquet-root /path/to/parquet \
  --qlib-root /path/to/qlib
```

## Programmatic Usage

```python
from pathlib import Path
from src.maintenance.data_integrity_checker import DataIntegrityChecker
from src.core.config_loader import ConfigLoader

# Initialize
checker = DataIntegrityChecker(
    parquet_root=Path("data/parquet"),
    qlib_root=Path("/path/to/qlib"),
    config=ConfigLoader()
)

# Run integrity check
results = checker.check_all()

# Generate backfill commands
commands = checker.generate_backfill_commands(
    results,
    output_file=Path("backfill_commands.sh")
)

# Or run backfill directly
backfill_results = checker.run_backfill(results)
```

## Output Examples

### No Issues Found

```
======================================================================
DATA INTEGRITY CHECK
======================================================================

======================================================================
Checking: stocks_daily
======================================================================
  Parquet: 5 dates found
  Qlib:    5 dates found
  Parquet range: 2025-09-26 to 2025-09-30
  Qlib range:    2025-09-26 to 2025-09-30
  ✅ No gaps in parquet data
  ✅ No gaps in qlib data

✅ No backfill needed - data is complete!
```

### Issues Found

```
======================================================================
Checking: stocks_daily
======================================================================
  Parquet: 10 dates found
  Qlib:    8 dates found
  Parquet range: 2025-09-20 to 2025-09-30
  Qlib range:    2025-09-20 to 2025-09-28
  ⚠️  Found 1 gaps in parquet data:
      2025-09-25 to 2025-09-26
  ✅ No gaps in qlib data
  ⚠️  2 dates in parquet but not in qlib (need conversion)
      2025-09-29, 2025-09-30

======================================================================
BACKFILL COMMANDS
======================================================================

stocks_daily - Parquet gaps:
  2025-09-25 to 2025-09-26

stocks_daily - Convert to qlib:
  2025-09-29 to 2025-09-30

✅ Commands written to: backfill_commands.sh
📋 Generated 4 commands
```

### Generated Backfill Script

```bash
#!/bin/bash
# Data backfill commands
# Generated: 2025-10-01 20:50:42

# Backfill parquet: stocks_daily from 2025-09-25 to 2025-09-26
python -c "import asyncio; from src.orchestration.ingestion_orchestrator import IngestionOrchestrator; from src.core.config_loader import ConfigLoader; asyncio.run(IngestionOrchestrator(config=ConfigLoader()).ingest_date_range('stocks_daily', '2025-09-25', '2025-09-26', use_polars=True))"

# Convert to qlib: stocks_daily from 2025-09-29 to 2025-09-30
python -c "from src.transform.qlib_binary_writer import QlibBinaryWriter; from src.core.config_loader import ConfigLoader; from pathlib import Path; writer = QlibBinaryWriter(enriched_root=Path('data/parquet'), qlib_root=Path('/path/to/qlib'), config=ConfigLoader()); writer.convert_data_type('stocks_daily', '2025-09-29', '2025-09-30', incremental=False)"
```

## How It Works

### Gap Detection

The checker identifies three types of issues:

1. **Parquet Gaps** - Missing dates within the parquet data range
   - Example: Have 2025-09-20 and 2025-09-25, missing 2025-09-21 to 2025-09-24
   - Fix: Download missing dates from Polygon.io

2. **Qlib Gaps** - Missing dates within the qlib data range
   - Example: Have 2025-09-20 and 2025-09-25 in calendar, missing 2025-09-21 to 2025-09-24
   - Fix: Convert missing dates from parquet

3. **Missing in Qlib** - Dates present in parquet but not in qlib
   - Example: Parquet has up to 2025-09-30, qlib only has up to 2025-09-28
   - Fix: Convert missing dates from parquet

4. **Missing in Parquet** - Dates in qlib but not in parquet (data loss)
   - Example: Qlib has 2025-09-25 but parquet doesn't
   - Fix: Re-download from Polygon.io

### Date Range Logic

The checker:
- Does NOT require a trading calendar
- Analyzes existing data to determine date ranges
- Only flags gaps WITHIN those ranges
- Groups consecutive missing dates into ranges for efficient backfill

For example, if you only have 2025-09-30, it won't flag 2025-09-29 as missing.
But if you have 2025-09-27, 2025-09-28, and 2025-09-30, it will flag 2025-09-29 as a gap.

## Best Practices

1. **Run regularly** - Check integrity after each data download
2. **Review before running** - Always review generated commands before executing
3. **Check backfill results** - Verify backfill completed successfully
4. **Monitor logs** - Watch for errors during backfill operations

## Integration with Pipeline

Add to your data pipeline:

```python
# After downloading new data
from src.maintenance.data_integrity_checker import DataIntegrityChecker

checker = DataIntegrityChecker()
results = checker.check_all()

# Auto-backfill if issues found
if any(r['parquet']['gaps'] or r['missing_in_qlib'] for r in results.values()):
    print("Issues found - running backfill...")
    checker.run_backfill(results)
```

## Troubleshooting

### "No dates found in parquet"
- Check that parquet_root path is correct
- Verify parquet files exist and contain 'date' column

### "No dates found in qlib"
- Check that qlib_root path is correct
- Verify calendars/day.txt files exist
- Run initial conversion if qlib directory is empty

### "Backfill fails"
- Check Polygon.io credentials in config/credentials.yaml
- Verify sufficient disk space
- Check logs for detailed error messages
