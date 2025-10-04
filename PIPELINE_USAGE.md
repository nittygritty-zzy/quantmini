# Unified Pipeline Usage

## Single Command for Complete Data Processing

Instead of running three separate commands (download, ingest, enrich), you can use the unified **`quantmini pipeline run`** command:

### Full Pipeline (Download → Parquet → Enrich)

```bash
# Process stocks daily data for a date range
uv run quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2025-10-07 \
  --end-date 2025-10-10 \
  --skip-convert

# Process options minute data
uv run quantmini pipeline run \
  --data-type options_minute \
  --start-date 2025-10-07 \
  --end-date 2025-10-07 \
  --skip-convert
```

### Skip Certain Steps

```bash
# Skip download/ingest (only enrich existing data)
uv run quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-10 \
  --skip-ingest \
  --skip-convert

# Skip enrichment (only download and convert to Parquet)
uv run quantmini pipeline run \
  --data-type options_daily \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --skip-enrich \
  --skip-convert
```

## Pipeline Stages

1. **Ingest** (Download CSV → Parquet)
   - Downloads from Polygon.io S3
   - Converts to optimized Parquet format
   - Validates schema and data types

2. **Enrich** (Add Alpha158 Features)
   - Computes technical indicators
   - Adds alpha factors
   - Uses DuckDB for efficient computation

3. **Convert** (Optional: Qlib Binary)
   - Converts to Qlib binary format
   - We skip this by using `--skip-convert`

## What We Use

- **Parquet files**: Stored in `/Volumes/sandisk/quantmini-data/data/data/parquet/`
- **Enriched data**: Stored in `/Volumes/sandisk/quantmini-data/data/data/enriched/`
- **No Qlib binaries**: We use `--skip-convert` to keep only Parquet format

## Benefits

✅ Single command for entire workflow
✅ Automatic incremental processing (skips already processed dates)
✅ Trading day filtering (skips weekends/holidays automatically)
✅ Memory-optimized streaming for large datasets
✅ Atomic operations with proper error handling

## Daily Update Example

```bash
# Update all data types for the latest trading day
TODAY=$(date +%Y-%m-%d)

for dtype in stocks_daily options_daily stocks_minute options_minute; do
  uv run quantmini pipeline run \
    --data-type $dtype \
    --start-date $TODAY \
    --end-date $TODAY \
    --skip-convert
done
```

## Historical Backfill Example

```bash
# Backfill missing data (automatically skips existing dates with incremental mode)
uv run quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2025-10-03 \
  --skip-convert
```

The pipeline automatically:
- Filters to trading days only
- Skips weekends and market holidays  
- Uses incremental mode (skips already processed dates)
- Handles errors gracefully with detailed logging
