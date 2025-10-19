# Data Ingestion Strategies

**Complete guide to loading data into QuantMini: Initial batch load, incremental updates, and backfill**

Last Updated: October 18, 2025

---

## Overview

QuantMini supports three primary data ingestion strategies:

1. **Initial Batch Load**: First-time full historical data download
2. **Incremental Updates**: Daily/periodic updates with new data only
3. **Backfill**: Fill gaps in historical data coverage

All strategies follow the **Medallion Architecture** pattern: Landing → Bronze → Silver → Gold

---

## Table of Contents

- [Initial Batch Load](#initial-batch-load)
- [Incremental Updates](#incremental-updates)
- [Backfill Strategies](#backfill-strategies)
- [Data Types Reference](#data-types-reference)
- [Best Practices](#best-practices)
- [Automation](#automation)
- [Troubleshooting](#troubleshooting)

---

## Initial Batch Load

**Use Case**: First-time setup, loading complete historical dataset

### Strategy 1: Full Historical Load (Recommended)

Load all available historical data from Polygon.io.

#### Stocks Daily Data (5+ years)

```bash
# Download all available daily stock data
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2020-01-01 \
  -e 2025-10-18

# Expected: ~5 years, 11,994 tickers, ~200GB
# Duration: 2-4 hours (depends on network)
```

#### Options Daily Data (2+ years)

```bash
# Download all available daily options data
uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s 2023-01-01 \
  -e 2025-10-18

# Expected: ~2 years, 1.4M contracts, ~500GB
# Duration: 4-8 hours
```

#### News Articles (8+ years)

```bash
# Download full news history (2017-present)
uv run python scripts/download/download_news_1year.py \
  --start-date 2017-04-10 \
  --end-date 2025-10-18

# Expected: 8+ years, millions of articles, ~50GB
# Duration: 6-12 hours
```

#### Fundamentals Data (10+ years)

```bash
# Download all fundamental data
uv run python scripts/download/download_fundamentals.py \
  --tickers-file tickers_cs.txt \
  --include-short-data

# Expected: 10+ years, income statements, balance sheets, cash flow
# Duration: 2-4 hours
```

### Strategy 2: Recent Data Only (Fast Start)

Load recent data for quick testing/prototyping.

```bash
# Last 1 year of stocks daily
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2025-10-18

# Last 6 months of options
uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s 2025-04-01 \
  -e 2025-10-18

# Last 1 year of news
uv run python scripts/download/download_news_1year.py \
  --start-date 2024-01-01
```

### Strategy 3: Bulk Download Script (All Data Types)

```bash
# Download everything with one command
bash scripts/bulk_download_all_data.sh

# This script downloads:
# - Stocks daily (5 years)
# - Options daily (2 years)
# - News (8 years)
# - Fundamentals (10 years)
# - Reference data (current)
```

---

## Incremental Updates

**Use Case**: Daily/periodic updates after initial batch load

### Daily Update Workflow

#### 1. Update Stocks Daily

```bash
# Download yesterday's data only
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-1d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental

# Or update last 7 days (safer, handles weekends)
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental
```

#### 2. Update Options Daily

```bash
# Update last 7 days of options
uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental
```

#### 3. Update News

```bash
# Download yesterday's news
uv run python scripts/download/download_news_1year.py \
  --start-date $(date -v-1d +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d)
```

### Weekly Update Workflow

```bash
# Update all data types for the last week
bash scripts/automation/orchestrate_weekly_pipeline.sh

# This runs:
# 1. Stocks daily (last 7 days)
# 2. Options daily (last 7 days)
# 3. News (last 7 days)
# 4. Fundamentals (latest)
# 5. Feature engineering (silver layer)
# 6. Qlib conversion (gold layer)
```

### Incremental Flag Behavior

The `--incremental` flag enables smart deduplication:

```bash
# Without --incremental: Downloads and overwrites all data
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2025-01-01 \
  -e 2025-01-31

# With --incremental: Skips existing dates, adds new only
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2025-01-01 \
  -e 2025-01-31 \
  --incremental
```

**How it works**:
- Checks existing Parquet files for date coverage
- Skips dates already present in bronze layer
- Only downloads missing dates
- Appends new data to existing partitions

---

## Backfill Strategies

**Use Case**: Fill gaps in historical data, recover from failures

### Strategy 1: Date Range Backfill

Fill specific date range gaps:

```bash
# Backfill stocks for missing month
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-06-01 \
  -e 2024-06-30 \
  --incremental

# Backfill options for specific week
uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s 2025-03-01 \
  -e 2025-03-07 \
  --incremental
```

### Strategy 2: Monthly Backfill (Large Gaps)

Use monthly chunks for large backfills:

```bash
# Backfill entire year, month by month
for month in {01..12}; do
  uv run python -m src.cli.main data ingest \
    -t stocks_daily \
    -s 2024-${month}-01 \
    -e 2024-${month}-31 \
    --incremental

  echo "Completed month: 2024-${month}"
  sleep 10  # Rate limiting
done
```

### Strategy 3: Gap Detection & Auto-Backfill

Detect and fill gaps automatically:

```bash
# Use gap detection script
uv run python scripts/validation/detect_data_gaps.py \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2025-10-18

# Output: List of missing date ranges
# 2024-03-15 to 2024-03-20 (6 days)
# 2024-07-04 (holiday)
# 2024-09-10 to 2024-09-12 (3 days)

# Auto-backfill detected gaps
uv run python scripts/backfill/auto_backfill_gaps.py \
  --data-type stocks_daily \
  --gaps-file gaps_detected.json
```

### Strategy 4: News Backfill (Historical)

Backfill news articles for specific periods:

```bash
# Backfill 1 year of news
uv run python scripts/download/download_news_1year.py \
  --start-date 2023-01-01 \
  --end-date 2023-12-31

# Backfill full 8-year history
uv run python scripts/download/download_news_1year.py \
  --start-date 2017-04-10 \
  --end-date 2025-10-18
```

### Strategy 5: Fundamentals Backfill

```bash
# Backfill fundamentals for all tickers
uv run python scripts/download/download_fundamentals.py \
  --tickers-file tickers_cs.txt \
  --include-short-data \
  --backfill-years 10
```

---

## Data Types Reference

### Market Data (CLI Ingestion)

| Data Type | CLI Command | History | Size | Update Frequency |
|-----------|-------------|---------|------|------------------|
| **stocks_daily** | `data ingest -t stocks_daily` | 5+ years | ~200GB | Daily |
| **options_daily** | `data ingest -t options_daily` | 2+ years | ~500GB | Daily |
| **stocks_minute** | `data ingest -t stocks_minute` | 2+ years | ~5TB | Daily |
| **options_minute** | `data ingest -t options_minute` | 2+ years | ~10TB | Daily |

### Alternative Data (Script Downloaders)

| Data Type | Script | History | Size | Update Frequency |
|-----------|--------|---------|------|------------------|
| **News** | `scripts/download/download_news_1year.py` | 8+ years | ~50GB | Daily |
| **Fundamentals** | `scripts/download/download_fundamentals.py` | 10+ years | ~20GB | Quarterly |
| **Corporate Actions** | `scripts/download/download_corporate_actions.py` | 5+ years | ~5GB | Weekly |
| **Reference Data** | `scripts/download/download_reference_data.py` | Current | ~1GB | Monthly |

---

## Best Practices

### 1. Start with Recent Data

```bash
# ✅ Good: Start with 1 year
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2025-10-18

# ❌ Avoid: Starting with 10 years on first try
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2015-01-01 \
  -e 2025-10-18  # May fail, too large
```

### 2. Use Incremental Mode

```bash
# ✅ Always use --incremental after initial load
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2025-10-18 \
  --incremental

# ✅ Safe for daily updates
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental
```

### 3. Monitor Progress

```bash
# Check download status
uv run python scripts/validation/check_download_status.py

# Monitor pipeline progress
uv run python scripts/validation/monitor_pipeline_progress.py
```

### 4. Verify Data Quality

```bash
# Validate bronze layer
uv run python scripts/validation/validate_duckdb_access.py

# Check for gaps
uv run python scripts/validation/detect_data_gaps.py
```

### 5. Automate Updates

```bash
# Setup daily automation (cron)
bash scripts/automation/setup_cron_jobs.sh

# Cron will run:
# - Daily: stocks_daily, options_daily, news
# - Weekly: fundamentals, full pipeline
```

---

## Automation

### Daily Automation (Recommended)

#### Setup Cron Job

```bash
# Install cron jobs
bash scripts/automation/setup_cron_jobs.sh

# Cron schedule:
# 0 6 * * * /path/to/scripts/automation/orchestrate_daily_pipeline.sh
# Runs daily at 6 AM
```

#### Daily Pipeline Script

`scripts/automation/orchestrate_daily_pipeline.sh`:

```bash
#!/bin/bash
# Daily data refresh pipeline

# 1. Download yesterday's data
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-1d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental

uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s $(date -v-1d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental

uv run python scripts/download/download_news_1year.py \
  --start-date $(date -v-1d +%Y-%m-%d)

# 2. Feature engineering (silver layer)
uv run python scripts/transformation/transform_add_features.py

# 3. Qlib conversion (gold layer)
uv run python scripts/conversion/convert_to_qlib_binary.py

# 4. Validation
uv run python scripts/validation/validate_duckdb_access.py
```

### Weekly Automation

#### Setup Weekly Pipeline

```bash
# Run weekly comprehensive update
bash scripts/automation/orchestrate_weekly_pipeline.sh

# Includes:
# - Full data validation
# - Gap detection and backfill
# - Fundamentals update
# - Reference data refresh
```

---

## Troubleshooting

### Issue 1: Rate Limiting

**Symptom**: `429 Too Many Requests` errors

**Solution**:
```bash
# Reduce concurrency
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2024-12-31 \
  --max-concurrent 10  # Default: 50

# Add delays between requests
uv run python scripts/download/download_news_1year.py \
  --rate-limit 5  # 5 requests/second
```

### Issue 2: Incomplete Downloads

**Symptom**: Missing dates in downloaded data

**Solution**:
```bash
# 1. Detect gaps
uv run python scripts/validation/detect_data_gaps.py \
  --data-type stocks_daily

# 2. Backfill gaps
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-06-01 \
  -e 2024-06-30 \
  --incremental

# 3. Verify
uv run python scripts/validation/validate_duckdb_access.py
```

### Issue 3: Disk Space

**Symptom**: `No space left on device`

**Solution**:
```bash
# Check disk usage
df -h /Volumes/sandisk/quantmini-data

# Clean old data
uv run python scripts/maintenance/cleanup_old_data.py \
  --keep-days 365

# Move to external drive
rsync -av --progress \
  /Volumes/sandisk/quantmini-data/ \
  /Volumes/backup/quantmini-archive/
```

### Issue 4: Network Failures

**Symptom**: Connection timeouts, partial downloads

**Solution**:
```bash
# Use incremental mode (auto-resumes)
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2024-12-31 \
  --incremental

# Retry failed downloads
uv run python scripts/download/retry_failed_downloads.py
```

---

## Example Workflows

### Workflow 1: New Project Setup

```bash
# Day 1: Initial batch load (recent data)
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2025-10-18
uv run python -m src.cli.main data ingest -t options_daily -s 2024-01-01 -e 2025-10-18
uv run python scripts/download/download_news_1year.py --start-date 2024-01-01

# Day 2: Feature engineering
uv run python scripts/transformation/transform_add_features.py

# Day 3: Qlib conversion
uv run python scripts/conversion/convert_to_qlib_binary.py

# Day 4: Setup automation
bash scripts/automation/setup_cron_jobs.sh
```

### Workflow 2: Daily Maintenance

```bash
# Automated daily update (via cron)
# 0 6 * * * /path/to/scripts/automation/orchestrate_daily_pipeline.sh

# Manual daily update
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental
```

### Workflow 3: Historical Backfill

```bash
# Step 1: Identify gaps
uv run python scripts/validation/detect_data_gaps.py \
  --data-type stocks_daily \
  --start-date 2020-01-01 \
  --end-date 2025-10-18

# Step 2: Backfill year by year
for year in {2020..2024}; do
  uv run python -m src.cli.main data ingest \
    -t stocks_daily \
    -s ${year}-01-01 \
    -e ${year}-12-31 \
    --incremental

  echo "Completed year: ${year}"
done

# Step 3: Verify completeness
uv run python scripts/validation/validate_data_completeness.py
```

---

## Performance Tips

### 1. Parallel Downloads

```bash
# Download multiple data types in parallel
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31 &
uv run python -m src.cli.main data ingest -t options_daily -s 2024-01-01 -e 2024-12-31 &
uv run python scripts/download/download_news_1year.py --start-date 2024-01-01 &

wait  # Wait for all to complete
```

### 2. Optimize Concurrency

```bash
# High-bandwidth connection: Increase concurrency
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2024-12-31 \
  --max-concurrent 100

# Low-bandwidth: Decrease concurrency
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s 2024-01-01 \
  -e 2024-12-31 \
  --max-concurrent 10
```

### 3. Compression

```bash
# All Parquet files use ZSTD compression by default
# No additional configuration needed

# Verify compression
uv run python scripts/validation/check_compression_ratio.py
```

---

## Data Coverage Summary

**Current Coverage** (as of October 18, 2025):

| Data Type | Start Date | End Date | Tickers/Contracts | Size |
|-----------|------------|----------|-------------------|------|
| Stocks Daily | 2020-01-01 | 2025-10-18 | 11,994 | ~200GB |
| Options Daily | 2023-01-01 | 2025-10-18 | 1,388,382 | ~500GB |
| News Articles | 2017-04-10 | 2025-10-18 | Millions | ~50GB |
| Fundamentals | 2015-01-01 | 2025-10-18 | 11,994 | ~20GB |

**Total Storage**: ~770GB (bronze layer)

---

## See Also

- [Batch Downloader Guide](./batch-downloader.md)
- [Data Loader Guide](./data-loader.md)
- [Medallion Architecture](../architecture/MEDALLION_ARCHITECTURE_SCRIPTS.md)
- [Automation Scripts](../../scripts/README.md)

---

**Last Updated**: October 18, 2025
