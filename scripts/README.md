# QuantMini Scripts

**Utility scripts for Medallion Architecture data pipeline management**

Last Updated: October 18, 2025

---

## Overview

This directory contains utility scripts for managing the QuantMini data pipeline with Medallion Architecture (Bronze → Silver → Gold). All scripts are designed to work with the current project structure and run with `uv`.

```bash
# Activate environment
source .venv/bin/activate

# Run scripts
uv run python scripts/category/script_name.py
```

---

## Directory Structure

```
scripts/
├── download/          # Bronze Layer: Polygon REST API downloaders
├── ingestion/         # Landing → Bronze: Data ingestion scripts
├── transformation/    # Bronze → Silver: Feature engineering
├── conversion/        # Silver → Gold: Qlib binary conversion
├── automation/        # Automated pipeline orchestration
├── validation/        # Data quality and verification
├── archive/           # Deprecated/completed one-time scripts
└── README.md          # This file
```

---

## Download Scripts (Bronze Layer)

Location: `scripts/download/`

### Polygon REST API Downloaders

**News Data**
- `download_news_1year.py` - Download financial news articles (8+ years available)
  ```bash
  # Download 1 year of news
  uv run python scripts/download/download_news_1year.py

  # Download full history (8+ years)
  uv run python scripts/download/download_news_1year.py --start-date 2017-04-10

  # Custom date range
  uv run python scripts/download/download_news_1year.py --start-date 2024-01-01 --end-date 2024-12-31
  ```

**Corporate Actions**
- `download_corporate_actions.py` - Download dividends, splits, IPOs
  - API Endpoints: `/v3/reference/dividends`, `/v3/reference/splits`, `/vX/reference/ipos`
- `download_ticker_events_optimized.py` - Batch download ticker changes/rebranding
  - API Endpoint: `/vX/reference/tickers/{id}/events`
  ```bash
  # Download ticker events for all CS tickers (optimized, 2-5 minutes)
  uv run python scripts/download/download_ticker_events_optimized.py
  ```

**Fundamentals**
- `download_fundamentals.py` - Download fundamental data (income statements, balance sheets, cash flow)
  ```bash
  # Download fundamentals with short data
  uv run python scripts/download/download_fundamentals.py --tickers-file tickers_cs.txt --include-short-data
  ```
- `download_short_data.py` - Download short interest & short volume
  - API Endpoints:
    - `/stocks/v1/short-interest` - Bi-weekly short interest (2 year max)
    - `/stocks/v1/short-volume` - Daily short volume (all history)

**Reference Data**
- `download_reference_data.py` - Download ticker metadata and reference data
- `download_all_tickers.py` - Download complete ticker list
- `download_all_relationships.py` - Download ticker relationships

---

## Ingestion Scripts (Landing → Bronze)

Location: `scripts/ingestion/`

**Purpose**: Move data from landing layer to bronze layer with validation

- `landing_to_bronze.py` - General landing → bronze ingestion with schema validation
- `download_to_landing.py` - Download directly to landing layer
- `ingest_news.py` - Ingest news data to bronze
- `ingest_fundamentals.py` - Ingest fundamental data to bronze
- `ingest_reference_data.py` - Ingest reference data to bronze
- `ingest_delisted_stocks.py` - Ingest delisted stock data
- `ingest_s3_flatfiles.sh` - Ingest S3 flat files (legacy)

---

## Transformation Scripts (Bronze → Silver)

Location: `scripts/transformation/`

**Purpose**: Enrich bronze data with features for silver layer

- `transform_add_features.py` - Add technical indicators and features
  ```bash
  # Generate Alpha158 features
  uv run python scripts/transformation/transform_add_features.py
  ```

**Root Level (Legacy)**
- `enrich_features.py` - Calculate financial ratios and features (legacy, use transformation/ instead)

---

## Conversion Scripts (Silver → Gold)

Location: `scripts/conversion/`

**Purpose**: Convert silver data to Qlib binary format for ML backtesting

- `convert_to_qlib_binary.py` - Convert Parquet to Qlib binary format
  ```bash
  # Convert stocks_daily to Qlib format
  uv run python scripts/conversion/convert_to_qlib_binary.py
  ```
- `validate_qlib_format.py` - Validate Qlib binary data integrity

**Root Level (Legacy)**
- `convert_to_qlib.py` - Legacy Qlib conversion (use conversion/ instead)
- `verify_qlib_conversion.py` - Legacy validation script

---

## Automation Scripts

Location: `scripts/automation/`

**Purpose**: Automated pipeline orchestration

- `orchestrate_daily_pipeline.sh` - Daily automated data refresh
- `orchestrate_weekly_pipeline.sh` - Weekly comprehensive update
- `setup_cron_jobs.sh` - Configure automated scheduling

**Root Level**
- `bulk_download_all_data.sh` - Bulk download all data types
- **`daily_update.sh`** - **MAIN ENTRY POINT** - Complete bronze→silver→gold pipeline
- `weekly_update.sh` - Weekly update script (legacy)
- `setup_weekly_automation.sh` - Setup weekly automation

### Daily Update Script (MAIN ENTRY POINT)

**File**: `daily_update.sh`

The **primary entry point** for all daily data pipeline operations using CLI commands. This script orchestrates the complete bronze → silver → gold data flow.

#### Features
- **Complete Pipeline**: All bronze, silver, and gold layer operations via CLI
- **Flexible**: Process yesterday's data, specific dates, or backfill multiple days
- **Selective**: Skip bronze, silver, or gold layers as needed
- **Comprehensive Logging**: Timestamped logs in `logs/`
- **Error Tracking**: Reports success/failure of each stage

#### Usage

```bash
# Process yesterday's data (default)
./scripts/daily_update.sh

# Process specific date
./scripts/daily_update.sh --date 2025-09-15

# Backfill last 7 days
./scripts/daily_update.sh --days-back 7

# Only transformations (skip bronze)
./scripts/daily_update.sh --skip-bronze

# Dry run to preview
./scripts/daily_update.sh --dry-run

# Custom tickers
./scripts/daily_update.sh --fundamental-tickers "AAPL MSFT GOOGL"
```

#### Pipeline Stages

**Bronze Layer (Data Ingestion):**
1. Stocks Daily/Minute (S3 → Parquet)
2. Options Daily/Minute (S3 → Parquet)
3. Fundamentals (Polygon REST API)
4. Financial Ratios (Calculated)
5. Corporate Actions (Polygon REST API)
6. News (Polygon REST API)
7. Reference Data (Weekly, Mondays only)

**Silver Layer (Transformations):**
1. Financial Ratios → Silver (via `quantmini transform financial-ratios`)
2. Corporate Actions → Silver (via `quantmini transform corporate-actions`)
3. Fundamentals → Silver (via `quantmini transform fundamentals`)

**Gold Layer (Enrichment):**
1. Stocks Daily → Enriched + Qlib Binary
2. Stocks Minute → Enriched Parquet
3. Options Daily → Enriched Parquet

#### Cron Setup

```bash
# Edit crontab
crontab -e

# Run daily at 6 AM (weekdays only) - update paths to your installation
0 6 * * 1-5 /path/to/quantmini/scripts/daily_update.sh >> /path/to/quantmini/logs/cron.log 2>&1
```

#### Logs

All operations logged to: `logs/daily_update_YYYYMMDD_HHMMSS.log`

---

## Validation Scripts

Location: `scripts/validation/`

**Purpose**: Data quality verification and monitoring

- `validate_duckdb_access.py` - Verify DuckDB can access Parquet tables
- `monitor_pipeline_progress.py` - Monitor download and processing progress

**Root Level**
- `verify_duckdb_tables.py` - Verify DuckDB table access (legacy)
- `check_download_status.py` - Check download progress (current)

---

## Backfill Scripts

Location: `scripts/backfill/` (to be created)

**Root Level (Current)**
- `backfill_historical.py` - General historical backfill
- `backfill_news_1year.py` - Backfill 1 year of news (use download/download_news_1year.py instead)

---

## Archived Scripts

Location: `scripts/archive/`

**Deprecated/completed one-time scripts**

- `fix_failed_ratios.py` - One-time schema migration (completed)
- `partition_by_date_screener.py` - One-time data restructuring (completed)

---

## Common Workflows

### 1. Complete Data Refresh

```bash
# 1. Download reference data (Bronze)
uv run python scripts/download/download_reference_data.py
uv run python scripts/download/download_all_tickers.py

# 2. Download market data (Bronze)
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31

# 3. Download fundamentals (Bronze)
uv run python scripts/download/download_fundamentals.py --tickers-file tickers_cs.txt

# 4. Download news (Bronze)
uv run python scripts/download/download_news_1year.py

# 5. Enrich with features (Silver)
uv run python scripts/transformation/transform_add_features.py

# 6. Convert to Qlib (Gold)
uv run python scripts/conversion/convert_to_qlib_binary.py

# 7. Verify
uv run python scripts/validation/validate_duckdb_access.py
uv run python scripts/conversion/validate_qlib_format.py
```

### 2. Daily Update

```bash
# Run daily automation
bash scripts/automation/orchestrate_daily_pipeline.sh
```

### 3. News Backfill (8+ years)

```bash
# Download full news history
uv run python scripts/download/download_news_1year.py --start-date 2017-04-10
```

### 4. Ticker Events (Optimized)

```bash
# Download ticker events for all CS tickers (2-5 minutes)
uv run python scripts/download/download_ticker_events_optimized.py
```

---

## Medallion Architecture Alignment

**Scripts are organized by data layer:**

| Layer | Purpose | Scripts Location | Output |
|-------|---------|------------------|--------|
| **Landing** | Raw source data | `ingestion/` | `landing/` |
| **Bronze** | Validated Parquet | `download/` | `bronze/{type}/` |
| **Silver** | Feature-enriched | `transformation/` | `silver/{type}/` |
| **Gold** | ML-ready binary | `conversion/` | `gold/qlib/` |

---

## Testing Scripts

Location: `scripts/tests/` (if exists)

**Root Level Test Scripts**
- `test_all_polygon_endpoints.sh` - Test all Polygon REST API endpoints
- `test_partition_strategy.sh` - Test partitioning strategies
- `test_screener_partition.sh` - Test screener data partitioning

---

## Legacy Scripts (Root Level)

These scripts are currently in the root `scripts/` directory but should be moved to appropriate subdirectories or archived:

**To Move:**
- `batch_load_fundamentals_all.py` → `download/` (or merge with download_fundamentals.py)
- `download_ticker_metadata.py` → `download/`
- `download_delisted_stocks.py` → `download/`
- `backfill_historical.py` → `backfill/`
- `backfill_news_1year.py` → Archive (replaced by download/download_news_1year.py)

**To Archive:**
- `publish_package.sh` → Archive (PyPI packaging no longer used)
- `monitor_backfill.sh` → Archive (replaced by validation/monitor_pipeline_progress.py)

---

## Best Practices

1. **Always use uv**: Run scripts with `uv run python scripts/...`
2. **Check dates**: Verify today's date before running time-sensitive commands
3. **Test first**: Use test scripts to validate endpoints before bulk downloads
4. **Monitor progress**: Use validation scripts to track download status
5. **Incremental updates**: Use `--incremental` flag for daily updates
6. **Batch optimization**: Use optimized downloaders for large datasets

---

## Environment Requirements

- Python 3.10+
- uv package manager
- Polygon.io API key and S3 credentials
- External storage (recommended: 500GB+)

---

## Documentation References

- **Medallion Architecture**: `docs/architecture/MEDALLION_ARCHITECTURE_SCRIPTS.md`
- **Batch Downloader**: `docs/guides/batch-downloader.md`
- **Data Loader**: `docs/guides/data-loader.md`
- **API Reference**: `docs/api-reference/POLYGON_REST_API.md`

---

For detailed documentation, see the `docs/` directory.
