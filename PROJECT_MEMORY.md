# QuantMini Project Memory

**Last Updated**: October 21, 2025
**Project**: QuantMini - High-Performance Financial Data Pipeline
**Architecture**: Medallion Architecture (Bronze → Silver → Gold)

---

## Project Overview

QuantMini is a production-grade financial data pipeline implementing Medallion Architecture with Qlib integration for quantitative trading research. The system processes market data from Polygon.io through multiple quality layers, culminating in ML-ready binary formats for backtesting.

**Key Features**:
- Medallion Architecture data lake (Landing → Bronze → Silver → Gold)
- Polygon.io REST API integration (direct downloads, no S3)
- High-performance async downloaders (HTTP/2, 100+ concurrent requests)
- 8+ years of news data, 5+ years of market data
- Qlib binary format for ML backtesting
- Complete automation with incremental updates

---

## Architecture

### Medallion Architecture Layers

```
Landing Layer          Bronze Layer         Silver Layer          Gold Layer
(Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
      ↓                    ↓                    ↓                     ↓
Polygon.io         →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
  REST API             (Schema Check)        (Alpha158)           (Backtesting)
      ↓                    ↓                    ↓                     ↓
landing/              bronze/{type}/      silver/{type}/        gold/qlib/
```

### Data Flow

1. **Landing**: Polygon REST API → Raw JSON responses
2. **Bronze**: Validated Parquet files (schema-checked, ZSTD compressed)
3. **Silver**: Parquet + Alpha158 features (158 technical indicators)
4. **Gold**: Qlib binary format (columnar storage for ML)

---

## Pipeline Entry Points

**IMPORTANT**: The project has ONLY two entry point scripts for all data operations:

1. **`scripts/daily_update_parallel.sh`** - Daily/incremental updates
   - Processes recent data (default: yesterday, configurable with `--days-back`)
   - Runs all layers in parallel for maximum performance
   - Use for: daily automation, backfilling recent data

2. **`scripts/historical_data_load.sh`** - Historical data downloads
   - Downloads large historical datasets (multi-year)
   - Optimized for bulk downloads with aggressive parallelization
   - Use for: initial setup, downloading historical fundamentals/short data

**All other scripts are internal components** called by these two entry points. Do not run individual scripts directly unless debugging.

---

## Critical Technical Details

### Data Storage

**Primary Data Root**: `/Volumes/990EVOPLUS/quantlake/` (External SSD)
**Config File**: `config/paths.yaml` (ONLY source of truth for all paths)
**Legacy Path**: `/Volumes/sandisk/quantmini-lake/` (deprecated)
- Configured in `config/pipeline_config.yaml`
- Must use external drive (500GB+ required)

**Directory Structure**:
```
/Volumes/990EVOPLUS/quantlake/
├── landing/           # Raw API responses (ephemeral)
├── bronze/            # Validated Parquet (~100GB + minute data)
│   ├── stocks_daily/      # Daily OHLCV data
│   ├── stocks_minute/     # Minute OHLCV data (34GB, partitioned)
│   ├── options_daily/     # Daily options aggregates
│   ├── options_minute/    # Minute options data (17GB, partitioned)
│   ├── news/              # News articles (12GB, 739K files, 10 years)
│   ├── fundamentals/      # Financial statements, ratios
│   ├── corporate_actions/ # Dividends, splits, IPOs
│   └── reference_data/    # Tickers, relationships
├── silver/            # Feature-enriched Parquet
│   ├── stocks_daily/  # + Alpha158 features
│   └── options_daily/
└── gold/              # ML-ready binary
    └── qlib/          # Microsoft Qlib format
        ├── instruments/
        ├── calendars/
        └── features/
```

### Partitioning Strategy

**Date-First Hive Partitioning** (NOT ticker-first):
```
bronze/news/news/year=2025/month=09/ticker=AAPL.parquet
                 ^^^^^^^^^^^^^^^^^^^^^^ Date first (enables partition pruning)
```

**Why date-first**:
- Efficient time-range queries (common use case)
- Partition pruning reduces I/O by 90%+
- Better compression ratios

---

## Data Sources

### Polygon.io REST API (Primary)

**Authentication**:
- API Key: Stored in `config/credentials.yaml`
- No S3 credentials needed (migrated from S3 to REST API)

**Key Downloaders** (`src/download/`):
- `polygon_rest_client.py` - Base HTTP/2 async client
- `news.py` - News articles downloader (8+ years available)
- `bars.py` - OHLCV data downloader
- `fundamentals.py` - Income statements, balance sheets, cash flow, short data
- `corporate_actions.py` - Dividends, splits, IPOs, ticker changes
- `reference_data.py` - Ticker metadata, relationships

**Polygon API Endpoints Covered:**
1. **Dividends** - `/v3/reference/dividends` - Cash dividends, payment dates
2. **Stock Splits** - `/v3/reference/splits` - Forward and reverse splits
3. **IPOs** - `/vX/reference/ipos` - Initial public offerings
4. **Ticker Events** - `/vX/reference/tickers/{id}/events` - Symbol changes, rebranding
5. **Short Interest** - `/stocks/v1/short-interest` - Bi-weekly short interest (2 year max)
6. **Short Volume** - `/stocks/v1/short-volume` - Daily short volume (all history)

**API Optimizations**:
- HTTP/2 multiplexing (100+ concurrent requests)
- Automatic retries with exponential backoff
- Rate limiting compliance
- Cursor-based pagination

### Data Coverage (as of October 18, 2025)

| Data Type | Start Date | Coverage | Size | Tickers/Contracts |
|-----------|-----------|----------|------|-------------------|
| **Stocks Daily** | 2020-01-01 | 5+ years | ~200GB | 11,994 |
| **Options Daily** | 2023-01-01 | 2+ years | ~500GB | 1,388,382 |
| **Stocks Minute** | 2020-01-01 | 5+ years | ~5TB | 11,994 |
| **Options Minute** | 2023-01-01 | 2+ years | ~10TB | 1,388,382 |
| **News** | 2017-04-10 | 8+ years | ~50GB | Millions |
| **Fundamentals** | 2015-01-01 | 10+ years | ~20GB | 11,994 |

**Total Bronze Layer**: ~770GB (excluding minute data)

---

## CLI Commands

### Data Ingestion (Primary Interface)

**Command Pattern**:
```bash
uv run python -m src.cli.main data ingest \
  -t <data_type> \
  -s <start_date> \
  -e <end_date> \
  [--incremental]
```

**Data Types**:
- `stocks_daily` - Daily OHLCV bars for stocks
- `options_daily` - Daily OHLCV bars for options
- `stocks_minute` - Minute bars for stocks (large)
- `options_minute` - Minute bars for options (very large)

**Key Flags**:
- `--incremental` - Skip existing dates (smart deduplication)
- `--max-concurrent N` - Control concurrency (default: 50)

**Examples**:
```bash
# Initial batch load (1 year)
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2025-10-18

# Incremental daily update
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) -e $(date +%Y-%m-%d) --incremental

# Backfill gap
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s 2024-06-01 -e 2024-06-30 --incremental
```

### Download Scripts (Alternative Data)

**News**:
```bash
# Download 1 year of news
uv run python scripts/download/download_news_1year.py --start-date 2024-01-01

# Download full 8-year history
uv run python scripts/download/download_news_1year.py --start-date 2017-04-10
```

**Fundamentals**:
```bash
uv run python scripts/download/download_fundamentals.py \
  --tickers-file tickers_cs.txt \
  --include-short-data
```

**Corporate Actions** (Dividends, Splits, IPOs):
```bash
# Download dividends, splits, and IPOs
quantmini polygon corporate-actions \
  --start-date 2024-01-01 \
  --end-date 2025-10-21 \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions

# Download ticker changes/rebranding
quantmini polygon ticker-events AAPL,MSFT,GOOGL \
  --output-dir $BRONZE_DIR/corporate_actions
```

**Short Interest & Short Volume**:
```bash
# Downloads both short interest AND short volume
quantmini polygon short-data AAPL,MSFT,GOOGL \
  --settlement-date-gte 2024-01-01 \
  --date-gte 2024-01-01 \
  --output-dir $BRONZE_DIR/fundamentals
```

**Bulk Download**:
```bash
# Download all data types at once
bash scripts/bulk_download_all_data.sh
```

---

## Data Ingestion Strategies

### 1. Initial Batch Load (First Time)

**Strategy A: Full Historical (Recommended for production)**
```bash
# 5+ years of stocks
uv run python -m src.cli.main data ingest -t stocks_daily -s 2020-01-01 -e 2025-10-18

# Duration: 2-4 hours
# Size: ~200GB
```

**Strategy B: Recent Data Only (Fast start)**
```bash
# 1 year of stocks
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2025-10-18

# Duration: 30-60 minutes
# Size: ~40GB
```

### 2. Incremental Updates (Daily Maintenance)

**Daily Update Workflow**:
```bash
# Update last 7 days (handles weekends/holidays)
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) -e $(date +%Y-%m-%d) --incremental

uv run python -m src.cli.main data ingest -t options_daily \
  -s $(date -v-7d +%Y-%m-%d) -e $(date +%Y-%m-%d) --incremental

# Download yesterday's news
uv run python scripts/download/download_news_1year.py \
  --start-date $(date -v-1d +%Y-%m-%d)
```

**How --incremental works**:
- Scans existing Parquet files for date coverage
- Skips dates already present in bronze layer
- Only downloads missing dates
- Prevents duplicate data and wasted API calls

### 3. Backfill (Fill Data Gaps)

**Date Range Backfill**:
```bash
# Backfill specific month
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s 2024-06-01 -e 2024-06-30 --incremental
```

**Monthly Backfill (Large Gaps)**:
```bash
# Backfill year 2024, month by month
for month in {01..12}; do
  uv run python -m src.cli.main data ingest -t stocks_daily \
    -s 2024-${month}-01 -e 2024-${month}-31 --incremental

  echo "Completed month: 2024-${month}"
  sleep 10  # Rate limiting
done
```

---

## Pipeline Scripts

### Transformation (Bronze → Silver)

**Location**: `scripts/transformation/`

**Add Features**:
```bash
# Generate Alpha158 features
uv run python scripts/transformation/transform_add_features.py
```

**What it does**:
- Reads bronze Parquet files
- Calculates 158 technical indicators (Alpha158)
- Writes to silver layer with same partitioning
- Preserves date-first structure

### Conversion (Silver → Gold)

**Location**: `scripts/conversion/`

**Convert to Qlib Binary**:
```bash
# Convert silver Parquet to Qlib format
uv run python scripts/conversion/convert_to_qlib_binary.py
```

**Output Structure**:
```
gold/qlib/
├── instruments/
│   └── all.txt              # List of tickers
├── calendars/
│   └── day.txt              # Trading dates
└── features/
    ├── {ticker}/
    │   ├── open.bin         # Binary column files
    │   ├── high.bin
    │   ├── low.bin
    │   ├── close.bin
    │   └── volume.bin
    └── ...
```

### Automation

**Daily Automation**:
```bash
# Setup cron jobs
bash scripts/automation/setup_cron_jobs.sh

# Daily pipeline (6 AM)
bash scripts/automation/orchestrate_daily_pipeline.sh
```

**Weekly Automation**:
```bash
# Comprehensive weekly update
bash scripts/automation/orchestrate_weekly_pipeline.sh
```

---

## Key Technical Fixes & Learnings

### 1. Qlib Binary Writer (CRITICAL)

**File**: `src/conversion/qlib_binary_writer.py`

**Issue**: Original implementation assumed dict input, but received DataFrame.

**Fix** (October 18, 2025):
```python
def _write_bin(self, symbol_df: Union[pl.DataFrame, Dict], code: str, _calendar):
    """
    Write binary data for a single symbol.

    CRITICAL: Handles both DataFrame AND dict input (legacy compatibility)
    """
    # Handle DataFrame input (current standard)
    if isinstance(symbol_df, pl.DataFrame):
        symbol_df = symbol_df.to_dict(as_series=False)

    # ... rest of implementation
```

**Key Points**:
- Always check input type (DataFrame vs dict)
- Qlib format requires dict with lists
- Polars DataFrames are primary data structure in pipeline

### 2. Date-First Partitioning (CRITICAL)

**Wrong** (ticker-first):
```
bronze/stocks_daily/ticker=AAPL/year=2025/month=09/data.parquet
```

**Correct** (date-first):
```
bronze/stocks_daily/year=2025/month=09/ticker=AAPL.parquet
```

**Why**:
- Time-range queries are most common use case
- Partition pruning eliminates 90%+ of files
- Better compression (similar dates compress better)
- DuckDB/Polars can skip entire year/month directories

### 3. Async HTTP/2 Client (Performance)

**File**: `src/download/polygon_rest_client.py`

**Key Optimizations**:
```python
async with PolygonRESTClient(
    api_key=credentials['api_key'],
    max_concurrent=50,        # 50 concurrent requests
    max_connections=100       # 100 HTTP/2 connections
) as client:
    # HTTP/2 multiplexing enables 50+ parallel requests per connection
```

**Performance**:
- 50+ concurrent requests via HTTP/2
- Automatic retries with exponential backoff
- Connection pooling and keepalive
- Result: 10-20x faster than sequential

### 4. Incremental Update Logic

**Implementation** (in CLI command):
1. Scan existing Parquet files in bronze layer
2. Extract date coverage from Hive partitions
3. Build set of existing dates
4. Filter requested date range to missing dates only
5. Download only missing dates

**Benefits**:
- No wasted API calls
- Idempotent (safe to rerun)
- Handles failures gracefully (just rerun)

### 5. Polars > Pandas

**Why Polars**:
- 10-100x faster than Pandas
- Lazy evaluation (query optimization)
- Better memory management
- Native Parquet support
- Arrow-compatible

**Key Pattern**:
```python
# Read Parquet (lazy)
df = pl.scan_parquet('bronze/stocks_daily/**/*.parquet')

# Filter (lazy, not executed yet)
df = df.filter(pl.col('date').is_between('2024-01-01', '2024-12-31'))

# Collect (execute optimized query)
result = df.collect()
```

---

## Common Workflows

### New Project Setup (Day 1-4)

```bash
# Day 1: Install and configure
git clone https://github.com/nittygritty-zzy/quantmini.git
cd quantmini
uv sync
cp config/credentials.yaml.example config/credentials.yaml
# Edit config/credentials.yaml with Polygon API key

# Day 1: Initial batch load (1 year)
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

### Daily Maintenance (Automated)

```bash
# Runs via cron at 6 AM daily
bash scripts/automation/orchestrate_daily_pipeline.sh

# What it does:
# 1. Download yesterday's data (stocks, options, news)
# 2. Run feature engineering on new data
# 3. Update Qlib binary format
# 4. Validate data quality
```

### Historical Backfill (Fill Gaps)

```bash
# Step 1: Detect gaps
uv run python scripts/validation/detect_data_gaps.py \
  --data-type stocks_daily \
  --start-date 2020-01-01 \
  --end-date 2025-10-18

# Step 2: Backfill year by year
for year in {2020..2024}; do
  uv run python -m src.cli.main data ingest -t stocks_daily \
    -s ${year}-01-01 -e ${year}-12-31 --incremental

  echo "Completed year: ${year}"
done

# Step 3: Verify completeness
uv run python scripts/validation/validate_data_completeness.py
```

---

## Testing

### Complete Pipeline Test

**Location**: `scripts/tests/run_complete_pipeline.py`

**What it does**:
- Downloads test data (5 tickers, 1 month)
- Processes through all layers (Bronze → Silver → Gold)
- Validates each layer
- Generates summary report

**Usage**:
```bash
# Clean test directory
rm -rf /Users/zheyuanzhao/workspace/quantmini/test_pipeline/*

# Run complete pipeline test
uv run python scripts/tests/run_complete_pipeline.py

# Check results
cat /Users/zheyuanzhao/workspace/quantmini/test_pipeline/PIPELINE_SUMMARY.md
```

**Recent Test Results** (September 2025):
- **Bronze**: 1,107 news articles (0.48 MB)
- **Silver**: 144 enriched records (0.01 MB)
- **Gold**: 15 binary files + 2 metadata (0.00 MB)
- **API Success Rate**: 100% (23/23 requests)

---

## Configuration Files

### Primary Config Files

**config/pipeline_config.yaml**:
- `data_root`: Primary data storage location
- `partition_strategy`: Date-first Hive partitioning
- `compression`: ZSTD (best compression ratio)

**config/credentials.yaml** (NOT in git):
```yaml
polygon:
  api_key: "YOUR_POLYGON_API_KEY"
  # NO S3 credentials needed (migrated to REST API)
```

**config/system_profile.yaml**:
- System-specific optimizations
- Memory limits
- Concurrency settings

### Environment Variables

```bash
export DATA_ROOT=/Volumes/sandisk/quantmini-lake
export POLYGON_API_KEY=your_api_key_here
```

---

## Dependencies & Environment

### Package Manager: uv (NOT pip)

**Why uv**:
- 10-100x faster than pip
- Better dependency resolution
- UV_LINK_MODE=copy for external drives

**Install dependencies**:
```bash
# Standard install
uv sync

# External drive (copy mode)
export UV_LINK_MODE=copy
uv sync
```

### Key Dependencies

**Core**:
- `polars` - DataFrame library (10-100x faster than pandas)
- `httpx` - HTTP/2 async client
- `pyarrow` - Parquet I/O
- `duckdb` - Query engine

**ML/Qlib**:
- `qlib` - Microsoft quantitative investment framework
- `numpy` - Numerical computing
- `scipy` - Scientific computing

**CLI**:
- `click` - Command-line interface
- `rich` - Terminal formatting
- `tqdm` - Progress bars

---

## File Naming Conventions

### Parquet Files (Bronze/Silver)

**Date-First Structure**:
```
{data_type}/year={YYYY}/month={MM}/ticker={SYMBOL}.parquet
```

**Examples**:
- `stocks_daily/year=2025/month=09/ticker=AAPL.parquet`
- `options_daily/year=2025/month=09/ticker=AAPL250117C00100000.parquet`
- `news/year=2025/month=09/ticker=AAPL.parquet`

### Binary Files (Gold/Qlib)

**Structure**:
```
gold/qlib/features/{ticker}/{feature}.bin
```

**Examples**:
- `gold/qlib/features/aapl/open.bin`
- `gold/qlib/features/aapl/close.bin`

---

## Documentation

### Key Documentation Files

1. **[docs/guides/data-ingestion-strategies.md](docs/guides/data-ingestion-strategies.md)**
   - Complete guide to initial load, incremental, backfill
   - 500+ lines, most comprehensive guide

2. **[docs/guides/batch-downloader.md](docs/guides/batch-downloader.md)**
   - Polygon REST API downloader guide
   - Performance optimization tips

3. **[docs/guides/data-loader.md](docs/guides/data-loader.md)**
   - Query bronze layer with DataLoader
   - DuckDB integration examples

4. **[scripts/README.md](scripts/README.md)**
   - Complete scripts reference
   - Organized by Medallion Architecture layer

### Documentation Standards

- Concise but complete
- Code examples for every feature
- Cross-references to related docs
- All code samples tested
- Updated October 18, 2025

---

## Important Notes

### DO NOT

1. ❌ Use ticker-first partitioning (use date-first)
2. ❌ Use pandas (use polars for 10-100x speedup)
3. ❌ Use S3 flat files (use REST API)
4. ❌ Use pip (use uv package manager)
5. ❌ Forget `--incremental` flag for daily updates
6. ❌ Skip validation after backfill

### ALWAYS

1. ✅ Use date-first Hive partitioning
2. ✅ Use `--incremental` flag for daily updates
3. ✅ Run validation scripts after backfill
4. ✅ Check today's date before time-sensitive commands
5. ✅ Use uv for all Python commands
6. ✅ Monitor background processes with BashOutput

### Critical Paths

**Data Root** (NEVER change without migration):
```
/Volumes/sandisk/quantmini-lake/
```

**Config Files** (MUST exist):
```
config/credentials.yaml
config/pipeline_config.yaml
config/system_profile.yaml
```

**Test Directory** (safe to delete):
```
/Users/zheyuanzhao/workspace/quantmini/test_pipeline/
```

---

## Recent Changes (October 18, 2025)

### Completed

1. ✅ **Qlib Binary Writer Fix** - Handle DataFrame input
2. ✅ **Test Pipeline Scripts** - Complete Bronze → Silver → Gold
3. ✅ **Documentation Update** - Data ingestion strategies guide
4. ✅ **Scripts Organization** - Medallion Architecture aligned
5. ✅ **September Test** - Validated 1-month pipeline (1,107 articles)

### Data Migration Status

- ✅ **Migrated from S3 to REST API** (complete)
- ✅ **Bronze layer** (770GB, excluding minute data)
- 🔄 **Silver layer** (partial, needs feature engineering)
- 🔄 **Gold layer** (partial, needs Qlib conversion)

### Active Background Processes

Multiple data ingestion processes running:
- `stocks_minute` (2020-2025)
- `options_minute` (2020-2025)
- `stocks_daily` (2020-2025)
- `options_daily` (2020-2025)

**Check status**:
```bash
# Monitor background process
uv run python scripts/validation/check_download_status.py
```

---

## Quick Reference

### Most Common Commands

```bash
# Daily update
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) -e $(date +%Y-%m-%d) --incremental

# Backfill gap
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s 2024-06-01 -e 2024-06-30 --incremental

# Feature engineering
uv run python scripts/transformation/transform_add_features.py

# Qlib conversion
uv run python scripts/conversion/convert_to_qlib_binary.py

# Validate
uv run python scripts/validation/validate_duckdb_access.py

# Test pipeline
uv run python scripts/tests/run_complete_pipeline.py
```

### Troubleshooting

**Rate limiting**:
```bash
uv run python -m src.cli.main data ingest -t stocks_daily \
  -s 2024-01-01 -e 2024-12-31 --max-concurrent 10
```

**Disk space**:
```bash
df -h /Volumes/sandisk/quantlake
du -sh /Volumes/sandisk/quantlake/*
```

**Check gaps**:
```bash
uv run python scripts/validation/detect_data_gaps.py --data-type stocks_daily
```

---

**End of Project Memory**
**Maintained by**: Claude Code Assistant
**Last Review**: October 18, 2025
