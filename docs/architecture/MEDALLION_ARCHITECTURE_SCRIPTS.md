# Medallion Architecture - Script Reference

**Date**: October 18, 2025
**Status**: All scripts updated to use production Medallion paths with source-based landing structure

---

## Quick Summary

### Key Changes
- ✅ **Source-Based Landing Structure**: Landing layer now organized by data source (`polygon-s3/`, `polygon-api/`, `external/`)
- ✅ **Path Alignment**: All scripts (`download_to_landing.py`, `landing_to_bronze.py`, etc.) updated to use correct paths
- ⚠️ **Options Data Limitation**: S3 flat file access limited to 2 years (2023-10-18 to present)
- ✅ **Stocks Data**: Full 5-year access (2020-10-18 to present)

### Landing Layer Structure
```
/Volumes/sandisk/quantmini-lake/landing/
├── polygon-s3/          # S3 flat files (time-series data)
│   ├── stocks_daily/
│   ├── stocks_minute/
│   ├── options_daily/   # 2-year limit
│   └── options_minute/  # 2-year limit
├── polygon-api/         # REST API data (direct to bronze)
│   ├── reference_data/
│   ├── fundamentals/
│   ├── corporate_actions/
│   └── news/
└── external/            # External sources
    └── delisted_stocks/
```

---

## Overview

The QuantMini data pipeline follows the **Medallion Architecture** pattern with four layers:

```
Landing (Raw) → Bronze (Validated) → Silver (Enriched) → Gold (Production)
```

All scripts now use centralized path configuration from `config/paths.yaml`:
- **Production**: `/Volumes/sandisk/quantmini-lake/`
- **Test**: `~/workspace/quantmini/data/test-lake/`

---

## Layer-by-Layer Script Guide

### 1. Landing Layer (Raw Files)

**Purpose**: Store raw CSV.GZ files from Polygon S3 for archival and audit

**Scripts**:

#### `scripts/ingestion/download_to_landing.py` ✅
**Data Flow**: `Polygon S3 → Landing`

```bash
# Download all data types to landing
python scripts/ingestion/download_to_landing.py \
    --data-type all \
    --start-date 2020-10-18 \
    --end-date 2025-10-18
```

**Features**:
- Downloads raw CSV.GZ files (keeps compressed format)
- Date-based partitioning: `landing/{type}/{year}/{month}/{file}.csv.gz`
- Skips existing files (idempotent)
- No processing - pure archival storage

**Output**:
```
/Volumes/sandisk/quantmini-lake/landing/
├── polygon-s3/
│   ├── stocks_daily/
│   │   ├── 2020/10/2020-10-19.csv.gz
│   │   ├── 2020/10/2020-10-20.csv.gz
│   │   └── ...
│   ├── stocks_minute/
│   ├── options_daily/
│   └── options_minute/
├── polygon-api/
│   ├── corporate_actions/
│   ├── fundamentals/
│   ├── news/
│   └── reference/
└── external/
    └── delisted_stocks/
```

---

### 2. Bronze Layer (Validated Parquet)

**Purpose**: Validated, schema-enforced Parquet data

**Scripts**:

#### `scripts/ingestion/landing_to_bronze.py` ✅ **NEW**
**Data Flow**: `Landing → Bronze`

```bash
# Process landing files to bronze (proper Medallion flow)
python scripts/ingestion/landing_to_bronze.py \
    --data-type all \
    --start-date 2020-10-18 \
    --end-date 2025-10-18
```

**Features**:
- Reads from local landing files (no internet required)
- Decompresses CSV.GZ and validates schema
- Writes to Parquet with ZSTD compression
- Incremental processing with watermarks
- **This is the recommended way to populate bronze!**

**Path Configuration**:
- Reads from: `config.get('data_lake_root')/landing/`
- Writes to: `config.get_bronze_path()`

#### `src/orchestration/ingestion_orchestrator.py` ✅
**Data Flow**: `Polygon S3 → Bronze` (legacy, direct download)

**Note**: This still downloads from S3. For proper Medallion flow, use `landing_to_bronze.py` instead.

#### Bronze Layer Scripts for REST API Data ✅ **NEW**

For REST API data (reference_data, fundamentals, corporate_actions), we use a **direct-to-bronze** approach (no landing layer). This is appropriate because:
- API responses can be re-fetched anytime (unlike expensive batch S3 downloads)
- Parquet format is the "source of truth" for these data types
- More efficient storage without duplicate JSON/CSV archives

**Scripts**:

##### `scripts/download/download_reference_data.py` ✅
**Data Flow**: `Polygon REST API → Bronze`

```bash
# Download ticker metadata and details
python scripts/download/download_reference_data.py \
    --tickers AAPL,MSFT,GOOGL \
    --all

# Download only related tickers for many symbols
python scripts/download/download_reference_data.py \
    --tickers-file tickers.txt \
    --related-tickers-only
```

**Features**:
- Downloads ticker types, ticker details, related tickers
- Parallel batch downloads (100 concurrent requests)
- Ticker-partitioned structure: `reference_data/related_tickers/ticker=AAPL.parquet`
- ZSTD compression

##### `scripts/download/download_fundamentals.py` ✅
**Data Flow**: `Polygon REST API → Bronze`

```bash
# Download all financials for tickers
python scripts/download/download_fundamentals.py \
    --tickers AAPL,MSFT,GOOGL \
    --timeframe quarterly \
    --all

# Download with short data
python scripts/download/download_fundamentals.py \
    --tickers-file tickers.txt \
    --timeframe annual \
    --include-short-data
```

**Features**:
- Downloads balance sheets, cash flow, income statements, short interest, short volume
- Date-first partitioning: `fundamentals/balance_sheets/year=2024/month=10/ticker=AAPL.parquet`
- Parallel downloads with full pagination
- ZSTD compression

##### `scripts/download/download_corporate_actions.py` ✅
**Data Flow**: `Polygon REST API → Bronze`

```bash
# Download all corporate actions for tickers
python scripts/download/download_corporate_actions.py \
    --tickers AAPL,MSFT,GOOGL \
    --start-date 2020-01-01 \
    --all

# Download specific types
python scripts/download/download_corporate_actions.py \
    --tickers-file tickers.txt \
    --dividends-only \
    --start-date 2024-01-01
```

**Features**:
- Downloads dividends, stock splits, IPOs, ticker events
- Date-first partitioning: `corporate_actions/dividends/year=2024/month=10/ticker=AAPL.parquet`
- Flexible date filtering (gte/lte)
- ZSTD compression

**Output**:
```
/Volumes/sandisk/quantmini-lake/bronze/
├── stocks_daily/
│   ├── year=2020/month=10/data.parquet
│   └── ...
├── stocks_minute/
├── options_daily/
├── options_minute/
├── reference_data/
│   ├── ticker_types_*.parquet
│   ├── ticker_details_*.parquet
│   └── related_tickers/
│       ├── ticker=AAPL.parquet
│       └── ticker=MSFT.parquet
├── fundamentals/
│   ├── balance_sheets/year=2024/month=10/ticker=AAPL.parquet
│   ├── cash_flow/year=2024/month=10/ticker=AAPL.parquet
│   ├── income_statements/year=2024/month=10/ticker=AAPL.parquet
│   ├── short_interest/year=2024/month=10/ticker=AAPL.parquet
│   └── short_volume/year=2024/month=10/ticker=AAPL.parquet
└── corporate_actions/
    ├── dividends/year=2024/month=10/ticker=AAPL.parquet
    ├── splits/year=2024/month=10/ticker=AAPL.parquet
    ├── ipos/year=2024/month=10/ticker=AAPL.parquet
    └── ticker_events/year=2024/month=10/ticker=AAPL.parquet
```

---

### 3. Silver Layer (Enriched Features)

**Purpose**: Feature-engineered data with technical indicators

**Scripts**:

#### `scripts/transformation/transform_add_features.py` ✅
**Data Flow**: `Bronze → Silver`

```bash
# Add features to bronze data
python scripts/transformation/transform_add_features.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18
```

**Features**:
- Reads validated Parquet from bronze
- Adds technical indicators (SMA, EMA, RSI, etc.)
- Data quality checks
- Writes enriched Parquet to silver

**Path Configuration**:
- Reads from: `config.get_bronze_path()`
- Writes to: `config.get_silver_path()`

#### `scripts/enrich_features.py` ✅
**Data Flow**: `Bronze → Silver`

```bash
# Alternative enrichment script
python scripts/enrich_features.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18 \
    --sequential  # For large date ranges
```

**Path Configuration**:
- Reads from: `config.get_bronze_path()`
- Writes to: `config.get_silver_path()`

**Output**:
```
/Volumes/sandisk/quantmini-lake/silver/
├── stocks_daily/
│   ├── year=2020/month=10/data.parquet
│   └── ... (with additional feature columns)
├── stocks_minute/
├── options_daily/
└── options_minute/
```

---

### 4. Gold Layer (Production ML Formats)

**Purpose**: ML-ready binary formats optimized for training

**Scripts**:

#### `scripts/conversion/convert_to_qlib_binary.py` ✅
**Data Flow**: `Silver → Gold (Qlib)`

```bash
# Convert to Qlib binary format
python scripts/conversion/convert_to_qlib_binary.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18
```

**Features**:
- Reads enriched Parquet from silver
- Converts to Qlib binary format
- Optimized for fast ML training
- Incremental conversion

**Path Configuration**:
- Reads from: `config.get_silver_path()`
- Writes to: `config.get_gold_path() / 'qlib'`

**Output**:
```
/Volumes/sandisk/quantmini-lake/gold/
└── qlib/
    └── stocks_daily/
        ├── features/
        │   ├── open.bin
        │   ├── close.bin
        │   └── ...
        └── calendars/
            └── day.txt
```

---

## Complete Pipeline Workflow

### Recommended Flow (Proper Medallion Architecture)

#### Stocks Data (5-year history: 2020-10-18 to present)

```bash
# Step 1: Download raw files to landing (once, archival)
python scripts/ingestion/download_to_landing.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18

python scripts/ingestion/download_to_landing.py \
    --data-type stocks_minute \
    --start-date 2020-10-18 \
    --end-date 2025-10-18

# Step 2: Process landing → bronze (repeatable, no internet)
python scripts/ingestion/landing_to_bronze.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18

python scripts/ingestion/landing_to_bronze.py \
    --data-type stocks_minute \
    --start-date 2020-10-18 \
    --end-date 2025-10-18

# Step 3: Enrich bronze → silver
python scripts/transformation/transform_add_features.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18

# Step 4: Convert silver → gold
python scripts/conversion/convert_to_qlib_binary.py \
    --data-type stocks_daily \
    --start-date 2020-10-18 \
    --end-date 2025-10-18
```

#### Options Data (2-year history: 2023-10-18 to present)

```bash
# Step 1: Download raw files to landing (2-year limit)
python scripts/ingestion/download_to_landing.py \
    --data-type options_daily \
    --start-date 2023-10-18 \
    --end-date 2025-10-18

python scripts/ingestion/download_to_landing.py \
    --data-type options_minute \
    --start-date 2023-10-18 \
    --end-date 2025-10-18

# Step 2: Process landing → bronze
python scripts/ingestion/landing_to_bronze.py \
    --data-type options_daily \
    --start-date 2023-10-18 \
    --end-date 2025-10-18

python scripts/ingestion/landing_to_bronze.py \
    --data-type options_minute \
    --start-date 2023-10-18 \
    --end-date 2025-10-18

# Step 3: Enrich bronze → silver
python scripts/transformation/transform_add_features.py \
    --data-type options_daily \
    --start-date 2023-10-18 \
    --end-date 2025-10-18

# Note: Options data not supported in Qlib format (Gold layer)
```

#### Reference Data (REST API → Bronze directly)

```bash
# Download reference data (tickers file with all symbols)
python scripts/download/download_reference_data.py \
    --tickers-file tickers.txt \
    --all

# Download fundamentals (all financial statements)
python scripts/download/download_fundamentals.py \
    --tickers-file tickers.txt \
    --timeframe quarterly \
    --all \
    --include-short-data

# Download corporate actions (dividends, splits, IPOs)
python scripts/download/download_corporate_actions.py \
    --tickers-file tickers.txt \
    --start-date 2020-01-01 \
    --all

# Note: Reference data goes directly to bronze (no landing layer)
# Silver/Gold processing for reference data TBD
```

### Legacy Flow (Direct S3 → Bronze)

```bash
# Old way: Direct download to bronze (skips landing layer)
python -m src.cli.main data ingest \
    -t stocks_daily \
    -s 2020-10-18 \
    -e 2025-10-18 \
    --incremental
```

**Note**: This works but doesn't follow proper Medallion Architecture. Use landing → bronze flow instead.

---

## Path Configuration Summary

All scripts use centralized configuration from `config/paths.yaml`:

| Layer | Config Method | Production Path | Test Path |
|-------|--------------|-----------------|-----------|
| Landing | `data_lake_root + '/landing/polygon-s3'` | `/Volumes/sandisk/quantmini-lake/landing/polygon-s3/` | `~/workspace/quantmini/data/test-lake/landing/polygon-s3/` |
| Bronze | `config.get_bronze_path()` | `/Volumes/sandisk/quantmini-lake/bronze/` | `~/workspace/quantmini/data/test-lake/bronze/` |
| Silver | `config.get_silver_path()` | `/Volumes/sandisk/quantmini-lake/silver/` | `~/workspace/quantmini/data/test-lake/silver/` |
| Gold | `config.get_gold_path()` | `/Volumes/sandisk/quantmini-lake/gold/` | `~/workspace/quantmini/data/test-lake/gold/` |
| Metadata | `config.get_metadata_path()` | `/Volumes/sandisk/quantmini-lake/metadata/` | `~/workspace/quantmini/data/test-lake/metadata/` |

---

## Environment Switching

To switch between production and test:

```yaml
# Edit config/paths.yaml
active_environment: production  # or 'test'
```

That's it! All scripts automatically use the correct paths.

---

## Data Access Limitations

### Stocks Data (S3 Flat Files)
- **Access**: Full 5-year history available
- **Date Range**: 2020-10-18 to present
- **Data Types**: stocks_daily, stocks_minute

### Options Data (S3 Flat Files)
- **Access**: Limited to 2-year history
- **Date Range**: 2023-10-18 to present (403 Forbidden for older dates)
- **Data Types**: options_daily, options_minute
- **Note**: Historical options data before Oct 2023 not accessible via S3 flat files

### REST API Data
- **Access**: Full historical access based on subscription tier
- **Data Types**: reference_data, fundamentals, corporate_actions, news
- **Note**: No landing layer - goes directly to Bronze

---

## Benefits of Medallion Architecture

1. **Audit Trail**: Landing layer keeps raw source files
2. **Repeatability**: Can reprocess bronze from landing anytime
3. **Faster Development**: No S3 downloads for testing
4. **Cost Savings**: Fewer API calls to Polygon
5. **Clear Separation**: Each layer has a specific purpose
6. **Easy Debugging**: Can inspect data at each stage
7. **Incremental Processing**: Watermarks prevent duplicate work

---

## Data Format by Layer

| Layer | Format | Compression | Purpose |
|-------|--------|-------------|---------|
| Landing | CSV.GZ | Gzip | Raw archival |
| Bronze | Parquet | ZSTD (level 3) | Validated storage |
| Silver | Parquet | ZSTD (level 3) | Feature-rich analytics |
| Gold | Binary (Qlib) | Custom | ML training |

---

## Script Status

### Time-Series Data (S3 Flat Files)

| Script | Layer Flow | Paths Updated | Status |
|--------|-----------|---------------|---------|
| `download_to_landing.py` | Polygon S3 → Landing | ✅ | NEW |
| `landing_to_bronze.py` | Landing → Bronze | ✅ | NEW (recommended) |
| `ingestion_orchestrator.py` | Polygon S3 → Bronze | ✅ | Legacy (direct download) |
| `transform_add_features.py` | Bronze → Silver | ✅ | Updated |
| `enrich_features.py` | Bronze → Silver | ✅ | Updated |
| `convert_to_qlib_binary.py` | Silver → Gold | ✅ | Updated |

### REST API Data (Direct to Bronze)

| Script | Layer Flow | Paths Updated | Status |
|--------|-----------|---------------|---------|
| `download_reference_data.py` | REST API → Bronze | ✅ | NEW |
| `download_fundamentals.py` | REST API → Bronze | ✅ | NEW |
| `download_corporate_actions.py` | REST API → Bronze | ✅ | NEW |

---

## Next Steps

1. **Populate Landing**: Run `download_to_landing.py` for all data types (5 years)
2. **Process to Bronze**: Run `landing_to_bronze.py` to create validated Parquet
3. **Enrich to Silver**: Run feature engineering scripts
4. **Convert to Gold**: Generate Qlib binary for ML training

---

**Last Updated**: October 18, 2025
