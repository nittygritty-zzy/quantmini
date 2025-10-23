# QuantMini Pipeline Operations Guide

**Comprehensive guide for running and optimizing the QuantMini data pipeline**

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Parallel Execution](#parallel-execution)
3. [Data Refresh Strategies](#data-refresh-strategies)
4. [Performance Optimization](#performance-optimization)
5. [Corporate Actions Architecture](#corporate-actions-architecture)
6. [Metadata Tracking](#metadata-tracking)
7. [Troubleshooting](#troubleshooting)

---

## 1. Quick Start

### Running Daily Updates

```bash
# Default: Process yesterday's data in parallel (5-10 minutes)
./scripts/daily_update_parallel.sh

# Backfill last 7 days
./scripts/daily_update_parallel.sh --days-back 7

# Sequential mode (for lower-spec hardware, 17-30 minutes)
./scripts/daily_update.sh --days-back 1
```

### Performance Comparison

| Mode | Duration | Use Case |
|------|----------|----------|
| **Parallel** | 5-10 min | 8+ cores, 32+ GB RAM, SSD |
| **Sequential (optimized)** | 17-30 min | 4+ cores, 16+ GB RAM |
| **Sequential (legacy)** | 55-105 min | <4 cores, <16 GB RAM |

---

## 2. Parallel Execution

### Parallelization Strategy

**Landing Layer (4 parallel S3 downloads):**
```
Job 1: Stocks Daily S3
Job 2: Stocks Minute S3
Job 3: Options Daily S3
Job 4: Options Minute S3
Time: ~2-3 minutes (vs 8-12 min sequential)
```

**Bronze Layer (11 parallel jobs):**
```
S3 Ingestion (4 jobs) + Polygon API Downloads (7 jobs)
├── Stocks Daily/Minute → Bronze
├── Options Daily/Minute → Bronze
├── Fundamentals (180-day window)
├── Corporate Actions (Dividends, Splits, IPOs)
│   ├── /v3/reference/dividends
│   ├── /v3/reference/splits
│   └── /vX/reference/ipos
├── Ticker Events (Symbol changes/rebranding)
│   └── /vX/reference/tickers/{id}/events
├── News
└── Short Interest/Volume (30-day window)
    ├── /stocks/v1/short-interest (2 year max)
    └── /stocks/v1/short-volume (all history)

Sequential after parallel:
└── Financial Ratios (depends on fundamentals)
└── Reference Data (Mondays only)

Time: ~2-4 minutes (vs 10-15 min sequential)
```

**Silver Layer (3 parallel jobs):**
```
Job 1: Financial Ratios → Silver
Job 2: Corporate Actions → Silver
Job 3: Fundamentals Flattening → Silver
Time: ~1-2 minutes (vs 3-5 min sequential)
```

**Gold Layer (Sequential):**
```
1. Enrich Stocks Daily
2. Convert to Qlib Binary
3. Enrich Stocks Minute
4. Enrich Options Daily
Time: ~1-2 minutes (feature dependencies require sequential)
```

### Usage Options

```bash
# Basic usage
./scripts/daily_update_parallel.sh

# Advanced options
./scripts/daily_update_parallel.sh \
  --date 2024-01-15 \
  --max-parallel 4 \
  --skip-landing \
  --skip-gold \
  --fundamental-tickers "AAPL MSFT GOOGL"

# Dry run (see execution plan)
./scripts/daily_update_parallel.sh --dry-run
```

### System Requirements

**Minimum:**
- CPU: 4 cores
- RAM: 16 GB
- Disk: Fast SSD
- Network: 100 Mbps

**Recommended:**
- CPU: 8+ cores
- RAM: 32 GB
- Disk: NVMe SSD
- Network: 500 Mbps+

---

## 3. Data Refresh Strategies

### Summary Table

| Data Type | Frequency | Lookback | Future Window | API Endpoint | API Calls/Week |
|-----------|-----------|----------|---------------|--------------|----------------|
| **Fundamentals** | Weekly | 180 days | N/A | (Balance sheets, Income, Cash flow) | ~100 |
| **Corporate Actions - Dividends** | Daily | 30 days | 90 days | `/v3/reference/dividends` | ~14 |
| **Corporate Actions - Splits** | Daily | 30 days | 90 days | `/v3/reference/splits` | ~14 |
| **Corporate Actions - IPOs** | Daily | 30 days | 90 days | `/vX/reference/ipos` | ~14 |
| **Ticker Events** | Weekly | All time | N/A | `/vX/reference/tickers/{id}/events` | ~50 |
| **Short Interest** | Weekly | 30 days | N/A | `/stocks/v1/short-interest` (2 year max) | ~2,000 |
| **Short Volume** | Weekly | 30 days | N/A | `/stocks/v1/short-volume` (all history) | ~2,000 |
| **Financial Ratios** | Weekly | Derived | N/A | (Calculated from fundamentals) | 0 |

### Fundamentals (Weekly)

**Recommended Refresh: Every Sunday at 2 AM**

```bash
# 180-day lookback captures last 2 quarters
quantmini polygon fundamentals $TICKERS \
  --timeframe quarterly \
  --filing-date-gte $(date -d '180 days ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals

# Calculate ratios immediately after
quantmini polygon financial-ratios $TICKERS \
  --input-dir $BRONZE_DIR/fundamentals \
  --output-dir $BRONZE_DIR/fundamentals \
  --include-growth
```

**Rationale:**
- Companies file 10-Q quarterly (~90 days)
- 180-day window captures amendments and late filings
- Earnings seasons: Late Jan, Apr, Jul, Oct
- Weekly refresh sufficient for quarterly data

### Corporate Actions (Daily)

**Recommended Refresh: Every day at 3 AM**

**API Endpoints:**
- Dividends: `/v3/reference/dividends`
- Splits: `/v3/reference/splits`
- IPOs: `/vX/reference/ipos`
- Ticker Changes: `/vX/reference/tickers/{id}/events`

```bash
# Historical (last 30 days) - Downloads Dividends, Splits, IPOs
quantmini polygon corporate-actions \
  --start-date $(date -d '30 days ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions

# Future events (next 90 days) - critical for dividend strategies!
quantmini polygon corporate-actions \
  --start-date $(date +%Y-%m-%d) \
  --end-date $(date -d '90 days' +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_future

# Ticker changes/rebranding (weekly recommended)
quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

**Rationale:**
- Dividends/splits announced unpredictably
- 30-day lookback captures recent changes and corrections
- 90-day future window captures announced dividends for strategies
- Ticker events capture symbol changes and rebranding
- Daily refresh ensures timely updates

**Monthly Full Backfill (1st of month, 1 AM):**
```bash
quantmini polygon corporate-actions \
  --start-date $(date -d '2 years ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions
```

### Short Interest & Volume (Weekly)

**Recommended Refresh: Every Monday at 4 AM**

**API Endpoints:**
- Short Interest: `/stocks/v1/short-interest` (2 year history max, bi-weekly)
- Short Volume: `/stocks/v1/short-volume` (all history, daily)

```bash
# ⚠️ IMPORTANT: API returns ALL tickers regardless of ticker parameter
# Downloads both short interest AND short volume in one command
quantmini polygon short-data $FUNDAMENTAL_TICKERS \
  --settlement-date-gte $(date -d '30 days ago' +%Y-%m-%d) \
  --date-gte $(date -d '30 days ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals
```

**Rationale:**
- Short interest: Updated bi-weekly (15th and end of month), 2 year max history
- Short volume: Daily venue-specific volume, all history available
- 30-day window captures 2 short interest reporting cycles
- Weekly refresh captures updates without daily overhead

**Performance:** 2-5 minutes with 30-day window (vs 30-60+ min without filtering)

### Weekly Schedule

**Sunday (2-4 AM):**
```bash
# 2:00 AM - Fundamentals (180-day window)
# 2:30 AM - Financial Ratios
# 3:00 AM - Ticker Events
# API calls: ~100
```

**Monday (4 AM):**
```bash
# 4:00 AM - Short Interest/Volume (30-day window)
# API calls: ~2,000 (paginated)
```

**Daily (3 AM):**
```bash
# 3:00 AM - Corporate Actions (30-day historical + 90-day future)
# API calls: ~2 per day = 14/week
```

**Monthly (1st of Month, 1 AM):**
```bash
# 1:00 AM - Full Corporate Actions Backfill (2 years)
# API calls: ~1
```

### Total API Usage

**Per Week:** ~2,114 calls (well within free tier: 5 calls/min)
**Per Month:** ~8,457 calls

---

## 4. Performance Optimization

### Date Filtering Optimization

**Impact: 3-4x faster (55-105 min → 17-30 min)**

All Polygon API calls now use date filtering to avoid downloading ALL historical data:

**Short Data (10-20x faster):**
```bash
# Before: Downloaded ~1.2M records
# After: 30-day window downloads ~50-100K records
# Duration: 30-60 min → 2-5 min

quantmini polygon short-data $TICKERS \
  --settlement-date-gte $(date -d '30 days ago' +%Y-%m-%d) \
  --date-gte $(date -d '30 days ago' +%Y-%m-%d)
```

**Fundamentals (5-10x faster):**
```bash
# Before: Downloaded ALL filings since 2000
# After: 180-day window downloads last 2 quarters
# Duration: 15-30 min → 3-5 min

quantmini polygon fundamentals $TICKERS \
  --timeframe quarterly \
  --filing-date-gte $(date -d '180 days ago' +%Y-%m-%d)
```

### Lookback Window Strategy

| Data Type | Daily Update | Aggressive | Rationale |
|-----------|--------------|------------|-----------|
| **Short Interest** | 30 days | 30 days | Bi-weekly reporting |
| **Short Volume** | 30 days | 30 days | Daily data, 30d sufficient |
| **Fundamentals (Quarterly)** | 180 days | 365 days | Catch amendments |
| **Fundamentals (Annual)** | 365 days | 365 days | Annual cycle |
| **Corporate Actions (Historical)** | 30 days | 90 days | Recent activity |
| **Corporate Actions (Future)** | 90 days | 180 days | Announced events |

### API Usage Impact

**Daily Pipeline API Calls:**

| Endpoint | Before | After | Reduction |
|----------|--------|-------|-----------|
| Short Interest | ~60,000 | ~100 | **99.8%** |
| Short Volume | ~1.2M | ~300 | **99.97%** |
| Fundamentals | ~50,000 | ~500 | **99%** |
| **Total** | ~1.3M | ~900 | **99.9%** |

Benefits even with unlimited API tier:
- Reduced server load
- Improved reliability
- Faster downloads
- Lower bandwidth costs

---

## 5. Corporate Actions Architecture

### Silver Layer Design

**Partitioning Structure:**
```
silver/corporate_actions/
├── ticker=ABBV/
│   ├── event_type=dividend/data.parquet
│   └── event_type=ticker_change/data.parquet
├── ticker=ABT/
│   └── event_type=dividend/data.parquet
└── ... (1,198+ tickers)
```

**Key Features:**
- **Ticker-first partitioning**: Optimizes stock screening (100x faster for single ticker)
- **Event-type sub-partitioning**: Filter without scanning irrelevant data
- **Unified schema**: All event types share common base + nullable type-specific fields
- **Derived features**: Pre-calculated annualized dividends, split flags, etc.

### Event Types Tracked

**1. Dividend Fields:** (API: `/v3/reference/dividends`)
- cash_amount, currency, declaration_date, ex_dividend_date
- record_date, pay_date, frequency, div_type
- **Derived:** annualized_amount, is_special, quarter

**2. Split Fields:** (API: `/v3/reference/splits`)
- execution_date, from, to, ratio
- **Derived:** is_reverse (ratio < 1.0)

**3. IPO Fields:** (API: `/vX/reference/ipos`)
- listing_date, issue_price, shares_offered, exchange, status

**4. Ticker Change Fields:** (API: `/vX/reference/tickers/{id}/events`)
- new_ticker, announcement_date, effective_date
- **Tracks:** Symbol changes, rebranding events

### Query Performance

| Query Type | Time | Files Read |
|------------|------|------------|
| Single ticker lookup | ~5-10ms | 1 file |
| Portfolio (10 tickers) | ~50-100ms | 10 files |
| Event-type scan | ~100-200ms | N files for event type |
| Full table scan | ~500ms-1s | All files |

**Example: Get ABBV dividend history**
```python
import polars as pl
from src.utils.paths import get_quantlake_root

silver_path = get_quantlake_root() / 'silver' / 'corporate_actions'

df = pl.scan_parquet(
    str(silver_path / 'ticker=ABBV' / 'event_type=dividend' / 'data.parquet')
).collect()

print(df.select(['event_date', 'div_cash_amount', 'div_annualized_amount']))
```

### Transformation Script

```bash
# Transform bronze → silver with metadata tracking
export QUANTLAKE_ROOT=/Users/zheyuanzhao/workspace/quantlake

# Transform all tickers
python scripts/transformation/corporate_actions_silver_optimized.py

# Transform specific tickers
python scripts/transformation/corporate_actions_silver_optimized.py --tickers AAPL MSFT
```

---

## 6. Metadata Tracking

### Layer-Based Architecture

Metadata is organized by Medallion Architecture layers:

```
metadata/
├── bronze/
│   ├── stocks_daily/
│   │   ├── watermark.json
│   │   └── 2025/10/2025-10-20.json
│   ├── fundamentals/
│   └── corporate_actions/
├── silver/
│   ├── corporate_actions/
│   ├── fundamentals/
│   └── financial_ratios/
└── gold/
    └── stocks_daily_qlib/
        ├── watermark.json
        └── 2025/10/2025-10-20.json
```

### Metadata Content

**Ingestion Record Example:**
```json
{
  "data_type": "stocks_daily",
  "date": "2025-10-20",
  "symbol": null,
  "status": "success",
  "layer": "bronze",
  "timestamp": "2025-10-21T11:33:46.123456",
  "statistics": {
    "records": 11782,
    "file_size_mb": 45.2,
    "processing_time_sec": 3.5
  },
  "error": null
}
```

**Watermark Example:**
```json
{
  "data_type": "stocks_daily",
  "symbol": null,
  "date": "2025-10-20",
  "timestamp": "2025-10-21T11:33:46.456789"
}
```

### Benefits

✅ **Incremental Processing**: Resume from last successful date
✅ **Gap Detection**: Identify missing dates for backfilling
✅ **Success Monitoring**: Track pipeline health and success rates
✅ **Error Tracking**: Review which dates failed and why
✅ **Statistics**: Monitor records processed, file sizes, times
✅ **Watermarks**: Know exactly what's been processed

### Viewing Metadata

```bash
# CLI display of all metadata
python -m src.storage.metadata_manager

# Example output:
# 📊 stocks_daily (Bronze):
#    Total jobs: 7
#    Success: 7, Skipped: 0, Failed: 0
#    Success rate: 100.0%
#    Records: 82,474
#    Size: 316.4 MB
#    Watermark: 2025-10-20
#
# 📊 stocks_daily_qlib (Gold):
#    Total jobs: 1
#    Success: 1, Skipped: 0, Failed: 0
#    Success rate: 100.0%
#    Symbols Converted: 11,782
#    Watermark: 2025-10-20

# Check specific date
cat /Users/zheyuanzhao/workspace/quantlake/metadata/gold/stocks_daily_qlib/2025/10/2025-10-20.json
```

---

## 7. Troubleshooting

### Parallel Jobs Failing Randomly

**Symptoms:** Some jobs fail intermittently

**Possible Causes:**
1. Insufficient memory
2. Network bandwidth saturation
3. API rate limiting

**Solutions:**
```bash
# Reduce max parallel jobs
./scripts/daily_update_parallel.sh --max-parallel 4

# Use sequential script
./scripts/daily_update.sh
```

### Slower Than Expected

**Symptoms:** Parallel script slower than sequential

**Possible Causes:**
1. Low CPU cores (<4)
2. Slow disk (HDD vs SSD)
3. Limited network bandwidth
4. High system load

**Solutions:**
```bash
# Check system load
top  # or htop

# Run during low-load periods
./scripts/daily_update_parallel.sh  # Run at night

# Use sequential for constrained systems
./scripts/daily_update.sh
```

### High Memory Usage

**Symptoms:** System runs out of memory

**Solutions:**
```bash
# Limit parallel jobs
./scripts/daily_update_parallel.sh --max-parallel 2

# Skip memory-intensive layers
./scripts/daily_update_parallel.sh --skip-landing --skip-bronze

# Use streaming mode
export PIPELINE_MODE=streaming
./scripts/daily_update.sh
```

### Disk I/O Bottleneck

**Symptoms:** Jobs queued waiting for disk writes

**Solutions:**
```bash
# Reduce parallel jobs
./scripts/daily_update_parallel.sh --max-parallel 4

# Use sequential for HDD
./scripts/daily_update.sh

# Consider SSD upgrade
```

### Metadata Not Recording

**Symptoms:** Empty metadata directory

**Check:**
```bash
# Verify metadata directory exists
ls -la /Users/zheyuanzhao/workspace/quantlake/metadata/

# Re-run ingestion (will skip existing, record metadata)
python scripts/ingestion/landing_to_bronze.py \
  --data-type stocks_daily \
  --start-date 2025-10-20 \
  --end-date 2025-10-20 \
  --no-incremental
```

### Schema Validation Errors

**Symptoms:** Parquet write failures with schema conflicts

**Solution:**
```bash
# Verify parquet.use_dictionary = false in config
cat config/pipeline_config.yaml | grep use_dictionary

# Check existing schema
python -c "
import pyarrow.parquet as pq
metadata = pq.read_metadata('data/bronze/stocks_daily/year=2024/month=01/day=01/part.parquet')
print(metadata.schema)
"
```

### API Rate Limit Errors

**Symptoms:** 429 Too Many Requests errors

**Solutions:**
```bash
# Check your API tier limits
# Free tier: 5 calls/min
# Starter: Unlimited

# Reduce parallel API downloads
./scripts/daily_update_parallel.sh --max-parallel 2

# Use longer date windows (fewer API calls)
# Already optimized with date filtering
```

---

## Best Practices

### 1. Choose Right Script for Your Hardware

| Hardware | Script | Performance |
|----------|--------|-------------|
| **8+ cores, 32 GB, NVMe SSD** | parallel | 5-7 min |
| **4-8 cores, 16 GB, SSD** | parallel | 7-10 min |
| **2-4 cores, 8 GB, HDD** | sequential | 17-30 min |

### 2. Monitor First Few Runs

```bash
# Watch logs in real-time
tail -f logs/daily_update_parallel_*.log

# Check system resources
htop  # or top

# Verify data integrity
ls -lh ~/workspace/quantlake/bronze/fundamentals/**/*.parquet
```

### 3. Production Deployment

**Recommended cron setup:**
```bash
# Daily at 2 AM: Fast parallel execution
0 2 * * * /path/to/quantmini/scripts/daily_update_parallel.sh

# Weekly at 3 AM Sunday: Full backfill for safety
0 3 * * 0 /path/to/quantmini/scripts/daily_update.sh --days-back 7
```

### 4. Incremental Updates

Use watermarks for efficient processing:
```python
from src.storage.metadata_manager import MetadataManager

metadata = MetadataManager(metadata_root)

# Get last processed date
last_date = metadata.get_watermark('stocks_daily', layer='bronze')

# Process only new dates
start_date = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
```

---

## Quick Reference

### Common Commands

```bash
# Daily update (parallel, default: yesterday)
./scripts/daily_update_parallel.sh

# 7-day backfill (parallel)
./scripts/daily_update_parallel.sh --days-back 7

# Daily update (sequential, all layers)
./scripts/daily_update.sh --days-back 1

# View metadata
python -m src.storage.metadata_manager

# Transform corporate actions to silver
python scripts/transformation/corporate_actions_silver_optimized.py

# Check pipeline configuration
quantmini config show
```

### Performance Targets

| Pipeline | Target Duration | Bottleneck |
|----------|----------------|------------|
| Landing (parallel) | 2-3 min | S3 download speed |
| Bronze (parallel) | 2-4 min | Short data API |
| Silver (parallel) | 1-2 min | Transformation compute |
| Gold (sequential) | 1-2 min | Feature dependencies |
| **Total (parallel)** | **5-10 min** | System resources |
| **Total (sequential)** | **17-30 min** | Processing mode |

### Data Quality Metrics

Monitor these key metrics:

1. **Freshness**: Days since latest data (alert if >14 days)
2. **Coverage**: % of tickers with data (alert if <95%)
3. **Success Rate**: Successful vs failed jobs (alert if <95%)
4. **Record Counts**: Anomalies in records added (0 or huge spikes)

---

**Last Updated:** 2025-10-21
**Version:** 2.0 (Consolidated from 6 operational docs)
**Status:** Production Ready
