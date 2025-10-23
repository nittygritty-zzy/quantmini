# Historical Data Load Plan

**Created:** 2025-10-21
**Purpose:** Load maximum available historical data from Polygon API

---

## Executive Summary

Based on API testing, here's the comprehensive plan to load all available historical data:

| Data Type | Available History | Recommended Load | Estimated Size | Est. Time |
|-----------|-------------------|------------------|----------------|-----------|
| **Fundamentals** | 2010-present (15+ years) | 15 years | ~50 MB | 2-3 hours |
| **Corporate Actions** | 2000-present (25+ years) | 20 years | ~100 MB | 1-2 hours |
| **Price Data (S3 Daily)** | Full history available | 10 years | ~10 GB | 2-3 hours |
| **Price Data (S3 Minute)** | Full history available | 5 years | ~500 GB | 10-15 hours |
| **Short Interest/Volume** | Limited history | 2 years | ~50 MB | 30 min |
| **News** | 5+ years available | 3 years | ~500 MB | 1-2 hours |

**Total Estimated Time (without minute data):** 6-10 hours
**Total Estimated Storage:** ~15 GB

**With minute data:** 16-25 hours, ~515 GB storage

---

## Detailed Plan by Data Type

### 1. Fundamentals (Highest Priority)

**Data Available:** 2010-01-01 to present (15+ years)

**API Endpoints:**
- Balance Sheets
- Income Statements
- Cash Flow Statements

**Recommended Load Strategy:**

```bash
# Option 1: Load all 50 tickers, 15 years
# Estimated: 50 tickers × 15 years × 4 quarters = 3,000 filings
# Time: ~2-3 hours
START_DATE="2010-01-01"
END_DATE=$(date +%Y-%m-%d)

quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
  --timeframe quarterly \
  --filing-date-gte $START_DATE \
  --output-dir $BRONZE_DIR/fundamentals

# Option 2: Split by year to avoid timeouts (recommended)
for YEAR in {2010..2025}; do
  echo "Processing year $YEAR..."
  quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
    --timeframe quarterly \
    --filing-date-gte ${YEAR}-01-01 \
    --filing-date-lt $((YEAR+1))-01-01 \
    --output-dir $BRONZE_DIR/fundamentals
done
```

**API Usage:** ~50 calls/year × 15 years = 750 calls

---

### 2. Corporate Actions (High Priority)

**Data Available:** 2000-01-01 to present (25+ years)

**Types:**
- Dividends (most common) - `/v3/reference/dividends`
- Stock Splits - `/v3/reference/splits`
- IPOs - `/vX/reference/ipos`
- Ticker Changes - `/vX/reference/tickers/{id}/events`

**Recommended Load Strategy:**

```bash
# Load 20 years of corporate actions (dividends, splits, IPOs)
# Recommended: Split into 5-year chunks to avoid timeouts

START_YEARS=(2005 2010 2015 2020)

for START_YEAR in "${START_YEARS[@]}"; do
  END_YEAR=$((START_YEAR + 5))
  echo "Processing $START_YEAR to $END_YEAR..."

  quantmini polygon corporate-actions \
    --start-date ${START_YEAR}-01-01 \
    --end-date ${END_YEAR}-12-31 \
    --include-ipos \
    --output-dir $BRONZE_DIR/corporate_actions
done

# Load ticker changes (symbol rebranding/changes) for all tickers
quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

# Load most recent 5 years
quantmini polygon corporate-actions \
  --start-date 2020-01-01 \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions
```

**API Usage:** ~1 call per 5-year period = ~5 calls total

**Note:** Corporate actions API returns ALL tickers, so one call covers entire market.

---

### 3. Price Data - Daily (S3) (High Priority)

**Data Available:** Full history from Polygon S3

**Recommended Load Strategy:**

```bash
# Load 10 years of daily price data
# Estimated size: ~10 GB
# Time: 2-3 hours (depending on network speed)

# Download from S3 (Landing layer)
for YEAR in {2015..2025}; do
  for MONTH in {01..12}; do
    echo "Downloading $YEAR-$MONTH..."
    quantmini data download \
      --data-type stocks_daily \
      --start-date ${YEAR}-${MONTH}-01 \
      --end-date ${YEAR}-${MONTH}-31
  done
done

# Ingest to Bronze layer (parallel recommended)
python scripts/ingestion/landing_to_bronze.py \
  --data-type stocks_daily \
  --start-date 2015-01-01 \
  --end-date $(date +%Y-%m-%d) \
  --processing-mode batch
```

**Storage:** ~1 GB/year × 10 years = ~10 GB

---

### 4. Price Data - Minute (S3) (Optional - Large Storage)

**Data Available:** Full history from Polygon S3

**⚠️ WARNING: Very large dataset!**
- Estimated: ~100 GB per year
- 5 years = ~500 GB storage required
- Download time: 10-15 hours

**Recommended Load Strategy:**

```bash
# Only load if you have sufficient storage and time!
# Recommended: Start with 1 year, then expand

# Load 1 year of minute data (for high-frequency backtesting)
YEAR=2024
quantmini data download \
  --data-type stocks_minute \
  --start-date ${YEAR}-01-01 \
  --end-date ${YEAR}-12-31
```

**Alternative:** Load only specific symbols for minute data to reduce storage:

```bash
# Load minute data for select high-volume tickers only
# This reduces storage by 90%+
HIGH_VOLUME_TICKERS="AAPL MSFT GOOGL AMZN NVDA TSLA SPY QQQ"

# Modify S3 download script to filter by ticker
# (Custom implementation required)
```

---

### 5. Short Interest & Short Volume (Medium Priority)

**Data Available:**
- Short Interest: 2 years (bi-weekly publications) - `/stocks/v1/short-interest`
- Short Volume: All history available - `/stocks/v1/short-volume`

**Recommended Load Strategy:**

```bash
# Load 2 years of short interest + short volume
# Both endpoints downloaded via single command
# Note: Short interest limited to 2 years, but short volume has full history

quantmini polygon short-data $FUNDAMENTAL_TICKERS \
  --settlement-date-gte $(date -d '2 years ago' +%Y-%m-%d) \
  --date-gte $(date -d '2 years ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals
```

**API Usage:** ~100 calls (50 tickers × 2 endpoints)
**Time:** ~30 minutes

**What Gets Downloaded:**
- Short Interest: Bi-weekly settlement data (last 2 years max)
- Short Volume: Daily venue-specific volume (all history available)

---

### 6. News (Optional - Medium Priority)

**Data Available:** 5+ years

**Recommended Load Strategy:**

```bash
# Load 3 years of news for sentiment analysis
# Split by ticker to avoid timeouts

for TICKER in $FUNDAMENTAL_TICKERS; do
  echo "Downloading news for $TICKER..."
  quantmini polygon news $TICKER \
    --published-gte $(date -d '3 years ago' +%Y-%m-%d) \
    --output-dir $BRONZE_DIR/news
  sleep 1  # Rate limiting
done
```

**API Usage:** ~50 tickers × 1 call each = 50 calls
**Time:** ~1-2 hours

---

## Recommended Execution Order

### Phase 1: Critical Data (Run First)
1. **Corporate Actions** (1-2 hours, small size)
   - Needed for price adjustments
   - Small API footprint

2. **Fundamentals** (2-3 hours, small size)
   - Core financial data
   - Required for factor analysis

3. **Daily Price Data** (2-3 hours, 10 GB)
   - Most important for backtesting
   - Moderate size

**Phase 1 Total:** 5-8 hours, ~10 GB

### Phase 2: Extended Data (Optional)
4. **Short Interest/Volume** (30 min, 50 MB)
   - Useful for short analysis
   - Quick to download

5. **News** (1-2 hours, 500 MB)
   - For sentiment analysis
   - Can skip if not needed

**Phase 2 Total:** 1.5-2.5 hours, ~550 MB

### Phase 3: Minute Data (Only if Needed)
6. **Minute Price Data** (10-15 hours, 500 GB)
   - Only for intraday strategies
   - Requires significant storage

---

## Storage Requirements

### Minimum Configuration (Daily Data Only)
- Bronze: ~10 GB
- Silver: ~2 GB
- Gold (Qlib): ~5 GB
- **Total: ~17 GB**

### Full Configuration (with 5 years minute data)
- Bronze: ~510 GB
- Silver: ~102 GB
- Gold (Qlib): ~205 GB
- **Total: ~817 GB**

### Recommended Configuration (Daily + 1 year minute)
- Bronze: ~110 GB
- Silver: ~22 GB
- Gold (Qlib): ~55 GB
- **Total: ~187 GB**

---

## API Rate Limits & Optimization

### Free Tier
- 5 calls/minute
- **Estimated total time with rate limiting:** 8-12 hours for Phase 1

### Unlimited Tier
- No rate limits
- **Estimated total time:** 5-8 hours for Phase 1
- **Recommended for historical load**

### Optimization Tips

1. **Use year-based chunking** to avoid timeouts:
   ```bash
   for YEAR in {2010..2025}; do
     # Process one year at a time
   done
   ```

2. **Run downloads in parallel** (if unlimited tier):
   ```bash
   # Run fundamentals, corporate actions, and price downloads simultaneously
   quantmini polygon fundamentals ... &
   quantmini polygon corporate-actions ... &
   quantmini data download ... &
   wait
   ```

3. **Use incremental loading** to resume from failures:
   - Check metadata for last successful date
   - Resume from that point

4. **Monitor progress** with metadata tracking:
   ```bash
   python -m src.storage.metadata_manager
   ```

---

## Execution Script Template

I'll create a comprehensive bash script that:
1. Checks available storage
2. Prompts for data selection
3. Shows estimated time/storage
4. Executes load in phases
5. Validates completion
6. Generates summary report

See: `scripts/historical_data_load.sh` (to be created)

---

## Data Quality Considerations

### Fundamentals
- Data quality improves after 2010
- Some companies may have missing quarters before 2015
- Recent data (last 2 years) is most complete

### Corporate Actions
- Historical data very complete
- Dividend data reliable back to 2000
- IPO data may be less complete before 2010

### Price Data
- S3 data very reliable
- Full tick history available
- Survivorship bias: Only active/recently delisted stocks

### Short Data
- Limited historical availability
- Bi-weekly reporting (not daily historical)
- Best quality for last 2 years

---

## Next Steps

1. Review this plan and confirm data requirements
2. Decide on scope (daily only vs. daily + minute)
3. Run Phase 1 (critical data) first
4. Validate data quality before proceeding to Phase 2
5. Optionally add minute data in Phase 3

---

**Questions to Answer Before Starting:**

1. ✅ Do you need minute data? (Adds 10-15 hours, 500 GB)
2. ✅ How many years of daily data? (Recommended: 10 years)
3. ✅ Do you need news data? (Adds 1-2 hours, 500 MB)
4. ✅ Do you have Unlimited API tier? (Saves 3-4 hours)
5. ✅ Available storage space? (Minimum 20 GB, recommended 200 GB)

**Ready to proceed?** Let me know your answers and I'll create the execution script!
