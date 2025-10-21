# Data Refresh Strategies for Fundamentals and Corporate Actions

**Date:** 2025-10-21
**Purpose:** Optimal refresh frequencies and date ranges for bronze layer data sources

---

## Executive Summary

Based on analysis of Polygon API characteristics and the `daily_update.sh` script, here are the recommended refresh strategies:

| Data Type | Current Frequency | Recommended Frequency | Lookback | Future Window | Rationale |
|-----------|-------------------|----------------------|----------|---------------|-----------|
| **Fundamentals** | On-demand | **Weekly** | 180 days (6 months) | N/A | Quarterly filings, predictable schedule |
| **Corporate Actions** | Daily (7-day backfill) | **Daily** | 30 days | 90 days | Announcements anytime, need future events |
| **Short Interest/Volume** | On-demand | **Weekly** | Full dataset | N/A | Bi-weekly updates, bulk download required |
| **Ticker Events** | On-demand | **Weekly** | All time | N/A | Rare changes, per-ticker API calls |
| **Financial Ratios** | On-demand | **Weekly** | Derived from fundamentals | N/A | Calculated, not downloaded |

---

## 1. Fundamentals Data

### Current Implementation (from daily_update.sh)
```bash
# Step 5: Fundamentals (Polygon REST API)
quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
  --timeframe quarterly \
  --output-dir $BRONZE_DIR/fundamentals
```

### Data Types Included:
1. **Balance Sheets** (`/vX/reference/financials`)
   - Assets, Liabilities, Equity
   - Quarterly and Annual filings

2. **Income Statements** (`/vX/reference/financials`)
   - Revenue, Expenses, Net Income
   - Quarterly and Annual filings

3. **Cash Flow Statements** (`/vX/reference/financials`)
   - Operating, Investing, Financing cash flows
   - Quarterly and Annual filings

### Recommended Refresh Strategy

**Frequency:** Weekly (Every Sunday at 2 AM)

**Rationale:**
- Companies file 10-Q (quarterly) and 10-K (annual) reports on predictable schedules
- Most filings occur within 45 days of quarter-end
- Earnings seasons: Late Jan, Late Apr, Late Jul, Late Oct
- Weekly refresh captures all new filings without excessive API usage

**Date Range:**
- **Lookback:** 180 days (6 months)
  - Captures last 2 quarters completely
  - Accounts for late amendments and restatements
  - Ensures no gaps in data

**Optimization - Incremental Updates:**
```bash
# Track latest filing_date in database
LAST_FILING=$(python -c "from src.storage.metadata_manager import MetadataManager; \
  m = MetadataManager('metadata'); \
  print(m.get_watermark('fundamentals', 'bronze'))")

# Only fetch newer filings
quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
  --timeframe quarterly \
  --filing-date.gte $LAST_FILING \
  --output-dir $BRONZE_DIR/fundamentals
```

**API Usage:**
- 50 tickers × 1 API call each = 50 calls/week
- Annual cost: 2,600 API calls
- Well within free tier limits (5 calls/min = 7,200/day)

---

## 2. Corporate Actions

### Current Implementation (from daily_update.sh)
```bash
# Step 7: Corporate Actions (Polygon REST API)
quantmini polygon corporate-actions \
  --start-date $START_DATE \
  --end-date $END_DATE \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions

# Step 8: Ticker Events (Symbol Changes)
quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

### Data Types Included:
1. **Dividends** (`/v3/reference/dividends`)
   - Cash dividends, special dividends
   - Ex-dividend date, payment date, amount

2. **Stock Splits** (`/v3/reference/splits`)
   - Forward and reverse splits
   - Execution date, split ratio

3. **IPOs** (`/vX/reference/ipos`)
   - Initial public offerings
   - Listing date, issue price, status

4. **Ticker Symbol Changes** (`/vX/reference/tickers/{ticker}/events`)
   - Rebranding, mergers, ticker changes
   - Old ticker → New ticker mapping

### Recommended Refresh Strategy

**Frequency:** Daily (3 AM)

**Rationale:**
- Corporate actions announced unpredictably
- Need to capture future announced dividends/splits
- Daily refresh ensures timely updates for trading strategies

#### A. Historical Refresh (Daily)

**Lookback:** 30 days

```bash
# Capture recent events and any late additions
START_DATE=$(date -d '30 days ago' +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

quantmini polygon corporate-actions \
  --start-date $START_DATE \
  --end-date $END_DATE \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions
```

**Why 30 days?**
- Captures all recent activity
- Accounts for retroactive corrections
- Minimal API overhead (1-2 calls)

#### B. Future Events Refresh (Daily)

**Future Window:** 90 days (3 months)

```bash
# Capture announced future dividends and splits
TODAY=$(date +%Y-%m-%d)
FUTURE=$(date -d '90 days' +%Y-%m-%d)

quantmini polygon corporate-actions \
  --start-date $TODAY \
  --end-date $FUTURE \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_future
```

**Why Future Events Matter:**
- Dividends announced weeks before ex-dividend date
- Stock splits announced with future execution dates
- Critical for dividend capture strategies
- Enables proactive portfolio management

**API Test Results:**
- Future dividends available: ✅ Yes (1,554 records for all tickers in 90-day window)
- Future splits available: ✅ Yes (33 records)
- AAPL future dividends: 0 (no announcement in test period)

#### C. Full Historical Load (Monthly)

**Lookback:** 2 years

```bash
# Monthly comprehensive refresh
# Run on 1st of month at 1 AM
quantmini polygon corporate-actions \
  --start-date $(date -d '2 years ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions
```

**Purpose:**
- Backfill new tickers added to universe
- Fix any data gaps from failed daily runs
- Comprehensive validation of historical data

**API Usage:**
- Daily: 2 calls (historical + future)
- Monthly: +1 call (full refresh)
- Annual: ~750 calls total

---

## 3. Short Interest & Short Volume

### Current Implementation (from daily_update.sh)
```bash
# Step 10: Short Interest & Short Volume
quantmini polygon short-data $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/fundamentals
```

### Data Types Included:
1. **Short Interest** (`/stocks/v1/short-interest`)
   - Settlement-based reporting
   - Updated ~every 2 weeks by exchanges
   - Total shares sold short

2. **Short Volume** (`/stocks/v1/short-volume`)
   - Daily trading data
   - Short exempt volume, total volume
   - Updated daily

### Recommended Refresh Strategy

**Frequency:** Weekly (Every Monday at 4 AM)

**Rationale:**
- Short interest updated bi-weekly (15th and end of month)
- Short volume less time-critical than price data
- Weekly captures all updates without daily overhead

**⚠️ IMPORTANT: API Behavior**

**The `/stocks/v1/short-interest` and `/stocks/v1/short-volume` endpoints return ALL tickers regardless of the ticker parameter!**

**Correct Implementation:**
```bash
# Download full dataset once (no ticker filtering on API side)
quantmini polygon short-data ALL \
  --output-dir $BRONZE_DIR/fundamentals \
  --limit 1000  # Paginate through all results

# Client-side filtering happens in code after download
```

**Why This Design?**
- Download full dataset = All tickers available for free
- Add new tickers without re-downloading
- Filter later in Silver layer based on your universe

**API Usage:**
- ~2,000-3,000 paginated calls per refresh
- Returns 200,000+ records (all US tickers)
- One-time download captures everything

**Alternative Approach (If API Usage is Concern):**
```python
# In code: Download once, filter for needed tickers, cache rest
df_all = await downloader.download_short_interest()  # All tickers

# Save full dataset for future use
df_all.write_parquet(f'{BRONZE_DIR}/short_interest_full.parquet')

# Filter for active universe
df_filtered = df_all.filter(pl.col('ticker').is_in(FUNDAMENTAL_TICKERS))
```

---

## 4. Ticker Events (Symbol Changes)

### Current Implementation (from daily_update.sh)
```bash
# Step 8: Ticker Events
quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

### Data Included:
- Ticker symbol changes
- Rebranding events
- Merger-related ticker transitions

### Recommended Refresh Strategy

**Frequency:** Weekly (Every Sunday at 3 AM)

**Rationale:**
- Symbol changes are rare (few per month across all tickers)
- Per-ticker API calls required (no bulk endpoint)
- Weekly refresh sufficient to catch all changes

**API Limitation:**
- Endpoint: `/vX/reference/tickers/{ticker}/events`
- **Requires specific ticker in URL path** (not query parameter)
- No bulk download option
- Must call once per ticker

**API Usage:**
- 50 tickers × 1 call each = 50 calls/week
- Annual: 2,600 calls

**Optimization:**
```bash
# Only refresh tickers that had price/volume activity
# Inactive tickers won't have symbol changes
ACTIVE_TICKERS=$(python -c "
from src.utils.data_loader import get_active_tickers
print(' '.join(get_active_tickers(days=7)))
")

quantmini polygon ticker-events $ACTIVE_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

---

## 5. Financial Ratios

### Current Implementation (from daily_update.sh)
```bash
# Step 6: Financial Ratios (Calculated from Fundamentals)
quantmini polygon financial-ratios $FUNDAMENTAL_TICKERS \
  --input-dir $BRONZE_DIR/fundamentals \
  --output-dir $BRONZE_DIR/fundamentals \
  --include-growth
```

### Ratios Calculated:
- **Profitability:** ROE, ROA, Profit Margin
- **Liquidity:** Current Ratio, Quick Ratio
- **Leverage:** Debt/Equity, Interest Coverage
- **Efficiency:** Asset Turnover, Inventory Turnover
- **Growth:** Revenue Growth, Earnings Growth

### Recommended Refresh Strategy

**Frequency:** Weekly (Immediately after Fundamentals refresh)

**Rationale:**
- Derived from fundamentals data (no API calls)
- Should run whenever fundamentals are updated
- Fast computation (<1 min for 50 tickers)

**Implementation:**
```bash
# Chained with fundamentals refresh
# Step 1: Download fundamentals
quantmini polygon fundamentals $TICKERS ...

# Step 2: Calculate ratios (no API calls)
quantmini polygon financial-ratios $TICKERS \
  --input-dir $BRONZE_DIR/fundamentals \
  --output-dir $BRONZE_DIR/fundamentals \
  --include-growth
```

**API Usage:** 0 (calculated locally)

---

## Recommended Weekly Schedule

### Sunday (2-4 AM)
```bash
# 2:00 AM - Fundamentals refresh
quantmini polygon fundamentals $TICKERS \
  --timeframe quarterly \
  --filing-date.gte $(date -d '180 days ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals

# 2:30 AM - Financial Ratios calculation
quantmini polygon financial-ratios $TICKERS \
  --input-dir $BRONZE_DIR/fundamentals \
  --output-dir $BRONZE_DIR/fundamentals \
  --include-growth

# 3:00 AM - Ticker Events
quantmini polygon ticker-events $TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions
```

**API Calls:** ~100 (50 fundamentals + 50 ticker events)

### Monday (4 AM)
```bash
# 4:00 AM - Short Interest & Short Volume
quantmini polygon short-data ALL \
  --output-dir $BRONZE_DIR/fundamentals \
  --limit 1000
```

**API Calls:** ~2,000 (paginated, all tickers)

### Daily (3 AM)
```bash
# 3:00 AM - Corporate Actions (Historical + Future)
# Historical (last 30 days)
quantmini polygon corporate-actions \
  --start-date $(date -d '30 days ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions

# Future (next 90 days)
quantmini polygon corporate-actions \
  --start-date $(date +%Y-%m-%d) \
  --end-date $(date -d '90 days' +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_future
```

**API Calls:** ~2 per day = 14/week

### Monthly (1st of Month, 1 AM)
```bash
# 1:00 AM - Full Corporate Actions Backfill
quantmini polygon corporate-actions \
  --start-date $(date -d '2 years ago' +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions
```

**API Calls:** ~1 (bulk historical)

---

## Total API Usage Summary

### Per Week:
- **Sunday:** ~100 calls (fundamentals + ticker events)
- **Monday:** ~2,000 calls (short data)
- **Daily (7 days):** ~14 calls (corporate actions)
- **Total:** ~2,114 calls/week

### Per Month:
- **Weekly refreshes:** 2,114 × 4 = 8,456 calls
- **Monthly backfill:** +1 call
- **Total:** ~8,457 calls/month

### API Tier Requirements:
- **Free Tier:** 5 calls/min (sufficient for current 50-ticker universe)
- **Starter ($29/mo):** Unlimited (recommended for 500+ tickers)
- **Current Usage:** Well within free tier limits

---

## Incremental Update Strategy

To minimize API usage and processing time, implement watermark-based incremental updates:

### 1. Track Latest Update Timestamps

```python
from src.storage.metadata_manager import MetadataManager

metadata = MetadataManager(metadata_root='/Users/zheyuanzhao/workspace/quantlake/metadata')

# After successful fundamentals download
metadata.update_watermark(
    data_type='fundamentals',
    stage='bronze',
    date=latest_filing_date
)

# Before next download
last_update = metadata.get_watermark('fundamentals', 'bronze')
filing_date_gte = str(last_update)  # Only fetch newer data
```

### 2. Smart Ticker Selection

```python
# Only process tickers with recent activity
def get_active_tickers(days=7):
    """Get tickers with trading activity in last N days"""
    # Query price/volume data
    # Return list of active tickers
    pass

# Use in refresh scripts
ACTIVE_TICKERS = get_active_tickers(days=7)
# Reduces API calls for inactive/delisted stocks
```

### 3. Deduplication

```python
# When appending new data to existing partitions
if output_file.exists():
    existing_df = pl.read_parquet(output_file)
    new_df = pl.concat([existing_df, downloaded_df], how="diagonal")

    # Deduplicate by primary key
    new_df = new_df.unique(subset=['ticker', 'filing_date', 'fiscal_period'])

    new_df.write_parquet(output_file)
```

---

## Data Quality Monitoring

### Key Metrics to Track:

1. **Data Freshness**
   - Fundamentals: Days since latest filing
   - Corporate Actions: Days since latest dividend/split
   - Alert if > 14 days stale

2. **Coverage**
   - % of tickers with data
   - Alert if < 95% for active tickers

3. **API Success Rate**
   - Track failed requests
   - Alert if error rate > 5%

4. **Record Counts**
   - Track records added per refresh
   - Alert on anomalies (0 records, huge spikes)

### Implementation:

```python
# After each refresh
from src.monitoring.data_quality import DataQualityMonitor

monitor = DataQualityMonitor()
metrics = monitor.check_fundamentals_freshness(data_path)

if metrics['freshness_days'] > 14:
    alert_admin("Fundamentals data is stale")

if metrics['coverage_pct'] < 95:
    alert_admin(f"Coverage dropped to {metrics['coverage_pct']}%")
```

---

## Scaling Considerations

### Current State (50 Tickers)
- API calls: ~2,114/week
- Processing time: ~10-15 minutes/refresh
- Storage: ~500 MB bronze data

### Scaling to S&P 500 (500 Tickers)
- API calls: ~20,000/week (10x increase)
- Processing time: ~1-2 hours/refresh
- Storage: ~5 GB bronze data
- **Requires Starter tier ($29/mo) for unlimited API**

### Scaling to Russell 2000 (2,000 Tickers)
- API calls: ~80,000/week (40x increase)
- Processing time: ~4-8 hours/refresh
- Storage: ~20 GB bronze data
- **Consider Professional tier ($299/mo) with priority support**

### Optimization for Scale:
1. **Parallel processing:** Use `--max-concurrent` flag
2. **Incremental updates:** Only fetch changed data
3. **Smart ticker prioritization:** Process large-cap first
4. **Caching:** Store immutable historical data separately

---

## Next Steps

### Immediate (This Week):
1. ✅ Create test script for API endpoints
2. ✅ Document refresh strategies
3. 📋 Separate daily vs weekly refresh scripts
4. 📋 Add future corporate actions download

### Short-term (This Month):
1. 📋 Implement watermark-based incremental updates
2. 📋 Add data quality monitoring
3. 📋 Create alerting for stale data
4. 📋 Optimize daily_update.sh for new strategy

### Long-term (This Quarter):
1. 📋 Expand to full S&P 500 (500 tickers)
2. 📋 Build monitoring dashboard
3. 📋 Implement smart ticker prioritization
4. 📋 Add automated reprocessing for failed refreshes

---

## References

### Polygon API Documentation:
- Fundamentals: https://polygon.io/docs/rest/stocks/fundamentals/financials
- Dividends: https://polygon.io/docs/rest/stocks/corporate-actions/dividends
- Splits: https://polygon.io/docs/rest/stocks/corporate-actions/splits
- Short Interest: https://polygon.io/docs/rest/stocks/fundamentals/short-interest
- Short Volume: https://polygon.io/docs/rest/stocks/fundamentals/short-volume

### Internal Documentation:
- `scripts/daily_update.sh` - Current pipeline implementation
- `docs/guides/data-ingestion-strategies.md` - Medallion architecture
- `src/download/` - Downloader implementations

---

**Last Updated:** 2025-10-21
**Author:** Generated by API Refresh Strategy Tester
**Version:** 1.0
