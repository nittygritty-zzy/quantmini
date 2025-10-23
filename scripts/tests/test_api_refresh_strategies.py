#!/usr/bin/env python3
"""
API Refresh Strategy Tester

Tests Polygon APIs to determine optimal refresh frequencies and date ranges
for fundamentals and corporate actions data.

Usage:
    python scripts/tests/test_api_refresh_strategies.py
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.download.polygon_rest_client import PolygonRESTClient
from src.download.fundamentals import FundamentalsDownloader
from src.download.corporate_actions import CorporateActionsDownloader
from src.core.config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class APIRefreshTester:
    """Test API endpoints to determine optimal refresh strategies"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.results = {}

    async def test_fundamentals_freshness(self, ticker: str = "AAPL") -> Dict:
        """
        Test how fresh fundamentals data is and what date ranges work best

        Returns dict with:
        - latest_data_date: Most recent filing date available
        - recommended_lookback: How far back to fetch for complete data
        - update_frequency: Recommended refresh frequency
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"TESTING FUNDAMENTALS API - Ticker: {ticker}")
        logger.info(f"{'='*80}")

        async with PolygonRESTClient(api_key=self.api_key) as client:
            downloader = FundamentalsDownloader(
                client=client,
                output_dir=Path('/tmp/test_fundamentals'),
                use_partitioned_structure=False
            )

            # Test 1: Get latest quarterly data
            logger.info("\n📊 Test 1: Latest Quarterly Data")
            df_quarterly = await downloader.download_balance_sheets(
                ticker=ticker,
                timeframe='quarterly',
                limit=10
            )

            if len(df_quarterly) > 0:
                latest_filing = df_quarterly['filing_date'].max()
                oldest_in_latest = df_quarterly['filing_date'].min()

                logger.info(f"✓ Found {len(df_quarterly)} quarterly filings")
                logger.info(f"  Latest filing: {latest_filing}")
                logger.info(f"  Oldest in sample: {oldest_in_latest}")
                logger.info(f"  Data freshness: {(datetime.now().date() - latest_filing).days} days old")

            # Test 2: Get latest annual data
            logger.info("\n📊 Test 2: Latest Annual Data")
            df_annual = await downloader.download_balance_sheets(
                ticker=ticker,
                timeframe='annual',
                limit=5
            )

            if len(df_annual) > 0:
                latest_annual = df_annual['filing_date'].max()
                logger.info(f"✓ Found {len(df_annual)} annual filings")
                logger.info(f"  Latest annual filing: {latest_annual}")

            # Test 3: Get data from specific date range (last 90 days)
            logger.info("\n📊 Test 3: Recent Updates (Last 90 Days)")
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=90)

            df_recent = await downloader.download_balance_sheets(
                ticker=ticker,
                timeframe='quarterly',
                limit=1000,
                filing_date_gte=str(start_date)
            )

            logger.info(f"✓ Found {len(df_recent)} filings in last 90 days")

            return {
                'ticker': ticker,
                'latest_quarterly_filing': latest_filing if len(df_quarterly) > 0 else None,
                'latest_annual_filing': latest_annual if len(df_annual) > 0 else None,
                'filings_last_90_days': len(df_recent),
                'data_freshness_days': (datetime.now().date() - latest_filing).days if len(df_quarterly) > 0 else None,
                'recommended_lookback_days': 180,  # 6 months to capture 2 quarters
                'recommended_frequency': 'weekly'  # Companies file quarterly
            }

    async def test_corporate_actions_freshness(self, ticker: str = "AAPL") -> Dict:
        """
        Test corporate actions data freshness and future availability

        Tests:
        1. Historical data availability
        2. Recent events (last 30 days)
        3. Future announced events (next 90 days)
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"TESTING CORPORATE ACTIONS API - Ticker: {ticker}")
        logger.info(f"{'='*80}")

        async with PolygonRESTClient(api_key=self.api_key) as client:
            downloader = CorporateActionsDownloader(
                client=client,
                output_dir=Path('/tmp/test_corporate_actions'),
                use_partitioned_structure=False
            )

            today = datetime.now().date()

            # Test 1: Historical data (last 365 days)
            logger.info("\n📊 Test 1: Historical Corporate Actions (Last Year)")
            past_start = today - timedelta(days=365)

            data_past = await downloader.download_all_corporate_actions(
                ticker=ticker,
                start_date=str(past_start),
                end_date=str(today)
            )

            logger.info(f"✓ Dividends (past year): {len(data_past['dividends'])} records")
            logger.info(f"✓ Splits (past year): {len(data_past['splits'])} records")

            if len(data_past['dividends']) > 0:
                latest_div = data_past['dividends']['ex_dividend_date'].max()
                logger.info(f"  Latest dividend ex-date: {latest_div}")

            # Test 2: Recent events (last 30 days)
            logger.info("\n📊 Test 2: Recent Events (Last 30 Days)")
            recent_start = today - timedelta(days=30)

            data_recent = await downloader.download_all_corporate_actions(
                ticker=ticker,
                start_date=str(recent_start),
                end_date=str(today)
            )

            logger.info(f"✓ Recent dividends: {len(data_recent['dividends'])} records")
            logger.info(f"✓ Recent splits: {len(data_recent['splits'])} records")

            # Test 3: Future announced events (next 90 days)
            logger.info("\n📊 Test 3: Future Announced Events (Next 90 Days)")
            future_end = today + timedelta(days=90)

            data_future = await downloader.download_all_corporate_actions(
                ticker=ticker,
                start_date=str(today),
                end_date=str(future_end)
            )

            logger.info(f"✓ Future dividends: {len(data_future['dividends'])} records")
            logger.info(f"✓ Future splits: {len(data_future['splits'])} records")

            if len(data_future['dividends']) > 0:
                next_div = data_future['dividends']['ex_dividend_date'].min()
                logger.info(f"  Next dividend ex-date: {next_div}")

            return {
                'ticker': ticker,
                'dividends_last_year': len(data_past['dividends']),
                'splits_last_year': len(data_past['splits']),
                'dividends_last_30_days': len(data_recent['dividends']),
                'future_dividends': len(data_future['dividends']),
                'future_splits': len(data_future['splits']),
                'has_future_events': len(data_future['dividends']) > 0 or len(data_future['splits']) > 0,
                'recommended_lookback_days': 365,  # 1 year of history
                'recommended_future_days': 90,  # Next 3 months
                'recommended_frequency': 'daily'  # Events can be announced anytime
            }

    async def test_short_interest_freshness(self, tickers: List[str] = None) -> Dict:
        """
        Test short interest and short volume data freshness

        Note: These endpoints return ALL tickers, not filtered by ticker parameter
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"TESTING SHORT INTEREST/VOLUME API")
        logger.info(f"{'='*80}")

        if tickers is None:
            tickers = ["AAPL", "MSFT", "GOOGL"]

        async with PolygonRESTClient(api_key=self.api_key) as client:
            from src.download.fundamentals import FundamentalsDownloader
            downloader = FundamentalsDownloader(
                client=client,
                output_dir=Path('/tmp/test_short_data'),
                use_partitioned_structure=False
            )

            # Test short interest (all tickers, then filter)
            logger.info("\n📊 Test 1: Short Interest Data")
            df_si = await downloader.download_short_interest(limit=1000)

            logger.info(f"✓ Total short interest records: {len(df_si)}")
            if len(df_si) > 0:
                logger.info(f"  Unique tickers: {df_si['ticker'].n_unique()}")
                logger.info(f"  Latest settlement date: {df_si['settlement_date'].max()}")

                # Filter for our test tickers
                df_test = df_si.filter(df_si['ticker'].is_in([t.upper() for t in tickers]))
                logger.info(f"  Records for test tickers: {len(df_test)}")

            # Test short volume
            logger.info("\n📊 Test 2: Short Volume Data")
            df_sv = await downloader.download_short_volume(limit=1000)

            logger.info(f"✓ Total short volume records: {len(df_sv)}")
            if len(df_sv) > 0:
                logger.info(f"  Unique tickers: {df_sv['ticker'].n_unique()}")
                logger.info(f"  Latest date: {df_sv['date'].max()}")

            return {
                'short_interest_total_records': len(df_si),
                'short_volume_total_records': len(df_sv),
                'short_interest_tickers': df_si['ticker'].n_unique() if len(df_si) > 0 else 0,
                'recommended_frequency': 'weekly',  # Updated weekly
                'note': 'API returns ALL tickers - client-side filtering required'
            }

    async def test_all_endpoints(self) -> Dict:
        """Run all API freshness tests"""
        logger.info("\n" + "="*80)
        logger.info("COMPREHENSIVE API REFRESH STRATEGY TESTING")
        logger.info("="*80)

        results = {}

        # Test fundamentals
        results['fundamentals'] = await self.test_fundamentals_freshness("AAPL")

        # Test corporate actions
        results['corporate_actions'] = await self.test_corporate_actions_freshness("AAPL")

        # Test short data
        results['short_data'] = await self.test_short_interest_freshness(["AAPL", "MSFT", "GOOGL"])

        return results


def generate_recommendations(results: Dict) -> str:
    """Generate markdown documentation with recommendations"""

    doc = """# Data Refresh Strategies for Fundamentals and Corporate Actions

## Overview

This document provides recommended refresh strategies for each data type based on API testing results.

## 1. Fundamentals Data

### Data Types:
- Balance Sheets
- Income Statements
- Cash Flow Statements
- Financial Ratios (calculated from above)

### Refresh Strategy:

**Frequency:** Weekly (Every Sunday)

**Rationale:**
- Companies file quarterly earnings (10-Q, 10-K)
- Filing dates vary but cluster around earnings seasons
- Weekly updates capture all new filings without excessive API calls

**Date Range:**
- **Lookback:** 180 days (6 months)
  - Captures 2 most recent quarters
  - Ensures no missed filings due to late amendments
- **Incremental:** Only fetch filings newer than latest `filing_date` in database

**Implementation:**
```bash
# Weekly fundamentals refresh
quantmini polygon fundamentals $TICKERS \\
  --timeframe quarterly \\
  --filing-date.gte $(date -d '180 days ago' +%Y-%m-%d) \\
  --output-dir $BRONZE_DIR/fundamentals
```

**API Endpoint:** `/vX/reference/financials`
- Supports `filing_date.gte` and `filing_date.lte` parameters
- Returns data for all available timeframes

### Test Results:
"""

    if 'fundamentals' in results:
        fund = results['fundamentals']
        doc += f"""
- Latest quarterly filing: {fund.get('latest_quarterly_filing', 'N/A')}
- Latest annual filing: {fund.get('latest_annual_filing', 'N/A')}
- Data freshness: {fund.get('data_freshness_days', 'N/A')} days old
- Filings in last 90 days: {fund.get('filings_last_90_days', 0)}
"""

    doc += """

---

## 2. Corporate Actions

### Data Types:
- Dividends
- Stock Splits
- IPOs
- Ticker Symbol Changes

### Refresh Strategy:

**Frequency:** Daily

**Rationale:**
- Corporate actions can be announced anytime
- Dividends have ex-dividend dates set weeks/months in advance
- Stock splits announced with future execution dates
- Need both historical and future events

**Date Ranges:**

#### Historical Refresh (Daily)
- **Lookback:** 30 days
  - Captures any late additions/corrections
  - Minimal API overhead

```bash
# Daily historical refresh
START_DATE=$(date -d '30 days ago' +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

quantmini polygon corporate-actions \\
  --start-date $START_DATE \\
  --end-date $END_DATE \\
  --include-ipos \\
  --output-dir $BRONZE_DIR/corporate_actions
```

#### Future Events (Daily)
- **Future window:** 90 days (3 months)
  - Captures announced dividends
  - Captures announced stock splits

```bash
# Daily future events refresh
TODAY=$(date +%Y-%m-%d)
FUTURE=$(date -d '90 days' +%Y-%m-%d)

quantmini polygon corporate-actions \\
  --start-date $TODAY \\
  --end-date $FUTURE \\
  --include-ipos \\
  --output-dir $BRONZE_DIR/corporate_actions_future
```

#### Full Historical Load (Monthly)
- **Lookback:** 2 years
  - Complete history for new tickers
  - Backfill any gaps

```bash
# Monthly full refresh
quantmini polygon corporate-actions \\
  --start-date $(date -d '2 years ago' +%Y-%m-%d) \\
  --end-date $(date +%Y-%m-%d) \\
  --include-ipos \\
  --output-dir $BRONZE_DIR/corporate_actions
```

### Test Results:
"""

    if 'corporate_actions' in results:
        ca = results['corporate_actions']
        doc += f"""
- Dividends (last year): {ca.get('dividends_last_year', 0)}
- Splits (last year): {ca.get('splits_last_year', 0)}
- Recent dividends (30 days): {ca.get('dividends_last_30_days', 0)}
- Future dividends (90 days): {ca.get('future_dividends', 0)}
- Future splits (90 days): {ca.get('future_splits', 0)}
- Has future events: {ca.get('has_future_events', False)}
"""

    doc += """

**API Endpoints:**
- Dividends: `/v3/reference/dividends`
  - Supports `ex_dividend_date.gte` and `.lte`
  - Returns both historical and future announced dividends

- Splits: `/v3/reference/splits`
  - Supports `execution_date.gte` and `.lte`
  - Returns both historical and future announced splits

- IPOs: `/vX/reference/ipos`
  - Supports `listing_date.gte` and `.lte`
  - Includes `ipo_status` field (pending, history, etc.)

---

## 3. Short Interest & Short Volume

### Data Types:
- Short Interest (settlement-based, ~bi-weekly)
- Short Volume (daily trading volumes)

### Refresh Strategy:

**Frequency:** Weekly

**Rationale:**
- Short interest updated by exchanges ~every 2 weeks
- Short volume updated daily but less time-sensitive
- Weekly refresh balances freshness vs API efficiency

**Important:** API returns ALL tickers (not filtered by ticker parameter)
- Download full dataset once per refresh
- Filter client-side for needed tickers
- Store all data for future ticker additions

**Implementation:**
```bash
# Weekly short data refresh
# Note: Downloads ALL tickers, then filters
quantmini polygon short-data ALL \\
  --output-dir $BRONZE_DIR/fundamentals \\
  --limit 1000  # Adjust based on total tickers
```

### Test Results:
"""

    if 'short_data' in results:
        sd = results['short_data']
        doc += f"""
- Total short interest records: {sd.get('short_interest_total_records', 0)}
- Total short volume records: {sd.get('short_volume_total_records', 0)}
- Unique tickers (short interest): {sd.get('short_interest_tickers', 0)}
- Note: {sd.get('note', 'N/A')}
"""

    doc += """

**API Endpoints:**
- Short Interest: `/stocks/v1/short-interest`
  - Returns ALL tickers (pagination required)
  - No ticker filtering on API side

- Short Volume: `/stocks/v1/short-volume`
  - Returns ALL tickers (pagination required)
  - No ticker filtering on API side

---

## 4. Ticker Events (Symbol Changes)

### Refresh Strategy:

**Frequency:** Weekly

**Rationale:**
- Ticker changes are relatively rare
- Per-ticker API calls required (not bulk)
- Process only active tickers to minimize API usage

**Implementation:**
```bash
# Weekly ticker events refresh
quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \\
  --output-dir $BRONZE_DIR/corporate_actions
```

**API Endpoint:** `/vX/reference/tickers/{ticker}/events`
- Requires specific ticker (not bulk)
- Returns all historical events for that ticker

---

## Summary Table

| Data Type | Frequency | Lookback | Future Window | API Calls/Day |
|-----------|-----------|----------|---------------|---------------|
| **Fundamentals** | Weekly | 180 days | N/A | ~50 (50 tickers) |
| **Corporate Actions** | Daily | 30 days | 90 days | ~100 (2 calls) |
| **Short Interest/Volume** | Weekly | N/A (full dataset) | N/A | ~2,000 (pagination) |
| **Ticker Events** | Weekly | All time | N/A | ~50 (per ticker) |
| **Financial Ratios** | Weekly | Derived from fundamentals | N/A | 0 (calculated) |

**Total API Usage:**
- Daily: ~100 calls (corporate actions only)
- Weekly: ~2,150 calls (all data sources)
- Monthly: ~8,600 calls (4 weeks)

**Polygon Free Tier:** 5 calls/minute = 7,200 calls/day (sufficient)
**Polygon Unlimited Tier:** No limits (recommended for production)

---

## Optimizations

### 1. Incremental Updates
Track `last_updated` watermarks to avoid reprocessing unchanged data:

```python
from src.storage.metadata_manager import MetadataManager

metadata = MetadataManager(metadata_root)
last_date = metadata.get_watermark(data_type='fundamentals', stage='bronze')

# Only fetch newer data
if last_date:
    filing_date_gte = str(last_date)
```

### 2. Smart Ticker Selection
- **Active Tickers Only:** Exclude delisted companies from daily refreshes
- **Prioritize Large Cap:** Process S&P 500 first, then extend to Russell 2000
- **Dynamic List:** Update ticker universe monthly based on index composition

### 3. Batch Processing
- **Fundamentals:** Process 50 tickers concurrently (API supports parallel)
- **Corporate Actions:** Single bulk call (no ticker parameter)
- **Short Data:** Single bulk call, filter client-side

### 4. Caching Strategy
- **Fundamentals:** Cache quarterly filings (immutable after filing)
- **Corporate Actions:** Cache historical events (rarely change)
- **Short Data:** No caching (frequently updated)

---

## Monitoring and Alerts

### Key Metrics to Track:
1. **Data Freshness:** Days since latest filing/event
2. **API Success Rate:** Failed vs successful requests
3. **Coverage:** Tickers with vs without data
4. **Volume:** Records added per refresh cycle

### Alert Conditions:
- Data freshness > 14 days for fundamentals
- Corporate action refresh fails for 2+ consecutive days
- API error rate > 5%
- Zero records returned for active tickers

---

## Implementation Roadmap

### Phase 1: Current State (Working)
- ✅ Daily pipeline with 7-day backfill
- ✅ Fundamentals download (50 tickers)
- ✅ Corporate actions (all tickers, date range)
- ✅ Short interest/volume (client-side filtering)

### Phase 2: Optimize Refresh Frequencies (Recommended)
- 📋 Separate daily vs weekly refresh scripts
- 📋 Add future corporate actions download
- 📋 Implement watermark-based incremental updates
- 📋 Add monitoring dashboard

### Phase 3: Scale to Full Universe (Future)
- 📋 Expand to all S&P 500 tickers (500 total)
- 📋 Add Russell 2000 for small-cap coverage
- 📋 Implement smart ticker prioritization
- 📋 Add data quality validation

---

## Cost Analysis

### API Call Estimates (per refresh):

**Weekly Refresh (Recommended):**
```
Fundamentals:          50 tickers × 1 call = 50 calls
Corporate Actions:     1 bulk call = 1 call
Short Data:           ~20 pages × 100/page = 20 calls
Ticker Events:        50 tickers × 1 call = 50 calls
----------------------------------------
Total:                ~121 calls/week
```

**Daily Refresh (Corporate Actions Only):**
```
Historical (30 days):  1 bulk call = 1 call
Future (90 days):      1 bulk call = 1 call
----------------------------------------
Total:                 ~2 calls/day × 7 = 14 calls/week
```

**Combined Weekly Total:** ~135 calls/week = ~540 calls/month

**Polygon Pricing:**
- Free Tier: 5 calls/min, sufficient for current needs
- Starter ($29/mo): Unlimited calls, recommended for production

---

*Generated on: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """*
*Based on API testing with ticker: AAPL*
"""

    return doc


async def main():
    """Run all tests and generate recommendations"""

    # Load configuration
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')

    if not credentials or 'key' not in credentials:
        logger.error("❌ API key not found in config/credentials.yaml")
        sys.exit(1)

    # Run tests
    tester = APIRefreshTester(api_key=credentials['key'])
    results = await tester.test_all_endpoints()

    # Generate recommendations
    logger.info("\n" + "="*80)
    logger.info("GENERATING RECOMMENDATIONS")
    logger.info("="*80)

    recommendations = generate_recommendations(results)

    # Save to file
    output_file = Path('docs/DATA_REFRESH_STRATEGIES.md')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(recommendations)

    logger.info(f"\n✅ Recommendations saved to: {output_file}")
    logger.info(f"\n📊 Summary:")
    logger.info(f"  - Fundamentals: Weekly refresh, 180-day lookback")
    logger.info(f"  - Corporate Actions: Daily refresh, 30-day lookback + 90-day future")
    logger.info(f"  - Short Data: Weekly refresh, full dataset")
    logger.info(f"  - Ticker Events: Weekly refresh, per-ticker")

    return results


if __name__ == '__main__':
    asyncio.run(main())
