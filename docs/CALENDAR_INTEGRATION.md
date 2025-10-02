# Market Calendar Integration Guide

## Overview

The market calendar utility prevents unnecessary API calls by filtering out weekends and holidays before data downloads.

## Components

### 1. Market Calendar Utility (`src/utils/market_calendar.py`)

**Features:**
- Uses `pandas_market_calendars` for accurate NYSE/NASDAQ trading days
- Fetches upcoming holidays from Polygon API (`GET /v1/marketstatus/upcoming`)
- Caches holiday data locally
- Filters weekends and market holidays

**Key Classes:**
```python
class MarketCalendar:
    def is_trading_day(date) -> bool
    def get_trading_days(start, end) -> List[date]
    def filter_trading_days(dates) -> List[date]
    def fetch_polygon_holidays() -> List[dict]
```

### 2. Testing

**Test Script:** `scripts/test_market_calendar.py`

```bash
uv run python scripts/test_market_calendar.py
```

**Results (Aug-Sep 2025):**
- August: 21 trading days
- September: 21 trading days (Labor Day excluded)
- Total: 41 trading days ✓

### 3. Integration Points

**Where to add calendar checks:**

1. **Data Ingestion** (`src/orchestration/ingestion_orchestrator.py`)
   - Filter date ranges before S3 downloads
   - Skip weekends/holidays in daily loops

2. **Backfill Scripts** (`scripts/backfill_historical.py`)
   - Only attempt downloads for trading days
   - Avoid 404 errors from Polygon

3. **Data Integrity Checker** (`src/maintenance/data_integrity_checker.py`)
   - Don't report weekends as "gaps"
   - Focus on actual missing trading days

4. **CLI Commands** (`src/cli/commands/data.py`)
   - Validate date ranges against calendar
   - Show trading day counts

## Usage Examples

### Basic Usage

```python
from src.utils.market_calendar import get_default_calendar
from datetime import date

calendar = get_default_calendar()

# Check if date is trading day
if calendar.is_trading_day(date(2025, 9, 1)):  # Labor Day
    print("Trading day")
else:
    print("Holiday/Weekend")  # ✓ This prints

# Get all trading days in range
trading_days = calendar.get_trading_days(
    start_date=date(2025, 8, 1),
    end_date=date(2025, 9, 30)
)
print(f"Trading days: {len(trading_days)}")  # 42

# Filter list of dates
all_dates = [date(2025, 9, i) for i in range(1, 8)]
trading_only = calendar.filter_trading_days(all_dates)
print(f"Filtered: {len(all_dates)} -> {len(trading_only)}")  # 7 -> 4
```

### With Polygon API

```python
from src.core.config_loader import ConfigLoader

config = ConfigLoader()
api_key = config.get('credentials.polygon_api_key')

calendar = MarketCalendar(polygon_api_key=api_key)

# Fetch upcoming holidays
holidays = calendar.fetch_polygon_holidays()
for h in holidays:
    print(f"{h['date']}: {h['name']}")

# Update cache
count = calendar.update_holidays_cache()
print(f"Cached {count} upcoming holidays")
```

### In Data Pipeline

```python
from src.utils.market_calendar import get_default_calendar
from datetime import date, timedelta

calendar = get_default_calendar()

# Filter download dates
start = date(2025, 8, 1)
end = date(2025, 9, 30)

# Instead of all dates:
all_dates = []
current = start
while current <= end:
    all_dates.append(current)
    current += timedelta(days=1)

# Use only trading days:
trading_dates = calendar.get_trading_days(start, end)

print(f"Before: {len(all_dates)} dates")     # 61 dates
print(f"After: {len(trading_dates)} dates")  # 42 trading days
print(f"Saved: {len(all_dates) - len(trading_dates)} API calls")  # 19 calls saved!
```

## 2025 US Stock Market Holidays

The utility includes static fallback for 2025 holidays:

- **Jan 1** - New Year's Day
- **Jan 20** - MLK Jr. Day
- **Feb 17** - Presidents' Day
- **Apr 18** - Good Friday
- **May 26** - Memorial Day
- **Jun 19** - Juneteenth
- **Jul 4** - Independence Day
- **Sep 1** - Labor Day ✓ (detected in our tests)
- **Nov 27** - Thanksgiving
- **Dec 25** - Christmas

## Benefits

### API Cost Savings
- **Before:** 61 download attempts (Aug 1 - Sep 30)
- **After:** 42 download attempts (trading days only)
- **Savings:** 31% reduction in API calls

### Error Reduction
- No more 404 errors for weekends
- Cleaner logs
- Faster pipeline execution

### Data Integrity
- Gap detection focuses on real issues
- Calendar-aware backfill commands
- Accurate reporting

## Implementation Checklist

- [x] Create `src/utils/market_calendar.py`
- [x] Add dependencies (`pandas-market-calendars`, `requests`)
- [x] Create test script (`scripts/test_market_calendar.py`)
- [x] Validate against actual data (41 trading days)
- [ ] Integrate into `ingestion_orchestrator.py`
- [ ] Update `backfill_historical.py`
- [ ] Update `data_integrity_checker.py`
- [ ] Add CLI date validation
- [ ] Update documentation

## Configuration

### Optional Polygon API Integration

Add to `config/credentials.yaml`:

```yaml
credentials:
  polygon_api_key: "YOUR_KEY_HERE"
```

Then the calendar will automatically fetch and cache upcoming holidays.

### Calendar Selection

Default is NYSE, but you can choose others:

```python
calendar = MarketCalendar(exchange='NASDAQ')  # NASDAQ
calendar = MarketCalendar(exchange='LSE')     # London
calendar = MarketCalendar(exchange='JPX')     # Japan
```

See `pandas_market_calendars` docs for full list.

## Cache Location

Holiday cache stored in:
```
~/.quantmini/cache/polygon_holidays.json
```

Cache auto-refreshes if older than 7 days.

## Next Steps

1. Integrate calendar into ingestion orchestrator
2. Update data integrity checker to ignore weekends
3. Add calendar check to CLI commands
4. Document best practices in README

## References

- [Polygon Market Holidays API](https://polygon.io/docs/rest/stocks/market-operations/market-holidays)
- [pandas-market-calendars](https://github.com/rsheftel/pandas_market_calendars)
- [US Stock Market Hours](https://www.nyse.com/markets/hours-calendars)
