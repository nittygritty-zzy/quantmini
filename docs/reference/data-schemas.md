# Polygon.io S3 Flat File Schemas

**Date**: September 30, 2025
**Source**: Polygon.io S3 Flat Files (`flatfiles` bucket)

---

## Overview

All flat files are gzip-compressed CSV files with headers. Access requires S3 signature v4.

---

## 1. Stock Daily Aggregates

### S3 Path Structure
```
us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz
```

**Example**: `us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz`

### Schema (8 columns)

| # | Column Name | Type | Description |
|---|-------------|------|-------------|
| 1 | ticker | STRING | Stock symbol (e.g., "AAPL", "TSLA") |
| 2 | volume | INTEGER | Total trading volume for the day |
| 3 | open | FLOAT | Opening price |
| 4 | close | FLOAT | Closing price |
| 5 | high | FLOAT | Highest price of the day |
| 6 | low | FLOAT | Lowest price of the day |
| 7 | window_start | INTEGER | Unix nanosecond timestamp of bar start |
| 8 | transactions | INTEGER | Number of trades |

### Sample Row
```csv
ticker,volume,open,close,high,low,window_start,transactions
A,1486050,123.76,123.75,124.23,122.5725,1759118400000000000,24819
```

### Notes
- One file per date, contains all stock symbols
- window_start is in nanoseconds: `1759118400000000000` = 2025-09-29 00:00:00 UTC
- File size: ~50-100 MB compressed, ~300-500 MB uncompressed

---

## 2. Stock Minute Aggregates

### S3 Path Structure
```
us_stocks_sip/minute_aggs_v1/{year}/{month}/{symbol}.csv.gz
```

**Example**: `us_stocks_sip/minute_aggs_v1/2025/09/AAPL.csv.gz`

### Schema (8 columns)

| # | Column Name | Type | Description |
|---|-------------|------|-------------|
| 1 | ticker | STRING | Stock symbol (same for entire file) |
| 2 | volume | INTEGER | Volume for the minute bar |
| 3 | open | FLOAT | Opening price of minute bar |
| 4 | close | FLOAT | Closing price of minute bar |
| 5 | high | FLOAT | Highest price in minute bar |
| 6 | low | FLOAT | Lowest price in minute bar |
| 7 | window_start | INTEGER | Unix nanosecond timestamp of bar start |
| 8 | transactions | INTEGER | Number of trades in minute |

### Sample Row
```csv
ticker,volume,open,close,high,low,window_start,transactions
AAPL,125430,225.50,225.48,225.52,225.47,1759154940000000000,456
```

### Notes
- One file per symbol per month
- Contains all minute bars for the symbol during market hours
- Typical file size: 1-10 MB compressed per symbol
- Market hours: 9:30 AM - 4:00 PM ET (390 minutes per day)

---

## 3. Options Daily Aggregates

### S3 Path Structure
```
us_options_opra/day_aggs_v1/{year}/{month}/{symbol}.csv.gz
```

**Example**: `us_options_opra/day_aggs_v1/2025/09/AAPL.csv.gz`

### Schema (13 columns)

| # | Column Name | Type | Description |
|---|-------------|------|-------------|
| 1 | ticker | STRING | Options contract ticker (e.g., "O:A251017C00125000") |
| 2 | underlying | STRING | Underlying stock symbol (e.g., "A") |
| 3 | expiration_date | STRING | Contract expiration (YYYY-MM-DD) |
| 4 | contract_type | STRING | "call" or "put" |
| 5 | strike_price | FLOAT | Strike price |
| 6 | datetime | INTEGER | Unix nanosecond timestamp |
| 7 | open | FLOAT | Opening price |
| 8 | high | FLOAT | Highest price of the day |
| 9 | low | FLOAT | Lowest price of the day |
| 10 | close | FLOAT | Closing price |
| 11 | volume | INTEGER | Total trading volume |
| 12 | transactions | INTEGER | Number of trades |
| 13 | date | STRING | Trading date (YYYY-MM-DD) |

### Sample Row
```csv
ticker,underlying,expiration_date,contract_type,strike_price,datetime,open,high,low,close,volume,transactions,date
O:A251017C00125000,A,2025-10-17,call,125.0,1758686400000000000,3.5,3.6,3.5,3.58,7,3,2025-09-24
```

### Ticker Format Breakdown
- `O:` - Options prefix
- `A` - Underlying symbol
- `251017` - Expiration date (YYMMDD) = 2025-10-17
- `C` - Contract type (C=call, P=put)
- `00125000` - Strike price × 1000 (125.000)

### Notes
- One file per underlying symbol per month
- Contains all contracts (calls + puts, all strikes, all expirations)
- File size: 5-50 MB compressed depending on underlying popularity
- Includes expired contracts if they traded that month

---

## 4. Options Minute Aggregates

### S3 Path Structure
```
us_options_opra/minute_aggs_v1/{year}/{month}/{date}.csv.gz
```

**Example**: `us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz`

### Schema (13 columns)

| # | Column Name | Type | Description |
|---|-------------|------|-------------|
| 1 | ticker | STRING | Options contract ticker (e.g., "O:A251017C00125000") |
| 2 | underlying | STRING | Underlying stock symbol |
| 3 | expiration_date | STRING | Contract expiration (YYYY-MM-DD) |
| 4 | contract_type | STRING | "call" or "put" |
| 5 | strike_price | FLOAT | Strike price |
| 6 | datetime | INTEGER | Unix nanosecond timestamp of bar start |
| 7 | open | FLOAT | Opening price of minute bar |
| 8 | high | FLOAT | Highest price in minute bar |
| 9 | low | FLOAT | Lowest price in minute bar |
| 10 | close | FLOAT | Closing price of minute bar |
| 11 | volume | INTEGER | Volume for the minute bar |
| 12 | transactions | INTEGER | Number of trades in minute |
| 13 | date | STRING | Trading date (YYYY-MM-DD) |

### Sample Row
```csv
ticker,underlying,expiration_date,contract_type,strike_price,datetime,open,high,low,close,volume,transactions,date
O:A251017C00125000,A,2025-10-17,call,125.0,1759154940000000000,2.5,2.5,2.5,2.5,5,1,2025-09-29
```

### Notes
- One file per date, contains all option contracts across all underlyings
- Very large files: 500 MB - 2 GB compressed
- Contains minute bars only for contracts that traded
- Most contracts have sparse trading (many minutes with no activity)

---

## Timestamp Conversion

All `window_start` and `datetime` fields are Unix nanosecond timestamps:

```python
import pandas as pd

# Convert nanosecond timestamp to datetime
timestamp_ns = 1759118400000000000
dt = pd.to_datetime(timestamp_ns, unit='ns')
# Result: 2025-09-29 00:00:00

# Convert datetime to nanosecond timestamp
dt = pd.Timestamp('2025-09-29')
timestamp_ns = int(dt.value)
# Result: 1759118400000000000
```

---

## File Size Reference

| Data Type | Compressed | Uncompressed | Records/File |
|-----------|-----------|--------------|--------------|
| Stock Daily | 50-100 MB | 300-500 MB | ~10,000-15,000 symbols |
| Stock Minute (per symbol) | 1-10 MB | 10-100 MB | ~8,000-10,000 minutes/month |
| Options Daily (per symbol) | 5-50 MB | 50-500 MB | ~50,000-500,000 contracts |
| Options Minute | 500 MB-2 GB | 2-10 GB | ~3,000,000 bars |

---

## Access Pattern

### Authentication
```python
from botocore.config import Config
import boto3

s3 = boto3.client(
    's3',
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET',
    endpoint_url='https://files.polygon.io',
    config=Config(signature_version='s3v4')
)
```

### Download Example
```python
# Stock daily
response = s3.get_object(
    Bucket='flatfiles',
    Key='us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz'
)

# Options minute
response = s3.get_object(
    Bucket='flatfiles',
    Key='us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz'
)
```

---

## Column Mapping to Database

### Stock Daily → SQLite
| S3 Column | SQLite Column | Transformation |
|-----------|---------------|----------------|
| ticker | symbol | Direct |
| window_start | datetime | Convert ns to datetime string |
| - | date | Extract date from window_start |
| open, high, low, close, volume, transactions | Same | Direct |
| - | alpha_daily | Calculated: `-ln(open/prev_close)` |

### Stock Minute → SQLite
| S3 Column | SQLite Column | Transformation |
|-----------|---------------|----------------|
| ticker | symbol | Direct |
| window_start | datetime | Convert ns to datetime string |
| - | date | Extract date from window_start |
| open, high, low, close, volume, transactions | Same | Direct |
| - | alpha_minute | Calculated: `-ln(open/prev_close)` |

### Options Daily/Minute → SQLite
All columns mapped directly, with:
- `datetime` → Stored as both nanosecond integer and date string
- Underlying, expiration, type, strike extracted from ticker if needed

---

## Data Quality Notes

### Stock Data
- ✅ Complete coverage of all US stocks (SIP feed)
- ✅ Includes pre-market and after-hours for minute data
- ✅ Adjusted for splits and dividends
- ⚠️ May include halted stocks (volume = 0)

### Options Data
- ✅ Complete OPRA feed coverage
- ⚠️ Very sparse minute data (most contracts don't trade every minute)
- ⚠️ Strike prices need division by 1000 in ticker format
- ⚠️ Includes expired contracts if they traded

### Known Issues
- Weekend/holiday files return 404 (expected)
- Some symbols may have missing data on low-volume days
- Options minute files are very large (2+ GB uncompressed)

---

## Related Documentation

- Main README: `data/README.md`
- Database schemas: See SQLite `.schema` output above
- Pipeline scripts: `data/scripts/{stocks,options}/batch/`
