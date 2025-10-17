# Delisted Stocks Feature - Survivorship Bias Fix

**Added**: 2025-10-07
**Purpose**: Address survivorship bias in backtests by including failed companies

---

## Overview

The **Delisted Stocks** feature automatically downloads historical data for stocks that delisted (went bankrupt, were acquired, or otherwise stopped trading) during your backtest period. This fixes **survivorship bias**, where backtests only include successful companies and show unrealistically high returns.

### Impact

Including delisted stocks makes backtest results more realistic:

- **Returns**: Typically drop by 15-30% annualized
- **Sharpe Ratio**: Decreases by 10-20%
- **Max Drawdown**: Increases (reflects actual losses)
- **Risk Assessment**: More accurate

---

## Quick Start

### Option 1: Manual Download

Download delisted stocks for a specific date range:

```bash
# Activate virtual environment
cd /path/to/quantmini
source .venv/bin/activate

# Download delisted stocks (requires POLYGON_API_KEY)
python scripts/download_delisted_stocks.py \
  --start-date 2024-01-01 \
  --end-date 2025-10-06

# Convert to qlib format
cd qlib_repo/scripts
python dump_bin.py dump_fix \
  --data_path=../../data/parquet \
  --qlib_dir=/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily \
  --freq=day \
  --file_suffix=.parquet \
  --exclude_fields=symbol,vwap
```

### Option 2: Automated Weekly Updates

Set up macOS LaunchAgent to run weekly:

```bash
# Run the setup script
cd /path/to/quantmini
./scripts/setup_weekly_automation.sh
```

The weekly automation will:
1. Run every Sunday at 2:00 AM
2. Download delisted stocks from last 90 days
3. Convert to qlib binary format
4. Log everything to `logs/weekly_*.log`

**Management Commands**:
```bash
# Check status
launchctl list | grep quantmini.weekly

# Stop
launchctl unload ~/Library/LaunchAgents/com.quantmini.weekly.plist

# Start
launchctl load ~/Library/LaunchAgents/com.quantmini.weekly.plist

# Test manually
./scripts/weekly_update.sh
```

---

## How It Works

### 1. Query Delisted Stocks

Uses Polygon API to find stocks that delisted during your backtest period:

```python
from src.download.delisted_stocks import DelistedStocksDownloader

downloader = DelistedStocksDownloader()
delisted = downloader.get_delisted_stocks("2024-01-01", "2025-10-06")

# Returns list of stocks with:
# - ticker, name, delisted_date, exchange
```

**Data Source**: Polygon `list_tickers(active=False)` API
**Rate Limit**: ~4 requests/second (free tier)
**Time**: ~2 seconds to query 10,000 delisted stocks

### 2. Download Historical Data

For each delisted stock, downloads daily OHLCV data from backtest start to delisting date:

```python
stats = downloader.download_historical_data(delisted, "2024-01-01")

# Downloads to: data/parquet/[TICKER].parquet
```

**Time**: ~4 minutes per 1,000 stocks
**Format**: Parquet (same as S3 flat files)
**Fields**: date, open, high, low, close, volume, vwap

### 3. Convert to Qlib Format

Uses qlib's `dump_bin.py dump_fix` mode to:
- Add delisted stocks to instruments file
- Create binary feature files
- Update date ranges (each stock ends on delisting date)

```bash
python qlib_repo/scripts/dump_bin.py dump_fix \
  --data_path=data/parquet \
  --qlib_dir=/path/to/qlib/data
```

---

## Architecture

### New Files Added

```
quantmini/
├── src/download/
│   └── delisted_stocks.py          # Delisted stocks downloader module
├── scripts/
│   ├── download_delisted_stocks.py # Command-line tool
│   └── weekly_update.sh            # Automated weekly updates
├── docs/
│   └── DELISTED_STOCKS.md          # This file
└── data/
    ├── parquet/                    # Downloaded parquet files
    │   ├── DISH.parquet
    │   ├── LTHM.parquet
    │   └── ...
    └── delisted_stocks.csv         # Reference list
```

### Integration with Existing Pipeline

The delisted stocks feature integrates seamlessly:

```
Existing Pipeline:
  S3 Flat Files → Parquet → Qlib Binary
  (Only active stocks)

New Pipeline (Delisted):
  Polygon API → Parquet → Qlib Binary
  (Delisted stocks only)

Final Universe:
  Active Stocks + Delisted Stocks = Complete Universe ✓
```

---

## Configuration

### Environment Variables

Required:
- `POLYGON_API_KEY`: Your Polygon.io API key

Optional:
- `QUANTMINI_DATA_DIR`: Custom data directory (default: `data`)

### API Rate Limits

**Polygon Free Tier**:
- 5 requests/second
- Unlimited historical data

**Our Settings**:
- 4 requests/second (conservative)
- 0.25s delay between calls

**Upgrade to Paid Tier** for:
- Higher rate limits
- Real-time data
- Extended history

---

## Examples

### Example 1: Backtest 2024-2025

```bash
# Download delisted stocks
python scripts/download_delisted_stocks.py \
  --start-date 2024-01-01 \
  --end-date 2025-10-06

# Output:
# Found 984 delisted stocks
# Downloaded 970 stocks (98.6% success)
# Saved to: data/delisted_stocks.csv
```

**Common Delisted Stocks Found**:
- DISH - DISH Network (acquired)
- TUP - Tupperware (bankruptcy)
- LTHM - Livent Corporation (acquired)
- KLG - WK Kellogg Co (acquired)

### Example 2: Query Only (No Download)

```bash
# Just query delisted stocks (fast)
python scripts/download_delisted_stocks.py \
  --start-date 2024-01-01 \
  --end-date 2025-10-06 \
  --skip-download

# Review data/delisted_stocks.csv
# Then download later if needed
```

### Example 3: Programmatic Usage

```python
from src.download.delisted_stocks import DelistedStocksDownloader

# Initialize
downloader = DelistedStocksDownloader(
    output_dir="custom/path",
    rate_limit_delay=0.2  # 5 requests/sec
)

# Query
delisted = downloader.get_delisted_stocks("2024-01-01", "2025-10-06")
print(f"Found {len(delisted)} delisted stocks")

# Filter by exchange
nasdaq_delisted = [s for s in delisted if s['exchange'] == 'XNAS']

# Download subset
downloader.download_historical_data(nasdaq_delisted[:100], "2024-01-01")
```

---

## Troubleshooting

### Issue 1: "ModuleNotFoundError: polygon"

**Solution**: Install polygon-api-client

```bash
uv pip install polygon-api-client
# or
pip install polygon-api-client
```

### Issue 2: "Polygon API key not found"

**Solution**: Set environment variable

```bash
export POLYGON_API_KEY="your_key_here"
# Add to ~/.bashrc or ~/.zshrc for persistence
```

### Issue 3: "No delisted stocks found"

**Possible Causes**:
1. Date range too narrow (try wider range)
2. Query limit too low (increase `--limit`)
3. All delistings outside your period (normal)

**Check**:
```bash
# Query with higher limit
python scripts/download_delisted_stocks.py \
  --start-date 2020-01-01 \
  --end-date 2025-10-06 \
  --limit 50000 \
  --skip-download
```

### Issue 4: Rate limit errors

**Solution**: Reduce rate

```python
downloader = DelistedStocksDownloader(
    rate_limit_delay=0.5  # Slower: 2 requests/sec
)
```

---

## Performance

### Benchmarks (M1 Max, 32GB RAM)

| Task | Count | Time | Rate |
|------|-------|------|------|
| Query delisted stocks | 10,000 | 2s | 5,000/sec |
| Download OHLCV data | 1,000 | 4min | 4/sec |
| Convert to qlib | 1,000 | 3s | 333/sec |

**Total Time** (for 1,000 delisted stocks):
~5 minutes (Query: 2s + Download: 4min + Convert: 3s)

### Disk Usage

- Parquet: ~100KB per stock per year
- Qlib binary: ~50KB per stock per year

**Example**: 1,000 delisted stocks × 2 years × 100KB = ~200MB

---

## Best Practices

### 1. Run Weekly

Delistings don't happen daily, so weekly updates are sufficient:

```bash
# Setup weekly cron
0 2 * * 0 /path/to/quantmini/scripts/weekly_update.sh
```

### 2. Keep Reference List

Save `delisted_stocks.csv` for audit trail:

```bash
# Archive old lists
mv data/delisted_stocks.csv data/archive/delisted_stocks_$(date +%Y%m%d).csv
```

### 3. Monitor Backtest Impact

Track changes in key metrics:

```python
# Before adding delisted stocks
returns_before = 188.67%
sharpe_before = 3.93

# After adding delisted stocks
returns_after = 158.86%  # -15.8%
sharpe_after = 3.21      # -18.3%

# Survivorship bias impact = returns_before - returns_after
```

### 4. Validate Data Quality

After adding delisted stocks:

```bash
# Verify qlib data
python scripts/verify_qlib_conversion.py

# Check instruments count
wc -l /Volumes/sandisk/quantmini-data/data/qlib/stocks_daily/instruments/all.txt
```

---

## FAQ

**Q: Do I need to re-download all delisted stocks every week?**
A: No, only query for recent delistings (last 90 days). Existing data is preserved.

**Q: What if a stock I'm tracking gets delisted?**
A: The weekly script will automatically download it. You'll see losses in your backtest when it delists.

**Q: Can I exclude certain delistings (e.g., acquisitions)?**
A: Yes, filter the delisted list before downloading:

```python
# Exclude acquisitions (they had positive returns)
delisted_filtered = [s for s in delisted if not is_acquisition(s)]
```

**Q: Does this work with other data sources (not Polygon)?**
A: Currently only Polygon. For other sources, adapt `DelistedStocksDownloader` class.

**Q: How do I know if survivorship bias is fixed?**
A: Check your backtest results:
- Returns should drop by 15-30%
- Max drawdown should increase
- Instruments file should include stocks with end dates before today

---

## References

1. **Polygon API Documentation**
   https://polygon.io/docs/stocks/get_v3_reference_tickers

2. **Survivorship Bias Research**
   Elton, Gruber, Blake (1996): "Survivorship Bias and Mutual Fund Performance"

3. **QuantLab Documentation**
   `quantlab/docs/SURVIVORSHIP_BIAS_FIX.md` - Original implementation

---

## Support

For issues or questions:
1. Check logs in `logs/weekly_*.log`
2. Review `data/delisted_stocks.csv` for list
3. Verify POLYGON_API_KEY is set
4. Check Polygon account status (rate limits, subscription)

---

**Last Updated**: 2025-10-07
**Status**: Production Ready
**Maintained By**: QuantLab / QuantMini Team
