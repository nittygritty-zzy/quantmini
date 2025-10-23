# QuantMini Quick Start

## Daily Data Pipeline - Single Command Entry Point

The **`scripts/daily_update.sh`** script is your single entry point for all data pipeline operations.

### Quick Start

```bash
# Process yesterday's data (default)
./scripts/daily_update.sh

# Dry run to preview
./scripts/daily_update.sh --dry-run
```

That's it! This runs the complete pipeline: **Bronze → Silver → Gold**

### Common Usage

```bash
# Process specific date
./scripts/daily_update.sh --date 2025-09-15

# Backfill last week
./scripts/daily_update.sh --days-back 7

# Only transformations (bronze already populated)
./scripts/daily_update.sh --skip-bronze

# Only transformations and enrichment (skip bronze)
./scripts/daily_update.sh --skip-bronze
```

### What It Does

#### Bronze Layer (Data Ingestion)
- ✓ Stocks daily & minute data (S3 → Parquet)
- ✓ Options daily & minute data (S3 → Parquet)
- ✓ Fundamentals (Polygon API)
- ✓ Financial ratios (calculated)
- ✓ Corporate actions:
  - Dividends (`/v3/reference/dividends`)
  - Splits (`/v3/reference/splits`)
  - IPOs (`/vX/reference/ipos`)
  - Ticker changes (`/vX/reference/tickers/{id}/events`)
- ✓ Short data:
  - Short interest (`/stocks/v1/short-interest` - 2 year max)
  - Short volume (`/stocks/v1/short-volume` - all history)
- ✓ News articles
- ✓ Reference data (weekly on Mondays)

#### Silver Layer (Transformations)
- ✓ Financial ratios → consolidated & partitioned
- ✓ Corporate actions → unified table
- ✓ Fundamentals → flattened wide format

#### Gold Layer (Enrichment)
- ✓ Stocks daily → enriched + Qlib binary
- ✓ Stocks minute → enriched Parquet
- ✓ Options daily → enriched Parquet

### Automated Daily Runs

```bash
# Edit crontab
crontab -e

# Add this line (runs weekdays at 6 AM) - update paths to your installation
0 6 * * 1-5 /path/to/quantmini/scripts/daily_update.sh >> /path/to/quantmini/logs/cron.log 2>&1
```

### Logs

All operations are logged to:
```
logs/daily_update_YYYYMMDD_HHMMSS.log
```

### Using Individual CLI Commands

If you need granular control, all commands are available via the `quantmini` CLI:

#### Bronze Layer
```bash
# Stocks & Options
quantmini data ingest --data-type stocks_daily --start-date 2025-09-01 --end-date 2025-09-30

# Fundamentals
quantmini polygon fundamentals AAPL MSFT GOOGL --timeframe quarterly

# Corporate Actions (Dividends, Splits, IPOs)
quantmini polygon corporate-actions --start-date 2025-09-01 --end-date 2025-09-30 --include-ipos

# Ticker Events (Symbol changes)
quantmini polygon ticker-events AAPL MSFT GOOGL

# Short Interest & Volume
quantmini polygon short-data AAPL MSFT GOOGL --settlement-date-gte 2025-09-01 --date-gte 2025-09-01
```

#### Silver Layer
```bash
# Financial Ratios
quantmini transform financial-ratios

# Corporate Actions
quantmini transform corporate-actions

# Fundamentals Flattening
quantmini transform fundamentals
```

#### Gold Layer
```bash
# Enrichment
quantmini data enrich --data-type stocks_daily --start-date 2025-09-01 --end-date 2025-09-30

# Qlib Conversion
quantmini data convert --data-type stocks_daily --start-date 2025-09-01 --end-date 2025-09-30
```

### Data Lake Structure

After running the pipeline, your data lake will look like this:

```
$HOME/quantlake/          # Default location (set via QUANTLAKE_ROOT env var)
├── parquet/              # Bronze: S3 Flatfiles
│   ├── stocks_daily/
│   ├── stocks_minute/
│   ├── options_daily/
│   └── options_minute/
├── fundamentals/         # Bronze: Polygon API
│   ├── balance_sheets/
│   ├── income_statements/
│   ├── cash_flow/
│   └── financial_ratios/
├── corporate_actions/    # Bronze: Polygon API
│   ├── dividends/
│   ├── splits/
│   ├── ipos/
│   └── ticker_events/
├── news/                 # Bronze: Polygon API
├── silver/               # Silver: Transformed
│   ├── financial_ratios/
│   ├── corporate_actions/
│   └── fundamentals_wide/
├── enriched/             # Gold: Enriched Parquet
│   ├── stocks_daily/
│   ├── stocks_minute/
│   └── options_daily/
└── qlib/                 # Gold: Qlib Binary
    ├── instruments/
    └── features/
```

### Configuration

Set your data lake location (optional, defaults to `$HOME/quantlake`):

```bash
# Add to your ~/.bashrc or ~/.zshrc
export QUANTLAKE_ROOT=/path/to/your/quantlake
```

### Customization

Edit `scripts/daily_update.sh` to customize:
- **Tickers**: Modify `DEFAULT_FUNDAMENTAL_TICKERS` array (line 39)
- **News limit**: Change `--limit 1000` parameter (line 264)
- **Data path**: Set `QUANTLAKE_ROOT` environment variable

### Help

```bash
# Script help
./scripts/daily_update.sh --help

# CLI help
quantmini --help
quantmini data --help
quantmini polygon --help
quantmini transform --help
```

### Troubleshooting

If something fails:
1. Check the log file (path shown at start)
2. Run in dry-run mode: `./scripts/daily_update.sh --dry-run`
3. Run specific layer only:
   - `--skip-silver --skip-gold` (bronze only)
   - `--skip-bronze --skip-gold` (silver only)
   - `--skip-bronze --skip-silver` (gold only)

### Next Steps

1. Run initial backfill: `./scripts/daily_update.sh --days-back 30`
2. Set up cron for daily automation
3. Query data with `quantmini data query`
4. Build strategies with Qlib data

---

For detailed documentation, see `scripts/README.md` and `docs/`.
