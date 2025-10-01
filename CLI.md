# QuantMini CLI

Command-line interface for the QuantMini data pipeline.

## Installation

```bash
pip install quantmini
```

After installation, the `quantmini` command will be available globally.

## Quick Start

```bash
# Initialize configuration
quantmini config init

# Edit credentials (add your Polygon.io API keys)
nano config/credentials.yaml

# Run daily pipeline
quantmini pipeline daily --data-type stocks_daily

# Query data
quantmini data query --data-type stocks_daily \
  --symbols AAPL MSFT \
  --fields date close volume \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

## Commands

### Config Management

#### `quantmini config init`
Initialize configuration files.

```bash
quantmini config init

# Force overwrite existing files
quantmini config init --force
```

Creates:
- `config/credentials.yaml` - API credentials template
- `config/pipeline_config.yaml` - Pipeline configuration
- `config/system_profile.yaml` - System hardware profile

#### `quantmini config show`
Show current configuration.

```bash
quantmini config show
```

#### `quantmini config profile`
Show system hardware profile.

```bash
quantmini config profile
```

#### `quantmini config set`
Set configuration value.

```bash
quantmini config set pipeline.mode streaming
quantmini config set processing.chunk_size 50000
```

#### `quantmini config get`
Get configuration value.

```bash
quantmini config get pipeline.mode
quantmini config get processing.use_polars
```

---

### Data Operations

#### `quantmini data download`
Download data from Polygon.io S3.

```bash
quantmini data download \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --output data/raw
```

**Options:**
- `-t, --data-type`: Type of data (`stocks_daily`, `stocks_minute`, `options_daily`, `options_minute`)
- `-s, --start-date`: Start date (YYYY-MM-DD)
- `-e, --end-date`: End date (YYYY-MM-DD)
- `-o, --output`: Output directory (default: `data/raw`)

#### `quantmini data ingest`
Ingest data into Parquet format.

```bash
quantmini data ingest \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --mode polars \
  --incremental
```

**Options:**
- `-t, --data-type`: Type of data
- `-s, --start-date`: Start date
- `-e, --end-date`: End date
- `-m, --mode`: Ingestion mode (`polars` or `streaming`, default: `polars`)
- `--incremental/--full`: Incremental or full ingestion (default: incremental)

**Modes:**
- `polars`: 5-10x faster, recommended for most systems
- `streaming`: Memory-efficient, for systems with <32GB RAM

#### `quantmini data enrich`
Add features to ingested data.

```bash
quantmini data enrich \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --incremental
```

**Features Added:**
- Returns (1d, 5d, 20d)
- Alpha factors
- Price features
- Volume features
- Volatility

#### `quantmini data convert`
Convert enriched data to Qlib binary format.

```bash
quantmini data convert \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --incremental
```

Outputs Qlib-compatible binary format:
- `data/binary/{data_type}/instruments/all.txt`
- `data/binary/{data_type}/calendars/day.txt`
- `data/binary/{data_type}/features/{symbol}/{feature}.bin`

#### `quantmini data query`
Query enriched data.

```bash
quantmini data query \
  --data-type stocks_daily \
  --symbols AAPL MSFT GOOGL \
  --fields date close volume return_1d alpha_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --output results.csv
```

**Options:**
- `-t, --data-type`: Type of data
- `-s, --symbols`: Symbols to query (can specify multiple)
- `-f, --fields`: Fields to query (can specify multiple)
- `--start-date`: Start date
- `--end-date`: End date
- `-o, --output`: Output CSV file (default: print to stdout)
- `-l, --limit`: Limit number of rows

**Example with multiple symbols/fields:**
```bash
quantmini data query \
  --data-type stocks_daily \
  -s AAPL -s MSFT -s GOOGL -s AMZN \
  -f date -f close -f volume -f return_1d \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  -o portfolio_data.csv
```

#### `quantmini data status`
Show ingestion status.

```bash
# Show all data types
quantmini data status

# Show specific data type
quantmini data status --data-type stocks_daily

# Filter by date range
quantmini data status \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

---

### Pipeline Workflows

#### `quantmini pipeline run`
Run complete pipeline: ingest → enrich → convert.

```bash
quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

**Options:**
- `-t, --data-type`: Type of data
- `-s, --start-date`: Start date
- `-e, --end-date`: End date
- `--skip-ingest`: Skip ingestion step
- `--skip-enrich`: Skip enrichment step
- `--skip-convert`: Skip conversion step

**Example - only enrich and convert:**
```bash
quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --skip-ingest
```

#### `quantmini pipeline daily`
Run daily update.

```bash
# Update yesterday's data
quantmini pipeline daily --data-type stocks_daily

# Update last 3 days
quantmini pipeline daily --data-type stocks_daily --days 3
```

**Options:**
- `-t, --data-type`: Type of data
- `-d, --days`: Number of days to update (default: 1)

Automatically:
1. Ingests recent data
2. Adds features
3. Converts to Qlib format

#### `quantmini pipeline backfill`
Backfill missing data.

```bash
quantmini pipeline backfill \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

Automatically detects and processes only missing dates.

---

### Validation

#### `quantmini validate binary`
Validate Qlib binary format conversion.

```bash
quantmini validate binary --data-type stocks_daily
```

Checks:
- Instruments file format
- Calendar file format
- Binary files exist and are valid
- Metadata file exists

#### `quantmini validate parquet`
Validate Parquet data integrity.

```bash
quantmini validate parquet --data-type stocks_daily
```

Shows:
- Total partitions
- Total size
- Date range
- Symbol count

#### `quantmini validate config`
Validate configuration files.

```bash
# Check configuration
quantmini validate config

# Check and fix
quantmini validate config --fix
```

---

## Common Workflows

### Initial Setup

```bash
# 1. Initialize configuration
quantmini config init

# 2. Edit credentials
nano config/credentials.yaml
# Add your Polygon.io access_key_id and secret_access_key

# 3. Run backfill for historical data
quantmini pipeline run \
  --data-type stocks_daily \
  --start-date 2020-01-01 \
  --end-date 2024-12-31

# 4. Validate conversion
quantmini validate binary --data-type stocks_daily
```

### Daily Updates

```bash
# Run as a daily cron job
0 9 * * * /usr/local/bin/quantmini pipeline daily --data-type stocks_daily
```

Or manually:
```bash
quantmini pipeline daily --data-type stocks_daily
```

### Custom Pipeline

```bash
# 1. Download data
quantmini data download \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# 2. Ingest with custom settings
quantmini data ingest \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --mode streaming  # Use for <32GB RAM systems

# 3. Add features
quantmini data enrich \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# 4. Convert to Qlib
quantmini data convert \
  --data-type stocks_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# 5. Query results
quantmini data query \
  --data-type stocks_daily \
  --symbols AAPL MSFT \
  --fields date close return_1d alpha_daily \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### Backtesting Workflow

```bash
# 1. Ensure data is available
quantmini data status --data-type stocks_daily

# 2. Query features for backtesting
quantmini data query \
  --data-type stocks_daily \
  --symbols AAPL MSFT GOOGL AMZN META \
  --fields date close return_1d alpha_daily volatility_20d \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --output backtest_data.csv

# 3. Use in your backtesting script
python my_backtest.py --data backtest_data.csv
```

---

## Environment Variables

Override configuration with environment variables:

```bash
export PIPELINE_MODE=streaming
export MAX_MEMORY_GB=16
export DATA_ROOT=/path/to/data

quantmini pipeline daily --data-type stocks_daily
```

**Available Variables:**
- `PIPELINE_MODE`: Override processing mode (`streaming`, `batch`, `parallel`)
- `MAX_MEMORY_GB`: Override max memory limit
- `LOG_LEVEL`: Override log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `DATA_ROOT`: Override data root directory

---

## Tips & Tricks

### Check System Resources

```bash
# Check system profile
quantmini config profile

# Recommended mode shown in output
```

### Monitor Progress

All commands show progress bars and status updates:
```
📊 Ingesting stocks_daily from 2024-01-01 to 2024-01-31...
   Mode: polars, Incremental: True
Downloading  [####################################]  100%
✅ Ingested 21 dates
   Total records: 156,789
   Total size: 45.23 MB
   Time: 23.45s
   Success rate: 100.0%
```

### Troubleshooting

```bash
# Validate everything
quantmini validate config
quantmini validate parquet --data-type stocks_daily
quantmini validate binary --data-type stocks_daily

# Check data status
quantmini data status --data-type stocks_daily

# View configuration
quantmini config show
```

### Performance Tuning

```bash
# For systems with <32GB RAM
quantmini config set pipeline.mode streaming
quantmini config set processing.chunk_size 50000

# For high-performance systems
quantmini config set pipeline.mode batch
quantmini config set processing.use_polars true
```

---

## Help

Get help for any command:

```bash
quantmini --help
quantmini data --help
quantmini data ingest --help
quantmini pipeline --help
```

---

## Examples

See the `examples/` directory for complete Python examples using the CLI's underlying modules.

---

## Support

- Documentation: https://quantmini.readthedocs.io/
- Issues: https://github.com/nittygritty-zzy/quantmini/issues
- Email: zheyuan28@gmail.com
