# Getting Started

Welcome to QuantMini! This guide will help you get up and running with the high-performance Medallion Architecture data pipeline.

## Prerequisites

- Python 3.10 or higher
- Polygon.io API key and S3 credentials (for data ingestion)
- External storage (recommended: 500GB+ for comprehensive data)
- Basic understanding of quantitative trading concepts

## Installation

### From Source (Recommended)

```bash
# Clone repository
git clone https://github.com/nittygritty-zzy/quantmini.git
cd quantmini

# Install with uv
uv sync

# Configure credentials
cp config/credentials.yaml.example config/credentials.yaml
# Edit config/credentials.yaml with your Polygon.io API key and S3 credentials
```

## Configuration

1. **Copy the credentials template:**

```bash
cp config/credentials.yaml.example config/credentials.yaml
```

2. **Add your API credentials:**

```yaml
polygon:
  api_key: "YOUR_POLYGON_API_KEY"
  s3:
    access_key_id: "YOUR_S3_ACCESS_KEY"
    secret_access_key: "YOUR_S3_SECRET_KEY"
```

3. **Configure data paths:**

Edit `config/pipeline_config.yaml` to set your data storage location:

```yaml
data_root: /Volumes/sandisk/quantmini-lake
```

## Medallion Architecture Overview

QuantMini uses a structured data lake pattern:

```
Landing Layer          Bronze Layer         Silver Layer          Gold Layer
(Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
      ↓                    ↓                    ↓                     ↓
Polygon.io         →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
  REST API             (Schema Check)        (Indicators)         (Backtesting)
      ↓                    ↓                    ↓                     ↓
landing/              bronze/{type}/      silver/{type}/        gold/qlib/
```

## Quick Start

### 1. Download Data (Bronze Layer)

**Choose your ingestion strategy**:

#### Option A: Initial Batch Load (First Time)

```bash
# Activate environment
source .venv/bin/activate

# Download 1 year of stocks daily data
uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2025-10-18

# Download 1 year of options data
uv run python -m src.cli.main data ingest -t options_daily -s 2024-01-01 -e 2025-10-18

# Download 1 year of news articles (8+ years available)
uv run python scripts/download/download_news_1year.py --start-date 2024-01-01
```

#### Option B: Incremental Updates (Daily)

```bash
# Update last 7 days with --incremental flag (skips existing dates)
uv run python -m src.cli.main data ingest \
  -t stocks_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental

uv run python -m src.cli.main data ingest \
  -t options_daily \
  -s $(date -v-7d +%Y-%m-%d) \
  -e $(date +%Y-%m-%d) \
  --incremental

# Download yesterday's news
uv run python scripts/download/download_news_1year.py \
  --start-date $(date -v-1d +%Y-%m-%d)
```

#### Option C: Bulk Download (All Data Types)

```bash
# Download everything at once
bash scripts/bulk_download_all_data.sh
```

**See [Data Ingestion Strategies](../guides/data-ingestion-strategies.md) for complete workflows including backfill.**

### 2. Enrich with Features (Silver Layer)

Generate technical indicators:

```bash
# Generate Alpha158 features
uv run python scripts/features/generate_alpha158.py
```

### 3. Convert to Qlib Format (Gold Layer)

Prepare data for ML backtesting:

```bash
# Convert to Qlib binary format
uv run python scripts/qlib/convert_to_qlib.py
```

### 4. Query and Analyze

Use the data loader to query data:

```python
from src.utils.data_loader import DataLoader

# Initialize loader
loader = DataLoader()

# Load stocks data
df = loader.load_stocks_daily(
    symbols=['AAPL', 'MSFT'],
    start_date='2024-01-01',
    end_date='2024-12-31'
)
```

## Next Steps

- **[Batch Downloader Guide](../docs/guides/batch-downloader.md)**: High-performance parallel downloads
- **[Data Loader Guide](../docs/guides/data-loader.md)**: Query and analyze data
- **[Alpha158 Features](../docs/guides/ALPHA158_FEATURES.md)**: Generate technical indicators
- **[API Reference](api/index.rst)**: Explore the full API documentation

## Common Issues

### Issue: Polygon API rate limits

Use batch downloaders for efficient parallel requests:

```bash
uv run python scripts/download/download_ticker_events_optimized.py
```

### Issue: External drive setup

If using an external drive, set copy mode for uv:

```bash
export UV_LINK_MODE=copy
```

### Issue: Missing data

Check download status:

```bash
uv run python scripts/check_download_status.py
```

## Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/nittygritty-zzy/quantmini/issues)
- **Documentation**: See `docs/` directory for comprehensive guides
- **Examples**: [Example scripts](https://github.com/nittygritty-zzy/quantmini/tree/main/examples)
