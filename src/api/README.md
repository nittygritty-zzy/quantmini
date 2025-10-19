# Polygon API Integration

This module provides direct access to Polygon.io REST API for refreshing real-time market data.

## Overview

The API module is separate from the flat file pipeline (`src/download`) and provides:
- Real-time data fetching from Polygon REST API
- Support for stocks and options (daily and minute bars)
- Automatic rate limiting and retry logic
- Async/await for high-performance concurrent requests

## Architecture

```
src/api/
├── __init__.py           # Module exports
├── client.py            # Base API client with authentication
├── stocks.py            # Stock data fetcher
├── options.py           # Options data fetcher
└── README.md            # This file
```

## Usage

### Command Line

The easiest way to use the API is through the CLI:

```bash
# Test connection
quantmini api test-connection

# Fetch daily stock bars
quantmini api fetch-stocks-daily -t AAPL -t MSFT -s 2025-01-01

# Fetch minute stock bars (small date range recommended)
quantmini api fetch-stocks-minute -t AAPL -s 2025-01-15 -e 2025-01-15

# Fetch options data
quantmini api fetch-options-daily -u AAPL -s 2025-01-01

# See all available commands
quantmini api --help
```

### Python API

For programmatic access:

```python
import asyncio
from datetime import date
from src.api import PolygonAPIClient, StocksAPIFetcher

async def fetch_data():
    # Initialize client
    async with PolygonAPIClient(api_key="your_api_key") as client:
        fetcher = StocksAPIFetcher(client)

        # Fetch daily bars
        df = await fetcher.fetch_daily_bars(
            tickers=['AAPL', 'MSFT', 'GOOGL'],
            from_date=date(2025, 1, 1),
            to_date=date(2025, 1, 31),
            adjusted=True
        )

        print(f"Fetched {len(df)} bars")
        print(df.head())

        # Save to parquet
        from pathlib import Path
        fetcher.save_to_parquet(df, Path('output.parquet'))

# Run
asyncio.run(fetch_data())
```

## Configuration

### API Key

Set your Polygon API key in one of two ways:

1. **Environment variable** (recommended):
   ```bash
   export POLYGON_API_KEY=your_api_key_here
   ```

2. **credentials.yaml**:
   ```yaml
   polygon:
     api_key: your_api_key_here
   ```

### Rate Limits

Configure rate limits in `config/pipeline_config.yaml`:

```yaml
source:
  api:
    enabled: true
    base_url: "https://api.polygon.io"
    max_retries: 3
    timeout_seconds: 30
    rate_limit_calls: 5      # Calls per second (adjust for your tier)
    rate_limit_period: 1.0   # Seconds
```

**API Tier Limits:**
- Free: 5 calls/minute
- Starter: 5 calls/second
- Developer: 100 calls/second
- Advanced: 1000 calls/second

## Data Output (Medallion Architecture)

API data integrates with the Medallion Architecture:

```
data/
├── landing/               # Landing Layer
│   └── polygon-api/      # API-fetched data (timestamped)
│       ├── stocks_daily/
│       ├── stocks_minute/
│       ├── options_daily/
│       └── options_minute/
├── bronze/               # Bronze Layer (validated Parquet)
├── silver/               # Silver Layer (feature-enriched)
└── gold/                 # Gold Layer (ML-ready)
    └── qlib/
```

**API Data Flow:**
1. Fetch from Polygon API → `landing/polygon-api/`
2. Ingest to validated Parquet → `bronze/`
3. Enrich with features → `silver/`
4. Convert to Qlib binary → `gold/qlib/`

Output files are timestamped:
```
landing/polygon-api/stocks_daily/stocks_daily_2025-01-01_2025-01-31_20251017_143022.parquet
```

## API vs S3 Flat Files

| Feature | API (this module) | S3 Flat Files |
|---------|------------------|---------------|
| **Use Case** | Real-time refresh, specific tickers | Bulk historical data |
| **Speed** | Slower (API rate limits) | Faster (parallel downloads) |
| **Freshness** | Near real-time | T+1 (next day) |
| **Cost** | API calls count | Bandwidth only |
| **Landing Path** | `landing/polygon-api/` | `landing/polygon-s3/` |
| **Best For** | Daily updates, live trading | Initial backfill, research |

## Integration with Medallion Pipeline

API data integrates seamlessly with the Medallion Architecture:

1. **Fetch to Landing Layer:**
   ```bash
   quantmini api fetch-stocks-daily -t AAPL -s 2025-01-17
   # Output: landing/polygon-api/stocks_daily/*.parquet
   ```

2. **Ingest to Bronze Layer:**
   ```bash
   quantmini data ingest -t stocks_daily -s 2025-01-17 -e 2025-01-17
   # Validates and moves to: bronze/stocks_daily/
   ```

3. **Enrich to Silver Layer:**
   ```bash
   quantmini data enrich -t stocks_daily -s 2025-01-17 -e 2025-01-17
   # Adds features, outputs to: silver/stocks_daily/
   ```

4. **Convert to Gold Layer:**
   ```bash
   quantmini data convert -t stocks_daily -s 2025-01-17 -e 2025-01-17
   # ML-ready binary: gold/qlib/stocks_daily/
   ```

**Or use the complete pipeline:**
```bash
quantmini pipeline run -t stocks_daily -s 2025-01-17 -e 2025-01-17
# Automatically runs: Landing → Bronze → Silver → Gold
```

## Module Classes

### PolygonAPIClient

Base HTTP client for all API requests.

**Features:**
- Async HTTP with connection pooling
- Automatic rate limiting
- Exponential backoff retry
- Error handling

### StocksAPIFetcher

Fetch stock market data.

**Methods:**
- `fetch_daily_bars()` - Daily OHLCV
- `fetch_minute_bars()` - Minute OHLCV
- `get_latest_trading_day()` - Find latest trading date

### OptionsAPIFetcher

Fetch options market data.

**Methods:**
- `fetch_options_contracts()` - Discover contracts
- `fetch_daily_bars()` - Daily OHLCV for contracts
- `fetch_minute_bars()` - Minute OHLCV for contracts
- `fetch_options_for_underlyings()` - Complete workflow

## Performance Tips

1. **Batch requests**: Fetch multiple tickers in one call
2. **Rate limits**: Upgrade API tier for higher throughput
3. **Date ranges**: Use smaller ranges for minute data
4. **Concurrency**: The client handles parallel requests automatically
5. **Caching**: Save fetched data to avoid redundant API calls

## Troubleshooting

### API Key Not Found
```
❌ Error: Polygon API key not found.
```
**Solution**: Set `POLYGON_API_KEY` env var or add to credentials.yaml

### Rate Limit Errors
```
Rate limit reached, sleeping for 0.5s
```
**Solution**: Normal behavior. Adjust rate limits in config or upgrade API tier.

### No Data Returned
```
❌ No data fetched
```
**Possible causes:**
- Invalid ticker symbol
- Date range has no trading days
- Data not available for that period

### Connection Timeout
```
Request failed after 3 attempts
```
**Solution**: Check internet connection and Polygon API status

## Examples

See `examples/` directory for complete examples:
- `examples/api_fetch_example.py` - Basic API usage
- `examples/api_to_pipeline_example.py` - Integration with pipeline

## API Documentation

Full Polygon.io API documentation:
- https://polygon.io/docs/stocks
- https://polygon.io/docs/options

## Support

- GitHub Issues: https://github.com/nittygritty-zzy/quantmini/issues
- Polygon Support: https://polygon.io/contact
