# Polygon REST API Data Pipeline

High-performance data pipeline for downloading reference data, corporate actions, fundamentals, and economy data from Polygon.io REST API.

## Features

- **High Performance**: HTTP/2 connection pooling, 100+ concurrent requests
- **Unlimited API Rate**: Optimized for unlimited tier with massive parallelization
- **Comprehensive Coverage**: Reference data, corporate actions, fundamentals, economy indicators
- **Efficient Storage**: Parquet format with zstd compression
- **Parallel Pagination**: Fetch multiple pages simultaneously
- **Batch Operations**: Download data for multiple tickers in parallel

## Architecture

### Components

1. **PolygonRESTClient** - High-performance HTTP client using httpx
   - HTTP/2 support
   - Connection pooling (200 connections)
   - Automatic retry with exponential backoff
   - Parallel pagination

2. **ReferenceDataDownloader** - Ticker metadata and relationships
   - Ticker types
   - Related tickers
   - Ticker details

3. **CorporateActionsDownloader** - Corporate actions data
   - Dividends (`/v3/reference/dividends`)
   - Stock splits (`/v3/reference/splits`)
   - IPOs (`/vX/reference/ipos`)
   - Ticker events (`/vX/reference/tickers/{id}/events`)

4. **FundamentalsDownloader** - Financial statements & short data
   - Balance sheets
   - Cash flow statements
   - Income statements
   - Short interest (`/stocks/v1/short-interest` - 2 year max)
   - Short volume (`/stocks/v1/short-volume` - all history)

5. **EconomyDataDownloader** - Macroeconomic indicators
   - Treasury yields
   - Inflation data
   - Inflation expectations

## Installation

Ensure httpx is installed:

```bash
uv add httpx
```

## Configuration

Add your Polygon API key to `config/credentials.yaml`:

```yaml
polygon:
  api_key: "your_api_key_here"
```

## CLI Usage

### Reference Data

#### Download Ticker Types
```bash
# All ticker types
quantmini polygon ticker-types

# Filter by asset class
quantmini polygon ticker-types --asset-class stocks

# Filter by locale
quantmini polygon ticker-types --locale us
```

#### Download Related Tickers
```bash
# Single ticker
quantmini polygon related-tickers AAPL

# Multiple tickers (parallel)
quantmini polygon related-tickers AAPL MSFT GOOGL AMZN TSLA
```

### Corporate Actions

#### Download All Corporate Actions
```bash
# For specific ticker
quantmini polygon corporate-actions --ticker AAPL

# Date range
quantmini polygon corporate-actions --start-date 2024-01-01 --end-date 2024-12-31

# All tickers in date range
quantmini polygon corporate-actions --start-date 2024-01-01
```

### Fundamentals

#### Download Financial Statements
```bash
# Quarterly financials for single ticker
quantmini polygon fundamentals AAPL

# Annual financials
quantmini polygon fundamentals AAPL --timeframe annual

# Multiple tickers (parallel)
quantmini polygon fundamentals AAPL MSFT GOOGL --timeframe quarterly
```

#### Download Short Interest
```bash
# Single ticker
quantmini polygon short-interest AAPL

# Multiple tickers (parallel)
quantmini polygon short-interest AAPL MSFT GOOGL AMZN TSLA
```

#### Download Short Volume
```bash
# Single ticker
quantmini polygon short-volume AAPL

# Multiple tickers (parallel)
quantmini polygon short-volume AAPL MSFT GOOGL AMZN TSLA
```

#### Download Both Short Interest and Short Volume
```bash
# Download both short interest and volume for multiple tickers
quantmini polygon short-data AAPL MSFT GOOGL AMZN TSLA
```

### Economy Data

#### Download All Economy Indicators
```bash
# Last 90 days (default)
quantmini polygon economy

# Custom date range
quantmini polygon economy --start-date 2024-01-01 --end-date 2024-12-31

# Last 365 days
quantmini polygon economy --days 365
```

#### Download Treasury Yield Curve
```bash
# Today's yield curve
quantmini polygon yield-curve

# Specific date
quantmini polygon yield-curve --date 2024-12-31
```

## Python API Usage

### Reference Data

```python
import asyncio
from pathlib import Path
from quantmini.download import PolygonRESTClient, ReferenceDataDownloader

async def main():
    # Create client with high concurrency
    async with PolygonRESTClient(
        api_key="your_api_key",
        max_concurrent=100,  # High parallelism
        max_connections=200
    ) as client:
        # Create downloader
        downloader = ReferenceDataDownloader(
            client=client,
            output_dir=Path('data/reference')
        )

        # Download ticker types
        ticker_types = await downloader.download_ticker_types()
        print(f"Downloaded {len(ticker_types)} ticker types")

        # Download related tickers (batch)
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        related = await downloader.download_related_tickers_batch(tickers)
        print(f"Downloaded {len(related)} relationships")

        # Download ticker details (batch)
        details = await downloader.download_ticker_details_batch(tickers)
        print(f"Downloaded details for {len(details)} tickers")

asyncio.run(main())
```

### Corporate Actions

```python
from quantmini.download import CorporateActionsDownloader

async def main():
    async with PolygonRESTClient(api_key="your_api_key") as client:
        downloader = CorporateActionsDownloader(
            client=client,
            output_dir=Path('data/corporate_actions')
        )

        # Download all corporate actions for a ticker
        data = await downloader.download_all_corporate_actions(
            ticker='AAPL',
            start_date='2024-01-01'
        )

        print(f"Dividends: {len(data['dividends'])}")
        print(f"Splits: {len(data['splits'])}")
        print(f"Events: {len(data['events'])}")

asyncio.run(main())
```

### Fundamentals

```python
from quantmini.download import FundamentalsDownloader

async def main():
    async with PolygonRESTClient(api_key="your_api_key") as client:
        downloader = FundamentalsDownloader(
            client=client,
            output_dir=Path('data/fundamentals')
        )

        # Download all financials for one ticker (including short data)
        data = await downloader.download_all_fundamentals_extended(
            'AAPL',
            timeframe='quarterly',
            include_short_data=True
        )

        print(f"Balance sheets: {len(data['balance_sheets'])}")
        print(f"Cash flow: {len(data['cash_flow'])}")
        print(f"Income statements: {len(data['income_statements'])}")
        print(f"Short interest: {len(data['short_interest'])}")
        print(f"Short volume: {len(data['short_volume'])}")

        # Download short data for multiple tickers
        tickers = ['AAPL', 'MSFT', 'GOOGL']
        short_data = await downloader.download_short_data_batch(tickers)

        print(f"\nShort data across all tickers:")
        print(f"Short interest: {len(short_data['short_interest'])}")
        print(f"Short volume: {len(short_data['short_volume'])}")

        # Batch download financials (traditional)
        batch_data = await downloader.download_financials_batch(tickers, 'annual')

        print(f"\nTotal records across all tickers:")
        print(f"Balance sheets: {len(batch_data['balance_sheets'])}")
        print(f"Cash flow: {len(batch_data['cash_flow'])}")
        print(f"Income statements: {len(batch_data['income_statements'])}")

asyncio.run(main())
```

### Economy Data

```python
from quantmini.download import EconomyDataDownloader
from datetime import date, timedelta

async def main():
    async with PolygonRESTClient(api_key="your_api_key") as client:
        downloader = EconomyDataDownloader(
            client=client,
            output_dir=Path('data/economy')
        )

        # Download all economy data
        end_date = date.today()
        start_date = end_date - timedelta(days=90)

        data = await downloader.download_all_economy_data(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

        print(f"Treasury yields: {len(data['treasury_yields'])}")
        print(f"Inflation: {len(data['inflation'])}")
        print(f"Expectations: {len(data['inflation_expectations'])}")

        # Download yield curve for specific date
        curve = await downloader.download_treasury_curve('2024-12-31')
        print(f"\nYield curve: {len(curve)} maturities")

asyncio.run(main())
```

## Performance Optimization

### Concurrency Settings

For maximum performance with unlimited API rate:

```python
client = PolygonRESTClient(
    api_key="your_api_key",
    max_concurrent=200,      # Very high for unlimited
    max_connections=400,     # 2x concurrent requests
    enable_http2=True        # Use HTTP/2
)
```

### Batch Operations

Always use batch operations for multiple tickers:

```python
# ✅ GOOD: Parallel batch request
related = await downloader.download_related_tickers_batch(['AAPL', 'MSFT', 'GOOGL'])

# ❌ BAD: Sequential requests
for ticker in tickers:
    related = await downloader.download_related_tickers(ticker)
```

### Parallel Pagination

The client automatically fetches multiple pages in parallel:

```python
# Fetches 10 pages at a time
results = await client.paginate_all(
    '/v3/reference/dividends',
    params={'ticker': 'AAPL'},
    parallel_pages=10
)
```

## Data Storage

All data is saved to Parquet format with:
- **Compression**: zstd (high compression ratio)
- **Schema**: Polars DataFrames
- **Naming**: Timestamped files (e.g., `dividends_20241231_143022.parquet`)

### Directory Structure

```
data/
├── reference/
│   ├── ticker_types_20241231_120000.parquet
│   ├── related_tickers_20241231_120500.parquet
│   └── ticker_details_20241231_121000.parquet
├── corporate_actions/
│   ├── dividends_20241231_130000.parquet
│   ├── stock_splits_20241231_130100.parquet
│   └── ticker_events_20241231_130200.parquet
├── fundamentals/
│   ├── balance_sheets_20241231_140000.parquet
│   ├── cash_flow_20241231_140100.parquet
│   └── income_statements_20241231_140200.parquet
└── economy/
    ├── treasury_yields_20241231_150000.parquet
    ├── inflation_20241231_150100.parquet
    └── inflation_expectations_20241231_150200.parquet
```

## API Reference

### PolygonRESTClient

```python
class PolygonRESTClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        max_concurrent: int = 100,
        max_connections: int = 200,
        max_retries: int = 3,
        timeout: int = 30,
        enable_http2: bool = True
    )

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]

    async def paginate_all(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: Optional[int] = None,
        parallel_pages: int = 10
    ) -> List[Dict[str, Any]]

    async def batch_request(
        self,
        requests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]
```

### ReferenceDataDownloader

```python
class ReferenceDataDownloader:
    async def download_ticker_types(
        self,
        asset_class: Optional[str] = None,
        locale: Optional[str] = None
    ) -> pl.DataFrame

    async def download_related_tickers_batch(
        self,
        tickers: List[str]
    ) -> pl.DataFrame

    async def download_ticker_details_batch(
        self,
        tickers: List[str],
        date: Optional[str] = None
    ) -> pl.DataFrame
```

### CorporateActionsDownloader

```python
class CorporateActionsDownloader:
    async def download_dividends(
        self,
        ticker: Optional[str] = None,
        ex_dividend_date: Optional[str] = None,
        ...
    ) -> pl.DataFrame

    async def download_stock_splits(
        self,
        ticker: Optional[str] = None,
        execution_date: Optional[str] = None,
        ...
    ) -> pl.DataFrame

    async def download_all_corporate_actions(
        self,
        ticker: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]
```

### FundamentalsDownloader

```python
class FundamentalsDownloader:
    async def download_all_financials(
        self,
        ticker: str,
        timeframe: str = 'quarterly'
    ) -> Dict[str, pl.DataFrame]

    async def download_all_fundamentals_extended(
        self,
        ticker: str,
        timeframe: str = 'quarterly',
        include_short_data: bool = True
    ) -> Dict[str, pl.DataFrame]

    async def download_financials_batch(
        self,
        tickers: List[str],
        timeframe: str = 'quarterly'
    ) -> Dict[str, pl.DataFrame]

    async def download_short_interest(
        self,
        ticker: str,
        limit: int = 100
    ) -> pl.DataFrame

    async def download_short_volume(
        self,
        ticker: str,
        limit: int = 100
    ) -> pl.DataFrame

    async def download_short_data_batch(
        self,
        tickers: List[str],
        limit: int = 100
    ) -> Dict[str, pl.DataFrame]
```

### EconomyDataDownloader

```python
class EconomyDataDownloader:
    async def download_all_economy_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]

    async def download_treasury_curve(
        self,
        date: str
    ) -> pl.DataFrame
```

## Financial Ratios Calculation

Since the Polygon API ratios endpoint is not accessible, we compute financial ratios locally using the `FinancialRatiosCalculator`.

### Usage

```python
from quantmini.features import FinancialRatiosCalculator
from quantmini.download import FundamentalsDownloader

async def main():
    async with PolygonRESTClient(api_key="your_api_key") as client:
        # Download financials
        downloader = FundamentalsDownloader(client, Path('data/fundamentals'))
        data = await downloader.download_all_financials('AAPL', timeframe='quarterly')

        # Calculate ratios
        calculator = FinancialRatiosCalculator()
        ratios = calculator.calculate_all_ratios(
            balance_sheet=data['balance_sheets'],
            income_statement=data['income_statements'],
            cash_flow=data['cash_flow']
        )

        print(f"Calculated {len(ratios)} periods of ratios")
        print(ratios.select([
            'ticker', 'fiscal_year', 'fiscal_period',
            'gross_profit_margin', 'net_profit_margin',
            'current_ratio', 'debt_to_equity', 'return_on_equity'
        ]))

asyncio.run(main())
```

### Computed Ratios

**Profitability (6 ratios)**
- Gross Profit Margin
- Operating Profit Margin
- Net Profit Margin
- Return on Assets (ROA)
- Return on Equity (ROE)
- Return on Invested Capital (ROIC)

**Liquidity (4 ratios)**
- Current Ratio
- Quick Ratio
- Cash Ratio
- Working Capital

**Leverage/Solvency (4 ratios)**
- Debt to Equity
- Debt to Assets
- Equity Multiplier
- Interest Coverage Ratio

**Efficiency/Activity (4 ratios)**
- Asset Turnover
- Inventory Turnover
- Receivables Turnover
- Days Sales Outstanding

**Cash Flow (3 ratios)**
- Operating Cash Flow Ratio
- Free Cash Flow
- Cash Flow to Debt

**Valuation (4 ratios, if market data provided)**
- P/E Ratio
- P/B Ratio
- P/S Ratio
- EV/EBITDA (simplified)

**Growth Metrics**
- Revenue Growth (YoY/QoQ)
- Net Income Growth
- Asset Growth
- Equity Growth

## Examples

See individual module files for more examples:
- `src/download/reference_data.py`
- `src/download/corporate_actions.py`
- `src/download/fundamentals.py`
- `src/download/economy.py`
- `src/features/financial_ratios.py`

Each module has a `main()` function with usage examples.

## Performance Benchmarks

With unlimited API rate and optimal settings:
- **Related tickers**: 100 tickers in ~5 seconds
- **Financials**: 10 tickers (all statements) in ~10 seconds
- **Treasury yield curve**: All maturities in ~2 seconds
- **Corporate actions**: Full history for 1 ticker in ~3 seconds

## Troubleshooting

### Rate Limiting

If you encounter rate limiting despite having unlimited tier:
1. Reduce `max_concurrent` parameter
2. Check your API plan limits
3. Add delays between batches

### Connection Errors

If you see connection errors:
1. Increase `timeout` parameter
2. Reduce `max_connections`
3. Check network stability

### Memory Issues

For large downloads:
1. Process data in batches
2. Save intermediate results
3. Use streaming where possible

## Support

For issues or questions:
1. Check Polygon.io API documentation
2. Review error messages and logs
3. Adjust concurrency settings
