# Data Loader - Unified Data Access

The DataLoader provides a single entry point to load all tables from the partitioned screener database.

## Overview

All data in QuantMini is stored in Hive-style partitioned structure:
```
data/partitioned_screener/{table_name}/year=YYYY/month=MM/ticker=SYMBOL.parquet
```

The DataLoader provides a consistent interface to:
- Load any table by name
- Filter by tickers, date ranges, or custom expressions
- Handle schema inconsistencies automatically
- Access metadata about tables

## Available Tables

| Table | Description | Records | Size |
|-------|-------------|---------|------|
| `balance_sheets` | Quarterly balance sheets from Polygon | ~50-100 per ticker | ~10 MB |
| `income_statements` | Quarterly income statements from Polygon | ~50-100 per ticker | ~10 MB |
| `cash_flow` | Quarterly cash flow statements from Polygon | ~50-100 per ticker | ~10 MB |
| `financial_ratios` | Calculated financial ratios | ~50-100 per ticker | ~3 MB |
| `dividends` | Dividend history | Varies | ~80 MB |
| `splits` | Stock split history | Varies | ~4 MB |
| `ticker_events` | IPO, delisting, ticker changes | Varies | <1 MB |
| `related_tickers` | Related companies | Varies | <1 MB |

## Quick Start

### Basic Usage

```python
from src.utils import DataLoader

# Create loader
loader = DataLoader()

# Load financial ratios for AAPL
ratios = loader.load('financial_ratios', tickers=['AAPL'])

# Load with date filtering
ratios_2024 = loader.load('financial_ratios',
                          start_date='2024-01-01',
                          end_date='2024-12-31')
```

### Convenience Function

```python
from src.utils import load_table

# Quick load
ratios = load_table('financial_ratios', tickers=['AAPL', 'MSFT'])
```

## API Reference

### DataLoader Class

#### `__init__(base_dir='data/partitioned_screener')`

Initialize the data loader.

**Parameters:**
- `base_dir` (str|Path): Base directory containing partitioned tables

#### `list_tables() -> List[str]`

List all available tables.

**Returns:**
- List of table names

**Example:**
```python
loader = DataLoader()
tables = loader.list_tables()
# ['balance_sheets', 'cash_flow', 'dividends', 'financial_ratios', ...]
```

#### `load(table_name, tickers=None, start_date=None, end_date=None, columns=None, filter_expr=None) -> pl.DataFrame`

Load data from a partitioned table.

**Parameters:**
- `table_name` (str): Name of the table
- `tickers` (List[str], optional): Filter by ticker symbols
- `start_date` (str|date, optional): Start date filter (format: 'YYYY-MM-DD')
- `end_date` (str|date, optional): End date filter (format: 'YYYY-MM-DD')
- `columns` (List[str], optional): Select specific columns
- `filter_expr` (pl.Expr, optional): Custom Polars filter expression

**Returns:**
- Polars DataFrame

**Examples:**
```python
# Load all data for AAPL
data = loader.load('financial_ratios', tickers=['AAPL'])

# Load with date range
data = loader.load('financial_ratios',
                   start_date='2024-01-01',
                   end_date='2024-12-31')

# Load specific columns
data = loader.load('financial_ratios',
                   tickers=['AAPL'],
                   columns=['ticker', 'filing_date', 'return_on_equity'])

# Load with custom filter
import polars as pl
data = loader.load('financial_ratios',
                   filter_expr=pl.col('return_on_equity') > 50)
```

#### `load_fundamentals(tickers=None, start_date=None, end_date=None, include_balance_sheet=True, include_income_statement=True, include_cash_flow=True) -> Dict[str, pl.DataFrame]`

Load fundamentals data (balance sheet, income statement, cash flow).

**Parameters:**
- `tickers` (List[str], optional): Filter by ticker symbols
- `start_date` (str|date, optional): Start date filter
- `end_date` (str|date, optional): End date filter
- `include_balance_sheet` (bool): Include balance sheet data
- `include_income_statement` (bool): Include income statement data
- `include_cash_flow` (bool): Include cash flow data

**Returns:**
- Dictionary with keys: `balance_sheets`, `income_statements`, `cash_flow`

**Example:**
```python
fundamentals = loader.load_fundamentals(
    tickers=['AAPL', 'MSFT'],
    start_date='2024-01-01'
)

balance_sheet = fundamentals['balance_sheets']
income = fundamentals['income_statements']
cash_flow = fundamentals['cash_flow']
```

#### `load_financial_ratios(tickers=None, start_date=None, end_date=None, ratio_columns=None) -> pl.DataFrame`

Load financial ratios.

**Parameters:**
- `tickers` (List[str], optional): Filter by ticker symbols
- `start_date` (str|date, optional): Start date filter
- `end_date` (str|date, optional): End date filter
- `ratio_columns` (List[str], optional): Specific ratio columns to load

**Returns:**
- DataFrame with financial ratios

**Example:**
```python
# Load all ratios
ratios = loader.load_financial_ratios(tickers=['AAPL'])

# Load specific ratios
profitability = loader.load_financial_ratios(
    tickers=['AAPL'],
    ratio_columns=[
        'ticker', 'filing_date',
        'return_on_equity', 'return_on_assets',
        'gross_profit_margin', 'net_profit_margin'
    ]
)
```

#### `load_corporate_actions(tickers=None, start_date=None, end_date=None, action_types=None) -> pl.DataFrame`

Load corporate actions (dividends, splits, etc.).

**Parameters:**
- `tickers` (List[str], optional): Filter by ticker symbols
- `start_date` (str|date, optional): Start date filter
- `end_date` (str|date, optional): End date filter
- `action_types` (List[str], optional): Filter by action types

**Returns:**
- DataFrame with corporate actions

**Example:**
```python
# Load all corporate actions
actions = loader.load_corporate_actions(tickers=['AAPL'])

# Load only dividends
dividends = loader.load_corporate_actions(
    tickers=['AAPL'],
    action_types=['dividend']
)
```

#### `load_ticker_events(tickers=None, start_date=None, end_date=None, event_types=None) -> pl.DataFrame`

Load ticker events (IPO, ticker changes, delistings).

**Parameters:**
- `tickers` (List[str], optional): Filter by ticker symbols
- `start_date` (str|date, optional): Start date filter
- `end_date` (str|date, optional): End date filter
- `event_types` (List[str], optional): Filter by event types

**Returns:**
- DataFrame with ticker events

**Example:**
```python
events = loader.load_ticker_events(
    event_types=['ticker_change', 'ipo']
)
```

#### `get_table_info(table_name) -> Dict`

Get information about a table.

**Parameters:**
- `table_name` (str): Name of the table

**Returns:**
- Dictionary with table metadata:
  - `table_name`: Name of the table
  - `file_count`: Number of parquet files
  - `total_size_mb`: Total size in megabytes
  - `ticker_count`: Number of unique tickers
  - `tickers`: List of ticker symbols
  - `year_range`: Tuple of (min_year, max_year)

**Example:**
```python
info = loader.get_table_info('financial_ratios')
print(f"Table has {info['file_count']} files")
print(f"Covers {info['ticker_count']} tickers")
print(f"Date range: {info['year_range']}")
```

## Common Use Cases

### 1. Financial Ratio Analysis

```python
from src.utils import DataLoader
import polars as pl

loader = DataLoader()

# Load ratios for tech companies
ratios = loader.load_financial_ratios(
    tickers=['AAPL', 'MSFT', 'GOOGL', 'AMZN']
)

# Find high-ROE periods
high_roe = ratios.filter(pl.col('return_on_equity') > 40)

# Calculate average ratios by ticker
avg_ratios = (ratios
    .group_by('ticker')
    .agg([
        pl.col('return_on_equity').mean().alias('avg_roe'),
        pl.col('current_ratio').mean().alias('avg_current_ratio'),
        pl.col('debt_to_equity').mean().alias('avg_debt_to_equity')
    ])
)
```

### 2. Time Series Analysis

```python
# Load historical ratios for a single company
aapl_ratios = loader.load_financial_ratios(
    tickers=['AAPL']
).sort('filing_date')

# Plot ROE over time
import matplotlib.pyplot as plt

dates = aapl_ratios['filing_date']
roe = aapl_ratios['return_on_equity']

plt.figure(figsize=(12, 6))
plt.plot(dates, roe)
plt.title('AAPL Return on Equity Over Time')
plt.xlabel('Date')
plt.ylabel('ROE (%)')
plt.grid(True)
plt.show()
```

### 3. Screening

```python
# Find companies with strong fundamentals
strong_fundamentals = loader.load_financial_ratios(
    filter_expr=(
        (pl.col('return_on_equity') > 20) &
        (pl.col('current_ratio') > 1.5) &
        (pl.col('debt_to_equity') < 1.0)
    )
)

# Get unique tickers
strong_tickers = strong_fundamentals['ticker'].unique().to_list()
print(f"Found {len(strong_tickers)} companies with strong fundamentals")
```

### 4. Comparative Analysis

```python
# Compare AAPL vs MSFT profitability
ratios = loader.load_financial_ratios(
    tickers=['AAPL', 'MSFT'],
    start_date='2024-01-01'
)

comparison = (ratios
    .group_by('ticker')
    .agg([
        pl.col('return_on_equity').mean().alias('avg_roe'),
        pl.col('return_on_assets').mean().alias('avg_roa'),
        pl.col('gross_profit_margin').mean().alias('avg_gpm'),
        pl.col('net_profit_margin').mean().alias('avg_npm')
    ])
)

print(comparison)
```

### 5. Combining Data Sources

```python
# Load both fundamentals and ratios
fundamentals = loader.load_fundamentals(tickers=['AAPL'])
ratios = loader.load_financial_ratios(tickers=['AAPL'])

# Join on filing date
balance_sheet = fundamentals['balance_sheets']
combined = ratios.join(
    balance_sheet,
    on=['ticker', 'filing_date'],
    how='inner'
)
```

## Performance Tips

1. **Filter early**: Use `tickers` and date parameters to reduce data loaded
   ```python
   # Good - only loads AAPL data
   ratios = loader.load('financial_ratios', tickers=['AAPL'])

   # Bad - loads all data then filters
   ratios = loader.load('financial_ratios').filter(pl.col('ticker') == 'AAPL')
   ```

2. **Select specific columns**: Load only columns you need
   ```python
   ratios = loader.load('financial_ratios',
                       columns=['ticker', 'filing_date', 'return_on_equity'])
   ```

3. **Use custom filters**: Apply Polars expressions for complex filtering
   ```python
   ratios = loader.load('financial_ratios',
                       filter_expr=pl.col('return_on_equity') > 50)
   ```

## Data Schema

### Financial Ratios

Key columns:
- `ticker`: Stock ticker symbol
- `filing_date`: Date of the filing
- `fiscal_period`: Fiscal period (Q1, Q2, Q3, Q4, FY)
- `fiscal_year`: Fiscal year

**Profitability:**
- `return_on_equity`: ROE (%)
- `return_on_assets`: ROA (%)
- `return_on_invested_capital`: ROIC (%)
- `gross_profit_margin`: Gross margin (%)
- `operating_profit_margin`: Operating margin (%)
- `net_profit_margin`: Net margin (%)

**Liquidity:**
- `current_ratio`: Current assets / Current liabilities
- `quick_ratio`: (Current assets - Inventory) / Current liabilities
- `working_capital`: Current assets - Current liabilities

**Leverage:**
- `debt_to_equity`: Total debt / Total equity
- `debt_to_assets`: Total debt / Total assets
- `equity_multiplier`: Total assets / Total equity
- `interest_coverage_ratio`: EBIT / Interest expense

**Efficiency:**
- `asset_turnover`: Revenue / Total assets
- `inventory_turnover`: COGS / Average inventory
- `receivables_turnover`: Revenue / Average receivables
- `days_sales_outstanding`: 365 / Receivables turnover

**Cash Flow:**
- `operating_cash_flow_ratio`: Operating cash flow / Current liabilities
- `free_cash_flow`: Operating cash flow - Capital expenditures
- `cash_flow_to_debt`: Operating cash flow / Total debt

### Fundamentals (Raw Polygon Data)

Note: Fundamentals data is stored in Polygon's nested format. Use the FinancialRatiosDownloader
to calculate ratios from raw fundamentals.

Metadata columns:
- `tickers`: List of ticker symbols
- `filing_date`: Date of filing
- `fiscal_period`: Fiscal period (Q1, Q2, Q3, FY)
- `fiscal_year`: Fiscal year
- `cik`: CIK number
- `financials`: Nested struct with balance sheet/income statement/cash flow data

## Examples

See `examples/data_loader_example.py` for comprehensive examples.

## Notes

- The DataLoader automatically handles schema inconsistencies in Polygon data using `diagonal_relaxed` concatenation
- Date filtering works on any column named `filing_date`, `date`, `timestamp`, or `start_date`
- All tickers are automatically uppercased for consistency
- Missing columns in filtered results are added as null values
