# Polygon API Migration Plan: v1 (Deprecated) → v2 (Official)

## Executive Summary

**Goal**: Migrate from deprecated `/vX/reference/financials` to official v2 endpoints while maintaining zero downtime.

**Strategy**: Create parallel v2 pipeline, validate data parity, then cutover when ready.

**Timeline**: 2-3 weeks (phased approach)

**Risk Level**: LOW (parallel operation allows safe rollback)

---

## Table of Contents

1. [Migration Architecture](#migration-architecture)
2. [Phase 1: Preparation](#phase-1-preparation)
3. [Phase 2: Implementation](#phase-2-implementation)
4. [Phase 3: Testing & Validation](#phase-3-testing--validation)
5. [Phase 4: Parallel Operation](#phase-4-parallel-operation)
6. [Phase 5: Cutover](#phase-5-cutover)
7. [Phase 6: Cleanup](#phase-6-cleanup)
8. [Rollback Strategy](#rollback-strategy)

---

## Migration Architecture

### Parallel Pipeline Approach

```
┌─────────────────────────────────────────────────────────────┐
│                    CURRENT SYSTEM (v1)                       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ /vX/reference/financials (deprecated)                │   │
│  │   ↓                                                  │   │
│  │ FundamentalsDownloader                               │   │
│  │   ↓                                                  │   │
│  │ bronze/fundamentals/balance_sheets/                  │   │
│  │                     /cash_flow/                      │   │
│  │                     /income_statements/              │   │
│  │   (nested JSON with metadata)                        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    NEW SYSTEM (v2)                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ /stocks/financials/v1/balance-sheets                 │   │
│  │ /stocks/financials/v1/cash-flow-statements           │   │
│  │ /stocks/financials/v1/income-statements              │   │
│  │   ↓                                                  │   │
│  │ FundamentalsDownloaderV2                             │   │
│  │   ↓                                                  │   │
│  │ bronze/fundamentals_v2/balance_sheets/               │   │
│  │                        /cash_flow/                   │   │
│  │                        /income_statements/           │   │
│  │   (flat JSON, no metadata)                           │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              VALIDATION & COMPARISON                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Compare v1 vs v2 data                                │   │
│  │   - Field value parity                               │   │
│  │   - Coverage completeness                            │   │
│  │   - Performance metrics                              │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Directory Structure

```
quantmini/
├── src/
│   ├── download/
│   │   ├── fundamentals.py              ← Keep as-is (v1)
│   │   ├── fundamentals_v2.py           ← NEW (v2 implementation)
│   │   └── ratios.py                    ← Already v2 ✓
│   └── storage/
│       ├── schemas.py                   ← Keep existing
│       └── schemas_v2.py                ← NEW (v2 schemas)
├── scripts/
│   ├── download/
│   │   ├── batch_load_fundamentals_all.py     ← v1 (current)
│   │   └── batch_load_fundamentals_all_v2.py  ← NEW (v2)
│   ├── historical_data_load.sh               ← v1 (current)
│   ├── historical_data_load_v2.sh            ← NEW (v2)
│   ├── daily_update_parallel.sh              ← v1 (current)
│   └── daily_update_parallel_v2.sh           ← NEW (v2)
└── tests/
    └── migration/
        ├── test_v1_v2_parity.py              ← NEW (validation)
        └── compare_schemas.py                ← NEW (comparison)
```

---

## Phase 1: Preparation (Week 1, Days 1-2)

### 1.1 Document Current State ✅ DONE

- [x] Schema analysis completed
- [x] Endpoint comparison documented
- [x] Field mapping identified

### 1.2 Create Field Mapping Dictionary

**File**: `src/download/field_mappings.py`

```python
"""
Field mappings between v1 (deprecated) and v2 (official) APIs
"""

# Balance Sheet Mappings
BALANCE_SHEET_MAPPINGS = {
    # v1 nested path → v2 flat field
    'financials.balance_sheet.assets.value': 'total_assets',
    'financials.balance_sheet.current_assets.value': 'total_current_assets',
    'financials.balance_sheet.liabilities.value': 'total_liabilities',
    'financials.balance_sheet.current_liabilities.value': 'total_current_liabilities',
    'financials.balance_sheet.equity.value': 'total_equity',
    'financials.balance_sheet.accounts_payable.value': 'accounts_payable',
    'financials.balance_sheet.inventory.value': 'inventories',
    'financials.balance_sheet.long_term_debt.value': 'long_term_debt_and_capital_lease_obligations',
    'financials.balance_sheet.fixed_assets.value': 'property_plant_equipment_net',
    # ... complete mapping
}

# Cash Flow Mappings
CASH_FLOW_MAPPINGS = {
    'financials.cash_flow_statement.net_cash_flow_from_operating_activities.value': 'net_cash_flow_from_operating_activities',
    'financials.cash_flow_statement.net_cash_flow_from_investing_activities.value': 'net_cash_flow_from_investing_activities',
    'financials.cash_flow_statement.net_cash_flow_from_financing_activities.value': 'net_cash_flow_from_financing_activities',
    # ... complete mapping
}

# Income Statement Mappings
INCOME_STATEMENT_MAPPINGS = {
    'financials.income_statement.revenues.value': 'revenues',
    'financials.income_statement.cost_of_revenue.value': 'cost_of_revenue',
    'financials.income_statement.gross_profit.value': 'gross_profit',
    'financials.income_statement.operating_expenses.value': 'operating_expenses',
    'financials.income_statement.net_income_loss.value': 'net_income_loss',
    # ... complete mapping
}

# Metadata defaults (since v2 doesn't provide them)
DEFAULT_METADATA = {
    'unit': 'USD',
    'order': None,
}

# Field labels (for v2 → v1 compatibility)
FIELD_LABELS = {
    'total_assets': 'Total Assets',
    'total_current_assets': 'Total Current Assets',
    'total_liabilities': 'Total Liabilities',
    # ... complete labels
}
```

**Task**: Create complete field mapping by examining API responses.

**Time**: 4-6 hours

---

### 1.3 Set Up Testing Infrastructure

**File**: `tests/migration/test_v1_v2_parity.py`

```python
"""
Validation tests to ensure v1 and v2 data match
"""
import polars as pl
import pytest
from pathlib import Path

class TestV1V2Parity:
    """Compare v1 and v2 downloads for the same ticker/date"""
    
    def test_balance_sheet_parity(self):
        """Verify balance sheet values match between v1 and v2"""
        # Load v1 data
        v1_file = Path('bronze/fundamentals/balance_sheets/year=2024/month=05/ticker=AAPL.parquet')
        v1_df = pl.read_parquet(v1_file)
        
        # Load v2 data
        v2_file = Path('bronze/fundamentals_v2/balance_sheets/year=2024/month=05/ticker=AAPL.parquet')
        v2_df = pl.read_parquet(v2_file)
        
        # Compare key metrics
        v1_assets = v1_df['financials'][0]['balance_sheet']['assets']['value']
        v2_assets = v2_df['total_assets'][0]
        
        assert abs(v1_assets - v2_assets) < 1.0, f"Assets mismatch: {v1_assets} vs {v2_assets}"
    
    def test_coverage_completeness(self):
        """Ensure v2 has all tickers from v1"""
        # Count tickers in v1
        v1_tickers = set()
        for file in Path('bronze/fundamentals/balance_sheets').rglob('*.parquet'):
            ticker = file.stem.replace('ticker=', '')
            v1_tickers.add(ticker)
        
        # Count tickers in v2
        v2_tickers = set()
        for file in Path('bronze/fundamentals_v2/balance_sheets').rglob('*.parquet'):
            ticker = file.stem.replace('ticker=', '')
            v2_tickers.add(ticker)
        
        missing = v1_tickers - v2_tickers
        assert len(missing) == 0, f"Missing tickers in v2: {missing}"
```

**Time**: 2 hours

---

## Phase 2: Implementation (Week 1, Days 3-5)

### 2.1 Create V2 Downloader

**File**: `src/download/fundamentals_v2.py`

```python
"""
V2 Fundamentals Downloader - Uses official Polygon endpoints

Differences from v1:
- Separate endpoints for each statement type
- Flat JSON structure (no nested financials)
- No metadata (unit, label, order)
- Parameter name changes: ticker → tickers
"""

import polars as pl
import asyncio
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import logging

from .polygon_rest_client import PolygonRESTClient
from .field_mappings import FIELD_LABELS, DEFAULT_METADATA

logger = logging.getLogger(__name__)


class FundamentalsDownloaderV2:
    """
    Download fundamentals from official v2 endpoints
    
    Endpoints:
    - /stocks/financials/v1/balance-sheets
    - /stocks/financials/v1/cash-flow-statements
    - /stocks/financials/v1/income-statements
    """
    
    def __init__(
        self,
        client: PolygonRESTClient,
        output_dir: Path,
        use_partitioned_structure: bool = True
    ):
        self.client = client
        self.output_dir = Path(output_dir)
        self.use_partitioned_structure = use_partitioned_structure
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"FundamentalsDownloaderV2 initialized (output: {output_dir})")
    
    def _save_partitioned(
        self,
        df: pl.DataFrame,
        statement_type: str,
        ticker: str
    ) -> None:
        """
        Save DataFrame in partitioned structure (same as v1)
        
        Structure: output_dir/{statement_type}/year=YYYY/month=MM/ticker=SYMBOL.parquet
        """
        if len(df) == 0:
            return
        
        # Ensure filing_date exists
        if 'filing_date' not in df.columns:
            logger.warning(f"No filing_date in {statement_type} for {ticker}")
            return
        
        # Parse filing_date
        if df.schema['filing_date'] == pl.String:
            df = df.with_columns([
                pl.col('filing_date').str.to_date("%Y-%m-%d").alias('_date_parsed')
            ])
        else:
            df = df.with_columns([
                pl.col('filing_date').cast(pl.Date).alias('_date_parsed')
            ])
        
        df = df.with_columns([
            pl.col('_date_parsed').dt.year().cast(pl.Int32).alias('year'),
            pl.col('_date_parsed').dt.month().cast(pl.Int32).alias('month'),
        ]).drop('_date_parsed')
        
        # Get unique year/month combinations
        partitions = df.select(['year', 'month']).unique()
        
        for row in partitions.iter_rows(named=True):
            year = row['year']
            month = row['month']
            
            # Filter for this partition
            partition_df = df.filter(
                (pl.col('year') == year) &
                (pl.col('month') == month)
            ).drop(['year', 'month'])
            
            # Create partition directory
            partition_dir = self.output_dir / statement_type / f'year={year}' / f'month={month:02d}'
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = partition_dir / f'ticker={ticker.upper()}.parquet'
            
            # Append if exists
            if output_file.exists():
                existing_df = pl.read_parquet(output_file)
                partition_df = pl.concat([existing_df, partition_df], how="diagonal_relaxed")
            
            partition_df.write_parquet(str(output_file), compression='zstd')
            logger.info(f"Saved {len(partition_df)} records to {output_file}")
    
    async def download_balance_sheets(
        self,
        ticker: Optional[str] = None,
        tickers: Optional[str] = None,  # Note: plural for v2
        cik: Optional[str] = None,
        filing_date: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        period_end: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """
        Download balance sheets from v2 endpoint
        
        V2 Changes:
        - Endpoint: /stocks/financials/v1/balance-sheets (not /vX/reference/financials)
        - Parameter: tickers (plural, not ticker)
        - Response: Flat structure (no nested financials)
        - No metadata: unit, label, order removed
        """
        logger.info(f"Downloading balance sheets v2 (ticker={ticker or tickers})")
        
        # Build params (v2 format)
        params = {'limit': limit}
        
        # Handle both ticker (singular) and tickers (plural) for compatibility
        if ticker:
            params['tickers'] = ticker.upper()
        elif tickers:
            params['tickers'] = tickers.upper()
        
        if cik:
            params['cik'] = cik
        if filing_date:
            params['filing_date'] = filing_date
        if filing_date_gte:
            params['filing_date.gte'] = filing_date_gte
        if filing_date_lt:
            params['filing_date.lt'] = filing_date_lt
        if period_end:
            params['period_end'] = period_end
        if fiscal_year:
            params['fiscal_year'] = fiscal_year
        if fiscal_quarter:
            params['fiscal_quarter'] = fiscal_quarter
        if timeframe:
            params['timeframe'] = timeframe
        
        # V2 endpoint
        results = await self.client.paginate_all(
            '/stocks/financials/v1/balance-sheets',  # ← V2 endpoint
            params
        )
        
        if not results:
            logger.warning(f"No balance sheets found for {ticker or tickers}")
            return pl.DataFrame()
        
        # Convert to DataFrame (flat structure, no nesting)
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))
        
        logger.info(f"Downloaded {len(df)} balance sheet records (v2)")
        
        # Save to parquet
        if self.use_partitioned_structure and (ticker or tickers):
            self._save_partitioned(df, 'balance_sheets', ticker or tickers)
        
        return df
    
    async def download_cash_flow_statements(
        self,
        ticker: Optional[str] = None,
        tickers: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """Download cash flow statements from v2 endpoint"""
        logger.info(f"Downloading cash flow v2 (ticker={ticker or tickers})")
        
        params = {'limit': limit}
        
        if ticker:
            params['tickers'] = ticker.upper()
        elif tickers:
            params['tickers'] = tickers.upper()
        
        if filing_date_gte:
            params['filing_date.gte'] = filing_date_gte
        if filing_date_lt:
            params['filing_date.lt'] = filing_date_lt
        if timeframe:
            params['timeframe'] = timeframe
        
        # V2 endpoint
        results = await self.client.paginate_all(
            '/stocks/financials/v1/cash-flow-statements',  # ← V2 endpoint
            params
        )
        
        if not results:
            logger.warning(f"No cash flow found for {ticker or tickers}")
            return pl.DataFrame()
        
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))
        
        logger.info(f"Downloaded {len(df)} cash flow records (v2)")
        
        if self.use_partitioned_structure and (ticker or tickers):
            self._save_partitioned(df, 'cash_flow', ticker or tickers)
        
        return df
    
    async def download_income_statements(
        self,
        ticker: Optional[str] = None,
        tickers: Optional[str] = None,
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None,
        timeframe: Optional[str] = None,
        limit: int = 100
    ) -> pl.DataFrame:
        """Download income statements from v2 endpoint"""
        logger.info(f"Downloading income statements v2 (ticker={ticker or tickers})")
        
        params = {'limit': limit}
        
        if ticker:
            params['tickers'] = ticker.upper()
        elif tickers:
            params['tickers'] = tickers.upper()
        
        if filing_date_gte:
            params['filing_date.gte'] = filing_date_gte
        if filing_date_lt:
            params['filing_date.lt'] = filing_date_lt
        if timeframe:
            params['timeframe'] = timeframe
        
        # V2 endpoint
        results = await self.client.paginate_all(
            '/stocks/financials/v1/income-statements',  # ← V2 endpoint
            params
        )
        
        if not results:
            logger.warning(f"No income statements found for {ticker or tickers}")
            return pl.DataFrame()
        
        df = pl.DataFrame(results)
        df = df.with_columns(pl.lit(datetime.now()).alias('downloaded_at'))
        
        logger.info(f"Downloaded {len(df)} income statement records (v2)")
        
        if self.use_partitioned_structure and (ticker or tickers):
            self._save_partitioned(df, 'income_statements', ticker or tickers)
        
        return df
    
    async def download_all_financials(
        self,
        ticker: str,
        timeframe: str = 'quarterly',
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        Download all financial statements in parallel
        
        V2 Change: Three separate API calls (not one unified call)
        """
        logger.info(f"Downloading all financials v2 for {ticker}")
        
        # Download all in parallel
        results = await asyncio.gather(
            self.download_balance_sheets(
                ticker=ticker,
                timeframe=timeframe,
                filing_date_gte=filing_date_gte,
                filing_date_lt=filing_date_lt
            ),
            self.download_cash_flow_statements(
                ticker=ticker,
                timeframe=timeframe,
                filing_date_gte=filing_date_gte,
                filing_date_lt=filing_date_lt
            ),
            self.download_income_statements(
                ticker=ticker,
                timeframe=timeframe,
                filing_date_gte=filing_date_gte,
                filing_date_lt=filing_date_lt
            ),
            return_exceptions=True
        )
        
        # Process results
        data = {}
        statement_types = ['balance_sheets', 'cash_flow', 'income_statements']
        
        for stmt_type, result in zip(statement_types, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {stmt_type}: {result}")
                data[stmt_type] = pl.DataFrame()
            else:
                data[stmt_type] = result
        
        logger.info(
            f"Downloaded all financials v2 for {ticker}: "
            f"{len(data['balance_sheets'])} balance sheets, "
            f"{len(data['cash_flow'])} cash flow, "
            f"{len(data['income_statements'])} income statements"
        )
        
        return data
    
    async def download_financials_batch(
        self,
        tickers: List[str],
        timeframe: str = 'quarterly',
        filing_date_gte: Optional[str] = None,
        filing_date_lt: Optional[str] = None
    ) -> Dict[str, int]:
        """Batch download for multiple tickers"""
        logger.info(f"Downloading financials v2 for {len(tickers)} tickers")
        
        tasks = [
            self.download_all_financials(
                ticker=ticker,
                timeframe=timeframe,
                filing_date_gte=filing_date_gte,
                filing_date_lt=filing_date_lt
            )
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count totals
        total_counts = {
            'balance_sheets': 0,
            'cash_flow': 0,
            'income_statements': 0
        }
        
        for result in results:
            if isinstance(result, Exception):
                continue
            
            for stmt_type in total_counts.keys():
                df = result.get(stmt_type, pl.DataFrame())
                total_counts[stmt_type] += len(df)
        
        logger.info(
            f"Downloaded financials v2 for {len(tickers)} tickers: "
            f"{total_counts['balance_sheets']} balance sheets, "
            f"{total_counts['cash_flow']} cash flow, "
            f"{total_counts['income_statements']} income statements"
        )
        
        return total_counts


# Example usage
async def main():
    """Example v2 downloader usage"""
    from ..core.config_loader import ConfigLoader
    
    config = ConfigLoader()
    credentials = config.get_credentials('polygon')
    
    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=100,
        max_connections=200
    ) as client:
        
        downloader = FundamentalsDownloaderV2(
            client=client,
            output_dir=Path('data/bronze/fundamentals_v2')
        )
        
        # Download for AAPL
        data = await downloader.download_all_financials(
            'AAPL',
            timeframe='quarterly',
            filing_date_gte='2024-01-01'
        )
        
        print(f"Balance sheets: {len(data['balance_sheets'])}")
        print(f"Cash flow: {len(data['cash_flow'])}")
        print(f"Income statements: {len(data['income_statements'])}")


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
```

**Time**: 8-12 hours

---

### 2.2 Create V2 CLI Command

**File**: `src/cli/commands/polygon.py` (add new command)

```python
@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--timeframe', type=click.Choice(['annual', 'quarterly']), default='quarterly')
@click.option('--filing-date-gte', type=str, default=None)
@click.option('--filing-date-lt', type=str, default=None)
@click.option('--output-dir', type=Path, default=None)
@click.option('--use-v2', is_flag=True, help='Use v2 endpoints (official)')
def fundamentals(tickers, timeframe, filing_date_gte, filing_date_lt, output_dir, use_v2):
    """Download fundamentals (with optional v2 mode)"""
    from datetime import datetime, timedelta
    
    if not output_dir:
        output_dir = get_quantlake_root() / 'bronze' / ('fundamentals_v2' if use_v2 else 'fundamentals')
    
    # Default to last 180 days
    if not filing_date_gte and not filing_date_lt:
        today = datetime.now().date()
        default_start = today - timedelta(days=180)
        filing_date_gte = str(default_start)
        click.echo(f"ℹ️  No date range specified, defaulting to last 180 days")
    
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        
        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            
            # Choose downloader version
            if use_v2:
                from ...download.fundamentals_v2 import FundamentalsDownloaderV2
                downloader = FundamentalsDownloaderV2(client, output_dir, True)
                click.echo("📥 Using V2 endpoints (official)")
            else:
                downloader = FundamentalsDownloader(client, output_dir, True)
                click.echo("📥 Using V1 endpoints (deprecated)")
            
            date_info = f" from {filing_date_gte or 'beginning'} to {filing_date_lt or 'today'}"
            click.echo(f"📥 Downloading {timeframe} fundamentals for {len(tickers)} tickers{date_info}...")
            
            data = await downloader.download_financials_batch(
                list(tickers),
                timeframe,
                filing_date_gte=filing_date_gte,
                filing_date_lt=filing_date_lt
            )
            
            click.echo(f"✅ Downloaded fundamentals:")
            click.echo(f"   Balance sheets: {data['balance_sheets']} records")
            click.echo(f"   Cash flow: {data['cash_flow']} records")
            click.echo(f"   Income statements: {data['income_statements']} records")
            
            return 0
    
    return asyncio.run(run())
```

**Usage**:
```bash
# v1 (current)
quantmini polygon fundamentals AAPL MSFT --filing-date-gte 2024-01-01

# v2 (new)
quantmini polygon fundamentals AAPL MSFT --filing-date-gte 2024-01-01 --use-v2
```

**Time**: 2 hours

---

### 2.3 Create V2 Scripts

**File**: `scripts/historical_data_load_v2.sh`

```bash
#!/bin/bash
# V2 version using official endpoints

# ... (copy from historical_data_load.sh and modify)

# Change fundamentals download to use --use-v2 flag
quantmini polygon fundamentals "$TICKER" \
    --timeframe quarterly \
    --filing-date-gte "2010-01-01" \
    --filing-date-lt "2026-01-01" \
    --output-dir "$BRONZE_DIR/fundamentals_v2" \
    --use-v2 \
    2>&1 | tee -a "$LOG_FILE" &
```

**File**: `scripts/daily_update_parallel_v2.sh`

```bash
#!/bin/bash
# V2 version using official endpoints

# ... (copy from daily_update_parallel.sh and modify)

run_parallel "bronze_fundamentals_v2" \
    "quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
        --timeframe quarterly \
        --filing-date-gte $FILING_DATE_GTE \
        --output-dir $BRONZE_DIR/fundamentals_v2 \
        --use-v2"
```

**Time**: 2 hours

---

## Phase 3: Testing & Validation (Week 2, Days 1-3)

### 3.1 Unit Tests

```python
# tests/unit/test_fundamentals_v2.py

import pytest
from src.download.fundamentals_v2 import FundamentalsDownloaderV2

@pytest.mark.asyncio
async def test_balance_sheet_download_v2(mock_client):
    """Test v2 balance sheet download"""
    downloader = FundamentalsDownloaderV2(mock_client, Path('/tmp'))
    
    df = await downloader.download_balance_sheets('AAPL')
    
    # Verify flat structure
    assert 'total_assets' in df.columns
    assert 'financials' not in df.columns  # No nesting in v2
```

**Time**: 4 hours

---

### 3.2 Integration Tests

```python
# tests/integration/test_v1_v2_integration.py

@pytest.mark.integration
def test_download_same_ticker_both_versions():
    """Download same ticker with v1 and v2, compare results"""
    
    # Download with v1
    v1_data = download_with_v1('AAPL', '2024-01-01', '2024-12-31')
    
    # Download with v2
    v2_data = download_with_v2('AAPL', '2024-01-01', '2024-12-31')
    
    # Compare record counts
    assert len(v1_data) == len(v2_data), "Record count mismatch"
    
    # Compare key values
    v1_assets = extract_assets_v1(v1_data)
    v2_assets = extract_assets_v2(v2_data)
    
    assert abs(v1_assets - v2_assets) < 1.0, "Asset values differ"
```

**Time**: 6 hours

---

### 3.3 Data Validation Script

**File**: `scripts/validation/compare_v1_v2_data.py`

```python
"""
Compare v1 and v2 downloaded data for validation
"""
import polars as pl
from pathlib import Path
import sys

def compare_balance_sheets(ticker: str, year: int, month: int):
    """Compare balance sheet data between v1 and v2"""
    
    # Load v1
    v1_path = f'bronze/fundamentals/balance_sheets/year={year}/month={month:02d}/ticker={ticker}.parquet'
    v1_df = pl.read_parquet(v1_path)
    
    # Load v2
    v2_path = f'bronze/fundamentals_v2/balance_sheets/year={year}/month={month:02d}/ticker={ticker}.parquet'
    v2_df = pl.read_parquet(v2_path)
    
    print(f"\n{'='*80}")
    print(f"Comparing {ticker} - {year}/{month:02d}")
    print(f"{'='*80}")
    
    print(f"\nV1 Records: {len(v1_df)}")
    print(f"V2 Records: {len(v2_df)}")
    
    if len(v1_df) != len(v2_df):
        print("⚠️  WARNING: Record count mismatch!")
    
    # Compare values
    if len(v1_df) > 0 and len(v2_df) > 0:
        v1_row = v1_df.to_dicts()[0]
        v2_row = v2_df.to_dicts()[0]
        
        # Extract assets value
        v1_assets = v1_row['financials']['balance_sheet']['assets']['value']
        v2_assets = v2_row['total_assets']
        
        print(f"\nAssets Comparison:")
        print(f"  V1: ${v1_assets:,.0f}")
        print(f"  V2: ${v2_assets:,.0f}")
        print(f"  Difference: ${abs(v1_assets - v2_assets):,.2f}")
        
        if abs(v1_assets - v2_assets) < 1.0:
            print("  ✅ MATCH")
        else:
            print("  ❌ MISMATCH")
            return False
    
    return True


def main():
    """Run comparison for sample tickers"""
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    year = 2024
    month = 5
    
    all_match = True
    for ticker in tickers:
        try:
            if not compare_balance_sheets(ticker, year, month):
                all_match = False
        except Exception as e:
            print(f"❌ Error comparing {ticker}: {e}")
            all_match = False
    
    if all_match:
        print("\n✅ All comparisons passed!")
        sys.exit(0)
    else:
        print("\n❌ Some comparisons failed!")
        sys.exit(1)


if __name__ == '__main__':
    main()
```

**Usage**:
```bash
python scripts/validation/compare_v1_v2_data.py
```

**Time**: 4 hours

---

## Phase 4: Parallel Operation (Week 2, Days 4-5)

### 4.1 Run Both Pipelines Simultaneously

```bash
# Run v1 (current)
./scripts/daily_update_parallel.sh --days-back 7 &
V1_PID=$!

# Run v2 (new)
./scripts/daily_update_parallel_v2.sh --days-back 7 &
V2_PID=$!

# Wait for both
wait $V1_PID
wait $V2_PID

# Compare results
python scripts/validation/compare_v1_v2_data.py
```

### 4.2 Monitor Both Pipelines

**File**: `scripts/monitoring/monitor_dual_pipelines.py`

```python
"""Monitor v1 and v2 pipelines side by side"""

import time
from pathlib import Path
import polars as pl

def count_records(base_path: str):
    """Count total records across all parquet files"""
    total = 0
    for file in Path(base_path).rglob('*.parquet'):
        df = pl.read_parquet(file)
        total += len(df)
    return total

def monitor_loop():
    """Continuously monitor both pipelines"""
    while True:
        v1_bs = count_records('bronze/fundamentals/balance_sheets')
        v2_bs = count_records('bronze/fundamentals_v2/balance_sheets')
        
        print(f"\nBalance Sheets:")
        print(f"  V1: {v1_bs:,} records")
        print(f"  V2: {v2_bs:,} records")
        print(f"  Diff: {abs(v1_bs - v2_bs):,}")
        
        time.sleep(60)  # Update every minute

if __name__ == '__main__':
    monitor_loop()
```

**Time**: Ongoing monitoring

---

## Phase 5: Cutover (Week 3)

### 5.1 Pre-Cutover Checklist

- [ ] V2 pipeline running successfully for 5+ days
- [ ] Data validation passes for 100% of test cases
- [ ] Performance metrics acceptable (v2 ≤ v1 runtime)
- [ ] Storage usage within limits
- [ ] Silver layer compatible with v2 schema
- [ ] Team trained on v2 differences
- [ ] Rollback plan tested

### 5.2 Cutover Process

**Step 1**: Stop scheduling new v1 jobs
```bash
# Disable v1 cron jobs
crontab -l > crontab_backup.txt
crontab -r  # Remove all cron jobs temporarily
```

**Step 2**: Wait for v1 to finish current jobs
```bash
# Check for running v1 processes
ps aux | grep historical_data_load.sh
ps aux | grep daily_update_parallel.sh
```

**Step 3**: Rename directories (archive v1, promote v2)
```bash
# Backup v1
mv bronze/fundamentals bronze/fundamentals_v1_archive

# Promote v2 to primary
mv bronze/fundamentals_v2 bronze/fundamentals
```

**Step 4**: Update scripts to use v2 by default
```bash
# Remove --use-v2 flags (v2 is now default)
# Update historical_data_load.sh
sed -i '' 's/--use-v2//g' scripts/historical_data_load.sh
```

**Step 5**: Re-enable scheduling with v2
```bash
# Add v2 scripts to cron
crontab -e
# (Edit to use *_v2.sh scripts)
```

**Time**: 2-4 hours (scheduled maintenance window)

---

## Phase 6: Cleanup (Week 3+)

### 6.1 Archive V1 Code

After 30 days of successful v2 operation:

```bash
# Move v1 code to archive
mkdir -p archive/v1_deprecated/
mv src/download/fundamentals.py archive/v1_deprecated/
mv scripts/historical_data_load.sh archive/v1_deprecated/
mv scripts/daily_update_parallel.sh archive/v1_deprecated/
```

### 6.2 Archive V1 Data

After 90 days:

```bash
# Compress and archive v1 data
tar -czf fundamentals_v1_archive_$(date +%Y%m%d).tar.gz bronze/fundamentals_v1_archive/
mv fundamentals_v1_archive_*.tar.gz /archive/
rm -rf bronze/fundamentals_v1_archive/
```

---

## Rollback Strategy

### Immediate Rollback (< 24 hours after cutover)

```bash
# 1. Stop v2 pipeline
killall -9 daily_update_parallel_v2.sh

# 2. Revert directory rename
mv bronze/fundamentals bronze/fundamentals_v2
mv bronze/fundamentals_v1_archive bronze/fundamentals

# 3. Restore v1 cron jobs
crontab crontab_backup.txt

# 4. Resume v1 pipeline
./scripts/daily_update_parallel.sh
```

**Time**: 15 minutes

---

### Delayed Rollback (> 24 hours after cutover)

If issues discovered after 24+ hours:

1. Keep v2 running (don't want data gaps)
2. Start v1 alongside v2
3. Backfill v1 for missed dates
4. Switch primary back to v1 once caught up
5. Debug v2 issues offline

**Time**: 4-8 hours

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| V2 API schema changes | Low | High | Comprehensive validation tests |
| Data loss during cutover | Low | Critical | Parallel operation, backups |
| Performance degradation | Medium | Medium | Load testing, monitoring |
| Silver layer incompatibility | Medium | High | Abstraction layer, gradual migration |
| Unexpected API behavior | Low | High | Extensive testing phase |

---

## Success Metrics

### Phase 2 (Implementation)
- [ ] V2 downloader successfully downloads all statement types
- [ ] CLI command works with --use-v2 flag
- [ ] Code coverage >80%

### Phase 3 (Testing)
- [ ] 100% of validation tests pass
- [ ] Data parity confirmed for 100 sample tickers
- [ ] No schema mismatches detected

### Phase 4 (Parallel Operation)
- [ ] Both pipelines run successfully for 5 consecutive days
- [ ] V2 runtime ≤ V1 runtime
- [ ] Storage usage <110% of v1

### Phase 5 (Cutover)
- [ ] Zero downtime during cutover
- [ ] No data gaps after cutover
- [ ] Silver layer processes v2 data successfully

### Phase 6 (Cleanup)
- [ ] V1 code archived
- [ ] V1 data compressed and archived
- [ ] Documentation updated

---

## Timeline Summary

| Week | Phase | Key Deliverables | Hours |
|------|-------|------------------|-------|
| Week 1, Days 1-2 | Preparation | Field mappings, test infrastructure | 8-10h |
| Week 1, Days 3-5 | Implementation | V2 downloader, CLI, scripts | 12-16h |
| Week 2, Days 1-3 | Testing | Unit tests, integration tests, validation | 14-16h |
| Week 2, Days 4-5 | Parallel Operation | Run both, monitor, compare | 8-10h |
| Week 3 | Cutover & Cleanup | Switch primary, archive v1 | 6-8h |
| **Total** | | | **48-60 hours** |

---

## Additional Resources

### Documentation to Create

1. **V2 Migration Guide** (for team)
   - What changed
   - How to use v2 endpoints
   - Troubleshooting

2. **Schema Reference**
   - V1 vs V2 field mappings
   - Type conversions
   - Missing metadata handling

3. **Runbook**
   - How to run v2 pipeline
   - How to rollback
   - Emergency procedures

### Training Materials

1. Demo: Side-by-side comparison of v1 vs v2 downloads
2. Walkthrough: Using validation scripts
3. Practice: Performing rollback in staging environment

---

## Contact & Support

**Migration Lead**: [Your Name]

**Rollback Authority**: [Decision Maker]

**Escalation Path**:
1. Check monitoring dashboard
2. Review validation logs
3. Contact migration lead
4. Execute rollback if critical

---

## Appendix A: Complete Field Mapping

```python
# Complete field mapping dictionary (to be populated during Phase 1)

COMPLETE_FIELD_MAPPINGS = {
    'balance_sheet': {
        'assets': 'total_assets',
        'current_assets': 'total_current_assets',
        'cash_and_cash_equivalents': 'cash_and_equivalents',
        'short_term_investments': 'short_term_investments',
        'accounts_receivable': 'receivables',
        'inventory': 'inventories',
        'other_current_assets': 'other_current_assets',
        'fixed_assets': 'property_plant_equipment_net',
        'goodwill': 'goodwill',
        'intangible_assets': 'intangible_assets_net',
        'other_noncurrent_assets': 'other_assets',
        'liabilities': 'total_liabilities',
        'current_liabilities': 'total_current_liabilities',
        'accounts_payable': 'accounts_payable',
        'current_debt': 'debt_current',
        'deferred_revenue': 'deferred_revenue_current',
        'other_current_liabilities': 'other_current_liabilities',
        'long_term_debt': 'long_term_debt_and_capital_lease_obligations',
        'other_noncurrent_liabilities': 'other_noncurrent_liabilities',
        'equity': 'total_equity',
        'common_stock': 'common_stock',
        'retained_earnings': 'retained_earnings_deficit',
        'treasury_stock': 'treasury_stock',
        'liabilities_and_equity': 'total_liabilities_and_equity',
    },
    'cash_flow': {
        'net_cash_flow_from_operating_activities': 'net_cash_flow_from_operating_activities',
        'net_cash_flow_from_investing_activities': 'net_cash_flow_from_investing_activities',
        'net_cash_flow_from_financing_activities': 'net_cash_flow_from_financing_activities',
        # ... (populate from API docs)
    },
    'income_statement': {
        'revenues': 'revenues',
        'cost_of_revenue': 'cost_of_revenue',
        'gross_profit': 'gross_profit',
        'operating_expenses': 'operating_expenses',
        'operating_income': 'operating_income_loss',
        'net_income': 'net_income_loss',
        # ... (populate from API docs)
    }
}
```

---

## Appendix B: Validation Queries

```python
# Sample validation queries

# Count records
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT ticker) as unique_tickers,
    MIN(filing_date) as earliest_date,
    MAX(filing_date) as latest_date
FROM balance_sheets_v1
UNION ALL
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT tickers) as unique_tickers,
    MIN(filing_date) as earliest_date,
    MAX(filing_date) as latest_date
FROM balance_sheets_v2;

# Compare specific values
SELECT 
    v1.ticker,
    v1.filing_date,
    v1.financials.balance_sheet.assets.value as v1_assets,
    v2.total_assets as v2_assets,
    ABS(v1_assets - v2_assets) as difference
FROM balance_sheets_v1 v1
JOIN balance_sheets_v2 v2 
    ON v1.ticker = v2.tickers[1] 
    AND v1.filing_date = v2.filing_date
WHERE ABS(v1_assets - v2_assets) > 1.0;
```

---

## Appendix C: Performance Benchmarks

Target benchmarks for v2:

| Metric | V1 Baseline | V2 Target | V2 Acceptable |
|--------|-------------|-----------|---------------|
| Download time (1000 tickers) | 15 min | 12 min | 18 min |
| API requests per minute | 100 | 100 | 80 |
| Storage size (1 year data) | 2 GB | 1.5 GB | 2.5 GB |
| Memory usage (peak) | 4 GB | 3 GB | 5 GB |
| Data freshness (lag) | 30 min | 30 min | 45 min |

---

**END OF MIGRATION PLAN**

---

**Document Version**: 1.0
**Last Updated**: 2025-10-22
**Next Review**: After Phase 3 completion
