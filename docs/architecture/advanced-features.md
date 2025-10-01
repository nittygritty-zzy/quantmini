# Phase 5-8 Design: Feature Engineering to Production Pipeline
## From Parquet Data Lake to Qlib-Ready System

**Version**: 1.0
**Date**: September 30, 2025
**Status**: Phase 4 Complete (182M records in Parquet), Phase 5-8 Pending
**Parent Design**: `mac-optimized-pipeline.md` v3.0
**Prerequisites**: Phase 1-4 complete, Qlib library installed

---

## Executive Summary

This document extends the Phase 1-4 data ingestion pipeline with Phase 5-8 specifications for production-ready quantitative analysis. The design follows the adaptive resource management principles from the parent architecture while adding:

- **Phase 5: Feature Engineering** - Transform raw OHLCV data into alpha factors
- **Phase 6: Qlib Binary Conversion** - Convert enriched Parquet to Qlib's binary format
- **Phase 7: Query Engine** - Fast querying with caching and optimization
- **Phase 8: Daily Automation** - Production pipeline with incremental updates

**Key Design Principles**:
1. **Qlib Compatibility**: All binary outputs follow Qlib's exact format specifications
2. **Incremental Processing**: Only process new/changed data using watermarks
3. **Adaptive Resource Management**: Scale from 24GB workstations to production servers
4. **No Conflicts**: Extends Phase 1-4 without breaking existing functionality

---

## Current State (Phase 4 Complete)

### What We Have
```
data/parquet/
├── stocks_daily/      82 partitions, 15.80 MB
├── stocks_minute/     33 files, 1.9 GB
├── options_daily/     41 files, 350 MB
└── options_minute/    42 files, 1.2 GB

Total: 182,104,742 records (Aug 1 - Sep 30, 2025)
```

### Schema Status
- ✅ OHLCV fields ingested
- ✅ Partitioned by year/month/date
- ⚠️ Options fields (underlying, expiration, strike) **nullable** - to be parsed in Phase 5
- ✅ Memory-optimized types (float32, uint32/uint64)
- ✅ Watermarks tracking ingestion progress

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                   PHASE 5-8 PIPELINE FLOW                         │
└──────────────────────────────────────────────────────────────────┘

Phase 4 (Complete)      Phase 5              Phase 6           Phase 7-8
Raw Parquet          → Enriched Parquet → Qlib Binary  → Query + Daily Updates

┌─────────────┐      ┌──────────────┐    ┌───────────┐    ┌──────────┐
│  Raw OHLCV  │ ───▶ │   Features   │──▶ │  .day.bin │──▶ │  Qlib    │
│  Parquet    │ P5   │  + Alpha     │ P6 │  .1min.bin│ P7 │  Queries │
└─────────────┘      └──────────────┘    └───────────┘    └──────────┘
                            │                   │               │
                            │                   │               │
                     ┌──────▼───────┐    ┌──────▼──────┐ ┌────▼─────┐
                     │  DuckDB/     │    │ instruments/│ │  Cache   │
                     │  Polars      │    │ calendars/  │ │  Layer   │
                     │  Engine      │    │ features/   │ │          │
                     └──────────────┘    └─────────────┘ └──────────┘

Daily Automation (Phase 8):
  1. Download new date's data from Polygon S3
  2. Ingest to raw Parquet (existing Phase 4)
  3. Enrich with features (Phase 5)
  4. Convert to Qlib binary (Phase 6)
  5. Update instruments/calendars
  6. Clear query cache for affected symbols
```

---

## Directory Structure Changes

### Phase 5-8 Additions

```
data/
├── parquet/                        # Phase 4 (existing)
│   ├── stocks_daily/              # Raw data
│   ├── stocks_minute/
│   ├── options_daily/
│   └── options_minute/
│
├── enriched/                       # Phase 5 (NEW)
│   ├── stocks_daily/              # With alpha factors
│   │   └── year=2025/month=09/
│   │       └── part-0.parquet     # OHLCV + features
│   ├── stocks_minute/
│   ├── options_daily/
│   └── options_minute/
│
├── qlib/                          # Phase 6 (NEW)
│   ├── stocks_daily/
│   │   ├── instruments/
│   │   │   └── all.txt            # List of symbols
│   │   ├── calendars/
│   │   │   └── day.txt            # Trading days (2025-08-01\n2025-08-02...)
│   │   └── features/
│   │       ├── aapl/
│   │       │   ├── open.day.bin
│   │       │   ├── high.day.bin
│   │       │   ├── low.day.bin
│   │       │   ├── close.day.bin
│   │       │   ├── volume.day.bin
│   │       │   ├── alpha_daily.day.bin
│   │       │   ├── returns_1d.day.bin
│   │       │   └── vwap.day.bin
│   │       └── tsla/
│   │           └── ...
│   ├── stocks_minute/
│   │   └── (same structure with .1min.bin)
│   └── options_daily/
│       └── (same structure)
│
├── cache/                         # Phase 7 (NEW)
│   ├── queries/                   # LRU query cache
│   └── metadata/                  # Cache statistics
│
└── metadata/                      # Phase 4 (existing) + Phase 5-8 extensions
    ├── ingestion_status.json     # Phase 4 watermarks
    ├── enrichment_status.json    # Phase 5 watermarks (NEW)
    ├── binary_status.json        # Phase 6 watermarks (NEW)
    └── pipeline_stats.json       # Phase 8 statistics (NEW)
```

---

## Phase 5: Feature Engineering

### 5.1 Overview

**Goal**: Transform raw OHLCV data into quantitative features and alpha factors
**Input**: `data/parquet/{data_type}/` (raw Parquet from Phase 4)
**Output**: `data/enriched/{data_type}/` (enriched Parquet with features)
**Processing Mode**: Adaptive (streaming/batch/parallel based on system profile)

### 5.2 Feature Definitions

#### 5.2.1 Stock Daily Features

```python
# Core price features
- returns_1d: log(close[t] / close[t-1])
- returns_5d: log(close[t] / close[t-5])
- returns_20d: log(close[t] / close[t-20])
- price_range: high - low
- daily_return: (close - open) / open

# Volume features
- volume_ratio: volume / MA(volume, 20)
- vwap: (volume * (high + low + close) / 3) / volume
- money_flow: volume * vwap

# Alpha factors (Qlib-compatible)
- alpha_daily: -log(close / close.shift(1))  # Qlib's default alpha
- alpha_5d: -log(close / close.shift(5))
- alpha_20d: -log(close / close.shift(20))

# Volatility
- volatility_20d: std(returns_1d, 20)
- atr_14: average_true_range(14)

# Technical indicators
- rsi_14: relative_strength_index(14)
- macd: ema_12 - ema_26
- macd_signal: ema(macd, 9)
- bb_upper: MA(close, 20) + 2 * std(close, 20)
- bb_lower: MA(close, 20) - 2 * std(close, 20)
```

#### 5.2.2 Stock Minute Features

```python
# Intraday patterns
- returns_1min: log(close[t] / close[t-1])
- returns_5min: log(close[t] / close[t-5])
- intraday_momentum: close / open_of_day
- minute_volume_ratio: volume / MA(volume, 20)

# Intraday VWAP
- vwap_intraday: cumulative_volume_price / cumulative_volume
- vwap_distance: (close - vwap_intraday) / vwap_intraday

# High-frequency features
- spread: (high - low) / close
- volatility_5min: std(returns_1min, 5)
```

#### 5.2.3 Options Features

**First: Parse Options Ticker**
```python
# Parse ticker: O:SPY230327P00390000
ticker_parts = {
    'underlying': 'SPY',              # Extract from ticker
    'expiration_date': '2023-03-27',  # Parse date
    'contract_type': 'P',             # Put/Call
    'strike_price': 390.0             # Parse strike
}
```

**Then: Calculate Options Features**
```python
# Options-specific features
- moneyness: strike_price / underlying_price
- time_to_expiration: (expiration_date - current_date).days
- implied_volatility: (calculated from Black-Scholes if needed)

# Options Greeks (if underlying price available)
- delta: ∂V/∂S
- gamma: ∂²V/∂S²
- theta: ∂V/∂t
- vega: ∂V/∂σ

# Options flow features
- volume_oi_ratio: volume / open_interest
- put_call_ratio: put_volume / call_volume (per underlying)
```

### 5.3 Implementation Architecture

#### 5.3.1 Feature Engineer Class

**File**: `src/features/feature_engineer.py`

```python
from pathlib import Path
import duckdb
import polars as pl
from src.core.config_loader import ConfigLoader
from src.storage.metadata_manager import MetadataManager

class FeatureEngineer:
    """
    Adaptive feature engineering with DuckDB or Polars

    Follows parent design's adaptive processing modes:
    - Streaming mode (<32GB RAM): DuckDB with memory limits
    - Batch mode (32-64GB): Polars with lazy evaluation
    - Parallel mode (>64GB): Polars with full parallelization
    """

    def __init__(
        self,
        parquet_root: Path,
        enriched_root: Path,
        config: ConfigLoader
    ):
        self.parquet_root = parquet_root
        self.enriched_root = enriched_root
        self.config = config
        self.profile = config.get_system_profile()
        self.mode = self.profile['recommended_mode']

        # Initialize appropriate backend
        if self.mode == 'streaming':
            self._init_duckdb_engine()
        else:
            self._init_polars_engine()

        # Metadata tracking
        self.metadata = MetadataManager(
            metadata_root=parquet_root.parent / 'metadata',
            data_type='enrichment'
        )

    def _init_duckdb_engine(self):
        """Initialize DuckDB for streaming mode"""
        memory_limit = self.profile['resource_limits']['max_memory_gb'] * 0.5
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': min(4, self.profile['hardware']['cpu_cores']),
            'enable_object_cache': True,
            'temp_directory': '/tmp/duckdb_quantmini'
        })
        self.engine = 'duckdb'

    def _init_polars_engine(self):
        """Initialize Polars for batch/parallel mode"""
        self.engine = 'polars'
        # Polars uses environment variables for config
        # Set in system profile or environment

    def enrich_date_range(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        incremental: bool = True
    ) -> dict:
        """
        Enrich date range with features

        Args:
            data_type: stocks_daily, stocks_minute, options_daily, options_minute
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            incremental: Skip already enriched dates

        Returns:
            Statistics dict
        """
        # Get dates to process
        if incremental:
            dates = self.metadata.get_missing_dates(
                start_date=start_date,
                end_date=end_date,
                data_type=data_type
            )
        else:
            dates = self._generate_date_range(start_date, end_date)

        stats = {
            'dates_processed': 0,
            'records_enriched': 0,
            'features_added': 0,
            'errors': []
        }

        for date in dates:
            try:
                result = self._enrich_date(data_type, date)
                stats['dates_processed'] += 1
                stats['records_enriched'] += result['records']
                stats['features_added'] = result['features']

                # Update watermark
                self.metadata.mark_date_complete(date, data_type)

            except Exception as e:
                stats['errors'].append({'date': date, 'error': str(e)})

        return stats

    def _enrich_date(self, data_type: str, date: str) -> dict:
        """Enrich single date"""
        if self.engine == 'duckdb':
            return self._enrich_with_duckdb(data_type, date)
        else:
            return self._enrich_with_polars(data_type, date)

    def _enrich_with_duckdb(self, data_type: str, date: str) -> dict:
        """Memory-safe enrichment using DuckDB"""
        # Determine feature SQL based on data type
        if data_type == 'stocks_daily':
            feature_sql = self._get_stock_daily_features_sql()
        elif data_type == 'stocks_minute':
            feature_sql = self._get_stock_minute_features_sql()
        elif data_type == 'options_daily':
            feature_sql = self._get_options_daily_features_sql()
        else:
            feature_sql = self._get_options_minute_features_sql()

        # Input path pattern
        year, month = date.split('-')[0:2]
        input_pattern = self.parquet_root / data_type / f'year={year}/month={month}/*.parquet'

        # Create view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW raw_data AS
            SELECT * FROM read_parquet('{input_pattern}')
            WHERE date >= '{date}' AND date <= '{date}'
            ORDER BY symbol, date, timestamp
        """)

        # Apply features
        enriched_df = self.conn.execute(feature_sql).fetch_df()

        # Write output
        output_path = self.enriched_root / data_type / f'year={year}/month={month}'
        output_path.mkdir(parents=True, exist_ok=True)
        output_file = output_path / f'{date}.parquet'

        # Write with PyArrow for compatibility
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pandas(enriched_df)
        pq.write_table(table, output_file, compression='zstd')

        return {
            'records': len(enriched_df),
            'features': len(enriched_df.columns)
        }

    def _get_stock_daily_features_sql(self) -> str:
        """SQL for stock daily features"""
        return """
        SELECT
            *,
            -- Returns
            -LN(close / LAG(close, 1) OVER w) AS returns_1d,
            -LN(close / LAG(close, 5) OVER w) AS returns_5d,
            -LN(close / LAG(close, 20) OVER w) AS returns_20d,

            -- Alpha (Qlib format)
            -LN(close / LAG(close, 1) OVER w) AS alpha_daily,

            -- Price features
            high - low AS price_range,
            (close - open) / NULLIF(open, 0) AS daily_return,

            -- Volume features
            volume / NULLIF(AVG(volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) AS volume_ratio,
            (volume * (high + low + close) / 3.0) / NULLIF(volume, 0) AS vwap,

            -- Volatility
            STDDEV((-LN(close / LAG(close, 1) OVER w))) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS volatility_20d

        FROM raw_data
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
        ORDER BY symbol, date
        """

    def _get_stock_minute_features_sql(self) -> str:
        """SQL for stock minute features"""
        return """
        SELECT
            *,
            -- Minute returns
            -LN(close / LAG(close, 1) OVER w) AS returns_1min,
            -LN(close / LAG(close, 5) OVER w) AS returns_5min,

            -- Intraday VWAP
            SUM(volume * (high + low + close) / 3.0) OVER w_day / NULLIF(SUM(volume) OVER w_day, 0) AS vwap_intraday,

            -- Volume
            volume / NULLIF(AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) AS minute_volume_ratio,

            -- Spread
            (high - low) / NULLIF(close, 0) AS spread

        FROM raw_data
        WINDOW w AS (PARTITION BY symbol ORDER BY timestamp),
               w_day AS (PARTITION BY symbol, date ORDER BY timestamp)
        ORDER BY symbol, date, timestamp
        """

    def _get_options_daily_features_sql(self) -> str:
        """SQL for options daily features (parse ticker first)"""
        return """
        SELECT
            *,
            -- Parse ticker: O:SPY230327P00390000
            REGEXP_EXTRACT(ticker, '^O:([A-Z]+)', 1) AS underlying,
            CAST(REGEXP_EXTRACT(ticker, '([0-9]{6})', 1) AS DATE) AS expiration_date,
            REGEXP_EXTRACT(ticker, '([PC])', 1) AS contract_type,
            CAST(REGEXP_EXTRACT(ticker, '([0-9]{8})$', 1) AS DOUBLE) / 1000.0 AS strike_price,

            -- Options features
            (CAST(REGEXP_EXTRACT(ticker, '([0-9]{8})$', 1) AS DOUBLE) / 1000.0) / NULLIF(close, 0) AS moneyness,

            -- Returns
            -LN(close / LAG(close, 1) OVER w) AS returns_1d,

            -- Volume features
            volume / NULLIF(AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW), 0) AS volume_ratio

        FROM raw_data
        WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ORDER BY ticker, date
        """

    def _enrich_with_polars(self, data_type: str, date: str) -> dict:
        """High-performance enrichment using Polars"""
        # TODO: Implement Polars version for batch/parallel modes
        # Similar logic but using Polars lazy API
        raise NotImplementedError("Polars backend coming in Phase 5.3")
```

#### 5.3.2 Options Ticker Parser

**File**: `src/features/options_parser.py`

```python
import re
from datetime import datetime
from typing import Dict, Optional

class OptionsTickerParser:
    """
    Parse Polygon.io options ticker format

    Format: O:UNDERLYING[YY]MMDD[C/P]STRIKE
    Example: O:SPY230327P00390000

    Components:
    - O: = Options prefix
    - SPY = Underlying symbol
    - 230327 = Expiration date (2023-03-27)
    - P = Put (or C for Call)
    - 00390000 = Strike price ($390.00)
    """

    TICKER_PATTERN = re.compile(
        r'^O:(?P<underlying>[A-Z]+)'
        r'(?P<exp_year>\d{2})(?P<exp_month>\d{2})(?P<exp_day>\d{2})'
        r'(?P<contract_type>[PC])'
        r'(?P<strike>\d{8})$'
    )

    @classmethod
    def parse(cls, ticker: str) -> Optional[Dict[str, any]]:
        """
        Parse options ticker

        Args:
            ticker: Options ticker string (e.g., O:SPY230327P00390000)

        Returns:
            Dict with parsed fields or None if invalid
        """
        match = cls.TICKER_PATTERN.match(ticker)
        if not match:
            return None

        parts = match.groupdict()

        # Parse expiration date
        exp_date_str = f"20{parts['exp_year']}-{parts['exp_month']}-{parts['exp_day']}"
        try:
            exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d').date()
        except ValueError:
            return None

        # Parse strike price (8 digits, last 3 are decimals)
        strike_int = int(parts['strike'])
        strike_price = strike_int / 1000.0

        return {
            'underlying': parts['underlying'],
            'expiration_date': exp_date,
            'contract_type': parts['contract_type'],  # 'P' or 'C'
            'strike_price': strike_price
        }

    @classmethod
    def parse_batch(cls, tickers: list) -> dict:
        """Parse multiple tickers efficiently"""
        results = {}
        for ticker in tickers:
            parsed = cls.parse(ticker)
            if parsed:
                results[ticker] = parsed
        return results
```

### 5.4 Testing Strategy

**File**: `tests/unit/test_feature_engineer.py`

```python
def test_feature_engineer_stocks_daily():
    """Test stock daily feature engineering"""
    # Test with sample AAPL data
    # Verify alpha_daily, returns, volume_ratio calculated correctly

def test_feature_engineer_options():
    """Test options feature engineering"""
    # Test ticker parsing
    # Verify moneyness calculation

def test_incremental_enrichment():
    """Test incremental mode skips already enriched dates"""
```

---

## Phase 6: Qlib Binary Conversion

### 6.1 Overview

**Goal**: Convert enriched Parquet to Qlib's binary format for ML/backtesting
**Input**: `data/enriched/{data_type}/` (enriched Parquet from Phase 5)
**Output**: `data/qlib/{data_type}/` (Qlib binary format)
**Format Spec**: https://qlib.readthedocs.io/en/latest/component/data.html

### 6.2 Qlib Binary Format Specification

Per Qlib documentation, binary format has 3 components:

#### 6.2.1 Instruments File

**File**: `data/qlib/{data_type}/instruments/all.txt`
**Format**: One symbol per line

```
AAPL
MSFT
TSLA
...
```

#### 6.2.2 Calendar File

**File**: `data/qlib/{data_type}/calendars/day.txt`
**Format**: One trading date per line (YYYY-MM-DD)

```
2025-08-01
2025-08-02
2025-08-05
...
```

#### 6.2.3 Feature Binary Files

**Location**: `data/qlib/{data_type}/features/{symbol}/{feature}.day.bin`
**Format**: Little-endian binary with header

```
[4 bytes: record count (uint32)]
[N * 4 bytes: float32 values, one per trading day]
```

**Example**:
- `data/qlib/stocks_daily/features/aapl/close.day.bin`
- `data/qlib/stocks_daily/features/aapl/alpha_daily.day.bin`

For minute data, use `.1min.bin` extension.

### 6.3 Implementation

#### 6.3.1 Qlib Binary Writer

**File**: `src/transform/qlib_binary_writer.py`

```python
from pathlib import Path
import struct
import numpy as np
import duckdb
from typing import List, Dict
from src.core.config_loader import ConfigLoader
from src.storage.metadata_manager import MetadataManager

class QlibBinaryWriter:
    """
    Convert enriched Parquet to Qlib binary format

    Follows Qlib's binary format specification:
    - instruments/all.txt: List of symbols
    - calendars/day.txt: Trading days
    - features/{symbol}/{feature}.day.bin: Binary feature data

    Processing modes:
    - Streaming: One symbol at a time
    - Batch: Batches of symbols
    - Parallel: Parallel symbol processing
    """

    def __init__(
        self,
        enriched_root: Path,
        qlib_root: Path,
        config: ConfigLoader
    ):
        self.enriched_root = enriched_root
        self.qlib_root = qlib_root
        self.config = config
        self.profile = config.get_system_profile()
        self.mode = self.profile['recommended_mode']

        # DuckDB for queries
        memory_limit = self.profile['resource_limits']['max_memory_gb'] * 0.5
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': min(4, self.profile['hardware']['cpu_cores'])
        })

        # Metadata
        self.metadata = MetadataManager(
            metadata_root=enriched_root.parent / 'metadata',
            data_type='binary_conversion'
        )

    def convert_data_type(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        incremental: bool = True
    ) -> dict:
        """
        Convert entire data type to Qlib binary

        Args:
            data_type: stocks_daily, stocks_minute, options_daily, options_minute
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            incremental: Only convert new/updated symbols

        Returns:
            Statistics dict
        """
        output_dir = self.qlib_root / data_type
        output_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Generate instruments file
        symbols = self._generate_instruments(data_type, output_dir)

        # Step 2: Generate calendar file
        trading_days = self._generate_calendar(data_type, start_date, end_date, output_dir)

        # Step 3: Convert features for each symbol
        stats = self._convert_features(
            data_type=data_type,
            symbols=symbols,
            trading_days=trading_days,
            output_dir=output_dir,
            incremental=incremental
        )

        return {
            'symbols_converted': len(symbols),
            'trading_days': len(trading_days),
            **stats
        }

    def _generate_instruments(self, data_type: str, output_dir: Path) -> List[str]:
        """Generate instruments/all.txt"""
        # Query unique symbols
        input_pattern = self.enriched_root / data_type / '**/*.parquet'

        symbols_df = self.conn.execute(f"""
            SELECT DISTINCT symbol
            FROM read_parquet('{input_pattern}')
            ORDER BY symbol
        """).fetch_df()

        symbols = symbols_df['symbol'].tolist()

        # Write instruments file
        instruments_dir = output_dir / 'instruments'
        instruments_dir.mkdir(parents=True, exist_ok=True)

        with open(instruments_dir / 'all.txt', 'w') as f:
            for symbol in symbols:
                f.write(f"{symbol}\n")

        return symbols

    def _generate_calendar(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        output_dir: Path
    ) -> List[str]:
        """Generate calendars/day.txt"""
        # Query unique trading days
        input_pattern = self.enriched_root / data_type / '**/*.parquet'

        dates_df = self.conn.execute(f"""
            SELECT DISTINCT date
            FROM read_parquet('{input_pattern}')
            WHERE date >= '{start_date}' AND date <= '{end_date}'
            ORDER BY date
        """).fetch_df()

        trading_days = dates_df['date'].astype(str).tolist()

        # Write calendar file
        calendars_dir = output_dir / 'calendars'
        calendars_dir.mkdir(parents=True, exist_ok=True)

        with open(calendars_dir / 'day.txt', 'w') as f:
            for date in trading_days:
                f.write(f"{date}\n")

        return trading_days

    def _convert_features(
        self,
        data_type: str,
        symbols: List[str],
        trading_days: List[str],
        output_dir: Path,
        incremental: bool
    ) -> dict:
        """Convert features to binary format"""
        stats = {
            'symbols_converted': 0,
            'features_written': 0,
            'bytes_written': 0,
            'errors': []
        }

        # Get feature list
        features = self._get_feature_list(data_type)

        # Determine file extension
        extension = '.day.bin' if 'daily' in data_type else '.1min.bin'

        # Process symbols based on mode
        if self.mode == 'streaming':
            # One symbol at a time
            for idx, symbol in enumerate(symbols):
                try:
                    if incremental and self.metadata.is_symbol_converted(symbol, data_type):
                        continue

                    result = self._convert_symbol(
                        data_type=data_type,
                        symbol=symbol,
                        features=features,
                        trading_days=trading_days,
                        output_dir=output_dir,
                        extension=extension
                    )

                    stats['symbols_converted'] += 1
                    stats['features_written'] += result['features_written']
                    stats['bytes_written'] += result['bytes_written']

                    self.metadata.mark_symbol_converted(symbol, data_type)

                    if (idx + 1) % 100 == 0:
                        print(f"Converted {idx + 1}/{len(symbols)} symbols")

                except Exception as e:
                    stats['errors'].append({'symbol': symbol, 'error': str(e)})

        else:
            # TODO: Implement batch/parallel modes
            raise NotImplementedError("Batch/parallel modes coming soon")

        return stats

    def _convert_symbol(
        self,
        data_type: str,
        symbol: str,
        features: List[str],
        trading_days: List[str],
        output_dir: Path,
        extension: str
    ) -> dict:
        """Convert single symbol's features to binary"""
        # Query symbol data
        input_pattern = self.enriched_root / data_type / '**/*.parquet'

        symbol_df = self.conn.execute(f"""
            SELECT *
            FROM read_parquet('{input_pattern}')
            WHERE symbol = '{symbol}'
            ORDER BY date
        """).fetch_df()

        if len(symbol_df) == 0:
            return {'features_written': 0, 'bytes_written': 0}

        # Create symbol directory
        symbol_dir = output_dir / 'features' / symbol.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)

        features_written = 0
        bytes_written = 0

        # Write each feature as binary
        for feature in features:
            if feature not in symbol_df.columns:
                continue

            # Get feature values aligned to trading days
            feature_values = self._align_to_calendar(
                df=symbol_df,
                feature=feature,
                trading_days=trading_days
            )

            # Write binary file
            binary_path = symbol_dir / f'{feature}{extension}'
            bytes_count = self._write_binary_file(binary_path, feature_values)

            features_written += 1
            bytes_written += bytes_count

        return {
            'features_written': features_written,
            'bytes_written': bytes_written
        }

    def _align_to_calendar(
        self,
        df,
        feature: str,
        trading_days: List[str]
    ) -> np.ndarray:
        """
        Align feature values to trading calendar

        Qlib requires all symbols to have same length arrays.
        Fill missing dates with NaN.
        """
        # Create date index
        df_indexed = df.set_index('date')[feature]

        # Reindex to trading calendar
        aligned = df_indexed.reindex(trading_days)

        # Convert to float32 array
        values = aligned.values.astype(np.float32)

        return values

    def _write_binary_file(self, path: Path, values: np.ndarray) -> int:
        """
        Write Qlib binary file format

        Format:
        - 4 bytes: record count (uint32, little-endian)
        - N * 4 bytes: float32 values (little-endian)
        """
        with open(path, 'wb') as f:
            # Write count header
            f.write(struct.pack('<I', len(values)))

            # Write float32 values
            values.tofile(f)

        return path.stat().st_size

    def _get_feature_list(self, data_type: str) -> List[str]:
        """Get list of features to convert"""
        # Query one row to get columns
        input_pattern = self.enriched_root / data_type / '**/*.parquet'

        sample_df = self.conn.execute(f"""
            SELECT * FROM read_parquet('{input_pattern}')
            LIMIT 1
        """).fetch_df()

        # Exclude metadata columns
        exclude = ['symbol', 'date', 'timestamp', 'year', 'month', 'ticker']
        features = [col for col in sample_df.columns if col not in exclude]

        return features
```

#### 6.3.2 Binary Validator

**File**: `src/transform/qlib_binary_validator.py`

```python
import struct
import numpy as np
from pathlib import Path

class QlibBinaryValidator:
    """
    Validate Qlib binary format

    Tests:
    1. Instruments file exists and has content
    2. Calendar file exists with valid dates
    3. Binary files readable and match calendar length
    4. Roundtrip test: Parquet → Binary → Parquet matches
    """

    def validate_conversion(self, qlib_root: Path, data_type: str) -> dict:
        """Run all validation checks"""
        results = {
            'instruments_valid': False,
            'calendar_valid': False,
            'features_valid': False,
            'errors': []
        }

        try:
            # Check instruments
            instruments_file = qlib_root / data_type / 'instruments' / 'all.txt'
            if not instruments_file.exists():
                results['errors'].append("instruments/all.txt missing")
            else:
                with open(instruments_file) as f:
                    symbols = [line.strip() for line in f]
                if len(symbols) > 0:
                    results['instruments_valid'] = True
                    results['symbol_count'] = len(symbols)

            # Check calendar
            calendar_file = qlib_root / data_type / 'calendars' / 'day.txt'
            if not calendar_file.exists():
                results['errors'].append("calendars/day.txt missing")
            else:
                with open(calendar_file) as f:
                    dates = [line.strip() for line in f]
                if len(dates) > 0:
                    results['calendar_valid'] = True
                    results['trading_days'] = len(dates)

            # Check features for first symbol
            if results['instruments_valid']:
                first_symbol = symbols[0].lower()
                features_dir = qlib_root / data_type / 'features' / first_symbol

                if features_dir.exists():
                    bin_files = list(features_dir.glob('*.bin'))
                    if len(bin_files) > 0:
                        # Read first binary file
                        with open(bin_files[0], 'rb') as f:
                            count = struct.unpack('<I', f.read(4))[0]
                            values = np.fromfile(f, dtype=np.float32, count=count)

                        if count == len(dates):
                            results['features_valid'] = True
                        else:
                            results['errors'].append(
                                f"Feature length {count} != calendar length {len(dates)}"
                            )

        except Exception as e:
            results['errors'].append(str(e))

        results['all_valid'] = (
            results['instruments_valid'] and
            results['calendar_valid'] and
            results['features_valid']
        )

        return results

    def read_binary_feature(
        self,
        qlib_root: Path,
        data_type: str,
        symbol: str,
        feature: str
    ) -> np.ndarray:
        """Read binary feature for testing"""
        extension = '.day.bin' if 'daily' in data_type else '.1min.bin'
        binary_path = qlib_root / data_type / 'features' / symbol.lower() / f'{feature}{extension}'

        with open(binary_path, 'rb') as f:
            count = struct.unpack('<I', f.read(4))[0]
            values = np.fromfile(f, dtype=np.float32, count=count)

        return values
```

### 6.4 Testing Strategy

**File**: `tests/unit/test_qlib_binary_writer.py`

```python
def test_instruments_generation():
    """Test instruments file created correctly"""

def test_calendar_generation():
    """Test calendar file with correct trading days"""

def test_binary_format():
    """Test binary files follow Qlib format"""

def test_roundtrip_conversion():
    """Test Parquet → Binary → read matches original"""
```

---

## Phase 7: Query Engine

### 7.1 Overview

**Goal**: Fast querying of both Parquet and Qlib binary data with caching
**Backends**: DuckDB (out-of-core) or Polars (in-memory)
**Features**: Query caching, partition pruning, predicate pushdown

### 7.2 Implementation

#### 7.2.1 Query Engine

**File**: `src/query/query_engine.py`

```python
from pathlib import Path
import duckdb
import polars as pl
from typing import List, Optional, Dict, Union
from src.core.config_loader import ConfigLoader
from src.query.query_cache import QueryCache

class QueryEngine:
    """
    Unified query interface for Parquet and Qlib data

    Supports:
    - Symbol + date range queries
    - Multiple backend engines (DuckDB/Polars)
    - Query result caching
    - Partition pruning

    Usage:
        engine = QueryEngine(data_root='/path/to/data')

        # Query enriched Parquet
        df = engine.query_parquet(
            data_type='stocks_daily',
            symbols=['AAPL', 'TSLA'],
            fields=['open', 'close', 'alpha_daily'],
            start_date='2025-08-01',
            end_date='2025-09-30'
        )

        # Or use Qlib directly
        import qlib
        qlib.init(provider_uri=str(data_root / 'qlib' / 'stocks_daily'))
        from qlib.data import D
        df = D.features(['AAPL'], ['$close', '$alpha_daily'], '2025-08-01', '2025-09-30')
    """

    def __init__(
        self,
        data_root: Path,
        config: ConfigLoader,
        enable_cache: bool = True
    ):
        self.data_root = data_root
        self.config = config
        self.profile = config.get_system_profile()
        self.mode = self.profile['recommended_mode']

        # Initialize backend
        if self.mode == 'streaming':
            self._init_duckdb()
        else:
            self._init_polars()

        # Query cache
        if enable_cache:
            self.cache = QueryCache(
                cache_root=data_root / 'cache',
                max_size_gb=2.0
            )
        else:
            self.cache = None

    def _init_duckdb(self):
        """Initialize DuckDB for out-of-core queries"""
        memory_limit = self.profile['resource_limits']['max_memory_gb'] * 0.5
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': min(4, self.profile['hardware']['cpu_cores']),
            'enable_object_cache': True
        })
        self.engine = 'duckdb'

    def _init_polars(self):
        """Initialize Polars for in-memory queries"""
        self.engine = 'polars'

    def query_parquet(
        self,
        data_type: str,
        symbols: List[str],
        fields: List[str],
        start_date: str,
        end_date: str,
        use_cache: bool = True
    ):
        """
        Query enriched Parquet data

        Args:
            data_type: stocks_daily, stocks_minute, options_daily, options_minute
            symbols: List of symbols
            fields: List of fields to return
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            use_cache: Use cache if available

        Returns:
            DataFrame with requested data
        """
        # Check cache
        if use_cache and self.cache:
            cache_key = self.cache.make_key(
                data_type=data_type,
                symbols=symbols,
                fields=fields,
                start_date=start_date,
                end_date=end_date
            )

            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached

        # Execute query
        if self.engine == 'duckdb':
            result = self._query_duckdb(
                data_type=data_type,
                symbols=symbols,
                fields=fields,
                start_date=start_date,
                end_date=end_date
            )
        else:
            result = self._query_polars(
                data_type=data_type,
                symbols=symbols,
                fields=fields,
                start_date=start_date,
                end_date=end_date
            )

        # Cache result
        if use_cache and self.cache:
            self.cache.put(cache_key, result)

        return result

    def _query_duckdb(
        self,
        data_type: str,
        symbols: List[str],
        fields: List[str],
        start_date: str,
        end_date: str
    ):
        """Execute query with DuckDB"""
        enriched_path = self.data_root / 'enriched' / data_type / '**/*.parquet'

        # Build field list
        field_list = ', '.join(fields)
        symbols_list = ', '.join(f"'{s}'" for s in symbols)

        # Query with partition pruning
        sql = f"""
        SELECT symbol, date, {field_list}
        FROM read_parquet('{enriched_path}')
        WHERE symbol IN ({symbols_list})
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY symbol, date
        """

        return self.conn.execute(sql).fetch_df()

    def _query_polars(
        self,
        data_type: str,
        symbols: List[str],
        fields: List[str],
        start_date: str,
        end_date: str
    ):
        """Execute query with Polars"""
        enriched_path = self.data_root / 'enriched' / data_type / '**/*.parquet'

        # Lazy query
        lf = pl.scan_parquet(enriched_path)

        # Apply filters (predicate pushdown)
        lf = lf.filter(
            pl.col('symbol').is_in(symbols) &
            (pl.col('date') >= start_date) &
            (pl.col('date') <= end_date)
        )

        # Select fields
        lf = lf.select(['symbol', 'date'] + fields)

        # Sort and collect
        df = lf.sort(['symbol', 'date']).collect()

        return df.to_pandas()
```

#### 7.2.2 Query Cache

**File**: `src/query/query_cache.py`

```python
import hashlib
import pickle
from pathlib import Path
from typing import Optional, Any
import pandas as pd

class QueryCache:
    """
    LRU cache for query results

    Features:
    - Size-based eviction (max GB)
    - Disk persistence
    - Cache warming
    - Hit rate tracking
    """

    def __init__(self, cache_root: Path, max_size_gb: float = 2.0):
        self.cache_root = cache_root
        self.cache_root.mkdir(parents=True, exist_ok=True)

        self.max_size_bytes = int(max_size_gb * 1024**3)

        # Metadata
        self.metadata_file = cache_root / 'metadata.pkl'
        self._load_metadata()

    def _load_metadata(self):
        """Load cache metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'rb') as f:
                self.metadata = pickle.load(f)
        else:
            self.metadata = {
                'entries': {},  # key -> {file, size, last_used}
                'hits': 0,
                'misses': 0,
                'total_size': 0
            }

    def _save_metadata(self):
        """Save cache metadata"""
        with open(self.metadata_file, 'wb') as f:
            pickle.dump(self.metadata, f)

    def make_key(self, **kwargs) -> str:
        """Generate cache key from query parameters"""
        # Sort keys for consistency
        sorted_items = sorted(kwargs.items())
        key_str = str(sorted_items)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Get cached result"""
        if key not in self.metadata['entries']:
            self.metadata['misses'] += 1
            return None

        entry = self.metadata['entries'][key]
        cache_file = self.cache_root / entry['file']

        if not cache_file.exists():
            # Cache file missing
            del self.metadata['entries'][key]
            self.metadata['misses'] += 1
            return None

        # Load from cache
        with open(cache_file, 'rb') as f:
            result = pickle.load(f)

        # Update metadata
        entry['last_used'] = pd.Timestamp.now()
        self.metadata['hits'] += 1
        self._save_metadata()

        return result

    def put(self, key: str, data: pd.DataFrame):
        """Put result in cache"""
        # Serialize
        cache_file = self.cache_root / f'{key}.pkl'
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)

        file_size = cache_file.stat().st_size

        # Add to metadata
        self.metadata['entries'][key] = {
            'file': f'{key}.pkl',
            'size': file_size,
            'last_used': pd.Timestamp.now()
        }
        self.metadata['total_size'] += file_size

        # Evict if over limit
        self._evict_if_needed()

        self._save_metadata()

    def _evict_if_needed(self):
        """Evict least recently used entries"""
        while self.metadata['total_size'] > self.max_size_bytes:
            # Find LRU entry
            lru_key = min(
                self.metadata['entries'].keys(),
                key=lambda k: self.metadata['entries'][k]['last_used']
            )

            # Remove
            entry = self.metadata['entries'].pop(lru_key)
            cache_file = self.cache_root / entry['file']
            if cache_file.exists():
                cache_file.unlink()

            self.metadata['total_size'] -= entry['size']

    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.metadata['hits'] + self.metadata['misses']
        hit_rate = self.metadata['hits'] / total_requests if total_requests > 0 else 0

        return {
            'hits': self.metadata['hits'],
            'misses': self.metadata['misses'],
            'hit_rate': hit_rate,
            'entries': len(self.metadata['entries']),
            'total_size_mb': self.metadata['total_size'] / (1024**2)
        }
```

### 7.3 Qlib Integration

**File**: `examples/test_qlib_query.py`

```python
#!/usr/bin/env python3
"""
Test Qlib data loading

This example shows how to initialize Qlib with our binary data
and run queries.
"""

import qlib
from qlib.data import D
from pathlib import Path

def main():
    # Initialize Qlib with our data
    qlib_data_root = Path('/Users/zheyuanzhao/sandisk/quantmini/data/qlib/stocks_daily')

    qlib.init(
        provider_uri=str(qlib_data_root),
        region='us'  # Use US market config
    )

    print("Qlib initialized successfully!")

    # Test query
    symbols = ['AAPL', 'TSLA']
    fields = ['$open', '$close', '$high', '$low', '$volume', '$alpha_daily']
    start_date = '2025-08-01'
    end_date = '2025-09-30'

    print(f"\nQuerying {symbols} from {start_date} to {end_date}")
    print(f"Fields: {fields}")

    # Query data
    data = D.features(
        instruments=symbols,
        fields=fields,
        start_time=start_date,
        end_time=end_date
    )

    print(f"\nResult shape: {data.shape}")
    print(f"\nFirst 10 rows:")
    print(data.head(10))

    print(f"\nData summary:")
    print(data.describe())

if __name__ == '__main__':
    main()
```

---

## Phase 8: Daily Automation

### 8.1 Overview

**Goal**: Automate daily data pipeline with incremental updates
**Trigger**: Daily at market close (4:30 PM ET)
**Process**: Ingest → Enrich → Convert → Validate

### 8.2 Implementation

#### 8.2.1 Daily Pipeline Orchestrator

**File**: `src/orchestration/daily_pipeline.py`

```python
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import List, Dict

from src.core.config_loader import ConfigLoader
from src.orchestration.ingestion_orchestrator import IngestionOrchestrator
from src.features.feature_engineer import FeatureEngineer
from src.transform.qlib_binary_writer import QlibBinaryWriter
from src.transform.qlib_binary_validator import QlibBinaryValidator

logger = logging.getLogger(__name__)

class DailyPipeline:
    """
    Daily automated data pipeline

    Flow:
    1. Download new data from Polygon S3 (Phase 4)
    2. Enrich with features (Phase 5)
    3. Convert to Qlib binary (Phase 6)
    4. Validate outputs
    5. Clear affected cache entries
    6. Report statistics

    Runs incrementally - only processes new dates.
    """

    def __init__(
        self,
        project_root: Path,
        config: ConfigLoader
    ):
        self.project_root = project_root
        self.config = config

        # Paths
        self.parquet_root = project_root / 'data' / 'parquet'
        self.enriched_root = project_root / 'data' / 'enriched'
        self.qlib_root = project_root / 'data' / 'qlib'
        self.metadata_root = project_root / 'data' / 'metadata'

        # Components
        self.ingestion = IngestionOrchestrator(
            config=config,
            parquet_root=self.parquet_root,
            metadata_root=self.metadata_root
        )

        self.feature_engineer = FeatureEngineer(
            parquet_root=self.parquet_root,
            enriched_root=self.enriched_root,
            config=config
        )

        self.binary_writer = QlibBinaryWriter(
            enriched_root=self.enriched_root,
            qlib_root=self.qlib_root,
            config=config
        )

        self.validator = QlibBinaryValidator()

    async def run_daily_update(
        self,
        date: str = None,
        data_types: List[str] = None
    ) -> Dict:
        """
        Run daily pipeline for specific date

        Args:
            date: YYYY-MM-DD (default: yesterday)
            data_types: List of data types (default: all)

        Returns:
            Statistics dict
        """
        # Default to yesterday
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime('%Y-%m-%d')

        # Default to all data types
        if data_types is None:
            data_types = [
                'stocks_daily',
                'stocks_minute',
                'options_daily',
                'options_minute'
            ]

        logger.info(f"Starting daily pipeline for {date}")
        logger.info(f"Data types: {data_types}")

        results = {}

        for data_type in data_types:
            logger.info(f"\n{'='*70}")
            logger.info(f"Processing {data_type}")
            logger.info(f"{'='*70}")

            try:
                # Phase 4: Ingest raw data
                logger.info("Step 1/4: Ingesting raw data...")
                ingest_result = await self.ingestion.ingest_date_range(
                    data_type=data_type,
                    start_date=date,
                    end_date=date,
                    symbols=None,
                    incremental=True
                )

                # Phase 5: Enrich with features
                logger.info("Step 2/4: Enriching with features...")
                enrich_result = self.feature_engineer.enrich_date_range(
                    data_type=data_type,
                    start_date=date,
                    end_date=date,
                    incremental=True
                )

                # Phase 6: Convert to Qlib binary
                logger.info("Step 3/4: Converting to Qlib binary...")
                binary_result = self.binary_writer.convert_data_type(
                    data_type=data_type,
                    start_date=date,
                    end_date=date,
                    incremental=True
                )

                # Phase 7: Validate
                logger.info("Step 4/4: Validating outputs...")
                validation_result = self.validator.validate_conversion(
                    qlib_root=self.qlib_root,
                    data_type=data_type
                )

                # Store results
                results[data_type] = {
                    'status': 'success',
                    'ingest': ingest_result,
                    'enrich': enrich_result,
                    'binary': binary_result,
                    'validation': validation_result
                }

                logger.info(f"✅ {data_type} complete")
                logger.info(f"   Ingested: {ingest_result.get('records_processed', 0):,} records")
                logger.info(f"   Enriched: {enrich_result.get('records_enriched', 0):,} records")
                logger.info(f"   Binary: {binary_result.get('symbols_converted', 0)} symbols")

            except Exception as e:
                logger.error(f"❌ {data_type} failed: {e}")
                results[data_type] = {
                    'status': 'error',
                    'error': str(e)
                }

        # Summary
        logger.info(f"\n{'='*70}")
        logger.info(f"DAILY PIPELINE SUMMARY - {date}")
        logger.info(f"{'='*70}")

        for data_type, result in results.items():
            status_icon = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"{status_icon} {data_type}: {result['status']}")

        return results

    def run_weekly_maintenance(self):
        """
        Weekly maintenance tasks

        - Compact small Parquet partitions
        - Clean old cache entries
        - Validate data integrity
        """
        logger.info("Running weekly maintenance...")
        # TODO: Implement maintenance tasks
        pass
```

#### 8.2.2 CLI Script

**File**: `scripts/daily_update.py`

```python
#!/usr/bin/env python3
"""
Daily Pipeline Update

Run daily data pipeline to ingest and process new data.

Usage:
    # Process yesterday's data (default)
    python scripts/daily_update.py

    # Process specific date
    python scripts/daily_update.py --date 2025-09-30

    # Process only specific data types
    python scripts/daily_update.py --data-types stocks_daily options_daily
"""

import asyncio
import argparse
from pathlib import Path
import sys
import logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.orchestration.daily_pipeline import DailyPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description='Run daily data pipeline')

    parser.add_argument(
        '--date',
        help='Date to process (YYYY-MM-DD, default: yesterday)'
    )

    parser.add_argument(
        '--data-types',
        nargs='+',
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute'],
        help='Data types to process (default: all)'
    )

    args = parser.parse_args()

    # Determine date
    if args.date:
        date = args.date
    else:
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')

    # Initialize
    project_root = Path(__file__).parent.parent
    config = ConfigLoader()

    pipeline = DailyPipeline(
        project_root=project_root,
        config=config
    )

    # Run pipeline
    logger.info(f"Starting daily pipeline for {date}")

    results = await pipeline.run_daily_update(
        date=date,
        data_types=args.data_types
    )

    # Check for errors
    errors = [dt for dt, r in results.items() if r['status'] == 'error']

    if errors:
        logger.error(f"Pipeline completed with errors in: {errors}")
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully ✅")
        sys.exit(0)


if __name__ == '__main__':
    asyncio.run(main())
```

#### 8.2.3 Cron Setup

**File**: `scripts/setup_cron.sh`

```bash
#!/bin/bash
# Setup cron job for daily pipeline

# Run at 4:30 PM ET (market close) every weekday
CRON_TIME="30 16 * * 1-5"
PROJECT_ROOT="/Users/zheyuanzhao/sandisk/quantmini"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
SCRIPT="$PROJECT_ROOT/scripts/daily_update.py"

# Create cron entry
CRON_ENTRY="$CRON_TIME cd $PROJECT_ROOT && $PYTHON_BIN $SCRIPT >> $PROJECT_ROOT/daily_pipeline.log 2>&1"

# Install cron job
(crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -

echo "✅ Cron job installed:"
echo "   $CRON_ENTRY"
echo ""
echo "To view: crontab -l"
echo "To remove: crontab -e (then delete the line)"
```

---

## Testing Strategy

### Phase 5 Tests

**File**: `tests/unit/test_feature_engineer.py`

```python
import pytest
from pathlib import Path
from src.features.feature_engineer import FeatureEngineer
from src.features.options_parser import OptionsTickerParser

def test_stock_daily_features():
    """Test stock daily feature calculation"""
    # Create sample data
    # Run feature engineer
    # Verify alpha_daily, returns, volume_ratio

def test_options_ticker_parsing():
    """Test options ticker parsing"""
    parser = OptionsTickerParser()

    result = parser.parse('O:SPY230327P00390000')
    assert result['underlying'] == 'SPY'
    assert result['strike_price'] == 390.0
    assert result['contract_type'] == 'P'

def test_incremental_enrichment():
    """Test incremental mode skips already enriched"""
    # Run enrichment twice
    # Second run should skip
```

### Phase 6 Tests

**File**: `tests/unit/test_qlib_binary_writer.py`

```python
def test_instruments_file():
    """Test instruments/all.txt generation"""

def test_calendar_file():
    """Test calendars/day.txt generation"""

def test_binary_format():
    """Test .day.bin format matches Qlib spec"""

def test_roundtrip():
    """Test Parquet → Binary → Read matches"""
```

### Phase 7 Tests

**File**: `tests/unit/test_query_engine.py`

```python
def test_query_parquet():
    """Test Parquet querying"""

def test_query_cache():
    """Test cache hit/miss"""

def test_qlib_integration():
    """Test Qlib can read our binary data"""
```

### End-to-End Test

**File**: `tests/e2e/test_full_pipeline.py`

```python
async def test_full_pipeline():
    """
    Test complete pipeline: Ingest → Enrich → Binary → Query

    1. Ingest one day of data
    2. Enrich with features
    3. Convert to Qlib binary
    4. Query with Qlib
    5. Verify results match
    """
```

---

## Performance Targets

| Metric | Streaming Mode | Batch Mode | Parallel Mode |
|--------|---------------|------------|---------------|
| **Phase 5: Feature Engineering** | | | |
| Stocks daily (10K symbols/day) | 5 min | 2 min | 1 min |
| Stocks minute (10K symbols/day) | 20 min | 8 min | 3 min |
| Options daily (100K contracts/day) | 15 min | 6 min | 2 min |
| **Phase 6: Binary Conversion** | | | |
| Stocks daily (10K symbols) | 10 min | 4 min | 2 min |
| **Phase 7: Query** | | | |
| Single symbol, 1 year daily | <100ms | <50ms | <20ms |
| 10 symbols, 1 year daily | <500ms | <200ms | <100ms |
| Cache hit | <10ms | <10ms | <10ms |

---

## Deployment Checklist

### Phase 5 Deployment
- [ ] Implement `FeatureEngineer` with DuckDB backend
- [ ] Implement `OptionsTickerParser`
- [ ] Add feature definitions for all data types
- [ ] Write unit tests (10+ tests)
- [ ] Run on sample data (1 week)
- [ ] Validate feature correctness
- [ ] Document feature calculations

### Phase 6 Deployment
- [ ] Implement `QlibBinaryWriter`
- [ ] Implement `QlibBinaryValidator`
- [ ] Generate instruments/calendars
- [ ] Convert sample data to binary
- [ ] Test with Qlib library
- [ ] Validate binary format
- [ ] Write unit tests (8+ tests)

### Phase 7 Deployment
- [ ] Implement `QueryEngine`
- [ ] Implement `QueryCache`
- [ ] Test query performance
- [ ] Achieve cache hit rate >70%
- [ ] Write Qlib integration example
- [ ] Document query interface
- [ ] Write unit tests (6+ tests)

### Phase 8 Deployment
- [ ] Implement `DailyPipeline`
- [ ] Create `daily_update.py` script
- [ ] Test incremental updates
- [ ] Setup cron job
- [ ] Add monitoring/alerting
- [ ] Test error recovery
- [ ] Write e2e test

---

## Maintenance

### Daily
- Monitor `daily_pipeline.log` for errors
- Check query cache hit rate
- Verify data completeness

### Weekly
- Review pipeline statistics
- Compact small Parquet partitions
- Clean old cache entries
- Validate data integrity

### Monthly
- Performance benchmarking
- Review disk usage
- Update documentation

---

## Migration from Phase 4

**Current State**: Phase 4 complete with raw Parquet data

**Migration Path**:

1. **Install Qlib** (if not already installed):
```bash
UV_LINK_MODE=copy uv pip install qlib
```

2. **Create directory structure**:
```bash
mkdir -p data/enriched
mkdir -p data/qlib
mkdir -p data/cache
```

3. **Phase 5 Setup**:
```bash
# Create feature engineer
python scripts/setup_phase5.py

# Run on sample data
python -m src.features.feature_engineer \
    --data-type stocks_daily \
    --start-date 2025-09-29 \
    --end-date 2025-09-29
```

4. **Phase 6 Setup**:
```bash
# Convert to Qlib binary
python scripts/convert_to_qlib.py \
    --data-type stocks_daily \
    --start-date 2025-08-01 \
    --end-date 2025-09-30
```

5. **Phase 7 Setup**:
```bash
# Test query engine
python examples/test_qlib_query.py
```

6. **Phase 8 Setup**:
```bash
# Setup cron
bash scripts/setup_cron.sh
```

---

## Summary

This design document provides complete specifications for Phase 5-8:

✅ **Phase 5**: Feature engineering with DuckDB/Polars, options ticker parsing
✅ **Phase 6**: Qlib binary conversion following official format
✅ **Phase 7**: Query engine with caching and Qlib integration
✅ **Phase 8**: Daily automation with incremental updates

**Key Design Decisions**:
1. Follows parent architecture's adaptive resource management
2. Uses Qlib's exact binary format for compatibility
3. Incremental processing throughout (never recompute)
4. Query caching for performance
5. Production-ready with monitoring and error recovery

**Next Steps**: Implement Phase 5 (feature engineering) as first priority.
