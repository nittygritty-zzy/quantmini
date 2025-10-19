API Reference
=============

Complete API documentation for all QuantMini modules.

Core Modules
------------

.. toctree::
   :maxdepth: 2

   core
   download
   features
   transform
   utils

Core Infrastructure
-------------------

.. toctree::
   :maxdepth: 2

   core

**Core** (``src.core``)
   System profiling, memory monitoring, configuration management, and exceptions

**Key Classes**:
   - ``ConfigLoader``: Manage credentials and pipeline configuration
   - ``SystemProfiler``: Auto-detect hardware capabilities
   - ``AdvancedMemoryMonitor``: Track memory usage and pressure

Polygon REST API Integration
-----------------------------

.. toctree::
   :maxdepth: 2

   download

**Download** (``src.download``)
   Polygon REST API client and specialized downloaders

**Key Classes**:
   - ``PolygonRESTClient``: Async HTTP client with rate limiting
   - ``NewsDownloader``: Download financial news articles
   - ``BarsDownloader``: Download OHLCV data
   - ``CorporateActionsDownloader``: Download corporate events
   - ``FundamentalsDownloader``: Download fundamental data
   - ``ReferenceDataDownloader``: Download ticker metadata

**Features**:
   - Batch request optimization (100+ concurrent)
   - Automatic retries with exponential backoff
   - Date-first Hive partitioning
   - ZSTD compression

Feature Engineering
-------------------

.. toctree::
   :maxdepth: 2

   features

**Features** (``src.features``)
   Technical indicators and feature engineering

**Key Modules**:
   - ``Alpha158``: 158 technical indicators for Qlib
   - ``FinancialRatios``: Financial metrics and ratios

**Alpha158 Features**:
   - KBAR: Open-close-high-low features
   - KDJ: Stochastic indicators
   - RSI: Relative strength
   - MACD: Moving average convergence divergence
   - And 154 more features

Data Transformation
-------------------

.. toctree::
   :maxdepth: 2

   transform

**Transform** (``src.transform``)
   Qlib binary format conversion and validation

**Key Classes**:
   - ``QlibBinaryWriter``: Convert Parquet to Qlib binary format
   - ``BinaryValidator``: Validate binary data integrity

**Qlib Binary Format**:
   - ``.day.bin``: Daily frequency data
   - ``.1min.bin``: Minute frequency data
   - Symbol-based directory structure
   - Instruments and calendars metadata

Utilities
---------

.. toctree::
   :maxdepth: 2

   utils

**Utils** (``src.utils``)
   Data loader and utility functions

**Key Classes**:
   - ``DataLoader``: High-performance query engine for bronze layer
   - DuckDB-powered SQL queries
   - Multiple output formats (Polars, Pandas, PyArrow)

Module Overview
---------------

**Core** (``src.core``)
   System profiling, memory monitoring, configuration, and exceptions

**Download** (``src.download``)
   Polygon REST API client with specialized downloaders for:

   - News articles (8+ years historical)
   - OHLCV bars (stocks and options)
   - Corporate actions (splits, dividends, ticker changes)
   - Fundamentals (income statements, balance sheets, cash flow)
   - Reference data (ticker metadata, market status)

**Features** (``src.features``)
   Feature engineering and technical indicators:

   - Alpha158: 158 technical indicators
   - Financial ratios: Profitability, liquidity, leverage

**Transform** (``src.transform``)
   Qlib binary format conversion and validation

**Utils** (``src.utils``)
   Data loader for efficient queries on bronze layer

Data Pipeline Architecture
--------------------------

.. code-block:: text

   Landing Layer          Bronze Layer         Silver Layer          Gold Layer
   (Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
         ↓                    ↓                    ↓                     ↓
   Polygon.io         →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
     REST API             (Schema Check)        (Indicators)         (Backtesting)
         ↓                    ↓                    ↓                     ↓
   landing/              bronze/{type}/      silver/{type}/        gold/qlib/

**Directory Structure**:

.. code-block:: text

   $DATA_ROOT/
   ├── landing/           # Raw source data
   ├── bronze/            # Validated Parquet files
   │   ├── stocks_daily/
   │   ├── options_daily/
   │   ├── news/
   │   └── fundamentals/
   ├── silver/            # Feature-enriched data
   │   └── stocks_daily/  # with Alpha158 features
   └── gold/qlib/         # ML-ready binary format
       ├── stocks_daily/
       ├── stocks_minute/
       └── options/

External Documentation
----------------------

- **Polygon.io API**: https://polygon.readthedocs.io/en/latest/
- **Qlib Framework**: https://qlib.readthedocs.io/en/latest/
- **Polars**: https://pola-rs.github.io/polars/
- **DuckDB**: https://duckdb.org/docs/
