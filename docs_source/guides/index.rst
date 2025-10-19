Guides
======

User guides and tutorials for QuantMini's Medallion Architecture data pipeline.

.. toctree::
   :maxdepth: 2

   quickstart
   medallion-architecture
   batch-downloader
   data-loader
   alpha158-features

Quickstart
----------

Get started with QuantMini in 10 minutes.

**Installation**

.. code-block:: bash

   # Clone repository
   git clone https://github.com/nittygritty-zzy/quantmini.git
   cd quantmini

   # Install with uv
   uv sync

   # Configure credentials
   cp config/credentials.yaml.example config/credentials.yaml

**Download Data**

.. code-block:: bash

   # Activate environment
   source .venv/bin/activate

   # Download stocks data (Bronze Layer)
   uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31

   # Download news articles (8+ years available)
   uv run python scripts/download/download_news_1year.py

**Query Data**

.. code-block:: python

   from src.utils.data_loader import DataLoader

   # Initialize loader
   loader = DataLoader()

   # Load stocks data
   df = loader.load_stocks_daily(
       symbols=['AAPL', 'MSFT'],
       start_date='2024-01-01',
       end_date='2024-12-31'
   )

Medallion Architecture
----------------------

QuantMini uses a structured data lake pattern:

.. code-block:: text

   Landing Layer          Bronze Layer         Silver Layer          Gold Layer
   (Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
         ↓                    ↓                    ↓                     ↓
   Polygon.io         →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
     REST API             (Schema Check)        (Indicators)         (Backtesting)
         ↓                    ↓                    ↓                     ↓
   landing/              bronze/{type}/      silver/{type}/        gold/qlib/

**Data Quality Progression**: Raw → Validated → Enriched → ML-Ready

Batch Downloader
----------------

High-performance parallel downloads using Polygon REST API.

**Features**:
- Batch request optimization (100+ concurrent requests)
- Automatic retries with exponential backoff
- Incremental saving to avoid data loss
- Date-first Hive partitioning

**Example**:

.. code-block:: bash

   # Download ticker events for all CS tickers (optimized)
   uv run python scripts/download/download_ticker_events_optimized.py

   # Download 8+ years of news articles
   uv run python scripts/download/download_news_1year.py --start-date 2017-04-10

See detailed guide at ``docs/guides/batch-downloader.md``

Data Loader
-----------

Query and analyze data from the bronze layer efficiently.

**Features**:
- DuckDB-powered SQL queries on Parquet
- Automatic partition pruning
- Multiple output formats (Polars, Pandas, PyArrow)

**Example**:

.. code-block:: python

   from src.utils.data_loader import DataLoader

   loader = DataLoader()

   # Load stocks daily data
   df = loader.load_stocks_daily(
       symbols=['AAPL', 'TSLA'],
       start_date='2024-01-01',
       end_date='2024-12-31',
       columns=['date', 'close', 'volume']
   )

   # Filter by conditions
   df_filtered = df.filter(pl.col('volume') > 1_000_000)

See detailed guide at ``docs/guides/data-loader.md``

Alpha158 Features
-----------------

Generate 158 technical indicators for ML backtesting.

**Features**:
- KBAR (Open-close-high-low features)
- KDJ (Stochastic indicators)
- RSI (Relative strength)
- MACD (Moving average convergence divergence)
- And 154 more technical features

**Example**:

.. code-block:: bash

   # Generate Alpha158 features for silver layer
   uv run python scripts/features/generate_alpha158.py

See detailed guide at ``docs/guides/ALPHA158_FEATURES.md``

Additional Guides
-----------------

For comprehensive guides, see the ``docs/`` directory:

- **Delisted Stocks**: ``docs/guides/delisted-stocks.md``
- **Benchmark Data**: ``docs/guides/BENCHMARK_DATA_GUIDE.md``
- **Trading Signals**: ``docs/guides/TRADING_SIGNALS_GUIDE.md``
- **Testing**: ``docs/guides/testing.md``
