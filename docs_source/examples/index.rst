Examples
========

Example scripts demonstrating QuantMini's Medallion Architecture data pipeline.

Overview
--------

All examples are available in the `examples/ directory <https://github.com/nittygritty-zzy/quantmini/tree/main/examples>`_ of the repository.

Data Pipeline Examples
----------------------

**Download Data**

.. code-block:: bash

   # Download stocks daily data (Bronze Layer)
   source .venv/bin/activate
   uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31

   # Download news articles (8+ years available)
   uv run python scripts/download/download_news_1year.py

   # Download ticker events (optimized batch downloader)
   uv run python scripts/download/download_ticker_events_optimized.py

**Load and Query Data**

.. code-block:: python

   from src.utils.data_loader import DataLoader
   import polars as pl

   # Initialize loader
   loader = DataLoader()

   # Load stocks daily data
   df = loader.load_stocks_daily(
       symbols=['AAPL', 'MSFT', 'TSLA'],
       start_date='2024-01-01',
       end_date='2024-12-31',
       columns=['date', 'close', 'volume']
   )

   # Filter by conditions
   high_volume = df.filter(pl.col('volume') > 1_000_000)

   # Convert to pandas if needed
   df_pandas = df.to_pandas()

**Generate Features** (Silver Layer)

.. code-block:: bash

   # Generate Alpha158 technical indicators
   uv run python scripts/features/generate_alpha158.py

**Convert to Qlib** (Gold Layer)

.. code-block:: bash

   # Convert to Qlib binary format for ML backtesting
   uv run python scripts/qlib/convert_to_qlib.py

Polygon REST API Examples
--------------------------

**News Downloader**

.. code-block:: python

   from src.download.polygon_rest_client import PolygonRESTClient
   from src.download.news import NewsDownloader
   from pathlib import Path

   async def download_news():
       async with PolygonRESTClient(api_key="YOUR_API_KEY") as client:
           downloader = NewsDownloader(
               client=client,
               output_dir=Path("bronze/news"),
               use_partitioned_structure=True
           )

           # Download news for AAPL
           await downloader.download_news_batch(
               tickers=['AAPL'],
               published_utc_gte='2024-01-01',
               published_utc_lte='2024-12-31',
               limit=1000
           )

**Ticker Events Downloader**

.. code-block:: python

   from src.download.corporate_actions_optimized import OptimizedTickerEventsDownloader

   async def download_events():
       async with PolygonRESTClient(api_key="YOUR_API_KEY") as client:
           downloader = OptimizedTickerEventsDownloader(
               client=client,
               output_dir=Path("bronze/corporate_actions"),
               use_partitioned_structure=True
           )

           # Download ticker events (optimized batch processing)
           await downloader.download_ticker_events_optimized(
               tickers=['AAPL', 'MSFT', 'TSLA'],
               chunk_size=1000,
               save_interval=500
           )

Data Loader Examples
--------------------

**Load Stocks Daily**

.. code-block:: python

   from src.utils.data_loader import DataLoader
   import polars as pl

   loader = DataLoader()

   # Basic load
   df = loader.load_stocks_daily(
       symbols=['AAPL'],
       start_date='2024-01-01',
       end_date='2024-12-31'
   )

   # With column selection
   df = loader.load_stocks_daily(
       symbols=['AAPL', 'MSFT'],
       start_date='2024-01-01',
       end_date='2024-12-31',
       columns=['date', 'close', 'volume', 'vwap']
   )

   # Calculate returns
   df = df.with_columns([
       ((pl.col('close') - pl.col('close').shift(1)) / pl.col('close').shift(1))
       .alias('return')
   ])

**Load News Data**

.. code-block:: python

   # Load news articles
   news_df = loader.load_news(
       tickers=['AAPL'],
       start_date='2024-01-01',
       end_date='2024-12-31'
   )

   # Filter by sentiment
   positive_news = news_df.filter(
       pl.col('sentiment_score') > 0.5
   )

**Load Fundamentals**

.. code-block:: python

   # Load fundamental data
   fundamentals_df = loader.load_fundamentals(
       tickers=['AAPL', 'MSFT'],
       start_date='2020-01-01',
       end_date='2024-12-31'
   )

Qlib Integration Examples
--------------------------

**Initialize Qlib**

.. code-block:: python

   import qlib
   from qlib.data import D

   # Initialize Qlib with binary data
   qlib.init(
       provider_uri='gold/qlib/stocks_daily',
       region='us'
   )

   # Load data
   instruments = D.instruments('all')
   data = D.features(
       instruments,
       ['$close', '$volume', '$high', '$low'],
       start_time='2024-01-01',
       end_time='2024-12-31'
   )

**Backtest with Qlib**

See the comprehensive Qlib examples in the repository for ML model training and backtesting.

Complete Workflow Example
--------------------------

**End-to-End Data Pipeline**

.. code-block:: python

   """
   Complete workflow: Download → Enrich → Convert → Backtest
   """

   # 1. Download data (Bronze Layer)
   # Run: uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31

   # 2. Generate features (Silver Layer)
   # Run: uv run python scripts/features/generate_alpha158.py

   # 3. Convert to Qlib (Gold Layer)
   # Run: uv run python scripts/qlib/convert_to_qlib.py

   # 4. Query and analyze
   from src.utils.data_loader import DataLoader
   import polars as pl

   loader = DataLoader()
   df = loader.load_stocks_daily(
       symbols=['AAPL', 'MSFT'],
       start_date='2024-01-01',
       end_date='2024-12-31'
   )

   # Calculate statistics
   stats = df.group_by('ticker').agg([
       pl.col('close').mean().alias('avg_close'),
       pl.col('volume').mean().alias('avg_volume'),
       pl.col('close').std().alias('volatility')
   ])

   print(stats)

Installation
------------

To run the examples:

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/nittygritty-zzy/quantmini.git
   cd quantmini

   # Install with uv
   uv sync

   # Activate environment
   source .venv/bin/activate

   # Configure credentials
   cp config/credentials.yaml.example config/credentials.yaml
   # Edit config/credentials.yaml with your Polygon.io API key

Running Examples
----------------

**Data Download Examples**

.. code-block:: bash

   # Download stocks daily
   uv run python -m src.cli.main data ingest -t stocks_daily -s 2024-01-01 -e 2024-12-31

   # Download news (1 year)
   uv run python scripts/download/download_news_1year.py

   # Download ticker events (optimized)
   uv run python scripts/download/download_ticker_events_optimized.py

**Data Query Examples**

.. code-block:: bash

   # Run data loader example
   uv run python examples/data_loader_example.py

See the `examples/ directory <https://github.com/nittygritty-zzy/quantmini/tree/main/examples>`_ for more working examples.

Additional Resources
--------------------

- **Documentation**: See ``docs/`` directory for comprehensive guides
- **API Reference**: Full API documentation in ``docs_source/api/``
- **Testing**: Run ``pytest tests/`` to see more usage examples
