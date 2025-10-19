QuantMini Documentation
========================

**QuantMini** is a high-performance financial data pipeline with Medallion Architecture and Qlib integration for quantitative trading.

.. image:: https://img.shields.io/github/license/nittygritty-zzy/quantmini.svg
   :target: https://github.com/nittygritty-zzy/quantmini/blob/main/LICENSE
   :alt: License

Features
--------

* **Medallion Architecture**: Bronze → Silver → Gold data lake pattern for data quality
* **Polygon.io Integration**: REST API and S3 flat files data ingestion
* **High-Performance Downloads**: Batch downloaders with massive parallelization
* **Partitioned Storage**: Date-first and ticker-based Hive partitioning
* **Qlib Integration**: Binary format conversion for fast ML backtesting
* **Feature Engineering**: Alpha158 and custom technical indicators
* **News Data**: 8+ years of financial news articles
* **Delisted Stocks**: Complete handling of delisted ticker data

Installation
------------

Clone and install from source with uv:

.. code-block:: bash

   # Clone repository
   git clone https://github.com/nittygritty-zzy/quantmini.git
   cd quantmini

   # Install with uv
   uv sync

   # Configure credentials
   cp config/credentials.yaml.example config/credentials.yaml
   # Edit config/credentials.yaml with your Polygon.io API key

Quick Start
-----------

Check out our :doc:`getting_started` guide or explore the :doc:`examples/index`.

**Data Pipeline Architecture:**

.. code-block:: text

   Landing Layer          Bronze Layer         Silver Layer          Gold Layer
   (Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
        ↓                    ↓                    ↓                     ↓
   Polygon.io         →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
     REST API             (Schema Check)        (Indicators)         (Backtesting)
        ↓                    ↓                    ↓                     ↓
   landing/              bronze/{type}/      silver/{type}/        gold/qlib/

.. toctree::
   :maxdepth: 2
   :caption: User Guide:

   getting_started
   installation
   guides/index
   examples/index

.. toctree::
   :maxdepth: 2
   :caption: API Reference:

   api/index

.. toctree::
   :maxdepth: 1
   :caption: About:

   changelog

.. toctree::
   :maxdepth: 1
   :caption: Development:

   contributing
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
