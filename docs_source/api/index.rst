API Reference
=============

Complete API documentation for all QuantMini modules.

Core Modules
------------

.. toctree::
   :maxdepth: 2

   core
   download
   ingest
   storage

Feature Engineering
-------------------

.. toctree::
   :maxdepth: 2

   features
   transform

Query & Orchestration
---------------------

.. toctree::
   :maxdepth: 2

   query
   orchestration

Utilities
---------

.. toctree::
   :maxdepth: 2

   utils

Module Overview
---------------

**Core** (`src.core`)
   System profiling, memory monitoring, configuration, and exceptions

**Download** (`src.download`)
   S3 downloaders (async/sync) and S3 catalog management

**Ingest** (`src.ingest`)
   Data ingestion with Polars and streaming support

**Storage** (`src.storage`)
   Parquet management, metadata tracking, and schemas

**Features** (`src.features`)
   Feature engineering and technical indicators

**Transform** (`src.transform`)
   Qlib binary format conversion and validation

**Query** (`src.query`)
   Query engine with DuckDB and caching

**Orchestration** (`src.orchestration`)
   Pipeline coordination and workflow management

**Utils** (`src.utils`)
   Utility functions and helpers
