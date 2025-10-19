# Changelog

All notable changes to QuantMini will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (2025-10-18)
- **Medallion Architecture**: Bronze → Silver → Gold data lake pattern
- **Polygon REST API Integration**: Direct API downloaders for all endpoints
- **High-Performance Downloaders**: Batch request optimization with massive parallelization
- **Date-First Partitioning**: Year/month/ticker Hive partitioning for efficient queries
- **News Data Support**: 8+ years of historical financial news articles
- **Delisted Stocks Support**: Complete handling of delisted ticker data
- **Data Loader**: High-performance query engine for bronze layer data
- **Optimized Corporate Actions**: Batch ticker events downloader
- **DuckDB Integration**: Fast SQL queries on Parquet files
- **Weekly Automation**: Automated weekly data downloads and processing

### Changed (2025-10-18)
- Migrated from PyPI package to source-based installation
- Replaced S3 flat files with Polygon REST API as primary data source
- Reorganized project structure to Medallion Architecture
- Updated all documentation to reflect new architecture
- Moved from pip to uv package manager

### Core Modules (Current)
- `src.core`: Configuration management, memory monitoring, system profiling
- `src.download`: Polygon REST API client and specialized downloaders
  - News, bars, corporate actions, fundamentals, reference data
  - Batch request optimization for parallel downloads
- `src.features`: Feature engineering (Alpha158, financial ratios)
- `src.transform`: Qlib binary writer and validator
- `src.utils`: Data loader for efficient queries
- `src.cli`: Command-line interface for data operations

### Data Coverage
- **Stocks**: 11,994 symbols
- **Options**: 1,388,382 contracts
- **News Articles**: 8+ years (2017-04-10 to present)
- **Date Range**: 2025-08-01 to 2025-09-30 (primary)
- **Total Records**: 182M+

### Infrastructure
- MIT License
- GitHub repository: https://github.com/nittygritty-zzy/quantmini
- Test coverage: 138 tests, 100% passing
- Documentation: Comprehensive markdown docs in docs/

## [0.1.0] - 2024-09-30

### Added
- Initial release of QuantMini
- Data ingestion pipeline from Polygon.io S3 flat files
- Qlib binary format conversion
- Alpha expression framework
- Support for ML models: LightGBM, XGBoost, CatBoost
- Trading strategies: TopkDropoutStrategy, EnhancedIndexingStrategy
- 10+ comprehensive example scripts
- Complete documentation
- PyPI packaging and publishing

### Core Modules (v0.1.0)
- `src.core`: Configuration management, memory monitoring, system profiling
- `src.ingest`: Data ingestion with Polars and streaming support
- `src.storage`: Parquet storage with metadata management
- `src.transform`: Qlib binary writer and validator
- `src.features`: Feature engineering and definitions
- `src.download`: Async/sync downloaders for S3
- `src.query`: Query engine with caching
- `src.orchestration`: Pipeline orchestration

---

[0.1.0]: https://github.com/nittygritty-zzy/quantmini/releases/tag/v0.1.0
