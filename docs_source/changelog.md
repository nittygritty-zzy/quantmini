# Changelog

All notable changes to QuantMini will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-09-30

### Added
- Initial release of QuantMini
- Data ingestion pipeline from Polygon.io API
- Qlib binary format conversion
- Alpha expression framework
- Support for ML models: LightGBM, XGBoost, CatBoost
- Trading strategies: TopkDropoutStrategy, EnhancedIndexingStrategy
- 10+ comprehensive example scripts
- Complete documentation
- PyPI packaging and publishing
- GitHub Actions CI/CD workflow
- ReadTheDocs documentation

### Core Modules
- `src.core`: Configuration management, memory monitoring, system profiling
- `src.ingest`: Data ingestion with Polars and streaming support
- `src.storage`: Parquet storage with metadata management
- `src.transform`: Qlib binary writer and validator
- `src.features`: Feature engineering and definitions
- `src.download`: Async/sync downloaders for S3
- `src.query`: Query engine with caching
- `src.orchestration`: Pipeline orchestration

### Examples
- Qlib model examples (LightGBM, XGBoost, CatBoost)
- Custom model implementation examples
- Alpha expression examples
- Strategy examples (TopkDropout, EnhancedIndexing)
- Complete workflow examples

### Documentation
- Installation guide
- Getting started guide
- API reference
- Trading signals guide
- Benchmark data guide
- Changelog documentation

### Infrastructure
- MIT License
- GitHub repository setup
- PyPI publishing with trusted publisher
- GitHub Actions workflows
- Comprehensive .gitignore
- Project memory and cleanup protocols

## [Unreleased]

### Planned
- Additional alpha factor library
- More trading strategy implementations
- Performance optimizations
- Additional data sources
- Real-time data streaming
- Portfolio optimization tools
- Risk management modules
- Backtesting framework enhancements

---

[0.1.0]: https://github.com/nittygritty-zzy/quantmini/releases/tag/v0.1.0
