# QuantMini Documentation

High-Performance Data Pipeline for Financial Market Data - Complete documentation hub.

## 🚀 Quick Links

- **[Installation Guide](getting-started/installation.md)** - Get up and running in 10 minutes
- **[Architecture Overview](architecture/overview.md)** - Understand the system design
- **[API References](api-reference/)** - Polygon.io and Qlib integration guides

## 📚 Documentation Structure

### Getting Started
Start here if you're new to the project:

- **[Installation](getting-started/installation.md)** - Setup and configuration
- **[Data Configuration](getting-started/DATA_CONFIGURATION.md)** - Configure data sources and storage

### Architecture
Understand how the system works:

- **[System Overview](architecture/overview.md)** - High-level architecture and components
- **[Data Pipeline](architecture/data-pipeline.md)** - Pipeline design and optimization
- **[Advanced Features](architecture/advanced-features.md)** - Phase 5-8 implementation details
- **[Medallion Architecture Scripts](architecture/MEDALLION_ARCHITECTURE_SCRIPTS.md)** - Automated download and processing scripts

### API Reference
Integration with external services:

- **[Polygon.io Integration](api-reference/polygon.md)** - S3 flat files data access
  - **Official Docs**: https://polygon.readthedocs.io/en/latest/Library-Interface-Documentation.html
- **[Polygon REST API](api-reference/POLYGON_REST_API.md)** - REST API client and downloaders
- **[Polygon S3 Flat Files](api-reference/polygon-s3-flatfiles.md)** - S3 data structure details
- **[Qlib Integration](api-reference/qlib.md)** - Quantitative research framework
  - **Official Docs**: https://qlib.readthedocs.io/en/latest/reference/api.html

### User Guides
Step-by-step guides for common tasks:

- **[Data Ingestion Strategies](guides/data-ingestion-strategies.md)** - Complete guide: Initial batch load, incremental updates, backfill
- **[Batch Downloader](guides/batch-downloader.md)** - High-performance batch data downloads
- **[Data Loader](guides/data-loader.md)** - Load and query data from the bronze layer
- **[Delisted Stocks](guides/delisted-stocks.md)** - Handle delisted stock data
- **[Alpha158 Features](guides/ALPHA158_FEATURES.md)** - Generate Alpha158 technical indicators
- **[Benchmark Data](guides/BENCHMARK_DATA_GUIDE.md)** - Download and process benchmark data
- **[Trading Signals](guides/TRADING_SIGNALS_GUIDE.md)** - Generate trading signals from features
- **[Testing](guides/testing.md)** - Run tests and validate data

### Reference
Technical reference materials:

- **[Data Schemas](reference/data-schemas.md)** - Complete schema documentation

### Development
For contributors and developers:

- **[Contributing Guide](development/contributing.md)** - How to contribute
- **[Architecture Decisions](development/architecture-decisions.md)** - Design notes and ADRs
- **[Changelog](changelog/README.md)** - Project history and updates

### Examples
Practical examples and sample data:

- **[Sample Data](examples/sample-data/)** - Example CSV files
- **[Code Examples](../examples/)** - Usage examples and notebooks

## 🎯 Common Tasks

### First Time Setup
1. [Install and configure](getting-started/installation.md) the project
2. [Configure data paths](getting-started/DATA_CONFIGURATION.md)
3. Set up Polygon.io credentials

### Daily Operations
1. [Choose ingestion strategy](guides/data-ingestion-strategies.md) - Initial load, incremental, or backfill
2. [Download data with batch downloader](guides/batch-downloader.md)
3. [Load and query data](guides/data-loader.md)
4. [Generate features](guides/ALPHA158_FEATURES.md)

### Development
1. Review [contributing guidelines](development/contributing.md)
2. Study [architecture decisions](development/architecture-decisions.md)
3. Run [tests](guides/testing.md)

## 📊 Data Pipeline Overview (Medallion Architecture)

```
Landing Layer          Bronze Layer         Silver Layer          Gold Layer
(Raw Sources)         (Validated)          (Enriched)            (ML-Ready)
      ↓                    ↓                    ↓                     ↓
Polygon.io S3      →  Validated Parquet  →  Feature-Enriched  →  Qlib Binary
  CSV.GZ Files         (Schema Check)        (Indicators)         (Backtesting)
      ↓                    ↓                    ↓                     ↓
landing/polygon-s3/   bronze/{data_type}/  silver/{data_type}/  gold/qlib/

Data Quality Progression: Raw → Validated → Enriched → ML-Ready
```

## 🔑 Key Configuration

### Data Root
Configure where data is stored (Medallion Architecture):

```yaml
# config/pipeline_config.yaml
data_root: /Volumes/sandisk/quantmini-lake
```

Or use environment variable:
```bash
export DATA_ROOT=/Volumes/sandisk/quantmini-lake
```

**Directory Structure:**
- `$DATA_ROOT/landing/` - Raw source data (CSV.GZ from S3)
- `$DATA_ROOT/bronze/` - Validated Parquet files
- `$DATA_ROOT/silver/` - Feature-enriched Parquet files
- `$DATA_ROOT/gold/qlib/` - ML-ready binary formats

### Polygon.io Credentials

```yaml
# config/credentials.yaml
polygon:
  s3:
    access_key_id: "YOUR_KEY"
    secret_access_key: "YOUR_SECRET"
```

See [Installation Guide](getting-started/installation.md) for full setup.

## 📈 Project Status

**Latest Update**: 2025-10-18

**Completed Phases**:
- ✅ Phase 1-4: Core Pipeline (S3 Download, Parquet Storage, Query Engine)
- ✅ Phase 5: Feature Engineering
- ✅ Phase 6: Qlib Binary Conversion
- ✅ Phase 7: Query Engine Optimization
- ✅ Phase 8: Incremental Processing

**Data Coverage**:
- **Stocks**: 11,994 symbols
- **Options**: 1,388,382 contracts
- **Date Range**: 2025-08-01 to 2025-09-30
- **Records**: 182M+ total

**Test Coverage**: 138 tests, 100% passing

## 🆘 Getting Help

1. **Check documentation** - Most answers are in the guides above
2. **Review examples** - See `examples/` directory
3. **Check test files** - Tests show usage patterns
4. **Read architecture decisions** - [Architecture Decisions](development/architecture-decisions.md)

## 🔧 External Resources

### Official Documentation
- **Polygon.io**: https://polygon.readthedocs.io/en/latest/
- **Qlib**: https://qlib.readthedocs.io/en/latest/
- **PyArrow**: https://arrow.apache.org/docs/python/
- **Polars**: https://pola-rs.github.io/polars/

### Related Projects
- **Polygon.io Python Client**: https://github.com/polygon-io/client-python
- **Qlib Framework**: https://github.com/microsoft/qlib

## 📝 Documentation Standards

When updating documentation:

1. **Keep it concise** - Be brief but complete
2. **Use examples** - Code examples for every feature
3. **Update cross-references** - Link related docs
4. **Test code samples** - Ensure all code works
5. **Update this index** - Keep navigation current

## 🗂️ Organization

```
docs/
├── README.md                    # This file (documentation hub)
├── getting-started/             # First-time setup
├── architecture/                # System design
├── api-reference/               # External API integration
├── guides/                      # How-to guides
├── reference/                   # Technical reference
├── development/                 # For contributors
└── examples/                    # Sample data
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.
