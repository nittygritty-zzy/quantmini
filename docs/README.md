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
- **[Configuration Guide](getting-started/configuration.md)** - Configure data sources and storage

### Architecture
Understand how the system works:

- **[System Overview](architecture/overview.md)** - High-level architecture and components
- **[Data Pipeline](architecture/data-pipeline.md)** - Pipeline design and optimization
- **[Advanced Features](architecture/advanced-features.md)** - Phase 5-8 implementation details

### API Reference
Integration with external services:

- **[Polygon.io Integration](api-reference/polygon.md)** - S3 flat files data access
  - **Official Docs**: https://polygon.readthedocs.io/en/latest/Library-Interface-Documentation.html
- **[Qlib Integration](api-reference/qlib.md)** - Quantitative research framework
  - **Official Docs**: https://qlib.readthedocs.io/en/latest/reference/api.html
- **[Polygon S3 Flat Files](api-reference/polygon-s3-flatfiles.md)** - S3 data structure details

### User Guides
Step-by-step guides for common tasks:

- **[Data Ingestion](guides/data-ingestion.md)** - Download and process market data
- **[Feature Engineering](guides/feature-engineering.md)** - Create derived features
- **[Testing](guides/testing.md)** - Run tests and validate data
- **[Deployment](guides/deployment.md)** - Deploy to production

### Reference
Technical reference materials:

- **[Data Schemas](reference/data-schemas.md)** - Complete schema documentation
- **[Configuration Reference](reference/configuration.md)** - All config options
- **[CLI Tools](reference/cli-tools.md)** - Command-line interface reference

### Development
For contributors and developers:

- **[Contributing Guide](development/contributing.md)** - How to contribute
- **[Architecture Decisions](development/architecture-decisions.md)** - Design notes and ADRs
- **[Changelog](development/changelog.md)** - Project history

### Examples
Practical examples and sample data:

- **[Sample Data](examples/sample-data/)** - Example CSV files
- **[Code Examples](../examples/)** - Usage examples and notebooks

## 🎯 Common Tasks

### First Time Setup
1. [Install and configure](getting-started/installation.md) the project
2. [Configure Polygon.io credentials](api-reference/polygon.md#authentication)
3. [Download sample data](guides/testing.md)

### Daily Operations
1. [Ingest new data](guides/data-ingestion.md)
2. [Enrich features](guides/feature-engineering.md)
3. [Convert to Qlib format](api-reference/qlib.md#converting-data-to-qlib-format)

### Development
1. Review [contributing guidelines](development/contributing.md)
2. Study [architecture decisions](development/architecture-decisions.md)
3. Run [tests](guides/testing.md)

## 📊 Data Pipeline Overview

```
Polygon.io S3 Flat Files
         ↓
   Download (Async)
         ↓
  Parquet Storage (Partitioned)
         ↓
  Feature Engineering
         ↓
   Enriched Parquet
         ↓
 Qlib Binary Format
         ↓
   Research & Backtesting
```

## 🔑 Key Configuration

### Data Root
Configure where data is stored:

```yaml
# config/pipeline_config.yaml
data_root: /Volumes/sandisk/quantmini-data
```

Or use environment variable:
```bash
export DATA_ROOT=/Volumes/sandisk/quantmini-data
```

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

**Latest Update**: 2025-09-30

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
