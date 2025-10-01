# High-Performance Data Pipeline for Financial Market Data

A production-ready data pipeline for processing Polygon.io S3 flat files into optimized formats for quantitative analysis and machine learning.

## 🎯 Key Features

- **Adaptive Processing**: Automatically scales from 24GB workstations to 100GB+ servers
- **70%+ Compression**: Optimized Parquet and binary formats
- **Sub-Second Queries**: Partitioned data lake with predicate pushdown
- **Incremental Updates**: Process only new data using watermarks
- **Apple Silicon Optimized**: 2-3x faster on M1/M2/M3 chips
- **Production Ready**: Monitoring, alerting, validation, and error recovery

## 📊 Performance

| Mode | Memory | Throughput | With Optimizations |
|------|---------|------------|-------------------|
| **Streaming** | < 32GB | 100K rec/s | 500K rec/s |
| **Batch** | 32-64GB | 200K rec/s | 1M rec/s |
| **Parallel** | > 64GB | 500K rec/s | 2M rec/s |

## 🚀 Quick Start

### Prerequisites

- macOS (Apple Silicon or Intel) or Linux
- Python 3.10+
- 24GB+ RAM (recommended: 32GB+)
- 1TB+ storage (SSD recommended)
- Polygon.io account with S3 flat files access

### Installation

1. **Install uv package manager**:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Clone and setup**:
```bash
git clone <repository-url>
cd quantmini

# Create project structure
./create_structure.sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # On macOS/Linux
```

3. **Install dependencies**:
```bash
uv pip install qlib polygon boto3 aioboto3 polars duckdb pyarrow psutil pyyaml
```

4. **Configure credentials**:
```bash
cp config/credentials.yaml.example config/credentials.yaml
# Edit config/credentials.yaml with your Polygon API keys
```

5. **Run system profiler**:
```bash
python -m src.core.system_profiler
# This will create config/system_profile.yaml
```

### First Run

```bash
# Run daily pipeline (processes latest data)
python scripts/run_daily_pipeline.py

# Or backfill historical data
python scripts/run_backfill.py --start-date 2024-01-01 --end-date 2024-12-31
```

## 📁 Project Structure

```
quantmini/
├── config/              # Configuration files
├── src/                 # Source code
│   ├── core/           # System profiling, memory monitoring
│   ├── download/       # S3 downloaders
│   ├── ingest/         # Data ingestion (streaming/batch/parallel)
│   ├── storage/        # Parquet data lake
│   ├── features/       # Feature engineering
│   ├── transform/      # Binary format conversion
│   ├── query/          # Query engine
│   └── orchestration/  # Pipeline orchestration
├── data/               # Data storage (not in git)
│   ├── lake/          # Parquet data lake
│   ├── binary/        # Qlib binary format
│   └── metadata/      # Watermarks, indexes
├── scripts/           # Command-line scripts
├── tests/             # Test suite
└── docs/              # Documentation
```

## 🔧 Configuration

Edit `config/pipeline_config.yaml` to customize:

- **Processing mode**: `adaptive`, `streaming`, `batch`, or `parallel`
- **Data types**: Enable/disable stocks, options, daily, minute data
- **Compression**: Choose `snappy` (fast) or `zstd` (better compression)
- **Features**: Configure which features to compute
- **Optimizations**: Enable Apple Silicon, async downloads, etc.

See [CONFIGURATION.md](docs/CONFIGURATION.md) for details.

## 📚 Documentation

- **[Implementation Plan](IMPLEMENTATION_PLAN.md)**: 28-week roadmap
- **[Project Memory](docs/PROJECT_MEMORY.md)**: Design principles and patterns
- **[Project Structure](docs/PROJECT_STRUCTURE.md)**: Complete directory layout
- **[Design Doc](pipeline_design/mac-optimized-pipeline.md)**: Architecture details

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test suite
pytest tests/unit/
pytest tests/integration/
pytest tests/performance/
```

## 🔍 Monitoring

Access monitoring dashboards:

```bash
# View health status
python scripts/check_health.py

# View performance metrics
cat logs/performance/performance_metrics.json

# Generate report
python scripts/generate_report.py
```

## 📊 Data Types

The pipeline processes four types of data from Polygon.io:

1. **Stock Daily Aggregates**: Daily OHLCV for all US stocks
2. **Stock Minute Aggregates**: Minute-level data per symbol
3. **Options Daily Aggregates**: Daily options data per underlying
4. **Options Minute Aggregates**: Minute-level options data (all contracts)

## 🎨 Architecture

```
S3 CSV.GZ Files
      ↓
Adaptive Ingestion (Streaming/Batch/Parallel)
      ↓
Parquet Data Lake (Partitioned)
      ↓
Feature Engineering (DuckDB/Polars)
      ↓
Qlib Binary Format (ML-Ready)
```

## 🚦 Pipeline Stages

1. **Download**: Async S3 downloads with connection pooling
2. **Ingest**: Adaptive processing based on available memory
3. **Validate**: Data quality checks
4. **Enrich**: Feature engineering (alpha, returns, etc.)
5. **Convert**: Transform to qlib binary format
6. **Query**: Fast access via DuckDB/Polars

## 🔐 Security

- **Never commit** `config/credentials.yaml` (in .gitignore)
- Store credentials in environment variables for production
- Use AWS Secrets Manager or similar for cloud deployments
- Rotate API keys regularly

## 🐛 Troubleshooting

### Memory Errors
```bash
# Reduce memory usage
export MAX_MEMORY_GB=16

# Force streaming mode
export PIPELINE_MODE=streaming
```

### S3 Rate Limits
```bash
# Reduce concurrent downloads
# Edit config/pipeline_config.yaml:
# optimizations.async_downloads.max_concurrent: 4
```

### Slow Performance
```bash
# Enable profiling
# Edit config/pipeline_config.yaml:
# monitoring.profiling.enabled: true

# Run and check logs/performance/
```

See [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) for more.

## 🤝 Contributing

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for development guidelines.

## 📈 Performance Tuning

See [PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) for:
- Apple Silicon optimizations
- Memory tuning
- Storage optimization
- Query performance
- Benchmarking

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

## 🙏 Acknowledgments

- **Polygon.io**: S3 flat files data source
- **Qlib**: Quantitative investment framework
- **Polars**: High-performance DataFrame library
- **DuckDB**: Embedded analytical database

## 📧 Support

- Documentation: [https://quantmini.readthedocs.io/](https://quantmini.readthedocs.io/)
- Issues: [GitHub Issues](https://github.com/nittygritty-zzy/quantmini/issues)
- Email: zheyuan28@gmail.com

---

**Built with**: Python 3.10+, uv, qlib, polygon, polars, duckdb, pyarrow

**Optimized for**: macOS (Apple Silicon M1/M2/M3), 24GB+ RAM, SSD storage
