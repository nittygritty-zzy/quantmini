# Project Structure

**Project**: High-Performance Data Pipeline for Financial Market Data
**Based on**: `pipeline_design/mac-optimized-pipeline.md` v3.0

---

## Complete Directory Structure

```
quantmini/
│
├── README.md                          # Project overview and quick start
├── IMPLEMENTATION_PLAN.md             # 28-week implementation roadmap
├── PROJECT_MEMORY.md                  # Design principles and patterns
├── PROJECT_STRUCTURE.md               # This file
├── .gitignore                         # Git ignore patterns
├── pyproject.toml                     # Project metadata and dependencies (uv)
├── uv.lock                            # Locked dependencies (uv)
│
├── pipeline_design/                   # Design documents
│   ├── mac-optimized-pipeline.md      # Main architecture document
│   ├── ploygon_s3_flatfiles_schemas.md
│   ├── polygon_s3_flatfiles_intro.md
│   └── sample_files/                  # Sample data for testing
│       ├── stocks_day_candlesticks_example.csv
│       ├── stocks_minute_candlesticks_example.csv
│       ├── options_day_candlesticks_example.csv
│       └── options_minute_candlesticks_example.csv
│
├── config/                            # Configuration files
│   ├── system_profile.yaml            # Auto-generated hardware profile
│   ├── pipeline_config.yaml           # Pipeline settings
│   ├── credentials.yaml.example       # Credentials template (tracked)
│   └── credentials.yaml               # Actual credentials (in .gitignore)
│
├── src/                               # Source code
│   ├── __init__.py
│   │
│   ├── core/                          # Core infrastructure
│   │   ├── __init__.py
│   │   ├── system_profiler.py         # SystemProfiler class
│   │   ├── memory_monitor.py          # AdvancedMemoryMonitor class
│   │   ├── config_loader.py           # Configuration management
│   │   └── exceptions.py              # Custom exceptions
│   │
│   ├── download/                      # S3 download layer
│   │   ├── __init__.py
│   │   ├── sync_downloader.py         # Boto3 synchronous downloader
│   │   ├── async_downloader.py        # Aioboto3 async downloader
│   │   ├── s3_catalog.py              # S3 file path management
│   │   └── download_queue.py          # Download queue manager
│   │
│   ├── ingest/                        # Data ingestion
│   │   ├── __init__.py
│   │   ├── base_ingestor.py           # BaseIngestor abstract class
│   │   ├── streaming_ingestor.py      # Streaming mode (<32GB RAM)
│   │   ├── batch_ingestor.py          # Batch mode (32-64GB RAM)
│   │   ├── parallel_ingestor.py       # Parallel mode (>64GB RAM)
│   │   ├── polars_ingestor.py         # Polars-based ingestor (optimized)
│   │   └── adaptive_ingestor.py       # Adaptive mode selector
│   │
│   ├── storage/                       # Storage layer
│   │   ├── __init__.py
│   │   ├── schemas.py                 # PyArrow schema definitions
│   │   ├── partitioning.py            # Partition management
│   │   ├── parquet_writer.py          # Optimized Parquet writer
│   │   └── metadata.py                # Metadata and watermark management
│   │
│   ├── features/                      # Feature engineering
│   │   ├── __init__.py
│   │   ├── definitions.py             # Feature definitions
│   │   ├── duckdb_engineer.py         # DuckDB-based feature engine
│   │   └── polars_engineer.py         # Polars-based feature engine
│   │
│   ├── transform/                     # Data transformation
│   │   ├── __init__.py
│   │   ├── binary_writer.py           # Qlib binary format writer
│   │   └── binary_validator.py        # Binary format validation
│   │
│   ├── query/                         # Query engine
│   │   ├── __init__.py
│   │   ├── query_engine.py            # QueryEngine class
│   │   ├── cache.py                   # QueryCache with LRU
│   │   └── optimizer.py               # Query optimization
│   │
│   ├── orchestration/                 # Pipeline orchestration
│   │   ├── __init__.py
│   │   ├── daily_pipeline.py          # DailyPipeline orchestrator
│   │   ├── backfill_pipeline.py       # BackfillPipeline for history
│   │   ├── incremental.py             # IncrementalProcessor
│   │   ├── watermarks.py              # Watermark management
│   │   └── resource_manager.py        # Resource allocation
│   │
│   ├── maintenance/                   # Maintenance operations
│   │   ├── __init__.py
│   │   ├── validator.py               # DataValidator class
│   │   ├── compactor.py               # PartitionCompactor
│   │   └── archiver.py                # DataArchiver for old data
│   │
│   ├── optimizations/                 # Platform optimizations
│   │   ├── __init__.py
│   │   ├── apple_silicon.py           # AppleSiliconOptimizer
│   │   └── macos_io.py                # MacOSFileOptimizer
│   │
│   ├── monitoring/                    # Monitoring and alerting
│   │   ├── __init__.py
│   │   ├── health_monitor.py          # PipelineMonitor
│   │   ├── alerting.py                # Alert system
│   │   └── profiler.py                # PerformanceProfiler
│   │
│   └── utils/                         # Utility functions
│       ├── __init__.py
│       ├── datetime_utils.py          # Date/time helpers
│       ├── file_utils.py              # File operations
│       └── logging_utils.py           # Logging configuration
│
├── scripts/                           # Command-line scripts
│   ├── setup_environment.sh           # Initial environment setup
│   ├── run_daily_pipeline.py          # Daily pipeline runner
│   ├── run_backfill.py                # Historical data backfill
│   ├── validate_data.py               # Data validation runner
│   ├── compact_partitions.py          # Partition compaction
│   └── generate_report.py             # Performance reporting
│
├── data/                              # Data storage (add to .gitignore)
│   ├── lake/                          # Parquet data lake
│   │   ├── stocks/
│   │   │   ├── daily/
│   │   │   │   ├── raw/               # Partitioned by year/month
│   │   │   │   │   ├── year=2024/
│   │   │   │   │   │   ├── month=01/
│   │   │   │   │   │   │   └── part-0.parquet
│   │   │   │   │   │   └── month=02/
│   │   │   │   │   └── year=2025/
│   │   │   │   │       └── month=09/
│   │   │   │   └── enriched/          # With features
│   │   │   │       └── [same structure]
│   │   │   └── minute/
│   │   │       ├── raw/               # Partitioned by symbol/year/month
│   │   │       │   ├── symbol=AAPL/
│   │   │       │   └── symbol=TSLA/
│   │   │       └── enriched/
│   │   │
│   │   └── options/
│   │       ├── daily/
│   │       │   ├── raw/               # Partitioned by underlying/year/month
│   │       │   └── enriched/
│   │       └── minute/
│   │           ├── raw/               # Partitioned by underlying/date
│   │           └── enriched/
│   │
│   ├── binary/                        # ML-optimized binary format
│   │   ├── stocks/
│   │   │   ├── daily/
│   │   │   │   ├── features/          # Organized by symbol
│   │   │   │   │   ├── aapl/
│   │   │   │   │   │   ├── open.day.bin
│   │   │   │   │   │   ├── high.day.bin
│   │   │   │   │   │   ├── close.day.bin
│   │   │   │   │   │   └── alpha_daily.day.bin
│   │   │   │   │   └── tsla/
│   │   │   │   ├── instruments/
│   │   │   │   │   └── all.txt
│   │   │   │   └── calendars/
│   │   │   │       └── day.txt
│   │   │   └── minute/
│   │   │       └── [similar structure with .1min.bin]
│   │   │
│   │   └── options/
│   │       └── [similar structure]
│   │
│   ├── metadata/                      # Fast lookup indexes
│   │   ├── stocks/
│   │   │   ├── symbols.parquet
│   │   │   ├── daily_stats.parquet
│   │   │   └── watermarks.json
│   │   └── options/
│   │       ├── contracts.parquet
│   │       ├── chains.parquet
│   │       └── watermarks.json
│   │
│   ├── cache/                         # Query result cache
│   │   ├── queries/                   # LRU cache
│   │   └── aggregations/              # Pre-computed aggregations
│   │
│   ├── temp/                          # Temporary storage
│   │   └── chunks/                    # Streaming buffers
│   │
│   └── archive/                       # Cold storage
│       ├── expired_options/
│       └── historical/
│
├── logs/                              # Application logs (add to .gitignore)
│   ├── pipeline/                      # Pipeline execution logs
│   │   └── daily_YYYY-MM-DD.log
│   ├── errors/                        # Error logs
│   │   └── errors_YYYY-MM-DD.log
│   ├── performance/                   # Performance profiles
│   │   ├── download_s3_files_TIMESTAMP.prof
│   │   └── performance_metrics.json
│   └── monitoring/                    # Health monitoring logs
│       └── health_YYYY-MM-DD.log
│
├── tests/                             # Test suite
│   ├── __init__.py
│   ├── conftest.py                    # Pytest configuration
│   │
│   ├── unit/                          # Unit tests
│   │   ├── test_system_profiler.py
│   │   ├── test_memory_monitor.py
│   │   ├── test_downloaders.py
│   │   ├── test_ingestors.py
│   │   ├── test_storage.py
│   │   ├── test_features.py
│   │   ├── test_query.py
│   │   └── test_orchestration.py
│   │
│   ├── integration/                   # Integration tests
│   │   ├── test_end_to_end.py
│   │   ├── test_daily_pipeline.py
│   │   └── test_backfill.py
│   │
│   ├── performance/                   # Performance benchmarks
│   │   ├── benchmark_ingestors.py
│   │   ├── benchmark_queries.py
│   │   └── benchmark_results.json
│   │
│   └── fixtures/                      # Test data
│       ├── sample_stocks_daily.csv
│       ├── sample_stocks_minute.csv
│       └── mock_config.yaml
│
├── docs/                              # Documentation
│   ├── API.md                         # API documentation
│   ├── USER_GUIDE.md                  # User guide
│   ├── CONFIGURATION.md               # Configuration guide
│   ├── DEPLOYMENT.md                  # Deployment instructions
│   ├── TROUBLESHOOTING.md             # Common issues and solutions
│   ├── PERFORMANCE_TUNING.md          # Performance optimization guide
│   └── CONTRIBUTING.md                # Contribution guidelines
│
└── notebooks/                         # Jupyter notebooks (optional)
    ├── exploration/                   # Data exploration
    ├── analysis/                      # Analysis notebooks
    └── benchmarking/                  # Performance benchmarking
```

---

## Key Files

### Configuration Files

| File | Purpose | Tracked in Git |
|------|---------|----------------|
| `config/system_profile.yaml` | Auto-generated hardware profile | No |
| `config/pipeline_config.yaml` | Pipeline settings | Yes |
| `config/credentials.yaml` | Polygon API credentials | No |
| `config/credentials.yaml.example` | Credentials template | Yes |

### Core Source Files

| File | Purpose |
|------|---------|
| `src/core/system_profiler.py` | Detect hardware, recommend mode |
| `src/core/memory_monitor.py` | Memory pressure management |
| `src/download/async_downloader.py` | Async S3 downloads |
| `src/ingest/polars_ingestor.py` | High-performance ingestion |
| `src/storage/parquet_writer.py` | Optimized Parquet writing |
| `src/features/polars_engineer.py` | Feature engineering |
| `src/orchestration/daily_pipeline.py` | Daily automation |

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/setup_environment.sh` | Initial setup |
| `scripts/run_daily_pipeline.py` | Run daily updates |
| `scripts/run_backfill.py` | Historical data backfill |
| `scripts/validate_data.py` | Data quality validation |

---

## .gitignore Template

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv/
*.egg-info/
dist/
build/

# UV
.uv/
uv.lock

# Data directories (IMPORTANT!)
data/
!data/.gitkeep

# Logs
logs/
*.log

# Configuration with secrets
config/credentials.yaml
config/system_profile.yaml

# Jupyter
.ipynb_checkpoints/
*.ipynb

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# macOS
.DS_Store
.AppleDouble
.LSOverride

# Performance profiles
*.prof
*.pstats

# Temporary files
temp/
tmp/
*.tmp
*.bak

# Test coverage
.coverage
htmlcov/
.pytest_cache/
```

---

## Directory Creation Script

Run this to create the complete structure:

```bash
#!/bin/bash
# create_structure.sh

# Root directories
mkdir -p config
mkdir -p pipeline_design/sample_files

# Source code
mkdir -p src/{core,download,ingest,storage,features,transform,query,orchestration,maintenance,optimizations,monitoring,utils}

# Data directories (with .gitkeep to track empty dirs)
mkdir -p data/{lake/stocks/{daily/{raw,enriched},minute/{raw,enriched}},lake/options/{daily/{raw,enriched},minute/{raw,enriched}}}
mkdir -p data/binary/stocks/{daily/{features,instruments,calendars},minute/{features,instruments,calendars}}
mkdir -p data/binary/options/{daily/{features,instruments,calendars},minute/{features,instruments,calendars}}
mkdir -p data/{metadata/stocks,metadata/options,cache/{queries,aggregations},temp/chunks,archive/{expired_options,historical}}

# Logs
mkdir -p logs/{pipeline,errors,performance,monitoring}

# Tests
mkdir -p tests/{unit,integration,performance,fixtures}

# Scripts
mkdir -p scripts

# Docs
mkdir -p docs

# Notebooks (optional)
mkdir -p notebooks/{exploration,analysis,benchmarking}

# Create __init__.py files
find src tests -type d -exec touch {}/__init__.py \;

# Create .gitkeep for empty directories
find data -type d -empty -exec touch {}/.gitkeep \;
find logs -type d -empty -exec touch {}/.gitkeep \;

echo "Project structure created successfully!"
```

---

## Initial Setup Checklist

After creating the structure, set up the project:

- [ ] Run `create_structure.sh` to create directories
- [ ] Copy `config/credentials.yaml.example` → `config/credentials.yaml`
- [ ] Edit `config/credentials.yaml` with Polygon API keys
- [ ] Create `.gitignore` from template above
- [ ] Install `uv`: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- [ ] Initialize uv project: `uv init`
- [ ] Install dependencies: `uv pip install qlib polygon boto3 aioboto3 polars duckdb pyarrow psutil pyyaml`
- [ ] Run system profiler: `python -m src.core.system_profiler`
- [ ] Verify S3 access: `python scripts/test_s3_access.py` (to be created)
- [ ] Review `PROJECT_MEMORY.md` for design principles
- [ ] Review `IMPLEMENTATION_PLAN.md` for roadmap

---

## Environment Variables

Create `.env` file (add to .gitignore):

```bash
# Polygon API
POLYGON_API_KEY=your_api_key_here
POLYGON_S3_KEY=your_s3_access_key
POLYGON_S3_SECRET=your_s3_secret_key

# Pipeline settings
PIPELINE_MODE=adaptive  # auto, streaming, batch, parallel
LOG_LEVEL=INFO
MAX_MEMORY_GB=20

# Paths (override in .env or environment)
DATA_ROOT=/Volumes/sandisk/quantmini-data
CONFIG_ROOT=./config
LOG_ROOT=./logs
```

---

## Dependencies (pyproject.toml)

```toml
[project]
name = "quantmini-pipeline"
version = "0.1.0"
description = "High-performance financial data pipeline"
requires-python = ">=3.10"

dependencies = [
    "qlib>=0.9.0",
    "polygon-api-client>=1.13.0",
    "boto3>=1.34.0",
    "aioboto3>=12.0.0",
    "polars>=0.20.0",
    "pandas>=2.0.0",
    "pyarrow>=15.0.0",
    "duckdb>=0.10.0",
    "psutil>=5.9.0",
    "pyyaml>=6.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-cov>=4.1.0",
    "pytest-asyncio>=0.23.0",
    "black>=24.0.0",
    "ruff>=0.2.0",
    "mypy>=1.8.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]

[tool.black]
line-length = 100
target-version = ['py310']

[tool.ruff]
line-length = 100
select = ["E", "F", "I", "N", "W"]
```

---

## Next Steps

1. **Create the structure**: Run `create_structure.sh`
2. **Set up environment**: Install uv and dependencies
3. **Configure credentials**: Copy and edit `credentials.yaml`
4. **Start Phase 1**: Implement `src/core/system_profiler.py` (see IMPLEMENTATION_PLAN.md)
5. **Write tests**: Create unit tests as you implement
6. **Document as you go**: Update docs/ with API details

Follow the 28-week plan in `IMPLEMENTATION_PLAN.md` for detailed implementation steps.
