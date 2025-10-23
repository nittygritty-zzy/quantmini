# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QuantMini is a high-performance data pipeline for processing Polygon.io financial market data into optimized formats for quantitative analysis and machine learning. It implements a **Medallion Architecture** (Landing → Bronze → Silver → Gold) with adaptive processing modes that scale from 24GB workstations to 100GB+ servers.

## Development Setup

### Installation
```bash
# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate virtual environment
uv venv
source .venv/bin/activate  # macOS/Linux

# Install dependencies
uv pip install -e .

# Install dev dependencies
uv pip install -e ".[dev]"
```

### Configuration
```bash
# Copy credentials template
cp config/credentials.yaml.example config/credentials.yaml
# Edit with your Polygon.io API keys

# Generate system profile (detects hardware capabilities)
python -m src.core.system_profiler
```

## Common Commands

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/unit/test_parquet_manager.py

# Run specific test
uv run pytest tests/unit/test_parquet_manager.py::test_write_batch

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Integration tests require credentials
uv run pytest tests/integration/ -v
```

### CLI Commands
The project uses a Click-based CLI (`quantmini` command) with the following groups:

```bash
# Config operations
quantmini config init          # Initialize configuration
quantmini config show          # Show current config
quantmini config profile       # Show system profile

# Data pipeline (Medallion layers)
quantmini data download        # Landing: Download from S3
quantmini data ingest          # Bronze: Validate & convert to Parquet
quantmini data enrich          # Silver: Add features/indicators
quantmini data convert         # Gold: Convert to Qlib binary

# Pipeline orchestration
quantmini pipeline daily --data-type stocks_daily
quantmini pipeline run --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31

# Query data
quantmini data query --data-type stocks_daily --symbols AAPL MSFT --start-date 2024-01-01
```

## Architecture

### Medallion Data Flow
```
Landing Layer (data/landing/)
  └─> Raw CSV.GZ from Polygon S3
       ↓
Bronze Layer (data/bronze/)
  └─> Validated Parquet with schema enforcement
       ↓
Silver Layer (data/silver/)
  └─> Feature-enriched Parquet with indicators
       ↓
Gold Layer (data/gold/)
  └─> ML-ready formats (Qlib binary, DuckDB)
```

### Module Structure

**src/core/** - System foundation
- `config_loader.py`: Hierarchical configuration (env vars → user config → system profile → defaults)
- `system_profiler.py`: Auto-detect hardware and recommend processing mode
- `memory_monitor.py`: Track memory usage and trigger GC
- `exceptions.py`: Custom exception hierarchy

**src/download/** - Data acquisition
- `async_downloader.py`: Async S3 downloads with connection pooling
- `s3_catalog.py`: S3 file catalog management
- `polygon_rest_client.py`: Polygon REST API client
- Specialized downloaders: `bars.py`, `news.py`, `fundamentals.py`, etc.

**src/ingest/** - Landing → Bronze transformation
- `base_ingestor.py`: Abstract base with CSV parsing, dtype optimization
- `polars_ingestor.py`: Fast ingestion (5-10x faster, recommended)
- `streaming_ingestor.py`: Memory-efficient for <32GB RAM systems
- Pattern: Reads CSV.GZ, validates schema, writes partitioned Parquet

**src/features/** - Bronze → Silver transformation
- `feature_engineer.py`: DuckDB-based feature computation
- `definitions.py`: Feature SQL generation (alpha factors, returns, indicators)
- `financial_ratios.py`: Fundamental analysis features
- Pattern: Reads validated Parquet, computes features, writes enriched Parquet

**src/transform/** - Silver → Gold transformation
- `qlib_binary_writer.py`: Convert to Qlib binary format for backtesting
- `qlib_binary_validator.py`: Validate binary format integrity

**src/storage/** - Data persistence layer
- `parquet_manager.py`: Partitioned Parquet I/O with schema enforcement
- `metadata_manager.py`: Watermarks for incremental processing
- `schemas.py`: PyArrow schema definitions for all data types

**src/orchestration/** - Pipeline coordination
- `ingestion_orchestrator.py`: End-to-end pipeline orchestration (download → ingest → enrich → convert)

**src/query/** - Data access layer
- `query_engine.py`: DuckDB/Polars query engine with predicate pushdown
- `query_cache.py`: LRU cache for repeated queries

**src/cli/** - Command-line interface
- `main.py`: CLI entry point with Click groups
- `commands/`: Subcommands (data, pipeline, config, validate, schema, api, polygon)

### Processing Modes

The system auto-selects mode based on available memory (see `SystemProfiler`):
- **Streaming** (<32GB): Process one date at a time, minimal memory
- **Batch** (32-64GB): Process multiple dates with moderate memory
- **Parallel** (>64GB): Parallel processing with maximum memory

Override with `config/pipeline_config.yaml` or env var `PIPELINE_MODE`.

## Key Design Patterns

### 1. Adaptive Processing
Components detect available memory and adapt processing strategy:
```python
from src.core.system_profiler import SystemProfiler
profiler = SystemProfiler()
mode = profiler.profile['recommended_mode']  # 'streaming', 'batch', or 'parallel'
```

### 2. Incremental Updates
All stages support incremental processing via watermarks:
```python
from src.storage.metadata_manager import MetadataManager
metadata = MetadataManager(metadata_root)
last_date = metadata.get_watermark(data_type='stocks_daily', stage='bronze')
```

### 3. Configuration Hierarchy
Settings resolved in priority order:
1. Environment variables (e.g., `DATA_ROOT`, `PIPELINE_MODE`)
2. User config (`config/pipeline_config.yaml`)
3. System profile (`config/system_profile.yaml`)
4. Default values (`ConfigLoader.DEFAULT_CONFIG`)

### 4. Schema Enforcement
All Parquet writes enforce strict schemas from `src/storage/schemas.py`:
```python
from src.storage.schemas import get_schema
schema = get_schema('stocks_daily')  # Returns PyArrow schema
```

**IMPORTANT**: `parquet.use_dictionary` must be `false` in config to prevent `dictionary<string>` vs `string` schema conflicts across ingestion runs.

### 5. Partitioning Strategy
- **Daily data**: Partitioned by `year/month/day`
- **Minute data**: Partitioned by `year/month/day/symbol` (one file per symbol per day)
- **Options**: Partitioned by `year/month/day/underlying` (grouped by underlying asset)

## Data Types

Four primary data types from Polygon.io:
1. **stocks_daily**: Daily OHLCV for all US stocks
2. **stocks_minute**: Minute-level data per symbol
3. **options_daily**: Daily options aggregates per underlying
4. **options_minute**: Minute-level options (all contracts)

Each flows through all Medallion layers with type-specific schemas and features.

## Testing Strategy

- **Unit tests** (`tests/unit/`): Mock all external dependencies (S3, Polygon API)
- **Integration tests** (`tests/integration/`): Require real credentials, test end-to-end flows
- **Performance tests** (`tests/performance/`): Benchmark processing modes
- **Fixtures** (`tests/fixtures/`): Shared test data and configurations

Target: 80%+ code coverage

## Important Implementation Notes

### Parquet Schema Consistency
**Always use `use_dictionary=false`** when writing Parquet to avoid schema drift between runs. Dictionary encoding can cause `dictionary<values=string>` vs `string` type mismatches that break partitioned datasets.

### Memory Management
- Use `AdvancedMemoryMonitor` for tracking: `from src.core.memory_monitor import AdvancedMemoryMonitor`
- GC frequency controlled by `processing.gc_frequency` in config
- Memory threshold triggers early GC at `processing.memory_threshold_percent`

### Apple Silicon Optimizations
- Auto-enabled for M1/M2/M3 chips
- Uses Accelerate framework for vectorized operations
- Configure in `optimizations.apple_silicon` section

### S3 Flat Files Structure
Polygon S3 bucket follows pattern:
```
flatfiles/us_stocks_sip/day_aggs_v1/{year}/{month}/YYYY-MM-DD.csv.gz
flatfiles/us_stocks_sip/minute_aggs_v1/{year}/{month}/{symbol}/YYYY-MM-DD.csv.gz
```

### Async Download Best Practices
- Max concurrent connections: `optimizations.async_downloads.max_concurrent` (default: 8)
- Connection pool size: `optimizations.async_downloads.connection_pool_size` (default: 50)
- Use `AsyncS3Downloader` for parallel downloads, sequential ingestion

## Credentials and Security

- **Never commit** `config/credentials.yaml` (git-ignored)
- For production, use environment variables or AWS Secrets Manager
- Required credentials in `credentials.yaml.example`:
  - Polygon S3: `access_key_id`, `secret_access_key`
  - Polygon API: `key` (optional, for metadata)

## CI/CD

GitHub Actions workflows in `.github/workflows/`:
- `test.yml`: Run pytest on push/PR
- `quality.yml`: Linting and code quality checks
- `publish.yml`: PyPI package publishing

## Common Development Tasks

### Adding a New Data Type
1. Define schema in `src/storage/schemas.py`
2. Add column mappings in `BaseIngestor.COLUMN_MAPPINGS`
3. Create feature definitions in `src/features/definitions*.py`
4. Update `config/pipeline_config.yaml` data types list
5. Add CLI commands in `src/cli/commands/`

### Adding New Features
1. Add SQL logic in `src/features/definitions.py`
2. Update feature list in config: `features.{data_type}`
3. Test with `quantmini data enrich --data-type {type}`

### Debugging Performance
```bash
# Enable profiling in config
quantmini config set monitoring.profiling.enabled true

# Run pipeline and check logs
quantmini pipeline daily --data-type stocks_daily
cat logs/performance/performance_metrics.json
```

## Common Issues

### ImportError with Qlib
Qlib has strict dependencies. Install in isolated environment:
```bash
uv pip install pyqlib==0.9.0
```

### Schema Validation Errors
Check Parquet schema consistency:
```python
import pyarrow.parquet as pq
metadata = pq.read_metadata('data/bronze/stocks_daily/year=2024/month=01/day=01/part.parquet')
print(metadata.schema)
```

### Memory Errors
Force streaming mode:
```bash
export PIPELINE_MODE=streaming
quantmini pipeline daily --data-type stocks_daily
```

## Version Information

- Current version: 0.2.0 (see `pyproject.toml`)
- Python: 3.10+
- Key dependencies: polars, duckdb, pyarrow, qlib, boto3, aioboto3
- Always check config/paths.yaml for dataset root folder