# Tests Directory

Comprehensive test suite for the QuantMini data pipeline.

## Test Structure

```
tests/
├── unit/              # Unit tests for individual components
│   ├── test_parquet_manager.py
│   ├── test_metadata_manager.py
│   ├── test_streaming_ingestor.py
│   └── ...
└── README.md          # This file
```

## Running Tests

### Run All Tests
```bash
uv run pytest
```

### Run Specific Test File
```bash
uv run pytest tests/unit/test_parquet_manager.py
```

### Run with Coverage
```bash
uv run pytest --cov=src --cov-report=html
```

### Run Specific Test
```bash
uv run pytest tests/unit/test_parquet_manager.py::test_write_batch
```

## Test Categories

### Unit Tests
- **Storage**: ParquetManager, MetadataManager
- **Ingestion**: StreamingIngestor, validation
- **Core**: ConfigLoader, SystemProfiler
- **Download**: S3Downloader, S3Catalog

### Integration Tests
See [E2E_TEST_INSTRUCTIONS.md](../docs/E2E_TEST_INSTRUCTIONS.md) for end-to-end testing.

## Writing Tests

Tests use:
- `pytest` - Test framework
- `pytest-cov` - Coverage reporting
- Fixtures in `conftest.py`
- Temporary directories for isolation

## Continuous Integration

Tests should pass before committing:
```bash
# Quick validation
uv run pytest -v

# Full validation with coverage
uv run pytest --cov=src --cov-report=term-missing
```

Target: 80%+ code coverage
