# Contributing to QuantMini

Thank you for your interest in contributing to QuantMini! This document provides guidelines for contributing to the high-performance Medallion Architecture data pipeline.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/quantmini.git
   cd quantmini
   ```
3. **Install with uv**:
   ```bash
   uv sync
   source .venv/bin/activate
   ```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 2. Make Changes

- Write clean, readable code
- Follow PEP 8 style guidelines
- Add docstrings to functions and classes
- Include type hints where appropriate
- Keep Medallion Architecture principles in mind

### 3. Test Your Changes

```bash
# Run all tests
pytest tests/ -v

# Run specific test module
pytest tests/unit/test_polygon_rest_client.py -v

# Run with coverage
pytest --cov=src tests/
```

### 4. Commit Your Changes

```bash
git add .
git commit -m "Add feature: brief description"
```

Use conventional commit messages:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/changes
- `refactor:` for code refactoring
- `chore:` for maintenance tasks

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Code Style

### Python Style

- Follow PEP 8
- Use type hints (Python 3.10+ syntax)
- Maximum line length: 100 characters
- Use meaningful variable names
- Prefer async/await for I/O operations

### Example

```python
async def download_ticker_data(
    ticker: str,
    start_date: str,
    end_date: str,
    output_dir: Path
) -> pl.DataFrame:
    """
    Download ticker data from Polygon REST API.

    Args:
        ticker: Ticker symbol (e.g., 'AAPL')
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory for Parquet files

    Returns:
        DataFrame with ticker data

    Example:
        >>> df = await download_ticker_data('AAPL', '2024-01-01', '2024-12-31', Path('bronze/'))
    """
    # Implementation here
    pass
```

### Documentation

- Add docstrings to all public functions
- Use Google-style docstrings
- Include examples in docstrings when helpful
- Update markdown docs when adding features

## Testing

### Writing Tests

- Place tests in `tests/` directory
- Mirror the source structure
- Use descriptive test names
- Test edge cases
- Use pytest fixtures for common setup

### Example Test

```python
import pytest
import polars as pl
from src.download.polygon_rest_client import PolygonRESTClient

@pytest.mark.asyncio
async def test_polygon_rest_client_basic_request():
    """Test basic REST API request."""
    async with PolygonRESTClient(api_key="test_key") as client:
        # Mock the request
        response = await client.get("/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-31")
        assert "results" in response
```

## Architecture Guidelines

### Medallion Architecture Layers

When contributing, maintain the layer separation:

1. **Landing Layer**: Raw source data only (no transformations)
2. **Bronze Layer**: Validated Parquet with schema checks
3. **Silver Layer**: Feature-enriched with technical indicators
4. **Gold Layer**: ML-ready binary formats (Qlib)

### Data Partitioning

Use date-first partitioning for time-series data:

```
bronze/news/
├── year=2024/
│   ├── month=01/
│   │   ├── ticker=AAPL.parquet
│   │   └── ticker=MSFT.parquet
│   └── month=02/
└── year=2025/
```

### Async Best Practices

- Use `async/await` for I/O operations
- Implement proper error handling
- Use context managers for resources
- Batch requests when possible

## Documentation

### Adding Documentation

1. Update relevant `.md` files in `docs/`
2. Update `docs_source/` for Sphinx documentation
3. Add examples when introducing new features
4. Update API reference if adding new modules

### Building Documentation Locally

```bash
cd docs_source
pip install sphinx sphinx_rtd_theme myst-parser
make html
```

View at `docs_source/_build/html/index.html`

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass (`pytest tests/ -v`)
- [ ] Documentation updated
- [ ] Code follows style guidelines
- [ ] Commit messages are clear
- [ ] No unnecessary files included
- [ ] Medallion Architecture preserved

### Pull Request Description

Include:
- What changes you made
- Why you made them
- Any relevant issue numbers
- Testing done
- Performance impact (if applicable)

### Example PR Description

```markdown
## Description
Add optimized batch downloader for ticker events

## Changes
- Created OptimizedTickerEventsDownloader class
- Uses batch_request() for parallel API calls
- Saves incrementally to avoid data loss
- Added comprehensive tests

## Testing
- All existing tests pass
- Added 8 new tests for batch downloader
- Tested with 11,464 tickers (2-5 minute completion)

## Performance
- 10x faster than individual requests
- Reduced API calls by 80% through batching

Fixes #123
```

## Reporting Issues

### Bug Reports

Include:
- Python version
- QuantMini version/commit
- Operating system
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages/stack traces
- Data sample (if applicable)

### Feature Requests

Include:
- Clear description of the feature
- Use cases
- Why it would be valuable
- How it fits Medallion Architecture
- Any implementation ideas

## Code Review Process

1. Maintainers will review your PR
2. Address any feedback
3. Tests must pass
4. Once approved, it will be merged
5. Your contribution will be credited

## Project Structure

When adding new code, follow the structure:

```
src/
├── core/              # Core infrastructure
├── download/          # Polygon REST API downloaders
├── features/          # Feature engineering
├── transform/         # Data transformations
└── utils/             # Utilities and helpers

scripts/
├── download/          # Download scripts
├── features/          # Feature generation scripts
└── qlib/              # Qlib conversion scripts

tests/
├── unit/              # Unit tests
└── integration/       # Integration tests
```

## Questions?

- Open an issue for questions
- Check existing issues first
- Be respectful and constructive
- Reference relevant documentation

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing to QuantMini! 🚀
