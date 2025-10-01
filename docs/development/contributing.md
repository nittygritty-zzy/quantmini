# Contributing to QuantMini

Thank you for your interest in contributing to QuantMini!

## Development Setup

1. Fork and clone the repository
2. Install dependencies:
   ```bash
   uv venv
   source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
   uv pip install -e ".[dev]"
   ```
3. Set up your environment:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/unit/test_parquet_manager.py
```

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Add docstrings to public functions and classes
- Keep functions focused and under 50 lines when possible

## Documentation

- Update relevant documentation when adding features
- Add code examples for new functionality
- Update PROJECT_MEMORY.md with design decisions

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear commit messages
3. Add tests for new functionality
4. Ensure all tests pass
5. Update documentation as needed
6. Submit a pull request with a clear description

## Commit Messages

Use clear, descriptive commit messages:
- `feat: Add query caching to QueryEngine`
- `fix: Handle missing metadata gracefully`
- `docs: Update setup instructions`
- `test: Add tests for S3Downloader`

## Questions?

- Check existing documentation in `docs/`
- Review [PROJECT_MEMORY.md](PROJECT_MEMORY.md) for design context
- Open an issue for discussion

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
