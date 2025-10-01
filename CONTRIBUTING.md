# Contributing to QuantMini

Thank you for your interest in contributing to QuantMini! This document provides guidelines for contributing to this project.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/quantmini.git
   cd quantmini
   ```
3. **Install development dependencies**:
   ```bash
   uv venv
   source .venv/bin/activate  # On macOS/Linux
   uv pip install -e ".[dev]"
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

### 3. Test Your Changes

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/unit/test_your_module.py

# Run with coverage
pytest --cov=src --cov-report=html tests/
```

### 4. Commit Your Changes

```bash
git add .
git commit -m "feat: Add your feature description"
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
- Use type hints
- Maximum line length: 100 characters
- Use meaningful variable names
- Add Google-style docstrings

### Example

```python
def calculate_alpha(prices: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    Calculate alpha factor from price data.

    Args:
        prices: DataFrame with price data
        window: Rolling window size

    Returns:
        Series with alpha values
    """
    return (prices - prices.rolling(window).mean()) / prices.rolling(window).std()
```

## Testing

### Writing Tests

- Place tests in `tests/` directory
- Mirror the source structure
- Use descriptive test names
- Test edge cases

### Example Test

```python
def test_calculate_alpha():
    """Test alpha calculation."""
    prices = pd.DataFrame({'close': [100, 101, 102, 103]})
    result = calculate_alpha(prices, window=2)
    assert not result.isna().all()
```

## Documentation

### Adding Documentation

1. Update relevant `.md` files in `docs_source/`
2. Add examples when introducing new features
3. Update API reference if adding new modules

### Building Documentation Locally

```bash
cd docs_source
pip install -e ".[docs]"
sphinx-build -b html . _build/html
```

View at `docs_source/_build/html/index.html`

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass
- [ ] Documentation updated
- [ ] Code follows style guidelines
- [ ] Commit messages are clear
- [ ] No unnecessary files included

### Pull Request Description

Include:
- What changes you made
- Why you made them
- Any relevant issue numbers
- Testing done

### Example PR Description

```markdown
## Description
Add support for custom alpha expressions

## Changes
- Added AlphaExpression class
- Updated documentation
- Added unit tests

## Testing
- All existing tests pass
- Added 5 new tests for alpha expressions

Fixes #123
```

## Reporting Issues

### Bug Reports

Include:
- Python version
- QuantMini version
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages/stack traces

### Feature Requests

Include:
- Clear description of the feature
- Use cases
- Why it would be valuable
- Any implementation ideas

## Code Review Process

1. Maintainers will review your PR
2. Address any feedback
3. Once approved, it will be merged
4. Your contribution will be credited

## Questions?

- Open an issue for questions
- Check existing issues first
- Review the [documentation](https://quantmini.readthedocs.io/)
- Email: zheyuan28@gmail.com

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to QuantMini!
