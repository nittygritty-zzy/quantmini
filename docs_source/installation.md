# Installation

## Requirements

- Python 3.10 or higher
- pip or uv package manager

## Installation Methods

### Via pip (Recommended)

Install from PyPI:

```bash
pip install quantmini
```

### With Optional Dependencies

QuantMini offers several optional dependency groups:

```bash
# ML models (LightGBM, XGBoost, CatBoost)
pip install quantmini[ml]

# Development tools (pytest, coverage)
pip install quantmini[dev]

# Everything
pip install quantmini[all]
```

### From Source

For development or to get the latest unreleased features:

```bash
git clone https://github.com/nittygritty-zzy/quantmini.git
cd quantmini
pip install -e ".[all]"
```

### Using uv (Alternative)

If you prefer the `uv` package manager:

```bash
uv pip install quantmini[all]
```

## Verify Installation

Check that QuantMini is installed correctly:

```python
import src
print("QuantMini installed successfully!")
```

## Dependencies

### Core Dependencies

- pyarrow >= 18.1.0
- polars >= 1.18.0
- pandas >= 2.2.3
- aioboto3 >= 13.0.1
- boto3 >= 1.35.74
- pyyaml >= 6.0.2
- psutil >= 6.1.1
- duckdb >= 1.0.0
- pyqlib >= 0.9.0

### Optional: ML Dependencies

- lightgbm >= 4.5.0
- xgboost >= 2.1.0
- catboost >= 1.2.7
- scikit-learn >= 1.5.0
- gymnasium >= 1.0.0

### Optional: Development Dependencies

- pytest >= 8.3.4
- pytest-asyncio >= 0.25.2
- pytest-cov >= 6.0.0

## Troubleshooting

### Issue: Build fails on macOS

Some dependencies may require additional build tools:

```bash
brew install cmake
pip install quantmini[all]
```

### Issue: NumPy version conflicts

If you encounter NumPy compatibility issues:

```bash
pip install --upgrade numpy
pip install quantmini[all]
```

### Issue: Qlib installation fails

Qlib requires specific versions. Try:

```bash
pip install pyqlib==0.9.0
pip install quantmini
```
