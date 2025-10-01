# Getting Started

Welcome to QuantMini! This guide will help you get up and running quickly.

## Prerequisites

- Python 3.10 or higher
- Polygon.io API key (for data ingestion)
- Basic understanding of quantitative trading concepts

## Installation

### Basic Installation

```bash
pip install quantmini
```

### With Optional Dependencies

```bash
# With ML models (LightGBM, XGBoost, CatBoost)
pip install quantmini[ml]

# With development tools
pip install quantmini[dev]

# Everything
pip install quantmini[all]
```

### From Source

```bash
git clone https://github.com/nittygritty-zzy/quantmini.git
cd quantmini
pip install -e ".[all]"
```

## Configuration

1. **Copy the credentials template:**

```bash
cp config/credentials.yaml.example config/credentials.yaml
```

2. **Add your API credentials:**

```yaml
polygon:
  api:
    key: "YOUR_POLYGON_API_KEY"
```

3. **Configure data paths:**

Edit `config/pipeline_config.yaml` to set your data storage location.

## Quick Example

Here's a simple example to get you started:

```python
import qlib
from qlib.contrib.model.gbdt import LGBModel
from qlib.data.dataset import DatasetH

# Initialize Qlib
qlib.init(
    provider_uri="/path/to/qlib/data",
    region="us"
)

# Create and train a model
model = LGBModel()
dataset = DatasetH(...)
model.fit(dataset)

# Make predictions
predictions = model.predict(dataset)
```

## Next Steps

- **Examples**: Check out the [examples directory](https://github.com/nittygritty-zzy/quantmini/tree/main/examples)
- **Guides**: Read our comprehensive guides
- **API Reference**: Explore the full API documentation

## Common Issues

### Issue: ModuleNotFoundError

Make sure you've installed all required dependencies:

```bash
pip install quantmini[all]
```

### Issue: Qlib data not found

You need to ingest and convert data first:

```bash
uv run python scripts/convert_to_qlib.py
```

See the [data pipeline guide](guides/data_pipeline.md) for details.

## Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/nittygritty-zzy/quantmini/issues)
- **Documentation**: [Full documentation](https://quantmini.readthedocs.io/)
- **Examples**: [Example scripts](https://github.com/nittygritty-zzy/quantmini/tree/main/examples)
