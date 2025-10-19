# Installation

## Requirements

- Python 3.10 or higher
- uv package manager (recommended)
- External storage (recommended: 500GB+ for comprehensive data)
- Polygon.io account with API key and S3 access

## Installation Methods

### From Source (Recommended)

Install from GitHub repository:

```bash
# Clone repository
git clone https://github.com/nittygritty-zzy/quantmini.git
cd quantmini

# Install with uv
uv sync

# Activate virtual environment
source .venv/bin/activate
```

### External Drive Setup

If installing on an external drive, use copy mode:

```bash
export UV_LINK_MODE=copy
uv sync
```

Add to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
echo 'export UV_LINK_MODE=copy' >> ~/.zshrc
source ~/.zshrc
```

## Configuration

### 1. Credentials

Copy and configure credentials:

```bash
cp config/credentials.yaml.example config/credentials.yaml
```

Edit `config/credentials.yaml`:

```yaml
polygon:
  api_key: "YOUR_POLYGON_API_KEY"
  s3:
    access_key_id: "YOUR_S3_ACCESS_KEY"
    secret_access_key: "YOUR_S3_SECRET_KEY"
```

### 2. Data Storage

Configure data root in `config/pipeline_config.yaml`:

```yaml
data_root: /Volumes/sandisk/quantmini-lake
```

Or set environment variable:

```bash
export DATA_ROOT=/Volumes/sandisk/quantmini-lake
```

## Verify Installation

Check that QuantMini is installed correctly:

```bash
source .venv/bin/activate
python -c "from src.core.config_loader import ConfigLoader; print('QuantMini installed successfully!')"
```

## Dependencies

### Core Dependencies

- **Data Processing**: polars >= 1.18.0, pandas >= 2.2.3, pyarrow >= 18.1.0
- **Cloud Storage**: aioboto3 >= 13.0.1, boto3 >= 1.35.74
- **Database**: duckdb >= 1.0.0
- **ML Framework**: pyqlib >= 0.9.0
- **Configuration**: pyyaml >= 6.0.2
- **System**: psutil >= 6.1.1

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

## Directory Structure

After installation, the data directory structure will be:

```
$DATA_ROOT/
├── landing/           # Raw source data
├── bronze/            # Validated Parquet files
├── silver/            # Feature-enriched data
└── gold/qlib/         # ML-ready binary format
```

## Troubleshooting

### Issue: "Failed to clone files" warning

**Solution**: Use copy mode for external drives:

```bash
UV_LINK_MODE=copy uv sync
```

### Issue: Virtual environment not working

**Solution**: Recreate the virtual environment:

```bash
rm -rf .venv
uv venv
source .venv/bin/activate
uv sync
```

### Issue: Import errors

**Solution**: Make sure the virtual environment is activated:

```bash
source .venv/bin/activate
```

### Issue: Polygon credentials not found

**Solution**: Verify credentials file exists and has correct format:

```bash
cat config/credentials.yaml
```

### Issue: Data directory not found

**Solution**: Create the directory structure:

```bash
mkdir -p $DATA_ROOT/{landing,bronze,silver,gold/qlib}
```

## Next Steps

1. **Configure credentials**: Add your Polygon.io API key and S3 credentials
2. **Download data**: See [Getting Started](getting_started.md) guide
3. **Run tests**: Verify installation with `pytest tests/`
4. **Explore examples**: Check `examples/` directory
