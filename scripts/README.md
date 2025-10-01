# Scripts Directory

Utility scripts for pipeline operations and maintenance.

## Available Scripts

### Data Conversion
- **convert_to_qlib.py** - Convert Parquet data to Qlib binary format for ML pipelines

## Usage

All scripts should be run from the project root using `uv`:

```bash
# Example: Convert parquet to qlib format
uv run python scripts/convert_to_qlib.py
```

## Environment

Scripts use the project's configuration system and respect the `DATA_ROOT` environment variable.

See [SETUP.md](../docs/SETUP.md) for configuration details.
