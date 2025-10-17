# Scripts Directory

This directory contains standalone Python scripts that have been **migrated to the QuantMini CLI**.

## Migration Status

All script functionality is now available through the `quantmini` CLI command.

### Scripts → CLI Command Mapping

| Old Script | New CLI Command | Status |
|-----------|----------------|--------|
| `backfill_historical.py` | `quantmini pipeline backfill` | ✅ Migrated |
| `convert_to_qlib.py` | `quantmini data convert` | ✅ Migrated |
| `enrich_features.py` | `quantmini data enrich` | ✅ Migrated |
| `download_delisted_stocks.py` | *(specialized)* | ⏳ Pending |
| `validate_production_schemas.py` | `quantmini schema validate` | ✅ Migrated |
| `diagnose_schema_issues.py` | `quantmini schema diagnose` | ✅ Migrated |
| `reingest_production.py` | `quantmini schema fix` | ✅ Migrated |
| `verify_qlib_conversion.py` | `quantmini schema verify-qlib` | ✅ Migrated |

## Using the CLI

Instead of running scripts directly, use the `quantmini` command:

```bash
# OLD WAY (deprecated)
python scripts/validate_production_schemas.py --data-root /path/to/data

# NEW WAY (recommended)
quantmini schema validate --data-root /path/to/data
```

### Schema Management

```bash
# Validate schemas
quantmini schema validate

# Diagnose specific data type
quantmini schema diagnose --data-type stocks_daily

# Fix schema inconsistencies
quantmini schema fix --data-type stocks_daily

# Verify Qlib binary format
quantmini schema verify-qlib
```

### Data Operations

```bash
# Download data
quantmini data download --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31

# Ingest data
quantmini data ingest --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31

# Enrich features
quantmini data enrich --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31

# Convert to Qlib
quantmini data convert --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31
```

### Pipeline Workflows

```bash
# Run full pipeline
quantmini pipeline run --data-type stocks_daily --start-date 2024-01-01 --end-date 2024-12-31

# Daily update
quantmini pipeline daily --data-type stocks_daily

# Backfill missing data
quantmini pipeline backfill --data-type stocks_daily --start-date 2020-01-01 --end-date 2024-12-31
```

## Documentation

For complete CLI documentation, see:
- [CLI.md](../CLI.md) - Complete CLI reference
- Run `quantmini --help` for command help
- Run `quantmini <command> --help` for specific command help

## Why Migrate to CLI?

Benefits of using the CLI:

1. **Single Entry Point**: All functionality accessible through `quantmini` command
2. **Better Help**: Integrated help system with `--help` flag
3. **Consistency**: Uniform command structure and options
4. **Error Handling**: Better error messages and validation
5. **Progress Tracking**: Built-in progress bars and status updates
6. **Configuration**: Automatic config loading from `config/`
7. **Testing**: CLI commands are easier to test and maintain

## Scripts Library

These scripts remain as **library modules** that the CLI imports. They can still be used programmatically:

```python
from scripts.validate_production_schemas import ProductionSchemaValidator

validator = ProductionSchemaValidator(data_root)
exit_code = validator.validate_all()
```

However, for command-line usage, **always use the CLI**.

## Deprecation Timeline

- **Phase 1 (Current)**: Scripts work but CLI is recommended
- **Phase 2 (Future)**: Scripts will show deprecation warnings
- **Phase 3 (Future)**: Scripts may be removed or converted to library-only modules

## Questions?

See `CLI.md` for full documentation or run:

```bash
quantmini --help
```
