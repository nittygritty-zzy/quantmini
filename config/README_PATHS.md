# Path Configuration Guide

This directory contains the centralized path configuration for the QuantMini data pipeline.

## Quick Start

**To switch between production and test environments:**

1. Edit `config/paths.yaml`
2. Change `active_environment` from `production` to `test` (or vice versa)
3. Restart your pipeline - all components automatically use the new paths!

```yaml
# In config/paths.yaml
active_environment: production  # or 'test'
```

That's it! No code changes needed.

---

## Configuration Files

### `paths.yaml` (Main Path Configuration)

**Purpose**: Centralized path management for all environments

**Features**:
- ✅ Single place to configure all data lake paths
- ✅ Easy environment switching (production ↔ test)
- ✅ Environment aliases (prod, dev, staging, local)
- ✅ Auto-creation of directories
- ✅ Path validation rules

**Structure**:
```yaml
# Active environment
active_environment: production  # or 'test'

# Production paths
production:
  data_lake_root: /Volumes/sandisk/quantmini-lake
  bronze_path: /Volumes/sandisk/quantmini-lake/bronze
  silver_path: /Volumes/sandisk/quantmini-lake/silver
  gold_path: /Volumes/sandisk/quantmini-lake/gold
  metadata_path: /Volumes/sandisk/quantmini-lake/metadata
  logs_path: /Volumes/sandisk/quantmini-lake/logs

# Test paths (for development)
test:
  data_lake_root: /Users/zheyuanzhao/workspace/quantmini/data/test-lake
  # ... test environment paths
```

---

## Usage Examples

### Example 1: Switch to Test Environment

```yaml
# Edit config/paths.yaml
active_environment: test  # Changed from 'production'
```

```bash
# Run pipeline - automatically uses test paths
python -m src.cli.main data ingest -t stocks_daily -s 2025-10-18 -e 2025-10-18

# Verify environment
python -c "from src.core.config_loader import ConfigLoader; \
           print(f'Environment: {ConfigLoader().get_environment()}')"
# Output: Environment: test
```

### Example 2: Use Environment Aliases

```yaml
# These all map to the same environment
active_environment: prod      # → production
active_environment: dev       # → test
active_environment: staging   # → test
active_environment: local     # → test
```

### Example 3: Override Environment in Code

```python
from src.core.config_loader import ConfigLoader

# Force test environment (ignore paths.yaml)
config = ConfigLoader(environment='test')

# Get paths
bronze_path = config.get_bronze_path()
print(f"Bronze: {bronze_path}")
# Output: Bronze: /Users/zheyuanzhao/workspace/quantmini/data/test-lake/bronze
```

### Example 4: Check Current Environment

```bash
# View current configuration
python -m src.core.config_loader

# Output shows:
# 🌍 Environment: PRODUCTION
#   Data Lake: /Volumes/sandisk/quantmini-lake
```

---

## Environment Setup

### Production Environment

**Purpose**: Full-scale production data processing

**Characteristics**:
- Large external SSD (`/Volumes/sandisk/quantmini-lake/`)
- Full Medallion Architecture (landing → bronze → silver → gold)
- All data types (stocks/options daily/minute)
- Complete historical data
- Production-grade storage

**When to use**:
- Production data pipelines
- Final model training
- Production deployments
- Full backfills

### Test Environment

**Purpose**: Development, testing, and experimentation

**Characteristics**:
- Local storage (`~/workspace/quantmini/data/test-lake/`)
- Same Medallion Architecture structure
- Smaller datasets (sample data)
- Faster iteration
- Isolated from production

**When to use**:
- Development and debugging
- Testing new features
- Schema validation
- Unit/integration tests
- CI/CD pipelines

---

## Configuration Priority

**Path resolution order** (highest to lowest priority):

1. **Explicit path in `paths.yaml`** (e.g., `bronze_path: /custom/path`)
2. **Computed from `data_lake_root`** (e.g., `data_lake_root/bronze`)
3. **Error if missing** (fail-fast on misconfiguration)

**Example**:
```yaml
production:
  data_lake_root: /Volumes/sandisk/quantmini-lake
  bronze_path: /custom/bronze  # Explicit path (highest priority)
  # silver_path not specified → uses data_lake_root/silver (computed)
```

---

## Path Validation

### Auto-Creation

Directories are automatically created when:
```yaml
validation:
  auto_create_directories: true  # Default: true
```

### Absolute Path Requirement

```yaml
validation:
  require_absolute_paths: true  # Enforces absolute paths
```

All paths must be absolute (start with `/`). Relative paths will raise an error.

### Minimum Free Space

```yaml
validation:
  min_free_space_gb: 50  # Warn if less than 50GB free
```

---

## Best Practices

### 1. Never Commit Production Credentials

```yaml
# ❌ BAD - Don't commit production paths with sensitive data
production:
  data_lake_root: /path/to/sensitive/data

# ✅ GOOD - Use paths that work for everyone
production:
  data_lake_root: /Volumes/sandisk/quantmini-lake  # Generic external drive
```

### 2. Use Environment-Specific Settings

```yaml
# Production: Large external drive
production:
  data_lake_root: /Volumes/sandisk/quantmini-lake

# Test: Local workspace
test:
  data_lake_root: ~/workspace/quantmini/data/test-lake
```

### 3. Test Before Switching

```bash
# Before changing active_environment, test the target environment
python -m src.core.config_loader --environment test

# If successful, update paths.yaml
```

### 4. Document Custom Environments

If you add custom environments:
```yaml
# Custom staging environment
staging:
  data_lake_root: /mnt/staging/quantmini-lake
  # ... staging paths

# Add alias for convenience
environments:
  stage: staging
```

---

## Troubleshooting

### Error: "data_lake_root not configured"

**Problem**: The active environment doesn't have `data_lake_root` set

**Solution**:
```yaml
# In config/paths.yaml, ensure your environment has data_lake_root
production:
  data_lake_root: /Volumes/sandisk/quantmini-lake  # Must be set
```

### Error: "Environment 'xyz' not found in paths.yaml"

**Problem**: `active_environment` references a non-existent environment

**Solution**:
```yaml
# Fix typo or add the environment
active_environment: production  # Must be 'production' or 'test'

# Or add custom environment:
my_custom_env:
  data_lake_root: /path/to/data
```

### Paths Not Changing

**Problem**: Changed `paths.yaml` but code still uses old paths

**Solution**:
1. Restart Python process (config is loaded at startup)
2. Verify file was saved: `cat config/paths.yaml`
3. Check for typos in environment name

---

## Integration with Other Config Files

### Relationship with `pipeline_config.yaml`

**Priority**: `paths.yaml` > `pipeline_config.yaml` for path settings

```yaml
# paths.yaml (HIGHEST priority for paths)
production:
  bronze_path: /Volumes/sandisk/quantmini-lake/bronze

# pipeline_config.yaml (lower priority - ignored for paths)
bronze_path: /old/path  # This is ignored!
```

### Relationship with `system_profile.yaml`

**Independence**: System profile (CPU/RAM) is independent of environment

```yaml
# system_profile.yaml - applies to ALL environments
hardware:
  cpu_cores: 10
  ram_gb: 16
```

---

## Migration from Legacy Configuration

**Old way** (multiple files, hardcoded paths):
```python
# ❌ Old: Hardcoded paths everywhere
data_root = Path('/Volumes/sandisk/quantmini-data')
bronze_path = data_root / 'parquet'
```

**New way** (centralized `paths.yaml`):
```python
# ✅ New: One source of truth
config = ConfigLoader()
bronze_path = config.get_bronze_path()  # Automatically uses active environment
```

---

## Summary

**Key Benefits**:
- ✅ **Single source of truth** for all paths
- ✅ **One-line environment switching** (production ↔ test)
- ✅ **No code changes** needed to change environments
- ✅ **Fail-fast validation** on misconfiguration
- ✅ **Clean separation** between production and test data

**Files to Know**:
- `config/paths.yaml` - Main path configuration (edit this!)
- `config/pipeline_config.yaml` - Pipeline settings (not paths)
- `config/system_profile.yaml` - Hardware profile (auto-generated)

**Quick Commands**:
```bash
# View current config
python -m src.core.config_loader

# Test specific environment
python -c "from src.core.config_loader import ConfigLoader; \
           ConfigLoader(environment='test').print_config()"

# Verify paths
python -c "from src.core.config_loader import ConfigLoader; \
           c = ConfigLoader(); \
           print(f'Bronze: {c.get_bronze_path()}'); \
           print(f'Silver: {c.get_silver_path()}'); \
           print(f'Gold: {c.get_gold_path()}')"
```

---

**Last Updated**: October 18, 2025
