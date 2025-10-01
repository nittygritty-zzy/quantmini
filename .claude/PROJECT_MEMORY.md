# QuantMini Project Memory

## Critical Workflow Rules

### 1. Test File Cleanup Protocol
**IMPORTANT**: After creating temporary test files or scripts, ALWAYS clean them up immediately after testing is complete.

**Cleanup Checklist:**
- [ ] Remove test scripts from root directory
- [ ] Remove test data files
- [ ] Remove temporary .pkl, .h5, .pt model files from examples/
- [ ] Remove any scratch/temp directories
- [ ] Move permanent documentation to appropriate docs/ subdirectories

**Example locations to check:**
- Root directory: No test files should remain here
- examples/: Only maintained example files, no saved models
- scripts/: Only production-ready scripts
- docs/: Organized into subdirectories (api-reference, architecture, changelog, etc.)

### 2. Project Structure Standards

**Root directory should only contain:**
- Configuration files: .env.example, .gitignore, pyproject.toml, uv.lock
- Documentation: README.md, LICENSE
- Directories: .claude/, .venv/, config/, docs/, examples/, scripts/, src/, tests/

**Documentation Organization:**
- `docs/api-reference/` - API documentation
- `docs/architecture/` - Architecture decisions and design docs
- `docs/changelog/` - Change logs, update notes, fix documentation
- `docs/development/` - Development guides
- `docs/examples/` - Example data
- `docs/getting-started/` - Installation and quickstart
- `docs/guides/` - User guides (testing, signals, benchmarks, etc.)
- `docs/reference/` - Technical reference

**Examples folder should contain:**
- Example scripts only (*.py)
- Configuration files (*.yaml)
- Documentation (README.md, *_README.md)
- NO saved models (*.pkl, *.h5, *.pt)
- NO temporary test files

### 3. Qlib Integration Notes

**Data Conversion Critical Fixes** (see docs/changelog/QLIB_BINARY_WRITER_UPDATES.md):
1. Filter null symbols in SQL query
2. Use tab-separated format for instruments files
3. Create `.qlib/dataset_info.json` with frequency metadata
4. Clean up macOS metadata files (._*)
5. Add proper date ranges to instruments file

**Gym Warning Suppression** (see docs/changelog/QLIB_GYM_WARNING.md):
- Use `src/utils/suppress_gym_warnings.py` in all Qlib examples
- Call `patch_gym()` BEFORE importing qlib
- All examples should use this pattern for clean output

**Qlib Functions Reference:**
- Always check qlib source code before reimplementing utilities
- Common patterns are already in qlib.contrib
- Keep notes of frequently used qlib utilities

### 4. File Naming Conventions

**Do NOT add these prefixes/suffixes to files:**
- `fixed_`, `modified_`, `version2_`, `_v2`, `_old`, `_backup`
- For testing: create new file with `_test` suffix, then replace original once confirmed working
- For examples: use descriptive names with `_example` suffix

### 5. Git Operations

**Before committing:**
- Review all changes
- Ensure no temporary files are included
- Verify .gitignore is working correctly
- Clean up any test files

**Ignored patterns:**
- `mlruns/`, `catboost_info/`, `logs/`
- `examples/*.pkl`, `examples/*.h5`, `examples/*.pt`, `examples/*.joblib`
- `.DS_Store`, `._*`, `*.tmp`, `*.bak`

### 6. Environment Management

**Python execution:**
- ALWAYS use `uv run python` for running scripts
- Verify uv venv is active before operations
- Check today's date before time-sensitive commands

### 7. Testing Workflow

**After completing a test:**
1. Verify the test works as expected
2. Check with user if utility function is needed
3. Clean up test files immediately
4. Update documentation if permanent changes were made
5. Update this PROJECT_MEMORY.md if new patterns emerge

## Recent Changes

### 2025-09-30: Project Cleanup
- Removed all temporary directories (Users/, catboost_info/, mlruns/, logs/)
- Removed all saved model files from examples/
- Removed legacy example files (qlib_model_example.py)
- Moved changelog docs to docs/changelog/
- Updated .gitignore with ML tracking directories and model file patterns
- Established cleanup protocol for future work

### 2025-09-30: Qlib Examples Completed
- Created 5 clean example files for Qlib integration
- All examples use suppress_gym_warnings for clean output
- Examples cover: models, custom models, strategies, enhanced indexing, workflow
- Created comprehensive guides: BENCHMARK_DATA_GUIDE.md, TRADING_SIGNALS_GUIDE.md

### 2025-09-30: Critical Data Conversion Fixes
- Fixed 6 critical issues in QlibBinaryWriter
- Documented all fixes in QLIB_BINARY_WRITER_UPDATES.md
- All Qlib data now properly formatted and compatible

## Useful Qlib Functions

### Data Access
- `qlib.init(provider_uri, region)` - Initialize Qlib
- `D.features([symbols], [fields], start_time, end_time)` - Get feature data
- `D.instruments(market, filter_pipe)` - Get instrument list

### Models
- `qlib.contrib.model.gbdt.LGBModel` - LightGBM model
- `qlib.contrib.model.gbdt.XGBModel` - XGBoost model  
- `qlib.contrib.model.gbdt.CatBoostModel` - CatBoost model
- `qlib.model.base.Model` - Base class for custom models

### Strategies
- `qlib.contrib.strategy.TopkDropoutStrategy` - Top-k portfolio with turnover control
- `qlib.contrib.strategy.EnhancedIndexingStrategy` - Benchmark tracking with optimization

### Data Handlers
- `qlib.contrib.data.handler.Alpha158` - 158 technical indicators

### Utils
- `qlib.utils.init_instance_by_config` - Create objects from config dicts
- `qlib.workflow.R` - Experiment recorder
- `qlib.backtest` - Backtesting utilities

