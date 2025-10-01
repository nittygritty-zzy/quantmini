# Setup Instructions for quantmini on External Drive

## Location
This project is now located on the external SanDisk drive:
- **Path**: `/Users/zheyuanzhao/sandisk/quantmini`
- **Original**: `/Users/zheyuanzhao/Documents/quantmini`

## Important: External Drive Considerations

Since this is on an external drive, you need to use **copy mode** for uv installations:

```bash
export UV_LINK_MODE=copy
```

Add this to your shell profile (`~/.zshrc` or `~/.bashrc`) for permanent effect:
```bash
echo 'export UV_LINK_MODE=copy' >> ~/.zshrc
source ~/.zshrc
```

## Quick Setup

1. **Navigate to project**:
   ```bash
   cd /Users/zheyuanzhao/sandisk/quantmini
   ```

2. **Activate virtual environment**:
   ```bash
   source .venv/bin/activate
   ```

3. **Install/Update dependencies** (if needed):
   ```bash
   UV_LINK_MODE=copy uv pip install -e ".[dev]"
   ```

## Running Tests

```bash
cd /Users/zheyuanzhao/sandisk/quantmini
source .venv/bin/activate

# Run all unit tests
pytest tests/unit/ -v

# Run e2e tests (requires credentials)
pytest tests/integration/test_e2e_ingestion.py -v -s -m e2e

# Run specific test
pytest tests/unit/test_system_profiler.py -v
```

## Configuration

The project credentials are in `config/credentials.yaml` (already configured).

## Project Structure

See `PHASE4_STRUCTURE.md` for complete project layout and documentation.

## Troubleshooting

### Issue: "Failed to clone files" warning
**Solution**: Already handled! Use `UV_LINK_MODE=copy` as shown above.

### Issue: Virtual environment not working
**Solution**: Recreate it:
```bash
cd /Users/zheyuanzhao/sandisk/quantmini
rm -rf .venv
uv venv
source .venv/bin/activate
UV_LINK_MODE=copy uv pip install -e ".[dev]"
```

### Issue: Import errors
**Solution**: Make sure you've installed the package in editable mode:
```bash
UV_LINK_MODE=copy uv pip install -e ".[dev]"
```

---

**Project Status**: ✅ Fully functional on external drive
**Phase**: 4 (Parquet Data Lake) - Complete
