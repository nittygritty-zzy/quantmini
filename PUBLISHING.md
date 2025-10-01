# Publishing QuantMini to PyPI

This guide walks you through publishing the QuantMini package to PyPI (Python Package Index).

## Prerequisites

1. **PyPI Account**
   - Create account at: https://pypi.org/account/register/
   - Verify your email address

2. **TestPyPI Account** (recommended for testing)
   - Create account at: https://test.pypi.org/account/register/
   - This is separate from your PyPI account

3. **API Tokens**
   - PyPI: https://pypi.org/manage/account/token/
   - TestPyPI: https://test.pypi.org/manage/account/token/
   - Scope: "Entire account" or "Project: quantmini"

## Step 1: Update Package Metadata

✅ Already done! Check `pyproject.toml` for:
- Version number
- Author email (update `your-email@example.com` to your real email)
- Description
- URLs
- Dependencies

**IMPORTANT**: Update your email in `pyproject.toml`:
```bash
# Edit line 9 to use your real email
nano pyproject.toml
```

## Step 2: Install Build Tools

```bash
# Install build and twine
uv pip install build twine
```

## Step 3: Build the Package

```bash
# Clean previous builds
rm -rf dist/ build/ src/*.egg-info

# Build source distribution and wheel
python -m build
```

This creates:
- `dist/quantmini-0.1.0.tar.gz` (source distribution)
- `dist/quantmini-0.1.0-py3-none-any.whl` (wheel)

## Step 4: Test the Build Locally

```bash
# Install from local wheel
pip install dist/quantmini-0.1.0-py3-none-any.whl

# Test import
python -c "import src; print('Success!')"

# Uninstall
pip uninstall quantmini -y
```

## Step 5: Upload to TestPyPI (Recommended)

Test your package on TestPyPI first:

```bash
# Upload to TestPyPI
twine upload --repository testpypi dist/*

# You'll be prompted for:
# Username: __token__
# Password: <your TestPyPI API token>
```

**Test installation from TestPyPI:**
```bash
pip install --index-url https://test.pypi.org/simple/ quantmini
```

Visit: https://test.pypi.org/project/quantmini/

## Step 6: Upload to PyPI (Production)

Once you've verified everything works on TestPyPI:

```bash
# Upload to PyPI
twine upload dist/*

# You'll be prompted for:
# Username: __token__
# Password: <your PyPI API token>
```

**After successful upload:**
- Visit: https://pypi.org/project/quantmini/
- Test installation: `pip install quantmini`

## Step 7: Configure API Tokens (Optional)

Store tokens in `~/.pypirc` to avoid entering them each time:

```bash
cat > ~/.pypirc << 'EOF'
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-your-api-token-here

[testpypi]
username = __token__
password = pypi-your-testpypi-token-here
EOF

chmod 600 ~/.pypirc
```

## Version Management

When releasing new versions:

1. **Update version in `pyproject.toml`:**
   ```toml
   version = "0.1.1"  # or "0.2.0", "1.0.0", etc.
   ```

2. **Commit changes:**
   ```bash
   git add pyproject.toml
   git commit -m "Bump version to 0.1.1"
   git tag v0.1.1
   git push && git push --tags
   ```

3. **Rebuild and upload:**
   ```bash
   rm -rf dist/
   python -m build
   twine upload dist/*
   ```

## Semantic Versioning

Follow semantic versioning (MAJOR.MINOR.PATCH):

- **PATCH** (0.1.0 → 0.1.1): Bug fixes
- **MINOR** (0.1.0 → 0.2.0): New features (backward compatible)
- **MAJOR** (0.1.0 → 1.0.0): Breaking changes

## Installation Methods

After publishing, users can install:

```bash
# Basic installation
pip install quantmini

# With ML dependencies
pip install quantmini[ml]

# With dev dependencies
pip install quantmini[dev]

# All dependencies
pip install quantmini[all]

# From GitHub (development version)
pip install git+https://github.com/nittygritty-zzy/quantmini.git
```

## Troubleshooting

### Error: File already exists

PyPI doesn't allow re-uploading the same version. Solutions:
1. Increment version number
2. Delete old `dist/` folder before building

### Error: Invalid package name

Package names on PyPI must be unique. Check availability:
https://pypi.org/project/quantmini/

### Build includes unwanted files

Check/update:
- `.gitignore`
- `MANIFEST.in`
- `pyproject.toml` → `[tool.hatch.build.targets.wheel]`

### Import errors after installation

Make sure package structure is correct:
```
quantmini/
├── src/
│   ├── __init__.py
│   ├── core/
│   ├── ingest/
│   └── ...
└── pyproject.toml
```

## Automation with GitHub Actions (Future)

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - name: Build
        run: |
          pip install build
          python -m build
      - name: Publish
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

## Security Best Practices

1. ✅ Never commit API tokens to git
2. ✅ Use scoped tokens (project-specific)
3. ✅ Store tokens in `~/.pypirc` with `chmod 600`
4. ✅ Use TestPyPI before production
5. ✅ Verify package contents before uploading
6. ✅ Sign releases with GPG (optional but recommended)

## Checklist Before Publishing

- [ ] Update version in `pyproject.toml`
- [ ] Update author email to real email
- [ ] Update README.md with installation instructions
- [ ] Test package builds successfully
- [ ] Test package installs locally
- [ ] Upload to TestPyPI first
- [ ] Test installation from TestPyPI
- [ ] Review package page on TestPyPI
- [ ] Upload to PyPI
- [ ] Create GitHub release
- [ ] Update documentation

## Support

- PyPI Help: https://pypi.org/help/
- Packaging Guide: https://packaging.python.org/
- Issues: https://github.com/nittygritty-zzy/quantmini/issues
