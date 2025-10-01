# ReadTheDocs Setup Guide

This guide explains how to set up ReadTheDocs documentation for QuantMini.

## What is ReadTheDocs?

ReadTheDocs (RTD) automatically builds, hosts, and versions your documentation from your GitHub repository.

## Prerequisites

✅ GitHub repository pushed (already done!)
✅ Documentation files created (already done!)

## Setup Steps

### Step 1: Create ReadTheDocs Account

1. Go to https://readthedocs.org/
2. Click "Sign Up"
3. Choose "Sign up with GitHub"
4. Authorize ReadTheDocs to access your GitHub account

### Step 2: Import Your Project

1. After logging in, click **"Import a Project"**
2. You'll see a list of your GitHub repositories
3. Find **"quantmini"** in the list
4. Click the **"+"** button next to it

**Or manually import:**
1. Click "Import a Project"
2. Click "Import Manually"
3. Fill in:
   - **Name**: quantmini
   - **Repository URL**: https://github.com/nittygritty-zzy/quantmini
   - **Repository type**: Git
   - **Default branch**: main
4. Click "Next"

### Step 3: Configure Build Settings

The `.readthedocs.yaml` file in your repository configures the build automatically.

Default settings:
- **Python version**: 3.10
- **Documentation type**: Sphinx
- **Source directory**: docs_source/
- **Configuration file**: docs_source/conf.py

### Step 4: Trigger First Build

1. After importing, RTD will automatically trigger a build
2. Watch the build progress on the "Builds" page
3. First build usually takes 2-5 minutes

### Step 5: Access Your Documentation

Once the build succeeds:

**Your docs will be at:**
- https://quantmini.readthedocs.io/

**Alternative URLs:**
- https://quantmini.readthedocs.io/en/latest/
- https://quantmini.readthedocs.io/en/stable/

## Build Configuration

### Files We Created

1. **`.readthedocs.yaml`** - RTD configuration
   ```yaml
   version: 2
   build:
     os: ubuntu-22.04
     tools:
       python: "3.10"
   sphinx:
     configuration: docs_source/conf.py
   ```

2. **`docs_source/conf.py`** - Sphinx configuration
   - Theme: sphinx_rtd_theme
   - Extensions: autodoc, napoleon, viewcode, myst_parser

3. **`docs_source/index.rst`** - Main documentation page

4. **Documentation content:**
   - `getting_started.md`
   - `installation.md`
   - `changelog.md`
   - `contributing.md`

### Adding Dependencies

Documentation dependencies are in `pyproject.toml`:

```toml
[project.optional-dependencies]
docs = [
    "sphinx>=7.0.0",
    "sphinx-rtd-theme>=2.0.0",
    "myst-parser>=2.0.0",
    "sphinx-autodoc-typehints>=1.25.0",
]
```

## Advanced Configuration

### Enable PDF and ePub Downloads

Already configured in `.readthedocs.yaml`:

```yaml
formats:
  - pdf
  - epub
```

### Versioning

RTD automatically creates documentation for:
- **latest**: Most recent commit on main branch
- **stable**: Most recent tagged release
- **Each tag**: Individual version docs

To create a new version:

```bash
git tag v0.1.1
git push origin v0.1.1
```

RTD will automatically build docs for this version.

### Custom Domain (Optional)

You can set up a custom domain:

1. Go to project settings on RTD
2. Click "Domains"
3. Add your custom domain
4. Configure DNS (CNAME record)

Example: `docs.quantmini.com` → `quantmini.readthedocs.io`

## Build Status Badge

Add to your README.md:

```markdown
[![Documentation Status](https://readthedocs.org/projects/quantmini/badge/?version=latest)](https://quantmini.readthedocs.io/en/latest/?badge=latest)
```

## Troubleshooting

### Build Fails

1. Check the build log on RTD
2. Common issues:
   - Missing dependencies
   - Import errors
   - Sphinx configuration errors

### Fix: Missing Dependencies

Make sure all dependencies are in `pyproject.toml`:

```toml
[project.optional-dependencies]
docs = [
    "sphinx>=7.0.0",
    "sphinx-rtd-theme>=2.0.0",
    "myst-parser>=2.0.0",
]
```

### Fix: Import Errors

If autodoc can't import modules, check:
- Python path in `conf.py`
- Module structure
- __init__.py files

### Test Build Locally

```bash
cd docs_source
pip install sphinx sphinx_rtd_theme myst-parser
sphinx-build -b html . _build/html
```

Open `_build/html/index.html` in your browser.

## Automatic Rebuilds

RTD automatically rebuilds documentation when:
- You push to GitHub
- You create a new tag/release
- You trigger a manual build

### Webhook (Automatic)

RTD sets up a GitHub webhook automatically. Every push triggers a build.

### Manual Build

1. Go to your RTD project
2. Click "Builds"
3. Click "Build Version: latest"

## Documentation Structure

Current structure:

```
docs_source/
├── conf.py              # Sphinx configuration
├── index.rst            # Main page
├── getting_started.md   # Getting started guide
├── installation.md      # Installation instructions
├── changelog.md         # Changelog
├── contributing.md      # Contributing guide
├── guides/              # User guides (TODO)
├── examples/            # Example documentation (TODO)
└── api/                 # API reference (TODO)
```

## Next Steps

After RTD is set up:

1. ✅ Documentation builds successfully
2. ✅ Add more content to guides/
3. ✅ Add API reference documentation
4. ✅ Link from README.md to docs
5. ✅ Share your documentation!

## Links

- **ReadTheDocs**: https://readthedocs.org/
- **Sphinx**: https://www.sphinx-doc.org/
- **RTD Theme**: https://sphinx-rtd-theme.readthedocs.io/
- **MyST Parser**: https://myst-parser.readthedocs.io/

## Support

- RTD Documentation: https://docs.readthedocs.io/
- Sphinx Tutorial: https://www.sphinx-doc.org/en/master/tutorial/
- Issues: Open an issue on GitHub
