#!/bin/bash
# Package Publishing Script for QuantMini
# Usage: ./scripts/publish_package.sh [test|prod]

set -e  # Exit on error

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default to test environment
ENV=${1:-test}

echo "======================================================================"
echo "QuantMini Package Publishing"
echo "======================================================================"
echo ""

# Check if build and twine are installed
echo -e "${YELLOW}[1/6] Checking dependencies...${NC}"
if ! command -v python &> /dev/null; then
    echo -e "${RED}Error: python not found${NC}"
    exit 1
fi

# Install build tools if needed
if ! python -c "import build" 2>/dev/null; then
    echo "Installing build tools..."
    uv pip install build twine
fi

echo -e "${GREEN}✓ Dependencies ready${NC}"
echo ""

# Clean old builds
echo -e "${YELLOW}[2/6] Cleaning old builds...${NC}"
rm -rf dist/ build/ src/*.egg-info
echo -e "${GREEN}✓ Cleaned${NC}"
echo ""

# Build package
echo -e "${YELLOW}[3/6] Building package...${NC}"
python -m build
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Package built successfully${NC}"
echo ""

# List built files
echo -e "${YELLOW}[4/6] Built files:${NC}"
ls -lh dist/
echo ""

# Check package
echo -e "${YELLOW}[5/6] Checking package...${NC}"
twine check dist/*
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Package check failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Package is valid${NC}"
echo ""

# Upload
echo -e "${YELLOW}[6/6] Uploading to ${ENV}...${NC}"
if [ "$ENV" = "test" ]; then
    echo "Uploading to TestPyPI..."
    echo "You'll need your TestPyPI API token"
    echo "Username: __token__"
    twine upload --repository testpypi dist/*

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ Successfully uploaded to TestPyPI!${NC}"
        echo ""
        echo "Test installation:"
        echo "  pip install --index-url https://test.pypi.org/simple/ quantmini"
        echo ""
        echo "View package:"
        echo "  https://test.pypi.org/project/quantmini/"
    fi

elif [ "$ENV" = "prod" ]; then
    echo -e "${RED}WARNING: You are about to publish to PRODUCTION PyPI${NC}"
    echo "This cannot be undone!"
    echo ""
    read -p "Have you tested on TestPyPI? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        echo "Aborted. Please test on TestPyPI first:"
        echo "  ./scripts/publish_package.sh test"
        exit 1
    fi

    echo ""
    echo "Uploading to PyPI..."
    echo "You'll need your PyPI API token"
    echo "Username: __token__"
    twine upload dist/*

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ Successfully uploaded to PyPI!${NC}"
        echo ""
        echo "Installation:"
        echo "  pip install quantmini"
        echo ""
        echo "View package:"
        echo "  https://pypi.org/project/quantmini/"
        echo ""
        echo "Don't forget to:"
        echo "  1. Create a GitHub release"
        echo "  2. Update documentation"
        echo "  3. Announce the release"
    fi
else
    echo -e "${RED}Error: Invalid environment. Use 'test' or 'prod'${NC}"
    exit 1
fi

echo ""
echo "======================================================================"
echo "Done!"
echo "======================================================================"
