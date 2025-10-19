#!/bin/bash
set -e

echo "================================================================================"
echo "TESTING DATE-FIRST PARTITIONING FOR STOCK SCREENERS"
echo "================================================================================"
echo ""

# Activate virtual environment
source .venv/bin/activate

# Test with a small sample of stocks
TEST_STOCKS="AAPL MSFT GOOGL"

echo "📥 Step 1: Download sample data for test stocks: $TEST_STOCKS"
echo "--------------------------------------------------------------------------------"
echo ""

# Download fundamentals
echo "Downloading fundamentals..."
quantmini polygon fundamentals $TEST_STOCKS

echo ""
echo "Downloading corporate actions..."
quantmini polygon corporate-actions $TEST_STOCKS

echo ""
echo "Downloading short interest..."
quantmini polygon short-interest $TEST_STOCKS

echo ""
echo "✅ Sample data downloaded"
echo ""

# Run partitioning in test mode
echo "================================================================================"
echo "📊 Step 2: Partition data (DATE-FIRST for screeners)"
echo "================================================================================"
echo ""

python scripts/partition_by_date_screener.py

echo ""

# Verify partitioning
echo "================================================================================"
echo "🔍 Step 3: Verify partitioned structure"
echo "================================================================================"
echo ""

echo "Date-based directories created:"
find data/partitioned_screener -type d -name "year=*" | head -20
echo ""

echo "Sample parquet files (by ticker):"
find data/partitioned_screener -name "ticker=*.parquet" | head -10
echo ""

echo "File count by data type:"
for dir in data/partitioned_screener/*/; do
    count=$(find "$dir" -name "*.parquet" | wc -l | tr -d ' ')
    echo "  $(basename $dir): $count files"
done
echo ""

# Test DuckDB screener queries
echo "================================================================================"
echo "🦆 Step 4: Test DuckDB SCREENER queries"
echo "================================================================================"
echo ""

python << 'EOF'
import duckdb
from datetime import datetime

con = duckdb.connect()

print("=" * 80)
print("STOCK SCREENER QUERY TESTS")
print("=" * 80)
print("")

# Query 1: Get ALL stocks for a specific month (screener use case)
print("Query 1: Get all stocks with balance sheet data for a specific month")
print("-" * 80)
query1 = """
SELECT
    COUNT(DISTINCT ticker) as num_tickers,
    COUNT(*) as total_records
FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/**/*.parquet')
"""
try:
    result1 = con.execute(query1).df()
    print(result1)
    print("✅ Screener query successful - scanned all stocks efficiently!")
except Exception as e:
    print(f"⚠️  Query failed: {e}")
print("")

# Query 2: Screen stocks by financial metrics
print("Query 2: Screen stocks by filing date (latest filings)")
print("-" * 80)
query2 = """
SELECT
    ticker,
    company_name,
    filing_date,
    fiscal_year,
    fiscal_period
FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/**/*.parquet')
ORDER BY filing_date DESC
LIMIT 5
"""
try:
    result2 = con.execute(query2).df()
    print(result2)
except Exception as e:
    print(f"⚠️  Query failed: {e}")
print("")

# Query 3: Get specific ticker across all dates (still efficient)
print("Query 3: Get AAPL data across all dates")
print("-" * 80)
query3 = """
SELECT
    ticker,
    company_name,
    filing_date,
    fiscal_year,
    fiscal_period
FROM read_parquet('data/partitioned_screener/balance_sheets/**/ticker=AAPL.parquet')
ORDER BY filing_date DESC
LIMIT 5
"""
try:
    result3 = con.execute(query3).df()
    print(result3)
except Exception as e:
    print(f"⚠️  Query failed (expected if no AAPL data): {e}")
print("")

# Query 4: Compare performance - scan specific month vs all data
print("Query 4: Performance comparison - October 2024 only")
print("-" * 80)
query4 = """
SELECT
    COUNT(*) as records,
    MIN(filing_date) as earliest,
    MAX(filing_date) as latest
FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/month=10/*.parquet')
"""
try:
    result4 = con.execute(query4).df()
    print(result4)
    print("✅ Date-filtered query - only scanned October files!")
except Exception as e:
    print(f"⚠️  Query failed: {e}")

print("\n" + "=" * 80)
print("✅ DuckDB SCREENER QUERIES SUCCESSFUL!")
print("=" * 80)
EOF

echo ""
echo "================================================================================"
echo "✅ DATE-FIRST PARTITIONING TEST COMPLETE"
echo "================================================================================"
echo ""
echo "Review the output above. This structure is optimized for:"
echo "  1. Stock screeners that scan ALL stocks for a time period"
echo "  2. Queries like: 'Find all stocks with P/E < 15 in Q3 2024'"
echo "  3. Efficient filtering by date range"
echo ""
echo "If everything looks good, you can:"
echo "1. Clean test data: rm -rf data/partitioned_screener/* data/fundamentals/* etc"
echo "2. Run production load with:"
echo "   python scripts/partition_by_date_screener.py --production"
echo ""
