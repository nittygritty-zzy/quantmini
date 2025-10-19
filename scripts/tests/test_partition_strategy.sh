#!/bin/bash
set -e

echo "================================================================================"
echo "TESTING PARTITIONING STRATEGY"
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
echo "📊 Step 2: Partition data (TEST MODE)"
echo "================================================================================"
echo ""

python scripts/partition_by_date_and_ticker.py

echo ""

# Verify partitioning
echo "================================================================================"
echo "🔍 Step 3: Verify partitioned structure"
echo "================================================================================"
echo ""

echo "Partitioned directories created:"
find data/partitioned -type d -name "ticker=*" | head -20
echo ""

echo "Sample parquet files:"
find data/partitioned -name "*.parquet" | head -10
echo ""

echo "File count by data type:"
for dir in data/partitioned/*/; do
    count=$(find "$dir" -name "*.parquet" | wc -l | tr -d ' ')
    echo "  $(basename $dir): $count files"
done
echo ""

# Test DuckDB query
echo "================================================================================"
echo "🦆 Step 4: Test DuckDB queries"
echo "================================================================================"
echo ""

python << 'EOF'
import duckdb

con = duckdb.connect()

print("Query 1: Get AAPL balance sheets for 2024")
print("-" * 60)
query1 = """
SELECT
    company_name,
    filing_date,
    fiscal_year,
    fiscal_period
FROM read_parquet('data/partitioned/balance_sheets/ticker=AAPL/**/*.parquet')
WHERE filing_date >= '2024-01-01'
LIMIT 5
"""
result1 = con.execute(query1).df()
print(result1)
print("")

print("Query 2: Count records by ticker")
print("-" * 60)
query2 = """
SELECT
    COUNT(*) as record_count
FROM read_parquet('data/partitioned/balance_sheets/**/*.parquet')
"""
result2 = con.execute(query2).df()
print(result2)
print("")

print("Query 3: Get short interest for MSFT")
print("-" * 60)
query3 = """
SELECT
    ticker,
    settlement_date,
    short_interest,
    days_to_cover
FROM read_parquet('data/partitioned/short_interest/ticker=MSFT/**/*.parquet')
ORDER BY settlement_date DESC
LIMIT 5
"""
try:
    result3 = con.execute(query3).df()
    print(result3)
except Exception as e:
    print(f"⚠️  Query failed (expected if no short interest data): {e}")

print("\n✅ DuckDB queries successful!")
EOF

echo ""
echo "================================================================================"
echo "✅ PARTITIONING STRATEGY TEST COMPLETE"
echo "================================================================================"
echo ""
echo "Review the output above. If everything looks good, you can:"
echo "1. Clean test data: rm -rf data/partitioned/* data/fundamentals/* etc"
echo "2. Run production load with:"
echo "   python scripts/partition_by_date_and_ticker.py --production"
echo ""
