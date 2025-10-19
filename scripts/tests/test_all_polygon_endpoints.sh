#!/bin/bash
set -e

echo "================================================================================"
echo "TESTING ALL POLYGON REST API ENDPOINTS (NON-STOCK/OPTION)"
echo "================================================================================"
echo ""
echo "This will test all fundamental, reference, and economy data endpoints"
echo ""

# Activate virtual environment
source .venv/bin/activate

# Test stocks for all endpoints
TEST_STOCKS="AAPL MSFT GOOGL"

# Clean test data
echo "🧹 Cleaning test data directories..."
rm -rf data/fundamentals/* data/corporate_actions/* data/reference/* data/economy/* data/market_status/* data/news/* 2>/dev/null
echo ""

# ============================================================================
# 1. FUNDAMENTALS
# ============================================================================
echo "================================================================================"
echo "1️⃣  FUNDAMENTALS"
echo "================================================================================"
echo ""

echo "📥 Testing fundamentals download..."
quantmini polygon fundamentals $TEST_STOCKS
echo ""

echo "✅ Files created:"
ls -lh data/fundamentals/*.parquet | wc -l
echo ""

# ============================================================================
# 2. SHORT DATA
# ============================================================================
echo "================================================================================"
echo "2️⃣  SHORT DATA"
echo "================================================================================"
echo ""

echo "📥 Testing short interest download..."
quantmini polygon short-interest $TEST_STOCKS
echo ""

echo "📥 Testing short volume download..."
quantmini polygon short-volume $TEST_STOCKS
echo ""

echo "✅ Files created:"
ls -lh data/fundamentals/short_*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# 3. CORPORATE ACTIONS
# ============================================================================
echo "================================================================================"
echo "3️⃣  CORPORATE ACTIONS"
echo "================================================================================"
echo ""

echo "📥 Testing corporate actions (all stocks, recent data)..."
quantmini polygon corporate-actions --start-date 2024-01-01
echo ""

echo "📥 Testing corporate actions for specific ticker..."
quantmini polygon corporate-actions --ticker AAPL --start-date 2024-01-01
echo ""

echo "✅ Files created:"
ls -lh data/corporate_actions/*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# 4. REFERENCE DATA
# ============================================================================
echo "================================================================================"
echo "4️⃣  REFERENCE DATA"
echo "================================================================================"
echo ""

echo "📥 Testing related tickers download..."
quantmini polygon related-tickers $TEST_STOCKS
echo ""

echo "📥 Testing ticker types download..."
quantmini polygon ticker-types --asset-class stocks
echo ""

echo "✅ Files created:"
ls -lh data/reference/*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# 5. ECONOMY DATA
# ============================================================================
echo "================================================================================"
echo "5️⃣  ECONOMY DATA"
echo "================================================================================"
echo ""

echo "📥 Testing economy data download (last 90 days)..."
quantmini polygon economy --days 90
echo ""

echo "📥 Testing yield curve download..."
quantmini polygon yield-curve
echo ""

echo "✅ Files created:"
ls -lh data/economy/*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# 6. MARKET STATUS
# ============================================================================
echo "================================================================================"
echo "6️⃣  MARKET STATUS"
echo "================================================================================"
echo ""

echo "📥 Testing market status download..."
quantmini polygon market-status
echo ""

echo "✅ Files created:"
ls -lh data/market_status/*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# 7. NEWS
# ============================================================================
echo "================================================================================"
echo "7️⃣  NEWS"
echo "================================================================================"
echo ""

echo "📥 Testing news download..."
quantmini polygon news --ticker AAPL --limit 10
echo ""

echo "✅ Files created:"
ls -lh data/news/*.parquet 2>/dev/null | wc -l
echo ""

# ============================================================================
# SUMMARY
# ============================================================================
echo "================================================================================"
echo "✅ ALL POLYGON ENDPOINTS TESTED"
echo "================================================================================"
echo ""

echo "Summary of downloaded data:"
echo ""
echo "📊 Fundamentals:"
ls -lh data/fundamentals/*.parquet 2>/dev/null | head -5
echo ""
echo "📈 Corporate Actions:"
ls -lh data/corporate_actions/*.parquet 2>/dev/null | head -5
echo ""
echo "🔗 Reference Data:"
ls -lh data/reference/*.parquet 2>/dev/null | head -5
echo ""
echo "💰 Economy Data:"
ls -lh data/economy/*.parquet 2>/dev/null | head -5
echo ""
echo "📰 News:"
ls -lh data/news/*.parquet 2>/dev/null | head -5
echo ""
echo "🏛️  Market Status:"
ls -lh data/market_status/*.parquet 2>/dev/null | head -5
echo ""

echo "================================================================================"
echo "Next steps:"
echo "1. Review the data files above"
echo "2. Run partitioning script: python scripts/partition_by_date_screener.py"
echo "3. For production: Use download_all_*.py scripts"
echo "================================================================================"
