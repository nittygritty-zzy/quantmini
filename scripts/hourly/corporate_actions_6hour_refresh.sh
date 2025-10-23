#!/bin/bash
#
# Corporate Actions 6-Hour Refresh - Unlimited API Tier
#
# Priority: Real-time announcement capture
# Runs: Every 6 hours (12 AM, 6 AM, 12 PM, 6 PM)
#
# Purpose: Capture corporate action announcements within 6 hours
#

set -euo pipefail

# Configuration
QUANTLAKE_ROOT="${QUANTLAKE_ROOT:-$HOME/workspace/quantlake}"
export QUANTLAKE_ROOT

BRONZE_DIR="$QUANTLAKE_ROOT/bronze"
LOGS_DIR="$(dirname "$0")/../../logs"

mkdir -p "$LOGS_DIR"

# Log file
LOG_FILE="$LOGS_DIR/corporate_actions_refresh_$(date +%Y%m%d_%H%M%S).log"
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

# Ticker universe
FUNDAMENTAL_TICKERS="${FUNDAMENTAL_TICKERS:-AAPL MSFT GOOGL AMZN NVDA}"

# Timing
START_TIME=$(date +%s)

echo "========================================================================"
echo "CORPORATE ACTIONS 6-HOUR REFRESH - $(date)"
echo "========================================================================"
echo ""
echo "Time: $(date +%H:%M)"
echo "Ticker Universe: $(echo $FUNDAMENTAL_TICKERS | wc -w) tickers"
echo ""

#==============================================================================
# 1. Historical Corporate Actions (90-day lookback for quality)
#==============================================================================
echo ""
echo "Refreshing historical corporate actions (90-day lookback)..."
echo ""

START_DATE=$(date -d '90 days ago' +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

if quantmini polygon corporate-actions \
  --start-date $START_DATE \
  --end-date $END_DATE \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions; then
    echo "✓ Historical corporate actions refreshed"
else
    echo "✗ Historical corporate actions failed"
fi

#==============================================================================
# 2. Future Corporate Actions (180-day window for extended planning)
#==============================================================================
echo ""
echo "Refreshing future corporate actions (180-day window)..."
echo ""

FUTURE_END=$(date -d '180 days' +%Y-%m-%d)

if quantmini polygon corporate-actions \
  --start-date $END_DATE \
  --end-date $FUTURE_END \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_future; then
    echo "✓ Future corporate actions refreshed"

    # Count and display future events
    if command -v python3 &> /dev/null; then
        echo ""
        echo "Future events summary:"
        python3 << 'EOF'
import polars as pl
from pathlib import Path
import os

ca_future_path = Path(os.environ.get('BRONZE_DIR', '.')) / 'corporate_actions_future'

if ca_future_path.exists():
    # Count dividends
    div_files = list(ca_future_path.glob('**/dividends/**/*.parquet'))
    if div_files:
        try:
            df_div = pl.read_parquet(div_files)
            print(f"  • Future dividends: {len(df_div)} records")
            if 'ex_dividend_date' in df_div.columns:
                next_date = df_div['ex_dividend_date'].min()
                print(f"    Next ex-dividend date: {next_date}")
        except Exception as e:
            print(f"  • Future dividends: Error reading - {e}")

    # Count splits
    split_files = list(ca_future_path.glob('**/splits/**/*.parquet'))
    if split_files:
        try:
            df_split = pl.read_parquet(split_files)
            print(f"  • Future splits: {len(df_split)} records")
            if 'execution_date' in df_split.columns:
                next_date = df_split['execution_date'].min()
                print(f"    Next split execution: {next_date}")
        except Exception as e:
            print(f"  • Future splits: Error reading - {e}")
else:
    print("  No future corporate actions directory found")
EOF
    fi
else
    echo "✗ Future corporate actions failed"
fi

#==============================================================================
# 3. Ticker Events (Symbol Changes) - Every 6 hours
#==============================================================================
echo ""
echo "Checking ticker events (symbol changes)..."
echo ""

if quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions; then
    echo "✓ Ticker events checked"
else
    echo "✗ Ticker events failed"
fi

#==============================================================================
# 4. IPO Calendar (Next 30 days) - Separate tracking
#==============================================================================
echo ""
echo "Updating IPO calendar (next 30 days)..."
echo ""

IPO_END=$(date -d '30 days' +%Y-%m-%d)

if quantmini polygon corporate-actions \
  --start-date $END_DATE \
  --end-date $IPO_END \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_ipo_calendar; then
    echo "✓ IPO calendar updated"

    # Display upcoming IPOs
    if command -v python3 &> /dev/null; then
        echo ""
        echo "Upcoming IPOs (next 30 days):"
        python3 << 'EOF'
import polars as pl
from pathlib import Path
import os

ipo_path = Path(os.environ.get('BRONZE_DIR', '.')) / 'corporate_actions_ipo_calendar'

if ipo_path.exists():
    ipo_files = list(ipo_path.glob('**/ipos/**/*.parquet'))
    if ipo_files:
        try:
            df_ipo = pl.read_parquet(ipo_files)
            if len(df_ipo) > 0:
                print(f"  Total upcoming IPOs: {len(df_ipo)}")
                if 'listing_date' in df_ipo.columns:
                    upcoming = df_ipo.sort('listing_date').head(5)
                    for row in upcoming.iter_rows(named=True):
                        ticker = row.get('ticker', 'Unknown')
                        date = row.get('listing_date', 'Unknown')
                        print(f"    • {ticker}: {date}")
            else:
                print("  No upcoming IPOs in next 30 days")
        except Exception as e:
            print(f"  Error: {e}")
    else:
        print("  No IPO data files found")
else:
    print("  No IPO calendar directory found")
EOF
    fi
else
    echo "✗ IPO calendar update failed"
fi

#==============================================================================
# COMPLETION SUMMARY
#==============================================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "========================================================================"
echo "CORPORATE ACTIONS REFRESH COMPLETE"
echo "========================================================================"
echo ""
echo "Refresh time: $(date +%H:%M)"
echo "Duration: ${DURATION}s"
echo "Log file: $LOG_FILE"
echo "Next refresh: $(date -d '6 hours' +%H:%M)"
echo ""

# Exit code: success if we got here
exit 0
