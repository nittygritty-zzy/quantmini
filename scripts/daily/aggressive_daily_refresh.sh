#!/bin/bash
#
# Aggressive Daily Refresh Script - Unlimited API Tier
#
# Priority: Data Quality > API Efficiency
# Runs: Daily at 1:00 AM
#
# Features:
# - 1-year fundamentals lookback (catch all amendments)
# - Daily short interest/volume (all tickers)
# - Daily ticker events (immediate symbol change detection)
# - Comprehensive quality checks
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Data lake root
QUANTLAKE_ROOT="${QUANTLAKE_ROOT:-$HOME/workspace/quantlake}"
export QUANTLAKE_ROOT

BRONZE_DIR="$QUANTLAKE_ROOT/bronze"
SILVER_DIR="$QUANTLAKE_ROOT/silver"
GOLD_DIR="$QUANTLAKE_ROOT/gold"
LOGS_DIR="$PROJECT_ROOT/logs"
SNAPSHOT_DIR="$QUANTLAKE_ROOT/snapshots"

# Create directories
mkdir -p "$LOGS_DIR" "$SNAPSHOT_DIR"

# Log file
LOG_FILE="$LOGS_DIR/aggressive_daily_$(date +%Y%m%d_%H%M%S).log"
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

# Ticker universe (expand as needed)
FUNDAMENTAL_TICKERS="${FUNDAMENTAL_TICKERS:-$(cat $PROJECT_ROOT/config/ticker_universe.txt 2>/dev/null || echo 'AAPL MSFT GOOGL AMZN NVDA META TSLA JPM V MA')}"

# Timing
START_TIME=$(date +%s)

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_section() {
    echo ""
    echo "========================================================================"
    echo "  $1"
    echo "========================================================================"
    echo ""
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

log_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

run_command() {
    local cmd="$1"
    local desc="${2:-Running command}"

    echo ""
    log_info "$desc"
    echo "Command: $cmd"
    echo ""

    if eval "$cmd"; then
        return 0
    else
        return 1
    fi
}

# Track success
OVERALL_SUCCESS=true

log_section "AGGRESSIVE DAILY REFRESH - $(date)"
log_info "Data Lake Root: $QUANTLAKE_ROOT"
log_info "Ticker Universe: $(echo $FUNDAMENTAL_TICKERS | wc -w) tickers"
log_info "Strategy: Maximum Quality (Unlimited API)"

#==============================================================================
# STEP 1: FUNDAMENTALS - 1 Year Lookback (Catch All Amendments)
#==============================================================================
log_section "STEP 1: Fundamentals Download (1-Year Lookback)"

# Quarterly fundamentals (1-year lookback)
if run_command "quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
  --timeframe quarterly \
  --filing-date-gte $(date -d '365 days ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals" \
  "Downloading quarterly fundamentals (365-day lookback)"; then
    log_success "Quarterly fundamentals downloaded"
else
    log_error "Quarterly fundamentals failed"
    OVERALL_SUCCESS=false
fi

# Annual fundamentals (1-year lookback for amendments)
if run_command "quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
  --timeframe annual \
  --filing-date-gte $(date -d '365 days ago' +%Y-%m-%d) \
  --output-dir $BRONZE_DIR/fundamentals" \
  "Downloading annual fundamentals (365-day lookback)"; then
    log_success "Annual fundamentals downloaded"
else
    log_error "Annual fundamentals failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 2: FINANCIAL RATIOS - Comprehensive Metrics
#==============================================================================
log_section "STEP 2: Financial Ratios Calculation"

if run_command "quantmini polygon financial-ratios $FUNDAMENTAL_TICKERS \
  --input-dir $BRONZE_DIR/fundamentals \
  --output-dir $BRONZE_DIR/fundamentals \
  --include-growth" \
  "Calculating comprehensive financial ratios"; then
    log_success "Financial ratios calculated"
else
    log_error "Financial ratios calculation failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 3: SHORT INTEREST/VOLUME - Full Dataset Daily
#==============================================================================
log_section "STEP 3: Short Interest & Short Volume (All Tickers)"

log_info "Note: API returns ALL tickers - downloading complete dataset"

if run_command "quantmini polygon short-data $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/fundamentals" \
  "Downloading short interest and short volume (all tickers)"; then
    log_success "Short interest/volume downloaded"

    # Create daily snapshot
    TODAY=$(date +%Y-%m-%d)
    SNAPSHOT_SHORT_DIR="$SNAPSHOT_DIR/short_data"
    mkdir -p "$SNAPSHOT_SHORT_DIR"

    log_info "Creating daily snapshot: $SNAPSHOT_SHORT_DIR/snapshot_$TODAY"
    # Archive for historical analysis
    if [ -d "$BRONZE_DIR/fundamentals/short_interest" ]; then
        cp -r "$BRONZE_DIR/fundamentals/short_interest" \
          "$SNAPSHOT_SHORT_DIR/short_interest_$TODAY"
    fi
    if [ -d "$BRONZE_DIR/fundamentals/short_volume" ]; then
        cp -r "$BRONZE_DIR/fundamentals/short_volume" \
          "$SNAPSHOT_SHORT_DIR/short_volume_$TODAY"
    fi

    log_success "Daily short data snapshot created"
else
    log_error "Short interest/volume download failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 4: TICKER EVENTS - Daily Check for Symbol Changes
#==============================================================================
log_section "STEP 4: Ticker Events (Symbol Changes)"

if run_command "quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
  --output-dir $BRONZE_DIR/corporate_actions" \
  "Checking ticker events for all tickers"; then
    log_success "Ticker events checked"
else
    log_error "Ticker events check failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 5: CORPORATE ACTIONS - Historical (90 days for quality)
#==============================================================================
log_section "STEP 5: Corporate Actions - Historical (90 days)"

START_DATE=$(date -d '90 days ago' +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

if run_command "quantmini polygon corporate-actions \
  --start-date $START_DATE \
  --end-date $END_DATE \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions" \
  "Downloading historical corporate actions (90-day window)"; then
    log_success "Historical corporate actions downloaded"
else
    log_error "Historical corporate actions failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 6: CORPORATE ACTIONS - Future Events (180 days for extended planning)
#==============================================================================
log_section "STEP 6: Corporate Actions - Future Events (180 days)"

FUTURE_END=$(date -d '180 days' +%Y-%m-%d)

if run_command "quantmini polygon corporate-actions \
  --start-date $END_DATE \
  --end-date $FUTURE_END \
  --include-ipos \
  --output-dir $BRONZE_DIR/corporate_actions_future" \
  "Downloading future corporate actions (180-day window)"; then
    log_success "Future corporate actions downloaded"

    # Count future events
    if command -v python3 &> /dev/null; then
        python3 << 'EOF'
import polars as pl
from pathlib import Path
import os

ca_path = Path(os.environ.get('BRONZE_DIR', '.')) / 'corporate_actions_future'
if ca_path.exists():
    div_files = list(ca_path.glob('*/dividends/**/*.parquet'))
    split_files = list(ca_path.glob('*/splits/**/*.parquet'))

    div_count = 0
    split_count = 0

    if div_files:
        try:
            df_div = pl.read_parquet(div_files)
            div_count = len(df_div)
        except:
            pass

    if split_files:
        try:
            df_split = pl.read_parquet(split_files)
            split_count = len(df_split)
        except:
            pass

    print(f"Future dividends: {div_count}")
    print(f"Future splits: {split_count}")
EOF
    fi
else
    log_error "Future corporate actions failed"
    OVERALL_SUCCESS=false
fi

#==============================================================================
# STEP 7: DATA QUALITY CHECKS
#==============================================================================
log_section "STEP 7: Data Quality Validation"

log_info "Running comprehensive quality checks..."

# Check fundamentals freshness
if command -v python3 &> /dev/null; then
    python3 << 'EOF'
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import os

def check_fundamentals_freshness():
    """Check how fresh fundamentals data is"""
    fund_path = Path(os.environ.get('BRONZE_DIR', '.')) / 'fundamentals'

    issues = []

    for subdir in ['balance_sheets', 'income_statements', 'cash_flow']:
        path = fund_path / subdir
        if not path.exists():
            issues.append(f"Missing: {subdir}")
            continue

        files = list(path.rglob('*.parquet'))
        if not files:
            issues.append(f"No data: {subdir}")
            continue

        # Check latest filing date
        try:
            df = pl.read_parquet(files)
            if 'filing_date' in df.columns:
                latest = df['filing_date'].max()
                days_old = (datetime.now().date() - latest).days

                if days_old > 90:
                    issues.append(f"{subdir}: {days_old} days stale")
                else:
                    print(f"✓ {subdir}: {days_old} days old (OK)")
        except Exception as e:
            issues.append(f"{subdir}: Error reading - {e}")

    if issues:
        print("\n⚠ Quality Issues Found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("\n✓ All quality checks passed")
        return True

check_fundamentals_freshness()
EOF
fi

#==============================================================================
# STEP 8: DAILY SNAPSHOT (Complete State Archive)
#==============================================================================
log_section "STEP 8: Daily Snapshot Archive"

TODAY=$(date +%Y-%m-%d)
DAILY_SNAPSHOT_DIR="$SNAPSHOT_DIR/daily/$TODAY"

log_info "Creating complete daily snapshot: $DAILY_SNAPSHOT_DIR"

mkdir -p "$DAILY_SNAPSHOT_DIR"

# Copy bronze layer (fundamentals + corporate actions)
if [ -d "$BRONZE_DIR/fundamentals" ]; then
    cp -r "$BRONZE_DIR/fundamentals" "$DAILY_SNAPSHOT_DIR/bronze_fundamentals"
    log_success "Fundamentals snapshot created"
fi

if [ -d "$BRONZE_DIR/corporate_actions" ]; then
    cp -r "$BRONZE_DIR/corporate_actions" "$DAILY_SNAPSHOT_DIR/bronze_corporate_actions"
    log_success "Corporate actions snapshot created"
fi

# Compress for storage
log_info "Compressing snapshot..."
tar -czf "$DAILY_SNAPSHOT_DIR.tar.gz" -C "$SNAPSHOT_DIR/daily" "$TODAY" && \
  rm -rf "$DAILY_SNAPSHOT_DIR"

SNAPSHOT_SIZE=$(du -h "$DAILY_SNAPSHOT_DIR.tar.gz" | cut -f1)
log_success "Snapshot compressed: $SNAPSHOT_SIZE"

# Cleanup old snapshots (keep last 90 days)
log_info "Cleaning up snapshots older than 90 days..."
find "$SNAPSHOT_DIR/daily" -name "*.tar.gz" -mtime +90 -delete 2>/dev/null || true

#==============================================================================
# COMPLETION SUMMARY
#==============================================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

log_section "AGGRESSIVE DAILY REFRESH COMPLETE"

if [ "$OVERALL_SUCCESS" = true ]; then
    log_success "All steps completed successfully!"
else
    log_error "Some steps failed - check logs above"
fi

echo ""
echo "Summary:"
echo "  Duration: ${DURATION_MIN}m ${DURATION_SEC}s"
echo "  Log file: $LOG_FILE"
echo "  Snapshot: $DAILY_SNAPSHOT_DIR.tar.gz ($SNAPSHOT_SIZE)"
echo ""

# Statistics
if command -v python3 &> /dev/null; then
    python3 << 'EOF'
from pathlib import Path
import os

bronze_dir = Path(os.environ.get('BRONZE_DIR', '.'))

def count_parquet_files(path):
    """Count parquet files in directory"""
    if not path.exists():
        return 0
    return len(list(path.rglob('*.parquet')))

print("Data Statistics:")
print(f"  Fundamentals files: {count_parquet_files(bronze_dir / 'fundamentals')}")
print(f"  Corporate actions files: {count_parquet_files(bronze_dir / 'corporate_actions')}")
print("")
EOF
fi

if [ "$OVERALL_SUCCESS" = true ]; then
    exit 0
else
    exit 1
fi
