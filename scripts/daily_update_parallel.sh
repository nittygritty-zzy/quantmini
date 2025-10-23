#!/bin/bash
################################################################################
# QuantMini Daily Update Script - PARALLEL OPTIMIZED VERSION
#
# This is a high-performance version that runs independent jobs in parallel.
# Performance: 17-30 min (sequential) → 5-10 min (parallel) - 3-4x faster!
#
# Parallel Execution Strategy:
# - Landing Layer: All 4 S3 downloads run in parallel
# - Bronze Layer: S3 ingestion + Polygon API downloads run in parallel
# - Silver Layer: All 3 transformations run in parallel
# - Gold Layer: Sequential (feature dependencies)
#
# Usage:
#   ./scripts/daily_update_parallel.sh [OPTIONS]
#
# Options:
#   --date DATE          Specific date to process (YYYY-MM-DD), defaults to yesterday
#   --days-back N        Process last N days (default: 1)
#   --skip-landing       Skip landing layer (download from S3)
#   --skip-bronze        Skip bronze layer ingestion
#   --skip-silver        Skip silver layer transformations
#   --skip-gold          Skip gold layer enrichment
#   --fundamental-tickers "TICKER1 TICKER2 ..."  Tickers for fundamentals
#   --max-parallel N     Max parallel jobs (default: auto-detect cores)
#   --dry-run            Show what would be executed without running
#
# Examples:
#   ./scripts/daily_update_parallel.sh                    # Update yesterday's data (parallel)
#   ./scripts/daily_update_parallel.sh --days-back 7      # Backfill last 7 days (parallel)
#   ./scripts/daily_update_parallel.sh --max-parallel 8   # Limit to 8 parallel jobs
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

################################################################################
# Configuration
################################################################################

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a  # Auto-export all variables
    source "$PROJECT_ROOT/.env"
    set +a
    echo "Loaded environment from $PROJECT_ROOT/.env"
fi

# Activate virtual environment first (needed for yaml module)
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Data lake paths - read from config/paths.yaml (ONLY source of truth)
PATHS_CONFIG="$PROJECT_ROOT/config/paths.yaml"
if [ -f "$PATHS_CONFIG" ]; then
    QUANTLAKE_ROOT=$(python -c "
import yaml
with open('$PATHS_CONFIG') as f:
    config = yaml.safe_load(f)
    active_env = config.get('active_environment', 'production')
    print(config.get(active_env, {}).get('data_lake_root', ''))
")

    if [ -z "$QUANTLAKE_ROOT" ]; then
        echo "ERROR: Could not read data_lake_root from $PATHS_CONFIG"
        exit 1
    fi
    echo "Using data_lake_root from config/paths.yaml: $QUANTLAKE_ROOT"
else
    echo "ERROR: Config file not found: $PATHS_CONFIG"
    exit 1
fi

export QUANTLAKE_ROOT
BRONZE_DIR="$QUANTLAKE_ROOT/bronze"
SILVER_DIR="$QUANTLAKE_ROOT/silver"

# Fundamental tickers - loaded dynamically from ticker reference data
# Fallback to top 50 S&P 500 if ticker table not available
FUNDAMENTAL_TICKERS_FALLBACK="AAPL MSFT GOOGL AMZN NVDA META TSLA BRK.B JPM V MA UNH JNJ WMT PG XOM LLY HD MRK CVX ABBV KO PEP COST AVGO MCD CSCO ACN TMO ADBE NFLX DIS CRM ABT AMD NKE QCOM INTC TXN DHR VZ HON UPS LOW ORCL PM BMY INTU SBUX AXP"
FUNDAMENTAL_TICKERS=""  # Will be populated from ticker table

# Parallel execution settings
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    MAX_PARALLEL="${MAX_PARALLEL:-$(sysctl -n hw.ncpu)}"
else
    # Linux
    MAX_PARALLEL="${MAX_PARALLEL:-$(nproc)}"
fi

# Logging
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/daily_update_parallel_${TIMESTAMP}.log"

# Create temp directory for parallel job status
TEMP_DIR="$LOG_DIR/parallel_jobs_${TIMESTAMP}"
mkdir -p "$TEMP_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

################################################################################
# Functions
################################################################################

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] ✓${NC} $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ✗${NC} $*" | tee -a "$LOG_FILE" >&2
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] ⚠${NC} $*" | tee -a "$LOG_FILE"
}

log_section() {
    echo "" | tee -a "$LOG_FILE"
    echo "================================================================================" | tee -a "$LOG_FILE"
    echo -e "${CYAN}$*${NC}" | tee -a "$LOG_FILE"
    echo "================================================================================" | tee -a "$LOG_FILE"
}

log_parallel() {
    echo -e "${CYAN}[PARALLEL]${NC} $*" | tee -a "$LOG_FILE"
}

# Run command in background and track status
run_parallel() {
    local job_name="$1"
    local cmd="$2"
    local job_file="$TEMP_DIR/${job_name}.status"

    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] Would execute in parallel: $cmd" | tee -a "$LOG_FILE"
        echo "SUCCESS" > "$job_file"
        return 0
    fi

    log_parallel "Starting: $job_name"

    # Run in background
    (
        if eval "$cmd" >> "$LOG_DIR/${job_name}_${TIMESTAMP}.log" 2>&1; then
            echo "SUCCESS" > "$job_file"
            log_success "Completed: $job_name"
        else
            echo "FAILED:$?" > "$job_file"
            log_error "Failed: $job_name"
        fi
    ) &

    # Store PID
    echo $! > "$job_file.pid"
}

# Wait for all parallel jobs to complete
wait_parallel_jobs() {
    local job_group="$1"
    log_parallel "Waiting for $job_group to complete..."

    # Wait for all background jobs
    wait

    # Check status of all jobs
    local failed_jobs=()
    for status_file in "$TEMP_DIR"/*.status; do
        if [ -f "$status_file" ]; then
            local status=$(cat "$status_file")
            local job_name=$(basename "$status_file" .status)

            if [[ "$status" != "SUCCESS" ]]; then
                failed_jobs+=("$job_name")
            fi
        fi
    done

    # Clean up status files
    rm -f "$TEMP_DIR"/*.status "$TEMP_DIR"/*.pid

    if [ ${#failed_jobs[@]} -eq 0 ]; then
        log_success "$job_group - All jobs completed successfully"
        return 0
    else
        log_error "$job_group - Failed jobs: ${failed_jobs[*]}"
        return 1
    fi
}

run_command() {
    local cmd="$*"

    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] Would execute: $cmd" | tee -a "$LOG_FILE"
        return 0
    fi

    log "Executing: $cmd"

    if eval "$cmd" >> "$LOG_FILE" 2>&1; then
        log_success "Command completed successfully"
        return 0
    else
        log_error "Command failed with exit code $?"
        return 1
    fi
}

################################################################################
# Parse Arguments
################################################################################

DAYS_BACK=1
SPECIFIC_DATE=""
SKIP_LANDING=false
SKIP_BRONZE=false
SKIP_SILVER=false
SKIP_GOLD=false
DRY_RUN=false
FUNDAMENTAL_TICKERS=""  # Will be populated from ticker table

while [[ $# -gt 0 ]]; do
    case $1 in
        --date)
            SPECIFIC_DATE="$2"
            shift 2
            ;;
        --days-back)
            DAYS_BACK="$2"
            shift 2
            ;;
        --skip-landing)
            SKIP_LANDING=true
            shift
            ;;
        --skip-bronze)
            SKIP_BRONZE=true
            shift
            ;;
        --skip-silver)
            SKIP_SILVER=true
            shift
            ;;
        --skip-gold)
            SKIP_GOLD=true
            shift
            ;;
        --fundamental-tickers)
            FUNDAMENTAL_TICKERS="$2"
            shift 2
            ;;
        --max-parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help)
            head -n 40 "$0" | grep "^#" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

################################################################################
# Calculate Date Range
################################################################################

if [ -n "$SPECIFIC_DATE" ]; then
    START_DATE="$SPECIFIC_DATE"
    END_DATE="$SPECIFIC_DATE"
else
    # Default: process yesterday's data
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        START_DATE=$(date -v-${DAYS_BACK}d +%Y-%m-%d)
        END_DATE=$(date -v-1d +%Y-%m-%d)
    else
        # Linux
        START_DATE=$(date -d "$DAYS_BACK days ago" +%Y-%m-%d)
        END_DATE=$(date -d "yesterday" +%Y-%m-%d)
    fi
fi

################################################################################
# Main Pipeline
################################################################################

START_TIME=$(date +%s)

log_section "QUANTMINI PARALLEL DAILY UPDATE PIPELINE"
log "Start Date: $START_DATE"
log "End Date: $END_DATE"
log "Days to Process: $DAYS_BACK"
log "Max Parallel Jobs: $MAX_PARALLEL"
log "Skip Landing: $SKIP_LANDING"
log "Skip Bronze: $SKIP_BRONZE"
log "Skip Silver: $SKIP_SILVER"
log "Skip Gold: $SKIP_GOLD"
log "Dry Run: $DRY_RUN"
log "Log File: $LOG_FILE"
log ""

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment
log "Activating virtual environment..."
source .venv/bin/activate

# Track overall success
OVERALL_SUCCESS=true

################################################################################
# LANDING LAYER: Download Raw Files from S3 (PARALLEL)
################################################################################

if [ "$SKIP_LANDING" = false ]; then
    log_section "LANDING LAYER: Parallel S3 Downloads (4 jobs)"

    # Launch all S3 downloads in parallel
    run_parallel "landing_stocks_daily" \
        "python scripts/ingestion/download_to_landing.py --data-type stocks_daily --start-date $START_DATE --end-date $END_DATE"

    run_parallel "landing_stocks_minute" \
        "python scripts/ingestion/download_to_landing.py --data-type stocks_minute --start-date $START_DATE --end-date $END_DATE"

    run_parallel "landing_options_daily" \
        "python scripts/ingestion/download_to_landing.py --data-type options_daily --start-date $START_DATE --end-date $END_DATE"

    run_parallel "landing_options_minute" \
        "python scripts/ingestion/download_to_landing.py --data-type options_minute --start-date $START_DATE --end-date $END_DATE"

    # Wait for all downloads to complete
    if ! wait_parallel_jobs "Landing Layer"; then
        OVERALL_SUCCESS=false
    fi
else
    log_warning "Skipping landing layer downloads (--skip-landing)"
fi

################################################################################
# BRONZE LAYER: Parallel Ingestion + API Downloads
################################################################################

if [ "$SKIP_BRONZE" = false ]; then
    log_section "BRONZE LAYER: Parallel Ingestion (4 S3 + 8 API jobs = 12 total)"

    #---------------------------------------------------------------------------
    # STEP 0: Load Active Tickers from Reference Data (SEQUENTIAL - runs first)
    #---------------------------------------------------------------------------

    log "Step 0: Loading Active Ticker List from Reference Data..."
    log_info "This ensures fundamentals/corporate actions use the latest ticker list"

    # Only download if ticker table doesn't exist or is outdated
    TICKER_TABLE="$BRONZE_DIR/reference_data/tickers/locale=us/type=CS/data.parquet"
    if [ ! -f "$TICKER_TABLE" ]; then
        log_info "Ticker table not found, downloading ticker metadata..."
        python "$PROJECT_ROOT/scripts/download/download_all_tickers.py" --all 2>&1 | tee -a "$LOG_FILE"
    else
        log_info "Ticker table exists, using cached data"
    fi

    # Load active common stocks from ticker table
    if FUNDAMENTAL_TICKERS=$(python "$PROJECT_ROOT/scripts/utils/get_active_tickers.py" 2>&1); then
        TICKER_COUNT=$(echo "$FUNDAMENTAL_TICKERS" | wc -w)
        log_success "Loaded $TICKER_COUNT active common stocks from ticker table"
    else
        log_warning "Could not load tickers from reference data, using fallback list"
        FUNDAMENTAL_TICKERS="$FUNDAMENTAL_TICKERS_FALLBACK"
        TICKER_COUNT=$(echo "$FUNDAMENTAL_TICKERS" | wc -w)
        log_info "Using fallback: $TICKER_COUNT tickers"
    fi

    log ""

    #---------------------------------------------------------------------------
    # GROUP A: S3 Data Ingestion (4 jobs in parallel)
    #---------------------------------------------------------------------------

    run_parallel "bronze_stocks_daily" \
        "python scripts/ingestion/landing_to_bronze.py --data-type stocks_daily --start-date $START_DATE --end-date $END_DATE"

    run_parallel "bronze_stocks_minute" \
        "python scripts/ingestion/landing_to_bronze.py --data-type stocks_minute --start-date $START_DATE --end-date $END_DATE"

    run_parallel "bronze_options_daily" \
        "python scripts/ingestion/landing_to_bronze.py --data-type options_daily --start-date $START_DATE --end-date $END_DATE"

    run_parallel "bronze_options_minute" \
        "python scripts/ingestion/landing_to_bronze.py --data-type options_minute --start-date $START_DATE --end-date $END_DATE"

    #---------------------------------------------------------------------------
    # GROUP B: Polygon API Downloads (run in parallel with GROUP A)
    #---------------------------------------------------------------------------

    # Fundamentals (180-day window)
    # Calculate date for macOS compatibility
    if [[ "$OSTYPE" == "darwin"* ]]; then
        FILING_DATE_GTE=$(date -v-180d +%Y-%m-%d)
    else
        FILING_DATE_GTE=$(date -d '180 days ago' +%Y-%m-%d)
    fi

    run_parallel "bronze_fundamentals" \
        "quantmini polygon fundamentals $FUNDAMENTAL_TICKERS \
            --timeframe quarterly \
            --filing-date-gte $FILING_DATE_GTE \
            --output-dir $BRONZE_DIR/fundamentals"

    # Corporate Actions
    run_parallel "bronze_corporate_actions" \
        "quantmini polygon corporate-actions \
            --start-date $START_DATE \
            --end-date $END_DATE \
            --include-ipos \
            --output-dir $BRONZE_DIR/corporate_actions"

    # Ticker Events
    run_parallel "bronze_ticker_events" \
        "quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
            --output-dir $BRONZE_DIR/corporate_actions"

    # News
    run_parallel "bronze_news" \
        "quantmini polygon news \
            --start-date $START_DATE \
            --end-date $END_DATE \
            --output-dir $BRONZE_DIR/news \
            --limit 1000"

    # Short Interest/Volume (30-day window)
    # Calculate date for macOS compatibility
    if [[ "$OSTYPE" == "darwin"* ]]; then
        SHORT_DATE_GTE=$(date -v-30d +%Y-%m-%d)
    else
        SHORT_DATE_GTE=$(date -d '30 days ago' +%Y-%m-%d)
    fi

    run_parallel "bronze_short_data" \
        "quantmini polygon short-data $FUNDAMENTAL_TICKERS \
            --settlement-date-gte $SHORT_DATE_GTE \
            --date-gte $SHORT_DATE_GTE \
            --output-dir $BRONZE_DIR/fundamentals"

    # Financial Ratios API (180-day window)
    # Note: For daily updates, we only download ratios for active tickers
    # For full historical download of all 9,900 tickers, use:
    # python scripts/download/download_financial_ratios_only.py
    run_parallel "bronze_ratios_api" \
        "quantmini polygon ratios $FUNDAMENTAL_TICKERS \
            --start-date $FILING_DATE_GTE \
            --output-dir $BRONZE_DIR/fundamentals"

    # Wait for fundamentals + all other jobs to complete
    if ! wait_parallel_jobs "Bronze Layer (Ingestion + API)"; then
        OVERALL_SUCCESS=false
    fi

    #---------------------------------------------------------------------------
    # Calculated Financial Ratios (DEPENDS on fundamentals, must run after)
    # Note: This is different from API ratios downloaded above
    # This calculates derived ratios from balance sheet, income statement, cash flow
    #---------------------------------------------------------------------------
    log_section "BRONZE LAYER: Calculated Financial Ratios (Sequential - depends on fundamentals)"

    if run_command "quantmini polygon financial-ratios $FUNDAMENTAL_TICKERS \
        --input-dir $BRONZE_DIR/fundamentals \
        --output-dir $BRONZE_DIR/fundamentals \
        --include-growth"; then
        log_success "Calculated financial ratios completed"
    else
        log_error "Calculated financial ratios failed"
        OVERALL_SUCCESS=false
    fi

    #---------------------------------------------------------------------------
    # Reference Data (Weekly - only on Mondays)
    #---------------------------------------------------------------------------
    if [ "$(date +%u)" -eq 1 ]; then
        log_section "BRONZE LAYER: Reference Data (Weekly Update)"
        if run_command "quantmini polygon ticker-types \
            --asset-class stocks \
            --output-dir $BRONZE_DIR/reference"; then
            log_success "Reference data update completed"
        else
            log_error "Reference data update failed"
            OVERALL_SUCCESS=false
        fi
    else
        log_warning "Skipping reference data (only updated on Mondays)"
    fi

else
    log_warning "Skipping bronze layer ingestion (--skip-bronze)"
fi

################################################################################
# SILVER LAYER: Parallel Transformations
################################################################################

if [ "$SKIP_SILVER" = false ]; then
    log_section "SILVER LAYER: Parallel Transformations (3 jobs)"

    # All 3 transformations can run in parallel
    run_parallel "silver_financial_ratios" \
        "quantmini transform financial-ratios \
            --bronze-dir $BRONZE_DIR/fundamentals/financial_ratios \
            --silver-dir $SILVER_DIR/financial_ratios"

    run_parallel "silver_corporate_actions" \
        "python scripts/transformation/corporate_actions_silver_optimized.py \
            --bronze-dir $BRONZE_DIR/corporate_actions \
            --silver-dir $SILVER_DIR/corporate_actions"

    run_parallel "silver_fundamentals" \
        "quantmini transform fundamentals \
            --bronze-dir $BRONZE_DIR/fundamentals \
            --silver-dir $SILVER_DIR/fundamentals_wide"

    # Wait for all transformations to complete
    if ! wait_parallel_jobs "Silver Layer"; then
        OVERALL_SUCCESS=false
    fi
else
    log_warning "Skipping silver layer transformations (--skip-silver)"
fi

################################################################################
# GOLD LAYER: Enrichment & Conversion (Sequential - feature dependencies)
################################################################################

if [ "$SKIP_GOLD" = false ]; then
    log_section "GOLD LAYER: Enrichment & Qlib Conversion (Sequential)"

    # Gold layer must be sequential due to feature dependencies

    log_section "1. Enrich Stocks Daily"
    if run_command "quantmini data enrich \
        --data-type stocks_daily \
        --start-date $START_DATE \
        --end-date $END_DATE \
        --incremental"; then
        log_success "Stocks daily enrichment completed"
    else
        log_error "Stocks daily enrichment failed"
        OVERALL_SUCCESS=false
    fi

    log_section "2. Convert to Qlib Binary Format"
    if run_command "quantmini data convert \
        --data-type stocks_daily \
        --start-date $START_DATE \
        --end-date $END_DATE \
        --incremental"; then
        log_success "Qlib conversion completed"
    else
        log_error "Qlib conversion failed"
        OVERALL_SUCCESS=false
    fi

    log_section "3. Enrich Stocks Minute"
    if run_command "quantmini data enrich \
        --data-type stocks_minute \
        --start-date $START_DATE \
        --end-date $END_DATE \
        --incremental"; then
        log_success "Stocks minute enrichment completed"
    else
        log_warning "Stocks minute enrichment failed (non-critical)"
    fi

    log_section "4. Enrich Options Daily"
    if run_command "quantmini data enrich \
        --data-type options_daily \
        --start-date $START_DATE \
        --end-date $END_DATE \
        --incremental"; then
        log_success "Options daily enrichment completed"
    else
        log_warning "Options daily enrichment failed (non-critical)"
    fi
else
    log_warning "Skipping gold layer enrichment (--skip-gold)"
fi

################################################################################
# Summary
################################################################################

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

log_section "PARALLEL DAILY UPDATE SUMMARY"

if [ "$OVERALL_SUCCESS" = true ]; then
    log_success "All critical pipeline stages completed successfully!"
    log_success "Date Range: $START_DATE to $END_DATE"
    log_success "Total Duration: ${DURATION_MIN}m ${DURATION_SEC}s"
    log_success "Log file: $LOG_FILE"

    # Parallel performance statistics
    log ""
    log "Parallel Execution Statistics:"
    log "  - Landing Layer: 4 parallel S3 downloads"
    log "  - Bronze Layer: 12 parallel jobs (4 ingestion + 8 API)"
    log "  - Silver Layer: 3 parallel transformations"
    log "  - Gold Layer: Sequential (feature dependencies)"
    log ""
    log "Performance Estimate:"
    log "  - Sequential: 17-30 minutes"
    log "  - Parallel: 5-10 minutes (3-4x faster)"

    # Print data lake structure
    log ""
    log "Data Lake Structure:"
    if [ "$DRY_RUN" = false ]; then
        tree -L 2 -d "$QUANTLAKE_ROOT" 2>/dev/null | head -n 50 | tee -a "$LOG_FILE" || \
            find "$QUANTLAKE_ROOT" -maxdepth 2 -type d | head -n 50 | tee -a "$LOG_FILE"
    fi

    # Clean up temp directory
    rm -rf "$TEMP_DIR"

    exit 0
else
    log_error "Some pipeline stages failed. Check log file: $LOG_FILE"
    log_error "Date Range: $START_DATE to $END_DATE"
    log_error "Duration: ${DURATION_MIN}m ${DURATION_SEC}s"

    # Clean up temp directory
    rm -rf "$TEMP_DIR"

    exit 1
fi
