#!/bin/bash
################################################################################
# QuantMini Historical Data Load - MAXIMUM PERFORMANCE VERSION
#
# Optimized for:
# - Apple M4 (10 cores: 4 performance + 6 efficiency)
# - 24GB RAM
# - 1000 Mbps internet
# - Unlimited Polygon API tier
# - 3.6TB external SSD (Samsung 990 EVO PLUS)
#
# Performance Strategy:
# - Phase 1: Parallel API downloads (Corporate Actions + Fundamentals + Short Data)
#   * Dividends, Splits, IPOs, Ticker Events (2005-2025)
#   * Balance Sheets, Income, Cash Flow (2010-2025)
#   * Short Interest & Short Volume (2 years)
# - Phase 2: Parallel S3 downloads (Stocks + Options daily data, based on S3 access)
#   * Stocks daily: 2020-10-26 onwards
#   * Options daily: 2023-10-24 onwards
# - Phase 3: Parallel news downloads (3 years)
# - Phase 4: Sequential minute data ingestion (based on S3 access)
#   * Stocks minute: 2020-10-26 onwards
#   * Options minute: 2023-10-24 onwards
# - Aggressive parallelization: 8-10 concurrent jobs (Phases 1-3)
# - Sequential processing for Phase 4 (memory-safe for large datasets)
# - Real-time monitoring with progress dashboard
#
# Usage:
#   ./scripts/historical_data_load.sh [--phase N] [--skip-confirmation] [--skip-minute]
#
# Options:
#   --phase N              Run specific phase only (1, 2, 3, or 4)
#   --skip-confirmation    Skip confirmation prompts
#   --skip-minute          Skip Phase 4 (minute data) - saves ~500 GB and 10-15 hours
#   --dry-run              Show execution plan without running
################################################################################

set -e  # Exit on error
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

################################################################################
# Configuration
################################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

# Hardware configuration (M4 MacBook Air)
MAX_PARALLEL_JOBS=8  # Conservative for API calls (10 cores, leave 2 for system)
MAX_S3_DOWNLOADS=10  # Aggressive for S3 downloads (network-bound)

# Data paths - read from config/paths.yaml (ONLY source of truth)
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

BRONZE_DIR="$QUANTLAKE_ROOT/bronze"
SILVER_DIR="$QUANTLAKE_ROOT/silver"
LOG_DIR="$QUANTLAKE_ROOT/logs"

# Create log directory
mkdir -p "$LOG_DIR"

# Log file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/historical_load_${TIMESTAMP}.log"
PROGRESS_FILE="/tmp/historical_load_progress_${TIMESTAMP}.json"

# Fundamental tickers - loaded dynamically from ticker reference data
# Fallback to top 50 S&P 500 if ticker table not available
FUNDAMENTAL_TICKERS_FALLBACK="AAPL MSFT GOOGL AMZN NVDA META TSLA BRK.B JPM V UNH XOM JNJ WMT MA PG AVGO HD CVX MRK ABBV COST KO PEP LLY BAC PFE ADBE TMO CSCO MCD ACN CRM DHR AMD ABT NFLX DIS CMCSA NKE TXN VZ QCOM UNP RTX WFC ORCL PM INTC"
FUNDAMENTAL_TICKERS=""  # Will be populated after ticker reference data download

################################################################################
# Color Output
################################################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

################################################################################
# Utility Functions
################################################################################

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $*" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" | tee -a "$LOG_FILE"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $*" | tee -a "$LOG_FILE"
}

print_header() {
    echo -e "\n${BOLD}${CYAN}========================================${NC}"
    echo -e "${BOLD}${CYAN}$1${NC}"
    echo -e "${BOLD}${CYAN}========================================${NC}\n"
}

update_progress() {
    local phase=$1
    local step=$2
    local status=$3
    local message=$4

    cat > "$PROGRESS_FILE" <<EOF
{
  "phase": "$phase",
  "step": "$step",
  "status": "$status",
  "message": "$message",
  "timestamp": "$(date -Iseconds)"
}
EOF
}

################################################################################
# Pre-flight Checks
################################################################################

preflight_checks() {
    print_header "Pre-flight Checks"

    log_info "Checking system requirements..."

    # Check Python environment
    if ! command -v python &> /dev/null; then
        log_error "Python not found in virtual environment"
        exit 1
    fi
    log_success "Python environment: OK"

    # Check quantmini CLI
    if ! command -v quantmini &> /dev/null; then
        log_error "quantmini CLI not found"
        exit 1
    fi
    log_success "quantmini CLI: OK"

    # Check data lake root
    if [ ! -d "$QUANTLAKE_ROOT" ]; then
        log_error "Data lake root not found: $QUANTLAKE_ROOT"
        exit 1
    fi
    log_success "Data lake root: $QUANTLAKE_ROOT"

    # Check available storage
    AVAILABLE_GB=$(df -g "$QUANTLAKE_ROOT" | tail -1 | awk '{print $4}')
    log_info "Available storage: ${AVAILABLE_GB}GB"

    if [ "$AVAILABLE_GB" -lt 100 ]; then
        log_warn "Low storage space (${AVAILABLE_GB}GB). Minimum 100GB recommended."
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        log_success "Storage space: ${AVAILABLE_GB}GB available"
    fi

    # Check internet connectivity
    if ! ping -c 1 8.8.8.8 &> /dev/null; then
        log_error "No internet connectivity"
        exit 1
    fi
    log_success "Internet connectivity: OK"

    # Display hardware info
    log_info "Hardware: Apple M4 (10 cores, 24GB RAM)"
    log_info "Max parallel jobs: $MAX_PARALLEL_JOBS (API), $MAX_S3_DOWNLOADS (S3)"
    log_info "Log file: $LOG_FILE"

    echo ""
}

################################################################################
# Phase 1: Corporate Actions + Fundamentals (Parallel)
################################################################################

phase1_corporate_actions_fundamentals() {
    print_header "PHASE 1: Corporate Actions + Fundamentals"
    update_progress "1" "corporate_actions_fundamentals" "running" "Loading corporate actions and fundamentals in parallel"

    PHASE1_START=$(date +%s)

    # Reference Data: Download/Update Ticker Metadata FIRST
    log "Step 0: Downloading/Updating Ticker Reference Data..."
    log_info "This ensures we have the latest ticker list for fundamentals"

    # Download all ticker types (especially CS for common stocks)
    log_info "Downloading ticker metadata..."
    python "$PROJECT_ROOT/scripts/download/download_all_tickers.py" --all 2>&1 | tee -a "$LOG_FILE"

    # Load active common stocks from ticker table
    log_info "Loading active ticker list from reference data..."
    if FUNDAMENTAL_TICKERS=$(python "$PROJECT_ROOT/scripts/utils/get_active_tickers.py" 2>&1); then
        TICKER_COUNT=$(echo "$FUNDAMENTAL_TICKERS" | wc -w)
        log_success "Loaded $TICKER_COUNT active common stocks from ticker table"
    else
        log_warning "Could not load tickers from reference data, using fallback list"
        FUNDAMENTAL_TICKERS="$FUNDAMENTAL_TICKERS_FALLBACK"
        TICKER_COUNT=$(echo "$FUNDAMENTAL_TICKERS" | wc -w)
        log_info "Using fallback: $TICKER_COUNT tickers"
    fi

    # Corporate Actions: 2005-2025 (20 years) - Split into 5-year chunks
    log "Starting Corporate Actions download (2005-2025)..."

    CORP_ACTIONS_PERIODS=(
        "2005-01-01:2009-12-31"
        "2010-01-01:2014-12-31"
        "2015-01-01:2019-12-31"
        "2020-01-01:2024-12-31"
        "2025-01-01:2025-12-31"
    )

    for PERIOD in "${CORP_ACTIONS_PERIODS[@]}"; do
        START_DATE="${PERIOD%%:*}"
        END_DATE="${PERIOD##*:}"
        YEAR_RANGE="${START_DATE:0:4}-${END_DATE:0:4}"

        log_info "Corporate Actions: $YEAR_RANGE"
        quantmini polygon corporate-actions \
            --start-date "$START_DATE" \
            --end-date "$END_DATE" \
            --include-ipos \
            --output-dir "$BRONZE_DIR/corporate_actions" \
            2>&1 | tee -a "$LOG_FILE" &
    done

    # Ticker Events (symbol changes/rebranding)
    log "Starting Ticker Events download..."
    log_info "Ticker Events: $FUNDAMENTAL_TICKERS"
    quantmini polygon ticker-events $FUNDAMENTAL_TICKERS \
        --output-dir "$BRONZE_DIR/corporate_actions" \
        2>&1 | tee -a "$LOG_FILE" &

    # Fundamentals: 2010-2025 (15 years) - Download 20 tickers in parallel
    log "Starting Fundamentals download (2010-2025) - 20 tickers in parallel..."

    # Convert space-separated tickers to array
    TICKER_ARRAY=($FUNDAMENTAL_TICKERS)
    TOTAL_TICKERS=${#TICKER_ARRAY[@]}
    MAX_PARALLEL_TICKERS=20

    log_info "Downloading fundamentals for $TOTAL_TICKERS tickers (20 at a time)"

    FUND_PIDS=()
    TICKER_COUNT=0

    for TICKER in "${TICKER_ARRAY[@]}"; do
        TICKER_COUNT=$((TICKER_COUNT + 1))
        log_info "[$TICKER_COUNT/$TOTAL_TICKERS] Downloading $TICKER (all years 2010-2025)..."

        # Download all years for this ticker in one job
        quantmini polygon fundamentals "$TICKER" \
            --timeframe quarterly \
            --filing-date-gte "2010-01-01" \
            --filing-date-lt "2026-01-01" \
            --output-dir "$BRONZE_DIR/fundamentals" \
            2>&1 | tee -a "$LOG_FILE" &

        FUND_PIDS+=($!)

        # Limit to 20 concurrent ticker downloads
        if [ ${#FUND_PIDS[@]} -ge $MAX_PARALLEL_TICKERS ]; then
            wait -n  # Wait for any job to finish
            FUND_PIDS=($(jobs -p))  # Update PID list
        fi
    done

    # Wait for remaining jobs
    log_info "Waiting for remaining fundamentals downloads to complete..."
    wait

    # Financial Ratios - Using focused downloader for all 9,900 tickers
    log "Starting Financial Ratios download (2010-2025) - Focused downloader..."

    log_info "Downloading financial ratios for all tickers (batch mode, smart resume)"

    python "$PROJECT_ROOT/scripts/download/download_financial_ratios_only.py" \
        2>&1 | tee -a "$LOG_FILE" &

    wait

    # Short Interest/Volume (2 years) - Download 20 tickers in parallel
    log "Starting Short Interest/Volume download (2 years) - 20 tickers in parallel..."
    TWO_YEARS_AGO=$(date -v-2y +%Y-%m-%d 2>/dev/null || date -d '2 years ago' +%Y-%m-%d)

    log_info "Downloading short data for $TOTAL_TICKERS tickers (20 at a time)"

    SHORT_PIDS=()
    TICKER_COUNT=0

    for TICKER in "${TICKER_ARRAY[@]}"; do
        TICKER_COUNT=$((TICKER_COUNT + 1))
        log_info "[$TICKER_COUNT/$TOTAL_TICKERS] Downloading short data for $TICKER..."

        quantmini polygon short-data "$TICKER" \
            --settlement-date-gte "$TWO_YEARS_AGO" \
            --date-gte "$TWO_YEARS_AGO" \
            --output-dir "$BRONZE_DIR/fundamentals" \
            --limit 1000 \
            2>&1 | tee -a "$LOG_FILE" &

        SHORT_PIDS+=($!)

        # Limit to 20 concurrent ticker downloads
        if [ ${#SHORT_PIDS[@]} -ge $MAX_PARALLEL_TICKERS ]; then
            wait -n
            SHORT_PIDS=($(jobs -p))
        fi
    done

    # Wait for remaining jobs
    log_info "Waiting for remaining short data downloads to complete..."
    wait

    # Additional Reference Data: Ticker Details and Relationships
    log "Downloading additional ticker metadata and relationships..."

    # Ticker Details/Metadata (comprehensive company info)
    log_info "Ticker Metadata: downloading detailed company information"
    python "$PROJECT_ROOT/scripts/download/download_ticker_metadata.py" \
        --bulk-concurrency 100 \
        --detail-concurrency 200 \
        2>&1 | tee -a "$LOG_FILE" &

    # Related Companies (CS, ETF, PFD types)
    log_info "Ticker Relationships: downloading related companies"
    python "$PROJECT_ROOT/scripts/download/download_all_relationships.py" \
        --types CS,ETF,PFD \
        2>&1 | tee -a "$LOG_FILE" &

    # Wait for all Phase 1 jobs to complete
    log_info "Waiting for Phase 1 jobs to complete..."
    wait

    # Consolidate Ticker Reference Data to Silver Layer
    log ""
    log "========================================="
    log "CONSOLIDATING TICKER DATA → SILVER LAYER"
    log "========================================="
    log ""

    log_info "Consolidating ticker reference data for screening and analysis..."
    if python "$PROJECT_ROOT/scripts/transformation/consolidate_ticker_reference.py" \
        --bronze-dir "$BRONZE_DIR" \
        --silver-dir "$SILVER_DIR" \
        2>&1 | tee -a "$LOG_FILE"; then
        log_success "Ticker reference data consolidated to Silver layer"
    else
        log_error "Ticker reference consolidation failed (non-fatal)"
    fi

    PHASE1_END=$(date +%s)
    PHASE1_DURATION=$((PHASE1_END - PHASE1_START))

    log_success "Phase 1 completed in $((PHASE1_DURATION / 60))m $((PHASE1_DURATION % 60))s"
    update_progress "1" "corporate_actions_fundamentals" "completed" "Phase 1 completed"
}

################################################################################
# Phase 2: Daily Price Data from S3 (10 Years)
################################################################################

phase2_daily_price_data() {
    print_header "PHASE 2: Daily Price Data (S3)"
    update_progress "2" "daily_price_data" "running" "Downloading daily price data from S3"

    PHASE2_START=$(date +%s)

    log "Starting S3 daily price data download..."
    log_info "Stocks daily: 2020-10-26 onwards"
    log_info "Options daily: 2023-10-24 onwards"
    log_info "Using aggressive parallelization: $MAX_S3_DOWNLOADS concurrent downloads"

    # Download and ingest daily data based on available S3 access
    DOWNLOAD_PIDS=()

    # Stocks daily: 2020-10-26 to present
    STOCKS_START_YEAR=2020
    STOCKS_START_MONTH=10

    # Options daily: 2023-10-24 to present
    OPTIONS_START_YEAR=2023
    OPTIONS_START_MONTH=10

    CURRENT_YEAR=$(date +%Y)
    CURRENT_MONTH=$(date +%m)

    # Download stocks daily (2020-10 onwards)
    for YEAR in $(seq $STOCKS_START_YEAR $CURRENT_YEAR); do
        START_MONTH=01
        if [ "$YEAR" -eq "$STOCKS_START_YEAR" ]; then
            START_MONTH=$STOCKS_START_MONTH
        fi

        for MONTH in $(seq -w $START_MONTH 12); do
            # Skip future months
            if [ "$YEAR" -eq "$CURRENT_YEAR" ] && [ "$MONTH" -gt "$CURRENT_MONTH" ]; then
                continue
            fi

            log_info "Downloading stocks_daily: $YEAR-$MONTH"

            quantmini data download \
                --data-type stocks_daily \
                --start-date "${YEAR}-${MONTH}-01" \
                --end-date "${YEAR}-${MONTH}-31" \
                2>&1 | tee -a "$LOG_FILE" &

            DOWNLOAD_PIDS+=($!)

            # Limit concurrent downloads
            if [ ${#DOWNLOAD_PIDS[@]} -ge $MAX_S3_DOWNLOADS ]; then
                wait -n
            fi
        done
    done

    # Download options daily (2023-10 onwards)
    for YEAR in $(seq $OPTIONS_START_YEAR $CURRENT_YEAR); do
        START_MONTH=01
        if [ "$YEAR" -eq "$OPTIONS_START_YEAR" ]; then
            START_MONTH=$OPTIONS_START_MONTH
        fi

        for MONTH in $(seq -w $START_MONTH 12); do
            # Skip future months
            if [ "$YEAR" -eq "$CURRENT_YEAR" ] && [ "$MONTH" -gt "$CURRENT_MONTH" ]; then
                continue
            fi

            log_info "Downloading options_daily: $YEAR-$MONTH"

            quantmini data download \
                --data-type options_daily \
                --start-date "${YEAR}-${MONTH}-01" \
                --end-date "${YEAR}-${MONTH}-31" \
                2>&1 | tee -a "$LOG_FILE" &

            DOWNLOAD_PIDS+=($!)

            # Limit concurrent downloads
            if [ ${#DOWNLOAD_PIDS[@]} -ge $MAX_S3_DOWNLOADS ]; then
                wait -n
            fi
        done
    done

    log_info "Waiting for all S3 downloads to complete..."
    wait

    # Ingest to Bronze layer - Stocks Daily
    log "Starting Bronze layer ingestion for stocks_daily..."
    python "$PROJECT_ROOT/scripts/ingestion/landing_to_bronze.py" \
        --data-type stocks_daily \
        --start-date 2020-10-26 \
        --end-date $(date +%Y-%m-%d) \
        --processing-mode batch \
        2>&1 | tee -a "$LOG_FILE"

    # Ingest to Bronze layer - Options Daily
    log "Starting Bronze layer ingestion for options_daily..."
    python "$PROJECT_ROOT/scripts/ingestion/landing_to_bronze.py" \
        --data-type options_daily \
        --start-date 2023-10-24 \
        --end-date $(date +%Y-%m-%d) \
        --processing-mode batch \
        2>&1 | tee -a "$LOG_FILE"

    PHASE2_END=$(date +%s)
    PHASE2_DURATION=$((PHASE2_END - PHASE2_START))

    log_success "Phase 2 completed in $((PHASE2_DURATION / 60))m $((PHASE2_DURATION % 60))s"
    update_progress "2" "daily_price_data" "completed" "Phase 2 completed"
}

################################################################################
# Phase 3: News Data (Parallel)
################################################################################

phase3_supplemental_data() {
    print_header "PHASE 3: News Data"
    update_progress "3" "supplemental_data" "running" "Loading news data"

    PHASE3_START=$(date +%s)

    # News data (3 years) - parallel by ticker
    log "Starting News download (3 years)..."
    THREE_YEARS_AGO=$(date -v-3y +%Y-%m-%d 2>/dev/null || date -d '3 years ago' +%Y-%m-%d)

    NEWS_PIDS=()
    for TICKER in $FUNDAMENTAL_TICKERS; do
        log_info "News: $TICKER"
        quantmini polygon news "$TICKER" \
            --published-gte "$THREE_YEARS_AGO" \
            --output-dir "$BRONZE_DIR/news" \
            2>&1 | tee -a "$LOG_FILE" &

        NEWS_PIDS+=($!)

        # Limit concurrent jobs
        if [ ${#NEWS_PIDS[@]} -ge $MAX_PARALLEL_JOBS ]; then
            wait -n
        fi

        sleep 0.1  # Small delay for rate limiting politeness
    done

    # Wait for all Phase 3 jobs
    log_info "Waiting for Phase 3 jobs to complete..."
    wait

    PHASE3_END=$(date +%s)
    PHASE3_DURATION=$((PHASE3_END - PHASE3_START))

    log_success "Phase 3 completed in $((PHASE3_DURATION / 60))m $((PHASE3_DURATION % 60))s"
    update_progress "3" "supplemental_data" "completed" "Phase 3 completed"
}

################################################################################
# Phase 4: Minute Data (Sequential, Year-by-Year)
################################################################################

phase4_minute_data() {
    print_header "PHASE 4: Minute Data (Sequential Ingestion)"
    update_progress "4" "minute_data" "running" "Sequential ingestion of minute data (memory-safe)"

    PHASE4_START=$(date +%s)

    log "Starting sequential minute data ingestion (2020-2025)..."
    log_warn "This phase uses sequential processing to avoid memory issues"
    log_info "Estimated time: 10-15 hours"
    log_info "Estimated storage: ~500 GB"
    echo ""

    # Helper function to ingest data for a specific year and type
    ingest_year() {
        local data_type=$1
        local year=$2
        local start_date="${year}-01-01"
        local end_date="${year}-12-31"

        # Handle partial year for 2020 (starts 10-17)
        if [ "$year" = "2020" ]; then
            start_date="2020-10-17"
        fi

        # Handle current year 2025 (ends at current date)
        if [ "$year" = "2025" ]; then
            end_date=$(date +%Y-%m-%d)
        fi

        log ""
        log "═══════════════════════════════════════════════════════════════"
        log "Ingesting ${data_type} for year ${year}"
        log "Date range: ${start_date} to ${end_date}"
        log "═══════════════════════════════════════════════════════════════"

        python -m src.cli.main data ingest \
            -t "${data_type}" \
            -s "${start_date}" \
            -e "${end_date}" \
            --incremental \
            2>&1 | tee -a "$LOG_FILE"

        if [ ${PIPESTATUS[0]} -eq 0 ]; then
            log_success "Successfully ingested ${data_type} for ${year}"
        else
            log_error "Failed to ingest ${data_type} for ${year}"
            return 1
        fi

        # Small delay between ingestions to allow system to recover
        sleep 5
    }

    # Phase 4a: Stocks Minute Data
    log "═══════════════════════════════════════════════════════════════"
    log "PHASE 4a: STOCKS MINUTE DATA (2020-2025)"
    log "═══════════════════════════════════════════════════════════════"

    for YEAR in {2020..2025}; do
        ingest_year "stocks_minute" "$YEAR"
    done

    log_success "All stocks_minute years completed!"
    echo ""

    # Phase 4b: Options Minute Data
    log "═══════════════════════════════════════════════════════════════"
    log "PHASE 4b: OPTIONS MINUTE DATA (2020-2025)"
    log "═══════════════════════════════════════════════════════════════"

    for YEAR in {2020..2025}; do
        ingest_year "options_minute" "$YEAR"
    done

    log_success "All options_minute years completed!"
    echo ""

    PHASE4_END=$(date +%s)
    PHASE4_DURATION=$((PHASE4_END - PHASE4_START))

    log_success "Phase 4 completed in $((PHASE4_DURATION / 60))m $((PHASE4_DURATION % 60))s"
    update_progress "4" "minute_data" "completed" "Phase 4 completed"
}

################################################################################
# Monitoring Dashboard (Background)
################################################################################

start_monitoring_dashboard() {
    log_info "Starting monitoring dashboard..."

    cat > /tmp/monitor_historical_load.sh << 'MONITOR_EOF'
#!/bin/bash

PROGRESS_FILE="$1"
LOG_FILE="$2"

clear
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║         QuantMini Historical Data Load Monitor                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

while true; do
    # Move cursor to top
    tput cup 4 0

    # Show progress
    if [ -f "$PROGRESS_FILE" ]; then
        echo "📊 Current Status:"
        python3 -c "import json; data=json.load(open('$PROGRESS_FILE')); print(f\"   Phase: {data['phase']} | Step: {data['step']} | Status: {data['status']}\")"
        python3 -c "import json; data=json.load(open('$PROGRESS_FILE')); print(f\"   {data['message']}\")"
        echo ""
    fi

    # Show system stats
    echo "💻 System Stats:"
    echo "   CPU: $(ps -A -o %cpu | awk '{s+=$1} END {printf "%.1f%%", s}')"
    echo "   Memory: $(ps -A -o %mem | awk '{s+=$1} END {printf "%.1f%%", s}')"
    echo "   Active Jobs: $(jobs -r | wc -l)"
    echo ""

    # Show recent log entries
    echo "📝 Recent Activity (last 5 lines):"
    tail -5 "$LOG_FILE" | sed 's/^/   /'
    echo ""

    echo "Press Ctrl+C to exit monitor (doesn't stop load process)"

    sleep 2
done
MONITOR_EOF

    chmod +x /tmp/monitor_historical_load.sh
    /tmp/monitor_historical_load.sh "$PROGRESS_FILE" "$LOG_FILE" &
    MONITOR_PID=$!

    log_info "Monitoring dashboard started (PID: $MONITOR_PID)"
}

################################################################################
# Main Execution
################################################################################

main() {
    print_header "QuantMini Historical Data Load - Maximum Performance"

    log "Hardware: Apple M4 (10 cores, 24GB RAM, 1000 Mbps)"
    log "API Tier: Unlimited"
    log "Storage: External SSD (3.6TB)"
    log ""

    # Parse arguments
    RUN_PHASE="all"
    SKIP_CONFIRM=false
    SKIP_MINUTE=false
    DRY_RUN=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --phase)
                RUN_PHASE="$2"
                shift 2
                ;;
            --skip-confirmation)
                SKIP_CONFIRM=true
                shift
                ;;
            --skip-minute)
                SKIP_MINUTE=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            *)
                echo "Unknown option: $1"
                exit 1
                ;;
        esac
    done

    # Pre-flight checks
    preflight_checks

    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}DRY RUN MODE - Execution Plan:${NC}"
        echo "  Phase 1: Corporate Actions + Fundamentals + Short Data (parallel)"
        echo "    • Dividends, Splits, IPOs, Ticker Events (2005-2025)"
        echo "    • Balance Sheets, Income, Cash Flow (2010-2025)"
        echo "    • Short Interest & Short Volume (2 years)"
        echo "  Phase 2: Daily Price Data (S3, 10 years)"
        echo "  Phase 3: News Data (parallel, 3 years)"
        if [ "$SKIP_MINUTE" = false ]; then
            echo "  Phase 4: Minute Data (Stocks + Options, sequential, 5 years)"
        else
            echo "  Phase 4: SKIPPED (--skip-minute flag)"
        fi
        echo ""
        log "DRY RUN mode - execution plan shown"
        exit 0
    fi

    # Confirmation
    if [ "$SKIP_CONFIRM" = false ]; then
        echo -e "${YELLOW}This will download and ingest historical data:${NC}"
        echo "  • Corporate Actions: Dividends, Splits, IPOs, Ticker Events (2005-2025)"
        echo "  • Fundamentals: Balance Sheets, Income, Cash Flow (2010-2025)"
        echo "  • Short Data: Short Interest & Volume (2 years)"
        echo "  • Daily Prices: 2015-2025 (10 years, ~10GB)"
        echo "  • News: Last 3 years"
        if [ "$SKIP_MINUTE" = false ]; then
            echo "  • Minute Data: 2020-2025 (5 years, ~500GB) - SEQUENTIAL INGESTION"
        fi
        echo ""
        if [ "$SKIP_MINUTE" = false ]; then
            echo "Estimated time: 12-20 hours (includes minute data)"
            echo "Estimated storage: ~515 GB"
        else
            echo "Estimated time: 2-4 hours (minute data skipped)"
            echo "Estimated storage: ~15 GB"
        fi
        echo ""
        read -p "Continue? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log "Aborted by user"
            exit 0
        fi
    fi

    # Start monitoring dashboard
    # start_monitoring_dashboard

    TOTAL_START=$(date +%s)

    # Execute phases
    if [ "$RUN_PHASE" = "all" ] || [ "$RUN_PHASE" = "1" ]; then
        phase1_corporate_actions_fundamentals
    fi

    if [ "$RUN_PHASE" = "all" ] || [ "$RUN_PHASE" = "2" ]; then
        phase2_daily_price_data
    fi

    if [ "$RUN_PHASE" = "all" ] || [ "$RUN_PHASE" = "3" ]; then
        phase3_supplemental_data
    fi

    # Phase 4: Minute data (optional, can be skipped with --skip-minute)
    if [ "$SKIP_MINUTE" = false ]; then
        if [ "$RUN_PHASE" = "all" ] || [ "$RUN_PHASE" = "4" ]; then
            phase4_minute_data
        fi
    else
        log_warn "Phase 4 (minute data) skipped (--skip-minute flag)"
    fi

    TOTAL_END=$(date +%s)
    TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

    # Final summary
    print_header "Historical Data Load Complete!"

    log_success "Total execution time: $((TOTAL_DURATION / 3600))h $((TOTAL_DURATION % 3600 / 60))m $((TOTAL_DURATION % 60))s"
    log_info "Log file: $LOG_FILE"
    log_info "Data location: $QUANTLAKE_ROOT"

    # Kill monitoring dashboard
    # [ -n "$MONITOR_PID" ] && kill "$MONITOR_PID" 2>/dev/null || true

    echo ""
    log "Next steps:"
    log "  1. Verify data with: quantmini data query --data-type stocks_daily"
    log "  2. Transform to Silver layer: ./scripts/daily_update_parallel.sh --skip-landing --skip-bronze"
    log "  3. Convert to Gold/Qlib format for backtesting"
}

# Run main function
main "$@"
