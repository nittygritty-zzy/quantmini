#!/bin/bash
# QuantMini Daily Update Script
# Runs daily pipeline to download and process latest market data

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log file with date
LOG_FILE="$LOG_DIR/daily_$(date +%Y%m%d_%H%M%S).log"

echo "========================================" | tee -a "$LOG_FILE"
echo "QuantMini Daily Update" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Change to project directory
cd "$PROJECT_DIR"

# Activate virtual environment
source .venv/bin/activate

# Run daily pipeline for all data types
echo "" | tee -a "$LOG_FILE"
echo "Running daily pipeline for stocks_daily..." | tee -a "$LOG_FILE"
python -m src.cli.main pipeline daily -t stocks_daily -d 1 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Running daily pipeline for stocks_minute..." | tee -a "$LOG_FILE"
python -m src.cli.main pipeline daily -t stocks_minute -d 1 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Running daily pipeline for options_daily..." | tee -a "$LOG_FILE"
python -m src.cli.main pipeline daily -t options_daily -d 1 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Keep only last 30 days of logs
find "$LOG_DIR" -name "daily_*.log" -mtime +30 -delete

exit 0
