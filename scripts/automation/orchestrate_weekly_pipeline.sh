#!/bin/bash
# QuantMini Weekly Update Script
# Runs weekly to update delisted stocks and perform maintenance tasks

set -e  # Exit on error

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log file with date
LOG_FILE="$LOG_DIR/weekly_$(date +%Y%m%d_%H%M%S).log"

echo "========================================" | tee -a "$LOG_FILE"
echo "QuantMini Weekly Update" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Change to project directory
cd "$PROJECT_DIR"

# Activate virtual environment
source .venv/bin/activate

# Calculate date range (last 90 days)
# This covers recent delistings that might have been missed
END_DATE=$(date +%Y-%m-%d)
START_DATE=$(date -v-90d +%Y-%m-%d)  # macOS date command

echo "" | tee -a "$LOG_FILE"
echo "Date range: $START_DATE to $END_DATE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Download delisted stocks
echo "Downloading delisted stocks..." | tee -a "$LOG_FILE"
python scripts/download_delisted_stocks.py \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    2>&1 | tee -a "$LOG_FILE"

# Convert to qlib format using existing conversion script
echo "" | tee -a "$LOG_FILE"
echo "Converting to qlib format..." | tee -a "$LOG_FILE"
python scripts/convert_to_qlib.py 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Keep only last 90 days of logs
find "$LOG_DIR" -name "weekly_*.log" -mtime +90 -delete

exit 0
