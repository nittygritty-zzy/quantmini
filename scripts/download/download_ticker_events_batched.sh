#!/usr/bin/env bash
#
# Download ticker events for all tickers in batches
#
# Ticker events include symbol changes and rebranding events.
# This script processes tickers in batches of 100 to avoid command-line argument limits.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$PROJECT_ROOT/.venv/bin/activate"

OUTPUT_DIR="/Volumes/990EVOPLUS/quantlake/bronze/corporate_actions"
BATCH_SIZE=100
LOG_FILE="/tmp/ticker_events_full.log"

echo "=========================================================================="
echo "TICKER EVENTS DOWNLOAD (BATCHED)"
echo "=========================================================================="
echo ""
echo "Getting active ticker list..."

# Get all tickers as newline-separated list
TICKERS=$(python "$PROJECT_ROOT/scripts/utils/get_active_tickers.py" --format lines)
TICKER_COUNT=$(echo "$TICKERS" | wc -l | tr -d ' ')

echo "Total tickers: $TICKER_COUNT"
echo "Batch size: $BATCH_SIZE"
echo "Output dir: $OUTPUT_DIR"
echo "Log file: $LOG_FILE"
echo ""

# Calculate number of batches
BATCHES=$(( ($TICKER_COUNT + $BATCH_SIZE - 1) / $BATCH_SIZE ))
echo "Total batches: $BATCHES"
echo ""

# Process in batches
echo "$TICKERS" | split -l $BATCH_SIZE - /tmp/ticker_batch_

BATCH_NUM=0
for batch_file in /tmp/ticker_batch_*; do
    BATCH_NUM=$((BATCH_NUM + 1))
    BATCH_TICKERS=$(cat "$batch_file" | wc -l | tr -d ' ')

    echo "[$BATCH_NUM/$BATCHES] Processing batch with $BATCH_TICKERS tickers..."

    cat "$batch_file" | xargs quantmini polygon ticker-events \
        --output-dir "$OUTPUT_DIR" \
        2>&1 | tee -a "$LOG_FILE"

    rm "$batch_file"

    echo ""
done

echo "=========================================================================="
echo "TICKER EVENTS DOWNLOAD COMPLETE"
echo "=========================================================================="
echo ""
echo "Check log file for details: $LOG_FILE"
echo ""
