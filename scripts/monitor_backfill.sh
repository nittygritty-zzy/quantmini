#!/bin/bash
# Monitor backfill progress

LOG_FILE="/Users/zheyuanzhao/sandisk/quantmini/backfill.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "❌ Log file not found: $LOG_FILE"
    exit 1
fi

echo "=== Backfill Progress Monitor ==="
echo "Log file: $LOG_FILE"
echo ""

# Check if process is running
if ps aux | grep "backfill_historical.py" | grep -v grep > /dev/null; then
    echo "✅ Backfill process is running"
else
    echo "⚠️  Backfill process not found"
fi

echo ""
echo "=== Latest Progress ==="
tail -20 "$LOG_FILE"

echo ""
echo "=== Summary ==="
echo "Downloads:"
grep "Downloaded.*KB" "$LOG_FILE" | wc -l | xargs echo "  Total files downloaded:"
grep "ERROR.*Download" "$LOG_FILE" | wc -l | xargs echo "  Download errors:"

echo ""
echo "Ingestion:"
grep "Complete:" "$LOG_FILE" | tail -4

echo ""
echo "=== Live tail (Ctrl-C to stop) ==="
tail -f "$LOG_FILE"
