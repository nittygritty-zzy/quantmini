#!/bin/bash
set -e

echo "================================================================================"
echo "SEQUENTIAL INGESTION OF MINUTE DATA (STOCKS & OPTIONS)"
echo "================================================================================"
echo "This script ingests minute data year-by-year to avoid memory issues"
echo ""

# Activate virtual environment
source .venv/bin/activate

# Function to ingest data for a specific year and type
ingest_year() {
    local data_type=$1
    local year=$2
    local start_date="${year}-01-01"
    local end_date="${year}-12-31"

    # Handle partial year for 2020 (starts 10-17)
    if [ "$year" = "2020" ]; then
        start_date="2020-10-17"
    fi

    # Handle current year 2025 (ends 10-17)
    if [ "$year" = "2025" ]; then
        end_date="2025-10-17"
    fi

    echo ""
    echo "================================================================================"
    echo "Ingesting ${data_type} for year ${year}"
    echo "Date range: ${start_date} to ${end_date}"
    echo "================================================================================"

    python -m src.cli.main data ingest \
        -t "${data_type}" \
        -s "${start_date}" \
        -e "${end_date}" \
        --incremental

    if [ $? -eq 0 ]; then
        echo "✅ Successfully ingested ${data_type} for ${year}"
    else
        echo "❌ Failed to ingest ${data_type} for ${year}"
        exit 1
    fi

    # Small delay between ingestions to allow system to recover
    sleep 5
}

# Years to ingest (reverse chronological order - newest first)
YEARS="2024 2023 2022 2021 2020"

# Ingest stocks_minute for all years
echo "================================================================================"
echo "PHASE 1: STOCKS MINUTE DATA"
echo "================================================================================"
for year in $YEARS; do
    ingest_year "stocks_minute" "$year"
done

echo ""
echo "================================================================================"
echo "✅ All stocks_minute years completed!"
echo "================================================================================"
echo ""
echo "================================================================================"
echo "PHASE 2: OPTIONS MINUTE DATA"
echo "================================================================================"

# Ingest options_minute for all years
for year in $YEARS; do
    ingest_year "options_minute" "$year"
done

echo ""
echo "================================================================================"
echo "✅ ALL INGESTION COMPLETE!"
echo "================================================================================"
echo ""
echo "================================================================================"
echo "PHASE 3: DATA ENRICHMENT (FEATURE ENGINEERING)"
echo "================================================================================"

# Enrich stocks_minute
echo ""
echo "Enriching stocks_minute..."
python scripts/enrich_features.py \
    --data-type stocks_minute \
    --start-date 2020-10-17 \
    --end-date 2025-10-17 \
    --sequential

if [ $? -eq 0 ]; then
    echo "✅ Successfully enriched stocks_minute"
else
    echo "❌ Failed to enrich stocks_minute"
    exit 1
fi

# Enrich options_minute
echo ""
echo "Enriching options_minute..."
python scripts/enrich_features.py \
    --data-type options_minute \
    --start-date 2020-10-17 \
    --end-date 2025-10-17 \
    --sequential

if [ $? -eq 0 ]; then
    echo "✅ Successfully enriched options_minute"
else
    echo "❌ Failed to enrich options_minute"
    exit 1
fi

echo ""
echo "================================================================================"
echo "✅ ALL INGESTION AND ENRICHMENT COMPLETE!"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "1. Validate stocks_minute: python -m src.cli.main validate parquet -t stocks_minute"
echo "2. Validate options_minute: python -m src.cli.main validate parquet -t options_minute"
echo ""
