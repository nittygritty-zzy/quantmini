#!/usr/bin/env python3
"""
Feature Engineering Script

Enrich raw Parquet data with calculated features.

Usage:
    # Enrich single date
    python scripts/enrich_features.py \\
        --data-type stocks_daily \\
        --start-date 2025-09-29 \\
        --end-date 2025-09-29

    # Enrich date range
    python scripts/enrich_features.py \\
        --data-type stocks_daily \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30

    # Enrich all data types
    python scripts/enrich_features.py \\
        --data-type all \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30

    # Force re-enrichment (skip incremental check)
    python scripts/enrich_features.py \\
        --data-type stocks_daily \\
        --start-date 2025-09-29 \\
        --end-date 2025-09-29 \\
        --no-incremental

    # Process large date range sequentially (year-by-year to avoid memory issues)
    python scripts/enrich_features.py \\
        --data-type stocks_minute \\
        --start-date 2020-10-17 \\
        --end-date 2025-10-17 \\
        --sequential
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.features.feature_engineer import FeatureEngineer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Enrich raw Parquet data with calculated features',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--data-type',
        required=True,
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        help='Data type to enrich'
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--end-date',
        required=True,
        help='End date (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--no-incremental',
        action='store_true',
        help='Re-enrich all dates (skip incremental check)'
    )
    
    parser.add_argument(
        '--parquet-root',
        type=Path,
        help='Override raw Parquet root path'
    )
    
    parser.add_argument(
        '--enriched-root',
        type=Path,
        help='Override enriched output path'
    )

    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Process year-by-year to avoid memory issues (recommended for large date ranges)'
    )

    args = parser.parse_args()
    
    # Determine data types
    if args.data_type == 'all':
        data_types = [
            'stocks_daily',
            'stocks_minute',
            'options_daily',
            'options_minute'
        ]
    else:
        data_types = [args.data_type]
    
    # Initialize config first
    config = ConfigLoader()

    # Paths - use Medallion Architecture paths
    # Bronze layer = validated Parquet, Silver layer = enriched features
    parquet_root = args.parquet_root or config.get_bronze_path()
    enriched_root = args.enriched_root or config.get_silver_path()
    
    logger.info(f"Feature Engineering: {args.start_date} to {args.end_date}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Incremental: {not args.no_incremental}")
    logger.info(f"Sequential: {args.sequential}")
    logger.info(f"Raw data: {parquet_root}")
    logger.info(f"Enriched output: {enriched_root}")

    # If sequential mode, break into yearly chunks
    if args.sequential:
        from datetime import datetime
        start = datetime.strptime(args.start_date, '%Y-%m-%d')
        end = datetime.strptime(args.end_date, '%Y-%m-%d')

        year_ranges = []
        current_year = start.year
        while current_year <= end.year:
            year_start = max(start, datetime(current_year, 1, 1)).strftime('%Y-%m-%d')
            year_end = min(end, datetime(current_year, 12, 31)).strftime('%Y-%m-%d')
            year_ranges.append((year_start, year_end, current_year))
            current_year += 1

        logger.info(f"\nProcessing {len(year_ranges)} year(s) sequentially:")
        for year_start, year_end, year in year_ranges:
            logger.info(f"  - {year}: {year_start} to {year_end}")
    else:
        year_ranges = [(args.start_date, args.end_date, None)]

    # Process each data type
    all_results = {}

    for data_type in data_types:
        logger.info(f"\n{'='*70}")
        logger.info(f"Enriching: {data_type}")
        logger.info(f"{'='*70}")
        
        try:
            # Accumulate results across all years
            combined_result = {
                'dates_processed': 0,
                'records_enriched': 0,
                'features_added': [],
                'errors': []
            }

            # Process each year range
            for year_start, year_end, year in year_ranges:
                if year is not None:
                    logger.info(f"\n--- Processing {data_type} for year {year} ---")

                with FeatureEngineer(
                    parquet_root=parquet_root,
                    enriched_root=enriched_root,
                    config=config
                ) as engineer:

                    result = engineer.enrich_date_range(
                        data_type=data_type,
                        start_date=year_start,
                        end_date=year_end,
                        incremental=not args.no_incremental
                    )

                    # Accumulate results
                    combined_result['dates_processed'] += result['dates_processed']
                    combined_result['records_enriched'] += result['records_enriched']
                    if result['features_added']:
                        combined_result['features_added'] = result['features_added']
                    combined_result['errors'].extend(result['errors'])

                    if year is not None:
                        logger.info(f"✅ Year {year} complete: {result['records_enriched']:,} records")

            all_results[data_type] = combined_result

            logger.info(f"\n✅ {data_type} Complete:")
            logger.info(f"   Dates processed: {combined_result['dates_processed']}")
            logger.info(f"   Records enriched: {combined_result['records_enriched']:,}")
            logger.info(f"   Features added: {combined_result['features_added']}")
            logger.info(f"   Errors: {len(combined_result['errors'])}")
                
        except Exception as e:
            logger.error(f"❌ {data_type} failed: {e}", exc_info=True)
            all_results[data_type] = {'status': 'error', 'error': str(e)}
    
    # Final summary
    logger.info(f"\n{'='*70}")
    logger.info(f"ENRICHMENT SUMMARY")
    logger.info(f"{'='*70}")
    
    total_dates = 0
    total_records = 0
    total_errors = 0
    
    for data_type, result in all_results.items():
        if 'error' in result:
            status_icon = "❌"
            logger.info(f"{status_icon} {data_type:20} ERROR: {result['error']}")
        else:
            status_icon = "✅"
            dates = result.get('dates_processed', 0)
            records = result.get('records_enriched', 0)
            errors = len(result.get('errors', []))
            
            logger.info(
                f"{status_icon} {data_type:20} "
                f"{dates} dates, {records:,} records, {errors} errors"
            )
            
            total_dates += dates
            total_records += records
            total_errors += errors
    
    logger.info(f"\n{'─'*70}")
    logger.info(
        f"TOTAL: {total_dates} dates, {total_records:,} records, "
        f"{total_errors} errors"
    )
    
    # Exit with error if any failures
    if any('error' in r for r in all_results.values()):
        sys.exit(1)


if __name__ == '__main__':
    main()
