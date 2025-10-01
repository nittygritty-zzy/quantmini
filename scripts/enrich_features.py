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

    # Paths - use data_root from config (supports external drives)
    data_root = Path(config.get('data_root', 'data'))
    parquet_root = args.parquet_root or data_root / 'data' / 'parquet'
    enriched_root = args.enriched_root or data_root / 'data' / 'enriched'
    
    logger.info(f"Feature Engineering: {args.start_date} to {args.end_date}")
    logger.info(f"Data types: {data_types}")
    logger.info(f"Incremental: {not args.no_incremental}")
    logger.info(f"Raw data: {parquet_root}")
    logger.info(f"Enriched output: {enriched_root}")
    
    # Process each data type
    all_results = {}
    
    for data_type in data_types:
        logger.info(f"\n{'='*70}")
        logger.info(f"Enriching: {data_type}")
        logger.info(f"{'='*70}")
        
        try:
            with FeatureEngineer(
                parquet_root=parquet_root,
                enriched_root=enriched_root,
                config=config
            ) as engineer:
                
                result = engineer.enrich_date_range(
                    data_type=data_type,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    incremental=not args.no_incremental
                )
                
                all_results[data_type] = result
                
                logger.info(f"\n✅ {data_type} Complete:")
                logger.info(f"   Dates processed: {result['dates_processed']}")
                logger.info(f"   Records enriched: {result['records_enriched']:,}")
                logger.info(f"   Features added: {result['features_added']}")
                logger.info(f"   Errors: {len(result['errors'])}")
                
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
