#!/usr/bin/env python3
"""
Qlib Binary Conversion Script

Convert enriched Parquet data to Qlib binary format for ML/backtesting.

Usage:
    # Convert single data type
    python scripts/convert_to_qlib.py \\
        --data-type stocks_daily \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30

    # Convert all data types
    python scripts/convert_to_qlib.py \\
        --data-type all \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30

    # Force full re-conversion (skip incremental)
    python scripts/convert_to_qlib.py \\
        --data-type stocks_daily \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30 \\
        --no-incremental

    # Validate conversion
    python scripts/convert_to_qlib.py \\
        --data-type stocks_daily \\
        --start-date 2025-08-01 \\
        --end-date 2025-09-30 \\
        --validate
"""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config_loader import ConfigLoader
from src.transform.qlib_binary_writer import QlibBinaryWriter
from src.transform.qlib_binary_validator import QlibBinaryValidator
from src.storage.metadata_manager import MetadataManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_data_type(
    data_type: str,
    start_date: str,
    end_date: str,
    enriched_root: Path,
    qlib_root: Path,
    config: ConfigLoader,
    incremental: bool = True,
    validate: bool = False
):
    """
    Convert single data type to Qlib binary

    Args:
        data_type: Data type to convert
        start_date: Start date
        end_date: End date
        enriched_root: Enriched Parquet root
        qlib_root: Qlib binary output root
        config: Config loader
        incremental: Use incremental mode
        validate: Validate conversion
    """
    logger.info(f"\n{'=' * 70}")
    logger.info(f"Converting {data_type} to Qlib binary format")
    logger.info(f"{'=' * 70}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Incremental: {incremental}")
    logger.info(f"Validate: {validate}")

    try:
        # Initialize metadata manager
        metadata_root = enriched_root.parent / 'metadata'
        metadata_manager = MetadataManager(metadata_root)

        # Initialize writer
        writer = QlibBinaryWriter(enriched_root, qlib_root, config)

        # Convert
        start_time = datetime.now()
        result = writer.convert_data_type(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            incremental=incremental,
            metadata_manager=metadata_manager if incremental else None
        )
        duration = (datetime.now() - start_time).total_seconds()

        # Log results
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Conversion completed in {duration:.1f} seconds")
        logger.info(f"{'=' * 70}")
        logger.info(f"Symbols converted: {result['symbols_converted']}")
        logger.info(f"Trading days: {result['trading_days']}")
        logger.info(f"Features written: {result['features_written']}")
        logger.info(f"Data size: {result['bytes_written'] / 1024 / 1024:.1f} MB")

        if result.get('errors'):
            logger.warning(f"Errors: {len(result['errors'])}")
            for error in result['errors'][:5]:
                logger.warning(f"  - {error['symbol']}: {error['error']}")

        # Validate if requested
        if validate:
            logger.info(f"\n{'=' * 70}")
            logger.info("Validating conversion...")
            logger.info(f"{'=' * 70}")

            validator = QlibBinaryValidator(qlib_root)
            validation = validator.validate_conversion(data_type)

            if validation['all_valid']:
                logger.info("✅ Validation passed")
                logger.info(f"  Symbols: {validation['symbol_count']}")
                logger.info(f"  Trading days: {validation['trading_days_count']}")
                logger.info(f"  Features checked: {validation['features_checked']}")
            else:
                logger.error("❌ Validation failed")
                if validation.get('errors'):
                    for error in validation['errors']:
                        logger.error(f"  - {error}")
                if validation.get('warnings'):
                    for warning in validation['warnings']:
                        logger.warning(f"  - {warning}")

        # Close writer
        writer.close()

        return result

    except Exception as e:
        logger.error(f"❌ Conversion failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Convert enriched Parquet to Qlib binary format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--data-type',
        required=True,
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        help='Data type to convert'
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
        help='Force full re-conversion (skip incremental)'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate conversion after completion'
    )

    parser.add_argument(
        '--enriched-root',
        type=Path,
        help='Override enriched Parquet root path'
    )

    parser.add_argument(
        '--qlib-root',
        type=Path,
        help='Override Qlib binary output path'
    )

    args = parser.parse_args()

    # Determine data types
    if args.data_type == 'all':
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']
    else:
        data_types = [args.data_type]

    # Load config
    config = ConfigLoader()

    # Set paths - use data_root from config (supports external drives)
    data_root = Path(config.get('data_root', 'data'))
    enriched_root = args.enriched_root or data_root / 'data' / 'enriched'
    qlib_root = args.qlib_root or data_root / 'data' / 'qlib'

    # Check paths exist
    if not enriched_root.exists():
        logger.error(f"❌ Enriched data path not found: {enriched_root}")
        sys.exit(1)

    logger.info(f"\n{'=' * 70}")
    logger.info("Qlib Binary Conversion")
    logger.info(f"{'=' * 70}")
    logger.info(f"Enriched root: {enriched_root}")
    logger.info(f"Qlib root: {qlib_root}")
    logger.info(f"Data types: {', '.join(data_types)}")

    # Convert each data type
    overall_start = datetime.now()
    results = {}

    for data_type in data_types:
        try:
            result = convert_data_type(
                data_type=data_type,
                start_date=args.start_date,
                end_date=args.end_date,
                enriched_root=enriched_root,
                qlib_root=qlib_root,
                config=config,
                incremental=not args.no_incremental,
                validate=args.validate
            )
            results[data_type] = result
        except Exception as e:
            logger.error(f"Failed to convert {data_type}: {e}")
            results[data_type] = {'error': str(e)}

    # Overall summary
    overall_duration = (datetime.now() - overall_start).total_seconds()

    logger.info(f"\n{'=' * 70}")
    logger.info("Overall Summary")
    logger.info(f"{'=' * 70}")
    logger.info(f"Total time: {overall_duration:.1f} seconds")

    for data_type, result in results.items():
        if 'error' in result:
            logger.error(f"  {data_type}: FAILED - {result['error']}")
        else:
            logger.info(
                f"  {data_type}: {result['symbols_converted']} symbols, "
                f"{result['features_written']} features, "
                f"{result['bytes_written'] / 1024 / 1024:.1f} MB"
            )

    logger.info(f"\n✅ Conversion complete")
    logger.info(f"Output: {qlib_root}")


if __name__ == '__main__':
    main()
