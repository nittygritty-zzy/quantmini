#!/usr/bin/env python3
"""
Fix failed ratio calculations for tickers with schema issues

This script recalculates financial ratios for tickers that failed due to
schema concatenation issues (diagonal vs diagonal_relaxed).

Failed tickers from Batch 1:
- AAPL: Schema mismatch (structs have different field counts)
- MSFT: Schema mismatch (structs have different field counts)
- GOOGL: No ratios calculated
- BRK.B: No ratios calculated
- JPM: No ratios calculated
- CMCSA: No ratios calculated
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.download.financial_ratios_downloader import FinancialRatiosDownloader
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

FAILED_TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'BRK.B', 'JPM', 'CMCSA']


async def fix_failed_ratios():
    """Recalculate ratios for failed tickers"""

    logger.info('=' * 80)
    logger.info('Fixing failed ratio calculations')
    logger.info('=' * 80)
    logger.info(f'Tickers to fix: {", ".join(FAILED_TICKERS)}')
    logger.info('=' * 80)

    # Create downloader
    downloader = FinancialRatiosDownloader(
        input_dir=Path('data/partitioned_screener'),
        output_dir=Path('data/partitioned_screener'),
        use_partitioned_structure=True
    )

    # Calculate ratios for each ticker
    results = {}
    for ticker in FAILED_TICKERS:
        logger.info(f'\nProcessing {ticker}...')
        try:
            ratios = await downloader.calculate_ratios_for_ticker(ticker)

            if len(ratios) > 0:
                results[ticker] = len(ratios)
                logger.info(f'  ✅ {ticker}: {len(ratios)} ratio records calculated')
            else:
                results[ticker] = 0
                logger.warning(f'  ⚠️  {ticker}: No ratios calculated')

        except Exception as e:
            results[ticker] = 0
            logger.error(f'  ❌ {ticker}: Failed - {e}')

    # Summary
    logger.info('\n' + '=' * 80)
    logger.info('SUMMARY')
    logger.info('=' * 80)

    successful = sum(1 for count in results.values() if count > 0)
    total_ratios = sum(results.values())

    for ticker, count in results.items():
        status = '✅' if count > 0 else '❌'
        logger.info(f'{status} {ticker:6s}: {count:,} ratios')

    logger.info('=' * 80)
    logger.info(f'Success rate: {successful}/{len(FAILED_TICKERS)} ({successful/len(FAILED_TICKERS)*100:.1f}%)')
    logger.info(f'Total ratios calculated: {total_ratios:,}')
    logger.info('=' * 80)

    return successful == len(FAILED_TICKERS)


if __name__ == '__main__':
    try:
        success = asyncio.run(fix_failed_ratios())
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f'Failed: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
