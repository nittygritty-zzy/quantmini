#!/usr/bin/env python3
"""
Check fundamentals download status for all universe tickers

This script provides a quick overview of:
- Which tickers have fundamentals downloaded
- Which tickers have ratios calculated
- Which tickers are missing data
- Overall completion statistics
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import polars as pl
from src.utils.data_loader import DataLoader


def main():
    print('=' * 80)
    print('FUNDAMENTALS DOWNLOAD STATUS CHECKER')
    print('=' * 80)

    # Load universe tickers
    universe_file = Path('data/universe_tickers.txt')
    if not universe_file.exists():
        print(f'❌ Universe file not found: {universe_file}')
        return 1

    with open(universe_file) as f:
        universe_tickers = set(line.strip().upper() for line in f if line.strip())

    print(f'\n📊 Universe: {len(universe_tickers)} tickers')

    # Get tickers with fundamentals
    bs_tickers = set()
    for file in Path('data/partitioned_screener/balance_sheets').rglob('ticker=*.parquet'):
        ticker = file.stem.replace('ticker=', '')
        bs_tickers.add(ticker)

    # Get tickers with ratios
    loader = DataLoader()
    ratios_tickers = set()
    try:
        ratios = loader.load_financial_ratios()
        if 'ticker' in ratios.columns:
            ratios_tickers = set(ratios['ticker'].unique().to_list())
    except Exception as e:
        print(f'⚠️  Could not load ratios: {e}')

    # Calculate missing
    missing_fundamentals = sorted(universe_tickers - bs_tickers)
    has_fundamentals = sorted(universe_tickers & bs_tickers)
    missing_ratios = sorted(bs_tickers - ratios_tickers)
    complete = sorted(universe_tickers & ratios_tickers)

    # Summary
    print(f'\n📈 SUMMARY')
    print('=' * 80)
    print(f'✅ Complete (Fundamentals + Ratios): {len(complete):3d} / {len(universe_tickers):3d} ({len(complete)/len(universe_tickers)*100:5.1f}%)')
    print(f'🟡 Has Fundamentals, Missing Ratios: {len(has_fundamentals)-len(complete):3d} / {len(universe_tickers):3d} ({(len(has_fundamentals)-len(complete))/len(universe_tickers)*100:5.1f}%)')
    print(f'🔴 Missing Fundamentals:           {len(missing_fundamentals):3d} / {len(universe_tickers):3d} ({len(missing_fundamentals)/len(universe_tickers)*100:5.1f}%)')

    # Missing fundamentals (critical)
    if missing_fundamentals:
        print(f'\n🔴 MISSING FUNDAMENTALS ({len(missing_fundamentals)} tickers):')
        print('-' * 80)
        for ticker in missing_fundamentals:
            print(f'  - {ticker}')

    # Has fundamentals but missing ratios
    universe_missing_ratios = sorted((universe_tickers & bs_tickers) - ratios_tickers)
    if universe_missing_ratios:
        print(f'\n🟡 MISSING RATIOS (in universe, {len(universe_missing_ratios)} tickers):')
        print('-' * 80)
        for ticker in universe_missing_ratios:
            print(f'  - {ticker}')

    # Extra tickers (not in universe but downloaded)
    extra_fundamentals = sorted(bs_tickers - universe_tickers)
    if extra_fundamentals:
        print(f'\n💡 EXTRA TICKERS ({len(extra_fundamentals)} tickers not in universe):')
        print('-' * 80)
        print(f'  These were downloaded but are not in universe_tickers.txt')
        print(f'  (May be historical tickers, variants, or preferred shares)')
        for ticker in extra_fundamentals[:20]:  # Show first 20
            print(f'  - {ticker}')
        if len(extra_fundamentals) > 20:
            print(f'  ... and {len(extra_fundamentals)-20} more')

    # Next steps
    print(f'\n📋 NEXT STEPS')
    print('=' * 80)
    if missing_fundamentals:
        print(f'  1. Download fundamentals for {len(missing_fundamentals)} missing tickers')
        print(f'     Command: python scripts/download_missing_fundamentals.py')
    if universe_missing_ratios:
        print(f'  2. Calculate ratios for {len(universe_missing_ratios)} tickers')
        print(f'     Command: python scripts/fix_failed_ratios.py')
    if not missing_fundamentals and not universe_missing_ratios:
        print(f'  🎉 All universe tickers are complete!')
        print(f'     Consider cleaning up {len(extra_fundamentals)} extra tickers')

    print('=' * 80)

    # Return code
    if missing_fundamentals:
        return 2  # Critical - missing fundamentals
    elif universe_missing_ratios:
        return 1  # Warning - missing ratios
    else:
        return 0  # Success - all complete


if __name__ == '__main__':
    sys.exit(main())
