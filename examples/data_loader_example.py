"""
Data Loader Examples - How to load partitioned screener data

This script demonstrates how to use the DataLoader to access all partitioned
data in the screener database.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.data_loader import DataLoader, load_table
import polars as pl


def example_1_list_tables():
    """Example 1: List all available tables"""
    print("=" * 80)
    print("Example 1: List All Available Tables")
    print("=" * 80)

    loader = DataLoader()
    tables = loader.list_tables()

    print(f"\nFound {len(tables)} tables:\n")
    for table in tables:
        info = loader.get_table_info(table)
        print(f"  {table:20s} - {info.get('file_count', 0):5d} files, "
              f"{info.get('ticker_count', 0):5d} tickers, "
              f"{info.get('total_size_mb', 0):8.2f} MB")

    print("\n")


def example_2_load_financial_ratios():
    """Example 2: Load financial ratios for specific tickers"""
    print("=" * 80)
    print("Example 2: Load Financial Ratios for AAPL")
    print("=" * 80)

    loader = DataLoader()

    # Load financial ratios for AAPL
    ratios = loader.load_financial_ratios(tickers=['AAPL'])

    print(f"\nLoaded {len(ratios)} ratio records\n")

    if len(ratios) > 0:
        # Show recent ratios
        recent = ratios.sort('filing_date', descending=True).head(5)

        print("Recent financial ratios:")
        print(recent.select([
            'ticker', 'filing_date', 'fiscal_period',
            'return_on_equity', 'return_on_assets',
            'current_ratio', 'debt_to_equity',
            'gross_profit_margin', 'net_profit_margin'
        ]))

    print("\n")


def example_3_load_fundamentals():
    """Example 3: Load fundamentals data"""
    print("=" * 80)
    print("Example 3: Load Fundamentals for AAPL and MSFT")
    print("=" * 80)

    loader = DataLoader()

    # Load all fundamentals
    fundamentals = loader.load_fundamentals(
        tickers=['AAPL', 'MSFT'],
        start_date='2024-01-01'
    )

    print("\nFundamentals loaded:")
    for name, df in fundamentals.items():
        print(f"  {name:20s}: {len(df):4d} records")

    # Show sample balance sheet data
    if len(fundamentals['balance_sheets']) > 0:
        print("\nSample balance sheet data:")
        bs = fundamentals['balance_sheets']
        # Note: Polygon data is nested, so we show metadata columns
        print(bs.select([
            'tickers', 'filing_date', 'fiscal_period', 'fiscal_year'
        ]).head(3))

    print("\n")


def example_4_date_filtering():
    """Example 4: Load data with date filtering"""
    print("=" * 80)
    print("Example 4: Load 2024 Financial Ratios")
    print("=" * 80)

    loader = DataLoader()

    # Load ratios for 2024 only
    ratios_2024 = loader.load_financial_ratios(
        start_date='2024-01-01',
        end_date='2024-12-31'
    )

    print(f"\nLoaded {len(ratios_2024)} ratio records for 2024\n")

    if len(ratios_2024) > 0:
        # Group by ticker and count
        summary = (ratios_2024
                   .group_by('ticker')
                   .agg(pl.count('filing_date').alias('count'))
                   .sort('count', descending=True))

        print("Ratios by ticker:")
        print(summary)

    print("\n")


def example_5_custom_filtering():
    """Example 5: Load data with custom filter expressions"""
    print("=" * 80)
    print("Example 5: Find High-ROE Companies (ROE > 40%)")
    print("=" * 80)

    loader = DataLoader()

    # Load ratios with high ROE
    high_roe = loader.load(
        'financial_ratios',
        filter_expr=pl.col('return_on_equity') > 40
    )

    print(f"\nFound {len(high_roe)} quarters with ROE > 40%\n")

    if len(high_roe) > 0:
        # Show top ROE periods
        top_roe = (high_roe
                   .select([
                       'ticker', 'filing_date', 'fiscal_period',
                       'return_on_equity', 'return_on_assets',
                       'net_profit_margin'
                   ])
                   .sort('return_on_equity', descending=True)
                   .head(10))

        print("Top ROE quarters:")
        print(top_roe)

    print("\n")


def example_6_specific_columns():
    """Example 6: Load only specific columns"""
    print("=" * 80)
    print("Example 6: Load Specific Ratio Columns")
    print("=" * 80)

    loader = DataLoader()

    # Load only profitability ratios
    profitability = loader.load_financial_ratios(
        tickers=['AAPL'],
        ratio_columns=[
            'ticker', 'filing_date', 'fiscal_period',
            'return_on_equity', 'return_on_assets', 'return_on_invested_capital',
            'gross_profit_margin', 'operating_profit_margin', 'net_profit_margin'
        ]
    )

    print(f"\nLoaded {len(profitability)} records with profitability ratios\n")

    if len(profitability) > 0:
        print("Recent profitability ratios:")
        print(profitability.sort('filing_date', descending=True).head(5))

    print("\n")


def example_7_convenience_function():
    """Example 7: Use convenience function for quick loading"""
    print("=" * 80)
    print("Example 7: Quick Load with Convenience Function")
    print("=" * 80)

    # Quick load using the convenience function
    ratios = load_table('financial_ratios', tickers=['AAPL', 'MSFT'])

    print(f"\nQuickly loaded {len(ratios)} records\n")

    if len(ratios) > 0:
        print("Sample data:")
        print(ratios.select([
            'ticker', 'filing_date', 'return_on_equity'
        ]).head(5))

    print("\n")


def example_8_ticker_events():
    """Example 8: Load ticker events"""
    print("=" * 80)
    print("Example 8: Load Ticker Events")
    print("=" * 80)

    loader = DataLoader()

    # Load ticker events
    events = loader.load_ticker_events()

    print(f"\nLoaded {len(events)} ticker events\n")

    if len(events) > 0:
        print("Sample ticker events:")
        print(events.head(10))

    print("\n")


def main():
    """Run all examples"""
    print("\n" + "=" * 80)
    print("QuantMini Data Loader Examples")
    print("=" * 80 + "\n")

    # Run examples
    example_1_list_tables()
    example_2_load_financial_ratios()
    example_3_load_fundamentals()
    example_4_date_filtering()
    example_5_custom_filtering()
    example_6_specific_columns()
    example_7_convenience_function()
    example_8_ticker_events()

    print("=" * 80)
    print("All examples completed!")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
