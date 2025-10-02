#!/usr/bin/env python3
"""
Test Market Calendar Utility

Demonstrates calendar functionality and validates trading days.
"""

import sys
from pathlib import Path
from datetime import date

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.market_calendar import MarketCalendar, get_default_calendar


def test_calendar_basic():
    """Test basic calendar functionality"""
    print("\n" + "="*70)
    print("TEST: Basic Calendar Functionality")
    print("="*70)

    calendar = get_default_calendar()

    # Test August 2025
    print("\nAugust 2025 Trading Days:")
    trading_days = calendar.get_trading_days(
        start_date=date(2025, 8, 1),
        end_date=date(2025, 8, 31)
    )

    print(f"Total trading days: {len(trading_days)}")
    print(f"Trading days: {[d.isoformat() for d in trading_days]}")

    # Check specific dates
    test_dates = [
        (date(2025, 8, 1), "Friday"),
        (date(2025, 8, 2), "Saturday"),
        (date(2025, 8, 3), "Sunday"),
        (date(2025, 8, 4), "Monday"),
        (date(2025, 9, 1), "Monday - Labor Day"),
    ]

    print("\nSpecific Date Checks:")
    for test_date, description in test_dates:
        is_trading = calendar.is_trading_day(test_date)
        status = "✓ Trading Day" if is_trading else "✗ Not Trading"
        print(f"  {test_date.isoformat()} ({description}): {status}")


def test_september_2025():
    """Test September 2025 (the data we have)"""
    print("\n" + "="*70)
    print("TEST: September 2025 Analysis")
    print("="*70)

    calendar = get_default_calendar()

    # Get September trading days
    trading_days = calendar.get_trading_days(
        start_date=date(2025, 9, 1),
        end_date=date(2025, 9, 30)
    )

    print(f"\nSeptember 2025 Trading Days: {len(trading_days)}")

    # Group by week
    from collections import defaultdict
    by_week = defaultdict(list)

    for d in trading_days:
        week_num = d.isocalendar()[1]
        by_week[week_num].append(d)

    for week, days in sorted(by_week.items()):
        print(f"\n  Week {week}:")
        for d in days:
            print(f"    {d.isoformat()} ({d.strftime('%A')})")

    # Show non-trading days
    all_days = [date(2025, 9, d) for d in range(1, 31)]
    non_trading = [d for d in all_days if not calendar.is_trading_day(d)]

    print(f"\nNon-Trading Days in September: {len(non_trading)}")
    for d in non_trading:
        reason = "Weekend" if d.weekday() >= 5 else "Holiday (Labor Day)" if d.day == 1 else "Unknown"
        print(f"  {d.isoformat()} ({d.strftime('%A')}): {reason}")


def test_date_range_filtering():
    """Test filtering dates"""
    print("\n" + "="*70)
    print("TEST: Date Range Filtering")
    print("="*70)

    calendar = get_default_calendar()

    # All days in a week
    all_days = [date(2025, 9, d) for d in range(1, 8)]  # Sep 1-7

    print(f"\nAll dates Sep 1-7: {[d.isoformat() for d in all_days]}")

    # Filter to trading days
    trading_only = calendar.filter_trading_days(all_days)

    print(f"Trading days only: {[d.isoformat() for d in trading_only]}")
    print(f"Filtered out: {len(all_days) - len(trading_only)} non-trading days")


def test_with_polygon_api():
    """Test Polygon API integration (if API key available)"""
    print("\n" + "="*70)
    print("TEST: Polygon API Integration")
    print("="*70)

    try:
        from src.core.config_loader import ConfigLoader
        config = ConfigLoader()
        polygon_key = config.get('credentials.polygon_api_key')

        if not polygon_key:
            print("\n⚠️  No Polygon API key found in credentials.yaml")
            print("   Skipping Polygon API test")
            return

        calendar = MarketCalendar(polygon_api_key=polygon_key)

        print("\nFetching upcoming holidays from Polygon API...")
        holidays = calendar.fetch_polygon_holidays()

        if holidays:
            print(f"✓ Fetched {len(holidays)} upcoming holidays")
            print("\nUpcoming Market Holidays:")
            for holiday in holidays[:10]:  # Show first 10
                print(f"  {holiday.get('date')}: {holiday.get('name')}")
                print(f"    Exchange: {holiday.get('exchange')}")
                print(f"    Status: {holiday.get('status')}")
        else:
            print("✗ No holidays returned from API")

    except Exception as e:
        print(f"✗ Polygon API test failed: {e}")


def test_comparison_with_actual_data():
    """Compare calendar with actual data we have"""
    print("\n" + "="*70)
    print("TEST: Compare With Actual Data")
    print("="*70)

    calendar = get_default_calendar()

    # We know we have 41 trading days from Aug 1 - Sep 29
    actual_trading_days = 41

    calendar_trading_days = calendar.get_trading_days(
        start_date=date(2025, 8, 1),
        end_date=date(2025, 9, 29)
    )

    print(f"\nActual data: {actual_trading_days} trading days")
    print(f"Calendar says: {len(calendar_trading_days)} trading days")

    if len(calendar_trading_days) == actual_trading_days:
        print("✓ Calendar matches actual data!")
    else:
        print(f"✗ Mismatch: {abs(len(calendar_trading_days) - actual_trading_days)} day difference")


def main():
    """Run all calendar tests"""
    print("\n" + "="*70)
    print("MARKET CALENDAR TEST SUITE")
    print("="*70)

    tests = [
        test_calendar_basic,
        test_september_2025,
        test_date_range_filtering,
        test_comparison_with_actual_data,
        test_with_polygon_api,
    ]

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"\n❌ Test {test.__name__} failed: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*70)
    print("TESTS COMPLETE")
    print("="*70)


if __name__ == '__main__':
    main()
