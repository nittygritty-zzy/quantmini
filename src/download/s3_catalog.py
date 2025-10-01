"""
S3 Catalog - Manage S3 file paths for Polygon.io flat files

This module provides utilities to generate S3 keys for different data types
and manage symbol lists.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class S3Catalog:
    """
    Manage S3 file paths for Polygon.io flat files

    S3 Path Patterns:
    - Stock daily: us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz
    - Stock minute: us_stocks_sip/minute_aggs_v1/{year}/{month}/{date}.csv.gz
    - Options daily: us_options_opra/day_aggs_v1/{year}/{month}/{date}.csv.gz
    - Options minute: us_options_opra/minute_aggs_v1/{year}/{month}/{date}.csv.gz
    """

    # S3 path prefixes
    STOCKS_DAILY_PREFIX = "us_stocks_sip/day_aggs_v1"
    STOCKS_MINUTE_PREFIX = "us_stocks_sip/minute_aggs_v1"
    OPTIONS_DAILY_PREFIX = "us_options_opra/day_aggs_v1"
    OPTIONS_MINUTE_PREFIX = "us_options_opra/minute_aggs_v1"

    def __init__(self, bucket: str = 'flatfiles'):
        """
        Initialize S3 catalog

        Args:
            bucket: S3 bucket name (default: 'flatfiles')
        """
        self.bucket = bucket

    def get_stocks_daily_key(self, date: str) -> str:
        """
        Get S3 key for stock daily aggregates

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            S3 key path

        Example:
            >>> catalog.get_stocks_daily_key('2025-09-29')
            'us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz'
        """
        dt = pd.Timestamp(date)
        year = dt.year
        month = f"{dt.month:02d}"

        return f"{self.STOCKS_DAILY_PREFIX}/{year}/{month}/{date}.csv.gz"

    def get_stocks_minute_key(self, date: str) -> str:
        """
        Get S3 key for stock minute aggregates

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            S3 key path

        Example:
            >>> catalog.get_stocks_minute_key('2025-09-29')
            'us_stocks_sip/minute_aggs_v1/2025/09/2025-09-29.csv.gz'
        """
        dt = pd.Timestamp(date)
        year = dt.year
        month = f"{dt.month:02d}"

        return f"{self.STOCKS_MINUTE_PREFIX}/{year}/{month}/{date}.csv.gz"

    def get_options_daily_key(self, date: str) -> str:
        """
        Get S3 key for options daily aggregates

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            S3 key path

        Example:
            >>> catalog.get_options_daily_key('2025-09-29')
            'us_options_opra/day_aggs_v1/2025/09/2025-09-29.csv.gz'
        """
        dt = pd.Timestamp(date)
        year = dt.year
        month = f"{dt.month:02d}"

        return f"{self.OPTIONS_DAILY_PREFIX}/{year}/{month}/{date}.csv.gz"

    def get_options_minute_key(self, date: str) -> str:
        """
        Get S3 key for options minute aggregates

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            S3 key path

        Example:
            >>> catalog.get_options_minute_key('2025-09-29')
            'us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz'
        """
        dt = pd.Timestamp(date)
        year = dt.year
        month = f"{dt.month:02d}"

        return f"{self.OPTIONS_MINUTE_PREFIX}/{year}/{month}/{date}.csv.gz"

    def get_date_range_keys(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None
    ) -> List[str]:
        """
        Get S3 keys for a date range

        Args:
            data_type: Data type ('stocks_daily', 'stocks_minute', 'options_daily', 'options_minute')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            symbols: List of symbols (required for stocks_minute, options_daily)

        Returns:
            List of S3 keys

        Example:
            >>> catalog.get_date_range_keys('stocks_daily', '2025-09-01', '2025-09-03')
            ['us_stocks_sip/day_aggs_v1/2025/09/2025-09-01.csv.gz',
             'us_stocks_sip/day_aggs_v1/2025/09/2025-09-02.csv.gz',
             'us_stocks_sip/day_aggs_v1/2025/09/2025-09-03.csv.gz']
        """
        # Generate business days
        dates = pd.bdate_range(start=start_date, end=end_date)
        keys = []

        for date in dates:
            date_str = date.strftime('%Y-%m-%d')

            if data_type == 'stocks_daily':
                keys.append(self.get_stocks_daily_key(date_str))

            elif data_type == 'stocks_minute':
                keys.append(self.get_stocks_minute_key(date_str))

            elif data_type == 'options_daily':
                keys.append(self.get_options_daily_key(date_str))

            elif data_type == 'options_minute':
                keys.append(self.get_options_minute_key(date_str))

            else:
                raise ValueError(f"Invalid data_type: {data_type}")

        return keys

    def parse_key_metadata(self, key: str) -> Dict[str, str]:
        """
        Parse metadata from S3 key

        Args:
            key: S3 key path

        Returns:
            Dictionary with metadata

        Example:
            >>> catalog.parse_key_metadata('us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz')
            {'data_type': 'stocks_daily', 'year': '2025', 'month': '09', 'date': '2025-09-29'}
        """
        parts = key.split('/')

        if key.startswith(self.STOCKS_DAILY_PREFIX):
            return {
                'data_type': 'stocks_daily',
                'year': parts[2],
                'month': parts[3],
                'date': parts[4].replace('.csv.gz', ''),
            }

        elif key.startswith(self.STOCKS_MINUTE_PREFIX):
            return {
                'data_type': 'stocks_minute',
                'year': parts[2],
                'month': parts[3],
                'date': parts[4].replace('.csv.gz', ''),
            }

        elif key.startswith(self.OPTIONS_DAILY_PREFIX):
            return {
                'data_type': 'options_daily',
                'year': parts[2],
                'month': parts[3],
                'date': parts[4].replace('.csv.gz', ''),
            }

        elif key.startswith(self.OPTIONS_MINUTE_PREFIX):
            return {
                'data_type': 'options_minute',
                'year': parts[2],
                'month': parts[3],
                'date': parts[4].replace('.csv.gz', ''),
            }

        else:
            raise ValueError(f"Unknown key pattern: {key}")

    @staticmethod
    def get_business_days(start_date: str, end_date: str) -> List[str]:
        """
        Get list of business days between dates

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of date strings

        Example:
            >>> S3Catalog.get_business_days('2025-09-26', '2025-09-30')
            ['2025-09-26', '2025-09-29', '2025-09-30']  # Excludes weekend
        """
        dates = pd.bdate_range(start=start_date, end=end_date)
        return [d.strftime('%Y-%m-%d') for d in dates]

    @staticmethod
    def get_missing_dates(
        existing_dates: List[str],
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        Get missing business days

        Args:
            existing_dates: List of dates already processed
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            List of missing dates

        Example:
            >>> S3Catalog.get_missing_dates(['2025-09-26'], '2025-09-26', '2025-09-30')
            ['2025-09-29', '2025-09-30']
        """
        all_dates = set(S3Catalog.get_business_days(start_date, end_date))
        existing = set(existing_dates)
        missing = sorted(all_dates - existing)
        return missing

    def validate_key(self, key: str) -> bool:
        """
        Validate if key matches expected pattern

        Args:
            key: S3 key to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            self.parse_key_metadata(key)
            return True
        except (ValueError, IndexError):
            return False

    def get_summary(self, keys: List[str]) -> Dict[str, int]:
        """
        Get summary statistics for a list of keys

        Args:
            keys: List of S3 keys

        Returns:
            Summary dictionary

        Example:
            >>> catalog.get_summary(keys)
            {'total': 100, 'stocks_daily': 30, 'stocks_minute': 40, ...}
        """
        summary = {'total': len(keys)}

        for key in keys:
            try:
                metadata = self.parse_key_metadata(key)
                data_type = metadata['data_type']
                summary[data_type] = summary.get(data_type, 0) + 1
            except:
                summary['invalid'] = summary.get('invalid', 0) + 1

        return summary


def main():
    """Command-line interface for S3 catalog"""
    catalog = S3Catalog()

    print("S3 Catalog Examples")
    print("=" * 70)

    # Stock daily
    key = catalog.get_stocks_daily_key('2025-09-29')
    print(f"\nStock Daily:\n  {key}")

    # Stock minute
    key = catalog.get_stocks_minute_key('2025-09-29')
    print(f"\nStock Minute:\n  {key}")

    # Options daily
    key = catalog.get_options_daily_key('2025-09-29')
    print(f"\nOptions Daily:\n  {key}")

    # Options minute
    key = catalog.get_options_minute_key('2025-09-29')
    print(f"\nOptions Minute:\n  {key}")

    # Date range
    print("\nDate Range (stocks_daily, 2025-09-26 to 2025-09-30):")
    keys = catalog.get_date_range_keys('stocks_daily', '2025-09-26', '2025-09-30')
    for k in keys:
        print(f"  {k}")

    # Parse metadata
    print(f"\nParse Metadata:")
    metadata = catalog.parse_key_metadata(keys[0])
    for k, v in metadata.items():
        print(f"  {k}: {v}")

    # Business days
    print(f"\nBusiness Days (2025-09-26 to 2025-09-30):")
    bdays = S3Catalog.get_business_days('2025-09-26', '2025-09-30')
    print(f"  {', '.join(bdays)}")

    print("\n" + "=" * 70)


if __name__ == '__main__':
    main()
