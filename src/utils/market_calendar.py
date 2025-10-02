"""
Market Calendar Utility

Provides trading day calendar functionality using pandas_market_calendars
and Polygon API for upcoming holidays.
"""

import logging
from datetime import datetime, date, timedelta
from typing import List, Set, Optional
import pandas as pd
from pathlib import Path
import json
import requests

logger = logging.getLogger(__name__)


class MarketCalendar:
    """
    Trading day calendar using pandas_market_calendars and Polygon API

    Features:
    - Check if a date is a trading day
    - Get all trading days in a date range
    - Filter out weekends and holidays
    - Cache Polygon upcoming holidays
    """

    def __init__(
        self,
        exchange: str = 'NYSE',
        polygon_api_key: Optional[str] = None,
        cache_dir: Optional[Path] = None
    ):
        """
        Initialize market calendar

        Args:
            exchange: Exchange calendar to use (NYSE, NASDAQ, etc.)
            polygon_api_key: Polygon API key for upcoming holidays
            cache_dir: Directory to cache holiday data
        """
        try:
            import pandas_market_calendars as mcal
            self.calendar = mcal.get_calendar(exchange)
            self.has_mcal = True
        except ImportError:
            logger.warning(
                "pandas_market_calendars not installed. "
                "Will use basic weekday filtering only. "
                "Install with: pip install pandas-market-calendars"
            )
            self.has_mcal = False
            self.calendar = None

        self.exchange = exchange
        self.polygon_api_key = polygon_api_key
        self.cache_dir = cache_dir or Path.home() / '.quantmini' / 'cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Cache for trading days
        self._trading_days_cache = {}

        # Load cached holidays
        self._polygon_holidays = self._load_polygon_holidays_cache()

    def is_trading_day(self, check_date: date) -> bool:
        """
        Check if a date is a trading day

        Args:
            check_date: Date to check

        Returns:
            True if trading day, False otherwise
        """
        # Weekend check
        if check_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False

        # If pandas_market_calendars available, use it
        if self.has_mcal:
            schedule = self.calendar.schedule(
                start_date=check_date,
                end_date=check_date
            )
            return len(schedule) > 0

        # Fallback: check against Polygon holidays
        if check_date.isoformat() in self._polygon_holidays:
            return False

        # Default: assume trading day if not weekend and not in holiday list
        return True

    def get_trading_days(
        self,
        start_date: date,
        end_date: date
    ) -> List[date]:
        """
        Get all trading days in a date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of trading days
        """
        # Check cache
        cache_key = f"{start_date}_{end_date}"
        if cache_key in self._trading_days_cache:
            return self._trading_days_cache[cache_key]

        if self.has_mcal:
            # Use pandas_market_calendars
            schedule = self.calendar.schedule(
                start_date=start_date,
                end_date=end_date
            )
            trading_days = [d.date() for d in schedule.index]
        else:
            # Fallback: filter weekends and known holidays
            trading_days = []
            current = start_date
            while current <= end_date:
                if self.is_trading_day(current):
                    trading_days.append(current)
                current += timedelta(days=1)

        # Cache result
        self._trading_days_cache[cache_key] = trading_days

        return trading_days

    def filter_trading_days(
        self,
        dates: List[date]
    ) -> List[date]:
        """
        Filter a list of dates to only trading days

        Args:
            dates: List of dates to filter

        Returns:
            List of trading days only
        """
        return [d for d in dates if self.is_trading_day(d)]

    def fetch_polygon_holidays(self) -> List[dict]:
        """
        Fetch upcoming holidays from Polygon API

        Returns:
            List of holiday dictionaries
        """
        if not self.polygon_api_key:
            logger.warning("No Polygon API key provided, cannot fetch holidays")
            return []

        url = "https://api.polygon.io/v1/marketstatus/upcoming"
        params = {'apiKey': self.polygon_api_key}

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'OK':
                holidays = data.get('response', [])
                self._save_polygon_holidays_cache(holidays)
                return holidays
            else:
                logger.warning(f"Polygon holidays API returned: {data.get('status')}")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch Polygon holidays: {e}")
            return []

    def _save_polygon_holidays_cache(self, holidays: List[dict]):
        """Save Polygon holidays to cache"""
        cache_file = self.cache_dir / 'polygon_holidays.json'
        try:
            cache_data = {
                'updated': datetime.now().isoformat(),
                'holidays': holidays
            }
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)

            # Update in-memory cache
            self._polygon_holidays = {
                h.get('date'): h for h in holidays if h.get('date')
            }

            logger.info(f"Cached {len(holidays)} Polygon holidays to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to cache Polygon holidays: {e}")

    def _load_polygon_holidays_cache(self) -> dict:
        """Load Polygon holidays from cache"""
        cache_file = self.cache_dir / 'polygon_holidays.json'

        if not cache_file.exists():
            return {}

        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)

            # Check if cache is recent (within 7 days)
            updated = datetime.fromisoformat(cache_data.get('updated', '2000-01-01'))
            age_days = (datetime.now() - updated).days

            if age_days > 7:
                logger.info(f"Polygon holidays cache is {age_days} days old, consider updating")

            holidays = cache_data.get('holidays', [])
            return {h.get('date'): h for h in holidays if h.get('date')}

        except Exception as e:
            logger.warning(f"Failed to load Polygon holidays cache: {e}")
            return {}

    def update_holidays_cache(self) -> int:
        """
        Update holidays cache from Polygon API

        Returns:
            Number of holidays cached
        """
        holidays = self.fetch_polygon_holidays()
        return len(holidays)


# US Stock Market holidays (static fallback)
US_MARKET_HOLIDAYS_2025 = [
    "2025-01-01",  # New Year's Day
    "2025-01-20",  # Martin Luther King Jr. Day
    "2025-02-17",  # Presidents' Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-06-19",  # Juneteenth
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-12-25",  # Christmas
]


def get_default_calendar(polygon_api_key: Optional[str] = None) -> MarketCalendar:
    """
    Get default NYSE calendar instance

    Args:
        polygon_api_key: Optional Polygon API key

    Returns:
        MarketCalendar instance
    """
    return MarketCalendar(
        exchange='NYSE',
        polygon_api_key=polygon_api_key
    )


def is_weekend(check_date: date) -> bool:
    """Quick check if date is a weekend"""
    return check_date.weekday() >= 5


def date_range_to_list(start_date: date, end_date: date) -> List[date]:
    """Convert date range to list of dates"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates
