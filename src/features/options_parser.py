"""
Options Ticker Parser

Parse Polygon.io options ticker format to extract underlying, expiration, strike, and contract type.

Ticker Format: O:UNDERLYING[YY]MMDD[C/P]STRIKE
Example: O:SPY230327P00390000
    - O: = Options prefix
    - SPY = Underlying symbol
    - 230327 = Expiration date (2023-03-27)
    - P = Put (or C for Call)
    - 00390000 = Strike price ($390.00)
"""

import re
from datetime import datetime, date
from typing import Dict, Optional, List

class OptionsTickerParser:
    """
    Parse Polygon.io options ticker format
    
    Format: O:UNDERLYING[YY]MMDD[C/P]STRIKE
    Example: O:SPY230327P00390000
    
    Components:
    - O: = Options prefix
    - SPY = Underlying symbol
    - 230327 = Expiration date (2023-03-27)
    - P = Put (or C for Call)
    - 00390000 = Strike price ($390.00, last 3 digits are decimals)
    """
    
    TICKER_PATTERN = re.compile(
        r'^O:(?P<underlying>[A-Z]+)'
        r'(?P<exp_year>\d{2})(?P<exp_month>\d{2})(?P<exp_day>\d{2})'
        r'(?P<contract_type>[PC])'
        r'(?P<strike>\d{8})$'
    )
    
    @classmethod
    def parse(cls, ticker: str) -> Optional[Dict[str, any]]:
        """
        Parse options ticker
        
        Args:
            ticker: Options ticker string (e.g., O:SPY230327P00390000)
        
        Returns:
            Dict with parsed fields or None if invalid:
            {
                'underlying': str,           # e.g., 'SPY'
                'expiration_date': date,     # e.g., date(2023, 3, 27)
                'contract_type': str,        # 'P' or 'C'
                'strike_price': float        # e.g., 390.0
            }
        """
        if not ticker:
            return None
            
        match = cls.TICKER_PATTERN.match(ticker)
        if not match:
            return None
        
        parts = match.groupdict()
        
        # Parse expiration date
        exp_date_str = f"20{parts['exp_year']}-{parts['exp_month']}-{parts['exp_day']}"
        try:
            exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d').date()
        except ValueError:
            return None
        
        # Parse strike price (8 digits, last 3 are decimals)
        strike_int = int(parts['strike'])
        strike_price = strike_int / 1000.0
        
        return {
            'underlying': parts['underlying'],
            'expiration_date': exp_date,
            'contract_type': parts['contract_type'],  # 'P' or 'C'
            'strike_price': strike_price
        }
    
    @classmethod
    def parse_batch(cls, tickers: List[str]) -> Dict[str, Dict]:
        """
        Parse multiple tickers efficiently
        
        Args:
            tickers: List of ticker strings
        
        Returns:
            Dict mapping ticker -> parsed fields
            Only includes successfully parsed tickers
        """
        results = {}
        for ticker in tickers:
            parsed = cls.parse(ticker)
            if parsed:
                results[ticker] = parsed
        return results
    
    @classmethod
    def is_valid_ticker(cls, ticker: str) -> bool:
        """Check if ticker matches options format"""
        if not ticker:
            return False
        return cls.TICKER_PATTERN.match(ticker) is not None
    
    @classmethod
    def extract_underlying(cls, ticker: str) -> Optional[str]:
        """Quick extraction of underlying symbol"""
        match = cls.TICKER_PATTERN.match(ticker)
        return match.groupdict()['underlying'] if match else None
    
    @classmethod
    def extract_expiration(cls, ticker: str) -> Optional[date]:
        """Quick extraction of expiration date"""
        parsed = cls.parse(ticker)
        return parsed['expiration_date'] if parsed else None
