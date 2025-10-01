"""
Unit Tests for OptionsTickerParser

Test parsing of Polygon.io options ticker format.
"""

import pytest
from datetime import date

from src.features.options_parser import OptionsTickerParser


class TestOptionsTickerParser:
    """Test options ticker parsing"""
    
    def test_parse_valid_put(self):
        """Test parsing valid PUT option"""
        ticker = 'O:SPY230327P00390000'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['underlying'] == 'SPY'
        assert result['expiration_date'] == date(2023, 3, 27)
        assert result['contract_type'] == 'P'
        assert result['strike_price'] == 390.0
    
    def test_parse_valid_call(self):
        """Test parsing valid CALL option"""
        ticker = 'O:AAPL250117C00150000'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['underlying'] == 'AAPL'
        assert result['expiration_date'] == date(2025, 1, 17)
        assert result['contract_type'] == 'C'
        assert result['strike_price'] == 150.0
    
    def test_parse_high_strike(self):
        """Test parsing option with high strike price"""
        ticker = 'O:TSLA240315C00800000'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['strike_price'] == 800.0
    
    def test_parse_low_strike(self):
        """Test parsing option with low strike price"""
        ticker = 'O:F250620C00012500'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['strike_price'] == 12.5
    
    def test_parse_fractional_strike(self):
        """Test parsing option with fractional strike"""
        ticker = 'O:ORCL250117C00125750'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['strike_price'] == 125.75
    
    def test_parse_invalid_ticker(self):
        """Test parsing invalid ticker returns None"""
        invalid_tickers = [
            'SPY',  # Not options format
            'O:SPY',  # Missing components
            'O:SPY230327',  # Missing type and strike
            'O:SPY230327X00390000',  # Invalid type (X)
            'O:SPY231399P00390000',  # Invalid date (month 13)
            '',  # Empty string
            None,  # None value
        ]

        for ticker in invalid_tickers:
            result = OptionsTickerParser.parse(ticker)
            assert result is None, f"Expected None for: {ticker}"
    
    def test_parse_batch(self):
        """Test batch parsing"""
        tickers = [
            'O:SPY230327P00390000',
            'O:AAPL250117C00150000',
            'INVALID',
            'O:TSLA240315C00800000',
        ]
        
        results = OptionsTickerParser.parse_batch(tickers)
        
        # Should have 3 valid results
        assert len(results) == 3
        assert 'O:SPY230327P00390000' in results
        assert 'O:AAPL250117C00150000' in results
        assert 'O:TSLA240315C00800000' in results
        assert 'INVALID' not in results
    
    def test_is_valid_ticker(self):
        """Test ticker validation"""
        assert OptionsTickerParser.is_valid_ticker('O:SPY230327P00390000') is True
        assert OptionsTickerParser.is_valid_ticker('SPY') is False
        assert OptionsTickerParser.is_valid_ticker('') is False
        assert OptionsTickerParser.is_valid_ticker(None) is False
    
    def test_extract_underlying(self):
        """Test quick underlying extraction"""
        ticker = 'O:SPY230327P00390000'
        underlying = OptionsTickerParser.extract_underlying(ticker)
        
        assert underlying == 'SPY'
    
    def test_extract_expiration(self):
        """Test quick expiration extraction"""
        ticker = 'O:SPY230327P00390000'
        exp_date = OptionsTickerParser.extract_expiration(ticker)
        
        assert exp_date == date(2023, 3, 27)
    
    def test_multi_letter_underlying(self):
        """Test parsing tickers with multi-letter underlyings"""
        ticker = 'O:GOOG250620C00180000'
        result = OptionsTickerParser.parse(ticker)
        
        assert result is not None
        assert result['underlying'] == 'GOOG'
