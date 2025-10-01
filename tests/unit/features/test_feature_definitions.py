"""
Unit Tests for Feature Definitions

Test feature definition SQL generation.
"""

import pytest

from src.features.definitions import (
    get_feature_definitions,
    get_feature_list,
    build_feature_sql,
)


class TestFeatureDefinitions:
    """Test feature definitions"""
    
    def test_get_stock_daily_features(self):
        """Test getting stock daily features"""
        features = get_feature_definitions('stocks_daily')
        
        assert 'alpha_daily' in features
        assert 'returns_1d' in features
        assert 'volume_ratio' in features
        assert 'vwap' in features
        assert 'volatility_20d' in features
    
    def test_get_stock_minute_features(self):
        """Test getting stock minute features"""
        features = get_feature_definitions('stocks_minute')
        
        assert 'returns_1min' in features
        assert 'returns_5min' in features
        assert 'vwap_intraday' in features
        assert 'minute_volume_ratio' in features
        assert 'spread' in features
    
    def test_get_options_daily_features(self):
        """Test getting options daily features"""
        features = get_feature_definitions('options_daily')
        
        assert '_parse' in features  # Ticker parsing
        assert 'moneyness' in features
        assert 'returns_1d' in features
        assert 'volume_ratio' in features
    
    def test_get_feature_list_excludes_internal(self):
        """Test feature list excludes internal fields"""
        features = get_feature_list('options_daily')
        
        # Should not include _parse
        assert '_parse' not in features
        
        # Should include actual features
        assert 'moneyness' in features
        assert 'returns_1d' in features
    
    def test_build_stock_daily_sql(self):
        """Test building SQL for stock daily features"""
        sql = build_feature_sql('stocks_daily', base_view='test_data')
        
        # Check key components
        assert 'SELECT' in sql
        assert 'FROM test_data' in sql
        assert 'WINDOW w AS (PARTITION BY symbol ORDER BY date)' in sql
        assert 'alpha_daily' in sql
        assert 'returns_1d' in sql
        assert 'ORDER BY symbol, date' in sql
    
    def test_build_stock_minute_sql(self):
        """Test building SQL for stock minute features"""
        sql = build_feature_sql('stocks_minute', base_view='test_data')
        
        # Check for minute-specific components
        assert 'PARTITION BY symbol ORDER BY timestamp' in sql
        assert 'w_day AS (PARTITION BY symbol, date ORDER BY timestamp)' in sql
        assert 'returns_1min' in sql
        assert 'vwap_intraday' in sql
    
    def test_build_options_daily_sql(self):
        """Test building SQL for options daily features"""
        sql = build_feature_sql('options_daily', base_view='test_data')
        
        # Check for ticker parsing
        assert 'REGEXP_EXTRACT' in sql
        assert 'AS underlying' in sql
        assert 'AS strike_price' in sql
        
        # Check for window
        assert 'PARTITION BY ticker ORDER BY date' in sql
    
    def test_sql_no_syntax_errors(self):
        """Test SQL has no obvious syntax errors"""
        for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            sql = build_feature_sql(data_type)
            
            # Basic syntax checks
            assert sql.count('SELECT') >= 1
            assert sql.count('FROM') == 1
            assert sql.count('WINDOW') >= 1 or 'ORDER BY' in sql
            
            # No unbalanced parentheses
            assert sql.count('(') == sql.count(')')
