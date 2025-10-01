"""
Feature Definitions

Defines all features to be calculated for each data type:
- Stock daily features (alpha factors, technical indicators)
- Stock minute features (intraday patterns)
- Options features (Greeks, moneyness, etc.)
"""

from typing import Dict, List

# Stock Daily Features
STOCK_DAILY_FEATURES = {
    # Returns
    'returns_1d': '-LN(close / LAG(close, 1) OVER w)',
    'returns_5d': '-LN(close / LAG(close, 5) OVER w)',
    'returns_20d': '-LN(close / LAG(close, 20) OVER w)',
    
    # Alpha (Qlib format)
    'alpha_daily': '-LN(close / LAG(close, 1) OVER w)',
    
    # Price features
    'price_range': 'high - low',
    'daily_return': '(close - open) / NULLIF(open, 0)',
    
    # Volume features
    'volume_ratio': '''
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY date 
                ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
            ), 
            0
        )
    ''',
    'vwap': '(volume * (high + low + close) / 3.0) / NULLIF(volume, 0)',
    
    # Volatility
    'volatility_20d': '''
        STDDEV(-LN(close / LAG(close, 1) OVER w)) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        )
    ''',
}

# Stock Minute Features
STOCK_MINUTE_FEATURES = {
    # Minute returns
    'returns_1min': '-LN(close / LAG(close, 1) OVER w)',
    'returns_5min': '-LN(close / LAG(close, 5) OVER w)',
    
    # Intraday VWAP
    'vwap_intraday': '''
        SUM(volume * (high + low + close) / 3.0) OVER w_day 
        / NULLIF(SUM(volume) OVER w_day, 0)
    ''',
    
    # Volume
    'minute_volume_ratio': '''
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp 
                ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
            ), 
            0
        )
    ''',
    
    # Spread
    'spread': '(high - low) / NULLIF(close, 0)',
}

# Options Daily Features
# Note: Ticker parsing happens first, then these features are calculated
OPTIONS_DAILY_FEATURES = {
    # Parse ticker components (done in separate step via regex)
    '_parse': {
        'underlying': "REGEXP_EXTRACT(ticker, '^O:([A-Z]+)', 1)",
        'expiration_date': "STRPTIME(REGEXP_EXTRACT(ticker, '([0-9]{6})', 1), '%y%m%d')",
        'contract_type': "REGEXP_EXTRACT(ticker, '([PC])', 1)",
        'strike_price': "CAST(REGEXP_EXTRACT(ticker, '([0-9]{8})$', 1) AS DOUBLE) / 1000.0",
    },
    
    # Options-specific features
    'moneyness': '''
        (CAST(REGEXP_EXTRACT(ticker, '([0-9]{8})$', 1) AS DOUBLE) / 1000.0) 
        / NULLIF(close, 0)
    ''',
    
    # Returns
    'returns_1d': '-LN(close / LAG(close, 1) OVER w)',
    
    # Volume features
    'volume_ratio': '''
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY ticker 
                ORDER BY date 
                ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
            ), 
            0
        )
    ''',
}

# Options Minute Features
OPTIONS_MINUTE_FEATURES = {
    # Parse ticker (same as daily)
    '_parse': OPTIONS_DAILY_FEATURES['_parse'],
    
    # Minute returns
    'returns_1min': '-LN(close / LAG(close, 1) OVER w)',
    
    # Volume
    'minute_volume_ratio': '''
        volume / NULLIF(
            AVG(volume) OVER (
                PARTITION BY ticker 
                ORDER BY timestamp 
                ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
            ), 
            0
        )
    ''',
}


def get_feature_definitions(data_type: str) -> Dict[str, str]:
    """
    Get feature definitions for data type
    
    Args:
        data_type: stocks_daily, stocks_minute, options_daily, options_minute
    
    Returns:
        Dict mapping feature name to SQL expression
    """
    mapping = {
        'stocks_daily': STOCK_DAILY_FEATURES,
        'stocks_minute': STOCK_MINUTE_FEATURES,
        'options_daily': OPTIONS_DAILY_FEATURES,
        'options_minute': OPTIONS_MINUTE_FEATURES,
    }
    
    return mapping.get(data_type, {})


def get_feature_list(data_type: str) -> List[str]:
    """
    Get list of feature names for data type
    
    Args:
        data_type: stocks_daily, stocks_minute, options_daily, options_minute
    
    Returns:
        List of feature names (excluding internal _parse)
    """
    features = get_feature_definitions(data_type)
    return [name for name in features.keys() if not name.startswith('_')]


def build_feature_sql(data_type: str, base_view: str = 'raw_data') -> str:
    """
    Build complete SQL query with all features
    
    Args:
        data_type: stocks_daily, stocks_minute, options_daily, options_minute
        base_view: Name of view/table with raw data
    
    Returns:
        SQL SELECT statement with all features
    """
    features = get_feature_definitions(data_type)
    
    # Build SELECT clause
    select_parts = ['*']  # Include all original columns
    
    # Add ticker parsing for options
    if 'options' in data_type and '_parse' in features:
        for field, expr in features['_parse'].items():
            select_parts.append(f'{expr} AS {field}')
    
    # Add computed features
    for name, expr in features.items():
        if name.startswith('_'):
            continue
        # Clean up whitespace in multi-line expressions
        clean_expr = ' '.join(expr.split())
        select_parts.append(f'{clean_expr} AS {name}')
    
    select_clause = ',\n            '.join(select_parts)
    
    # Build window clause
    if 'stocks' in data_type:
        if 'daily' in data_type:
            window = """
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
        ORDER BY symbol, date
            """
        else:  # minute
            window = """
        WINDOW w AS (PARTITION BY symbol ORDER BY timestamp),
               w_day AS (PARTITION BY symbol, date ORDER BY timestamp)
        ORDER BY symbol, date, timestamp
            """
    else:  # options
        if 'daily' in data_type:
            window = """
        WINDOW w AS (PARTITION BY ticker ORDER BY date)
        ORDER BY ticker, date
            """
        else:  # minute
            window = """
        WINDOW w AS (PARTITION BY ticker ORDER BY timestamp)
        ORDER BY ticker, timestamp
            """
    
    sql = f"""
        SELECT
            {select_clause}
        FROM {base_view}
        {window}
    """
    
    return sql
