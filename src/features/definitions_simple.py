"""
Simplified Feature Definitions for Testing

Start with basic features that are known to work with DuckDB
"""

# Simplified stock daily features (no nested window functions)
STOCK_DAILY_FEATURES_SIMPLE = {
    # Returns
    'returns_1d': '-LN(close / LAG(close, 1) OVER w)',

    # Alpha (Qlib format)
    'alpha_daily': '-LN(close / LAG(close, 1) OVER w)',

    # Price features
    'price_range': 'high - low',
    'daily_return': '(close - open) / NULLIF(open, 0)',

    # Volume features
    'vwap': '(volume * (high + low + close) / 3.0) / NULLIF(volume, 0)',
}

def build_simple_stock_daily_sql(base_view: str = 'raw_data') -> str:
    """Build simple SQL for stock daily features"""
    features = STOCK_DAILY_FEATURES_SIMPLE

    select_parts = ['*']
    for name, expr in features.items():
        clean_expr = ' '.join(expr.split())
        select_parts.append(f'{clean_expr} AS {name}')

    select_clause = ',\n            '.join(select_parts)

    sql = f"""
        SELECT
            {select_clause}
        FROM {base_view}
        WINDOW w AS (PARTITION BY symbol ORDER BY date)
        ORDER BY symbol, date
    """

    return sql
