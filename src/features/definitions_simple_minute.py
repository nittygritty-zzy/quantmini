"""
Simplified Feature Definitions for Minute and Options Data

These use simpler SQL expressions to avoid DuckDB limitations with nested window functions.
"""


def build_simple_stock_minute_sql(base_view: str = 'raw_data') -> str:
    """
    Build simplified SQL for stocks_minute features

    Avoids complex window functions that cause DuckDB errors.

    Args:
        base_view: Name of view/table with raw data

    Returns:
        SQL SELECT statement
    """
    return f"""
        SELECT
            symbol,
            timestamp,
            date,
            open,
            high,
            low,
            close,
            volume,
            transactions,

            -- Simple features without complex windows
            (close - open) / NULLIF(open, 0) as minute_return,
            (high - low) / NULLIF(close, 0) as spread,
            (high + low + close) / 3.0 as typical_price,
            volume / NULLIF(transactions, 0) as avg_trade_size,

            -- VWAP approximation
            ((high + low + close) / 3.0) as vwap_approx

        FROM {base_view}
    """


def build_simple_options_daily_sql(base_view: str = 'raw_data') -> str:
    """
    Build simplified SQL for options_daily features

    Args:
        base_view: Name of view/table with raw data

    Returns:
        SQL SELECT statement
    """
    return f"""
        SELECT
            ticker,
            date,
            open,
            high,
            low,
            close,
            volume,
            transactions,

            -- Simple options features
            (close - open) / NULLIF(open, 0) as daily_return,
            (high - low) / NULLIF(close, 0) as spread,
            (high + low + close) / 3.0 as typical_price,
            volume / NULLIF(transactions, 0) as avg_trade_size

        FROM {base_view}
    """


def build_simple_options_minute_sql(base_view: str = 'raw_data') -> str:
    """
    Build simplified SQL for options_minute features

    Args:
        base_view: Name of view/table with raw data

    Returns:
        SQL SELECT statement
    """
    return f"""
        SELECT
            ticker,
            timestamp,
            date,
            open,
            high,
            low,
            close,
            volume,
            transactions,

            -- Simple minute features
            (close - open) / NULLIF(open, 0) as minute_return,
            (high - low) / NULLIF(close, 0) as spread,
            (high + low + close) / 3.0 as typical_price,
            volume / NULLIF(transactions, 0) as avg_trade_size

        FROM {base_view}
    """
