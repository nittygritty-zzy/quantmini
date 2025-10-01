# -*- coding: utf-8 -*-
"""
Qlib Integration Example - Using QueryEngine with Qlib

This example demonstrates:
1. Using QueryEngine for fast Parquet queries (flexible, cached)
2. Using Qlib's native API for binary data access (optimized for ML)
3. Combining both approaches for different use cases

References:
- Qlib docs: http://qlib.readthedocs.io/en/latest/
- Qlib data API: http://qlib.readthedocs.io/en/latest/component/data.html

Usage:
    python examples/qlib_integration_example.py
"""

import sys
from pathlib import Path
import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config_loader import ConfigLoader
from src.query.query_engine import QueryEngine

# Qlib imports
import qlib
from qlib.data import D
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP


def example1_queryengine_flexible_queries():
    """
    Example 1: Use QueryEngine for flexible ad-hoc queries

    Best for:
    - Exploratory data analysis
    - Custom date ranges and symbol lists
    - Quick prototyping
    - Queries with caching
    """
    print("\n" + "="*70)
    print("Example 1: QueryEngine - Flexible Ad-hoc Queries")
    print("="*70)

    config = ConfigLoader()
    engine = QueryEngine(data_root=config.get_data_root(), config=config)

    # Query 1: Get basic OHLCV data
    print("\n[Query 1] Basic OHLCV data for tech stocks")
    df = engine.query_parquet(
        data_type='stocks_daily',
        symbols=['AAPL', 'GOOGL', 'MSFT', 'NVDA'],
        fields=['open', 'high', 'low', 'close', 'volume'],
        start_date='2025-09-15',
        end_date='2025-09-30'
    )
    print(f"Returned {len(df)} rows")
    print(df.head(10))

    # Query 2: Get enriched alpha factors
    print("\n[Query 2] Enriched alpha factors")
    df_alpha = engine.query_parquet(
        data_type='stocks_daily',
        symbols=['AAPL', 'TSLA'],
        fields=['close', 'volume', 'daily_return', 'alpha_daily', 'volume_ratio'],
        start_date='2025-09-01',
        end_date='2025-09-30'
    )
    print(f"Returned {len(df_alpha)} rows")
    print(df_alpha.head())

    # Show alpha factor statistics
    print("\n[Alpha Factor Statistics]")
    print(f"Alpha Daily - Mean: {df_alpha['alpha_daily'].mean():.6f}")
    print(f"Alpha Daily - Std:  {df_alpha['alpha_daily'].std():.6f}")
    print(f"Volume Ratio - Mean: {df_alpha['volume_ratio'].mean():.2f}")

    # Cache stats
    stats = engine.get_cache_stats()
    print(f"\n[Cache Stats] Hits: {stats['hits']}, Misses: {stats['misses']}, "
          f"Hit Rate: {stats['hit_rate']:.1%}")

    engine.close()
    return df_alpha


def example2_qlib_native_api():
    """
    Example 2: Use Qlib's native API for binary data access

    Best for:
    - Machine learning pipelines
    - High-performance backtesting
    - Using Qlib's built-in models and strategies
    - Expression-based feature engineering
    """
    print("\n" + "="*70)
    print("Example 2: Qlib Native API - Binary Data Access")
    print("="*70)

    # Initialize Qlib with our converted binary data
    config = ConfigLoader()
    qlib_data_path = config.get_data_root() / 'qlib' / 'stocks_daily'

    print(f"\n[Initializing Qlib]")
    print(f"Provider URI: {qlib_data_path}")

    qlib.init(provider_uri=str(qlib_data_path), region='us')

    # Method 1: Direct feature access with D.features()
    print("\n[Method 1] D.features() - Direct feature access")

    instruments = ['AAPL', 'GOOGL', 'MSFT']
    fields = ['$close', '$volume', '$high', '$low']

    df = D.features(
        instruments=instruments,
        fields=fields,
        start_time='2025-09-01',
        end_time='2025-09-30'
    )

    print(f"Returned data shape: {df.shape}")
    print(df.head(10))

    # Method 2: Using Qlib expressions for feature engineering
    print("\n[Method 2] Qlib Expressions - Feature Engineering")

    # Qlib supports powerful expression-based features
    # See: http://qlib.readthedocs.io/en/latest/component/data.html#feature
    expressions = [
        '$close',
        '$volume',
        'Ref($close, 1)',  # Previous day close
        '($close - Ref($close, 1)) / Ref($close, 1)',  # Daily return
        'Mean($volume, 5)',  # 5-day average volume
        '($close - Mean($close, 20)) / Std($close, 20)',  # 20-day z-score
    ]

    df_expr = D.features(
        instruments=['AAPL'],
        fields=expressions,
        start_time='2025-09-01',
        end_time='2025-09-30'
    )

    print(f"Expression-based features shape: {df_expr.shape}")
    print(df_expr.head(10))

    # Method 3: Using DataHandler for ML pipelines
    print("\n[Method 3] DataHandler - ML Pipeline Integration")

    # DataHandler provides train/valid/test splits
    # See: http://qlib.readthedocs.io/en/latest/component/data.html#data-handler
    handler_config = {
        "start_time": "2025-09-01",
        "end_time": "2025-09-30",
        "fit_start_time": "2025-09-01",
        "fit_end_time": "2025-09-20",
        "instruments": "AAPL",
        "infer_processors": [
            {"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature", "clip_outlier": True}},
            {"class": "Fillna", "kwargs": {"fields_group": "feature"}},
        ],
        "learn_processors": [
            {"class": "DropnaLabel"},
        ],
        "label": ["Ref($close, -1) / $close - 1"],  # Next day return as label
    }

    # Note: This requires Alpha158 or custom features
    # For demo, we'll show the config structure
    print("DataHandler config:")
    print(f"  Instruments: {handler_config['instruments']}")
    print(f"  Time range: {handler_config['start_time']} to {handler_config['end_time']}")
    print(f"  Label: {handler_config['label']}")

    return df_expr


def example3_combined_workflow():
    """
    Example 3: Combined workflow - QueryEngine + Qlib

    Use Case:
    - Use QueryEngine for data exploration and custom alpha research
    - Use Qlib for production ML pipelines and backtesting
    """
    print("\n" + "="*70)
    print("Example 3: Combined Workflow - Research to Production")
    print("="*70)

    # Step 1: Research phase - Use QueryEngine for exploration
    print("\n[Step 1: Research] QueryEngine for exploration")

    config = ConfigLoader()
    engine = QueryEngine(data_root=config.get_data_root(), config=config)

    # Explore correlations between features
    df_research = engine.query_parquet(
        data_type='stocks_daily',
        symbols=['AAPL', 'MSFT', 'GOOGL'],
        fields=['close', 'volume', 'daily_return', 'alpha_daily', 'volume_ratio'],
        start_date='2025-09-01',
        end_date='2025-09-30'
    )

    print(f"Research data: {len(df_research)} rows")

    # Calculate feature correlations
    corr_matrix = df_research[['daily_return', 'alpha_daily', 'volume_ratio']].corr()
    print("\n[Feature Correlations]")
    print(corr_matrix)

    # Step 2: Production phase - Use Qlib for ML pipeline
    print("\n[Step 2: Production] Qlib for ML pipeline")

    qlib_data_path = config.get_data_root() / 'qlib' / 'stocks_daily'
    qlib.init(provider_uri=str(qlib_data_path), region='us')

    # Use discovered features in Qlib expressions
    production_features = [
        '$close',
        '$volume',
        '$daily_return',  # From enriched data
        '$alpha_daily',   # From enriched data
        'Mean($volume, 5)',
        '($close - Mean($close, 20)) / Std($close, 20)',
    ]

    df_prod = D.features(
        instruments=['AAPL', 'MSFT', 'GOOGL'],
        fields=production_features,
        start_time='2025-09-01',
        end_time='2025-09-30'
    )

    print(f"Production data shape: {df_prod.shape}")
    print(df_prod.head())

    engine.close()

    return df_research, df_prod


def example4_performance_comparison():
    """
    Example 4: Performance comparison - QueryEngine vs Qlib

    Shows when to use each approach
    """
    print("\n" + "="*70)
    print("Example 4: Performance Comparison")
    print("="*70)

    import time

    # QueryEngine - Good for ad-hoc queries with caching
    print("\n[QueryEngine] Ad-hoc query with caching")
    config = ConfigLoader()
    engine = QueryEngine(data_root=config.get_data_root(), config=config)

    start = time.time()
    df1 = engine.query_parquet(
        data_type='stocks_daily',
        symbols=['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA'],
        fields=['open', 'high', 'low', 'close', 'volume'],
        start_date='2025-09-01',
        end_date='2025-09-30'
    )
    time1 = time.time() - start
    print(f"First query: {time1:.4f}s ({len(df1)} rows)")

    # Second query (should hit cache)
    start = time.time()
    df2 = engine.query_parquet(
        data_type='stocks_daily',
        symbols=['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA'],
        fields=['open', 'high', 'low', 'close', 'volume'],
        start_date='2025-09-01',
        end_date='2025-09-30'
    )
    time2 = time.time() - start
    print(f"Cached query: {time2:.4f}s ({len(df2)} rows)")
    print(f"Speedup: {time1/time2:.1f}x faster")

    # Qlib - Good for repeated ML pipeline access
    print("\n[Qlib] Repeated ML pipeline access")
    qlib_data_path = config.get_data_root() / 'qlib' / 'stocks_daily'
    qlib.init(provider_uri=str(qlib_data_path), region='us')

    start = time.time()
    df3 = D.features(
        instruments=['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSLA'],
        fields=['$close', '$volume', '$high', '$low'],
        start_time='2025-09-01',
        end_time='2025-09-30'
    )
    time3 = time.time() - start
    print(f"Qlib query: {time3:.4f}s ({df3.shape[0]} rows)")

    engine.close()

    print("\n[Summary]")
    print("- Use QueryEngine for: exploration, custom queries, caching")
    print("- Use Qlib for: ML pipelines, backtesting, expression features")


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("Qlib Integration Examples")
    print("="*70)

    try:
        # Example 1: QueryEngine flexible queries
        example1_queryengine_flexible_queries()

        # Example 2: Qlib native API
        example2_qlib_native_api()

        # Example 3: Combined workflow
        example3_combined_workflow()

        # Example 4: Performance comparison
        example4_performance_comparison()

        print("\n" + "="*70)
        print("[OK] All examples completed!")
        print("="*70)

        print("\n[Next Steps]")
        print("1. Explore Qlib docs: http://qlib.readthedocs.io")
        print("2. Try Qlib's built-in models: LightGBM, XGBoost, etc.")
        print("3. Use Qlib's backtest engine for strategy evaluation")
        print("4. Integrate with your own alpha factors")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.WARNING)  # Reduce noise
    main()
