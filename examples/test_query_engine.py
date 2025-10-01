# -*- coding: utf-8 -*-
"""
Test Query Engine - Demonstrate Phase 7 Query Engine capabilities

Usage:
    python examples/test_query_engine.py
"""

import sys
from pathlib import Path
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config_loader import ConfigLoader
from src.query.query_engine import QueryEngine
from src.query.query_cache import QueryCache

logger = logging.getLogger(__name__)


def test_query_cache():
    """Test QueryCache basic functionality"""
    print("\n" + "="*70)
    print("Test 1: QueryCache Functionality")
    print("="*70)

    config = ConfigLoader()
    cache = QueryCache(cache_root=config.get_data_root() / 'cache', max_size_gb=0.1)
    print(f"[OK] Created: {cache}")

    key = cache.make_key(
        data_type='stocks_daily',
        symbols=['AAPL', 'TSLA'],
        fields=['open', 'close'],
        start_date='2025-09-01',
        end_date='2025-09-30'
    )
    print(f"   Cache key: {key[:16]}...")

    result = cache.get(key)
    print(f"   First access: {'HIT' if result is not None else 'MISS'}")

    import pandas as pd
    import numpy as np
    test_df = pd.DataFrame({
        'symbol': ['AAPL'] * 20,
        'date': pd.date_range('2025-09-01', periods=20),
        'open': np.random.rand(20) * 100,
        'close': np.random.rand(20) * 100
    })
    cache.put(key, test_df)
    print(f"   Cached test data: {len(test_df)} rows")

    result = cache.get(key)
    print(f"   Second access: {'HIT' if result is not None else 'MISS'}")

    stats = cache.get_stats()
    print(f"\n   [Stats] Cache Stats:")
    print(f"      Hits: {stats['hits']}")
    print(f"      Misses: {stats['misses']}")
    print(f"      Hit Rate: {stats['hit_rate']:.1%}")
    print(f"      Entries: {stats['entries']}")
    print(f"      Size: {stats['total_size_mb']:.2f} MB")

    print("\n[OK] QueryCache test passed!")


def test_query_engine():
    """Test QueryEngine with stocks_daily data"""
    print("\n" + "="*70)
    print("Test 2: QueryEngine with stocks_daily")
    print("="*70)

    config = ConfigLoader()
    engine = QueryEngine(data_root=config.get_data_root(), config=config, enable_cache=True)
    print(f"[OK] Created: {engine}")

    symbols = ['AAPL', 'TSLA', 'MSFT']
    fields = ['open', 'high', 'low', 'close', 'volume']

    print(f"\n   Querying {len(symbols)} symbols, {len(fields)} fields...")
    print(f"   Symbols: {symbols}")
    print(f"   Date range: 2025-09-01 to 2025-09-30")

    df1 = engine.query_parquet(
        data_type='stocks_daily',
        symbols=symbols,
        fields=fields,
        start_date='2025-09-01',
        end_date='2025-09-30'
    )

    print(f"\n   First query: {len(df1)} rows returned")
    print(f"   Sample data (first 5 rows):")
    print(df1.head())

    print(f"\n   Running same query again (should hit cache)...")
    df2 = engine.query_parquet(
        data_type='stocks_daily',
        symbols=symbols,
        fields=fields,
        start_date='2025-09-01',
        end_date='2025-09-30'
    )

    print(f"   Second query: {len(df2)} rows returned")

    stats = engine.get_cache_stats()
    if stats:
        print(f"\n   [Stats] Cache Stats:")
        print(f"      Hits: {stats['hits']}")
        print(f"      Misses: {stats['misses']}")
        print(f"      Hit Rate: {stats['hit_rate']:.1%}")

    engine.close()
    print("\n[OK] QueryEngine test passed!")


def main():
    """Run all tests"""
    try:
        print("\n" + "="*70)
        print("Phase 7: Query Engine Test Suite")
        print("="*70)

        test_query_cache()
        test_query_engine()

        print("\n" + "="*70)
        print("[OK] All tests passed!")
        print("="*70)

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()
