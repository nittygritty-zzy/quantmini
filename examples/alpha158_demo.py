#!/usr/bin/env python3
"""
Alpha158 Feature Demo

Demonstrates Alpha158 feature generation with real data.
Run this after qlib conversion is complete.
"""

import qlib
import pandas as pd
import numpy as np
from pathlib import Path
from qlib.data import D
from qlib.contrib.data.loader import Alpha158DL


def demo_kbar_features():
    """Demo: K-line candlestick features"""
    print("\n" + "="*70)
    print("1. K-LINE FEATURES")
    print("="*70)

    expressions = {
        "KMID": "($close-$open)/$open",                    # Body size
        "KLEN": "($high-$low)/$open",                      # Full range
        "KUP": "($high-Greater($open,$close))/$open",     # Upper shadow
        "KLOW": "(Less($open,$close)-$low)/$open",        # Lower shadow
    }

    df = D.features(
        instruments=['AAPL'],
        fields=list(expressions.values()),
        start_time='2025-09-01',
        end_time='2025-09-30'
    )
    df.columns = list(expressions.keys())

    print(f"\nK-line features capture candlestick patterns:")
    print(f"Rows: {len(df)}")
    print(f"\nSample data:")
    print(df.head(10).to_string())

    # Analyze patterns
    if not df.empty:
        bullish = (df['KMID'] > 0).sum()
        bearish = (df['KMID'] < 0).sum()
        print(f"\nPattern analysis:")
        print(f"  Bullish candles (close > open): {bullish}")
        print(f"  Bearish candles (close < open): {bearish}")

    return df


def demo_rolling_features():
    """Demo: Rolling window features"""
    print("\n" + "="*70)
    print("2. ROLLING WINDOW FEATURES")
    print("="*70)

    expressions = {
        "MA5": "Mean($close, 5)/$close",
        "MA20": "Mean($close, 20)/$close",
        "STD5": "Std($close, 5)/$close",
        "STD20": "Std($close, 20)/$close",
        "ROC5": "Ref($close, 5)/$close",
        "MAX5": "Max($close, 5)/$close",
        "MIN5": "Min($close, 5)/$close",
    }

    df = D.features(
        instruments=['AAPL'],
        fields=list(expressions.values()),
        start_time='2025-09-01',
        end_time='2025-09-30'
    )
    df.columns = list(expressions.keys())

    print(f"\nRolling features capture trends and momentum:")
    print(f"Rows: {len(df)}")
    print(f"\nSample data:")
    print(df.head(10).to_string())

    # Analyze trends
    if not df.empty and 'MA5' in df.columns:
        above_ma5 = (df['MA5'] < 1).sum()  # Close above MA5
        print(f"\nTrend analysis:")
        print(f"  Days above MA5: {above_ma5}/{len(df)}")

        if 'STD5' in df.columns:
            avg_volatility = df['STD5'].mean()
            print(f"  Average 5-day volatility: {avg_volatility:.2%}")

    return df


def demo_volume_features():
    """Demo: Volume-price correlation features"""
    print("\n" + "="*70)
    print("3. VOLUME-PRICE CORRELATION")
    print("="*70)

    expressions = {
        "CORR5": "Corr($close, Log($volume+1), 5)",
        "CORR10": "Corr($close, Log($volume+1), 10)",
        "CORR20": "Corr($close, Log($volume+1), 20)",
        "CORD5": "Corr($close/Ref($close,1), Log($volume/Ref($volume,1)+1), 5)",
    }

    df = D.features(
        instruments=['AAPL'],
        fields=list(expressions.values()),
        start_time='2025-09-01',
        end_time='2025-09-30'
    )
    df.columns = list(expressions.keys())

    print(f"\nCorrelation features show volume-price relationships:")
    print(f"Rows: {len(df)}")
    print(f"\nSample data:")
    print(df.dropna().head(10).to_string())

    # Analyze correlations
    if not df.empty:
        for col in df.columns:
            valid_data = df[col].dropna()
            if len(valid_data) > 0:
                avg_corr = valid_data.mean()
                print(f"  Avg {col}: {avg_corr:.3f}")

    return df


def demo_full_alpha158():
    """Demo: Full Alpha158 feature set"""
    print("\n" + "="*70)
    print("4. FULL ALPHA158 FEATURE SET")
    print("="*70)

    config = {
        "kbar": {},
        "price": {
            "windows": [0],
            "feature": ["OPEN", "HIGH", "LOW", "VWAP"],
        },
        "rolling": {
            "windows": [5, 10, 20, 30, 60],
        },
    }

    fields, names = Alpha158DL.get_feature_config(config)

    print(f"\nAlpha158 configuration:")
    print(f"  Total features: {len(fields)}")

    # Categorize features
    categories = {
        'K-line': [n for n in names if n.startswith('K')],
        'Price': [n for n in names if n.startswith(('OPEN', 'HIGH', 'LOW', 'VWAP'))],
        'Moving Avg': [n for n in names if n.startswith('MA')],
        'Std Dev': [n for n in names if n.startswith('STD')],
        'Correlation': [n for n in names if n.startswith('CORR')],
        'Beta': [n for n in names if n.startswith('BETA')],
        'ROC': [n for n in names if n.startswith('ROC')],
        'Max/Min': [n for n in names if n.startswith(('MAX', 'MIN', 'IMAX', 'IMIN'))],
        'Quantile': [n for n in names if n.startswith('QTLU') or n.startswith('QTLD')],
        'Count': [n for n in names if n.startswith('CNT')],
    }

    print(f"\nFeature breakdown by category:")
    for cat, feats in categories.items():
        if feats:
            print(f"  {cat:15s}: {len(feats):3d} features - {feats[:3]}")

    # Sample a few features
    sample_fields = fields[:20]
    sample_names = names[:20]

    df = D.features(
        instruments=['AAPL'],
        fields=sample_fields,
        start_time='2025-09-15',
        end_time='2025-09-30'
    )
    df.columns = sample_names

    print(f"\nSample of first 20 features:")
    print(f"Shape: {df.shape}")
    if not df.empty:
        print(f"\nData preview:")
        print(df.head().to_string())

        # Stats
        print(f"\nFeature statistics:")
        print(df.describe().T[['mean', 'std', 'min', 'max']].to_string())

    return df


def demo_multi_stock():
    """Demo: Alpha158 across multiple stocks"""
    print("\n" + "="*70)
    print("5. MULTI-STOCK ANALYSIS")
    print("="*70)

    stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']

    expressions = {
        "KMID": "($close-$open)/$open",
        "MA5": "Mean($close, 5)/$close",
        "STD5": "Std($close, 5)/$close",
        "CORR5": "Corr($close, Log($volume+1), 5)",
    }

    df = D.features(
        instruments=stocks,
        fields=list(expressions.values()),
        start_time='2025-09-15',
        end_time='2025-09-30'
    )
    df.columns = list(expressions.keys())

    print(f"\nComparing features across {len(stocks)} stocks:")
    print(f"Total rows: {len(df)}")

    # Per-stock analysis
    for stock in stocks:
        if (stock,) in df.index.get_level_values(0).unique():
            stock_data = df.xs(stock, level=0)
            print(f"\n{stock}:")
            print(f"  Rows: {len(stock_data)}")
            print(f"  Avg KMID (body): {stock_data['KMID'].mean():.4f}")
            if 'STD5' in stock_data.columns:
                print(f"  Avg 5d volatility: {stock_data['STD5'].mean():.4f}")

    return df


def main():
    """Run all Alpha158 demos"""
    print("\n" + "="*70)
    print("ALPHA158 FEATURE DEMONSTRATION")
    print("="*70)

    # Initialize Qlib
    qlib_path = Path("/Volumes/sandisk/quantmini-data/data/data/qlib/stocks_daily")

    if not qlib_path.exists():
        print(f"\n❌ Qlib data not found at {qlib_path}")
        print("\nPlease run the conversion first:")
        print("  python scripts/convert_to_qlib.py --data-type stocks_daily ...")
        return

    print(f"\nInitializing Qlib from: {qlib_path}")
    qlib.init(provider_uri=str(qlib_path), region='us')
    print("✓ Qlib initialized")

    # Run demos
    demos = [
        demo_kbar_features,
        demo_rolling_features,
        demo_volume_features,
        demo_full_alpha158,
        demo_multi_stock,
    ]

    results = []
    for demo in demos:
        try:
            result = demo()
            results.append((demo.__name__, "✓ Pass", result))
        except Exception as e:
            print(f"\n❌ Error in {demo.__name__}: {e}")
            results.append((demo.__name__, f"✗ Fail: {e}", None))

    # Summary
    print("\n" + "="*70)
    print("DEMO SUMMARY")
    print("="*70)

    for name, status, _ in results:
        print(f"  {name:30s} {status}")

    passed = sum(1 for _, s, _ in results if s.startswith("✓"))
    total = len(results)

    print(f"\nCompleted: {passed}/{total} demos successful")

    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("""
1. Use Alpha158 in model training:
   from qlib.contrib.data.handler import Alpha158
   handler = Alpha158(instruments='csi300', ...)

2. Compare with Alpha360:
   from qlib.contrib.data.handler import Alpha360

3. Create custom feature combinations:
   config = {"kbar": {}, "rolling": {"windows": [5, 20]}}
   fields, names = Alpha158DL.get_feature_config(config)

4. Run the full test suite:
   pytest tests/integration/test_alpha158.py -v
""")


if __name__ == '__main__':
    main()
