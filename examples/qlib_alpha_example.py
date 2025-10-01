#!/usr/bin/env python3
"""
Qlib Alpha Expressions Example

Alpha expressions are formulaic factors that can explain and predict future asset returns.
Based on: https://qlib.readthedocs.io/en/latest/advanced/alpha.html#introduction

This example demonstrates:
1. Creating custom alpha expressions using Qlib's expression language
2. Using built-in operators (EMA, MA, Ref, etc.)
3. Loading and evaluating alpha expressions
4. Comparing different alpha factors
5. Using alphas in trading strategies
"""

# Suppress gym warnings for clean output
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

# Core imports
import qlib
from qlib.data import D
from qlib.data.dataset import DatasetH
from qlib.utils import init_instance_by_config
import pandas as pd
import numpy as np
from typing import Dict, List


def explain_alpha_expressions():
    """
    Step 1: Explain what alpha expressions are
    """
    print("=" * 80)
    print("ALPHA EXPRESSIONS - OVERVIEW")
    print("=" * 80)

    print("\n[1.1] What are Alpha Expressions?")
    print("  Alpha expressions are formulaic factors that:")
    print("    • Predict future asset returns")
    print("    • Combine multiple financial indicators")
    print("    • Use mathematical operators and functions")
    print("    • Can be backtested and optimized")

    print("\n[1.2] Basic Syntax:")
    print("  Variables:")
    print("    $open, $close, $high, $low, $volume - OHLCV data")
    print("    $vwap, $factor - Additional fields")
    print()
    print("  Operators:")
    print("    +, -, *, / - Arithmetic")
    print("    >, <, >=, <= - Comparison")
    print("    &, | - Logical")
    print()
    print("  Functions:")
    print("    Mean(x, N) - Moving average over N periods")
    print("    EMA(x, N) - Exponential moving average")
    print("    Ref(x, N) - Value N periods ago")
    print("    Std(x, N) - Standard deviation")
    print("    Max(x, N), Min(x, N) - Rolling max/min")
    print("    Sum(x, N) - Rolling sum")
    print("    Rank(x) - Cross-sectional rank")
    print("    Log(x), Abs(x), Sign(x) - Math functions")

    print("\n✓ Conceptual explanation complete")


def basic_alpha_examples():
    """
    Step 2: Show basic alpha expression examples
    """
    print("\n" + "=" * 80)
    print("STEP 2: Basic Alpha Expression Examples")
    print("=" * 80)

    alphas = {
        "Simple Return": {
            "expression": "Ref($close, 1) / Ref($close, 2) - 1",
            "description": "1-day return",
            "interpretation": "Price momentum over 1 day"
        },
        "RSI (Relative Strength)": {
            "expression": "Mean($close, 14) / Ref($close, 1) - 1",
            "description": "14-day mean return",
            "interpretation": "Strength indicator based on recent price"
        },
        "Price to MA Ratio": {
            "expression": "$close / Mean($close, 20) - 1",
            "description": "Deviation from 20-day moving average",
            "interpretation": "Positive = above MA (strong), Negative = below MA (weak)"
        },
        "Volume Surge": {
            "expression": "$volume / Mean($volume, 20)",
            "description": "Volume relative to 20-day average",
            "interpretation": "Values > 1 indicate higher than normal volume"
        },
        "MACD": {
            "expression": "(EMA($close, 12) - EMA($close, 26)) / $close",
            "description": "MACD normalized by price",
            "interpretation": "Momentum indicator: positive = bullish, negative = bearish"
        },
        "Mean Reversion": {
            "expression": "($close - Mean($close, 20)) / Std($close, 20)",
            "description": "Z-score: distance from mean in standard deviations",
            "interpretation": "High values suggest overbought, low values suggest oversold"
        },
        "Volatility": {
            "expression": "Std($close, 20) / Mean($close, 20)",
            "description": "Coefficient of variation",
            "interpretation": "Higher values indicate more volatile stocks"
        },
    }

    print("\n[2.1] Common Alpha Expressions:")
    for name, info in alphas.items():
        print(f"\n  {name}:")
        print(f"    Expression: {info['expression']}")
        print(f"    Description: {info['description']}")
        print(f"    Interpretation: {info['interpretation']}")

    print("\n✓ Basic examples explained")
    return alphas


def load_and_evaluate_alpha(expression: str, name: str):
    """
    Step 3: Load and evaluate an alpha expression
    """
    print(f"\n" + "=" * 80)
    print(f"STEP 3: Evaluating Alpha Expression - {name}")
    print("=" * 80)

    print(f"\n[3.1] Expression: {expression}")

    # Initialize Qlib
    print("\n[3.2] Initializing Qlib...")
    qlib.init(
        provider_uri="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        region="us"
    )
    print("✓ Qlib initialized")

    # Load data using expression
    print("\n[3.3] Loading data with alpha expression...")
    try:
        data = D.features(
            instruments=D.instruments('all'),
            fields=[expression],
            start_time='2025-09-01',
            end_time='2025-09-29'
        )
        print(f"✓ Data loaded: {len(data)} data points")

        # Show statistics
        print(f"\n[3.4] Alpha Statistics:")
        print(f"  Count: {data.iloc[:, 0].count():,}")
        print(f"  Mean: {data.iloc[:, 0].mean():.6f}")
        print(f"  Std: {data.iloc[:, 0].std():.6f}")
        print(f"  Min: {data.iloc[:, 0].min():.6f}")
        print(f"  25%: {data.iloc[:, 0].quantile(0.25):.6f}")
        print(f"  50%: {data.iloc[:, 0].quantile(0.50):.6f}")
        print(f"  75%: {data.iloc[:, 0].quantile(0.75):.6f}")
        print(f"  Max: {data.iloc[:, 0].max():.6f}")

        # Show sample values for a specific date
        print(f"\n[3.5] Sample Values (2025-09-29):")
        last_day = data.index.get_level_values(0).max()
        last_day_data = data.loc[last_day].sort_values(by=data.columns[0], ascending=False)

        print(f"  Top 10 stocks (highest alpha values):")
        for i, (symbol, row) in enumerate(last_day_data.head(10).iterrows(), 1):
            print(f"    {i:2d}. {symbol:8s}: {row.iloc[0]:8.6f}")

        print(f"\n  Bottom 10 stocks (lowest alpha values):")
        for i, (symbol, row) in enumerate(last_day_data.tail(10).iterrows(), 1):
            print(f"    {i:2d}. {symbol:8s}: {row.iloc[0]:8.6f}")

        print(f"\n✓ Alpha evaluation complete")
        return data

    except Exception as e:
        print(f"✗ Error evaluating alpha: {e}")
        import traceback
        traceback.print_exc()
        return None


def compare_multiple_alphas():
    """
    Step 4: Compare multiple alpha expressions
    """
    print("\n" + "=" * 80)
    print("STEP 4: Comparing Multiple Alpha Expressions")
    print("=" * 80)

    # Select a few alphas to compare
    alphas_to_test = {
        "Momentum": "Ref($close, 1) / Ref($close, 5) - 1",
        "Mean Reversion": "($close - Mean($close, 20)) / Std($close, 20)",
        "Volume": "$volume / Mean($volume, 20) - 1",
    }

    print("\n[4.1] Alphas to compare:")
    for name, expr in alphas_to_test.items():
        print(f"  {name}: {expr}")

    # Initialize Qlib (if not already initialized)
    try:
        qlib.init(
            provider_uri="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
            region="us"
        )
    except:
        pass

    print("\n[4.2] Loading data for all alphas...")
    results = {}

    for name, expression in alphas_to_test.items():
        try:
            data = D.features(
                instruments=D.instruments('all'),
                fields=[expression],
                start_time='2025-09-20',
                end_time='2025-09-29'
            )
            results[name] = data.iloc[:, 0]
            print(f"  ✓ {name}: {len(data)} points, mean={data.iloc[:, 0].mean():.6f}")
        except Exception as e:
            print(f"  ✗ {name}: Error - {e}")
            results[name] = None

    # Compare correlations
    if len(results) > 1 and all(v is not None for v in results.values()):
        print("\n[4.3] Alpha Correlations:")
        print("  (How similar are the alpha signals?)")
        print()

        alpha_names = list(results.keys())
        for i, name1 in enumerate(alpha_names):
            for name2 in alpha_names[i+1:]:
                # Align the two series
                combined = pd.DataFrame({
                    name1: results[name1],
                    name2: results[name2]
                }).dropna()

                if len(combined) > 0:
                    corr = combined[name1].corr(combined[name2])
                    print(f"  {name1} vs {name2}: {corr:.4f}")
                    if abs(corr) > 0.7:
                        print(f"    → High correlation: alphas provide similar signals")
                    elif abs(corr) < 0.3:
                        print(f"    → Low correlation: alphas provide diverse signals")

    print("\n✓ Alpha comparison complete")
    return results


def use_alpha_in_strategy():
    """
    Step 5: Demonstrate using alpha in a trading strategy
    """
    print("\n" + "=" * 80)
    print("STEP 5: Using Alpha in Trading Strategy")
    print("=" * 80)

    print("\n[5.1] How to use alphas in strategies:")
    print("  1. Define alpha expression(s)")
    print("  2. Create dataset handler with alpha as label/feature")
    print("  3. Use alpha directly as signal, or train model on alpha")
    print("  4. Pass to strategy (e.g., TopkDropoutStrategy)")

    print("\n[5.2] Example Configuration:")
    print("""
  # Method 1: Use alpha directly as signal
  alpha_expression = "($close - Mean($close, 20)) / Std($close, 20)"

  data = D.features(
      instruments=D.instruments('all'),
      fields=[alpha_expression],
      start_time='2025-09-01',
      end_time='2025-09-29'
  )

  strategy = TopkDropoutStrategy(
      topk=30,
      n_drop=5,
      signal=data  # Use alpha as signal directly
  )

  # Method 2: Use alpha as feature for ML model
  handler_config = {
      "class": "DataHandlerLP",
      "module_path": "qlib.data.dataset.handler",
      "kwargs": {
          "start_time": "2025-08-01",
          "end_time": "2025-09-29",
          "instruments": "all",
          "infer_processors": [],
          "learn_processors": [],
          "process_type": DataHandlerLP.PTYPE_A,
          "data_loader": {
              "class": "QlibDataLoader",
              "kwargs": {
                  "config": {
                      "feature": [
                          "($close - Mean($close, 20)) / Std($close, 20)",
                          "Ref($close, 1) / Ref($close, 5) - 1",
                          "$volume / Mean($volume, 20) - 1",
                      ],
                      "label": ["Ref($close, -2) / Ref($close, -1) - 1"]
                  }
              }
          }
      }
  }

  # Train model on alpha features
  dataset = DatasetH(handler=handler)
  model = LGBModel(...)
  model.fit(dataset)
  predictions = model.predict(dataset)

  strategy = TopkDropoutStrategy(
      topk=30,
      n_drop=5,
      signal=predictions
  )
    """)

    print("\n[5.3] Key Points:")
    print("  • Alphas can be used directly as trading signals")
    print("  • Or combined as features for machine learning models")
    print("  • Multiple alphas can be averaged or ensembled")
    print("  • Alphas should be tested for predictive power (IC, IR)")

    print("\n✓ Strategy integration explained")


def advanced_alpha_examples():
    """
    Step 6: Show advanced alpha expressions
    """
    print("\n" + "=" * 80)
    print("STEP 6: Advanced Alpha Expressions")
    print("=" * 80)

    advanced_alphas = {
        "Alpha #1 (WorldQuant)": {
            "expression": "Rank($close / Ref($close, 1)) - Rank($volume / Ref($volume, 1))",
            "description": "Cross-sectional rank of returns vs volume changes",
            "note": "Captures divergence between price and volume"
        },
        "Alpha #2 (Momentum + Vol)": {
            "expression": "Ref($close, 1) / Ref($close, 5) * (1 / Std($close, 20))",
            "description": "Momentum adjusted by volatility",
            "note": "Favors momentum in low-volatility stocks"
        },
        "Alpha #3 (Mean Reversion)": {
            "expression": "-1 * ($close - Mean($close, 10)) / Std($close, 10)",
            "description": "Inverted z-score for mean reversion",
            "note": "Negative sign for contrarian strategy"
        },
        "Alpha #4 (Volume-Price)": {
            "expression": "($volume / Mean($volume, 20)) * ($close / Mean($close, 20) - 1)",
            "description": "Volume surge combined with price divergence",
            "note": "High volume + price breakout signal"
        },
    }

    print("\n[6.1] Advanced Alpha Patterns:")
    for name, info in advanced_alphas.items():
        print(f"\n  {name}:")
        print(f"    Expression: {info['expression']}")
        print(f"    Description: {info['description']}")
        print(f"    Note: {info['note']}")

    print("\n[6.2] Tips for Creating Alphas:")
    print("  • Start simple, add complexity gradually")
    print("  • Normalize alphas (z-score, rank) for cross-sectional comparison")
    print("  • Combine different signals (momentum, mean reversion, volume)")
    print("  • Test for overfitting - alphas should work out-of-sample")
    print("  • Consider transaction costs - avoid too-high-turnover alphas")

    print("\n✓ Advanced examples explained")


def main():
    """
    Main function running all alpha examples
    """
    print("\n" + "=" * 80)
    print("QLIB ALPHA EXPRESSIONS EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates:")
    print("  1. Understanding alpha expressions")
    print("  2. Basic alpha expression examples")
    print("  3. Loading and evaluating alphas")
    print("  4. Comparing multiple alphas")
    print("  5. Using alphas in trading strategies")
    print("  6. Advanced alpha patterns")

    try:
        # Step 1: Explain alpha expressions
        explain_alpha_expressions()

        # Step 2: Show basic examples
        basic_alphas = basic_alpha_examples()

        # Step 3: Load and evaluate one alpha
        alpha_name = "Mean Reversion"
        alpha_expr = "($close - Mean($close, 20)) / Std($close, 20)"
        alpha_data = load_and_evaluate_alpha(alpha_expr, alpha_name)

        # Step 4: Compare multiple alphas
        comparison_results = compare_multiple_alphas()

        # Step 5: Show how to use in strategy
        use_alpha_in_strategy()

        # Step 6: Advanced examples
        advanced_alpha_examples()

        print("\n" + "=" * 80)
        print("✅ ALL ALPHA EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)

        print("\n" + "=" * 80)
        print("KEY TAKEAWAYS")
        print("=" * 80)

        print("\n1. Alpha Expression Basics:")
        print("   • Alphas predict future returns")
        print("   • Built from OHLCV data + operators + functions")
        print("   • Can be simple (1-day return) or complex (multi-factor)")

        print("\n2. Creating Good Alphas:")
        print("   • Test predictive power (IC > 0.03, IR > 0.5)")
        print("   • Consider transaction costs")
        print("   • Avoid overfitting (use out-of-sample testing)")
        print("   • Combine multiple uncorrelated alphas")

        print("\n3. Common Patterns:")
        print("   • Momentum: Ref($close, 1) / Ref($close, N) - 1")
        print("   • Mean Reversion: ($close - Mean($close, N)) / Std($close, N)")
        print("   • Volume: $volume / Mean($volume, N)")
        print("   • Volatility: Std($close, N) / Mean($close, N)")

        print("\n4. Using Alphas:")
        print("   • Direct: Use alpha as signal for strategy")
        print("   • ML Features: Train model on multiple alpha features")
        print("   • Ensemble: Combine multiple alphas (average, weighted)")

        print("\n5. Next Steps:")
        print("   • Create your own alpha expressions")
        print("   • Backtest alphas to measure performance")
        print("   • Optimize alpha parameters (window sizes, weights)")
        print("   • Research alpha libraries (WorldQuant 101 Alphas)")

        return alpha_data, comparison_results

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    alpha_data, comparison_results = main()
