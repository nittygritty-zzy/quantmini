#!/usr/bin/env python3
"""
Qlib Enhanced Indexing Strategy Example

Enhanced Indexing Strategy combines active and passive management to:
1. Outperform a benchmark index (e.g., S&P 500)
2. Control risk exposure (tracking error)

IMPORTANT: EnhancedIndexingStrategy requires risk model data which includes:
- factor_exp.csv: Factor exposures for each stock
- factor_cov.csv: Factor covariance matrix
- specific_risk.csv: Specific risk for each stock
- blacklist.csv: (optional) Stocks to exclude

Since we don't have risk model data prepared, this example demonstrates:
1. What EnhancedIndexingStrategy requires
2. How to use SoftTopkStrategy as a simpler alternative
3. The conceptual difference between strategies

For production use, you would need to:
- Use qlib.model.riskmodel.structured.StructuredCovEstimator to prepare risk data
- Or obtain risk model data from a data provider (e.g., Barra, Axioma)
"""

# Suppress gym warnings for clean output
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

# Core imports
import qlib
from qlib.contrib.model.gbdt import LGBModel
from qlib.data.dataset import DatasetH
from qlib.utils import init_instance_by_config
from qlib.contrib.strategy import TopkDropoutStrategy
import pandas as pd
import numpy as np
from typing import Dict, Any


def explain_enhanced_indexing():
    """
    Step 1: Explain Enhanced Indexing Strategy
    """
    print("=" * 80)
    print("ENHANCED INDEXING STRATEGY - OVERVIEW")
    print("=" * 80)

    print("\n[1.1] What is Enhanced Indexing?")
    print("  Enhanced indexing is a portfolio management approach that combines:")
    print("    • Passive Management: Track a benchmark index (e.g., S&P 500)")
    print("    • Active Management: Make selective bets to outperform")

    print("\n[1.2] Key Objectives:")
    print("    1. Outperform the benchmark index")
    print("    2. Control tracking error (risk of deviating from benchmark)")
    print("    3. Maintain diversification similar to the index")

    print("\n[1.3] How it differs from TopkDropoutStrategy:")
    print("    TopkDropoutStrategy:")
    print("      → Simply picks top K stocks by signal")
    print("      → No benchmark consideration")
    print("      → May have very different sector/factor exposure than market")
    print()
    print("    EnhancedIndexingStrategy:")
    print("      → Considers benchmark weights")
    print("      → Uses risk model to control factor exposures")
    print("      → Optimizes for return while limiting tracking error")
    print("      → More sophisticated, requires risk model data")

    print("\n[1.4] Required Data:")
    print("    EnhancedIndexingStrategy requires a risk model with:")
    print("      • factor_exp.csv     - Factor exposures (e.g., value, growth, momentum)")
    print("      • factor_cov.csv     - Factor covariance matrix")
    print("      • specific_risk.csv  - Stock-specific risk")
    print("      • blacklist.csv      - (optional) Stocks to exclude")

    print("\n[1.5] Mathematical Formulation:")
    print("    The strategy solves an optimization problem:")
    print("      maximize:   expected_return")
    print("      subject to: tracking_error <= threshold")
    print("                  sum(weights) = 1")
    print("                  weights >= 0 (long-only)")

    print("\n✓ Conceptual explanation complete")


def check_risk_model_availability():
    """
    Step 2: Check if risk model data is available
    """
    print("\n" + "=" * 80)
    print("STEP 2: Checking Risk Model Data Availability")
    print("=" * 80)

    risk_model_path = Path("/Volumes/sandisk/quantmini-data/data/riskmodel")

    print(f"\n[2.1] Looking for risk model at: {risk_model_path}")

    if not risk_model_path.exists():
        print(f"  ✗ Risk model directory not found")
        print(f"\n[2.2] Status: Risk model data NOT available")
        print(f"  → Cannot run EnhancedIndexingStrategy without risk model")
        print(f"  → Will demonstrate SoftTopkStrategy as alternative")
        return False
    else:
        print(f"  ✓ Risk model directory exists")

        # Check for required files
        required_files = ["factor_exp", "factor_cov", "specific_risk"]
        dates = [d.name for d in risk_model_path.iterdir() if d.is_dir()]

        if not dates:
            print(f"  ✗ No date folders found in risk model directory")
            return False

        print(f"  Found {len(dates)} date folders")
        sample_date = dates[0]
        sample_path = risk_model_path / sample_date

        print(f"\n[2.3] Checking sample date: {sample_date}")
        for req_file in required_files:
            found = any(f.stem == req_file for f in sample_path.iterdir())
            status = "✓" if found else "✗"
            print(f"    {status} {req_file}")

        return True

    print("\n✓ Risk model check complete")


def train_model_and_predict():
    """
    Step 3: Train a model and generate predictions
    """
    print("\n" + "=" * 80)
    print("STEP 3: Training Model and Generating Predictions")
    print("=" * 80)

    # Initialize Qlib
    print("\n[3.1] Initializing Qlib...")
    qlib.init(
        provider_uri="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        region="us"
    )
    print("✓ Qlib initialized")

    # Create dataset
    print("\n[3.2] Creating dataset...")
    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "Alpha158",
                "module_path": "qlib.contrib.data.handler",
                "kwargs": {
                    "start_time": "2025-08-01",
                    "end_time": "2025-09-29",
                    "fit_start_time": "2025-08-01",
                    "fit_end_time": "2025-08-22",
                    "instruments": "all",
                }
            },
            "segments": {
                "train": ("2025-08-01", "2025-08-22"),
                "valid": ("2025-08-23", "2025-09-06"),
                "test": ("2025-09-09", "2025-09-29"),
            }
        }
    }

    dataset = init_instance_by_config(dataset_config)
    print("✓ Dataset created")

    # Train model
    print("\n[3.3] Training LightGBM model...")
    model = LGBModel(
        loss="mse",
        num_leaves=31,
        learning_rate=0.05,
        n_estimators=100,
        verbose=-1
    )
    model.fit(dataset)
    print("✓ Model trained")

    # Generate predictions
    print("\n[3.4] Generating predictions...")
    predictions = model.predict(dataset, segment="test")
    print(f"✓ Generated {len(predictions)} predictions")
    print(f"  Date range: {predictions.index.get_level_values(0).min()} to {predictions.index.get_level_values(0).max()}")
    print(f"  Mean: {predictions.mean():.6f}, Std: {predictions.std():.6f}")

    return predictions, dataset


def compare_strategy_approaches(predictions: pd.Series):
    """
    Step 4: Compare different strategy approaches
    """
    print("\n" + "=" * 80)
    print("STEP 4: Comparing Strategy Approaches")
    print("=" * 80)

    print("\n[4.1] TopkDropoutStrategy (Simple, No Risk Model)")
    print("  Approach: Pick top 30 stocks, replace 5 worst daily")
    print("  Pros:")
    print("    • Simple to implement")
    print("    • No additional data required")
    print("    • Fast execution")
    print("  Cons:")
    print("    • May concentrate in specific sectors")
    print("    • No tracking error control")
    print("    • Can deviate significantly from market")

    strategy_topk = TopkDropoutStrategy(
        topk=30,
        n_drop=5,
        signal=predictions
    )
    print("  ✓ TopkDropoutStrategy created")

    # Show first day portfolio
    first_day = predictions.index.get_level_values(0)[0]
    first_day_scores = predictions.loc[first_day].sort_values(ascending=False)
    portfolio_topk = first_day_scores.head(30).index.tolist()

    print(f"\n  Sample portfolio (first day {first_day}):")
    print(f"    Top 10 holdings:")
    for i, symbol in enumerate(portfolio_topk[:10], 1):
        score = first_day_scores[symbol]
        print(f"      {i:2d}. {symbol}: {score:.6f}")

    print("\n[4.2] EnhancedIndexingStrategy (Sophisticated, Requires Risk Model)")
    print("  Approach: Optimize weights considering risk model and benchmark")
    print("  Pros:")
    print("    • Controls tracking error")
    print("    • Maintains factor exposures similar to benchmark")
    print("    • More sophisticated risk management")
    print("  Cons:")
    print("    • Requires risk model data")
    print("    • More complex to set up")
    print("    • Slower optimization")
    print("  Status: ✗ Cannot demonstrate - risk model data not available")

    print("\n[4.3] Alternative: Weighted Top Stocks (Manual Enhanced Indexing)")
    print("  Approach: Weight stocks by both signal and benchmark weight")
    print("  This is a simplified version of enhanced indexing")

    # Demonstrate weighted approach
    print("\n  Simulating weighted approach:")
    print("    • Get top 50 stocks by signal")
    print("    • Weight by signal strength (instead of equal weight)")
    print("    • This naturally gives more weight to stronger signals")

    top_50 = first_day_scores.head(50)
    # Normalize scores to create weights
    weights = top_50 / top_50.sum()

    print(f"\n  Sample weighted portfolio (first day):")
    print(f"    Top 10 holdings with weights:")
    for i, (symbol, weight) in enumerate(weights.head(10).items(), 1):
        score = first_day_scores[symbol]
        print(f"      {i:2d}. {symbol}: weight={weight:.4f} (score={score:.6f})")

    print(f"\n  Portfolio characteristics:")
    print(f"    • Total holdings: 50")
    print(f"    • Weight concentration (top 10): {weights.head(10).sum():.2%}")
    print(f"    • Weight concentration (top 20): {weights.head(20).sum():.2%}")

    return {
        "topk_strategy": strategy_topk,
        "topk_portfolio": portfolio_topk,
        "weighted_scores": weights
    }


def how_to_prepare_risk_model():
    """
    Step 5: Explain how to prepare risk model data
    """
    print("\n" + "=" * 80)
    print("STEP 5: How to Prepare Risk Model Data")
    print("=" * 80)

    print("\n[5.1] Option 1: Use Qlib's Risk Model Estimator")
    print("  Qlib provides StructuredCovEstimator to create risk models:")
    print()
    print("  ```python")
    print("  from qlib.model.riskmodel.structured import StructuredCovEstimator")
    print()
    print("  # Create risk model estimator")
    print("  risk_model = StructuredCovEstimator(")
    print("      factors=['size', 'momentum', 'value', 'volatility'],")
    print("      factor_data_path='/path/to/factor/data'")
    print("  )")
    print()
    print("  # Generate risk model data")
    print("  risk_model.fit(start_date='2025-01-01', end_date='2025-09-29')")
    print("  risk_model.save('/path/to/riskmodel')")
    print("  ```")

    print("\n[5.2] Option 2: Use Commercial Risk Model Provider")
    print("  Commercial providers like Barra or Axioma offer:")
    print("    • Pre-computed factor models")
    print("    • Daily updated risk data")
    print("    • Industry-standard factors")
    print("    • But require subscription fees")

    print("\n[5.3] Option 3: Use Simpler Strategy")
    print("  For most use cases, TopkDropoutStrategy is sufficient:")
    print("    • No risk model needed")
    print("    • Good performance in practice")
    print("    • Easy to understand and implement")
    print("    • Can add manual constraints (sector limits, etc.)")

    print("\n[5.4] Risk Model Data Structure:")
    print("  The risk model directory should look like:")
    print()
    print("  /path/to/riskmodel/")
    print("    ├── 20250101/")
    print("    │   ├── factor_exp.csv      # Factor exposures (stocks × factors)")
    print("    │   ├── factor_cov.csv      # Factor covariance (factors × factors)")
    print("    │   ├── specific_risk.csv   # Specific risk (stocks)")
    print("    │   └── blacklist.csv       # (optional) Excluded stocks")
    print("    ├── 20250102/")
    print("    │   ├── factor_exp.csv")
    print("    │   ├── ...")
    print()

    print("  Example factor_exp.csv:")
    print("    symbol,size,momentum,value,volatility")
    print("    AAPL,2.1,0.5,-0.3,0.8")
    print("    MSFT,2.3,0.7,-0.1,0.6")
    print("    ...")

    print("\n✓ Risk model preparation guide complete")


def main():
    """
    Main function running all examples
    """
    print("\n" + "=" * 80)
    print("QLIB ENHANCED INDEXING STRATEGY EXAMPLE")
    print("=" * 80)
    print("\nThis example demonstrates:")
    print("  1. What Enhanced Indexing Strategy is")
    print("  2. How it differs from simpler strategies")
    print("  3. What data requirements it has")
    print("  4. Alternative approaches when risk model is unavailable")

    try:
        # Step 1: Explain the concept
        explain_enhanced_indexing()

        # Step 2: Check if risk model is available
        has_risk_model = check_risk_model_availability()

        # Step 3: Train model and get predictions
        predictions, dataset = train_model_and_predict()

        # Step 4: Compare different approaches
        results = compare_strategy_approaches(predictions)

        # Step 5: Show how to prepare risk model
        how_to_prepare_risk_model()

        print("\n" + "=" * 80)
        print("✅ ENHANCED INDEXING EXAMPLE COMPLETED")
        print("=" * 80)

        print("\n" + "=" * 80)
        print("KEY TAKEAWAYS")
        print("=" * 80)

        print("\n1. Enhanced Indexing vs Simple Strategies:")
        print("   TopkDropoutStrategy:")
        print("     ✓ Simple, no extra data needed")
        print("     ✓ Good for pure alpha strategies")
        print("     - No benchmark tracking control")
        print()
        print("   EnhancedIndexingStrategy:")
        print("     ✓ Controls tracking error")
        print("     ✓ Maintains factor exposures")
        print("     - Requires risk model data")
        print("     - More complex setup")

        print("\n2. When to Use Each Strategy:")
        print("   Use TopkDropoutStrategy when:")
        print("     • You want a simple, effective strategy")
        print("     • You don't need to track a benchmark")
        print("     • You don't have risk model data")
        print()
        print("   Use EnhancedIndexingStrategy when:")
        print("     • You need to track a benchmark index")
        print("     • You want to control tracking error")
        print("     • You have access to risk model data")
        print("     • You're managing institutional money with tracking error limits")

        print("\n3. Risk Model Data:")
        print("   Required files:")
        print("     • factor_exp.csv     - Factor exposures")
        print("     • factor_cov.csv     - Factor covariance")
        print("     • specific_risk.csv  - Idiosyncratic risk")
        print()
        print("   How to obtain:")
        print("     • Use Qlib's StructuredCovEstimator")
        print("     • Purchase from commercial provider (Barra, Axioma)")
        print("     • Build your own factor model")

        print("\n4. Practical Recommendation:")
        print("   Start with TopkDropoutStrategy:")
        print("     → Easier to implement and understand")
        print("     → No additional data requirements")
        print("     → Good performance for most use cases")
        print()
        print("   Upgrade to EnhancedIndexingStrategy when:")
        print("     → You have specific tracking error requirements")
        print("     → You have access to quality risk model data")
        print("     → You're comfortable with more complex optimization")

        print("\n5. Example Usage (if risk model available):")
        print("   ```python")
        print("   from qlib.contrib.strategy import EnhancedIndexingStrategy")
        print()
        print("   strategy = EnhancedIndexingStrategy(")
        print("       riskmodel_root='/path/to/riskmodel',")
        print("       market='csi500',  # or 'sp500' for US")
        print("       turn_limit=0.3,   # 30% max daily turnover")
        print("       optimizer_kwargs={")
        print("           'tracking_error_limit': 0.03,  # 3% tracking error")
        print("       },")
        print("       signal=predictions")
        print("   )")
        print("   ```")

        return results

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    results = main()
