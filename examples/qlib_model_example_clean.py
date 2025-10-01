#!/usr/bin/env python3
"""
Qlib Model Example (Clean Output)

Same as qlib_model_example.py but with gym warnings suppressed.

This version uses the suppress_gym_warnings utility to eliminate
the deprecation warnings while maintaining full Qlib compatibility.
"""

# Suppress gym warnings BEFORE importing qlib
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

# Now import qlib and other dependencies
import qlib
from qlib.contrib.model.gbdt import LGBModel
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord
import pandas as pd
from pathlib import Path


def simple_model_example():
    """
    Simple example: Create, train, and use a LightGBM model
    """
    print("=" * 80)
    print("QLIB MODEL EXAMPLE - Simple LightGBM (Clean Output)")
    print("=" * 80)

    # Step 1: Initialize Qlib
    print("\n[Step 1] Initializing Qlib...")
    qlib.init(
        provider_uri="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        region="us"
    )
    print("✓ Qlib initialized")

    # Step 2: Create dataset
    print("\n[Step 2] Creating dataset...")
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
    print(f"  Train period: {dataset_config['kwargs']['segments']['train']}")
    print(f"  Valid period: {dataset_config['kwargs']['segments']['valid']}")
    print(f"  Test period: {dataset_config['kwargs']['segments']['test']}")

    # Step 3: Create model
    print("\n[Step 3] Creating LightGBM model...")
    model_config = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": -1,
        }
    }

    model = init_instance_by_config(model_config)
    print("✓ Model created")
    print(f"  Type: {type(model).__name__}")
    print(f"  Learning rate: {model_config['kwargs']['learning_rate']}")

    # Step 4: Train model
    print("\n[Step 4] Training model...")
    model.fit(dataset)
    print("✓ Model trained")

    # Step 5: Make predictions
    print("\n[Step 5] Making predictions...")

    # Predict on validation set
    pred_valid = model.predict(dataset, segment="valid")
    print(f"✓ Validation predictions: {len(pred_valid)} samples")
    print(f"  Mean: {pred_valid.mean():.6f}")
    print(f"  Std:  {pred_valid.std():.6f}")

    # Predict on test set
    pred_test = model.predict(dataset, segment="test")
    print(f"✓ Test predictions: {len(pred_test)} samples")
    print(f"  Mean: {pred_test.mean():.6f}")
    print(f"  Std:  {pred_test.std():.6f}")

    # Show sample predictions
    print("\n  Sample predictions (test set):")
    print(pred_test.head(10))

    # Step 6: Save model
    print("\n[Step 6] Saving model...")
    model_path = Path("examples/saved_model_clean.pkl")
    import pickle
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    print(f"✓ Model saved to: {model_path}")

    # Step 7: Load model
    print("\n[Step 7] Loading model...")
    with open(model_path, 'rb') as f:
        loaded_model = pickle.load(f)
    print("✓ Model loaded")

    # Verify loaded model works
    pred_loaded = loaded_model.predict(dataset, segment="test")
    print(f"✓ Loaded model predictions: {len(pred_loaded)} samples")
    print(f"  Predictions match original: {(pred_loaded == pred_test).all()}")

    print("\n" + "=" * 80)
    print("✅ MODEL EXAMPLE COMPLETED SUCCESSFULLY")
    print("=" * 80)

    return model, dataset, pred_test


def compare_models_example():
    """
    Example: Train and compare multiple models (LightGBM, XGBoost, CatBoost)
    """
    print("\n" + "=" * 80)
    print("QLIB MODEL EXAMPLE - Model Comparison (Clean Output)")
    print("=" * 80)

    # Initialize Qlib
    qlib.init(
        provider_uri="/Volumes/sandisk/quantmini-data/data/qlib/stocks_daily",
        region="us"
    )

    # Import additional models
    from qlib.contrib.model.xgboost import XGBModel
    from qlib.contrib.model.catboost_model import CatBoostModel

    # Create dataset (reuse same dataset for all models)
    print("\n[Step 1] Creating shared dataset...")
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

    # Model 1: LightGBM
    print("\n[Step 2] Training Model 1: LightGBM...")
    model1 = LGBModel(
        loss="mse",
        num_leaves=31,
        learning_rate=0.05,
        n_estimators=100,
        verbose=-1
    )
    model1.fit(dataset)
    pred1 = model1.predict(dataset, segment="test")
    print(f"✓ LightGBM predictions: mean={pred1.mean():.6f}, std={pred1.std():.6f}")

    # Model 2: XGBoost
    print("\n[Step 3] Training Model 2: XGBoost...")
    model2 = XGBModel(
        loss="mse",
        max_depth=6,
        learning_rate=0.05,
        n_estimators=100,
        verbosity=0
    )
    model2.fit(dataset)
    pred2 = model2.predict(dataset, segment="test")
    print(f"✓ XGBoost predictions: mean={pred2.mean():.6f}, std={pred2.std():.6f}")

    # Model 3: CatBoost
    print("\n[Step 4] Training Model 3: CatBoost...")
    model3 = CatBoostModel(
        loss="RMSE",
        depth=6,
        learning_rate=0.05,
        iterations=100
    )
    model3.fit(dataset)
    pred3 = model3.predict(dataset, segment="test")
    print(f"✓ CatBoost predictions: mean={pred3.mean():.6f}, std={pred3.std():.6f}")

    # Compare predictions
    print("\n[Step 5] Comparing models...")
    print(f"  LightGBM: {len(pred1)} predictions")
    print(f"  XGBoost:  {len(pred2)} predictions")
    print(f"  CatBoost: {len(pred3)} predictions")
    print(f"\n  Correlation matrix:")
    print(f"    LightGBM vs XGBoost:  {pred1.corr(pred2):.4f}")
    print(f"    LightGBM vs CatBoost: {pred1.corr(pred3):.4f}")
    print(f"    XGBoost  vs CatBoost: {pred2.corr(pred3):.4f}")

    print("\n" + "=" * 80)
    print("✅ MODEL COMPARISON COMPLETED")
    print("=" * 80)

    return model1, model2, model3, dataset


if __name__ == "__main__":
    import sys

    print("\n" + "=" * 80)
    print("QLIB MODEL EXAMPLES (CLEAN OUTPUT)")
    print("=" * 80)
    print("\nAvailable examples:")
    print("  1. Simple model (default)")
    print("  2. Compare models (LightGBM, XGBoost, CatBoost)")
    print()

    # Allow user to choose example
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "1"

    try:
        if choice == "1":
            model, dataset, predictions = simple_model_example()
        elif choice == "2":
            model1, model2, model3, dataset = compare_models_example()
        else:
            print(f"Unknown choice: {choice}")
            print("Using default: Simple model example")
            model, dataset, predictions = simple_model_example()

        print("\n✅ All examples completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
