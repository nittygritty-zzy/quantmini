#!/usr/bin/env python3
"""
Qlib Custom Model Example - Base Class Interface

Demonstrates how to implement custom models using Qlib's base Model class interface.
Based on: https://qlib.readthedocs.io/en/latest/component/model.html#base-class-interface

This example shows:
1. How to inherit from qlib.model.base.Model
2. How to implement fit() and predict() methods
3. How to extract features and labels from Dataset
4. Creating simple custom models (Linear Regression, Random Forest)
5. Comparing custom models with built-in models
"""

# Suppress gym warnings for clean output
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

# Core imports
import qlib
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP
from qlib.utils import init_instance_by_config
import pandas as pd
import numpy as np
from typing import Union


class LinearRegressionModel(Model):
    """
    Custom Linear Regression Model using Qlib's Model interface.

    This demonstrates the minimal implementation required:
    - fit(): Train the model on dataset
    - predict(): Generate predictions on dataset
    """

    def __init__(self, **kwargs):
        """Initialize the model with optional parameters."""
        super().__init__()
        self.model = None
        self.feature_names = None

    def fit(self, dataset: DatasetH, reweighter=None):
        """
        Train the linear regression model.

        Args:
            dataset: Qlib Dataset with segments (train, valid, test)
            reweighter: Optional reweighter for sample weights
        """
        # Extract training data from dataset
        df_train = dataset.prepare(
            "train",
            col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )

        if df_train.empty:
            raise ValueError("Empty training data from dataset")

        # Get features and labels
        X_train = df_train["feature"]
        y_train = df_train["label"]

        # Store feature names for later
        self.feature_names = X_train.columns.tolist()

        # Handle NaN values - fill with 0 (same approach as Qlib's models)
        X_train = X_train.fillna(0)

        # Handle weights if reweighter provided
        if reweighter is not None:
            weights = reweighter.reweight(df_train)
        else:
            weights = None

        # Train using sklearn's LinearRegression
        from sklearn.linear_model import LinearRegression
        self.model = LinearRegression()

        if weights is not None:
            self.model.fit(X_train.values, y_train.values.ravel(), sample_weight=weights)
        else:
            self.model.fit(X_train.values, y_train.values.ravel())

        print(f"✓ Linear Regression trained on {len(X_train)} samples")
        print(f"  Features: {len(self.feature_names)}")
        print(f"  Coefficients range: [{self.model.coef_.min():.4f}, {self.model.coef_.max():.4f}]")

    def predict(self, dataset: DatasetH, segment: Union[str, slice] = "test") -> pd.Series:
        """
        Generate predictions for a dataset segment.

        Args:
            dataset: Qlib Dataset
            segment: Which segment to predict on (e.g., "test", "valid")

        Returns:
            pd.Series: Predictions with MultiIndex (datetime, instrument)
        """
        if self.model is None:
            raise ValueError("Model has not been fitted yet")

        # Extract data for the specified segment
        df_test = dataset.prepare(
            segment,
            col_set=["feature"],
            data_key=DataHandlerLP.DK_I  # DK_I for inference (no label needed)
        )

        if df_test.empty:
            raise ValueError(f"Empty data for segment: {segment}")

        # Get features
        X_test = df_test["feature"]

        # Handle NaN values - fill with 0
        X_test = X_test.fillna(0)

        # Make predictions
        predictions = self.model.predict(X_test.values)

        # Return as pandas Series with same index as input
        return pd.Series(predictions, index=df_test.index)


class RandomForestModel(Model):
    """
    Custom Random Forest Model using Qlib's Model interface.

    Demonstrates:
    - Customizable hyperparameters
    - Feature importance tracking
    - More complex sklearn model integration
    """

    def __init__(self, n_estimators=100, max_depth=10, random_state=42, **kwargs):
        """
        Initialize Random Forest model.

        Args:
            n_estimators: Number of trees
            max_depth: Maximum tree depth
            random_state: Random seed for reproducibility
        """
        super().__init__()
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.random_state = random_state
        self.model = None
        self.feature_names = None
        self.feature_importance = None

    def fit(self, dataset: DatasetH, reweighter=None):
        """Train the Random Forest model."""
        # Extract training data
        df_train = dataset.prepare(
            "train",
            col_set=["feature", "label"],
            data_key=DataHandlerLP.DK_L
        )

        if df_train.empty:
            raise ValueError("Empty training data")

        X_train = df_train["feature"]
        y_train = df_train["label"]

        self.feature_names = X_train.columns.tolist()

        # Handle NaN values
        X_train = X_train.fillna(0)

        # Get weights
        if reweighter is not None:
            weights = reweighter.reweight(df_train)
        else:
            weights = None

        # Train Random Forest
        from sklearn.ensemble import RandomForestRegressor
        self.model = RandomForestRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=self.random_state,
            n_jobs=-1
        )

        if weights is not None:
            self.model.fit(X_train.values, y_train.values.ravel(), sample_weight=weights)
        else:
            self.model.fit(X_train.values, y_train.values.ravel())

        # Store feature importance
        self.feature_importance = pd.Series(
            self.model.feature_importances_,
            index=self.feature_names
        ).sort_values(ascending=False)

        print(f"✓ Random Forest trained on {len(X_train)} samples")
        print(f"  Trees: {self.n_estimators}, Max depth: {self.max_depth}")
        print(f"  Top 3 features: {', '.join(self.feature_importance.head(3).index.tolist())}")

    def predict(self, dataset: DatasetH, segment: Union[str, slice] = "test") -> pd.Series:
        """Generate predictions."""
        if self.model is None:
            raise ValueError("Model not fitted")

        df_test = dataset.prepare(
            segment,
            col_set=["feature"],
            data_key=DataHandlerLP.DK_I
        )

        if df_test.empty:
            raise ValueError(f"Empty data for segment: {segment}")

        X_test = df_test["feature"]

        # Handle NaN values
        X_test = X_test.fillna(0)

        predictions = self.model.predict(X_test.values)

        return pd.Series(predictions, index=df_test.index)

    def get_feature_importance(self, top_n=10):
        """Get top N most important features."""
        if self.feature_importance is None:
            raise ValueError("Model not fitted yet")
        return self.feature_importance.head(top_n)


def custom_model_example():
    """
    Example: Create and train custom models using base Model interface
    """
    print("=" * 80)
    print("QLIB CUSTOM MODEL EXAMPLE - Base Class Interface")
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
    print(f"  Train: {dataset_config['kwargs']['segments']['train']}")
    print(f"  Valid: {dataset_config['kwargs']['segments']['valid']}")
    print(f"  Test:  {dataset_config['kwargs']['segments']['test']}")

    # Step 3: Train custom Linear Regression model
    print("\n[Step 3] Training Custom Linear Regression Model...")
    lr_model = LinearRegressionModel()
    lr_model.fit(dataset)

    # Make predictions
    pred_lr_test = lr_model.predict(dataset, segment="test")
    print(f"✓ Predictions: {len(pred_lr_test)} samples")
    print(f"  Mean: {pred_lr_test.mean():.6f}")
    print(f"  Std:  {pred_lr_test.std():.6f}")

    # Step 4: Train custom Random Forest model
    print("\n[Step 4] Training Custom Random Forest Model...")
    rf_model = RandomForestModel(n_estimators=50, max_depth=8)
    rf_model.fit(dataset)

    # Make predictions
    pred_rf_test = rf_model.predict(dataset, segment="test")
    print(f"✓ Predictions: {len(pred_rf_test)} samples")
    print(f"  Mean: {pred_rf_test.mean():.6f}")
    print(f"  Std:  {pred_rf_test.std():.6f}")

    # Show top features
    print("\n  Top 5 Important Features:")
    for i, (feat, importance) in enumerate(rf_model.get_feature_importance(5).items(), 1):
        print(f"    {i}. {feat}: {importance:.4f}")

    # Step 5: Compare with built-in LightGBM
    print("\n[Step 5] Comparing with Built-in LightGBM...")
    from qlib.contrib.model.gbdt import LGBModel
    lgb_model = LGBModel(
        loss="mse",
        num_leaves=31,
        learning_rate=0.05,
        n_estimators=50,
        verbose=-1
    )
    lgb_model.fit(dataset)
    pred_lgb_test = lgb_model.predict(dataset, segment="test")
    print(f"✓ LightGBM predictions: {len(pred_lgb_test)} samples")
    print(f"  Mean: {pred_lgb_test.mean():.6f}")
    print(f"  Std:  {pred_lgb_test.std():.6f}")

    # Step 6: Compare models
    print("\n[Step 6] Model Comparison...")
    print(f"  Linear Regression:  mean={pred_lr_test.mean():.6f}, std={pred_lr_test.std():.6f}")
    print(f"  Random Forest:      mean={pred_rf_test.mean():.6f}, std={pred_rf_test.std():.6f}")
    print(f"  LightGBM:           mean={pred_lgb_test.mean():.6f}, std={pred_lgb_test.std():.6f}")

    print("\n  Prediction Correlations:")
    print(f"    Linear Reg vs Random Forest:  {pred_lr_test.corr(pred_rf_test):.4f}")
    print(f"    Linear Reg vs LightGBM:       {pred_lr_test.corr(pred_lgb_test):.4f}")
    print(f"    Random Forest vs LightGBM:    {pred_rf_test.corr(pred_lgb_test):.4f}")

    # Step 7: Save custom model
    print("\n[Step 7] Saving custom model...")
    model_path = Path("examples/saved_custom_model.pkl")
    lr_model.to_pickle(model_path)
    print(f"✓ Model saved to: {model_path}")

    # Load and verify
    import pickle
    with open(model_path, 'rb') as f:
        loaded_model = pickle.load(f)
    pred_loaded = loaded_model.predict(dataset, segment="test")
    print(f"✓ Loaded model predictions match: {(pred_loaded == pred_lr_test).all()}")

    print("\n" + "=" * 80)
    print("✅ CUSTOM MODEL EXAMPLE COMPLETED SUCCESSFULLY")
    print("=" * 80)

    return lr_model, rf_model, lgb_model, dataset


def model_interface_demo():
    """
    Demonstrate the Model base class interface in detail
    """
    print("\n" + "=" * 80)
    print("MODEL BASE CLASS INTERFACE DEMONSTRATION")
    print("=" * 80)

    print("\nThe qlib.model.base.Model class provides the following interface:")
    print("\n1. Abstract Methods (MUST implement):")
    print("   - fit(dataset, reweighter=None)")
    print("     → Train the model on the given dataset")
    print("\n   - predict(dataset, segment='test')")
    print("     → Generate predictions for a dataset segment")

    print("\n2. Inherited Methods (already implemented):")
    print("   - to_pickle(path)")
    print("     → Save model to pickle file")
    print("\n   - general_load(path)")
    print("     → Load model from pickle file")
    print("\n   - config()")
    print("     → Get model configuration")

    print("\n3. Dataset Access Patterns:")
    print("   # For training (with labels):")
    print("   df = dataset.prepare('train', col_set=['feature', 'label'], data_key=DataHandlerLP.DK_L)")
    print("   X, y = df['feature'], df['label']")
    print("\n   # For inference (without labels):")
    print("   df = dataset.prepare('test', col_set=['feature'], data_key=DataHandlerLP.DK_I)")
    print("   X = df['feature']")

    print("\n4. Return Value Specification:")
    print("   - predict() should return pd.Series with MultiIndex (datetime, instrument)")
    print("   - The index should match the input data's index")

    print("\n5. Best Practices:")
    print("   - Store learned parameters as instance attributes (not starting with '_')")
    print("   - Raise ValueError for empty datasets")
    print("   - Support optional reweighter for sample weighting")
    print("   - Check if model is fitted before predicting")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    import sys

    print("\n" + "=" * 80)
    print("QLIB CUSTOM MODEL EXAMPLES")
    print("=" * 80)
    print("\nAvailable examples:")
    print("  1. Custom model implementation (default)")
    print("  2. Model interface demonstration")
    print()

    # Allow user to choose example
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "1"

    try:
        if choice == "1":
            lr_model, rf_model, lgb_model, dataset = custom_model_example()
        elif choice == "2":
            model_interface_demo()
        else:
            print(f"Unknown choice: {choice}")
            print("Using default: Custom model example")
            lr_model, rf_model, lgb_model, dataset = custom_model_example()

        print("\n✅ All examples completed successfully!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
