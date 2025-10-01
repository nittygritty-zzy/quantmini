#!/usr/bin/env python3
"""
Complete Qlib Workflow Example

This script demonstrates a complete quantitative research workflow using Qlib:
1. Data loading from our converted dataset
2. Feature engineering with Alpha158
3. Model training (LightGBM)
4. Backtesting with TopkDropoutStrategy
5. Performance analysis

Based on: https://qlib.readthedocs.io/en/latest/component/workflow.html
"""

import sys
import yaml
from pathlib import Path
from pprint import pprint

import qlib
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
from qlib.utils import init_instance_by_config


def run_workflow(config_path: str):
    """
    Run complete Qlib workflow from configuration file.

    Args:
        config_path: Path to YAML configuration file
    """
    # Load configuration
    with open(config_path) as f:
        config = yaml.safe_load(f)

    print("=" * 80)
    print("QLIB WORKFLOW EXAMPLE - US Stocks")
    print("=" * 80)
    print(f"\nConfiguration loaded from: {config_path}")
    print(f"Data source: {config['qlib_init']['provider_uri']}")
    print(f"Date range: {config['data_handler_config']['start_time']} to {config['data_handler_config']['end_time']}")
    print(f"Market: {config['market']}")
    print(f"Benchmark: {config['benchmark']}")

    # Initialize Qlib
    print("\n" + "=" * 80)
    print("STEP 1: Initializing Qlib")
    print("=" * 80)
    qlib.init(**config["qlib_init"])
    print("✓ Qlib initialized successfully")

    # Create dataset
    print("\n" + "=" * 80)
    print("STEP 2: Creating Dataset with Alpha158 Features")
    print("=" * 80)
    dataset_config = config["task"]["dataset"]
    print(f"Handler: {dataset_config['kwargs']['handler']['class']}")
    print(f"Train: {dataset_config['kwargs']['segments']['train']}")
    print(f"Valid: {dataset_config['kwargs']['segments']['valid']}")
    print(f"Test: {dataset_config['kwargs']['segments']['test']}")

    dataset = init_instance_by_config(dataset_config)
    print("✓ Dataset created successfully")

    # Create model
    print("\n" + "=" * 80)
    print("STEP 3: Creating LightGBM Model")
    print("=" * 80)
    model_config = config["task"]["model"]
    print(f"Model: {model_config['class']}")
    print(f"Learning rate: {model_config['kwargs']['learning_rate']}")
    print(f"Max depth: {model_config['kwargs']['max_depth']}")
    print(f"Num leaves: {model_config['kwargs']['num_leaves']}")

    model = init_instance_by_config(model_config)
    print("✓ Model created successfully")

    # Start experiment tracking
    print("\n" + "=" * 80)
    print("STEP 4: Training Model")
    print("=" * 80)
    with R.start(experiment_name="workflow_example_us_stocks"):
        print("Training on segment:", dataset_config['kwargs']['segments']['train'])

        # Train model
        model.fit(dataset)
        print("✓ Model training complete")

        # Make predictions
        print("\n" + "=" * 80)
        print("STEP 5: Making Predictions")
        print("=" * 80)

        # Predict on validation set
        print("Predicting on validation set...")
        pred_valid = model.predict(dataset, segment="valid")
        print(f"✓ Validation predictions: {len(pred_valid)} samples")
        print(f"  Prediction stats:")
        print(f"    Mean: {pred_valid.mean():.6f}")
        print(f"    Std:  {pred_valid.std():.6f}")
        print(f"    Min:  {pred_valid.min():.6f}")
        print(f"    Max:  {pred_valid.max():.6f}")

        # Predict on test set
        print("\nPredicting on test set...")
        pred_test = model.predict(dataset, segment="test")
        print(f"✓ Test predictions: {len(pred_test)} samples")
        print(f"  Prediction stats:")
        print(f"    Mean: {pred_test.mean():.6f}")
        print(f"    Std:  {pred_test.std():.6f}")
        print(f"    Min:  {pred_test.min():.6f}")
        print(f"    Max:  {pred_test.max():.6f}")

        # Record signals
        print("\n" + "=" * 80)
        print("STEP 6: Recording Predictions")
        print("=" * 80)
        sr = SignalRecord(model=model, dataset=dataset, recorder=R.get_recorder())
        sr.generate()
        print("✓ Signal record generated")

        # Backtest
        print("\n" + "=" * 80)
        print("STEP 7: Running Backtest")
        print("=" * 80)
        port_analysis_config = config["port_analysis_config"]
        print(f"Strategy: {port_analysis_config['strategy']['class']}")
        print(f"Topk: {port_analysis_config['strategy']['kwargs']['topk']}")
        print(f"N_drop: {port_analysis_config['strategy']['kwargs']['n_drop']}")
        print(f"Backtest period: {port_analysis_config['backtest']['start_time']} to {port_analysis_config['backtest']['end_time']}")
        print(f"Initial account: ${port_analysis_config['backtest']['account']:,.0f}")

        par = PortAnaRecord(
            recorder=R.get_recorder(),
            config=port_analysis_config
        )
        par.generate()
        print("✓ Backtest complete")

        # Print results
        print("\n" + "=" * 80)
        print("STEP 8: Performance Analysis")
        print("=" * 80)

        # Get backtest results
        recorder = R.get_recorder()

        print("\n📊 BACKTEST RESULTS:")
        print("-" * 80)

        # Try to get portfolio analysis
        try:
            # Get the analysis report
            report = recorder.load_object("port_analysis.pkl")
            if report is not None:
                print("\n✓ Portfolio Analysis Report:")
                print(report)
        except Exception as e:
            print(f"\nNote: Portfolio analysis report not available: {e}")

        # Print experiment info
        print("\n📁 EXPERIMENT INFO:")
        print("-" * 80)
        print(f"Experiment ID: {R.get_exp().info['exp_name']}")
        print(f"Recorder ID: {recorder.info['rid']}")
        print(f"URI: {recorder.uri}")

        # List all saved artifacts
        print("\n📦 SAVED ARTIFACTS:")
        print("-" * 80)
        artifacts = recorder.list_artifacts()
        for artifact in artifacts:
            print(f"  • {artifact}")

        print("\n" + "=" * 80)
        print("✅ WORKFLOW COMPLETED SUCCESSFULLY!")
        print("=" * 80)

        return recorder


if __name__ == "__main__":
    # Path to configuration file
    config_path = Path(__file__).parent / "qlib_workflow_config.yaml"

    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)

    try:
        recorder = run_workflow(str(config_path))

        print("\n" + "=" * 80)
        print("NEXT STEPS:")
        print("=" * 80)
        print("1. Check the experiment results in the Qlib mlruns directory")
        print("2. Analyze the backtest report and signals")
        print("3. Tune hyperparameters for better performance")
        print("4. Try different feature sets or models")
        print("5. Extend the date range as more data becomes available")

    except Exception as e:
        print(f"\n❌ Error running workflow: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
