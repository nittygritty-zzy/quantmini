# Examples Directory

Example scripts demonstrating how to use the QuantMini pipeline and Qlib integration.

## Quick Start - Qlib Examples

### Model Examples (Clean Output - Recommended)
```bash
# Simple model: train, predict, save/load
uv run python examples/qlib_model_example_clean.py 1

# Compare models: LightGBM vs XGBoost vs CatBoost
uv run python examples/qlib_model_example_clean.py 2
```

### Complete Workflow
```bash
uv run python examples/run_qlib_workflow.py
```

## Available Examples

### Qlib Model Examples
- **qlib_model_example_clean.py** ✅ - Model examples with clean output (no warnings)
- **qlib_custom_model_example.py** ✅ - Custom model implementation using base class interface
- **qlib_alpha_example.py** ✅ - Alpha expressions for factor creation and testing
- **qlib_strategy_example.py** ✅ - TopkDropoutStrategy for portfolio construction
- **qlib_enhanced_indexing_example.py** ✅ - EnhancedIndexingStrategy explanation and alternatives
- **run_qlib_workflow.py** - Complete workflow: data → features → training → predictions
- **qlib_workflow_config.yaml** - Configuration for workflow example

### Query Engine Examples
- **test_query_engine.py** - Query engine and caching demonstrations
- **qlib_integration_example.py** - Using QueryEngine with Qlib for ML pipelines

## Running Examples

All examples should be run from the project root using `uv`:

```bash
# Qlib model examples (clean output)
uv run python examples/qlib_model_example_clean.py 1  # Simple model
uv run python examples/qlib_model_example_clean.py 2  # Model comparison

# Qlib custom model examples
uv run python examples/qlib_custom_model_example.py 1  # Custom model implementation
uv run python examples/qlib_custom_model_example.py 2  # Interface demonstration

# Qlib alpha expressions
uv run python examples/qlib_alpha_example.py  # Alpha factor creation and testing

# Qlib strategy examples
uv run python examples/qlib_strategy_example.py            # TopkDropoutStrategy demonstration
uv run python examples/qlib_enhanced_indexing_example.py   # EnhancedIndexingStrategy explanation

# Qlib workflow
uv run python examples/run_qlib_workflow.py

# Query engine examples
uv run python examples/test_query_engine.py
uv run python examples/qlib_integration_example.py
```

## Prerequisites

1. Data must be ingested (see [E2E_TEST_INSTRUCTIONS.md](../docs/E2E_TEST_INSTRUCTIONS.md))
2. Configure `DATA_ROOT` in `.env` or `config/pipeline_config.yaml`
3. For Qlib examples, run binary conversion first:
   ```bash
   uv run python scripts/convert_to_qlib.py
   ```

## What You'll Learn

### Qlib Model Examples
- **qlib_model_example_clean.py**:
  - Creating and training models (LightGBM, XGBoost, CatBoost)
  - Making predictions on validation and test sets
  - Saving and loading trained models
  - Comparing model performance and correlations
  - **Clean output without deprecation warnings** ✅

- **qlib_custom_model_example.py**:
  - Implementing custom models using base Model class
  - Custom Linear Regression and Random Forest models
  - Proper fit() and predict() method implementation
  - Handling NaN values in features
  - Feature importance tracking
  - Model serialization (save/load)
  - Comparing custom models with built-in models
  - **Clean output + Base class interface demonstration** ✅

- **qlib_strategy_example.py**:
  - TopkDropoutStrategy for portfolio construction
  - Strategy parameters: topk (portfolio size) and n_drop (turnover)
  - Comparing different strategy configurations
  - Trading decision demonstration
  - Turnover management and optimization
  - Strategy method options (method_sell, method_buy, hold_thresh)
  - **Clean output + Strategy fundamentals** ✅

- **qlib_alpha_example.py**:
  - Understanding alpha expressions and syntax
  - Basic alpha patterns (momentum, mean reversion, volume)
  - Loading and evaluating alpha expressions
  - Comparing multiple alphas (correlation analysis)
  - Using alphas in trading strategies
  - Advanced alpha examples (WorldQuant-style)
  - **Clean output + Comprehensive alpha guide** ✅

- **qlib_enhanced_indexing_example.py**:
  - EnhancedIndexingStrategy conceptual explanation
  - Comparison with TopkDropoutStrategy
  - Risk model data requirements
  - Alternative approaches (weighted portfolios)
  - Practical recommendations for strategy selection
  - Guide on preparing risk model data
  - **Clean output + Educational demonstration** ✅

### Qlib Workflow Example
- **run_qlib_workflow.py**:
  - Complete ML workflow from data to predictions
  - Alpha158 feature engineering (158 technical indicators)
  - Model training with experiment tracking
  - Backtest analysis

### Query Engine Examples
- **test_query_engine.py**: Query caching, performance optimization
- **qlib_integration_example.py**:
  - QueryEngine for flexible data exploration
  - Qlib API for ML pipelines
  - Combined workflow (research to production)
  - Performance comparisons

## About Gym Warnings

Some Qlib examples may show deprecation warnings from the `gym` library. This is because Qlib uses `gym` for RL features (which we don't use).

**Solution**: Use the `*_clean.py` versions which suppress these warnings automatically.

See `../QLIB_GYM_WARNING.md` for detailed information.

## Documentation

- `QLIB_WORKFLOW_README.md` - Detailed workflow guide
- `../QLIB_BINARY_WRITER_UPDATES.md` - Data conversion updates
- `../QLIB_GYM_WARNING.md` - Gym warning explanation and solutions
- `../docs/api-reference/qlib.md` - API reference

See code comments for detailed explanations.
