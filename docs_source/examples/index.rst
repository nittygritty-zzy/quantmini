Examples
========

Example scripts demonstrating QuantMini features.

Overview
--------

All examples are available in the `examples/ directory <https://github.com/nittygritty-zzy/quantmini/tree/main/examples>`_ of the repository.

Qlib Examples
-------------

**Qlib Model Examples**

* ``qlib_lgb_example.py`` - LightGBM model with Qlib
* ``qlib_xgb_example.py`` - XGBoost model with Qlib
* ``qlib_catboost_example.py`` - CatBoost model with Qlib

**Custom Models**

* ``qlib_custom_model_example.py`` - Custom model implementation

**Trading Strategies**

* ``qlib_topk_strategy_example.py`` - TopkDropout strategy
* ``qlib_enhanced_indexing_example.py`` - Enhanced indexing strategy

**Complete Workflow**

* ``qlib_complete_workflow_example.py`` - End-to-end ML workflow

Data Pipeline Examples
----------------------

Coming soon: Examples for data ingestion, feature engineering, and querying.

Installation
------------

To run the examples:

.. code-block:: bash

   # Install QuantMini with ML dependencies
   pip install quantmini[ml]

   # Clone the repository
   git clone https://github.com/nittygritty-zzy/quantmini.git
   cd quantmini

   # Run an example
   python examples/qlib_lgb_example.py

Running Examples
----------------

All Qlib examples follow the same pattern:

1. Suppress gym warnings
2. Initialize Qlib with binary data
3. Load and prepare data
4. Train model or run strategy
5. Evaluate results

Example structure:

.. code-block:: python

   # Suppress gym warnings (IMPORTANT: before importing qlib)
   from src.utils.suppress_gym_warnings import patch_gym
   patch_gym()

   import qlib
   from qlib.data import D

   # Initialize Qlib
   qlib.init(provider_uri='data/binary/stocks_daily', region='us')

   # Your code here...

See the `examples/ directory <https://github.com/nittygritty-zzy/quantmini/tree/main/examples>`_ for complete working examples.
