QuantMini Documentation
========================

**QuantMini** is a high-performance quantitative trading pipeline with Qlib integration for ML-driven strategies.

.. image:: https://img.shields.io/pypi/v/quantmini.svg
   :target: https://pypi.org/project/quantmini/
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/quantmini.svg
   :target: https://pypi.org/project/quantmini/
   :alt: Python versions

.. image:: https://img.shields.io/github/license/nittygritty-zzy/quantmini.svg
   :target: https://github.com/nittygritty-zzy/quantmini/blob/main/LICENSE
   :alt: License

Features
--------

* **Data Pipeline**: Efficient ingestion from Polygon.io API
* **Qlib Integration**: Binary format conversion for fast backtesting
* **Alpha Expressions**: Comprehensive framework for creating trading signals
* **ML Models**: Support for LightGBM, XGBoost, and CatBoost
* **Trading Strategies**: TopkDropoutStrategy and EnhancedIndexingStrategy
* **Comprehensive Examples**: 10+ example scripts demonstrating all features

Installation
------------

Install via pip:

.. code-block:: bash

   # Basic installation
   pip install quantmini

   # With ML models
   pip install quantmini[ml]

   # With development tools
   pip install quantmini[dev]

   # Everything
   pip install quantmini[all]

Quick Start
-----------

Check out our :doc:`getting_started` guide or explore the :doc:`examples/index`.

.. toctree::
   :maxdepth: 2
   :caption: User Guide:

   getting_started
   installation
   guides/index
   examples/index

.. toctree::
   :maxdepth: 2
   :caption: API Reference:

   api/index

.. toctree::
   :maxdepth: 1
   :caption: About:

   changelog

.. toctree::
   :maxdepth: 1
   :caption: Development:

   contributing
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
