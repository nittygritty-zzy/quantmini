# Qlib API Reference

## Official Documentation

**IMPORTANT**: Always refer to the official Qlib documentation before making changes:
- **Qlib API Reference**: https://qlib.readthedocs.io/en/latest/reference/api.html

## Overview

This project uses Qlib as the quantitative research and backtesting framework. We convert our data into Qlib's binary format for efficient access during model training and backtesting.

## Integration Architecture (Medallion Pattern)

```
Landing Layer (Raw CSV.GZ)
    ↓
Bronze Layer (Validated Parquet)
    ↓
Silver Layer (Feature-Enriched Parquet)
    ↓
Gold Layer (Qlib Binary Format)  ← We are here
    ↓
Qlib Data Layer
    ↓
Models & Backtesting
```

## Qlib Data Layer

### Data Provider

We use Qlib's **LocalProvider** with binary format data:

```python
import qlib
from qlib.config import REG_CN

# Initialize Qlib with gold layer path
qlib.init(
    provider_uri='data/gold/qlib/stocks_daily',  # Gold layer
    region=REG_CN
)
```

### Data Format (Gold Layer)

Qlib expects data in a specific binary format stored in the gold layer:

```
data/gold/qlib/stocks_daily/        # Gold layer ML-ready format
├── calendars/
│   └── day.txt                     # Trading days list
├── instruments/
│   └── all.txt                     # Symbol list with date ranges
└── features/
    ├── aapl/                       # Lowercase symbol directories
    │   ├── open.day.bin            # Binary feature data
    │   ├── high.day.bin
    │   ├── low.day.bin
    │   ├── close.day.bin
    │   ├── volume.day.bin
    │   └── alpha_daily.day.bin     # Calculated features from silver layer
    └── tsla/
        └── ...
```

## Converting Data to Qlib Format

### Using Our Conversion Script

```bash
# Convert single data type
uv run python scripts/convert_to_qlib.py \
    --data-type stocks_daily \
    --start-date 2025-08-01 \
    --end-date 2025-09-30

# Convert all data types
uv run python scripts/convert_to_qlib.py \
    --data-type all \
    --start-date 2025-08-01 \
    --end-date 2025-09-30
```

### Programmatic Conversion

```python
from src.transform.qlib_binary_writer import QlibBinaryWriter
from pathlib import Path

# Initialize writer (converts silver → gold)
writer = QlibBinaryWriter(
    silver_root=Path('data/silver'),      # Input: feature-enriched data
    qlib_root=Path('data/gold/qlib')      # Output: ML-ready binary format
)

# Convert data
result = writer.convert_data_type(
    data_type='stocks_daily',
    start_date='2025-08-01',
    end_date='2025-09-30',
    incremental=True  # Skip already converted data
)

print(f"Converted {result['symbols_converted']} symbols")
print(f"Trading days: {result['trading_days']}")
print(f"Features: {result['features_written']}")
```

## Using Qlib for Research

### Data Retrieval

```python
import qlib
from qlib.data import D

# Initialize
qlib.init(provider_uri='data/qlib/stocks_daily')

# Get data for specific symbols
data = D.features(
    instruments=['AAPL', 'TSLA'],
    fields=['$open', '$high', '$low', '$close', '$volume'],
    start_time='2025-08-01',
    end_time='2025-09-30'
)

print(data.head())
```

### Feature Engineering with Expressions

Qlib supports powerful expression-based features:

```python
from qlib.data import D

# Use Qlib's expression language
data = D.features(
    instruments=['AAPL'],
    fields=[
        '$close',
        '($close - Ref($close, 1)) / Ref($close, 1)',  # Daily return
        'Mean($volume, 20)',                            # 20-day avg volume
        'Std($close, 20)',                              # 20-day volatility
    ],
    start_time='2025-08-01',
    end_time='2025-09-30'
)
```

## Qlib Workflow

### Model Training

```python
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord
from qlib.model.gbdt import LGBModel

# Define model
model = LGBModel(
    objective='regression',
    num_leaves=31,
    learning_rate=0.05
)

# Train
with R.start(experiment_name='my_experiment'):
    # Data handling
    data_handler_config = {
        'instruments': 'all',
        'start_time': '2025-08-01',
        'end_time': '2025-09-30',
        'fields': ['$close', '$volume']
    }

    # Train model
    model.fit(dataset=data_handler_config)

    # Record results
    R.save_objects(model=model)
```

### Backtesting

```python
from qlib.backtest import backtest
from qlib.contrib.strategy import TopkDropoutStrategy

# Define strategy
strategy = TopkDropoutStrategy(
    model=model,
    dataset=dataset,
    topk=30,
    n_drop=5
)

# Run backtest
report, positions = backtest(
    strategy=strategy,
    executor_config={
        'trade_exchange': {
            'class': 'SimulatorExchange',
            'freq': 'day'
        }
    }
)

# Analyze results
print(report.describe())
```

## Key Qlib Modules

### 1. Data Layer (`qlib.data`)

- **D.features()**: Retrieve feature data
- **D.instruments()**: Get instrument lists
- **D.calendar()**: Get trading calendar
- **PIT Database**: Point-in-time data access

### 2. Model Layer (`qlib.model`)

- **Base Models**: LGBModel, LinearModel, DNNModel
- **Custom Models**: Extend BaseModel
- **Ensemble**: Model combination

### 3. Workflow (`qlib.workflow`)

- **Experiment Tracking**: R.start(), R.log_metrics()
- **Task Management**: TaskGen, TaskManager
- **Record Management**: SignalRecord, PortfolioRecord

### 4. Backtest (`qlib.backtest`)

- **Strategy**: TopkDropout, WeightStrategy
- **Executor**: SimulatorExchange
- **Portfolio**: Position management

## Data Requirements

### Calendars

Trading day list in `calendars/day.txt`:

```
2025-08-01
2025-08-04
2025-08-05
...
2025-09-29
```

### Instruments

Symbol list with date ranges in `instruments/all.txt`:

```
AAPL 2025-08-01 2025-09-30
TSLA 2025-08-01 2025-09-30
MSFT 2025-08-01 2025-09-30
```

### Features

Binary files for each feature:
- **Format**: Little-endian float32
- **Size**: 4 bytes × number of trading days
- **Missing data**: NaN (0x7FC00000)

## Best Practices

### 1. Data Preparation

```python
# Always validate data before conversion
from src.transform.qlib_binary_validator import QlibBinaryValidator

validator = QlibBinaryValidator('data/qlib')
validation = validator.validate_conversion('stocks_daily')

if not validation['all_valid']:
    for error in validation['errors']:
        print(f"Error: {error}")
```

### 2. Memory Management

```python
# Use data in chunks for large datasets
import qlib
from qlib.data import D

# Process data in batches
symbols = D.instruments()
batch_size = 100

for i in range(0, len(symbols), batch_size):
    batch = symbols[i:i+batch_size]
    data = D.features(instruments=batch, fields=['$close'])
    # Process batch
```

### 3. Incremental Updates

```python
# Update only new data
writer = QlibBinaryWriter(
    enriched_root=Path('data/enriched'),
    qlib_root=Path('data/qlib')
)

result = writer.convert_data_type(
    data_type='stocks_daily',
    start_date='2025-09-30',  # Only new date
    end_date='2025-09-30',
    incremental=True
)
```

## Performance Optimization

### Binary Format Benefits

- **Fast Access**: Memory-mapped binary files
- **Efficient Storage**: ~10x smaller than Parquet
- **Quick Queries**: No decompression needed

### Qlib Optimization

```python
# Use Qlib's cache
qlib.init(
    provider_uri='data/qlib/stocks_daily',
    expression_cache=True,  # Cache computed expressions
    dataset_cache=True      # Cache loaded datasets
)
```

## Troubleshooting

### Common Issues

1. **Data Format Errors**
   ```python
   # Verify binary format
   from src.transform.qlib_binary_validator import QlibBinaryValidator

   validator = QlibBinaryValidator('data/qlib/stocks_daily')
   result = validator.validate_conversion('stocks_daily')

   if not result['all_valid']:
       print("Validation failed:", result['errors'])
   ```

2. **Missing Instruments**
   ```python
   # Check instruments file
   with open('data/qlib/stocks_daily/instruments/all.txt') as f:
       instruments = f.readlines()
   print(f"Found {len(instruments)} instruments")
   ```

3. **Calendar Mismatches**
   ```python
   # Verify calendar
   with open('data/qlib/stocks_daily/calendars/day.txt') as f:
       days = f.readlines()
   print(f"Trading days: {days[0].strip()} to {days[-1].strip()}")
   ```

## Integration Example

Complete workflow from Polygon.io to Qlib:

```python
import asyncio
from pathlib import Path
from src.orchestration.ingestion_orchestrator import IngestionOrchestrator
from src.features.feature_engineer import FeatureEngineer
from src.transform.qlib_binary_writer import QlibBinaryWriter
import qlib
from qlib.data import D

async def complete_pipeline():
    # 1. Download from Polygon.io
    orchestrator = IngestionOrchestrator()
    await orchestrator.ingest_date_range(
        data_type='stocks_daily',
        start_date='2025-08-01',
        end_date='2025-09-30'
    )

    # 2. Enrich features
    with FeatureEngineer() as engineer:
        engineer.enrich_date_range(
            data_type='stocks_daily',
            start_date='2025-08-01',
            end_date='2025-09-30'
        )

    # 3. Convert to Qlib format
    writer = QlibBinaryWriter(
        enriched_root=Path('data/enriched'),
        qlib_root=Path('data/qlib')
    )
    writer.convert_data_type(
        data_type='stocks_daily',
        start_date='2025-08-01',
        end_date='2025-09-30'
    )

    # 4. Use in Qlib
    qlib.init(provider_uri='data/qlib/stocks_daily')
    data = D.features(
        instruments=['AAPL'],
        fields=['$close', '$volume']
    )

    print(data.head())

asyncio.run(complete_pipeline())
```

## Related Documentation

- [Qlib Official API Reference](https://qlib.readthedocs.io/en/latest/reference/api.html)
- [Qlib Official Documentation](https://qlib.readthedocs.io/)
- [Feature Engineering Guide](../guides/feature-engineering.md)
- [Data Pipeline Architecture](../architecture/data-pipeline.md)
