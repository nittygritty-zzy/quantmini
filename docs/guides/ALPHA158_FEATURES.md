# Alpha158 Feature Guide

Complete guide to using Alpha158 features in QuantMini.

## What is Alpha158?

Alpha158 is a comprehensive feature set used in quantitative trading that includes **158 technical indicators** across three categories:

1. **K-line Features (9)** - Candlestick patterns
2. **Price Features (4)** - Basic price indicators
3. **Rolling Features (145)** - Time-series statistics

## Feature Breakdown

### K-Line Features (9 features)
Capture candlestick patterns and price action:

```python
KMID   = (close - open) / open              # Body size
KLEN   = (high - low) / open                # Full range
KMID2  = (close - open) / (high - low)      # Body ratio
KUP    = (high - max(open,close)) / open    # Upper shadow
KUP2   = (high - max(open,close)) / (high - low)
KLOW   = (min(open,close) - low) / open     # Lower shadow
KLOW2  = (min(open,close) - low) / (high - low)
KSFT   = (2*close - high - low) / open      # Shift from middle
KSFT2  = (2*close - high - low) / (high - low)
```

### Price Features (4 features)
Current price levels relative to close:

```python
OPEN0  = open / close
HIGH0  = high / close
LOW0   = low / close
VWAP0  = vwap / close
```

### Rolling Features (145 features)
Time-series statistics over windows [5, 10, 20, 30, 60]:

**Trend & Momentum:**
- `MA{n}` - Moving average ratio
- `ROC{n}` - Rate of change
- `MAX{n}`, `MIN{n}` - Rolling extremes
- `QTLU{n}`, `QTLD{n}` - Quantile position

**Volatility:**
- `STD{n}` - Standard deviation
- `RSQR{n}` - R-squared with time

**Volume-Price:**
- `CORR{n}` - Price-volume correlation
- `CORD{n}` - Return-volume delta correlation
- `BETA{n}` - Beta coefficient

**Position:**
- `IMAX{n}`, `IMIN{n}` - Days since extreme
- `IMXD{n}` - Max drawdown position

**Counts:**
- `CNTP{n}`, `CNTN{n}` - Positive/negative counts
- `CNTD{n}` - Down move counts

## Usage

### Quick Start

```python
import qlib
from qlib.data import D
from qlib.contrib.data.loader import Alpha158DL

# Initialize
qlib.init(provider_uri='./data/qlib/stocks_daily', region='us')

# Get Alpha158 config
config = {
    "kbar": {},
    "price": {"windows": [0], "feature": ["OPEN", "HIGH", "LOW", "VWAP"]},
    "rolling": {"windows": [5, 10, 20, 30, 60]},
}
fields, names = Alpha158DL.get_feature_config(config)

# Fetch features
df = D.features(
    instruments=['AAPL', 'MSFT'],
    fields=fields,
    start_time='2025-09-01',
    end_time='2025-09-30'
)
```

### With Data Handler

```python
from qlib.contrib.data.handler import Alpha158

handler = Alpha158(
    instruments='csi300',
    start_time='2025-01-01',
    end_time='2025-09-30',
    fit_start_time='2025-01-01',
    fit_end_time='2025-06-30'
)

handler.setup_data()
df_train = handler.fetch(selector=slice('2025-01-01', '2025-06-30'))
df_test = handler.fetch(selector=slice('2025-07-01', '2025-09-30'))
```

### Custom Configuration

```python
# Minimal config - fewer features
config = {
    "kbar": {},
    "rolling": {"windows": [5, 20]}  # Only 5 and 20 day windows
}
fields, names = Alpha158DL.get_feature_config(config)
# Returns ~70 features instead of 158

# Extended config - more features
config = {
    "kbar": {},
    "price": {"windows": [0], "feature": ["OPEN", "HIGH", "LOW", "VWAP"]},
    "rolling": {"windows": [3, 5, 10, 20, 30, 60, 120]}  # More windows
}
fields, names = Alpha158DL.get_feature_config(config)
# Returns ~200+ features
```

## Testing

### Run Demo Script

```bash
uv run python examples/alpha158_demo.py
```

This will:
1. Show K-line feature examples
2. Demo rolling window features
3. Show volume-price correlations
4. Display full Alpha158 config
5. Compare across multiple stocks

### Run Test Suite

```bash
# Run all Alpha158 tests
uv run pytest tests/integration/test_alpha158.py -v

# Run specific test
uv run pytest tests/integration/test_alpha158.py::TestAlpha158Features::test_kbar_features -v
```

Tests cover:
- K-bar feature calculation
- Price features
- Rolling window features
- Correlation features
- Full Alpha158 config
- Feature coverage
- Data quality

## Data Requirements

Alpha158 requires historical data with:
- **Minimum history:** 60 trading days (for 60-day rolling features)
- **Required fields:** open, high, low, close, volume, vwap
- **Data format:** Qlib binary format

### Convert Data to Qlib

```bash
uv run python scripts/convert_to_qlib.py \
    --data-type stocks_daily \
    --start-date 2025-08-01 \
    --end-date 2025-09-30 \
    --enriched-root /path/to/enriched \
    --qlib-root /path/to/qlib \
    --no-incremental
```

## Comparison: Alpha158 vs Alpha360

| Feature | Alpha158 | Alpha360 |
|---------|----------|----------|
| Total Features | 158 | 360 |
| K-line Features | 9 | 9 |
| Price Features | 4 | 4 |
| Rolling Windows | 5 (5,10,20,30,60) | 9 (5,10,20,30,60,120,180,240,360) |
| Feature Types | 11 types | 20+ types |
| Memory Usage | Lower | Higher |
| Training Speed | Faster | Slower |
| Accuracy | Good | Potentially better |

**When to use Alpha158:**
- Faster training needed
- Limited memory/compute
- Shorter timeframes (< 60 days lookback)
- Starting point for experimentation

**When to use Alpha360:**
- Maximum accuracy desired
- Sufficient compute resources
- Longer timeframes (360+ days lookback)
- Production models

## Examples

### Example 1: Single Stock Analysis

```python
import qlib
from qlib.data import D

qlib.init(provider_uri='./data/qlib/stocks_daily', region='us')

# Get MA and volatility features
df = D.features(
    instruments=['AAPL'],
    fields=[
        'Mean($close, 5)/$close',   # MA5
        'Mean($close, 20)/$close',  # MA20
        'Std($close, 5)/$close',    # 5-day volatility
        'Std($close, 20)/$close',   # 20-day volatility
    ],
    start_time='2025-09-01',
    end_time='2025-09-30'
)

df.columns = ['MA5', 'MA20', 'STD5', 'STD20']
print(df.head())
```

### Example 2: Multi-Stock Feature Matrix

```python
stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

config = {
    "kbar": {},
    "price": {"windows": [0], "feature": ["OPEN", "HIGH", "LOW"]},
    "rolling": {"windows": [5, 20]}
}

fields, names = Alpha158DL.get_feature_config(config)

df = D.features(
    instruments=stocks,
    fields=fields,
    start_time='2025-09-01',
    end_time='2025-09-30'
)

# Analyze per stock
for stock in stocks:
    stock_data = df.xs(stock, level=0)
    print(f"{stock}: {stock_data.shape}")
```

### Example 3: Feature Selection

```python
from sklearn.feature_selection import SelectKBest, f_regression

# Get all Alpha158 features
config = {"kbar": {}, "price": {"windows": [0], "feature": ["OPEN", "HIGH", "LOW", "VWAP"]}, "rolling": {"windows": [5, 10, 20, 30, 60]}}
fields, names = Alpha158DL.get_feature_config(config)

df = D.features(
    instruments=['AAPL'],
    fields=fields,
    start_time='2025-09-01',
    end_time='2025-09-30'
)

# Calculate target (next day return)
target = D.features(
    instruments=['AAPL'],
    fields=['Ref($close, -1)/$close - 1'],
    start_time='2025-09-01',
    end_time='2025-09-30'
)

# Select top K features
selector = SelectKBest(f_regression, k=50)
X_selected = selector.fit_transform(df.fillna(0), target.fillna(0))

# Get selected feature names
selected_features = [names[i] for i in selector.get_support(indices=True)]
print(f"Top 50 features: {selected_features}")
```

## Troubleshooting

### Empty DataFrame
```python
# Check date range has data
df = D.features(['AAPL'], ['$close'], start_time='2025-09-01', end_time='2025-09-30')
print(f"Rows: {len(df)}")  # Should be > 0
```

### Insufficient History
```python
# Rolling features need history
# For 60-day features, need at least 60 days of data
# Use shorter windows if limited data:
config = {"rolling": {"windows": [5, 10]}}  # Instead of [5,10,20,30,60]
```

### NaN Values
```python
# First N days will have NaN for N-day rolling features
df = df.dropna()  # Remove rows with any NaN
# Or
df = df.fillna(method='ffill')  # Forward fill
# Or
df = df.fillna(0)  # Fill with 0
```

## References

- [Qlib Documentation](https://qlib.readthedocs.io/)
- [Alpha158 Paper](https://arxiv.org/abs/2002.11100)
- QuantMini examples: `examples/alpha158_demo.py`
- QuantMini tests: `tests/integration/test_alpha158.py`
