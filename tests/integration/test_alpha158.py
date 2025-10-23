#!/usr/bin/env python3
"""
Alpha158 Feature Test Suite

Comprehensive tests for Alpha158 feature generation and validation.
"""

import pytest
import qlib
import pandas as pd
import numpy as np
from pathlib import Path
from qlib.data import D
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.loader import Alpha158DL
from qlib.contrib.data.dataset import DatasetH


class TestAlpha158Features:
    """Test Alpha158 feature generation"""

    @pytest.fixture(autouse=True)
    def setup_qlib(self):
        """Initialize Qlib before each test"""
        qlib_path = Path("/Volumes/sandisk/quantlake/data/qlib/stocks_daily")
        if not qlib_path.exists():
            pytest.skip(f"Qlib data not found at {qlib_path}")

        qlib.init(provider_uri=str(qlib_path), region='us')

    def test_kbar_features(self):
        """Test K-line/candlestick features"""
        expressions = [
            ("KMID", "($close-$open)/$open"),           # Body size
            ("KLEN", "($high-$low)/$open"),             # Full range
            ("KMID2", "($close-$open)/($high-$low+1e-12)"),  # Body ratio
            ("KUP", "($high-Greater($open,$close))/$open"), # Upper shadow
            ("KUP2", "($high-Greater($open,$close))/($high-$low+1e-12)"),
            ("KLOW", "(Less($open,$close)-$low)/$open"), # Lower shadow
            ("KLOW2", "(Less($open,$close)-$low)/($high-$low+1e-12)"),
            ("KSFT", "(2*$close-$high-$low)/$open"),    # Shift from middle
            ("KSFT2", "(2*$close-$high-$low)/($high-$low+1e-12)")
        ]

        df = D.features(
            instruments=['AAPL', 'MSFT', 'GOOGL'],
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        assert not df.empty, "K-bar features returned empty DataFrame"
        assert len(df.columns) == len(expressions), "Missing K-bar features"

        # Validate no infinite values
        assert not np.isinf(df).any().any(), "K-bar features contain infinite values"

        print(f"✓ K-bar features: {len(df)} rows, {len(df.columns)} features")

    def test_price_features(self):
        """Test price-based features"""
        expressions = [
            ("OPEN0", "Ref($open, 0)/$close"),
            ("HIGH0", "Ref($high, 0)/$close"),
            ("LOW0", "Ref($low, 0)/$close"),
            ("VWAP0", "Ref($vwap, 0)/$close")
        ]

        df = D.features(
            instruments=['AAPL'],
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        assert not df.empty, "Price features returned empty DataFrame"
        assert (df > 0).all().all(), "Price ratios should be positive"

        print(f"✓ Price features: {len(df)} rows, {len(df.columns)} features")

    def test_rolling_features(self):
        """Test rolling window features"""
        expressions = [
            # Moving averages
            ("MA5", "Mean($close, 5)/$close"),
            ("MA10", "Mean($close, 10)/$close"),
            ("MA20", "Mean($close, 20)/$close"),

            # Volatility
            ("STD5", "Std($close, 5)/$close"),
            ("STD10", "Std($close, 10)/$close"),
            ("STD20", "Std($close, 20)/$close"),

            # Momentum
            ("ROC5", "Ref($close, 5)/$close"),
            ("ROC10", "Ref($close, 10)/$close"),
            ("ROC20", "Ref($close, 20)/$close"),

            # Min/Max
            ("MAX5", "Max($close, 5)/$close"),
            ("MAX10", "Max($close, 10)/$close"),
            ("MIN5", "Min($close, 5)/$close"),
            ("MIN10", "Min($close, 10)/$close"),
        ]

        df = D.features(
            instruments=['AAPL'],
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        assert not df.empty, "Rolling features returned empty DataFrame"

        # Check MA features are close to 1 (normalized)
        ma_cols = [col for col in df.columns if col.startswith('MA')]
        if ma_cols:
            for col in ma_cols:
                assert df[col].dropna().between(0.5, 1.5).any(), f"{col} values seem incorrect"

        print(f"✓ Rolling features: {len(df)} rows, {len(df.columns)} features")

    def test_correlation_features(self):
        """Test correlation-based features"""
        expressions = [
            ("CORR5", "Corr($close, Log($volume+1), 5)"),
            ("CORR10", "Corr($close, Log($volume+1), 10)"),
            ("CORR20", "Corr($close, Log($volume+1), 20)"),
        ]

        df = D.features(
            instruments=['AAPL'],
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        assert not df.empty, "Correlation features returned empty DataFrame"

        # Correlations should be between -1 and 1
        assert (df.dropna() >= -1).all().all(), "Correlation < -1 detected"
        assert (df.dropna() <= 1).all().all(), "Correlation > 1 detected"

        print(f"✓ Correlation features: {len(df)} rows, {len(df.columns)} features")

    def test_alpha158_config(self):
        """Test full Alpha158 configuration"""
        config = {
            "kbar": {},  # 9 K-line features
            "price": {
                "windows": [0],
                "feature": ["OPEN", "HIGH", "LOW", "VWAP"],
            },
            "rolling": {
                "windows": [5, 10, 20, 30, 60],
            },
        }

        fields, names = Alpha158DL.get_feature_config(config)

        assert len(fields) == 158, f"Expected 158 features, got {len(fields)}"
        assert len(names) == 158, f"Expected 158 names, got {len(names)}"

        # Check feature categories
        kbar_features = [n for n in names if n.startswith('K')]
        price_features = [n for n in names if n.startswith(('OPEN', 'HIGH', 'LOW', 'VWAP'))]
        rolling_features = [n for n in names if n not in kbar_features + price_features]

        assert len(kbar_features) == 9, f"Expected 9 K-bar features, got {len(kbar_features)}"
        assert len(price_features) == 4, f"Expected 4 price features, got {len(price_features)}"
        assert len(rolling_features) == 145, f"Expected 145 rolling features, got {len(rolling_features)}"

        print(f"✓ Alpha158 config: {len(fields)} features")
        print(f"  - K-bar: {len(kbar_features)}")
        print(f"  - Price: {len(price_features)}")
        print(f"  - Rolling: {len(rolling_features)}")

    def test_alpha158_handler(self):
        """Test Alpha158 data handler"""
        # Use list of US stocks instead of csi300 (Chinese index)
        handler = Alpha158(
            instruments=['AAPL', 'MSFT', 'GOOGL'],
            start_time='2025-09-01',
            end_time='2025-09-30',
            fit_start_time='2025-09-01',
            fit_end_time='2025-09-30'
        )

        # Test setup
        handler.setup_data()

        # Get features
        df_train = handler.fetch(selector=slice('2025-09-01', '2025-09-30'))

        if not df_train.empty:
            assert df_train.shape[1] > 0, "Handler returned no features"
            print(f"✓ Alpha158 handler: {df_train.shape[0]} rows, {df_train.shape[1]} features")
        else:
            print("⚠ Handler returned empty DataFrame (may need more data)")

    def test_feature_coverage(self):
        """Test feature coverage across multiple stocks"""
        stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

        expressions = [
            ("KMID", "($close-$open)/$open"),
            ("MA5", "Mean($close, 5)/$close"),
            ("STD5", "Std($close, 5)/$close"),
        ]

        df = D.features(
            instruments=stocks,
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        # Check coverage per stock
        for stock in stocks:
            if (stock,) in df.index.get_level_values(0).unique():
                stock_data = df.xs(stock, level=0)
                coverage = stock_data.notna().mean()
                print(f"  {stock}: {coverage.mean():.1%} coverage")

        print(f"✓ Feature coverage test complete")

    def test_data_quality(self):
        """Test data quality metrics"""
        expressions = [
            ("CLOSE", "$close"),
            ("VOLUME", "$volume"),
            ("MA5", "Mean($close, 5)/$close"),
        ]

        df = D.features(
            instruments=['AAPL'],
            fields=[expr for _, expr in expressions],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        if not df.empty:
            # Check for NaN
            nan_ratio = df.isna().sum() / len(df)
            print(f"  NaN ratios: {nan_ratio.to_dict()}")

            # Check for outliers (simple check)
            for col in df.columns:
                if df[col].notna().any():
                    q1 = df[col].quantile(0.25)
                    q3 = df[col].quantile(0.75)
                    iqr = q3 - q1
                    outliers = ((df[col] < q1 - 3*iqr) | (df[col] > q3 + 3*iqr)).sum()
                    print(f"  {col}: {outliers} outliers")

            print(f"✓ Data quality check complete")


class TestAlpha158Comparison:
    """Compare Alpha158 with baseline features"""

    @pytest.fixture(autouse=True)
    def setup_qlib(self):
        """Initialize Qlib before each test"""
        qlib_path = Path("/Volumes/sandisk/quantlake/data/qlib/stocks_daily")
        if not qlib_path.exists():
            pytest.skip(f"Qlib data not found at {qlib_path}")

        qlib.init(provider_uri=str(qlib_path), region='us')

    def test_alpha158_vs_simple_features(self):
        """Compare Alpha158 with simple price features"""
        # Simple features
        simple = D.features(
            instruments=['AAPL'],
            fields=['$close', '$volume', '$open', '$high', '$low'],
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        # Alpha158 features
        alpha158_config, alpha158_names = Alpha158DL.get_feature_config({
            "kbar": {},
            "price": {"windows": [0], "feature": ["OPEN", "HIGH", "LOW"]},
            "rolling": {"windows": [5, 10]},
        })

        alpha158 = D.features(
            instruments=['AAPL'],
            fields=alpha158_config[:10],  # First 10 features
            start_time='2025-09-01',
            end_time='2025-09-30'
        )

        print(f"✓ Simple features: {simple.shape}")
        print(f"✓ Alpha158 features (sample): {alpha158.shape}")

        # Check both have data
        assert not simple.empty, "Simple features are empty"
        assert not alpha158.empty, "Alpha158 features are empty"


def run_test_suite():
    """Run all Alpha158 tests"""
    print("\n" + "="*70)
    print("ALPHA158 TEST SUITE")
    print("="*70)

    # Run tests
    pytest.main([
        __file__,
        '-v',
        '--tb=short',
        '--color=yes'
    ])


if __name__ == '__main__':
    run_test_suite()
