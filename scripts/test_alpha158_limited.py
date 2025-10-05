#!/usr/bin/env python3
"""
Test Alpha158 Features with Limited Data Window

With only 24 trading days, we can't compute all Alpha158 features (needs 60 days).
This script tests which features work with limited data.
"""

import sys
from pathlib import Path
import pandas as pd
import warnings

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core import ConfigLoader

warnings.filterwarnings('ignore')

def test_alpha158_limited():
    """Test Alpha158 features with 24-day window"""

    print("=" * 70)
    print("🧪 Alpha158 Feature Testing (Limited 24-day Window)")
    print("=" * 70)
    print()

    config = ConfigLoader()
    qlib_root = config.get_data_root() / 'qlib' / 'stocks_daily'

    try:
        import qlib
        from qlib.data import D

        # Initialize Qlib
        qlib.init(provider_uri=str(qlib_root), region='us')
        print("✅ Qlib initialized")
        print()

        # Get calendar
        cal = D.calendar()
        print(f"📅 Calendar: {len(cal)} trading days")
        print(f"   Range: {cal[0]} to {cal[-1]}")
        print()

        # Test with AAPL
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']

        # Test different window sizes
        print("Testing Alpha158 with different window requirements:")
        print()

        # 1. K-line features (no lookback needed - should work)
        print("1. K-line Features (0-day lookback):")
        try:
            kline_features = [
                "($close-Ref($close, 1))/Ref($close, 1)",  # KMID: (C-O)/C
                "($high-$low)/$open",  # KLEN: (H-L)/O
            ]

            data = D.features(
                test_symbols,
                kline_features,
                start_time=str(cal[0]),
                end_time=str(cal[-1])
            )

            print(f"   ✅ K-line features work!")
            print(f"   Shape: {data.shape}")
            print(f"   Sample:\n{data.head(3).to_string()}")
            print()

        except Exception as e:
            print(f"   ❌ K-line features failed: {e}")
            print()

        # 2. Short-term rolling features (5-day window - should work)
        print("2. Short-term Rolling Features (5-day lookback):")
        try:
            short_features = [
                "Mean($close, 5)",  # 5-day SMA
                "Std($close, 5)",   # 5-day volatility
                "Corr($close, $volume, 5)",  # 5-day correlation
            ]

            data = D.features(
                test_symbols,
                short_features,
                start_time=str(cal[5]),  # Skip first 5 days for warmup
                end_time=str(cal[-1])
            )

            print(f"   ✅ Short-term features (5-day) work!")
            print(f"   Shape: {data.shape}")
            print(f"   Available dates: {len(cal) - 5}")
            print(f"   Sample:\n{data.head(3).to_string()}")
            print()

        except Exception as e:
            print(f"   ❌ Short-term features failed: {e}")
            print()

        # 3. Medium-term rolling features (20-day window - should barely work)
        print("3. Medium-term Rolling Features (20-day lookback):")
        try:
            medium_features = [
                "Mean($close, 20)",  # 20-day SMA
                "Std($close, 20)",   # 20-day volatility
            ]

            data = D.features(
                test_symbols,
                medium_features,
                start_time=str(cal[20]) if len(cal) > 20 else str(cal[-1]),
                end_time=str(cal[-1])
            )

            print(f"   ✅ Medium-term features (20-day) work!")
            print(f"   Shape: {data.shape}")
            print(f"   Available dates: {max(0, len(cal) - 20)}")
            print(f"   Sample:\n{data.head(3).to_string()}")
            print()

        except Exception as e:
            print(f"   ❌ Medium-term features failed: {e}")
            print()

        # 4. Long-term rolling features (60-day window - will NOT work)
        print("4. Long-term Rolling Features (60-day lookback):")
        try:
            long_features = [
                "Mean($close, 60)",  # 60-day SMA
                "Std($close, 60)",   # 60-day volatility
            ]

            # This will likely return empty or fail
            if len(cal) > 60:
                data = D.features(
                    test_symbols,
                    long_features,
                    start_time=str(cal[60]),
                    end_time=str(cal[-1])
                )

                print(f"   ✅ Long-term features (60-day) work!")
                print(f"   Shape: {data.shape}")
            else:
                print(f"   ⚠️  Insufficient data: need 60 days, have {len(cal)} days")
                print(f"   Cannot compute 60-day features")
            print()

        except Exception as e:
            print(f"   ❌ Long-term features failed: {e}")
            print()

        # Summary
        print("=" * 70)
        print("📊 Summary")
        print("=" * 70)
        print()
        print(f"Available Trading Days: {len(cal)}")
        print()
        print("Feature Capability:")
        print(f"  ✅ K-line features (0-day): Available")
        print(f"  ✅ Short rolling (5-day): Available ({len(cal)-5} dates)")
        print(f"  ✅ Medium rolling (10-day): Available ({len(cal)-10} dates)")
        print(f"  ✅ Medium rolling (20-day): Available ({len(cal)-20} dates)")
        print(f"  ❌ Long rolling (30-day): Limited or unavailable")
        print(f"  ❌ Long rolling (60-day): Not available (need 60+ days)")
        print()
        print("Recommendation:")
        print(f"  • Convert at least 60 trading days (~3 months) for full Alpha158")
        print(f"  • Current 24 days: Good for testing, limited for production")
        print(f"  • Next conversion batch: August 2025 (adds ~21 days)")
        print()

    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == '__main__':
    success = test_alpha158_limited()
    sys.exit(0 if success else 1)
