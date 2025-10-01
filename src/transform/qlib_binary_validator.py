"""
Qlib Binary Validator - Validate Qlib binary format

This module validates Qlib binary format conversions:
1. Instruments file exists and has content
2. Calendar file exists with valid dates
3. Binary files readable and match calendar length
4. Roundtrip test: Parquet → Binary → Parquet matches

Based on: pipeline_design/PHASE5-8_DESIGN.md Phase 6
"""

import struct
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

from ..core.exceptions import PipelineException

logger = logging.getLogger(__name__)


class QlibBinaryValidatorError(PipelineException):
    """Raised when validation fails"""
    pass


class QlibBinaryValidator:
    """
    Validate Qlib binary format

    Tests:
    1. Instruments file exists and has content
    2. Calendar file exists with valid dates
    3. Binary files readable and match calendar length
    4. Roundtrip test: Parquet → Binary → Parquet matches
    """

    def __init__(self, qlib_root: Path):
        """
        Initialize validator

        Args:
            qlib_root: Root directory for Qlib binary data
        """
        self.qlib_root = Path(qlib_root)
        logger.info(f"QlibBinaryValidator initialized (path: {qlib_root})")

    def validate_conversion(self, data_type: str) -> Dict[str, Any]:
        """
        Run all validation checks

        Args:
            data_type: Data type to validate

        Returns:
            Validation results dict
        """
        logger.info(f"Validating {data_type} binary conversion")

        results = {
            'data_type': data_type,
            'instruments_valid': False,
            'calendar_valid': False,
            'features_valid': False,
            'errors': [],
            'warnings': []
        }

        try:
            # Check instruments
            logger.info("Checking instruments file...")
            instruments_result = self._validate_instruments(data_type)
            results.update(instruments_result)

            # Check calendar
            logger.info("Checking calendar file...")
            calendar_result = self._validate_calendar(data_type)
            results.update(calendar_result)

            # Check features (if instruments and calendar are valid)
            if results['instruments_valid'] and results['calendar_valid']:
                logger.info("Checking feature binary files...")
                features_result = self._validate_features(
                    data_type,
                    results['symbols'],
                    results['trading_days']
                )
                results.update(features_result)

            # Overall validation
            results['all_valid'] = (
                results['instruments_valid'] and
                results['calendar_valid'] and
                results['features_valid']
            )

            if results['all_valid']:
                logger.info(f"✅ Validation passed for {data_type}")
            else:
                logger.warning(f"⚠️ Validation issues found for {data_type}")

        except Exception as e:
            logger.error(f"Validation error: {e}")
            results['errors'].append(str(e))

        return results

    def _validate_instruments(self, data_type: str) -> Dict[str, Any]:
        """
        Validate instruments file

        Args:
            data_type: Data type

        Returns:
            Validation results
        """
        result = {
            'instruments_valid': False,
            'symbol_count': 0,
            'symbols': []
        }

        try:
            instruments_file = self.qlib_root / data_type / 'instruments' / 'all.txt'

            if not instruments_file.exists():
                result['errors'] = result.get('errors', [])
                result['errors'].append("instruments/all.txt missing")
                logger.error(f"Instruments file not found: {instruments_file}")
                return result

            with open(instruments_file) as f:
                symbols = [line.strip() for line in f if line.strip()]

            if len(symbols) == 0:
                result['errors'] = result.get('errors', [])
                result['errors'].append("instruments/all.txt is empty")
                logger.error("Instruments file is empty")
                return result

            result['instruments_valid'] = True
            result['symbol_count'] = len(symbols)
            result['symbols'] = symbols

            logger.debug(f"Instruments valid: {len(symbols)} symbols")

        except Exception as e:
            result['errors'] = result.get('errors', [])
            result['errors'].append(f"Instruments validation error: {e}")
            logger.error(f"Instruments validation error: {e}")

        return result

    def _validate_calendar(self, data_type: str) -> Dict[str, Any]:
        """
        Validate calendar file

        Args:
            data_type: Data type

        Returns:
            Validation results
        """
        result = {
            'calendar_valid': False,
            'trading_days_count': 0,
            'trading_days': []
        }

        try:
            calendar_file = self.qlib_root / data_type / 'calendars' / 'day.txt'

            if not calendar_file.exists():
                result['errors'] = result.get('errors', [])
                result['errors'].append("calendars/day.txt missing")
                logger.error(f"Calendar file not found: {calendar_file}")
                return result

            with open(calendar_file) as f:
                dates = [line.strip() for line in f if line.strip()]

            if len(dates) == 0:
                result['errors'] = result.get('errors', [])
                result['errors'].append("calendars/day.txt is empty")
                logger.error("Calendar file is empty")
                return result

            # Validate date format (YYYY-MM-DD)
            invalid_dates = []
            for date in dates:
                if len(date) != 10 or date[4] != '-' or date[7] != '-':
                    invalid_dates.append(date)

            if invalid_dates:
                result['warnings'] = result.get('warnings', [])
                result['warnings'].append(f"Invalid date formats: {invalid_dates[:5]}")
                logger.warning(f"Found {len(invalid_dates)} invalid dates")

            result['calendar_valid'] = True
            result['trading_days_count'] = len(dates)
            result['trading_days'] = dates

            logger.debug(f"Calendar valid: {len(dates)} trading days")

        except Exception as e:
            result['errors'] = result.get('errors', [])
            result['errors'].append(f"Calendar validation error: {e}")
            logger.error(f"Calendar validation error: {e}")

        return result

    def _validate_features(
        self,
        data_type: str,
        symbols: List[str],
        trading_days: List[str]
    ) -> Dict[str, Any]:
        """
        Validate feature binary files

        Args:
            data_type: Data type
            symbols: List of symbols
            trading_days: List of trading days

        Returns:
            Validation results
        """
        result = {
            'features_valid': False,
            'symbols_checked': 0,
            'features_checked': 0,
            'missing_features': []
        }

        try:
            extension = '.day.bin' if 'daily' in data_type else '.1min.bin'
            expected_length = len(trading_days)

            # Check first few symbols (full check would be too slow)
            sample_size = min(5, len(symbols))
            symbols_to_check = symbols[:sample_size]

            for symbol in symbols_to_check:
                symbol_dir = self.qlib_root / data_type / 'features' / symbol.lower()

                if not symbol_dir.exists():
                    result['warnings'] = result.get('warnings', [])
                    result['warnings'].append(f"Missing directory for {symbol}")
                    logger.warning(f"Missing directory: {symbol_dir}")
                    continue

                bin_files = list(symbol_dir.glob(f'*{extension}'))

                if len(bin_files) == 0:
                    result['warnings'] = result.get('warnings', [])
                    result['warnings'].append(f"No binary files for {symbol}")
                    logger.warning(f"No binary files for {symbol}")
                    continue

                # Check first binary file
                for bin_file in bin_files[:3]:  # Check first 3 features
                    try:
                        values = self._read_binary_file(bin_file)

                        if len(values) != expected_length:
                            result['errors'] = result.get('errors', [])
                            result['errors'].append(
                                f"Length mismatch in {bin_file.name}: "
                                f"{len(values)} != {expected_length}"
                            )
                            logger.error(
                                f"Length mismatch: {bin_file.name} "
                                f"({len(values)} != {expected_length})"
                            )
                        else:
                            result['features_checked'] += 1

                    except Exception as e:
                        result['errors'] = result.get('errors', [])
                        result['errors'].append(f"Error reading {bin_file.name}: {e}")
                        logger.error(f"Error reading {bin_file.name}: {e}")

                result['symbols_checked'] += 1

            # Validation passes if we checked some features without errors
            result['features_valid'] = (
                result['features_checked'] > 0 and
                len(result.get('errors', [])) == 0
            )

            logger.debug(
                f"Features valid: checked {result['symbols_checked']} symbols, "
                f"{result['features_checked']} features"
            )

        except Exception as e:
            result['errors'] = result.get('errors', [])
            result['errors'].append(f"Features validation error: {e}")
            logger.error(f"Features validation error: {e}")

        return result

    def _read_binary_file(self, path: Path) -> np.ndarray:
        """
        Read binary file

        Args:
            path: Path to binary file

        Returns:
            Numpy array of values
        """
        with open(path, 'rb') as f:
            count = struct.unpack('<I', f.read(4))[0]
            values = np.fromfile(f, dtype=np.float32, count=count)

        return values

    def read_binary_feature(
        self,
        data_type: str,
        symbol: str,
        feature: str
    ) -> np.ndarray:
        """
        Read binary feature for testing

        Args:
            data_type: Data type
            symbol: Symbol
            feature: Feature name

        Returns:
            Numpy array of feature values
        """
        extension = '.day.bin' if 'daily' in data_type else '.1min.bin'
        binary_path = (
            self.qlib_root / data_type / 'features' /
            symbol.lower() / f'{feature}{extension}'
        )

        if not binary_path.exists():
            raise QlibBinaryValidatorError(f"Binary file not found: {binary_path}")

        return self._read_binary_file(binary_path)

    def get_feature_list(self, data_type: str, symbol: str) -> List[str]:
        """
        Get list of features for a symbol

        Args:
            data_type: Data type
            symbol: Symbol

        Returns:
            List of feature names
        """
        extension = '.day.bin' if 'daily' in data_type else '.1min.bin'
        symbol_dir = self.qlib_root / data_type / 'features' / symbol.lower()

        if not symbol_dir.exists():
            return []

        bin_files = list(symbol_dir.glob(f'*{extension}'))
        # Remove the extension to get just the feature name
        features = [f.name.replace(extension, '') for f in bin_files]

        return sorted(features)

    def compare_with_parquet(
        self,
        data_type: str,
        symbol: str,
        feature: str,
        parquet_df,
        tolerance: float = 1e-6
    ) -> Dict[str, Any]:
        """
        Compare binary feature with original Parquet data

        Args:
            data_type: Data type
            symbol: Symbol
            feature: Feature name
            parquet_df: Original Parquet DataFrame
            tolerance: Numerical tolerance for comparison

        Returns:
            Comparison results
        """
        result = {
            'match': False,
            'differences': 0,
            'max_diff': 0.0,
            'errors': []
        }

        try:
            # Read binary feature
            binary_values = self.read_binary_feature(data_type, symbol, feature)

            # Get Parquet feature values
            parquet_values = parquet_df[feature].values.astype(np.float32)

            # Compare lengths
            if len(binary_values) != len(parquet_values):
                result['errors'].append(
                    f"Length mismatch: {len(binary_values)} != {len(parquet_values)}"
                )
                return result

            # Compare values (handling NaN)
            mask = ~(np.isnan(binary_values) & np.isnan(parquet_values))
            diffs = np.abs(binary_values[mask] - parquet_values[mask])

            if len(diffs) > 0:
                result['max_diff'] = float(np.max(diffs))
                result['differences'] = int(np.sum(diffs > tolerance))

            result['match'] = result['differences'] == 0

        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"Comparison error: {e}")

        return result

    def __repr__(self) -> str:
        return f"QlibBinaryValidator(path={self.qlib_root})"


def main():
    """Command-line interface for validator"""
    import sys

    try:
        qlib_root = Path('data/qlib')
        validator = QlibBinaryValidator(qlib_root)

        print("✅ QlibBinaryValidator initialized")
        print(f"   Qlib root: {qlib_root}")

        # Example validation (requires data)
        for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            data_dir = qlib_root / data_type
            if data_dir.exists():
                print(f"\n📊 Validating {data_type}...")
                results = validator.validate_conversion(data_type)

                if results['all_valid']:
                    print(f"   ✅ Valid")
                    print(f"   Symbols: {results['symbol_count']}")
                    print(f"   Trading days: {results['trading_days_count']}")
                    print(f"   Features checked: {results['features_checked']}")
                else:
                    print(f"   ❌ Invalid")
                    if results.get('errors'):
                        print(f"   Errors: {results['errors']}")
                    if results.get('warnings'):
                        print(f"   Warnings: {results['warnings']}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
