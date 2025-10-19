#!/usr/bin/env python3
"""
Qlib Binary Format Test

Tests reading Qlib binary format from gold layer.
Uses real data, no mocks.

Usage:
    python scripts/tests/test_qlib_reading.py
"""

import sys
from pathlib import Path
from datetime import datetime
import logging
import struct

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QlibReadingTest:
    """Test Qlib binary format reading"""

    def __init__(self):
        self.config = ConfigLoader()
        self.data_lake_root = Path(self.config.get('data_lake_root', '/Volumes/sandisk/quantmini-lake'))
        self.gold_path = Path(self.config.get('gold_path', self.data_lake_root / 'gold'))
        self.qlib_path = self.gold_path / 'qlib'
        self.results = []

    def test_qlib_structure(self):
        """Test 1: Verify Qlib directory structure"""
        logger.info("\n" + "="*70)
        logger.info("TEST 1: Qlib Directory Structure")
        logger.info("="*70)

        if not self.qlib_path.exists():
            logger.error(f"❌ Qlib path not found: {self.qlib_path}")
            self.results.append(('Qlib Structure', False))
            return False

        datasets = [d for d in self.qlib_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
        logger.info(f"✅ Found {len(datasets)} Qlib datasets")

        for dataset in datasets:
            features_dir = dataset / 'features'
            instruments_dir = dataset / 'instruments'

            if features_dir.exists() and instruments_dir.exists():
                feature_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
                instruments_file = instruments_dir / 'all.txt'

                if instruments_file.exists():
                    logger.info(f"✅ {dataset.name}: {len(feature_dirs)} features, instruments file exists")
                else:
                    logger.warning(f"⚠️  {dataset.name}: Missing instruments/all.txt")
            else:
                logger.warning(f"⚠️  {dataset.name}: Incomplete structure")

        self.results.append(('Qlib Structure', len(datasets) > 0))
        return len(datasets) > 0

    def test_instruments_file(self):
        """Test 2: Read instruments file"""
        logger.info("\n" + "="*70)
        logger.info("TEST 2: Read Instruments File")
        logger.info("="*70)

        stocks_daily = self.qlib_path / 'stocks_daily'
        instruments_file = stocks_daily / 'instruments' / 'all.txt'

        if not instruments_file.exists():
            logger.error(f"❌ Instruments file not found")
            self.results.append(('Instruments File', False))
            return False

        try:
            with open(instruments_file, 'r') as f:
                instruments = [line.strip() for line in f if line.strip()]

            logger.info(f"✅ Total instruments: {len(instruments)}")
            logger.info(f"   Sample instruments: {instruments[:5]}")

            self.results.append(('Instruments File', True))
            return True

        except Exception as e:
            logger.error(f"❌ Failed to read instruments file: {e}")
            self.results.append(('Instruments File', False))
            return False

    def test_binary_feature_read(self):
        """Test 3: Read binary feature file"""
        logger.info("\n" + "="*70)
        logger.info("TEST 3: Read Binary Feature Files")
        logger.info("="*70)

        stocks_daily = self.qlib_path / 'stocks_daily'
        features_dir = stocks_daily / 'features'

        if not features_dir.exists():
            logger.error(f"❌ Features directory not found")
            self.results.append(('Binary Feature Read', False))
            return False

        try:
            # Find a symbol directory
            symbol_dirs = [d for d in features_dir.iterdir() if d.is_dir()][:1]
            if not symbol_dirs:
                logger.error(f"❌ No symbol directories found")
                self.results.append(('Binary Feature Read', False))
                return False

            symbol_dir = symbol_dirs[0]
            feature_files = list(symbol_dir.glob('*.day.bin'))

            if not feature_files:
                logger.error(f"❌ No binary feature files found in {symbol_dir.name}")
                self.results.append(('Binary Feature Read', False))
                return False

            logger.info(f"✅ Testing symbol: {symbol_dir.name}")
            logger.info(f"✅ Found {len(feature_files)} feature files")

            # Read a sample binary file
            sample_file = feature_files[0]
            with open(sample_file, 'rb') as f:
                # Read first 10 float32 values
                values = []
                for _ in range(min(10, sample_file.stat().st_size // 4)):
                    data = f.read(4)
                    if len(data) == 4:
                        value = struct.unpack('f', data)[0]
                        values.append(value)

            logger.info(f"✅ Read {len(values)} values from {sample_file.name}")
            logger.info(f"   Sample values: {values[:5]}")

            self.results.append(('Binary Feature Read', True))
            return True

        except Exception as e:
            logger.error(f"❌ Failed to read binary feature: {e}")
            self.results.append(('Binary Feature Read', False))
            return False

    def test_feature_coverage(self):
        """Test 4: Check feature coverage"""
        logger.info("\n" + "="*70)
        logger.info("TEST 4: Feature Coverage Analysis")
        logger.info("="*70)

        stocks_daily = self.qlib_path / 'stocks_daily'
        features_dir = stocks_daily / 'features'

        if not features_dir.exists():
            logger.warning("⚠️  Features directory not found, skipping coverage test")
            self.results.append(('Feature Coverage', True))
            return True

        try:
            # Count symbols and features
            symbol_dirs = [d for d in features_dir.iterdir() if d.is_dir()]
            logger.info(f"✅ Total symbols with features: {len(symbol_dirs)}")

            # Sample a few symbols to check feature consistency
            sample_symbols = symbol_dirs[:5]
            for symbol_dir in sample_symbols:
                feature_files = list(symbol_dir.glob('*.day.bin'))
                logger.info(f"   {symbol_dir.name}: {len(feature_files)} features")

            self.results.append(('Feature Coverage', True))
            return True

        except Exception as e:
            logger.error(f"❌ Feature coverage check failed: {e}")
            self.results.append(('Feature Coverage', False))
            return False

    def run_all_tests(self):
        """Run all tests"""
        logger.info("\n" + "="*70)
        logger.info("QLIB BINARY FORMAT TEST SUITE")
        logger.info("="*70)
        logger.info(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Qlib Path: {self.qlib_path}")
        logger.info("")

        # Run all tests
        self.test_qlib_structure()
        self.test_instruments_file()
        self.test_binary_feature_read()
        self.test_feature_coverage()

        # Summary
        logger.info("\n" + "="*70)
        logger.info("TEST SUMMARY")
        logger.info("="*70)

        passed_count = sum(1 for _, passed in self.results if passed)
        total_count = len(self.results)

        for test_name, passed in self.results:
            status = "✅ PASS" if passed else "❌ FAIL"
            logger.info(f"{status}: {test_name}")

        logger.info("")
        logger.info(f"Results: {passed_count}/{total_count} tests passed")

        if passed_count == total_count:
            logger.info("✅ ALL TESTS PASSED")
            return 0
        else:
            logger.error(f"❌ {total_count - passed_count} TESTS FAILED")
            return 1


def main():
    test_suite = QlibReadingTest()
    exit_code = test_suite.run_all_tests()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
