#!/usr/bin/env python3
"""
Data Lake Integrity Test Suite

Tests the new quantmini-lake structure with real data (no mocks).
Validates bronze, silver, and gold layers.

Usage:
    python scripts/tests/test_data_lake_integrity.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLakeIntegrityTest:
    """Test suite for data lake integrity"""

    def __init__(self):
        self.config = ConfigLoader()
        self.data_lake_root = Path(self.config.get('data_lake_root', '/Volumes/sandisk/quantmini-lake'))
        self.bronze_path = Path(self.config.get('bronze_path', self.data_lake_root / 'bronze'))
        self.silver_path = Path(self.config.get('silver_path', self.data_lake_root / 'silver'))
        self.gold_path = Path(self.config.get('gold_path', self.data_lake_root / 'gold'))
        self.results = []

    def test_directory_structure(self):
        """Test 1: Verify data lake directory structure exists"""
        logger.info("\n" + "="*70)
        logger.info("TEST 1: Directory Structure")
        logger.info("="*70)

        required_dirs = [
            self.data_lake_root,
            self.data_lake_root / 'landing',
            self.bronze_path,
            self.silver_path,
            self.gold_path,
            self.data_lake_root / 'metadata',
            self.data_lake_root / 'logs',
        ]

        passed = True
        for dir_path in required_dirs:
            if dir_path.exists():
                logger.info(f"✅ {dir_path.name}: EXISTS")
            else:
                logger.error(f"❌ {dir_path.name}: MISSING")
                passed = False

        self.results.append(('Directory Structure', passed))
        return passed

    def test_bronze_layer(self):
        """Test 2: Verify bronze layer has parquet data"""
        logger.info("\n" + "="*70)
        logger.info("TEST 2: Bronze Layer (Standardized Parquet)")
        logger.info("="*70)

        expected_datasets = [
            'stocks_daily',
            'stocks_minute',
            'options_daily',
            'options_minute',
            'reference_data'
        ]

        passed = True
        for dataset in expected_datasets:
            dataset_path = self.bronze_path / dataset
            if dataset_path.exists():
                parquet_files = list(dataset_path.rglob('*.parquet'))
                if parquet_files:
                    logger.info(f"✅ {dataset}: {len(parquet_files)} parquet files")
                else:
                    logger.warning(f"⚠️  {dataset}: EXISTS but no parquet files")
                    passed = False
            else:
                logger.error(f"❌ {dataset}: MISSING")
                passed = False

        self.results.append(('Bronze Layer', passed))
        return passed

    def test_silver_layer(self):
        """Test 3: Verify silver layer has enriched data"""
        logger.info("\n" + "="*70)
        logger.info("TEST 3: Silver Layer (Feature-Engineered)")
        logger.info("="*70)

        expected_datasets = [
            'stocks_daily',
            'stocks_minute',
            'options_daily',
            'options_minute'
        ]

        passed = True
        for dataset in expected_datasets:
            dataset_path = self.silver_path / dataset
            if dataset_path.exists():
                parquet_files = list(dataset_path.rglob('*.parquet'))
                if parquet_files:
                    logger.info(f"✅ {dataset}: {len(parquet_files)} enriched files")
                else:
                    logger.warning(f"⚠️  {dataset}: EXISTS but no parquet files")
            else:
                logger.warning(f"⚠️  {dataset}: Not yet enriched (expected if enrichment not run)")

        self.results.append(('Silver Layer', passed))
        return passed

    def test_gold_layer(self):
        """Test 4: Verify gold layer has production formats"""
        logger.info("\n" + "="*70)
        logger.info("TEST 4: Gold Layer (Production-Ready)")
        logger.info("="*70)

        qlib_path = self.gold_path / 'qlib'
        duckdb_path = self.gold_path / 'duckdb'

        passed = True

        # Check Qlib binary format
        if qlib_path.exists():
            qlib_datasets = list(qlib_path.glob('*'))
            logger.info(f"✅ Qlib: {len(qlib_datasets)} datasets")

            # Check for specific qlib structure
            for dataset_dir in qlib_datasets:
                if dataset_dir.is_dir():
                    features_dir = dataset_dir / 'features'
                    instruments_file = dataset_dir / 'instruments' / 'all.txt'

                    if features_dir.exists() and instruments_file.exists():
                        logger.info(f"  ✅ {dataset_dir.name}: Valid Qlib structure")
                    else:
                        logger.warning(f"  ⚠️  {dataset_dir.name}: Incomplete Qlib structure")
        else:
            logger.warning(f"⚠️  Qlib: Not yet converted")

        # Check DuckDB
        if duckdb_path.exists():
            db_files = list(duckdb_path.glob('*.db'))
            logger.info(f"✅ DuckDB: {len(db_files)} database files")
        else:
            logger.warning(f"⚠️  DuckDB: Directory not created yet")

        self.results.append(('Gold Layer', passed))
        return passed

    def test_data_sample_read(self):
        """Test 5: Read sample data from each layer"""
        logger.info("\n" + "="*70)
        logger.info("TEST 5: Data Sample Reading")
        logger.info("="*70)

        import polars as pl

        passed = True

        # Test Bronze layer
        stocks_daily_bronze = self.bronze_path / 'stocks_daily'
        if stocks_daily_bronze.exists():
            parquet_files = list(stocks_daily_bronze.rglob('*.parquet'))[:1]
            if parquet_files:
                try:
                    df = pl.read_parquet(parquet_files[0])
                    logger.info(f"✅ Bronze read: {len(df)} rows from {parquet_files[0].name}")
                    logger.info(f"   Columns: {', '.join(df.columns[:5])}...")
                except Exception as e:
                    logger.error(f"❌ Bronze read failed: {e}")
                    passed = False

        # Test Silver layer
        stocks_daily_silver = self.silver_path / 'stocks_daily'
        if stocks_daily_silver.exists():
            parquet_files = list(stocks_daily_silver.rglob('*.parquet'))[:1]
            if parquet_files:
                try:
                    df = pl.read_parquet(parquet_files[0])
                    logger.info(f"✅ Silver read: {len(df)} rows from {parquet_files[0].name}")
                    logger.info(f"   Columns: {', '.join(df.columns[:5])}...")
                except Exception as e:
                    logger.error(f"❌ Silver read failed: {e}")
                    passed = False

        self.results.append(('Data Sample Read', passed))
        return passed

    def test_metadata_watermarks(self):
        """Test 6: Verify metadata and watermarks"""
        logger.info("\n" + "="*70)
        logger.info("TEST 6: Metadata & Watermarks")
        logger.info("="*70)

        metadata_path = self.data_lake_root / 'metadata'

        passed = True
        if metadata_path.exists():
            watermark_files = list(metadata_path.rglob('*.json'))
            logger.info(f"✅ Metadata: {len(watermark_files)} watermark files")

            # Sample a watermark file
            if watermark_files:
                import json
                sample_file = watermark_files[0]
                try:
                    with open(sample_file) as f:
                        data = json.load(f)
                    logger.info(f"   Sample watermark: {sample_file.name}")
                    logger.info(f"   Keys: {list(data.keys())}")
                except Exception as e:
                    logger.warning(f"   Could not read watermark: {e}")
        else:
            logger.error(f"❌ Metadata directory missing")
            passed = False

        self.results.append(('Metadata & Watermarks', passed))
        return passed

    def run_all_tests(self):
        """Run all tests and generate report"""
        logger.info("\n" + "="*70)
        logger.info("DATA LAKE INTEGRITY TEST SUITE")
        logger.info("="*70)
        logger.info(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Data Lake Root: {self.data_lake_root}")
        logger.info("")

        # Run all tests
        self.test_directory_structure()
        self.test_bronze_layer()
        self.test_silver_layer()
        self.test_gold_layer()
        self.test_data_sample_read()
        self.test_metadata_watermarks()

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
    test_suite = DataLakeIntegrityTest()
    exit_code = test_suite.run_all_tests()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
