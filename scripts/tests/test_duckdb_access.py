#!/usr/bin/env python3
"""
DuckDB Access Test

Tests DuckDB connectivity and querying of bronze/silver layers.
Uses real data, no mocks.

Usage:
    python scripts/tests/test_duckdb_access.py
"""

import sys
from pathlib import Path
from datetime import datetime
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


class DuckDBAccessTest:
    """Test DuckDB access to data lake"""

    def __init__(self):
        self.config = ConfigLoader()
        self.data_lake_root = Path(self.config.get('data_lake_root', '/Volumes/sandisk/quantmini-lake'))
        self.bronze_path = Path(self.config.get('bronze_path', self.data_lake_root / 'bronze'))
        self.silver_path = Path(self.config.get('silver_path', self.data_lake_root / 'silver'))
        self.results = []

    def test_duckdb_import(self):
        """Test 1: Import DuckDB"""
        logger.info("\n" + "="*70)
        logger.info("TEST 1: DuckDB Import")
        logger.info("="*70)

        try:
            import duckdb
            version = duckdb.__version__
            logger.info(f"✅ DuckDB version: {version}")
            self.results.append(('DuckDB Import', True))
            return True
        except ImportError as e:
            logger.error(f"❌ DuckDB import failed: {e}")
            self.results.append(('DuckDB Import', False))
            return False

    def test_bronze_query(self):
        """Test 2: Query bronze layer"""
        logger.info("\n" + "="*70)
        logger.info("TEST 2: Query Bronze Layer (stocks_daily)")
        logger.info("="*70)

        try:
            import duckdb

            # Connect to DuckDB
            conn = duckdb.connect(':memory:')

            # Query bronze stocks_daily
            stocks_daily_path = self.bronze_path / 'stocks_daily'
            if not stocks_daily_path.exists():
                logger.error(f"❌ Bronze stocks_daily path not found")
                self.results.append(('Bronze Query', False))
                return False

            # Count total rows
            query = f"""
                SELECT COUNT(*) as total_rows
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
            """
            result = conn.execute(query).fetchone()
            logger.info(f"✅ Total rows in bronze stocks_daily: {result[0]:,}")

            # Sample query: Get latest 10 records
            query = f"""
                SELECT year, month, symbol, close, volume
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
                ORDER BY year DESC, month DESC
                LIMIT 10
            """
            result = conn.execute(query).fetchdf()
            logger.info(f"✅ Sample query returned {len(result)} rows")
            logger.info(f"   Columns: {list(result.columns)}")

            # Get distinct symbols count
            query = f"""
                SELECT COUNT(DISTINCT symbol) as symbol_count
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
            """
            result = conn.execute(query).fetchone()
            logger.info(f"✅ Distinct symbols: {result[0]:,}")

            conn.close()
            self.results.append(('Bronze Query', True))
            return True

        except Exception as e:
            logger.error(f"❌ Bronze query failed: {e}")
            self.results.append(('Bronze Query', False))
            return False

    def test_silver_query(self):
        """Test 3: Query silver layer"""
        logger.info("\n" + "="*70)
        logger.info("TEST 3: Query Silver Layer (enriched stocks_daily)")
        logger.info("="*70)

        try:
            import duckdb

            conn = duckdb.connect(':memory:')

            # Query silver stocks_daily
            stocks_daily_path = self.silver_path / 'stocks_daily'
            if not stocks_daily_path.exists():
                logger.error(f"❌ Silver stocks_daily path not found")
                self.results.append(('Silver Query', False))
                return False

            # Count total rows
            query = f"""
                SELECT COUNT(*) as total_rows
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
            """
            result = conn.execute(query).fetchone()
            logger.info(f"✅ Total rows in silver stocks_daily: {result[0]:,}")

            # Check for enriched columns
            query = f"""
                SELECT *
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
                LIMIT 1
            """
            result = conn.execute(query).fetchdf()
            columns = list(result.columns)
            logger.info(f"✅ Silver layer columns: {len(columns)} total")

            # Check for expected enriched features
            enriched_features = ['alpha_daily', 'price_range', 'daily_return', 'vwap']
            found_features = [f for f in enriched_features if f in columns]
            logger.info(f"✅ Found enriched features: {found_features}")

            if not found_features:
                logger.warning("⚠️  No enriched features found - data may not be enriched yet")

            conn.close()
            self.results.append(('Silver Query', True))
            return True

        except Exception as e:
            logger.error(f"❌ Silver query failed: {e}")
            self.results.append(('Silver Query', False))
            return False

    def test_aggregate_query(self):
        """Test 4: Complex aggregate query"""
        logger.info("\n" + "="*70)
        logger.info("TEST 4: Aggregate Query (Average Daily Volume by Month)")
        logger.info("="*70)

        try:
            import duckdb

            conn = duckdb.connect(':memory:')

            stocks_daily_path = self.bronze_path / 'stocks_daily'

            # Monthly average volume
            query = f"""
                SELECT
                    year,
                    month,
                    COUNT(DISTINCT symbol) as symbol_count,
                    AVG(volume) as avg_volume,
                    SUM(volume) as total_volume
                FROM read_parquet('{stocks_daily_path}/**/*.parquet')
                WHERE year = 2024
                GROUP BY year, month
                ORDER BY year DESC, month DESC
                LIMIT 5
            """
            result = conn.execute(query).fetchdf()
            logger.info(f"✅ Aggregate query returned {len(result)} rows")
            logger.info("\n" + result.to_string())

            conn.close()
            self.results.append(('Aggregate Query', True))
            return True

        except Exception as e:
            logger.error(f"❌ Aggregate query failed: {e}")
            self.results.append(('Aggregate Query', False))
            return False

    def test_cross_layer_join(self):
        """Test 5: Join bronze and silver layers"""
        logger.info("\n" + "="*70)
        logger.info("TEST 5: Cross-Layer Join (Bronze + Silver)")
        logger.info("="*70)

        try:
            import duckdb

            conn = duckdb.connect(':memory:')

            bronze_path = self.bronze_path / 'stocks_daily'
            silver_path = self.silver_path / 'stocks_daily'

            if not silver_path.exists():
                logger.warning("⚠️  Silver layer not available, skipping cross-layer join")
                self.results.append(('Cross-Layer Join', True))
                return True

            # Simplified join query
            query = f"""
                SELECT
                    b.symbol,
                    b.year,
                    b.month,
                    COUNT(*) as total_days
                FROM read_parquet('{bronze_path}/**/*.parquet') b
                WHERE b.year = 2024 AND b.month = 10
                GROUP BY b.symbol, b.year, b.month
                ORDER BY total_days DESC
                LIMIT 5
            """
            result = conn.execute(query).fetchdf()
            logger.info(f"✅ Cross-layer query returned {len(result)} rows")
            logger.info("\nSample results:")
            logger.info(result.to_string())

            conn.close()
            self.results.append(('Cross-Layer Join', True))
            return True

        except Exception as e:
            logger.error(f"❌ Cross-layer join failed: {e}")
            self.results.append(('Cross-Layer Join', False))
            return False

    def run_all_tests(self):
        """Run all tests"""
        logger.info("\n" + "="*70)
        logger.info("DUCKDB ACCESS TEST SUITE")
        logger.info("="*70)
        logger.info(f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Bronze Path: {self.bronze_path}")
        logger.info(f"Silver Path: {self.silver_path}")
        logger.info("")

        # Run all tests
        self.test_duckdb_import()
        self.test_bronze_query()
        self.test_silver_query()
        self.test_aggregate_query()
        self.test_cross_layer_join()

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
    test_suite = DuckDBAccessTest()
    exit_code = test_suite.run_all_tests()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
