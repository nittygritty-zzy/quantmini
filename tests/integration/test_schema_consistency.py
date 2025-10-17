"""
Schema Consistency Tests

Validates that all dates/partitions have consistent schemas across:
- Parquet (raw ingested data)
- Enriched (feature-engineered data)
- Qlib (binary format)

Run with: pytest tests/integration/test_schema_consistency.py -v
"""

import pytest
import pyarrow.parquet as pq
import struct
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Set
import logging

logger = logging.getLogger(__name__)


class TestSchemaConsistency:
    """Test schema consistency across all datasets and dates"""

    @pytest.fixture
    def data_root(self):
        """Data root directory"""
        return Path('data')

    @pytest.fixture
    def parquet_root(self, data_root):
        """Parquet data root"""
        return data_root / 'parquet'

    @pytest.fixture
    def enriched_root(self, data_root):
        """Enriched data root"""
        return data_root / 'enriched'

    @pytest.fixture
    def qlib_root(self, data_root):
        """Qlib data root"""
        return data_root / 'qlib'

    # ========================================================================
    # PARQUET SCHEMA CONSISTENCY TESTS
    # ========================================================================

    def test_parquet_stocks_daily_schema_consistency(self, parquet_root):
        """Test all stocks_daily parquet files have same schema"""
        data_type = 'stocks_daily'
        schemas = self._collect_parquet_schemas(parquet_root / data_type)

        assert len(schemas) > 0, f"No parquet files found for {data_type}"

        # Group files by schema
        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(data_type, schema_groups)
            pytest.fail(f"{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ {data_type}: All {len(schemas)} files have consistent schema")

    def test_parquet_stocks_minute_schema_consistency(self, parquet_root):
        """Test all stocks_minute parquet files have same schema"""
        data_type = 'stocks_minute'
        schemas = self._collect_parquet_schemas(parquet_root / data_type)

        assert len(schemas) > 0, f"No parquet files found for {data_type}"

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(data_type, schema_groups)
            pytest.fail(f"{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ {data_type}: All {len(schemas)} files have consistent schema")

    def test_parquet_options_daily_schema_consistency(self, parquet_root):
        """Test all options_daily parquet files have same schema"""
        data_type = 'options_daily'
        schemas = self._collect_parquet_schemas(parquet_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No parquet files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(data_type, schema_groups)
            pytest.fail(f"{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ {data_type}: All {len(schemas)} files have consistent schema")

    def test_parquet_options_minute_schema_consistency(self, parquet_root):
        """Test all options_minute parquet files have same schema"""
        data_type = 'options_minute'
        schemas = self._collect_parquet_schemas(parquet_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No parquet files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(data_type, schema_groups)
            pytest.fail(f"{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ {data_type}: All {len(schemas)} files have consistent schema")

    # ========================================================================
    # ENRICHED SCHEMA CONSISTENCY TESTS
    # ========================================================================

    def test_enriched_stocks_daily_schema_consistency(self, enriched_root):
        """Test all enriched stocks_daily files have same schema"""
        data_type = 'stocks_daily'
        schemas = self._collect_parquet_schemas(enriched_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No enriched files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(f"enriched_{data_type}", schema_groups)
            pytest.fail(f"enriched_{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ enriched_{data_type}: All {len(schemas)} files have consistent schema")

    def test_enriched_stocks_minute_schema_consistency(self, enriched_root):
        """Test all enriched stocks_minute files have same schema"""
        data_type = 'stocks_minute'
        schemas = self._collect_parquet_schemas(enriched_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No enriched files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(f"enriched_{data_type}", schema_groups)
            pytest.fail(f"enriched_{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ enriched_{data_type}: All {len(schemas)} files have consistent schema")

    def test_enriched_options_daily_schema_consistency(self, enriched_root):
        """Test all enriched options_daily files have same schema"""
        data_type = 'options_daily'
        schemas = self._collect_parquet_schemas(enriched_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No enriched files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(f"enriched_{data_type}", schema_groups)
            pytest.fail(f"enriched_{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ enriched_{data_type}: All {len(schemas)} files have consistent schema")

    def test_enriched_options_minute_schema_consistency(self, enriched_root):
        """Test all enriched options_minute files have same schema"""
        data_type = 'options_minute'
        schemas = self._collect_parquet_schemas(enriched_root / data_type)

        if len(schemas) == 0:
            pytest.skip(f"No enriched files found for {data_type}")

        schema_groups = self._group_by_schema(schemas)

        if len(schema_groups) > 1:
            self._report_schema_differences(f"enriched_{data_type}", schema_groups)
            pytest.fail(f"enriched_{data_type}: Found {len(schema_groups)} different schemas!")

        logger.info(f"✅ enriched_{data_type}: All {len(schemas)} files have consistent schema")

    # ========================================================================
    # QLIB BINARY FORMAT CONSISTENCY TESTS
    # ========================================================================

    def test_qlib_stocks_daily_feature_consistency(self, qlib_root):
        """Test all stocks in Qlib have same features"""
        data_type = 'stocks_daily'
        qlib_dir = qlib_root / data_type

        if not qlib_dir.exists():
            pytest.skip(f"No Qlib data found for {data_type}")

        # Get all symbols
        instruments_file = qlib_dir / 'instruments' / 'all.txt'
        if not instruments_file.exists():
            pytest.skip(f"No instruments file found for {data_type}")

        symbols = []
        with open(instruments_file) as f:
            for line in f:
                parts = line.strip().split('\t')
                if parts:
                    symbols.append(parts[0])

        assert len(symbols) > 0, "No symbols found in instruments file"

        # Collect features for each symbol
        symbol_features = {}
        features_dir = qlib_dir / 'features'

        for symbol in symbols[:100]:  # Sample first 100 symbols
            symbol_dir = features_dir / symbol.lower()
            if symbol_dir.exists():
                features = set()
                for bin_file in symbol_dir.glob('*.bin'):
                    # Extract feature name (remove .day.bin or .1min.bin)
                    feature = bin_file.stem.replace('.day', '').replace('.1min', '')
                    features.add(feature)

                if features:
                    symbol_features[symbol] = features

        assert len(symbol_features) > 0, "No symbol features found"

        # Check all symbols have same features
        reference_features = None
        reference_symbol = None
        inconsistent_symbols = []

        for symbol, features in symbol_features.items():
            if reference_features is None:
                reference_features = features
                reference_symbol = symbol
            else:
                if features != reference_features:
                    missing = reference_features - features
                    extra = features - reference_features
                    inconsistent_symbols.append({
                        'symbol': symbol,
                        'missing': missing,
                        'extra': extra
                    })

        if inconsistent_symbols:
            logger.error(f"Reference symbol: {reference_symbol}")
            logger.error(f"Reference features ({len(reference_features)}): {sorted(reference_features)}")
            for issue in inconsistent_symbols[:5]:  # Show first 5
                logger.error(f"  {issue['symbol']}: missing={issue['missing']}, extra={issue['extra']}")

            pytest.fail(f"Found {len(inconsistent_symbols)} symbols with inconsistent features!")

        logger.info(f"✅ qlib_{data_type}: All {len(symbol_features)} symbols have consistent {len(reference_features)} features")

    def test_qlib_stocks_daily_binary_format(self, qlib_root):
        """Test Qlib binary files follow correct format"""
        data_type = 'stocks_daily'
        qlib_dir = qlib_root / data_type

        if not qlib_dir.exists():
            pytest.skip(f"No Qlib data found for {data_type}")

        # Get calendar length
        calendar_file = qlib_dir / 'calendars' / 'day.txt'
        if not calendar_file.exists():
            pytest.skip(f"No calendar file found")

        with open(calendar_file) as f:
            calendar_days = [line.strip() for line in f if line.strip()]

        expected_count = len(calendar_days)
        assert expected_count > 0, "Empty calendar file"

        # Check first 10 symbols
        features_dir = qlib_dir / 'features'
        symbols_checked = 0

        for symbol_dir in sorted(features_dir.iterdir())[:10]:
            if not symbol_dir.is_dir():
                continue

            for bin_file in symbol_dir.glob('*.bin'):
                # Read binary file
                with open(bin_file, 'rb') as f:
                    # First 4 bytes: count (uint32, little-endian)
                    count_bytes = f.read(4)
                    if len(count_bytes) != 4:
                        pytest.fail(f"Invalid binary file {bin_file}: truncated header")

                    count = struct.unpack('<I', count_bytes)[0]

                    # Remaining bytes: float32 values
                    values_bytes = f.read()
                    expected_bytes = count * 4

                    if len(values_bytes) != expected_bytes:
                        pytest.fail(
                            f"Invalid binary file {bin_file}: "
                            f"expected {expected_bytes} bytes, got {len(values_bytes)}"
                        )

                    # Count should match calendar
                    if count != expected_count:
                        pytest.fail(
                            f"Binary file {bin_file} has {count} values, "
                            f"but calendar has {expected_count} days"
                        )

            symbols_checked += 1

        assert symbols_checked > 0, "No symbols checked"
        logger.info(f"✅ qlib_{data_type}: All {symbols_checked} symbols have valid binary format")

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _collect_parquet_schemas(self, root_dir: Path) -> Dict[Path, List[str]]:
        """
        Collect schemas from all parquet files

        Returns:
            Dict mapping file path to list of column names
        """
        schemas = {}

        if not root_dir.exists():
            return schemas

        for parquet_file in root_dir.rglob('*.parquet'):
            try:
                # Read parquet metadata without loading data
                # This avoids schema merging issues
                parquet_meta = pq.read_metadata(parquet_file)
                schema = parquet_meta.schema.to_arrow_schema()

                # Get column names and types
                columns = [f"{field.name}:{field.type}" for field in schema]
                schemas[parquet_file] = columns

            except Exception as e:
                logger.warning(f"Could not read {parquet_file}: {e}")

        return schemas

    def _group_by_schema(self, schemas: Dict[Path, List[str]]) -> Dict[str, List[Path]]:
        """
        Group files by their schema

        Returns:
            Dict mapping schema signature to list of files with that schema
        """
        schema_groups = defaultdict(list)

        for file_path, columns in schemas.items():
            # Create schema signature (sorted column list)
            signature = '|'.join(sorted(columns))
            schema_groups[signature].append(file_path)

        return dict(schema_groups)

    def _report_schema_differences(self, data_type: str, schema_groups: Dict[str, List[Path]]):
        """Report schema differences for debugging"""
        logger.error(f"\n{'='*70}")
        logger.error(f"Schema Inconsistency Report: {data_type}")
        logger.error(f"{'='*70}")
        logger.error(f"Found {len(schema_groups)} different schemas:\n")

        for idx, (signature, files) in enumerate(schema_groups.items(), 1):
            columns = sorted(signature.split('|'))
            logger.error(f"Schema {idx} ({len(files)} files):")
            logger.error(f"  Columns ({len(columns)}):")
            for col in columns:
                logger.error(f"    - {col}")
            logger.error(f"  Example files:")
            for file in files[:3]:
                logger.error(f"    - {file}")
            logger.error("")

        # Show differences between first two schemas
        if len(schema_groups) >= 2:
            schemas = list(schema_groups.keys())
            schema1_cols = set(schemas[0].split('|'))
            schema2_cols = set(schemas[1].split('|'))

            missing = schema1_cols - schema2_cols
            extra = schema2_cols - schema1_cols

            logger.error("Differences between Schema 1 and Schema 2:")
            if missing:
                logger.error(f"  Missing in Schema 2: {missing}")
            if extra:
                logger.error(f"  Extra in Schema 2: {extra}")
            logger.error(f"{'='*70}\n")


class TestSchemaExpectations:
    """Test schemas match expected structure"""

    def test_parquet_stocks_daily_expected_columns(self):
        """Test stocks_daily parquet has expected columns"""
        parquet_root = Path('data/parquet/stocks_daily')

        if not parquet_root.exists():
            pytest.skip("No stocks_daily parquet data")

        # Find any parquet file
        parquet_file = next(parquet_root.rglob('*.parquet'), None)
        if not parquet_file:
            pytest.skip("No parquet files found")

        # Read metadata only
        parquet_meta = pq.read_metadata(parquet_file)
        schema = parquet_meta.schema.to_arrow_schema()
        columns = set(schema.names)

        # Expected base columns (from Polygon + partitioning)
        expected = {'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'transactions', 'year', 'month'}

        missing = expected - columns
        assert not missing, f"Missing expected columns: {missing}"

        logger.info(f"✅ stocks_daily has all {len(expected)} expected base columns + {len(columns - expected)} extra columns")

    def test_enriched_stocks_daily_has_features(self):
        """Test enriched stocks_daily has feature columns"""
        enriched_root = Path('data/enriched/stocks_daily')

        if not enriched_root.exists():
            pytest.skip("No enriched stocks_daily data")

        # Find any enriched file
        enriched_file = next(enriched_root.rglob('*.parquet'), None)
        if not enriched_file:
            pytest.skip("No enriched files found")

        # Read metadata only
        parquet_meta = pq.read_metadata(enriched_file)
        schema = parquet_meta.schema.to_arrow_schema()
        columns = set(schema.names)

        # Should have base columns + features
        expected_base = {'symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'transactions'}
        expected_features = {'returns_1d', 'daily_return', 'price_range', 'alpha_daily', 'vwap'}

        missing_base = expected_base - columns
        missing_features = expected_features - columns

        assert not missing_base, f"Missing base columns: {missing_base}"
        assert not missing_features, f"Missing feature columns: {missing_features}"

        logger.info(f"✅ enriched stocks_daily has base columns + features (total: {len(columns)} columns, {len(expected_features)} features)")
