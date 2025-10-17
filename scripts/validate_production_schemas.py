#!/usr/bin/env python3
"""
Validate Production Data Schemas

Comprehensive schema validation for production datasets.
Checks all parquet, enriched, and qlib data for consistency.

Usage:
    python scripts/validate_production_schemas.py
    python scripts/validate_production_schemas.py --data-root /Volumes/sandisk/quantmini-data
"""

import argparse
import pyarrow.parquet as pq
from pathlib import Path
from collections import defaultdict
from typing import Dict, List
import sys


class ProductionSchemaValidator:
    """Validate schemas across production datasets"""

    def __init__(self, data_root: Path):
        self.data_root = Path(data_root)
        self.parquet_root = self.data_root / 'data' / 'parquet'
        self.enriched_root = self.data_root / 'data' / 'enriched'
        self.qlib_root = self.data_root / 'data' / 'qlib'

        self.results = {
            'parquet': {},
            'enriched': {},
            'qlib': {},
            'errors': []
        }

    def validate_all(self):
        """Run all validations"""
        print(f"🔍 Validating production data at: {self.data_root}\n")

        # Validate parquet
        print("=" * 70)
        print("PARQUET DATA VALIDATION")
        print("=" * 70)
        self._validate_parquet()

        # Validate enriched
        print("\n" + "=" * 70)
        print("ENRICHED DATA VALIDATION")
        print("=" * 70)
        self._validate_enriched()

        # Validate qlib
        print("\n" + "=" * 70)
        print("QLIB DATA VALIDATION")
        print("=" * 70)
        self._validate_qlib()

        # Print summary
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        self._print_summary()

        # Return exit code
        return 0 if len(self.results['errors']) == 0 else 1

    def _validate_parquet(self):
        """Validate parquet datasets"""
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']

        for data_type in data_types:
            path = self.parquet_root / data_type
            if not path.exists():
                print(f"⚠️  {data_type}: No data found")
                continue

            result = self._check_schema_consistency(path, data_type)
            self.results['parquet'][data_type] = result

            if result['consistent']:
                print(f"✅ {data_type}: {result['file_count']} files, consistent schema")
            else:
                print(f"❌ {data_type}: {result['schema_count']} different schemas found!")
                self.results['errors'].append(f"parquet/{data_type}: schema inconsistency")

    def _validate_enriched(self):
        """Validate enriched datasets"""
        data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']

        for data_type in data_types:
            path = self.enriched_root / data_type
            if not path.exists():
                print(f"⚠️  {data_type}: No data found")
                continue

            result = self._check_schema_consistency(path, data_type)
            self.results['enriched'][data_type] = result

            if result['consistent']:
                print(f"✅ {data_type}: {result['file_count']} files, {result['column_count']} columns")
            else:
                print(f"❌ {data_type}: {result['schema_count']} different schemas found!")
                self.results['errors'].append(f"enriched/{data_type}: schema inconsistency")

    def _validate_qlib(self):
        """Validate qlib binary datasets"""
        data_types = ['stocks_daily']

        for data_type in data_types:
            path = self.qlib_root / data_type
            if not path.exists():
                print(f"⚠️  {data_type}: No data found")
                continue

            # Check instruments
            instruments_file = path / 'instruments' / 'all.txt'
            if not instruments_file.exists():
                print(f"❌ {data_type}: Missing instruments file")
                self.results['errors'].append(f"qlib/{data_type}: missing instruments")
                continue

            # Check calendar
            calendar_file = path / 'calendars' / 'day.txt'
            if not calendar_file.exists():
                print(f"❌ {data_type}: Missing calendar file")
                self.results['errors'].append(f"qlib/{data_type}: missing calendar")
                continue

            # Count symbols and features
            with open(instruments_file) as f:
                symbols = [line.strip().split('\t')[0] for line in f if line.strip()]

            with open(calendar_file) as f:
                trading_days = [line.strip() for line in f if line.strip()]

            # Check feature consistency
            features_dir = path / 'features'
            if not features_dir.exists():
                print(f"❌ {data_type}: Missing features directory")
                self.results['errors'].append(f"qlib/{data_type}: missing features")
                continue

            # Sample first 50 symbols
            sample_features = {}
            for symbol in symbols[:50]:
                symbol_dir = features_dir / symbol.lower()
                if symbol_dir.exists():
                    features = set()
                    for bin_file in symbol_dir.glob('*.bin'):
                        feature = bin_file.stem.replace('.day', '').replace('.1min', '')
                        features.add(feature)
                    if features:
                        sample_features[symbol] = features

            # Check consistency
            if sample_features:
                reference_features = next(iter(sample_features.values()))
                inconsistent = []

                for symbol, features in sample_features.items():
                    if features != reference_features:
                        inconsistent.append(symbol)

                if inconsistent:
                    print(f"❌ {data_type}: {len(inconsistent)} symbols have inconsistent features")
                    self.results['errors'].append(f"qlib/{data_type}: feature inconsistency")
                else:
                    print(f"✅ {data_type}: {len(symbols)} symbols, {len(reference_features)} features, {len(trading_days)} days")
            else:
                print(f"⚠️  {data_type}: No features found")

            self.results['qlib'][data_type] = {
                'symbol_count': len(symbols),
                'feature_count': len(reference_features) if sample_features else 0,
                'trading_days': len(trading_days),
                'consistent': len(inconsistent) == 0 if sample_features else False
            }

    def _check_schema_consistency(self, root_dir: Path, data_type: str) -> Dict:
        """Check schema consistency for a dataset"""
        schemas = {}

        # Collect all schemas
        for parquet_file in root_dir.rglob('*.parquet'):
            try:
                parquet_meta = pq.read_metadata(parquet_file)
                schema = parquet_meta.schema.to_arrow_schema()
                columns = tuple(sorted([f"{field.name}:{field.type}" for field in schema]))
                schemas[parquet_file] = columns
            except Exception as e:
                print(f"⚠️  Could not read {parquet_file.name}: {e}")

        if not schemas:
            return {'consistent': False, 'file_count': 0, 'schema_count': 0, 'column_count': 0}

        # Group by schema
        schema_groups = defaultdict(list)
        for file_path, columns in schemas.items():
            schema_groups[columns].append(file_path)

        # Get column count (from first schema)
        first_schema = next(iter(schemas.values()))
        column_count = len(first_schema)

        return {
            'consistent': len(schema_groups) == 1,
            'file_count': len(schemas),
            'schema_count': len(schema_groups),
            'column_count': column_count,
            'schema_groups': dict(schema_groups) if len(schema_groups) > 1 else {}
        }

    def _print_summary(self):
        """Print validation summary"""
        total_errors = len(self.results['errors'])

        if total_errors == 0:
            print("✅ ALL VALIDATIONS PASSED")
            print("\nProduction data is consistent:")

            # Parquet summary
            parquet_files = sum(r['file_count'] for r in self.results['parquet'].values())
            print(f"  • Parquet: {parquet_files} files validated")

            # Enriched summary
            enriched_files = sum(r['file_count'] for r in self.results['enriched'].values())
            print(f"  • Enriched: {enriched_files} files validated")

            # Qlib summary
            for data_type, result in self.results['qlib'].items():
                if result:
                    print(f"  • Qlib {data_type}: {result['symbol_count']} symbols validated")

        else:
            print(f"❌ FOUND {total_errors} ERRORS:\n")
            for error in self.results['errors']:
                print(f"  • {error}")

        print()


def main():
    parser = argparse.ArgumentParser(description='Validate production data schemas')
    parser.add_argument(
        '--data-root',
        default='/Volumes/sandisk/quantmini-data',
        help='Root directory for production data'
    )

    args = parser.parse_args()

    validator = ProductionSchemaValidator(args.data_root)
    exit_code = validator.validate_all()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
