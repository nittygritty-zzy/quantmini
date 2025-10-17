#!/usr/bin/env python3
"""
Diagnose Schema Inconsistencies

Shows detailed schema differences for datasets with inconsistent schemas.

Usage:
    python scripts/diagnose_schema_issues.py
    python scripts/diagnose_schema_issues.py --data-root /Volumes/sandisk/quantmini-data
    python scripts/diagnose_schema_issues.py --data-type stocks_daily
"""

import argparse
import pyarrow.parquet as pq
from pathlib import Path
from collections import defaultdict
from typing import Dict, List
import sys


def collect_schemas(root_dir: Path) -> Dict[Path, List[str]]:
    """Collect schemas from all parquet files"""
    schemas = {}

    for parquet_file in root_dir.rglob('*.parquet'):
        try:
            parquet_meta = pq.read_metadata(parquet_file)
            schema = parquet_meta.schema.to_arrow_schema()
            columns = [f"{field.name}:{field.type}" for field in schema]
            schemas[parquet_file] = columns
        except Exception as e:
            print(f"⚠️  Could not read {parquet_file}: {e}")

    return schemas


def group_by_schema(schemas: Dict[Path, List[str]]) -> Dict[str, List[Path]]:
    """Group files by their schema"""
    schema_groups = defaultdict(list)

    for file_path, columns in schemas.items():
        signature = '|'.join(sorted(columns))
        schema_groups[signature].append(file_path)

    return dict(schema_groups)


def diagnose_data_type(data_root: Path, data_type: str, dataset: str = 'parquet'):
    """Diagnose schema issues for a specific data type"""
    if dataset == 'parquet':
        path = data_root / 'data' / 'parquet' / data_type
    elif dataset == 'enriched':
        path = data_root / 'data' / 'enriched' / data_type
    else:
        print(f"Unknown dataset: {dataset}")
        return

    if not path.exists():
        print(f"❌ {dataset}/{data_type} does not exist at {path}")
        return

    print(f"\n{'='*70}")
    print(f"DIAGNOSING: {dataset}/{data_type}")
    print(f"Path: {path}")
    print(f"{'='*70}\n")

    # Collect schemas
    schemas = collect_schemas(path)
    print(f"📊 Found {len(schemas)} parquet files")

    # Group by schema
    schema_groups = group_by_schema(schemas)
    print(f"📋 Found {len(schema_groups)} different schemas\n")

    if len(schema_groups) == 1:
        print("✅ All files have consistent schema!")
        columns = sorted(next(iter(schemas.values())))
        print(f"\nColumns ({len(columns)}):")
        for col in columns:
            print(f"  • {col}")
        return

    # Show differences
    print("❌ Schema inconsistency detected!\n")

    # Sort schema groups by number of files (largest first)
    sorted_groups = sorted(
        schema_groups.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )

    for idx, (signature, files) in enumerate(sorted_groups, 1):
        columns = sorted(signature.split('|'))
        print(f"Schema #{idx} ({len(files)} files, {len(columns)} columns):")
        print(f"  Example files:")
        for file in files[:5]:
            # Get relative path from data_root
            rel_path = file.relative_to(data_root)
            print(f"    - {rel_path}")

        if idx <= 2:  # Show columns for first 2 schemas
            print(f"  Columns:")
            for col in columns:
                print(f"    • {col}")

        print()

    # Show differences between first two schemas
    if len(sorted_groups) >= 2:
        print(f"{'='*70}")
        print("DETAILED COMPARISON: Schema #1 vs Schema #2")
        print(f"{'='*70}\n")

        schema1_cols = set(sorted_groups[0][0].split('|'))
        schema2_cols = set(sorted_groups[1][0].split('|'))

        missing = sorted(schema1_cols - schema2_cols)
        extra = sorted(schema2_cols - schema1_cols)

        if missing:
            print(f"Missing in Schema #2 ({len(missing)} columns):")
            for col in missing:
                print(f"  - {col}")
            print()

        if extra:
            print(f"Extra in Schema #2 ({len(extra)} columns):")
            for col in extra:
                print(f"  + {col}")
            print()

        # Show date ranges
        schema1_files = sorted_groups[0][1]
        schema2_files = sorted_groups[1][1]

        # Extract dates from filenames
        def get_date(file_path):
            # Try to find YYYY-MM-DD pattern
            name = file_path.stem
            if len(name) >= 10 and name.count('-') >= 2:
                parts = name.split('-')
                if len(parts[0]) == 4:
                    return f"{parts[0]}-{parts[1]}-{parts[2][:2]}"
            return None

        schema1_dates = [get_date(f) for f in schema1_files if get_date(f)]
        schema2_dates = [get_date(f) for f in schema2_files if get_date(f)]

        if schema1_dates and schema2_dates:
            print(f"Date Range Analysis:")
            print(f"  Schema #1: {min(schema1_dates)} to {max(schema1_dates)}")
            print(f"  Schema #2: {min(schema2_dates)} to {max(schema2_dates)}")
            print()


def main():
    parser = argparse.ArgumentParser(description='Diagnose schema inconsistencies')
    parser.add_argument(
        '--data-root',
        default='/Volumes/sandisk/quantmini-data',
        help='Root directory for production data'
    )
    parser.add_argument(
        '--data-type',
        choices=['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all'],
        default='all',
        help='Data type to diagnose'
    )
    parser.add_argument(
        '--dataset',
        choices=['parquet', 'enriched', 'both'],
        default='both',
        help='Dataset to diagnose'
    )

    args = parser.parse_args()
    data_root = Path(args.data_root)

    data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute'] \
                 if args.data_type == 'all' else [args.data_type]

    datasets = ['parquet', 'enriched'] if args.dataset == 'both' else [args.dataset]

    for data_type in data_types:
        for dataset in datasets:
            diagnose_data_type(data_root, data_type, dataset)


if __name__ == '__main__':
    main()
