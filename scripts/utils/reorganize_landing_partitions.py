#!/usr/bin/env python3
"""
Reorganize Landing Layer to Partitioned Structure

Moves existing flat files in landing/ to proper partitioned structure:
  landing/{data_type}/year={YYYY}/month={MM}/{YYYY-MM-DD}.csv.gz

This script will:
1. Identify all CSV.GZ files in landing root
2. Parse dates from filenames
3. Determine data_type (stocks_minute or options_minute based on file size)
4. Move files to partitioned structure
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime
import shutil


def get_file_data_type(file_path: Path, date_str: str) -> str:
    """
    Determine data type based on file size and date.

    Logic:
    - Files >= 2023-10-01: Could be stocks_daily, options_daily, stocks_minute, or options_minute
    - Files < 2023-10-01: stocks_daily or stocks_minute only
    - Large files (>5MB): likely minute data
    - Small files (<5MB): likely daily data
    """
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

    # Heuristic: Minute data files are typically much larger (10MB+)
    # Daily data files are smaller (<1MB typically)
    if file_size_mb > 5:
        # Large file - minute data
        if date_obj >= datetime(2023, 10, 1).date():
            # Could be stocks or options minute, but more likely stocks
            # (options minute is much larger, often 50MB+)
            if file_size_mb > 50:
                return 'options_minute'
            else:
                return 'stocks_minute'
        else:
            # Before 2023-10, only stocks minute exists
            return 'stocks_minute'
    else:
        # Small file - daily data
        if date_obj >= datetime(2023, 10, 1).date():
            # Could be stocks or options daily
            # Options daily is larger than stocks daily
            if file_size_mb > 0.5:
                return 'options_daily'
            else:
                return 'stocks_daily'
        else:
            # Before 2023-10, only stocks daily exists
            return 'stocks_daily'


def main():
    from src.utils.paths import get_quantlake_root

    quantlake_root = get_quantlake_root()
    landing = quantlake_root / "landing"

    print("=" * 80)
    print("REORGANIZING LANDING LAYER TO PARTITIONED STRUCTURE")
    print("=" * 80)
    print()

    # Find all CSV.GZ files in landing root (not in subdirectories)
    csv_files = list(landing.glob("*.csv.gz"))

    if not csv_files:
        print("No CSV.GZ files found in landing root. Already partitioned?")
        return 0

    print(f"Found {len(csv_files)} files to reorganize")
    print()

    # Group files by data type
    files_by_type = {}

    for csv_file in csv_files:
        try:
            # Extract date from filename (YYYY-MM-DD.csv.gz)
            date_str = csv_file.stem.replace('.csv', '')

            # Validate date format
            datetime.strptime(date_str, '%Y-%m-%d')

            # Determine data type
            data_type = get_file_data_type(csv_file, date_str)

            if data_type not in files_by_type:
                files_by_type[data_type] = []

            files_by_type[data_type].append((csv_file, date_str))

        except Exception as e:
            print(f"⚠️  Skipping {csv_file.name}: {e}")

    # Show summary
    print("Files by data type:")
    for data_type, files in files_by_type.items():
        print(f"  {data_type:20s}: {len(files):5,} files")
    print()

    # Ask for confirmation
    response = input("Proceed with reorganization? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return 1

    print()
    print("Reorganizing files...")
    print()

    moved_count = 0
    failed_count = 0

    for data_type, files in files_by_type.items():
        print(f"Processing {data_type}...")

        for csv_file, date_str in files:
            try:
                # Parse date for partitioning
                year, month, day = date_str.split('-')

                # Create partition directory
                partition_dir = landing / data_type / f"year={year}" / f"month={month}"
                partition_dir.mkdir(parents=True, exist_ok=True)

                # Move file
                dest_file = partition_dir / f"{date_str}.csv.gz"

                if dest_file.exists():
                    print(f"  ⚠️  {date_str}: Already exists in partition, skipping")
                    continue

                shutil.move(str(csv_file), str(dest_file))
                moved_count += 1

                if moved_count % 100 == 0:
                    print(f"  Moved {moved_count} files...")

            except Exception as e:
                print(f"  ❌ Failed to move {csv_file.name}: {e}")
                failed_count += 1

    print()
    print("=" * 80)
    print("REORGANIZATION COMPLETE")
    print("=" * 80)
    print(f"Moved: {moved_count}")
    print(f"Failed: {failed_count}")
    print()

    # Show new structure
    print("New landing structure:")
    for data_type_dir in sorted(landing.iterdir()):
        if data_type_dir.is_dir() and not data_type_dir.name.startswith('.'):
            file_count = len(list(data_type_dir.rglob("*.csv.gz")))
            print(f"  {data_type_dir.name:20s}: {file_count:5,} files")
    print()

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
