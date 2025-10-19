#!/usr/bin/env python3
"""
Partition data by DATE-FIRST for optimal stock screener performance.

Partitioning structure:
  year=2024/month=10/ticker=AAPL.parquet
  year=2024/month=10/ticker=MSFT.parquet

This allows efficient screening queries that scan all stocks for a specific time period.
"""

import polars as pl
from pathlib import Path
from datetime import datetime

# Configuration
INPUT_DIR = Path("data")
OUTPUT_DIR = Path("data/partitioned_screener")

# Data types and their configurations
DATA_CONFIGS = {
    "fundamentals": {
        "subdirs": ["balance_sheets", "income_statements", "cash_flow"],
        "date_column": "filing_date",
        "ticker_column": "tickers",  # List column, need to extract
    },
    "corporate_actions": {
        "subdirs": ["dividends"],
        "date_column": "ex_dividend_date",
        "ticker_column": "ticker",
    },
    "short_interest": {
        "subdirs": None,  # Flat directory
        "date_column": "settlement_date",
        "ticker_column": "ticker",
    },
    "short_volume": {
        "subdirs": None,
        "date_column": "date",
        "ticker_column": "ticker",
    },
    "related_tickers": {
        "subdirs": None,
        "date_column": None,  # No date - partition by ticker only
        "ticker_column": "ticker",
    },
}

def extract_ticker(df: pl.DataFrame, ticker_column: str) -> pl.DataFrame:
    """
    Extract ticker from various formats.

    - If ticker_column is a List (like fundamentals), take the first element
    - Otherwise use as-is
    """
    if df.schema.get(ticker_column) == pl.List(pl.String):
        # Extract first ticker from list
        return df.with_columns([
            pl.col(ticker_column).list.first().alias('ticker_extracted')
        ])
    else:
        # Use ticker as-is
        return df.with_columns([
            pl.col(ticker_column).alias('ticker_extracted')
        ])

def partition_data_date_first(
    input_dir: Path,
    output_dir: Path,
    date_column: str = None,
    ticker_column: str = None,
    test_mode: bool = True
):
    """
    Partition parquet files by DATE-FIRST, then ticker.

    This structure is optimal for stock screeners that need to scan all stocks
    for a specific time period (e.g., latest quarter).

    Args:
        input_dir: Directory containing input parquet files
        output_dir: Directory for partitioned output
        date_column: Column name for date partitioning (optional)
        ticker_column: Column name for ticker partitioning
        test_mode: If True, only process first 3 files
    """
    if not input_dir.exists():
        print(f"  ⚠️  {input_dir} does not exist, skipping")
        return

    files = list(input_dir.glob("*.parquet"))

    if not files:
        print(f"  ⚠️  No parquet files found in {input_dir}")
        return

    if test_mode:
        files = files[:3]
        print(f"  🧪 TEST MODE: Processing first {len(files)} files only")

    print(f"  Processing {len(files)} files from {input_dir.name}/")

    total_rows = 0
    total_partitions = 0

    for file in files:
        try:
            df = pl.read_parquet(file)

            # Extract ticker
            if ticker_column not in df.columns:
                print(f"    ⚠️  No '{ticker_column}' column in {file.name}, skipping")
                continue

            df = extract_ticker(df, ticker_column)

            # Filter out null tickers
            df = df.filter(pl.col('ticker_extracted').is_not_null())

            if len(df) == 0:
                continue

            # Add date partitioning columns if date_column exists
            if date_column and date_column in df.columns:
                # Filter out null dates
                df = df.filter(pl.col(date_column).is_not_null())

                # Parse date
                if df.schema[date_column] == pl.String:
                    df = df.with_columns([
                        pl.col(date_column).str.to_date("%Y-%m-%d").alias('_date_parsed')
                    ])
                else:
                    df = df.with_columns([
                        pl.col(date_column).cast(pl.Date).alias('_date_parsed')
                    ])

                df = df.with_columns([
                    pl.col('_date_parsed').dt.year().cast(pl.Int32).alias('year'),
                    pl.col('_date_parsed').dt.month().cast(pl.Int32).alias('month'),
                ]).drop('_date_parsed')

                # Get unique year/month/ticker combinations (DATE-FIRST)
                partitions = df.select(['year', 'month', 'ticker_extracted']).unique()

                for row in partitions.iter_rows(named=True):
                    year = row['year']
                    month = row['month']
                    ticker = row['ticker_extracted']

                    # Filter for this partition
                    partition_df = df.filter(
                        (pl.col('year') == year) &
                        (pl.col('month') == month) &
                        (pl.col('ticker_extracted') == ticker)
                    ).drop(['ticker_extracted', 'year', 'month'])

                    # Create partition directory: year=2024/month=10/ticker=AAPL.parquet
                    partition_dir = output_dir / f'year={year}' / f'month={month:02d}'
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    # Write parquet file (one file per ticker)
                    output_file = partition_dir / f'ticker={ticker}.parquet'

                    # If file exists, append to it (diagonal concat to handle schema differences)
                    if output_file.exists():
                        existing_df = pl.read_parquet(output_file)
                        partition_df = pl.concat([existing_df, partition_df], how="diagonal")

                    partition_df.write_parquet(str(output_file), compression='zstd')

                    total_rows += len(partition_df)
                    total_partitions += 1
            else:
                # No date column - partition by ticker only
                tickers = df.select('ticker_extracted').unique()

                for row in tickers.iter_rows(named=True):
                    ticker = row['ticker_extracted']

                    # Filter for this ticker
                    partition_df = df.filter(
                        pl.col('ticker_extracted') == ticker
                    ).drop('ticker_extracted')

                    # Create partition directory: ticker=AAPL/
                    partition_dir = output_dir / f'ticker={ticker}'
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    # Write parquet file
                    output_file = partition_dir / f'{input_dir.name}_{ticker}.parquet'
                    partition_df.write_parquet(str(output_file), compression='zstd')

                    total_rows += len(partition_df)
                    total_partitions += 1

        except Exception as e:
            print(f"    ⚠️  Error processing {file.name}: {e}")
            continue

    print(f"  ✅ Partitioned {total_rows} rows into {total_partitions} partitions")

def main(test_mode: bool = True):
    """
    Main partitioning function.

    Args:
        test_mode: If True, only process first 3 files per data type for testing
    """
    print("=" * 80)
    print("PARTITIONING DATA BY DATE-FIRST (FOR STOCK SCREENERS)")
    print("=" * 80)

    if test_mode:
        print("\n🧪 RUNNING IN TEST MODE (first 3 files per data type)")
        print("   Set test_mode=False for production run\n")
    else:
        print("\n🚀 PRODUCTION MODE - Processing all files\n")

    # Process fundamentals
    print("\n📊 FUNDAMENTALS")
    print("-" * 80)

    for subdir in ["balance_sheets", "income_statements", "cash_flow"]:
        input_dir = INPUT_DIR / "fundamentals"
        output_dir = OUTPUT_DIR / subdir

        print(f"\n{subdir}:")

        # Process files that match this subdir pattern
        files = list(input_dir.glob(f"{subdir}_*.parquet"))

        if not files:
            print(f"  ⚠️  No {subdir} files found")
            continue

        if test_mode:
            files = files[:3]

        print(f"  Processing {len(files)} files from fundamentals/")

        total_rows = 0
        total_partitions = 0

        # Process each file separately (do NOT combine them - they have different schemas)
        for file in files:
            try:
                df = pl.read_parquet(file)

                # Extract ticker
                if "tickers" not in df.columns:
                    print(f"    ⚠️  No 'tickers' column in {file.name}, skipping")
                    continue

                df = extract_ticker(df, "tickers")

                # Filter out null tickers
                df = df.filter(pl.col('ticker_extracted').is_not_null())

                if len(df) == 0:
                    continue

                # Filter out null dates
                df = df.filter(pl.col("filing_date").is_not_null())

                # Parse date
                if df.schema["filing_date"] == pl.String:
                    df = df.with_columns([
                        pl.col("filing_date").str.to_date("%Y-%m-%d").alias('_date_parsed')
                    ])
                else:
                    df = df.with_columns([
                        pl.col("filing_date").cast(pl.Date).alias('_date_parsed')
                    ])

                df = df.with_columns([
                    pl.col('_date_parsed').dt.year().cast(pl.Int32).alias('year'),
                    pl.col('_date_parsed').dt.month().cast(pl.Int32).alias('month'),
                ]).drop('_date_parsed')

                # Get unique year/month/ticker combinations (DATE-FIRST)
                partitions = df.select(['year', 'month', 'ticker_extracted']).unique()

                for row in partitions.iter_rows(named=True):
                    year = row['year']
                    month = row['month']
                    ticker = row['ticker_extracted']

                    # Filter for this partition
                    partition_df = df.filter(
                        (pl.col('year') == year) &
                        (pl.col('month') == month) &
                        (pl.col('ticker_extracted') == ticker)
                    ).drop(['ticker_extracted', 'year', 'month'])

                    # Create partition directory: year=2024/month=10/ticker=AAPL.parquet
                    partition_dir = output_dir / f'year={year}' / f'month={month:02d}'
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    # Write parquet file (one file per ticker)
                    output_file = partition_dir / f'ticker={ticker}.parquet'

                    # If file exists, it means we have multiple records for same ticker/year/month
                    # This can happen if data is split across multiple downloaded files
                    if output_file.exists():
                        existing_df = pl.read_parquet(output_file)
                        # Append (no concat needed since same ticker/schema)
                        partition_df = pl.concat([existing_df, partition_df])

                    partition_df.write_parquet(str(output_file), compression='zstd')

                    total_rows += len(partition_df)
                    total_partitions += 1

            except Exception as e:
                print(f"    ⚠️  Error processing {file.name}: {e}")
                continue

        print(f"  ✅ Partitioned {total_rows} rows into {total_partitions} partitions")

    # Process other data types
    print("\n📊 OTHER DATA TYPES")
    print("-" * 80)

    for data_type, config in DATA_CONFIGS.items():
        if data_type == "fundamentals":
            continue  # Already processed

        input_dir = INPUT_DIR / data_type
        output_dir = OUTPUT_DIR / data_type

        print(f"\n{data_type}:")

        partition_data_date_first(
            input_dir=input_dir,
            output_dir=output_dir,
            date_column=config["date_column"],
            ticker_column=config["ticker_column"],
            test_mode=test_mode
        )

    print("\n" + "=" * 80)
    print("✅ PARTITIONING COMPLETE")
    print("=" * 80)
    print(f"\nPartitioned data location: {OUTPUT_DIR}/")
    print("\nExample DuckDB queries for STOCK SCREENERS:")
    print("""
    -- Get all stocks with balance sheet data for Oct 2024 (EFFICIENT!)
    SELECT * FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/month=10/*.parquet')

    -- Screen stocks by financial metrics (latest quarter)
    SELECT
        ticker,
        assets.value AS total_assets,
        liabilities.value AS total_liabilities,
        equity.value AS shareholders_equity
    FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/month=10/*.parquet')
    WHERE equity.value > 1000000000  -- Filter by equity > $1B

    -- Get specific ticker and date range (still efficient)
    SELECT * FROM read_parquet('data/partitioned_screener/balance_sheets/year=2024/month=*/ticker=AAPL.parquet')
    """)

if __name__ == "__main__":
    import sys

    # Check for --production flag
    production = "--production" in sys.argv

    main(test_mode=not production)
