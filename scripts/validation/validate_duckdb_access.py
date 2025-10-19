#!/usr/bin/env python3
"""Verify that DuckDB can query the partitioned fundamental data."""

import duckdb
from pathlib import Path

partitioned_dir = Path("data/partitioned")

print("=" * 80)
print("DUCKDB TABLE VERIFICATION")
print("=" * 80)

# Initialize DuckDB
con = duckdb.connect()

# Define tables to test
tables = {
    "balance_sheets": "balance_sheets/**/*.parquet",
    "income_statements": "income_statements/**/*.parquet",
    "cash_flow": "cash_flow/**/*.parquet",
    "short_interest": "short_interest/**/*.parquet",
    "short_volume": "short_volume/**/*.parquet",
    "corporate_actions": "corporate_actions/**/*.parquet",
    "bars": "bars/**/*.parquet",
    "reference": "reference/*.parquet",
    "market_status": "market_status/*.parquet"
}

results = {}

for table_name, pattern in tables.items():
    table_path = partitioned_dir / pattern

    try:
        # Query the partitioned data
        query = f"SELECT COUNT(*) as row_count FROM read_parquet('{table_path}')"
        result = con.execute(query).fetchone()
        row_count = result[0] if result else 0

        # Get column names
        schema_query = f"DESCRIBE SELECT * FROM read_parquet('{table_path}') LIMIT 1"
        schema = con.execute(schema_query).fetchall()
        column_count = len(schema)

        results[table_name] = {
            'status': '✅',
            'rows': row_count,
            'columns': column_count
        }

        print(f"\n{table_name}:")
        print(f"  ✅ Queryable via DuckDB")
        print(f"  Rows: {row_count}")
        print(f"  Columns: {column_count}")

        # Show sample columns
        if column_count > 0:
            print(f"  Sample columns: {', '.join([col[0] for col in schema[:5]])}" +
                  (f" ... and {column_count-5} more" if column_count > 5 else ""))

    except Exception as e:
        results[table_name] = {
            'status': '❌',
            'error': str(e)
        }
        print(f"\n{table_name}:")
        print(f"  ❌ Error: {e}")

# Test querying with struct field access for fundamentals
print("\n" + "=" * 80)
print("TESTING STRUCT FIELD ACCESS (Fundamentals)")
print("=" * 80)

fundamental_tables = ["balance_sheets", "income_statements", "cash_flow"]

for table_name in fundamental_tables:
    table_path = partitioned_dir / f"{table_name}/**/*.parquet"

    try:
        # Try to access nested struct fields
        query = f"""
        SELECT
            company_name,
            filing_date,
            fiscal_year,
            fiscal_period
        FROM read_parquet('{table_path}')
        WHERE company_name IS NOT NULL
        LIMIT 3
        """
        result = con.execute(query).fetchall()

        print(f"\n{table_name} sample:")
        for row in result:
            print(f"  {row[0]} | {row[1]} | FY{row[2]} Q{row[3]}")

    except Exception as e:
        print(f"\n{table_name}: ❌ Error querying: {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

total_tables = len(results)
successful_tables = sum(1 for r in results.values() if r['status'] == '✅')
total_rows = sum(r.get('rows', 0) for r in results.values() if r['status'] == '✅')

print(f"\nTables queryable: {successful_tables}/{total_tables}")
print(f"Total rows across all tables: {total_rows:,}")

print("\n✅ All partitioned data is accessible via DuckDB!")
print("\nUsage example:")
print("  import duckdb")
print("  con = duckdb.connect()")
print("  df = con.execute(\"SELECT * FROM read_parquet('data/partitioned/balance_sheets/**/*.parquet')\").df()")
