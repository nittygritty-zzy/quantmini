#!/usr/bin/env python3
"""
Phase 1: Extract Fundamental Schemas from Bronze Layer

This script scans ALL balance sheets, income statements, and cash flow statements
in the bronze layer to extract a union schema (superset of all columns across all tickers).

Polygon API returns different schemas for different companies, so we need to:
1. Scan all files and collect unique column names
2. Analyze data types for each column
3. Generate a unified schema for the silver layer
4. Save schema configuration as JSON for Phase 2

Usage:
    python scripts/transformation/extract_fundamental_schemas.py

Output:
    config/fundamentals_schema.json
"""

import sys
import json
from pathlib import Path
from typing import Dict, Set, List, Tuple
from datetime import datetime
from collections import defaultdict
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import polars as pl
from src.utils.paths import get_quantlake_root

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FundamentalsSchemaExtractor:
    """Extract and analyze schemas from bronze layer fundamentals"""

    def __init__(self, bronze_path: Path):
        self.bronze_path = Path(bronze_path)
        self.schemas = {
            'balance_sheets': {},
            'income_statements': {},
            'cash_flow': {}
        }
        self.column_stats = {
            'balance_sheets': defaultdict(lambda: {'count': 0, 'dtypes': set()}),
            'income_statements': defaultdict(lambda: {'count': 0, 'dtypes': set()}),
            'cash_flow': defaultdict(lambda: {'count': 0, 'dtypes': set()})
        }

    def extract_all_schemas(self, sample_size: int = None) -> Dict:
        """
        Extract schemas from all statement types

        Args:
            sample_size: If set, only scan this many files per type (for testing)

        Returns:
            Dictionary with union schemas for each statement type
        """
        logger.info("="*80)
        logger.info("FUNDAMENTALS SCHEMA EXTRACTION - Phase 1")
        logger.info("="*80)
        logger.info(f"Bronze path: {self.bronze_path}")
        logger.info("")

        statement_types = ['balance_sheets', 'income_statements', 'cash_flow']

        for statement_type in statement_types:
            logger.info(f"\n{'='*80}")
            logger.info(f"Extracting schema for: {statement_type.upper()}")
            logger.info(f"{'='*80}")

            self._extract_statement_schema(statement_type, sample_size)

        # Generate final unified schema
        unified_schema = self._generate_unified_schema()

        # Print summary
        self._print_summary(unified_schema)

        return unified_schema

    def _extract_statement_schema(self, statement_type: str, sample_size: int = None):
        """Extract schema for one statement type"""

        statement_dir = self.bronze_path / statement_type

        if not statement_dir.exists():
            logger.warning(f"Directory not found: {statement_dir}")
            return

        # Find all parquet files
        all_files = list(statement_dir.rglob("*.parquet"))

        if sample_size:
            import random
            all_files = random.sample(all_files, min(sample_size, len(all_files)))

        logger.info(f"Found {len(all_files)} files")
        logger.info("Scanning files for columns...")

        # Track unique columns and their data types
        unique_columns = set()
        tickers_scanned = set()

        # Scan files in batches to show progress
        batch_size = 100
        for i in range(0, len(all_files), batch_size):
            batch = all_files[i:i+batch_size]

            for file_path in batch:
                try:
                    # Extract ticker from filename
                    ticker = file_path.stem.replace('ticker=', '')
                    tickers_scanned.add(ticker)

                    # Read file and get schema
                    df = pl.read_parquet(file_path)

                    # Collect column info
                    for col_name, dtype in zip(df.columns, df.dtypes):
                        unique_columns.add(col_name)
                        self.column_stats[statement_type][col_name]['count'] += 1
                        self.column_stats[statement_type][col_name]['dtypes'].add(str(dtype))

                except Exception as e:
                    logger.error(f"Failed to read {file_path}: {e}")

            # Progress update
            scanned = min(i + batch_size, len(all_files))
            logger.info(f"  Progress: {scanned}/{len(all_files)} files ({scanned/len(all_files)*100:.1f}%)")

        logger.info(f"\n✓ Completed schema extraction for {statement_type}")
        logger.info(f"  Tickers scanned: {len(tickers_scanned)}")
        logger.info(f"  Unique columns: {len(unique_columns)}")
        logger.info(f"  Total files: {len(all_files)}")

        # Store results
        self.schemas[statement_type] = {
            'columns': sorted(list(unique_columns)),
            'tickers_scanned': len(tickers_scanned),
            'files_scanned': len(all_files)
        }

    def _generate_unified_schema(self) -> Dict:
        """Generate unified schema with metadata"""

        logger.info("\n" + "="*80)
        logger.info("GENERATING UNIFIED SCHEMA")
        logger.info("="*80)

        unified = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'bronze_path': str(self.bronze_path),
                'total_columns': 0
            },
            'statements': {}
        }

        for statement_type in ['balance_sheets', 'income_statements', 'cash_flow']:
            if statement_type not in self.schemas:
                continue

            columns_info = []
            for col_name in self.schemas[statement_type]['columns']:
                stats = self.column_stats[statement_type][col_name]

                # Determine most common dtype
                dtypes_list = list(stats['dtypes'])
                primary_dtype = dtypes_list[0] if len(dtypes_list) == 1 else 'Mixed'

                columns_info.append({
                    'name': col_name,
                    'occurrence_count': stats['count'],
                    'occurrence_pct': round(stats['count'] / self.schemas[statement_type]['files_scanned'] * 100, 1),
                    'dtypes': sorted(list(stats['dtypes'])),
                    'primary_dtype': primary_dtype,
                    'is_common': stats['count'] > (self.schemas[statement_type]['files_scanned'] * 0.5)  # Present in >50% of files
                })

            # Sort by occurrence count
            columns_info.sort(key=lambda x: x['occurrence_count'], reverse=True)

            unified['statements'][statement_type] = {
                'total_columns': len(columns_info),
                'common_columns': sum(1 for c in columns_info if c['is_common']),
                'rare_columns': sum(1 for c in columns_info if not c['is_common']),
                'files_scanned': self.schemas[statement_type]['files_scanned'],
                'tickers_scanned': self.schemas[statement_type]['tickers_scanned'],
                'columns': columns_info
            }

            unified['metadata']['total_columns'] += len(columns_info)

        return unified

    def _print_summary(self, unified_schema: Dict):
        """Print summary of extracted schemas"""

        logger.info("\n" + "="*80)
        logger.info("SCHEMA EXTRACTION SUMMARY")
        logger.info("="*80)

        for statement_type in ['balance_sheets', 'income_statements', 'cash_flow']:
            if statement_type not in unified_schema['statements']:
                continue

            stmt = unified_schema['statements'][statement_type]

            logger.info(f"\n{statement_type.upper()}:")
            logger.info(f"  Total columns: {stmt['total_columns']}")
            logger.info(f"  Common columns (>50% of files): {stmt['common_columns']}")
            logger.info(f"  Rare columns (<50% of files): {stmt['rare_columns']}")
            logger.info(f"  Files scanned: {stmt['files_scanned']}")
            logger.info(f"  Tickers scanned: {stmt['tickers_scanned']}")

            # Show top 10 most common columns
            logger.info(f"\n  Top 10 most common columns:")
            for i, col in enumerate(stmt['columns'][:10], 1):
                logger.info(f"    {i:2d}. {col['name']:40s} ({col['occurrence_pct']:5.1f}% coverage, dtype: {col['primary_dtype']})")

        logger.info(f"\n{'='*80}")
        logger.info(f"TOTAL UNIQUE COLUMNS ACROSS ALL STATEMENTS: {unified_schema['metadata']['total_columns']}")
        logger.info(f"{'='*80}")

    def save_schema(self, unified_schema: Dict, output_path: Path):
        """Save unified schema to JSON file"""

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(unified_schema, f, indent=2)

        logger.info(f"\n✓ Schema saved to: {output_path}")
        logger.info(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")

    def generate_silver_table_schema(self, unified_schema: Dict) -> List[Tuple[str, str]]:
        """
        Generate target silver layer table schema

        Returns list of (column_name, data_type) tuples for CREATE TABLE
        """

        logger.info("\n" + "="*80)
        logger.info("GENERATING SILVER LAYER TABLE SCHEMA")
        logger.info("="*80)

        # Define prefixes to avoid column name collisions
        prefixes = {
            'balance_sheets': 'bs_',
            'income_statements': 'is_',
            'cash_flow': 'cf_'
        }

        silver_columns = []

        # Add identity columns (no prefix)
        identity_cols = [
            ('ticker', 'VARCHAR'),
            ('filing_date', 'DATE'),
            ('fiscal_year', 'INT'),
            ('fiscal_period', 'VARCHAR'),
            ('fiscal_quarter', 'VARCHAR'),
        ]
        silver_columns.extend(identity_cols)

        # Add columns from each statement type with prefix
        for statement_type in ['balance_sheets', 'income_statements', 'cash_flow']:
            if statement_type not in unified_schema['statements']:
                continue

            prefix = prefixes[statement_type]
            stmt = unified_schema['statements'][statement_type]

            for col in stmt['columns']:
                col_name = col['name']

                # Skip identity columns (already added)
                if col_name in ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period', 'fiscal_quarter']:
                    continue

                # Map Polars dtype to SQL dtype
                dtype = col['primary_dtype']
                if 'Int' in dtype or 'UInt' in dtype:
                    sql_dtype = 'BIGINT'
                elif 'Float' in dtype or 'Decimal' in dtype:
                    sql_dtype = 'DECIMAL(20,2)'
                elif 'Date' in dtype:
                    sql_dtype = 'DATE'
                elif 'Datetime' in dtype:
                    sql_dtype = 'TIMESTAMP'
                elif 'Boolean' in dtype or 'Bool' in dtype:
                    sql_dtype = 'BOOLEAN'
                else:
                    sql_dtype = 'VARCHAR'

                silver_columns.append((f'{prefix}{col_name}', sql_dtype))

        # Add calculated/derived columns
        calculated_cols = [
            # Quality metrics
            ('has_complete_data', 'BOOLEAN'),
            ('data_quality_score', 'DECIMAL(5,2)'),

            # Metadata
            ('processed_at', 'TIMESTAMP'),
            ('bronze_files_merged', 'INT'),
        ]
        silver_columns.extend(calculated_cols)

        logger.info(f"Total silver layer columns: {len(silver_columns)}")
        logger.info(f"  Identity columns: {len(identity_cols)}")
        logger.info(f"  Balance sheet columns: {sum(1 for c in silver_columns if c[0].startswith('bs_'))}")
        logger.info(f"  Income statement columns: {sum(1 for c in silver_columns if c[0].startswith('is_'))}")
        logger.info(f"  Cash flow columns: {sum(1 for c in silver_columns if c[0].startswith('cf_'))}")
        logger.info(f"  Calculated columns: {len(calculated_cols)}")

        return silver_columns


def main():
    """Main entry point"""

    # Configuration (using centralized path function)
    bronze_path = get_quantlake_root() / 'fundamentals'
    output_path = Path(__file__).parent.parent.parent / 'config' / 'fundamentals_schema.json'

    # For testing, use sample_size=100 to scan only 100 files per type
    # For production, use sample_size=None to scan all files
    sample_size = None  # Set to 100 for quick testing

    # Initialize extractor
    extractor = FundamentalsSchemaExtractor(bronze_path)

    # Extract schemas
    unified_schema = extractor.extract_all_schemas(sample_size=sample_size)

    # Save to JSON
    extractor.save_schema(unified_schema, output_path)

    # Generate silver table schema
    silver_columns = extractor.generate_silver_table_schema(unified_schema)

    # Save silver table schema as well
    silver_schema_output = output_path.parent / "fundamentals_silver_schema.json"
    with open(silver_schema_output, 'w') as f:
        json.dump({
            'columns': [{'name': name, 'dtype': dtype} for name, dtype in silver_columns],
            'total_columns': len(silver_columns),
            'generated_at': datetime.now().isoformat()
        }, f, indent=2)

    logger.info(f"\n✓ Silver table schema saved to: {silver_schema_output}")

    # Generate SQL CREATE TABLE statement
    logger.info("\n" + "="*80)
    logger.info("SQL CREATE TABLE STATEMENT (Preview)")
    logger.info("="*80)
    logger.info("\nCREATE TABLE fundamentals_wide (")
    for i, (col_name, dtype) in enumerate(silver_columns[:20]):  # Show first 20
        comma = "," if i < len(silver_columns) - 1 else ""
        logger.info(f"    {col_name:50s} {dtype}{comma}")
    logger.info(f"    ... ({len(silver_columns) - 20} more columns)")
    logger.info(")")
    logger.info("PARTITIONED BY (fiscal_year, fiscal_quarter)")
    logger.info("STORED AS PARQUET")
    logger.info("COMPRESSION ZSTD;")

    logger.info("\n" + "="*80)
    logger.info("✓ PHASE 1 COMPLETE - Schema Extraction Successful")
    logger.info("="*80)
    logger.info("\nNext steps:")
    logger.info("  1. Review schema at: " + str(output_path))
    logger.info("  2. Proceed to Phase 2: Fundamental Flattening Pipeline")
    logger.info("  3. Use fundamentals_schema.json for column mapping")
    logger.info("")


if __name__ == '__main__':
    main()
