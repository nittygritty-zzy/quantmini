#!/usr/bin/env python3
"""
Consolidate Ticker Reference Data (Bronze → Silver)

Consolidates partitioned ticker reference data into optimized Silver layer tables:
1. tickers_master - Complete ticker metadata with details
2. ticker_relationships - Related companies/tickers
3. ticker_types - Ticker type definitions

Input: Bronze layer partitioned data
Output: Silver layer consolidated Parquet files
"""

import sys
from pathlib import Path
import polars as pl
import logging
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def consolidate_tickers(bronze_path: Path, silver_path: Path):
    """
    Consolidate ticker data and create query-optimized screening tables

    Creates:
    1. tickers_universe - All tickers (complete dataset)
    2. tickers_screening - Active US stocks/ETFs optimized for queries
    3. tickers_by_sector - Grouped by sector/industry
    4. tickers_by_market_cap - Classified by market cap ranges

    Args:
        bronze_path: Bronze layer root path
        silver_path: Silver layer root path
    """
    logger.info("Consolidating ticker metadata for screening/analysis...")

    tickers_dir = bronze_path / 'reference_data' / 'tickers'

    if not tickers_dir.exists():
        logger.warning(f"Tickers directory not found: {tickers_dir}")
        return None

    # Read all partitions
    all_dfs = []

    for locale_dir in tickers_dir.iterdir():
        if not locale_dir.is_dir() or not locale_dir.name.startswith('locale='):
            continue

        locale = locale_dir.name.split('=')[1]

        for type_dir in locale_dir.iterdir():
            if not type_dir.is_dir() or not type_dir.name.startswith('type='):
                continue

            ticker_type = type_dir.name.split('=')[1]
            data_file = type_dir / 'data.parquet'

            if not data_file.exists():
                continue

            logger.info(f"Reading {locale}/{ticker_type}...")
            df = pl.read_parquet(data_file)
            all_dfs.append(df)

    if not all_dfs:
        logger.warning("No ticker data found to consolidate")
        return None

    # Concatenate all partitions
    tickers_df = pl.concat(all_dfs, how="diagonal")

    # Deduplicate by ticker symbol (keep latest)
    if 'ticker' in tickers_df.columns:
        tickers_df = tickers_df.unique(subset=['ticker'], keep='last')

    logger.info(f"Consolidated {len(tickers_df):,} tickers")

    # Save to silver
    output_dir = silver_path / 'reference'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Complete universe (all tickers)
    output_file = output_dir / 'tickers_universe.parquet'
    tickers_df.write_parquet(output_file, compression='zstd')
    logger.info(f"✅ Saved tickers_universe: {len(tickers_df):,} tickers")

    # 2. Screening table - Active US stocks/ETFs only (optimized for queries)
    screening_types = ['CS', 'ETF', 'ADRC', 'ADRP', 'ADRR']

    screening_df = tickers_df.filter(
        (pl.col('active') == True) &
        (pl.col('locale') == 'us') &
        (pl.col('type').is_in(screening_types))
    )

    # Add market cap classification
    if 'market_cap' in screening_df.columns:
        screening_df = screening_df.with_columns(
            pl.when(pl.col('market_cap') >= 200_000_000_000)
            .then(pl.lit('mega'))
            .when(pl.col('market_cap') >= 10_000_000_000)
            .then(pl.lit('large'))
            .when(pl.col('market_cap') >= 2_000_000_000)
            .then(pl.lit('mid'))
            .when(pl.col('market_cap') >= 300_000_000)
            .then(pl.lit('small'))
            .when(pl.col('market_cap') >= 50_000_000)
            .then(pl.lit('micro'))
            .otherwise(pl.lit('nano'))
            .alias('cap_class')
        )

    # Sort by market cap (descending) for efficient queries
    if 'market_cap' in screening_df.columns:
        screening_df = screening_df.sort('market_cap', descending=True)

    screening_file = output_dir / 'tickers_screening.parquet'
    screening_df.write_parquet(screening_file, compression='zstd')
    logger.info(f"✅ Saved tickers_screening: {len(screening_df):,} active US stocks/ETFs")

    # 3. By sector (for sector analysis)
    if 'sic_code' in screening_df.columns or 'sector' in screening_df.columns:
        sector_file = output_dir / 'tickers_by_sector.parquet'

        # Sort by sector/industry for grouped queries
        sort_cols = []
        if 'sector' in screening_df.columns:
            sort_cols.append('sector')
        if 'sic_code' in screening_df.columns:
            sort_cols.append('sic_code')
        if 'market_cap' in screening_df.columns:
            sort_cols.append('market_cap')

        if sort_cols:
            sector_df = screening_df.sort(sort_cols, descending=[False] * (len(sort_cols) - 1) + [True])
            sector_df.write_parquet(sector_file, compression='zstd')
            logger.info(f"✅ Saved tickers_by_sector: {len(sector_df):,} tickers (sorted by sector/industry)")

    # 4. By market cap (for cap-based screening)
    if 'cap_class' in screening_df.columns:
        cap_file = output_dir / 'tickers_by_market_cap.parquet'
        cap_df = screening_df.sort(['cap_class', 'market_cap'], descending=[False, True])
        cap_df.write_parquet(cap_file, compression='zstd')
        logger.info(f"✅ Saved tickers_by_market_cap: {len(cap_df):,} tickers (sorted by cap class)")

    logger.info(f"   Columns: {tickers_df.columns}")

    return tickers_df


def consolidate_relationships(bronze_path: Path, silver_path: Path):
    """
    Consolidate ticker relationships and create query-optimized graph structures

    Creates:
    1. ticker_relationships - Complete graph (source_ticker -> ticker)
    2. ticker_relationships_indexed - Indexed by source for fast lookups
    3. relationship_stats - Aggregated statistics per ticker

    Args:
        bronze_path: Bronze layer root path
        silver_path: Silver layer root path
    """
    logger.info("Consolidating ticker relationships for graph analysis...")

    relationships_dir = bronze_path / 'reference_data' / 'relationships'

    if not relationships_dir.exists():
        logger.warning(f"Relationships directory not found: {relationships_dir}")
        return None

    # Read all relationship files
    all_files = list(relationships_dir.glob('ticker=*.parquet'))

    if not all_files:
        logger.warning("No relationship data found")
        return None

    logger.info(f"Reading {len(all_files):,} relationship files...")

    all_dfs = []
    for file in all_files:
        df = pl.read_parquet(file)
        all_dfs.append(df)

    # Concatenate
    relationships_df = pl.concat(all_dfs, how="diagonal")

    # Deduplicate
    if 'source_ticker' in relationships_df.columns and 'ticker' in relationships_df.columns:
        relationships_df = relationships_df.unique(subset=['source_ticker', 'ticker'], keep='last')

    logger.info(f"Consolidated {len(relationships_df):,} relationships")

    # Save to silver
    output_dir = silver_path / 'reference'
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Complete relationship graph
    output_file = output_dir / 'ticker_relationships.parquet'
    relationships_df.write_parquet(output_file, compression='zstd')
    logger.info(f"✅ Saved ticker_relationships: {len(relationships_df):,} edges")

    # 2. Indexed by source ticker (for fast "find all related" queries)
    indexed_file = output_dir / 'ticker_relationships_indexed.parquet'
    indexed_df = relationships_df.sort('source_ticker')
    indexed_df.write_parquet(indexed_file, compression='zstd')
    logger.info(f"✅ Saved ticker_relationships_indexed (sorted by source)")

    # 3. Relationship statistics (for quick analysis)
    if 'source_ticker' in relationships_df.columns:
        stats_df = relationships_df.group_by('source_ticker').agg([
            pl.count().alias('num_relationships'),
            pl.col('ticker').alias('related_tickers')
        ])

        stats_file = output_dir / 'relationship_stats.parquet'
        stats_df.write_parquet(stats_file, compression='zstd')
        logger.info(f"✅ Saved relationship_stats: {len(stats_df):,} tickers with relationships")

    return relationships_df


def consolidate_ticker_metadata(bronze_path: Path, silver_path: Path):
    """
    Consolidate ticker metadata (detailed ticker info)

    Args:
        bronze_path: Bronze layer root path
        silver_path: Silver layer root path
    """
    logger.info("Consolidating ticker metadata (detailed)...")

    # Look for ticker_metadata.parquet file
    metadata_file = bronze_path / 'reference' / 'ticker_metadata.parquet'

    if not metadata_file.exists():
        logger.warning(f"Ticker metadata file not found: {metadata_file}")
        return None

    logger.info(f"Reading {metadata_file}...")
    metadata_df = pl.read_parquet(metadata_file)

    logger.info(f"Found {len(metadata_df):,} ticker metadata records")

    # Save to silver
    output_dir = silver_path / 'reference'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / 'ticker_metadata.parquet'
    metadata_df.write_parquet(output_file, compression='zstd')

    logger.info(f"✅ Saved ticker_metadata to {output_file}")

    return metadata_df


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Consolidate ticker reference data (Bronze → Silver)'
    )
    parser.add_argument(
        '--bronze-dir',
        type=Path,
        help='Bronze layer directory (default: from config)'
    )
    parser.add_argument(
        '--silver-dir',
        type=Path,
        help='Silver layer directory (default: from config)'
    )

    args = parser.parse_args()

    # Load config
    config = ConfigLoader()

    # Get paths
    bronze_path = args.bronze_dir or config.get_bronze_path()
    silver_path = args.silver_dir or config.get_silver_path()

    logger.info(f"Bronze path: {bronze_path}")
    logger.info(f"Silver path: {silver_path}")
    logger.info("")

    # Consolidate all ticker reference data
    logger.info("=" * 80)
    logger.info("TICKER REFERENCE DATA CONSOLIDATION (Bronze → Silver)")
    logger.info("=" * 80)
    logger.info("")

    # 1. Tickers master table
    tickers_df = consolidate_tickers(bronze_path, silver_path)

    # 2. Ticker relationships
    relationships_df = consolidate_relationships(bronze_path, silver_path)

    # 3. Ticker metadata (detailed)
    metadata_df = consolidate_ticker_metadata(bronze_path, silver_path)

    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("CONSOLIDATION COMPLETE")
    logger.info("=" * 80)

    if tickers_df is not None:
        logger.info(f"✅ Tickers Master: {len(tickers_df):,} records")

    if relationships_df is not None:
        logger.info(f"✅ Ticker Relationships: {len(relationships_df):,} records")

    if metadata_df is not None:
        logger.info(f"✅ Ticker Metadata: {len(metadata_df):,} records")

    logger.info(f"📂 Output: {silver_path / 'reference'}")
    logger.info("")


if __name__ == '__main__':
    main()
