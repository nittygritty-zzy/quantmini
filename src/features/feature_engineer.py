"""
Feature Engineer

Compute features for financial data with adaptive memory usage.

Supports:
- Stock daily/minute features (alpha factors, technical indicators)
- Options features (parsed from ticker, moneyness, etc.)
- Adaptive processing modes (streaming/batch/parallel)
- Incremental updates using watermarks
"""

import gc
from pathlib import Path
from typing import Optional, Dict, List
import logging

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.config_loader import ConfigLoader
from src.storage.metadata_manager import MetadataManager
from src.features.definitions import build_feature_sql, get_feature_list
from src.features.definitions_simple import build_simple_stock_daily_sql
from src.features.definitions_simple_minute import (
    build_simple_stock_minute_sql,
    build_simple_options_daily_sql,
    build_simple_options_minute_sql
)

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """
    Adaptive feature engineering with DuckDB
    
    Follows parent design's adaptive processing modes:
    - Streaming mode (<32GB RAM): DuckDB with memory limits, one date at a time
    - Batch mode (32-64GB): DuckDB with higher limits
    - Parallel mode (>64GB): Future enhancement with Polars
    
    Features:
    - Memory-safe processing with DuckDB
    - Incremental updates (watermark-based)
    - SQL-based feature engineering
    - Automatic partition management
    """
    
    def __init__(
        self,
        parquet_root: Path,
        enriched_root: Path,
        config: ConfigLoader,
        metadata_root: Optional[Path] = None
    ):
        """
        Initialize Feature Engineer
        
        Args:
            parquet_root: Path to raw Parquet data (Phase 4 output)
            enriched_root: Path to write enriched data
            config: ConfigLoader instance
            metadata_root: Path to metadata (default: parquet_root.parent / 'metadata')
        """
        self.parquet_root = Path(parquet_root)
        self.enriched_root = Path(enriched_root)
        self.config = config
        self.profile = config.get('system_profile', {})
        self.mode = self.profile.get('recommended_mode', 'streaming')
        
        # Metadata tracking
        if metadata_root is None:
            metadata_root = self.parquet_root.parent / 'metadata'

        self.metadata = MetadataManager(metadata_root=Path(metadata_root))
        
        # Initialize DuckDB
        self._init_duckdb_engine()
        
        logger.info(f"FeatureEngineer initialized in {self.mode} mode")
        logger.info(f"Raw data: {self.parquet_root}")
        logger.info(f"Enriched output: {self.enriched_root}")
    
    def _init_duckdb_engine(self):
        """Initialize DuckDB for feature engineering"""
        resource_limits = self.profile.get('resource_limits', {})
        hardware = self.profile.get('hardware', {})

        memory_limit = resource_limits.get('max_memory_gb', 8) * 0.5
        threads = min(4, hardware.get('cpu_cores', 4))
        
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': threads,
            'enable_object_cache': True,
            'temp_directory': '/tmp/duckdb_quantmini',
            'preserve_insertion_order': False,  # Faster queries
        })
        
        logger.info(f"DuckDB initialized: {memory_limit}GB memory, {threads} threads")
    
    def enrich_date_range(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        incremental: bool = True
    ) -> Dict:
        """
        Enrich date range with features
        
        Args:
            data_type: stocks_daily, stocks_minute, options_daily, options_minute
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            incremental: Skip already enriched dates
        
        Returns:
            Statistics dict with:
                - dates_processed: Number of dates processed
                - records_enriched: Total records enriched
                - features_added: Number of features added
                - errors: List of errors
        """
        logger.info(f"Starting enrichment: {data_type} from {start_date} to {end_date}")
        logger.info(f"Incremental mode: {incremental}")
        
        # Generate date list
        dates = self._generate_date_list(start_date, end_date)
        
        # Filter to missing dates if incremental
        if incremental:
            enriched_dates = self._get_enriched_dates(data_type)
            dates = [d for d in dates if d not in enriched_dates]
            logger.info(f"Incremental: {len(dates)} dates to process (skipping {len(enriched_dates)} already enriched)")
        
        if len(dates) == 0:
            logger.info("No dates to process")
            return {
                'dates_processed': 0,
                'records_enriched': 0,
                'features_added': 0,
                'errors': []
            }
        
        # Process dates
        stats = {
            'dates_processed': 0,
            'records_enriched': 0,
            'features_added': 0,
            'errors': []
        }
        
        for idx, date in enumerate(dates):
            try:
                logger.info(f"Processing {date} ({idx + 1}/{len(dates)})")
                
                result = self._enrich_date(data_type, date)
                
                if result['records'] > 0:
                    stats['dates_processed'] += 1
                    stats['records_enriched'] += result['records']
                    stats['features_added'] = result['features']

                    logger.info(
                        f"  ✅ Enriched {result['records']:,} records "
                        f"with {result['features']} features"
                    )
                else:
                    logger.info(f"  ⚠️ No data for {date}")
                
                # Periodic garbage collection
                if (idx + 1) % 10 == 0:
                    gc.collect()
                    
            except Exception as e:
                logger.error(f"  ❌ Error processing {date}: {e}")
                stats['errors'].append({'date': date, 'error': str(e)})
        
        logger.info(f"Enrichment complete:")
        logger.info(f"  Dates processed: {stats['dates_processed']}")
        logger.info(f"  Records enriched: {stats['records_enriched']:,}")
        logger.info(f"  Features added: {stats['features_added']}")
        logger.info(f"  Errors: {len(stats['errors'])}")
        
        return stats
    
    def _enrich_date(self, data_type: str, date: str) -> Dict:
        """
        Enrich single date

        Args:
            data_type: Data type
            date: YYYY-MM-DD

        Returns:
            Dict with records count and features count
        """
        # Find input files for this date
        year, month = date.split('-')[0:2]
        # Filter out macOS resource fork files (._*)
        input_pattern = self.parquet_root / data_type / f'year={year}/month={month}/date=*.parquet'

        # Determine if this is minute-level data (timestamp) or daily (date)
        is_minute = 'minute' in data_type

        # Create view from Parquet files
        try:
            if is_minute:
                # Minute data uses timestamp, need to extract date and filter
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW raw_data AS
                    SELECT *,
                           CAST(timestamp AS DATE) as date
                    FROM read_parquet('{input_pattern}', union_by_name=true)
                    WHERE CAST(timestamp AS DATE) = '{date}'
                """)
            else:
                # Daily data has date column
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW raw_data AS
                    SELECT * FROM read_parquet('{input_pattern}', union_by_name=true)
                    WHERE date = '{date}'
                """)
        except Exception as e:
            logger.warning(f"Could not read data for {date}: {e}")
            return {'records': 0, 'features': 0}
        
        # Check if we have data
        count_result = self.conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()
        if count_result[0] == 0:
            return {'records': 0, 'features': 0}
        
        # Build feature SQL
        # Use simplified versions to avoid DuckDB nested window function issues
        if data_type == 'stocks_daily':
            feature_sql = build_simple_stock_daily_sql(base_view='raw_data')
        elif data_type == 'stocks_minute':
            feature_sql = build_simple_stock_minute_sql(base_view='raw_data')
        elif data_type == 'options_daily':
            feature_sql = build_simple_options_daily_sql(base_view='raw_data')
        elif data_type == 'options_minute':
            feature_sql = build_simple_options_minute_sql(base_view='raw_data')
        else:
            feature_sql = build_feature_sql(data_type, base_view='raw_data')
        
        # Execute and fetch
        enriched_df = self.conn.execute(feature_sql).fetch_df()
        
        if len(enriched_df) == 0:
            return {'records': 0, 'features': 0}
        
        # Write output
        output_path = self.enriched_root / data_type / f'year={year}/month={month}'
        output_path.mkdir(parents=True, exist_ok=True)
        output_file = output_path / f'{date}.parquet'
        
        # Convert to PyArrow table and write
        table = pa.Table.from_pandas(enriched_df)
        pq.write_table(
            table,
            output_file,
            compression='zstd',
            compression_level=3
        )
        
        return {
            'records': len(enriched_df),
            'features': len(enriched_df.columns)
        }
    
    def _generate_date_list(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end"""
        import pandas as pd
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        return [d.strftime('%Y-%m-%d') for d in dates]

    def _get_enriched_dates(self, data_type: str) -> set:
        """Get set of dates that have already been enriched"""
        enriched_path = self.enriched_root / data_type
        if not enriched_path.exists():
            return set()

        # Find all enriched parquet files and extract dates from filenames
        enriched_dates = set()
        for file in enriched_path.rglob('*.parquet'):
            # Filename format: YYYY-MM-DD.parquet
            date_str = file.stem
            if len(date_str) == 10 and date_str.count('-') == 2:
                enriched_dates.add(date_str)

        return enriched_dates
    
    def get_enrichment_status(self, data_type: str) -> Dict:
        """
        Get enrichment status for data type

        Args:
            data_type: Data type

        Returns:
            Dict with status information
        """
        enriched_dates = self._get_enriched_dates(data_type)
        return {
            'data_type': data_type,
            'dates_enriched': len(enriched_dates),
            'enriched_dates': sorted(enriched_dates)
        }
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
