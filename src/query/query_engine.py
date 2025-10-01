# Query engine implementation
from pathlib import Path
import duckdb
import logging
from typing import List

from ..core.config_loader import ConfigLoader
from ..core.system_profiler import SystemProfiler
from .query_cache import QueryCache

logger = logging.getLogger(__name__)


class QueryEngine:
    def __init__(self, data_root: Path, config: ConfigLoader, enable_cache: bool = True):
        self.data_root = Path(data_root)
        self.config = config
        
        # Get system profile
        profiler = SystemProfiler()
        self.profile = profiler.profile
        self.mode = self.profile['recommended_mode']
        
        # Initialize DuckDB
        memory_limit = self.profile['resource_limits']['max_memory_gb'] * 0.5
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': min(4, self.profile['hardware']['cpu_cores']),
            'enable_object_cache': True
        })
        self.engine = 'duckdb'
        
        # Query cache
        if enable_cache:
            self.cache = QueryCache(
                cache_root=self.data_root / 'cache',
                max_size_gb=2.0
            )
        else:
            self.cache = None
        
        logger.info(f"QueryEngine initialized (backend: {self.engine})")

    def query_parquet(self, data_type: str, symbols: List[str], fields: List[str],
                      start_date: str, end_date: str, use_cache: bool = True):
        # Check cache
        if use_cache and self.cache:
            cache_key = self.cache.make_key(
                data_type=data_type,
                symbols=sorted(symbols),
                fields=sorted(fields),
                start_date=start_date,
                end_date=end_date
            )
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {data_type} query")
                return cached
        
        # Execute query
        enriched_path = self.data_root / 'enriched' / data_type / '**/*.parquet'
        symbol_col = 'ticker' if 'options' in data_type else 'symbol'
        time_col = 'timestamp' if 'minute' in data_type else 'date'
        
        if 'minute' in data_type:
            date_select = f"CAST({time_col} AS DATE) as date"
            date_filter_col = f"CAST({time_col} AS DATE)"
        else:
            date_select = time_col
            date_filter_col = time_col
        
        field_list = ', '.join(fields)
        symbols_list = ', '.join(f"'{s}'" for s in symbols)
        
        sql = f"""
        SELECT {symbol_col}, {date_select}, {field_list}
        FROM read_parquet('{enriched_path}')
        WHERE {symbol_col} IN ({symbols_list})
          AND {date_filter_col} >= '{start_date}'
          AND {date_filter_col} <= '{end_date}'
        ORDER BY {symbol_col}, date
        """
        
        logger.debug(f"Executing DuckDB query for {len(symbols)} symbols, {len(fields)} fields")
        result = self.conn.execute(sql).fetch_df()
        logger.debug(f"Query returned {len(result)} rows")
        
        # Cache result
        if use_cache and self.cache:
            self.cache.put(cache_key, result)
        
        return result

    def get_cache_stats(self):
        if self.cache:
            return self.cache.get_stats()
        return None

    def clear_cache(self):
        if self.cache:
            self.cache.clear()

    def close(self):
        if self.conn:
            self.conn.close()

    def __repr__(self) -> str:
        cache_info = f", cache={self.cache}" if self.cache else ""
        return f"QueryEngine(backend={self.engine}{cache_info})"
