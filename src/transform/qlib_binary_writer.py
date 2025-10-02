"""
Qlib Binary Writer - Convert enriched Parquet to Qlib binary format

This module converts enriched Parquet files to Qlib's binary format for ML/backtesting.
Follows Qlib's binary format specification:
- instruments/all.txt: List of symbols
- calendars/day.txt: Trading days
- features/{symbol}/{feature}.day.bin: Binary feature data

Based on: pipeline_design/PHASE5-8_DESIGN.md Phase 6
"""

from pathlib import Path
import struct
import numpy as np
import duckdb
from typing import List, Dict, Optional
import logging

from ..core.config_loader import ConfigLoader
from ..core.system_profiler import SystemProfiler
from ..core.exceptions import PipelineException

logger = logging.getLogger(__name__)


class QlibBinaryWriterError(PipelineException):
    """Raised when binary conversion fails"""
    pass


class QlibBinaryWriter:
    """
    Convert enriched Parquet to Qlib binary format

    Follows Qlib's binary format specification:
    - instruments/all.txt: List of symbols
    - calendars/day.txt: Trading days
    - features/{symbol}/{feature}.day.bin: Binary feature data

    Processing modes:
    - Streaming: One symbol at a time (default)
    - Batch: Batches of symbols (future)
    - Parallel: Parallel symbol processing (future)
    """

    def __init__(
        self,
        enriched_root: Path,
        qlib_root: Path,
        config: ConfigLoader
    ):
        """
        Initialize Qlib binary writer

        Args:
            enriched_root: Root directory for enriched Parquet files
            qlib_root: Root directory for Qlib binary output
            config: Configuration loader
        """
        self.enriched_root = Path(enriched_root)
        self.qlib_root = Path(qlib_root)
        self.config = config

        # Get system profile
        profiler = SystemProfiler()
        self.profile = profiler.profile
        self.mode = self.profile['recommended_mode']

        # DuckDB for queries
        memory_limit = self.profile['resource_limits']['max_memory_gb'] * 0.5
        self.conn = duckdb.connect(':memory:', config={
            'memory_limit': f'{memory_limit}GB',
            'threads': min(4, self.profile['hardware']['cpu_cores'])
        })

        logger.info(f"QlibBinaryWriter initialized (mode: {self.mode})")

    def _get_symbol_column(self, data_type: str) -> str:
        """Get the symbol column name for a data type"""
        if 'options' in data_type:
            return 'ticker'
        return 'symbol'

    def _get_time_column(self, data_type: str) -> str:
        """Get the time column name for a data type"""
        if 'minute' in data_type:
            return 'timestamp'
        return 'date'

    def convert_data_type(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        incremental: bool = True,
        metadata_manager: Optional[object] = None
    ) -> Dict[str, any]:
        """
        Convert entire data type to Qlib binary

        Args:
            data_type: stocks_daily, stocks_minute, options_daily, options_minute
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            incremental: Only convert new/updated symbols
            metadata_manager: Optional metadata manager for tracking

        Returns:
            Statistics dict
        """
        try:
            logger.info(f"Converting {data_type} to Qlib binary format")
            logger.info(f"  Date range: {start_date} to {end_date}")
            logger.info(f"  Incremental: {incremental}")

            output_dir = self.qlib_root / data_type
            output_dir.mkdir(parents=True, exist_ok=True)

            # Step 1: Generate instruments file
            logger.info("Step 1: Generating instruments file...")
            symbols = self._generate_instruments(data_type, output_dir, start_date, end_date)
            logger.info(f"  Found {len(symbols)} symbols")

            # Step 2: Generate calendar file
            logger.info("Step 2: Generating calendar file...")
            trading_days = self._generate_calendar(data_type, start_date, end_date, output_dir)
            logger.info(f"  Found {len(trading_days)} trading days")

            # Step 3: Convert features for each symbol
            logger.info("Step 3: Converting features to binary...")
            stats = self._convert_features(
                data_type=data_type,
                symbols=symbols,
                trading_days=trading_days,
                output_dir=output_dir,
                incremental=incremental,
                metadata_manager=metadata_manager
            )

            # Step 4: Create Qlib metadata file (required for frequency detection)
            logger.info("Step 4: Creating Qlib metadata...")
            self._create_qlib_metadata(output_dir, data_type)

            # Step 5: Clean up macOS metadata files
            logger.info("Step 5: Cleaning up macOS metadata files...")
            self._cleanup_macos_metadata(output_dir)

            result = {
                'symbols_converted': len(symbols),
                'trading_days': len(trading_days),
                **stats
            }

            logger.info(f"✅ Conversion complete:")
            logger.info(f"  Symbols: {result['symbols_converted']}")
            logger.info(f"  Features: {result['features_written']}")
            logger.info(f"  Size: {result['bytes_written'] / 1024 / 1024:.1f} MB")

            return result

        except Exception as e:
            raise QlibBinaryWriterError(f"Failed to convert {data_type}: {e}")

    def _generate_instruments(self, data_type: str, output_dir: Path, start_date: str, end_date: str) -> List[str]:
        """
        Generate instruments/all.txt in Qlib format (tab-separated with date ranges)

        Args:
            data_type: Data type
            output_dir: Output directory
            start_date: Start date for instruments
            end_date: End date for instruments

        Returns:
            List of symbols
        """
        try:
            # Query unique symbols/tickers (filter out nulls)
            input_pattern = self.enriched_root / data_type / '**/*.parquet'
            symbol_col = self._get_symbol_column(data_type)

            symbols_df = self.conn.execute(f"""
                SELECT DISTINCT {symbol_col}
                FROM read_parquet('{input_pattern}')
                WHERE {symbol_col} IS NOT NULL
                ORDER BY {symbol_col}
            """).fetch_df()

            symbols = symbols_df[symbol_col].tolist()

            # Filter out any remaining null/NaN values
            symbols = [s for s in symbols if s and str(s).lower() != 'nan']

            # Write instruments file in Qlib format (tab-separated: SYMBOL\tSTART\tEND)
            instruments_dir = output_dir / 'instruments'
            instruments_dir.mkdir(parents=True, exist_ok=True)

            with open(instruments_dir / 'all.txt', 'w') as f:
                for symbol in symbols:
                    f.write(f"{symbol}\t{start_date}\t{end_date}\n")

            logger.debug(f"Generated instruments file with {len(symbols)} symbols")

            return symbols

        except Exception as e:
            raise QlibBinaryWriterError(f"Failed to generate instruments: {e}")

    def _generate_calendar(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        output_dir: Path
    ) -> List[str]:
        """
        Generate calendars/day.txt

        Args:
            data_type: Data type
            start_date: Start date
            end_date: End date
            output_dir: Output directory

        Returns:
            List of trading days
        """
        try:
            # Query unique trading days
            input_pattern = self.enriched_root / data_type / '**/*.parquet'

            # All parquet files have 'date' column now (added during ingestion)
            # Use it directly instead of casting timestamp
            date_expr = 'date'

            dates_df = self.conn.execute(f"""
                SELECT DISTINCT {date_expr} as date
                FROM read_parquet('{input_pattern}')
                WHERE {date_expr} >= '{start_date}' AND {date_expr} <= '{end_date}'
                ORDER BY date
            """).fetch_df()

            trading_days = dates_df['date'].astype(str).tolist()

            # Write calendar file
            calendars_dir = output_dir / 'calendars'
            calendars_dir.mkdir(parents=True, exist_ok=True)

            with open(calendars_dir / 'day.txt', 'w') as f:
                for date in trading_days:
                    f.write(f"{date}\n")

            logger.debug(f"Generated calendar file with {len(trading_days)} trading days")

            return trading_days

        except Exception as e:
            raise QlibBinaryWriterError(f"Failed to generate calendar: {e}")

    def _convert_features(
        self,
        data_type: str,
        symbols: List[str],
        trading_days: List[str],
        output_dir: Path,
        incremental: bool,
        metadata_manager: Optional[object] = None
    ) -> Dict[str, any]:
        """
        Convert features to binary format

        Args:
            data_type: Data type
            symbols: List of symbols
            trading_days: List of trading days
            output_dir: Output directory
            incremental: Only convert new/updated symbols
            metadata_manager: Optional metadata manager

        Returns:
            Statistics dict
        """
        stats = {
            'symbols_converted': 0,
            'features_written': 0,
            'bytes_written': 0,
            'errors': []
        }

        # Get feature list
        features = self._get_feature_list(data_type)
        logger.debug(f"Converting {len(features)} features per symbol")

        # Determine file extension
        extension = '.day.bin' if 'daily' in data_type else '.1min.bin'

        # Process symbols based on mode
        if self.mode == 'streaming':
            # One symbol at a time
            for idx, symbol in enumerate(symbols):
                try:
                    # Check if already converted (incremental mode)
                    if incremental and metadata_manager:
                        if metadata_manager.is_symbol_converted(symbol, data_type):
                            logger.debug(f"Skipping {symbol} (already converted)")
                            continue

                    result = self._convert_symbol(
                        data_type=data_type,
                        symbol=symbol,
                        features=features,
                        trading_days=trading_days,
                        output_dir=output_dir,
                        extension=extension
                    )

                    stats['symbols_converted'] += 1
                    stats['features_written'] += result['features_written']
                    stats['bytes_written'] += result['bytes_written']

                    # Mark as converted
                    if metadata_manager:
                        metadata_manager.mark_symbol_converted(symbol, data_type)

                    if (idx + 1) % 100 == 0:
                        logger.info(f"  Progress: {idx + 1}/{len(symbols)} symbols")

                except Exception as e:
                    logger.error(f"Failed to convert {symbol}: {e}")
                    stats['errors'].append({'symbol': symbol, 'error': str(e)})

        else:
            # TODO: Implement batch/parallel modes
            raise NotImplementedError("Batch/parallel modes coming soon")

        return stats

    def _convert_symbol(
        self,
        data_type: str,
        symbol: str,
        features: List[str],
        trading_days: List[str],
        output_dir: Path,
        extension: str
    ) -> Dict[str, int]:
        """
        Convert single symbol's features to binary

        Args:
            data_type: Data type
            symbol: Symbol to convert
            features: List of features
            trading_days: List of trading days
            output_dir: Output directory
            extension: File extension (.day.bin or .1min.bin)

        Returns:
            Statistics dict
        """
        # Query symbol data
        input_pattern = self.enriched_root / data_type / '**/*.parquet'
        symbol_col = self._get_symbol_column(data_type)
        time_col = self._get_time_column(data_type)

        # For minute data, aggregate to daily
        if 'minute' in data_type:
            # Aggregate minute data to daily using open, high, low, close, sum(volume), etc.
            symbol_df = self.conn.execute(f"""
                SELECT
                    {symbol_col},
                    CAST({time_col} AS DATE) as date,
                    FIRST(open) as open,
                    MAX(high) as high,
                    MIN(low) as low,
                    LAST(close) as close,
                    SUM(volume) as volume,
                    SUM(transactions) as transactions,
                    AVG(minute_return) as minute_return_avg,
                    AVG(spread) as spread,
                    AVG(typical_price) as typical_price,
                    AVG(avg_trade_size) as avg_trade_size,
                    AVG(vwap_approx) as vwap_approx
                FROM read_parquet('{input_pattern}')
                WHERE {symbol_col} = '{symbol}'
                GROUP BY {symbol_col}, CAST({time_col} AS DATE)
                ORDER BY date
            """).fetch_df()
        else:
            symbol_df = self.conn.execute(f"""
                SELECT *
                FROM read_parquet('{input_pattern}')
                WHERE {symbol_col} = '{symbol}'
                ORDER BY date
            """).fetch_df()

        if len(symbol_df) == 0:
            logger.warning(f"No data found for {symbol}")
            return {'features_written': 0, 'bytes_written': 0}

        # Create symbol directory
        symbol_dir = output_dir / 'features' / symbol.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)

        features_written = 0
        bytes_written = 0

        # Write each feature as binary
        for feature in features:
            if feature not in symbol_df.columns:
                logger.debug(f"Feature {feature} not found for {symbol}")
                continue

            # Get feature values aligned to trading days
            feature_values = self._align_to_calendar(
                df=symbol_df,
                feature=feature,
                trading_days=trading_days
            )

            # Write binary file
            binary_path = symbol_dir / f'{feature}{extension}'
            bytes_count = self._write_binary_file(binary_path, feature_values)

            features_written += 1
            bytes_written += bytes_count

        return {
            'features_written': features_written,
            'bytes_written': bytes_written
        }

    def _align_to_calendar(
        self,
        df,
        feature: str,
        trading_days: List[str]
    ) -> np.ndarray:
        """
        Align feature values to trading calendar

        Qlib requires all symbols to have same length arrays.
        Fill missing dates with NaN.

        Args:
            df: DataFrame with symbol data
            feature: Feature name
            trading_days: List of trading days

        Returns:
            Aligned numpy array
        """
        # Create date index
        df_indexed = df.set_index('date')[feature]

        # Reindex to trading calendar
        aligned = df_indexed.reindex(trading_days)

        # Convert to float32 array
        values = aligned.values.astype(np.float32)

        return values

    def _write_binary_file(self, path: Path, values: np.ndarray) -> int:
        """
        Write Qlib binary file format

        Format:
        - 4 bytes: record count (uint32, little-endian)
        - N * 4 bytes: float32 values (little-endian)

        Args:
            path: Output file path
            values: Numpy array of values

        Returns:
            Bytes written
        """
        with open(path, 'wb') as f:
            # Write count header
            f.write(struct.pack('<I', len(values)))

            # Write float32 values
            values.tofile(f)

        return path.stat().st_size

    def _get_feature_list(self, data_type: str) -> List[str]:
        """
        Get list of features to convert

        Args:
            data_type: Data type

        Returns:
            List of feature names
        """
        # Query one row to get columns
        input_pattern = self.enriched_root / data_type / '**/*.parquet'

        sample_df = self.conn.execute(f"""
            SELECT * FROM read_parquet('{input_pattern}')
            LIMIT 1
        """).fetch_df()

        # Exclude metadata columns
        exclude = ['symbol', 'date', 'timestamp', 'year', 'month', 'ticker']
        features = [col for col in sample_df.columns if col not in exclude]

        return features

    def _create_qlib_metadata(self, output_dir: Path, data_type: str):
        """
        Create .qlib/dataset_info.json with frequency metadata

        Qlib requires this file to detect the data frequency.

        Args:
            output_dir: Qlib data directory
            data_type: Data type (to determine frequency)
        """
        import json

        # Determine frequency from data type
        if 'minute' in data_type:
            freq = 'min'
        elif 'daily' in data_type or 'day' in data_type:
            freq = 'day'
        else:
            freq = 'day'  # Default to daily

        # Create .qlib directory
        qlib_metadata_dir = output_dir / '.qlib'
        qlib_metadata_dir.mkdir(parents=True, exist_ok=True)

        # Write dataset_info.json
        metadata = {
            "freq": [freq]
        }

        metadata_file = qlib_metadata_dir / 'dataset_info.json'
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.debug(f"Created Qlib metadata with frequency: {freq}")

    def _cleanup_macos_metadata(self, output_dir: Path):
        """
        Remove macOS metadata files (._*) that interfere with Qlib

        These files are created automatically by macOS and cause issues
        with Qlib's frequency detection via glob patterns.

        Args:
            output_dir: Directory to clean
        """
        import subprocess

        try:
            # Use find command to remove all ._* files
            result = subprocess.run(
                ['find', str(output_dir), '-name', '._*', '-type', 'f', '-delete'],
                capture_output=True,
                text=True,
                check=False
            )

            if result.returncode == 0:
                logger.debug("Cleaned up macOS metadata files")
            else:
                logger.warning(f"Cleanup had warnings: {result.stderr}")

        except Exception as e:
            logger.warning(f"Could not cleanup macOS metadata: {e}")

    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
            logger.debug("Closed DuckDB connection")

    def __repr__(self) -> str:
        return f"QlibBinaryWriter(enriched={self.enriched_root}, qlib={self.qlib_root})"


def main():
    """Command-line interface for binary writer"""
    import sys
    from ..core.config_loader import ConfigLoader

    try:
        config = ConfigLoader()

        enriched_root = Path('data/enriched')
        qlib_root = Path('data/qlib')

        writer = QlibBinaryWriter(enriched_root, qlib_root, config)

        print("✅ QlibBinaryWriter initialized")
        print(f"   Enriched root: {enriched_root}")
        print(f"   Qlib root: {qlib_root}")
        print(f"   Mode: {writer.mode}")

        # Example conversion (requires data)
        # result = writer.convert_data_type(
        #     data_type='stocks_daily',
        #     start_date='2025-01-01',
        #     end_date='2025-09-30',
        #     incremental=False
        # )
        # print(f"\n✅ Conversion complete: {result}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
