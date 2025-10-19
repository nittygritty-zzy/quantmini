"""
Metadata Manager - Track ingestion status and dataset metadata

This module provides metadata tracking for ingestion jobs, including
status, timestamps, statistics, and watermarks for incremental processing.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from ..core.exceptions import PipelineException

logger = logging.getLogger(__name__)


class MetadataManagerError(PipelineException):
    """Raised when metadata operations fail"""
    pass


class MetadataManager:
    """
    Manage ingestion metadata and watermarks

    Features:
    - Track ingestion status per date/symbol
    - Store watermarks for incremental processing
    - Record statistics and timestamps
    - Query ingestion history
    - Detect missing dates
    """

    def __init__(self, metadata_root: Path):
        """
        Initialize metadata manager

        Args:
            metadata_root: Root directory for metadata storage
        """
        self.metadata_root = Path(metadata_root)
        self.metadata_root.mkdir(parents=True, exist_ok=True)

        # Track binary conversion status
        self.binary_conversion_file = self.metadata_root / 'binary_conversions.json'

        logger.info(f"MetadataManager initialized (path: {self.metadata_root})")

    def record_ingestion(
        self,
        data_type: str,
        date: str,
        status: str,
        statistics: Dict[str, Any],
        symbol: Optional[str] = None,
        error: Optional[str] = None
    ):
        """
        Record ingestion result

        Args:
            data_type: Data type ('stocks_daily', etc.)
            date: Date string (YYYY-MM-DD)
            status: Status ('success', 'failed', 'skipped')
            statistics: Ingestion statistics
            symbol: Optional symbol (for minute data)
            error: Optional error message
        """
        try:
            # Build metadata record
            record = {
                'data_type': data_type,
                'date': date,
                'symbol': symbol,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'statistics': statistics,
                'error': error,
            }

            # Save to file
            metadata_file = self._get_metadata_file(data_type, date, symbol)
            metadata_file.parent.mkdir(parents=True, exist_ok=True)

            with open(metadata_file, 'w') as f:
                json.dump(record, f, indent=2)

            logger.debug(f"Recorded ingestion: {data_type} / {date} / {status}")

        except Exception as e:
            raise MetadataManagerError(f"Failed to record ingestion: {e}")

    def get_ingestion_status(
        self,
        data_type: str,
        date: str,
        symbol: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get ingestion status for a specific date

        Args:
            data_type: Data type
            date: Date string
            symbol: Optional symbol

        Returns:
            Metadata record or None if not found
        """
        try:
            metadata_file = self._get_metadata_file(data_type, date, symbol)

            if not metadata_file.exists():
                return None

            with open(metadata_file, 'r') as f:
                record = json.load(f)

            return record

        except Exception as e:
            logger.warning(f"Failed to read metadata: {e}")
            return None

    def list_ingestions(
        self,
        data_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List ingestion records with optional filtering

        Args:
            data_type: Data type
            start_date: Optional start date filter
            end_date: Optional end date filter
            status: Optional status filter

        Returns:
            List of metadata records
        """
        try:
            records = []

            metadata_dir = self.metadata_root / data_type
            if not metadata_dir.exists():
                return records

            # Find all metadata files
            for metadata_file in metadata_dir.rglob('*.json'):
                try:
                    with open(metadata_file, 'r') as f:
                        record = json.load(f)

                    # Apply filters
                    if start_date and record['date'] < start_date:
                        continue
                    if end_date and record['date'] > end_date:
                        continue
                    if status and record['status'] != status:
                        continue

                    records.append(record)

                except Exception as e:
                    logger.warning(f"Failed to read {metadata_file}: {e}")

            # Sort by date
            records.sort(key=lambda r: (r['date'], r.get('symbol', '')))

            return records

        except Exception as e:
            raise MetadataManagerError(f"Failed to list ingestions: {e}")

    def get_watermark(
        self,
        data_type: str,
        symbol: Optional[str] = None
    ) -> Optional[str]:
        """
        Get watermark (latest successfully ingested date) for incremental processing

        Args:
            data_type: Data type
            symbol: Optional symbol

        Returns:
            Latest date string or None
        """
        try:
            records = self.list_ingestions(data_type, status='success')

            if symbol:
                records = [r for r in records if r.get('symbol') == symbol]

            if not records:
                return None

            # Return latest date
            latest = max(records, key=lambda r: r['date'])
            return latest['date']

        except Exception as e:
            logger.warning(f"Failed to get watermark: {e}")
            return None

    def set_watermark(
        self,
        data_type: str,
        date: str,
        symbol: Optional[str] = None
    ):
        """
        Set watermark for incremental processing

        Args:
            data_type: Data type
            date: Date string
            symbol: Optional symbol
        """
        try:
            watermark_file = self._get_watermark_file(data_type, symbol)
            watermark_file.parent.mkdir(parents=True, exist_ok=True)

            watermark = {
                'data_type': data_type,
                'symbol': symbol,
                'date': date,
                'timestamp': datetime.now().isoformat(),
            }

            with open(watermark_file, 'w') as f:
                json.dump(watermark, f, indent=2)

            logger.debug(f"Set watermark: {data_type} / {date}")

        except Exception as e:
            raise MetadataManagerError(f"Failed to set watermark: {e}")

    def get_missing_dates(
        self,
        data_type: str,
        start_date: str,
        end_date: str,
        expected_dates: List[str]
    ) -> List[str]:
        """
        Get list of dates that haven't been successfully ingested

        Args:
            data_type: Data type
            start_date: Start of date range
            end_date: End of date range
            expected_dates: List of expected dates (business days)

        Returns:
            List of missing dates
        """
        try:
            # Get successfully ingested dates
            records = self.list_ingestions(
                data_type,
                start_date=start_date,
                end_date=end_date,
                status='success'
            )

            ingested_dates = set(r['date'] for r in records)

            # Find missing
            missing = [d for d in expected_dates if d not in ingested_dates]

            return sorted(missing)

        except Exception as e:
            raise MetadataManagerError(f"Failed to get missing dates: {e}")

    def get_statistics_summary(
        self,
        data_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get aggregated statistics for ingestion jobs

        Args:
            data_type: Data type
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Summary statistics
        """
        try:
            records = self.list_ingestions(data_type, start_date, end_date)

            if not records:
                return {
                    'data_type': data_type,
                    'total_jobs': 0,
                    'success': 0,
                    'failed': 0,
                    'skipped': 0,
                }

            # Aggregate statistics
            total_jobs = len(records)
            success = sum(1 for r in records if r['status'] == 'success')
            failed = sum(1 for r in records if r['status'] == 'failed')
            skipped = sum(1 for r in records if r['status'] == 'skipped')

            # Sum records processed
            total_records = sum(
                r['statistics'].get('records', 0)
                for r in records
                if r['status'] == 'success'
            )

            # Sum file sizes
            total_size_mb = sum(
                r['statistics'].get('file_size_mb', 0)
                for r in records
                if r['status'] == 'success'
            )

            return {
                'data_type': data_type,
                'date_range': {
                    'start': start_date or records[0]['date'],
                    'end': end_date or records[-1]['date'],
                },
                'total_jobs': total_jobs,
                'success': success,
                'failed': failed,
                'skipped': skipped,
                'success_rate': success / total_jobs if total_jobs > 0 else 0,
                'total_records': total_records,
                'total_size_mb': total_size_mb,
            }

        except Exception as e:
            raise MetadataManagerError(f"Failed to get statistics summary: {e}")

    def delete_metadata(
        self,
        data_type: str,
        date: str,
        symbol: Optional[str] = None
    ):
        """
        Delete metadata for a specific date

        Args:
            data_type: Data type
            date: Date string
            symbol: Optional symbol
        """
        try:
            metadata_file = self._get_metadata_file(data_type, date, symbol)

            if metadata_file.exists():
                metadata_file.unlink()
                logger.debug(f"Deleted metadata: {metadata_file}")

        except Exception as e:
            raise MetadataManagerError(f"Failed to delete metadata: {e}")

    def _get_metadata_file(
        self,
        data_type: str,
        date: str,
        symbol: Optional[str] = None
    ) -> Path:
        """
        Get metadata file path

        Args:
            data_type: Data type
            date: Date string
            symbol: Optional symbol

        Returns:
            Path to metadata file
        """
        path = self.metadata_root / data_type / date[:4] / date[5:7]

        if symbol:
            path = path / f"{date}_{symbol}.json"
        else:
            path = path / f"{date}.json"

        return path

    def _get_watermark_file(
        self,
        data_type: str,
        symbol: Optional[str] = None
    ) -> Path:
        """
        Get watermark file path

        Args:
            data_type: Data type
            symbol: Optional symbol

        Returns:
            Path to watermark file
        """
        path = self.metadata_root / data_type

        if symbol:
            path = path / f"watermark_{symbol}.json"
        else:
            path = path / "watermark.json"

        return path

    def is_symbol_converted(self, symbol: str, data_type: str) -> bool:
        """
        Check if symbol has been converted to binary format

        Args:
            symbol: Symbol to check
            data_type: Data type

        Returns:
            True if already converted
        """
        try:
            if not self.binary_conversion_file.exists():
                return False

            # Check if file is empty
            if self.binary_conversion_file.stat().st_size == 0:
                return False

            with open(self.binary_conversion_file, 'r') as f:
                conversions = json.load(f)

            key = f"{data_type}:{symbol}"
            return conversions.get(key, False)

        except Exception as e:
            logger.warning(f"Failed to check conversion status: {e}")
            return False

    def mark_symbol_converted(self, symbol: str, data_type: str):
        """
        Mark symbol as converted to binary format

        Args:
            symbol: Symbol to mark
            data_type: Data type
        """
        try:
            # Load existing conversions
            conversions = {}
            if self.binary_conversion_file.exists():
                with open(self.binary_conversion_file, 'r') as f:
                    conversions = json.load(f)

            # Mark as converted
            key = f"{data_type}:{symbol}"
            conversions[key] = {
                'converted': True,
                'timestamp': datetime.now().isoformat()
            }

            # Save
            with open(self.binary_conversion_file, 'w') as f:
                json.dump(conversions, f, indent=2)

            logger.debug(f"Marked {symbol} as converted for {data_type}")

        except Exception as e:
            logger.warning(f"Failed to mark conversion: {e}")

    def clear_conversion_status(self, data_type: Optional[str] = None):
        """
        Clear binary conversion status

        Args:
            data_type: Optional data type to clear (clears all if None)
        """
        try:
            if not self.binary_conversion_file.exists():
                return

            if data_type is None:
                # Clear all
                self.binary_conversion_file.unlink()
                logger.info("Cleared all conversion status")
            else:
                # Clear specific data type
                with open(self.binary_conversion_file, 'r') as f:
                    conversions = json.load(f)

                # Filter out data type
                conversions = {
                    k: v for k, v in conversions.items()
                    if not k.startswith(f"{data_type}:")
                }

                # Save
                with open(self.binary_conversion_file, 'w') as f:
                    json.dump(conversions, f, indent=2)

                logger.info(f"Cleared conversion status for {data_type}")

        except Exception as e:
            logger.warning(f"Failed to clear conversion status: {e}")

    def __repr__(self) -> str:
        return f"MetadataManager(path={self.metadata_root})"


def main():
    """Command-line interface for metadata manager"""
    import sys
    from ..core.config_loader import ConfigLoader

    try:
        config = ConfigLoader()
        # Use metadata path from Medallion Architecture
        metadata_root = config.get_metadata_path()
        manager = MetadataManager(metadata_root)

        print("✅ MetadataManager initialized")
        print(f"   Root: {metadata_root}")

        # List statistics for all data types
        for data_type in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            stats = manager.get_statistics_summary(data_type)

            if stats['total_jobs'] > 0:
                print(f"\n📊 {data_type}:")
                print(f"   Total jobs: {stats['total_jobs']}")
                print(f"   Success: {stats['success']} ({stats['success_rate']:.1%})")
                print(f"   Failed: {stats['failed']}")
                print(f"   Records: {stats['total_records']:,}")
                print(f"   Size: {stats['total_size_mb']:.1f} MB")

                # Get watermark
                watermark = manager.get_watermark(data_type)
                if watermark:
                    print(f"   Watermark: {watermark}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
