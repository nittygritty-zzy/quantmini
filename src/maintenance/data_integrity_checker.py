"""
Data Integrity Checker - Detect gaps and generate backfill commands

This module scans parquet and qlib binary data to identify missing dates
and generates backfill commands to fill the gaps.

Features:
- Check parquet data completeness
- Check qlib binary data completeness
- Compare against expected date ranges
- Generate backfill commands for missing dates
- Optionally run backfill directly

Supported data types:
- stocks_daily
- stocks_minute
- options_daily
"""

import asyncio
import duckdb
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import sys

from ..core.config_loader import ConfigLoader
from ..core.exceptions import PipelineException
from ..orchestration.ingestion_orchestrator import IngestionOrchestrator
from ..transform.qlib_binary_writer import QlibBinaryWriter

logger = logging.getLogger(__name__)


class DataIntegrityError(PipelineException):
    """Raised when data integrity check fails"""
    pass


class DataIntegrityChecker:
    """
    Check data integrity and generate backfill commands

    Workflow:
    1. Scan parquet files to find existing dates
    2. Scan qlib binary calendars to find existing dates
    3. Identify date gaps within each data range
    4. Generate backfill commands for missing dates
    5. Optionally execute backfill
    """

    def __init__(
        self,
        parquet_root: Optional[Path] = None,
        qlib_root: Optional[Path] = None,
        config: Optional[ConfigLoader] = None
    ):
        """
        Initialize integrity checker

        Args:
            parquet_root: Root directory for parquet files
            config: Configuration loader
            qlib_root: Root directory for qlib binary files
        """
        self.config = config or ConfigLoader()

        self.parquet_root = parquet_root or Path(
            self.config.get('storage.parquet_root', 'data/parquet')
        )

        self.qlib_root = qlib_root or Path(
            self.config.get('transform.qlib_root', 'data/qlib')
        )

        # DuckDB for parquet queries
        self.conn = duckdb.connect(':memory:')

        # Data types to check
        self.data_types = ['stocks_daily', 'stocks_minute', 'options_daily']

        logger.info(f"DataIntegrityChecker initialized")
        logger.info(f"  Parquet root: {self.parquet_root}")
        logger.info(f"  Qlib root: {self.qlib_root}")

    def _get_parquet_dates(self, data_type: str) -> Set[str]:
        """
        Get all dates present in parquet files for a data type

        Args:
            data_type: Data type to check

        Returns:
            Set of date strings (YYYY-MM-DD)
        """
        pattern = self.parquet_root / data_type / '**/*.parquet'

        try:
            query = f"""
                SELECT DISTINCT date
                FROM read_parquet('{pattern}')
                WHERE date IS NOT NULL
                ORDER BY date
            """
            result = self.conn.execute(query).fetchdf()

            if result.empty:
                return set()

            # Convert to set of strings
            dates = set(result['date'].astype(str).tolist())
            return dates

        except Exception as e:
            logger.warning(f"Failed to read parquet dates for {data_type}: {e}")
            return set()

    def _get_qlib_dates(self, data_type: str) -> Set[str]:
        """
        Get all dates in qlib binary calendar

        Args:
            data_type: Data type to check

        Returns:
            Set of date strings (YYYY-MM-DD)
        """
        calendar_file = self.qlib_root / data_type / 'calendars' / 'day.txt'

        if not calendar_file.exists():
            return set()

        dates = set()
        with open(calendar_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    dates.add(line)

        return dates

    def _find_date_gaps(self, dates: Set[str]) -> List[tuple]:
        """
        Find gaps in a set of dates

        Args:
            dates: Set of date strings

        Returns:
            List of (start_date, end_date) tuples for each gap
        """
        if not dates:
            return []

        # Convert to sorted list of datetime objects
        date_objs = sorted([datetime.strptime(d, '%Y-%m-%d') for d in dates])

        gaps = []
        for i in range(len(date_objs) - 1):
            current = date_objs[i]
            next_date = date_objs[i + 1]

            # Check if there's a gap (more than 1 day)
            expected_next = current + timedelta(days=1)

            if next_date > expected_next:
                # Found a gap
                gap_start = expected_next.strftime('%Y-%m-%d')
                gap_end = (next_date - timedelta(days=1)).strftime('%Y-%m-%d')
                gaps.append((gap_start, gap_end))

        return gaps

    def check_data_type(self, data_type: str) -> Dict[str, Any]:
        """
        Check integrity for a specific data type

        Args:
            data_type: Data type to check

        Returns:
            Dictionary with check results
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"Checking: {data_type}")
        logger.info(f"{'='*70}")

        # Get dates from parquet
        parquet_dates = self._get_parquet_dates(data_type)
        logger.info(f"  Parquet: {len(parquet_dates)} dates found")

        # Get dates from qlib
        qlib_dates = self._get_qlib_dates(data_type)
        logger.info(f"  Qlib:    {len(qlib_dates)} dates found")

        # Find date ranges
        parquet_range = (
            min(parquet_dates) if parquet_dates else None,
            max(parquet_dates) if parquet_dates else None
        )
        qlib_range = (
            min(qlib_dates) if qlib_dates else None,
            max(qlib_dates) if qlib_dates else None
        )

        if parquet_range[0]:
            logger.info(f"  Parquet range: {parquet_range[0]} to {parquet_range[1]}")

        if qlib_range[0]:
            logger.info(f"  Qlib range:    {qlib_range[0]} to {qlib_range[1]}")

        # Find gaps
        parquet_gaps = self._find_date_gaps(parquet_dates)
        qlib_gaps = self._find_date_gaps(qlib_dates)

        # Find dates in parquet but not in qlib (need conversion)
        missing_in_qlib = parquet_dates - qlib_dates

        # Find dates in qlib but not in parquet (data loss?)
        missing_in_parquet = qlib_dates - parquet_dates

        result = {
            'data_type': data_type,
            'parquet': {
                'total_dates': len(parquet_dates),
                'date_range': parquet_range,
                'gaps': parquet_gaps
            },
            'qlib': {
                'total_dates': len(qlib_dates),
                'date_range': qlib_range,
                'gaps': qlib_gaps
            },
            'missing_in_qlib': sorted(list(missing_in_qlib)),
            'missing_in_parquet': sorted(list(missing_in_parquet))
        }

        # Print summary
        if parquet_gaps:
            logger.warning(f"  ⚠️  Found {len(parquet_gaps)} gaps in parquet data:")
            for start, end in parquet_gaps:
                logger.warning(f"      {start} to {end}")
        else:
            logger.info(f"  ✅ No gaps in parquet data")

        if qlib_gaps:
            logger.warning(f"  ⚠️  Found {len(qlib_gaps)} gaps in qlib data:")
            for start, end in qlib_gaps:
                logger.warning(f"      {start} to {end}")
        else:
            logger.info(f"  ✅ No gaps in qlib data")

        if missing_in_qlib:
            logger.warning(f"  ⚠️  {len(missing_in_qlib)} dates in parquet but not in qlib (need conversion)")
            if len(missing_in_qlib) <= 10:
                logger.warning(f"      {', '.join(sorted(missing_in_qlib))}")
            else:
                logger.warning(f"      {', '.join(sorted(list(missing_in_qlib))[:5])} ... (showing first 5)")

        if missing_in_parquet:
            logger.warning(f"  ⚠️  {len(missing_in_parquet)} dates in qlib but not in parquet (data loss?)")
            if len(missing_in_parquet) <= 10:
                logger.warning(f"      {', '.join(sorted(missing_in_parquet))}")

        return result

    def check_all(self) -> Dict[str, Any]:
        """
        Check integrity for all data types

        Returns:
            Dictionary with results for all data types
        """
        results = {}

        logger.info("\n" + "="*70)
        logger.info("DATA INTEGRITY CHECK")
        logger.info("="*70)

        for data_type in self.data_types:
            results[data_type] = self.check_data_type(data_type)

        return results

    def generate_backfill_commands(
        self,
        results: Dict[str, Any],
        output_file: Optional[Path] = None
    ) -> List[str]:
        """
        Generate backfill commands based on integrity check results

        Args:
            results: Results from check_all()
            output_file: Optional file to write commands to

        Returns:
            List of backfill command strings
        """
        commands = []

        logger.info("\n" + "="*70)
        logger.info("BACKFILL COMMANDS")
        logger.info("="*70)

        for data_type, result in results.items():
            # Generate commands for parquet gaps
            parquet_gaps = result['parquet']['gaps']
            if parquet_gaps:
                logger.info(f"\n{data_type} - Parquet gaps:")
                for start, end in parquet_gaps:
                    cmd = f"# Backfill parquet: {data_type} from {start} to {end}"
                    commands.append(cmd)
                    cmd = f"python -c \"import asyncio; from src.orchestration.ingestion_orchestrator import IngestionOrchestrator; from src.core.config_loader import ConfigLoader; asyncio.run(IngestionOrchestrator(config=ConfigLoader()).ingest_date_range('{data_type}', '{start}', '{end}', use_polars=True))\""
                    commands.append(cmd)
                    logger.info(f"  {start} to {end}")

            # Generate commands for dates in parquet but not in qlib
            missing_in_qlib = result['missing_in_qlib']
            if missing_in_qlib:
                logger.info(f"\n{data_type} - Convert to qlib:")
                # Group consecutive dates
                date_ranges = self._group_consecutive_dates(missing_in_qlib)
                for start, end in date_ranges:
                    cmd = f"# Convert to qlib: {data_type} from {start} to {end}"
                    commands.append(cmd)
                    cmd = f"python -c \"from src.transform.qlib_binary_writer import QlibBinaryWriter; from src.core.config_loader import ConfigLoader; from pathlib import Path; writer = QlibBinaryWriter(enriched_root=Path('data/parquet'), qlib_root=Path('{self.qlib_root}'), config=ConfigLoader()); writer.convert_data_type('{data_type}', '{start}', '{end}', incremental=False)\""
                    commands.append(cmd)
                    logger.info(f"  {start} to {end}")

            # Generate commands for dates in qlib but not in parquet (potential data loss)
            missing_in_parquet = result['missing_in_parquet']
            if missing_in_parquet:
                logger.warning(f"\n{data_type} - Missing in parquet (need to re-download):")
                date_ranges = self._group_consecutive_dates(missing_in_parquet)
                for start, end in date_ranges:
                    cmd = f"# WARNING: Re-download missing parquet data: {data_type} from {start} to {end}"
                    commands.append(cmd)
                    cmd = f"python -c \"import asyncio; from src.orchestration.ingestion_orchestrator import IngestionOrchestrator; from src.core.config_loader import ConfigLoader; asyncio.run(IngestionOrchestrator(config=ConfigLoader()).ingest_date_range('{data_type}', '{start}', '{end}', use_polars=True, incremental=False))\""
                    commands.append(cmd)
                    logger.warning(f"  {start} to {end}")

        # Write to file if requested
        if output_file:
            output_file = Path(output_file)
            with open(output_file, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write("# Data backfill commands\n")
                f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                for cmd in commands:
                    f.write(cmd + "\n")
            logger.info(f"\n✅ Commands written to: {output_file}")

        logger.info(f"\n📋 Generated {len(commands)} commands")

        return commands

    def _group_consecutive_dates(self, dates: List[str]) -> List[tuple]:
        """
        Group consecutive dates into ranges

        Args:
            dates: List of date strings (sorted)

        Returns:
            List of (start_date, end_date) tuples
        """
        if not dates:
            return []

        # Convert to datetime objects
        date_objs = sorted([datetime.strptime(d, '%Y-%m-%d') for d in dates])

        ranges = []
        start = date_objs[0]
        prev = date_objs[0]

        for i in range(1, len(date_objs)):
            current = date_objs[i]

            # Check if consecutive
            if (current - prev).days > 1:
                # End current range
                ranges.append((start.strftime('%Y-%m-%d'), prev.strftime('%Y-%m-%d')))
                start = current

            prev = current

        # Add final range
        ranges.append((start.strftime('%Y-%m-%d'), prev.strftime('%Y-%m-%d')))

        return ranges

    async def run_backfill_async(
        self,
        results: Dict[str, Any],
        parquet_only: bool = False,
        qlib_only: bool = False
    ) -> Dict[str, Any]:
        """
        Run backfill operations directly

        Args:
            results: Results from check_all()
            parquet_only: Only backfill parquet data
            qlib_only: Only backfill qlib data

        Returns:
            Dictionary with backfill results
        """
        logger.info("\n" + "="*70)
        logger.info("RUNNING BACKFILL")
        logger.info("="*70)

        orchestrator = IngestionOrchestrator(config=self.config)
        writer = QlibBinaryWriter(
            enriched_root=self.parquet_root,
            qlib_root=self.qlib_root,
            config=self.config
        )

        backfill_results = defaultdict(dict)

        for data_type, result in results.items():
            logger.info(f"\n{data_type}:")

            # Backfill parquet gaps
            if not qlib_only:
                parquet_gaps = result['parquet']['gaps']
                if parquet_gaps:
                    logger.info(f"  Backfilling {len(parquet_gaps)} parquet gaps...")
                    for start, end in parquet_gaps:
                        try:
                            logger.info(f"    Downloading {start} to {end}...")
                            res = await orchestrator.ingest_date_range(
                                data_type=data_type,
                                start_date=start,
                                end_date=end,
                                use_polars=True,
                                incremental=False
                            )
                            backfill_results[data_type][f'parquet_{start}_{end}'] = res
                            logger.info(f"    ✅ Downloaded {res.get('total_files', 0)} files")
                        except Exception as e:
                            logger.error(f"    ❌ Failed: {e}")
                            backfill_results[data_type][f'parquet_{start}_{end}'] = {'error': str(e)}

            # Convert missing dates to qlib
            if not parquet_only:
                missing_in_qlib = result['missing_in_qlib']
                if missing_in_qlib:
                    date_ranges = self._group_consecutive_dates(missing_in_qlib)
                    logger.info(f"  Converting {len(date_ranges)} date ranges to qlib...")
                    for start, end in date_ranges:
                        try:
                            logger.info(f"    Converting {start} to {end}...")
                            res = writer.convert_data_type(
                                data_type=data_type,
                                start_date=start,
                                end_date=end,
                                incremental=False
                            )
                            backfill_results[data_type][f'qlib_{start}_{end}'] = res
                            logger.info(f"    ✅ Converted {res.get('symbols_converted', 0)} symbols")
                        except Exception as e:
                            logger.error(f"    ❌ Failed: {e}")
                            backfill_results[data_type][f'qlib_{start}_{end}'] = {'error': str(e)}

        logger.info("\n✅ Backfill complete")
        return dict(backfill_results)

    def run_backfill(
        self,
        results: Dict[str, Any],
        parquet_only: bool = False,
        qlib_only: bool = False
    ) -> Dict[str, Any]:
        """
        Synchronous wrapper for run_backfill_async

        Args:
            results: Results from check_all()
            parquet_only: Only backfill parquet data
            qlib_only: Only backfill qlib data

        Returns:
            Dictionary with backfill results
        """
        return asyncio.run(self.run_backfill_async(results, parquet_only, qlib_only))


def main():
    """CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Check data integrity and generate backfill commands'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Only check integrity, do not generate backfill commands'
    )
    parser.add_argument(
        '--run-backfill',
        action='store_true',
        help='Run backfill directly instead of generating commands'
    )
    parser.add_argument(
        '--parquet-only',
        action='store_true',
        help='Only backfill parquet data'
    )
    parser.add_argument(
        '--qlib-only',
        action='store_true',
        help='Only backfill qlib data'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file for backfill commands (default: backfill_commands.sh)'
    )
    parser.add_argument(
        '--parquet-root',
        type=str,
        help='Parquet root directory'
    )
    parser.add_argument(
        '--qlib-root',
        type=str,
        help='Qlib root directory'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Initialize checker
    checker = DataIntegrityChecker(
        parquet_root=Path(args.parquet_root) if args.parquet_root else None,
        qlib_root=Path(args.qlib_root) if args.qlib_root else None
    )

    # Run integrity check
    results = checker.check_all()

    if args.check_only:
        logger.info("\n✅ Integrity check complete (check-only mode)")
        return

    if args.run_backfill:
        # Run backfill directly
        backfill_results = checker.run_backfill(
            results,
            parquet_only=args.parquet_only,
            qlib_only=args.qlib_only
        )
        logger.info("\n✅ Backfill complete")
    else:
        # Generate backfill commands
        output_file = args.output or 'backfill_commands.sh'
        commands = checker.generate_backfill_commands(results, output_file=Path(output_file))

        if commands:
            logger.info(f"\n📝 Run the generated commands:")
            logger.info(f"   bash {output_file}")
        else:
            logger.info("\n✅ No backfill needed - data is complete!")


if __name__ == '__main__':
    main()
