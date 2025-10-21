"""Data transformation commands for silver layer generation."""

import click
import sys
from pathlib import Path
from datetime import datetime
import logging

import polars as pl

# Import centralized path utilities
from src.utils.paths import get_quantlake_root
from src.storage.metadata_manager import MetadataManager
from src.core.config_loader import ConfigLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def transform():
    """Bronze to silver layer transformations."""
    pass


@transform.command('financial-ratios')
@click.option('--bronze-dir', '-b',
              type=click.Path(exists=True),
              default=None,
              help='Bronze layer financial ratios directory (default: $QUANTLAKE_ROOT/fundamentals/financial_ratios)')
@click.option('--silver-dir', '-s',
              type=click.Path(),
              default=None,
              help='Silver layer output directory (default: $QUANTLAKE_ROOT/silver/financial_ratios)')
def financial_ratios(bronze_dir, silver_dir):
    """Move financial ratios from bronze to silver layer."""

    # Use environment variable defaults if not specified
    quantlake_root = get_quantlake_root()
    bronze_path = Path(bronze_dir) if bronze_dir else quantlake_root / 'fundamentals' / 'financial_ratios'
    silver_path = Path(silver_dir) if silver_dir else quantlake_root / 'silver' / 'financial_ratios'

    click.echo("="*80)
    click.echo("MOVING FINANCIAL RATIOS TO SILVER LAYER")
    click.echo("="*80)
    click.echo(f"Bronze path: {bronze_path}")
    click.echo(f"Silver path: {silver_path}")
    click.echo("")

    # Find all parquet files
    all_files = list(bronze_path.rglob("*.parquet"))
    click.echo(f"Found {len(all_files):,} files")

    # Load all files
    click.echo("Loading and consolidating files...")
    dfs = []
    for file_path in all_files:
        try:
            df = pl.read_parquet(file_path)
            dfs.append(df)
        except Exception as e:
            click.echo(f"Warning: Failed to read {file_path}: {e}", err=True)
            continue

    # Combine all data (using vertical_relaxed to handle schema differences)
    click.echo(f"Combining {len(dfs)} dataframes...")
    # Collect all unique columns across all dataframes
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)

    # Ensure all dataframes have the same columns (fill missing with nulls)
    aligned_dfs = []
    for df in dfs:
        missing_cols = all_columns - set(df.columns)
        for col in missing_cols:
            df = df.with_columns(pl.lit(None).alias(col))
        aligned_dfs.append(df.select(sorted(all_columns)))

    combined_df = pl.concat(aligned_dfs, how="vertical_relaxed")

    click.echo(f"Total records: {len(combined_df):,}")
    click.echo(f"Total columns: {len(combined_df.columns)}")
    click.echo(f"Unique tickers: {combined_df['ticker'].n_unique()}")

    # Add processed_at timestamp
    combined_df = combined_df.with_columns(
        pl.lit(datetime.now()).alias('processed_at')
    )

    # Save to silver layer partitioned by fiscal_year and fiscal_period
    click.echo("")
    click.echo("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    for (year, quarter), group_df in combined_df.group_by(['fiscal_year', 'fiscal_period']):
        partition_dir = silver_path / f"year={year}" / f"quarter={quarter}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"
        group_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3
        )

        click.echo(f"  Saved: year={year}, quarter={quarter} ({len(group_df):,} records)")

    click.echo("")
    click.echo("✓ Financial ratios moved to silver layer")
    click.echo(f"  Location: {silver_path}")
    click.echo(f"  Total records: {len(combined_df):,}")
    click.echo(f"  Total columns: {len(combined_df.columns)}")
    click.echo(f"  Partitioning: fiscal_year / fiscal_period")
    click.echo("")

    # Record metadata for silver layer
    try:
        config = ConfigLoader()
        metadata_root = config.get_metadata_path()
        metadata_manager = MetadataManager(metadata_root)

        # Get date range from the combined data
        if 'filing_date' in combined_df.columns:
            min_date = str(combined_df['filing_date'].min())
            max_date = str(combined_df['filing_date'].max())
        else:
            max_date = datetime.now().strftime('%Y-%m-%d')
            min_date = max_date

        # Record metadata
        metadata_manager.record_ingestion(
            data_type='financial_ratios',
            date=max_date,
            status='success',
            statistics={
                'records': len(combined_df),
                'tickers': combined_df['ticker'].n_unique(),
                'columns': len(combined_df.columns),
                'min_filing_date': min_date,
                'max_filing_date': max_date,
            },
            layer='silver'
        )

        # Update watermark
        metadata_manager.set_watermark(
            data_type='financial_ratios',
            date=max_date,
            layer='silver'
        )

        click.echo("✓ Metadata recorded for silver layer")

    except Exception as e:
        click.echo(f"Warning: Failed to record metadata: {e}", err=True)


@transform.command('corporate-actions')
@click.option('--bronze-dir', '-b',
              type=click.Path(exists=True),
              default=None,
              help='Bronze layer corporate actions directory (default: $QUANTLAKE_ROOT/bronze/corporate_actions)')
@click.option('--silver-dir', '-s',
              type=click.Path(),
              default=None,
              help='Silver layer output directory (default: $QUANTLAKE_ROOT/silver/ticker_events)')
def corporate_actions(bronze_dir, silver_dir):
    """Consolidate corporate actions (dividends, splits, IPOs) to silver layer."""

    # Use environment variable defaults if not specified
    quantlake_root = get_quantlake_root()
    bronze_path = Path(bronze_dir) if bronze_dir else quantlake_root / 'bronze' / 'corporate_actions'
    silver_path = Path(silver_dir) if silver_dir else quantlake_root / 'silver' / 'ticker_events'

    click.echo("="*80)
    click.echo("PHASE 3: CORPORATE ACTIONS CONSOLIDATION")
    click.echo("="*80)
    click.echo(f"Bronze path: {bronze_path}")
    click.echo(f"Silver path: {silver_path}")
    click.echo("")

    # Process dividends
    def process_dividends():
        click.echo("Processing DIVIDENDS...")
        dividends_path = bronze_path / "dividends"

        if not dividends_path.exists():
            click.echo(f"Warning: Dividends path not found: {dividends_path}", err=True)
            return None

        all_files = list(dividends_path.rglob("*.parquet"))
        click.echo(f"  Found {len(all_files):,} dividend files")

        dfs = []
        for file_path in all_files:
            try:
                df = pl.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                click.echo(f"Warning: Failed to read {file_path}: {e}", err=True)
                continue

        if not dfs:
            return None

        combined_df = pl.concat(dfs, how="vertical_relaxed")

        unified_df = combined_df.select([
            pl.col('ticker'),
            pl.lit('dividend').alias('action_type'),
            pl.col('ex_dividend_date').str.to_date().alias('event_date'),
            pl.col('id'),
            pl.col('downloaded_at'),
            pl.col('cash_amount').alias('div_cash_amount'),
            pl.col('currency').alias('div_currency'),
            pl.col('declaration_date').str.to_date().alias('div_declaration_date'),
            pl.col('dividend_type').alias('div_dividend_type'),
            pl.col('ex_dividend_date').str.to_date().alias('div_ex_dividend_date'),
            pl.col('frequency').alias('div_frequency'),
            pl.col('pay_date').str.to_date().alias('div_pay_date'),
            pl.col('record_date').str.to_date().alias('div_record_date'),
            pl.lit(None).cast(pl.Date).alias('split_execution_date'),
            pl.lit(None).cast(pl.Float64).alias('split_from'),
            pl.lit(None).cast(pl.Float64).alias('split_to'),
            pl.lit(None).cast(pl.Float64).alias('split_ratio'),
            pl.lit(None).cast(pl.Date).alias('ipo_last_updated'),
            pl.lit(None).cast(pl.Date).alias('ipo_announced_date'),
            pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
            pl.lit(None).cast(pl.String).alias('ipo_issuer_name'),
            pl.lit(None).cast(pl.String).alias('ipo_currency_code'),
            pl.lit(None).cast(pl.String).alias('ipo_us_code'),
            pl.lit(None).cast(pl.String).alias('ipo_isin'),
            pl.lit(None).cast(pl.Float64).alias('ipo_final_issue_price'),
            pl.lit(None).cast(pl.Int64).alias('ipo_max_shares_offered'),
            pl.lit(None).cast(pl.Float64).alias('ipo_lowest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_highest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_total_offer_size'),
            pl.lit(None).cast(pl.String).alias('ipo_primary_exchange'),
            pl.lit(None).cast(pl.Int64).alias('ipo_shares_outstanding'),
            pl.lit(None).cast(pl.String).alias('ipo_security_type'),
            pl.lit(None).cast(pl.Int64).alias('ipo_lot_size'),
            pl.lit(None).cast(pl.String).alias('ipo_security_description'),
            pl.lit(None).cast(pl.String).alias('ipo_status'),
            # Ticker event specific fields (null for dividends)
            pl.lit(None).cast(pl.String).alias('new_ticker'),
            pl.lit(None).cast(pl.String).alias('event_type'),
        ])

        click.echo(f"  Processed {len(unified_df):,} dividend records")
        return unified_df

    # Process splits
    def process_splits():
        click.echo("Processing SPLITS...")
        splits_path = bronze_path / "splits"

        if not splits_path.exists():
            click.echo(f"Warning: Splits path not found: {splits_path}", err=True)
            return None

        all_files = list(splits_path.rglob("*.parquet"))
        click.echo(f"  Found {len(all_files):,} split files")

        dfs = []
        for file_path in all_files:
            try:
                df = pl.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                click.echo(f"Warning: Failed to read {file_path}: {e}", err=True)
                continue

        if not dfs:
            return None

        combined_df = pl.concat(dfs, how="vertical_relaxed")

        unified_df = combined_df.select([
            pl.col('ticker'),
            pl.lit('split').alias('action_type'),
            pl.col('execution_date').str.to_date().alias('event_date'),
            pl.col('id'),
            pl.col('downloaded_at'),
            pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
            pl.lit(None).cast(pl.String).alias('div_currency'),
            pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
            pl.lit(None).cast(pl.String).alias('div_dividend_type'),
            pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
            pl.lit(None).cast(pl.Int64).alias('div_frequency'),
            pl.lit(None).cast(pl.Date).alias('div_pay_date'),
            pl.lit(None).cast(pl.Date).alias('div_record_date'),
            pl.col('execution_date').str.to_date().alias('split_execution_date'),
            pl.col('split_from').alias('split_from'),
            pl.col('split_to').alias('split_to'),
            (pl.col('split_to') / pl.col('split_from')).alias('split_ratio'),
            pl.lit(None).cast(pl.Date).alias('ipo_last_updated'),
            pl.lit(None).cast(pl.Date).alias('ipo_announced_date'),
            pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
            pl.lit(None).cast(pl.String).alias('ipo_issuer_name'),
            pl.lit(None).cast(pl.String).alias('ipo_currency_code'),
            pl.lit(None).cast(pl.String).alias('ipo_us_code'),
            pl.lit(None).cast(pl.String).alias('ipo_isin'),
            pl.lit(None).cast(pl.Float64).alias('ipo_final_issue_price'),
            pl.lit(None).cast(pl.Int64).alias('ipo_max_shares_offered'),
            pl.lit(None).cast(pl.Float64).alias('ipo_lowest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_highest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_total_offer_size'),
            pl.lit(None).cast(pl.String).alias('ipo_primary_exchange'),
            pl.lit(None).cast(pl.Int64).alias('ipo_shares_outstanding'),
            pl.lit(None).cast(pl.String).alias('ipo_security_type'),
            pl.lit(None).cast(pl.Int64).alias('ipo_lot_size'),
            pl.lit(None).cast(pl.String).alias('ipo_security_description'),
            pl.lit(None).cast(pl.String).alias('ipo_status'),
            # Ticker event specific fields (null for splits)
            pl.lit(None).cast(pl.String).alias('new_ticker'),
            pl.lit(None).cast(pl.String).alias('event_type'),
        ])

        click.echo(f"  Processed {len(unified_df):,} split records")
        return unified_df

    # Process IPOs
    def process_ipos():
        click.echo("Processing IPOS...")
        ipos_path = bronze_path / "ipos"

        if not ipos_path.exists():
            click.echo(f"Warning: IPOs path not found: {ipos_path}", err=True)
            return None

        all_files = list(ipos_path.rglob("*.parquet"))
        click.echo(f"  Found {len(all_files):,} IPO files")

        dfs = []
        for file_path in all_files:
            try:
                df = pl.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                click.echo(f"Warning: Failed to read {file_path}: {e}", err=True)
                continue

        if not dfs:
            return None

        combined_df = pl.concat(dfs, how="vertical_relaxed")

        # Generate ID if not present
        if 'id' not in combined_df.columns:
            combined_df = combined_df.with_columns(
                (pl.col('ticker') + '_' + pl.col('listing_date')).alias('id')
            )

        unified_df = combined_df.select([
            pl.col('ticker'),
            pl.lit('ipo').alias('action_type'),
            pl.col('listing_date').str.to_date().alias('event_date'),
            pl.col('id'),
            pl.col('downloaded_at'),
            pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
            pl.lit(None).cast(pl.String).alias('div_currency'),
            pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
            pl.lit(None).cast(pl.String).alias('div_dividend_type'),
            pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
            pl.lit(None).cast(pl.Int64).alias('div_frequency'),
            pl.lit(None).cast(pl.Date).alias('div_pay_date'),
            pl.lit(None).cast(pl.Date).alias('div_record_date'),
            pl.lit(None).cast(pl.Date).alias('split_execution_date'),
            pl.lit(None).cast(pl.Float64).alias('split_from'),
            pl.lit(None).cast(pl.Float64).alias('split_to'),
            pl.lit(None).cast(pl.Float64).alias('split_ratio'),
            pl.col('last_updated').str.to_date().alias('ipo_last_updated'),
            pl.col('announced_date').str.to_date().alias('ipo_announced_date'),
            pl.col('listing_date').str.to_date().alias('ipo_listing_date'),
            pl.col('issuer_name').alias('ipo_issuer_name'),
            pl.col('currency_code').alias('ipo_currency_code'),
            pl.col('us_code').alias('ipo_us_code'),
            pl.col('isin').alias('ipo_isin'),
            pl.col('final_issue_price').alias('ipo_final_issue_price'),
            pl.col('max_shares_offered').alias('ipo_max_shares_offered'),
            pl.col('lowest_offer_price').alias('ipo_lowest_offer_price'),
            pl.col('highest_offer_price').alias('ipo_highest_offer_price'),
            pl.col('total_offer_size').alias('ipo_total_offer_size'),
            pl.col('primary_exchange').alias('ipo_primary_exchange'),
            pl.col('shares_outstanding').alias('ipo_shares_outstanding'),
            pl.col('security_type').alias('ipo_security_type'),
            pl.col('lot_size').alias('ipo_lot_size'),
            pl.col('security_description').alias('ipo_security_description'),
            pl.col('ipo_status').alias('ipo_status'),
            # Ticker event specific fields (null for IPOs)
            pl.lit(None).cast(pl.String).alias('new_ticker'),
            pl.lit(None).cast(pl.String).alias('event_type'),
        ])

        click.echo(f"  Processed {len(unified_df):,} IPO records")
        return unified_df

    # Process ticker events (symbol changes)
    def process_ticker_events():
        click.echo("Processing TICKER EVENTS...")
        ticker_events_path = bronze_path / "ticker_events"

        if not ticker_events_path.exists():
            click.echo(f"Warning: Ticker events path not found: {ticker_events_path}", err=True)
            return None

        all_files = list(ticker_events_path.rglob("*.parquet"))
        click.echo(f"  Found {len(all_files):,} ticker event files")

        if not all_files:
            return None

        dfs = []
        for file_path in all_files:
            try:
                df = pl.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                click.echo(f"Warning: Failed to read {file_path}: {e}", err=True)
                continue

        if not dfs:
            return None

        combined_df = pl.concat(dfs, how="vertical_relaxed")

        # Generate ID if not present
        if 'id' not in combined_df.columns:
            combined_df = combined_df.with_columns(
                (pl.col('ticker') + '_' + pl.col('date')).alias('id')
            )

        # Create unified schema matching other action types
        unified_df = combined_df.select([
            pl.col('ticker'),
            pl.lit('ticker_change').alias('action_type'),
            pl.col('date').str.to_date().alias('event_date'),
            pl.col('id'),
            pl.col('downloaded_at'),
            pl.lit(None).cast(pl.Float64).alias('div_cash_amount'),
            pl.lit(None).cast(pl.String).alias('div_currency'),
            pl.lit(None).cast(pl.Date).alias('div_declaration_date'),
            pl.lit(None).cast(pl.String).alias('div_dividend_type'),
            pl.lit(None).cast(pl.Date).alias('div_ex_dividend_date'),
            pl.lit(None).cast(pl.Int64).alias('div_frequency'),
            pl.lit(None).cast(pl.Date).alias('div_pay_date'),
            pl.lit(None).cast(pl.Date).alias('div_record_date'),
            pl.lit(None).cast(pl.Date).alias('split_execution_date'),
            pl.lit(None).cast(pl.Float64).alias('split_from'),
            pl.lit(None).cast(pl.Float64).alias('split_to'),
            pl.lit(None).cast(pl.Float64).alias('split_ratio'),
            pl.lit(None).cast(pl.Date).alias('ipo_last_updated'),
            pl.lit(None).cast(pl.Date).alias('ipo_announced_date'),
            pl.lit(None).cast(pl.Date).alias('ipo_listing_date'),
            pl.lit(None).cast(pl.String).alias('ipo_issuer_name'),
            pl.lit(None).cast(pl.String).alias('ipo_currency_code'),
            pl.lit(None).cast(pl.String).alias('ipo_us_code'),
            pl.lit(None).cast(pl.String).alias('ipo_isin'),
            pl.lit(None).cast(pl.Float64).alias('ipo_final_issue_price'),
            pl.lit(None).cast(pl.Int64).alias('ipo_max_shares_offered'),
            pl.lit(None).cast(pl.Float64).alias('ipo_lowest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_highest_offer_price'),
            pl.lit(None).cast(pl.Float64).alias('ipo_total_offer_size'),
            pl.lit(None).cast(pl.String).alias('ipo_primary_exchange'),
            pl.lit(None).cast(pl.Int64).alias('ipo_shares_outstanding'),
            pl.lit(None).cast(pl.String).alias('ipo_security_type'),
            pl.lit(None).cast(pl.Int64).alias('ipo_lot_size'),
            pl.lit(None).cast(pl.String).alias('ipo_security_description'),
            pl.lit(None).cast(pl.String).alias('ipo_status'),
            # Ticker event specific fields
            pl.col('new_ticker') if 'new_ticker' in combined_df.columns else pl.lit(None).cast(pl.String).alias('new_ticker'),
            pl.col('event_type') if 'event_type' in combined_df.columns else pl.lit(None).cast(pl.String).alias('event_type'),
        ])

        click.echo(f"  Processed {len(unified_df):,} ticker event records")
        return unified_df

    # Process each corporate action type
    dividends_df = process_dividends()
    splits_df = process_splits()
    ipos_df = process_ipos()
    ticker_events_df = process_ticker_events()

    # Combine all corporate actions
    click.echo("")
    click.echo("Combining all corporate actions...")

    all_dfs = []
    if dividends_df is not None:
        all_dfs.append(dividends_df)
    if splits_df is not None:
        all_dfs.append(splits_df)
    if ipos_df is not None:
        all_dfs.append(ipos_df)
    if ticker_events_df is not None:
        all_dfs.append(ticker_events_df)

    if not all_dfs:
        click.echo("Error: No corporate actions found!", err=True)
        return

    combined_df = pl.concat(all_dfs, how="vertical_relaxed")

    # Add metadata columns
    combined_df = combined_df.with_columns([
        pl.lit(datetime.now()).alias('processed_at'),
        pl.col('event_date').dt.year().alias('year'),
        pl.col('event_date').dt.month().alias('month'),
    ])

    # Summary statistics
    click.echo(f"Total records: {len(combined_df):,}")
    click.echo(f"Total columns: {len(combined_df.columns)}")
    click.echo("")
    click.echo("Records by action type:")
    for action_type, count in combined_df.group_by('action_type').agg(pl.len()).iter_rows():
        click.echo(f"  {action_type}: {count:,}")

    click.echo("")
    click.echo(f"Unique tickers: {combined_df['ticker'].n_unique()}")
    click.echo(f"Date range: {combined_df['event_date'].min()} to {combined_df['event_date'].max()}")

    # Save to silver layer partitioned by year and month
    click.echo("")
    click.echo("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    for (year, month), group_df in combined_df.group_by(['year', 'month']):
        partition_dir = silver_path / f"year={year}" / f"month={month:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"
        group_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3,
            use_pyarrow_extension_array=False  # Disable dictionary encoding to prevent schema conflicts
        )

        click.echo(f"  Saved: year={year}, month={month:02d} ({len(group_df):,} records)")

    click.echo("")
    click.echo("✓ Corporate actions consolidated to silver layer")
    click.echo(f"  Location: {silver_path}")
    click.echo(f"  Total records: {len(combined_df):,}")
    click.echo(f"  Total columns: {len(combined_df.columns)}")
    click.echo(f"  Partitioning: year / month")
    click.echo(f"  Action types: {', '.join(combined_df['action_type'].unique().sort())}")
    click.echo("")


@transform.command('fundamentals')
@click.option('--bronze-dir', '-b',
              type=click.Path(exists=True),
              default=None,
              help='Bronze layer fundamentals directory (default: $QUANTLAKE_ROOT/fundamentals)')
@click.option('--silver-dir', '-s',
              type=click.Path(),
              default=None,
              help='Silver layer output directory (default: $QUANTLAKE_ROOT/silver/fundamentals_wide)')
@click.option('--tickers', '-t',
              multiple=True,
              help='Tickers to process (if not specified, processes all)')
def fundamentals(bronze_dir, silver_dir, tickers):
    """Flatten fundamentals (balance sheets, income statements, cash flow) to wide format."""

    # Use environment variable defaults if not specified
    quantlake_root = get_quantlake_root()
    bronze_path = Path(bronze_dir) if bronze_dir else quantlake_root / 'fundamentals'
    silver_path = Path(silver_dir) if silver_dir else quantlake_root / 'silver' / 'fundamentals_wide'

    click.echo("="*80)
    click.echo("FLATTENING FUNDAMENTALS TO SILVER LAYER")
    click.echo("="*80)
    click.echo(f"Bronze path: {bronze_path}")
    click.echo(f"Silver path: {silver_path}")
    click.echo("")

    # Find all tickers if not specified
    if not tickers:
        balance_sheet_dir = bronze_path / 'balance_sheets'
        if balance_sheet_dir.exists():
            ticker_files = list(balance_sheet_dir.rglob("ticker=*.parquet"))
            tickers = list(set([f.stem.replace('ticker=', '') for f in ticker_files]))
            click.echo(f"Found {len(tickers)} tickers to process")
        else:
            click.echo("Error: Balance sheets directory not found!", err=True)
            return
    else:
        click.echo(f"Processing {len(tickers)} specified tickers")

    # Process each ticker
    all_wide_dfs = []

    for ticker in tickers:
        try:
            # Load balance sheets
            bs_files = list(bronze_path.glob(f'balance_sheets/**/ticker={ticker}.parquet'))
            if not bs_files:
                click.echo(f"  Skipping {ticker}: No balance sheet data", err=True)
                continue

            bs_df = pl.read_parquet(bs_files[0]) if len(bs_files) == 1 else pl.concat([pl.read_parquet(f) for f in bs_files])

            # Extract ticker from tickers array (Polygon returns a list)
            if 'tickers' in bs_df.columns:
                bs_df = bs_df.with_columns(
                    pl.col('tickers').list.first().alias('ticker')
                ).drop('tickers')

            # Load income statements
            is_files = list(bronze_path.glob(f'income_statements/**/ticker={ticker}.parquet'))
            is_df = pl.read_parquet(is_files[0]) if is_files and len(is_files) == 1 else (pl.concat([pl.read_parquet(f) for f in is_files]) if is_files else None)

            # Extract ticker from tickers array
            if is_df is not None and 'tickers' in is_df.columns:
                is_df = is_df.with_columns(
                    pl.col('tickers').list.first().alias('ticker')
                ).drop('tickers')

            # Load cash flow
            cf_files = list(bronze_path.glob(f'cash_flow/**/ticker={ticker}.parquet'))
            cf_df = pl.read_parquet(cf_files[0]) if cf_files and len(cf_files) == 1 else (pl.concat([pl.read_parquet(f) for f in cf_files]) if cf_files else None)

            # Extract ticker from tickers array
            if cf_df is not None and 'tickers' in cf_df.columns:
                cf_df = cf_df.with_columns(
                    pl.col('tickers').list.first().alias('ticker')
                ).drop('tickers')

            # Rename columns with prefixes
            bs_df = bs_df.rename({col: f'bs_{col}' for col in bs_df.columns if col not in ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period', 'fiscal_quarter']})

            if is_df is not None:
                is_df = is_df.rename({col: f'is_{col}' for col in is_df.columns if col not in ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period', 'fiscal_quarter']})

            if cf_df is not None:
                cf_df = cf_df.rename({col: f'cf_{col}' for col in cf_df.columns if col not in ['ticker', 'filing_date', 'fiscal_year', 'fiscal_period', 'fiscal_quarter']})

            # Merge on common keys
            wide_df = bs_df

            if is_df is not None:
                wide_df = wide_df.join(
                    is_df,
                    on=['ticker', 'filing_date', 'fiscal_year', 'fiscal_period'],
                    how='outer_coalesce'
                )

            if cf_df is not None:
                wide_df = wide_df.join(
                    cf_df,
                    on=['ticker', 'filing_date', 'fiscal_year', 'fiscal_period'],
                    how='outer_coalesce'
                )

            all_wide_dfs.append(wide_df)
            click.echo(f"  Processed {ticker}: {len(wide_df)} quarters, {len(wide_df.columns)} columns")

        except Exception as e:
            click.echo(f"  Error processing {ticker}: {e}", err=True)
            continue

    if not all_wide_dfs:
        click.echo("Error: No fundamentals data processed!", err=True)
        return

    # Combine all tickers
    click.echo("")
    click.echo("Combining all tickers...")
    combined_df = pl.concat(all_wide_dfs, how="diagonal_relaxed")

    # Add processed_at timestamp
    combined_df = combined_df.with_columns(
        pl.lit(datetime.now()).alias('processed_at')
    )

    click.echo(f"Total records: {len(combined_df):,}")
    click.echo(f"Total columns: {len(combined_df.columns)}")
    click.echo(f"Unique tickers: {combined_df['ticker'].n_unique()}")

    # Save to silver layer partitioned by fiscal_year and fiscal_period
    click.echo("")
    click.echo("Saving to silver layer...")
    silver_path.mkdir(parents=True, exist_ok=True)

    for (year, quarter), group_df in combined_df.group_by(['fiscal_year', 'fiscal_period']):
        partition_dir = silver_path / f"year={year}" / f"quarter={quarter}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        output_file = partition_dir / "data.parquet"
        group_df.write_parquet(
            output_file,
            compression='zstd',
            compression_level=3
        )

        click.echo(f"  Saved: year={year}, quarter={quarter} ({len(group_df):,} records)")

    click.echo("")
    click.echo("✓ Fundamentals flattened to silver layer")
    click.echo(f"  Location: {silver_path}")
    click.echo(f"  Total records: {len(combined_df):,}")
    click.echo(f"  Total columns: {len(combined_df.columns)}")
    click.echo(f"  Partitioning: fiscal_year / fiscal_period")
    click.echo("")

    # Record metadata for silver layer
    try:
        config = ConfigLoader()
        metadata_root = config.get_metadata_path()
        metadata_manager = MetadataManager(metadata_root)

        # Get date range from the combined data
        if 'filing_date' in combined_df.columns:
            min_date = str(combined_df['filing_date'].min())
            max_date = str(combined_df['filing_date'].max())
        else:
            max_date = datetime.now().strftime('%Y-%m-%d')
            min_date = max_date

        # Record metadata
        metadata_manager.record_ingestion(
            data_type='fundamentals',
            date=max_date,
            status='success',
            statistics={
                'records': len(combined_df),
                'tickers': combined_df['ticker'].n_unique(),
                'columns': len(combined_df.columns),
                'min_filing_date': min_date,
                'max_filing_date': max_date,
            },
            layer='silver'
        )

        # Update watermark
        metadata_manager.set_watermark(
            data_type='fundamentals',
            date=max_date,
            layer='silver'
        )

        click.echo("✓ Metadata recorded for silver layer")

    except Exception as e:
        click.echo(f"Warning: Failed to record metadata: {e}", err=True)
