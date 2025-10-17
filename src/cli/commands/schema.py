"""Schema validation and management commands."""

import click
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

from src.core import ConfigLoader


@click.group()
def schema():
    """Schema validation and diagnostics."""
    pass


@schema.command()
@click.option('--data-root', type=click.Path(exists=True),
              help='Data root directory (defaults to configured data_root)')
def validate(data_root):
    """Validate production schema consistency across all datasets."""

    config = ConfigLoader()

    if data_root:
        data_root_path = Path(data_root)
    else:
        data_root_path = config.get_data_root()

    # Import the validation logic from the script
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

    from scripts.validate_production_schemas import ProductionSchemaValidator

    try:
        validator = ProductionSchemaValidator(data_root_path)
        exit_code = validator.validate_all()

        if exit_code != 0:
            click.echo("\n⚠️  Some datasets have inconsistent schemas!")
            click.echo("   Run 'quantmini schema diagnose --data-type <type>' to see details")
            click.echo("   Run 'quantmini schema fix --data-type <type>' to fix inconsistencies")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@schema.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Data type to diagnose')
@click.option('--dataset',
              type=click.Choice(['parquet', 'enriched', 'both']),
              default='both',
              help='Dataset to diagnose (default: both)')
@click.option('--data-root', type=click.Path(exists=True),
              help='Data root directory (defaults to configured data_root)')
def diagnose(data_type, dataset, data_root):
    """Diagnose schema inconsistencies for a specific data type."""

    config = ConfigLoader()

    if data_root:
        data_root_path = Path(data_root)
    else:
        data_root_path = config.get_data_root()

    # Import the diagnostic logic from the script
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

    from scripts.diagnose_schema_issues import diagnose_data_type

    try:
        datasets = ['parquet', 'enriched'] if dataset == 'both' else [dataset]

        for ds in datasets:
            diagnose_data_type(data_root_path, data_type, ds)

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()


@schema.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute', 'all']),
              required=True,
              help='Data type to fix')
@click.option('--start-date', '-s', help='Start date (YYYY-MM-DD, defaults to 2020-10-16)')
@click.option('--end-date', '-e', help='End date (YYYY-MM-DD, defaults to today)')
@click.option('--data-root', type=click.Path(exists=True),
              help='Data root directory (defaults to configured data_root)')
@click.option('--dry-run', is_flag=True, help='Show what would be done without actually doing it')
def fix(data_type, start_date, end_date, data_root, dry_run):
    """Fix schema inconsistencies by re-ingesting data with correct schema."""

    config = ConfigLoader()

    if data_root:
        data_root_path = Path(data_root)
    else:
        data_root_path = config.get_data_root()

    # Default dates
    if not start_date:
        start_date = '2020-10-16'
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    click.echo(f"🔧 Fixing schema inconsistencies for: {data_type}")
    click.echo(f"   Date range: {start_date} to {end_date}")
    click.echo(f"   Data root: {data_root_path}")

    if dry_run:
        click.echo(f"   Mode: DRY RUN (no changes will be made)\n")
    else:
        click.echo()
        click.confirm('This will re-ingest data with the correct schema. Continue?', abort=True)

    # Import re-ingestion logic
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

    from scripts.reingest_production import reingest_date_range

    async def fix_schema():
        if data_type == 'all':
            data_types = ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']
        else:
            data_types = [data_type]

        for dt in data_types:
            if not dry_run:
                click.echo(f"\n🔄 Re-ingesting {dt}...")
                await reingest_date_range(dt, start_date, end_date, data_root_path)
            else:
                click.echo(f"\n[DRY RUN] Would re-ingest {dt} from {start_date} to {end_date}")

        if not dry_run:
            click.echo("\n✅ Schema fix complete!")
            click.echo("   Run 'quantmini schema validate' to verify")
        else:
            click.echo("\n[DRY RUN] Complete - no changes made")

    asyncio.run(fix_schema())


@schema.command()
@click.option('--data-root', type=click.Path(exists=True),
              help='Data root directory (defaults to configured data_root)')
def verify_qlib(data_root):
    """Verify Qlib binary format compatibility and data integrity."""

    config = ConfigLoader()

    if data_root:
        data_root_path = Path(data_root)
    else:
        data_root_path = config.get_data_root()

    # Import verification logic
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

    from scripts.verify_qlib_conversion import verify_qlib_data

    try:
        # Set config data root for the script
        original_data_root = config.get_data_root()
        config.config['data_root'] = str(data_root_path)

        success = verify_qlib_data()

        # Restore original
        config.config['data_root'] = str(original_data_root)

        if not success:
            raise click.Abort()

    except Exception as e:
        click.echo(f"\n❌ Verification failed: {e}", err=True)
        import traceback
        traceback.print_exc()
        raise click.Abort()
