"""Data management commands."""

import click
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

from src.core import ConfigLoader
from src.download import AsyncS3Downloader, S3Catalog
from src.ingest import PolarsIngestor, StreamingIngestor
from src.features import FeatureEngineer
from src.transform import QlibBinaryWriter
from src.query import QueryEngine
from src.storage import MetadataManager
from src.utils.market_calendar import get_default_calendar


def validate_date_range(data_type: str, start_date: str, end_date: str) -> None:
    """
    Validate date range and show trading day info for daily data.

    Args:
        data_type: Type of data (e.g., 'stocks_daily')
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
    """
    if 'daily' not in data_type:
        return

    calendar = get_default_calendar()

    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as e:
        click.echo(f"❌ Invalid date format: {e}", err=True)
        raise click.Abort()

    # Get trading days
    trading_days = calendar.get_trading_days(start_dt, end_dt)
    total_days = (end_dt - start_dt).days + 1
    skipped = total_days - len(trading_days)

    if skipped > 0:
        click.echo(f"📅 Calendar Info:")
        click.echo(f"   Total days: {total_days}")
        click.echo(f"   Trading days: {len(trading_days)}")
        click.echo(f"   Weekends/holidays: {skipped} (will be skipped)")
        click.echo("")


@click.group()
def data():
    """Data operations (download, ingest, enrich, convert, query)."""
    pass


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to download')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--output', '-o', type=click.Path(), help='Output directory')
def download(data_type, start_date, end_date, output):
    """Download data from Polygon.io S3."""

    # Validate date range and show calendar info
    validate_date_range(data_type, start_date, end_date)

    config = ConfigLoader()
    polygon_creds = config.get_credentials('polygon')

    if not polygon_creds or 's3' not in polygon_creds:
        click.echo("❌ Error: Polygon S3 credentials not found. Check config/credentials.yaml", err=True)
        return

    credentials = {
        'access_key_id': polygon_creds['s3']['access_key_id'],
        'secret_access_key': polygon_creds['s3']['secret_access_key'],
    }

    catalog = S3Catalog()
    # Use configured landing path if no output specified
    if output:
        output_dir = Path(output)
    else:
        from src.utils.paths import get_quantlake_root
        quantlake_root = get_quantlake_root()
        output_dir = quantlake_root / "landing"
    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"📥 Downloading {data_type} from {start_date} to {end_date}...")

    async def download_data():
        downloader = AsyncS3Downloader(credentials)
        keys = catalog.get_date_range_keys(data_type, start_date, end_date)

        click.echo(f"   Found {len(keys)} files to download")

        with click.progressbar(keys, label='Downloading') as bar:
            for key in bar:
                try:
                    # Extract date from filename
                    date = key.split('/')[-1].replace('.csv.gz', '')

                    # Parse date for partitioning: YYYY-MM-DD
                    year, month, day = date.split('-')

                    # Create partitioned path: landing/{data_type}/year={YYYY}/month={MM}/{YYYY-MM-DD}.csv.gz
                    partition_dir = output_dir / data_type / f"year={year}" / f"month={month}"
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    output_file = partition_dir / f"{date}.csv.gz"
                    await downloader.download_to_file('flatfiles', key, output_file, decompress=False)
                except Exception as e:
                    click.echo(f"\n❌ Failed to download {key}: {e}", err=True)
        
        stats = downloader.get_statistics()
        click.echo(f"\n✅ Downloaded {stats['download_count']} files")
        click.echo(f"   Success rate: {stats['success_rate']:.1%}")
    
    asyncio.run(download_data())


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to ingest')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--mode', '-m',
              type=click.Choice(['polars', 'streaming']),
              default='polars',
              help='Ingestion mode (default: polars)')
@click.option('--incremental/--full', default=True, help='Incremental or full ingestion')
def ingest(data_type, start_date, end_date, mode, incremental):
    """Ingest data into Parquet format."""

    # Validate date range and show calendar info
    validate_date_range(data_type, start_date, end_date)

    from src.orchestration import IngestionOrchestrator

    config = ConfigLoader()

    click.echo(f"📊 Ingesting {data_type} from {start_date} to {end_date}...")
    click.echo(f"   Mode: {mode}, Incremental: {incremental}")
    
    async def run_ingestion():
        orchestrator = IngestionOrchestrator(config=config)
        
        result = await orchestrator.ingest_date_range(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            incremental=incremental,
            use_polars=(mode == 'polars')
        )
        
        if result.get('ingested', 0) > 0:
            click.echo(f"\n✅ Ingested {result['ingested']} files")
            click.echo(f"   Total records: {result.get('records_processed', 0):,}")

        if result.get('failed', 0) > 0:
            click.echo(f"\n❌ Failed files: {result['failed']}")
    
    asyncio.run(run_ingestion())


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to enrich')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--incremental/--full', default=True, help='Incremental or full enrichment')
def enrich(data_type, start_date, end_date, incremental):
    """Add features to ingested data."""

    # Validate date range and show calendar info
    validate_date_range(data_type, start_date, end_date)

    config = ConfigLoader()

    click.echo(f"⚙️  Enriching {data_type} from {start_date} to {end_date}...")

    with FeatureEngineer(
        parquet_root=config.get_bronze_path(),
        enriched_root=config.get_silver_path(),
        config=config
    ) as engineer:
        result = engineer.enrich_date_range(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            incremental=incremental
        )

        click.echo(f"\n✅ Enriched {result['records_enriched']:,} records")
        click.echo(f"   Dates processed: {result['dates_processed']}")
        click.echo(f"   Features added: {result['features_added']}")

        # Record metadata for silver layer (enrichment adds features to create silver layer)
        try:
            metadata_root = config.get_metadata_path()
            metadata_manager = MetadataManager(metadata_root)

            # Record metadata for the enrichment
            metadata_manager.record_ingestion(
                data_type=data_type,
                date=end_date,
                status='success',
                statistics={
                    'records_enriched': result['records_enriched'],
                    'dates_processed': result['dates_processed'],
                    'features_added': result['features_added'],
                    'start_date': start_date,
                    'end_date': end_date,
                },
                layer='silver'
            )

            # Update watermark
            metadata_manager.set_watermark(
                data_type=data_type,
                date=end_date,
                layer='silver'
            )

            click.echo("✓ Metadata recorded for silver layer")

        except Exception as e:
            click.echo(f"Warning: Failed to record metadata: {e}", err=True)


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily']),
              required=True,
              help='Type of data to convert (only stocks_daily supported for Qlib)')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--incremental/--full', default=True, help='Incremental or full conversion')
def convert(data_type, start_date, end_date, incremental):
    """Convert enriched data to Qlib binary format (stocks_daily only)."""

    # Enforce stocks_daily only restriction
    if data_type != 'stocks_daily':
        click.echo(f"❌ Error: Qlib conversion only supports stocks_daily data", err=True)
        click.echo(f"   Other data types (stocks_minute, options_*) should use enriched parquet format", err=True)
        click.echo(f"   Use 'quantmini data query' to access enriched parquet data", err=True)
        raise click.Abort()

    # Validate date range and show calendar info
    validate_date_range(data_type, start_date, end_date)

    config = ConfigLoader()

    click.echo(f"🔄 Converting {data_type} to Qlib binary format...")

    writer = QlibBinaryWriter(
        enriched_root=config.get_silver_path(),
        qlib_root=config.get_gold_path() / 'qlib',
        config=config
    )

    result = writer.convert_data_type(
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        incremental=incremental
    )

    click.echo(f"\n✅ Converted {result['symbols_converted']} symbols")
    click.echo(f"   Features: {result['features_written']}")

    # Record metadata for gold layer (Qlib binary format)
    try:
        metadata_root = config.get_metadata_path()
        metadata_manager = MetadataManager(metadata_root)

        # Record metadata for the conversion
        # Use a special data_type to distinguish from regular enrichment
        metadata_manager.record_ingestion(
            data_type=f"{data_type}_qlib",
            date=end_date,
            status='success',
            statistics={
                'symbols_converted': result['symbols_converted'],
                'features_written': result['features_written'],
                'start_date': start_date,
                'end_date': end_date,
            },
            layer='gold'
        )

        # Update watermark
        metadata_manager.set_watermark(
            data_type=f"{data_type}_qlib",
            date=end_date,
            layer='gold'
        )

        click.echo("✓ Metadata recorded for gold layer (Qlib conversion)")

    except Exception as e:
        click.echo(f"Warning: Failed to record metadata: {e}", err=True)
    if 'elapsed_time' in result:
        click.echo(f"   Time: {result['elapsed_time']:.2f}s")


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to query')
@click.option('--symbols', '-s', multiple=True, required=True, help='Symbols to query (can specify multiple)')
@click.option('--fields', '-f', multiple=True, required=True, help='Fields to query (can specify multiple)')
@click.option('--start-date', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', required=True, help='End date (YYYY-MM-DD)')
@click.option('--output', '-o', type=click.Path(), help='Output CSV file')
@click.option('--limit', '-l', type=int, help='Limit number of rows')
def query(data_type, symbols, fields, start_date, end_date, output, limit):
    """Query enriched data."""
    
    config = ConfigLoader()
    
    click.echo(f"🔍 Querying {data_type}...")
    click.echo(f"   Symbols: {', '.join(symbols)}")
    click.echo(f"   Fields: {', '.join(fields)}")

    engine = QueryEngine(
        data_root=config.get_silver_path(),
        config=config
    )
    
    result = engine.query_parquet(
        data_type=data_type,
        symbols=list(symbols),
        fields=list(fields),
        start_date=start_date,
        end_date=end_date
    )
    
    if limit:
        result = result.head(limit)
    
    click.echo(f"\n✅ Query returned {len(result)} rows")
    
    if output:
        result.to_csv(output, index=False)
        click.echo(f"   Saved to {output}")
    else:
        click.echo("\n" + result.to_string())


@data.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              help='Filter by data type')
@click.option('--start-date', '-s', help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', help='End date (YYYY-MM-DD)')
def status(data_type, start_date, end_date):
    """Show ingestion status."""
    
    config = ConfigLoader()
    metadata = MetadataManager(config.get_data_root() / 'metadata')
    
    if data_type:
        click.echo(f"📊 Status for {data_type}:")
        
        ingestions = metadata.list_ingestions(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date
        )
        
        if ingestions:
            click.echo(f"\n   Total ingestions: {len(ingestions)}")
            
            success = sum(1 for i in ingestions if i['status'] == 'success')
            failed = sum(1 for i in ingestions if i['status'] == 'failed')
            
            click.echo(f"   Successful: {success}")
            click.echo(f"   Failed: {failed}")
            
            watermark = metadata.get_watermark(data_type)
            if watermark:
                click.echo(f"   Latest data: {watermark}")
        else:
            click.echo("   No ingestions found")
    else:
        # Show all data types
        for dt in ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']:
            watermark = metadata.get_watermark(dt)
            if watermark:
                click.echo(f"{dt}: {watermark}")
