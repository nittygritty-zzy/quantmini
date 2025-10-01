"""Pipeline workflow commands."""

import click
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from src.core import ConfigLoader
from src.orchestration import IngestionOrchestrator
from src.features import FeatureEngineer
from src.transform import QlibBinaryWriter


@click.group()
def pipeline():
    """Run complete pipeline workflows."""
    pass


@pipeline.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to process')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
@click.option('--skip-ingest', is_flag=True, help='Skip ingestion step')
@click.option('--skip-enrich', is_flag=True, help='Skip enrichment step')
@click.option('--skip-convert', is_flag=True, help='Skip conversion step')
def run(data_type, start_date, end_date, skip_ingest, skip_enrich, skip_convert):
    """Run complete pipeline: ingest → enrich → convert."""
    
    config = ConfigLoader()
    
    click.echo(f"🚀 Running pipeline for {data_type}")
    click.echo(f"   Date range: {start_date} to {end_date}\n")
    
    async def run_pipeline():
        # Step 1: Ingest
        if not skip_ingest:
            click.echo("📊 Step 1/3: Ingesting data...")
            orchestrator = IngestionOrchestrator(config=config)
            
            result = await orchestrator.ingest_date_range(
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                incremental=True,
                use_polars=True
            )
            
            if result['success_rate'] < 1.0:
                click.echo(f"⚠️  Warning: Ingestion success rate {result['success_rate']:.1%}")
            
            click.echo(f"   ✅ Ingested {result['total_records']:,} records\n")
        else:
            click.echo("   ⏭️  Skipping ingestion\n")
        
        # Step 2: Enrich
        if not skip_enrich:
            click.echo("⚙️  Step 2/3: Adding features...")
            
            with FeatureEngineer(
                parquet_root=config.get_data_root() / 'lake',
                enriched_root=config.get_data_root() / 'enriched',
                config=config
            ) as engineer:
                result = engineer.enrich_date_range(
                    data_type=data_type,
                    start_date=start_date,
                    end_date=end_date,
                    incremental=True
                )
                
                click.echo(f"   ✅ Enriched {result['records_processed']:,} records\n")
        else:
            click.echo("   ⏭️  Skipping enrichment\n")
        
        # Step 3: Convert
        if not skip_convert:
            click.echo("🔄 Step 3/3: Converting to Qlib format...")
            
            with QlibBinaryWriter(
                enriched_root=config.get_data_root() / 'enriched',
                qlib_root=config.get_data_root() / 'binary',
                config=config
            ) as writer:
                result = writer.convert_data_type(
                    data_type=data_type,
                    start_date=start_date,
                    end_date=end_date,
                    incremental=True
                )
                
                click.echo(f"   ✅ Converted {result['symbols_converted']} symbols\n")
        else:
            click.echo("   ⏭️  Skipping conversion\n")
        
        click.echo("🎉 Pipeline complete!")
    
    asyncio.run(run_pipeline())


@pipeline.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to update')
@click.option('--days', '-d', type=int, default=1, help='Number of days to update (default: 1)')
def daily(data_type, days):
    """Run daily update (ingest → enrich → convert for recent days)."""
    
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    click.echo(f"📅 Running daily update for {data_type}")
    click.echo(f"   Updating last {days} day(s): {start_date} to {end_date}\n")
    
    config = ConfigLoader()
    
    async def run_daily():
        orchestrator = IngestionOrchestrator(config=config)
        
        result = await orchestrator.ingest_date_range(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            incremental=True,
            use_polars=True
        )
        
        if result['dates_succeeded'] == 0:
            click.echo("   ℹ️  No new data to ingest (already up to date)")
            return
        
        click.echo(f"   ✅ Ingested {result['total_records']:,} records")
        
        # Enrich
        with FeatureEngineer(
            parquet_root=config.get_data_root() / 'lake',
            enriched_root=config.get_data_root() / 'enriched',
            config=config
        ) as engineer:
            engineer.enrich_date_range(
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                incremental=True
            )
        
        click.echo(f"   ✅ Features added")
        
        # Convert
        with QlibBinaryWriter(
            enriched_root=config.get_data_root() / 'enriched',
            qlib_root=config.get_data_root() / 'binary',
            config=config
        ) as writer:
            writer.convert_data_type(
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                incremental=True
            )
        
        click.echo(f"   ✅ Converted to Qlib format\n")
        click.echo("🎉 Daily update complete!")
    
    asyncio.run(run_daily())


@pipeline.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to backfill')
@click.option('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
def backfill(data_type, start_date, end_date):
    """Backfill missing data for date range."""
    
    config = ConfigLoader()
    
    click.echo(f"🔙 Backfilling {data_type} from {start_date} to {end_date}...")
    
    async def run_backfill():
        orchestrator = IngestionOrchestrator(config=config)
        
        result = await orchestrator.backfill(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date
        )
        
        if result['dates_processed'] == 0:
            click.echo("   ℹ️  No missing dates found")
            return
        
        click.echo(f"\n✅ Backfilled {result['dates_processed']} dates")
        click.echo(f"   Records: {result['total_records']:,}")
        click.echo(f"   Success rate: {result['success_rate']:.1%}")
    
    asyncio.run(run_backfill())
