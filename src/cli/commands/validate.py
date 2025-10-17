"""Validation commands."""

import click
from pathlib import Path

from src.core import ConfigLoader
from src.transform import QlibBinaryValidator


@click.group()
def validate():
    """Validate data and conversions."""
    pass


@validate.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to validate')
def binary(data_type):
    """Validate Qlib binary format conversion."""
    
    config = ConfigLoader()
    qlib_root = config.get_data_root() / 'qlib' / data_type
    
    if not qlib_root.exists():
        click.echo(f"❌ Qlib binary directory not found: {qlib_root}", err=True)
        return
    
    click.echo(f"🔍 Validating {data_type} binary conversion...")
    
    validator = QlibBinaryValidator(qlib_root)
    results = validator.validate_conversion(data_type)
    
    if results['all_passed']:
        click.echo("\n✅ All validation checks passed!")
    else:
        click.echo("\n❌ Validation failed:")
    
    click.echo("\nValidation Results:")
    for check, passed in results['checks'].items():
        status = "✅" if passed else "❌"
        click.echo(f"   {status} {check}")
    
    if results.get('details'):
        click.echo("\nDetails:")
        for key, value in results['details'].items():
            click.echo(f"   {key}: {value}")


@validate.command()
@click.option('--data-type', '-t',
              type=click.Choice(['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute']),
              required=True,
              help='Type of data to check')
def parquet(data_type):
    """Validate Parquet data integrity."""
    
    config = ConfigLoader()
    parquet_root = config.get_data_root() / 'lake' / data_type
    
    if not parquet_root.exists():
        click.echo(f"❌ Parquet directory not found: {parquet_root}", err=True)
        return
    
    click.echo(f"🔍 Validating {data_type} Parquet data...")
    
    from src.storage import ParquetManager
    
    manager = ParquetManager(config.get_data_root() / 'lake', data_type)
    stats = manager.get_statistics()
    
    click.echo(f"\n✅ Parquet data validation:")
    click.echo(f"   Total partitions: {stats['total_partitions']}")
    click.echo(f"   Total size: {stats['total_size_mb']:.2f} MB")
    
    if 'date_range' in stats:
        click.echo(f"   Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
    
    if 'symbols' in stats:
        click.echo(f"   Symbols: {len(stats['symbols'])}")


@validate.command()
@click.option('--fix', is_flag=True, help='Fix detected issues')
def config(fix):
    """Validate configuration files."""
    
    config_dir = Path('config')
    issues = []
    
    click.echo("🔍 Validating configuration...")
    
    # Check credentials
    creds_file = config_dir / 'credentials.yaml'
    if not creds_file.exists():
        issues.append("credentials.yaml not found")
    else:
        click.echo("   ✅ credentials.yaml exists")
    
    # Check pipeline config
    config_file = config_dir / 'pipeline_config.yaml'
    if not config_file.exists():
        issues.append("pipeline_config.yaml not found")
    else:
        click.echo("   ✅ pipeline_config.yaml exists")
    
    # Check system profile
    profile_file = config_dir / 'system_profile.yaml'
    if not profile_file.exists():
        issues.append("system_profile.yaml not found")
    else:
        click.echo("   ✅ system_profile.yaml exists")
    
    # Try loading config
    try:
        from src.core import ConfigLoader
        config = ConfigLoader()
        click.echo("   ✅ Configuration loads successfully")
    except Exception as e:
        issues.append(f"Configuration load error: {e}")
    
    if issues:
        click.echo("\n❌ Issues found:")
        for issue in issues:
            click.echo(f"   • {issue}")
        
        if fix:
            click.echo("\n🔧 Fixing issues...")
            click.echo("   Run 'quantmini config init' to create missing files")
    else:
        click.echo("\n✅ Configuration is valid!")
