"""Configuration management commands."""

import click
import yaml
from pathlib import Path

from src.core import ConfigLoader, SystemProfiler


@click.group(name='config')
def config():
    """Manage configuration."""
    pass


@config.command()
@click.option('--force', is_flag=True, help='Overwrite existing configuration')
def init(force):
    """Initialize configuration files."""
    
    config_dir = Path('config')
    config_dir.mkdir(exist_ok=True)
    
    # Create credentials template
    creds_file = config_dir / 'credentials.yaml'
    if creds_file.exists() and not force:
        click.echo(f"   ℹ️  {creds_file} already exists (use --force to overwrite)")
    else:
        creds_template = {
            'polygon': {
                'access_key_id': 'YOUR_ACCESS_KEY_ID',
                'secret_access_key': 'YOUR_SECRET_ACCESS_KEY'
            }
        }
        with open(creds_file, 'w') as f:
            yaml.dump(creds_template, f, default_flow_style=False)
        click.echo(f"   ✅ Created {creds_file}")
        click.echo(f"      → Edit this file with your Polygon.io credentials")
    
    # Create pipeline config template
    config_file = config_dir / 'pipeline_config.yaml'
    if config_file.exists() and not force:
        click.echo(f"   ℹ️  {config_file} already exists (use --force to overwrite)")
    else:
        config_template = {
            'pipeline': {
                'mode': 'adaptive',
                'data_root': 'data'
            },
            's3_source': {
                'bucket': 'flatfiles',
                'endpoint_url': 'https://files.polygon.io'
            },
            'processing': {
                'use_polars': True,
                'chunk_size': 100000
            },
            'parquet': {
                'compression': 'snappy',
                'row_group_size': 1000000
            }
        }
        with open(config_file, 'w') as f:
            yaml.dump(config_template, f, default_flow_style=False)
        click.echo(f"   ✅ Created {config_file}")
    
    # Run system profiler
    click.echo("\n📊 Profiling system...")
    profiler = SystemProfiler(config_dir)
    profiler.print_summary()
    
    click.echo("\n✅ Configuration initialized!")
    click.echo(f"   Edit {creds_file} with your Polygon.io credentials")


@config.command()
def show():
    """Show current configuration."""
    
    try:
        config = ConfigLoader()
        config.print_config()
    except Exception as e:
        click.echo(f"❌ Error loading configuration: {e}", err=True)
        click.echo("   Run 'quantmini config init' to create configuration files")


@config.command()
def profile():
    """Show system profile."""
    
    config_dir = Path('config')
    profile_file = config_dir / 'system_profile.yaml'
    
    if not profile_file.exists():
        click.echo("📊 Creating system profile...")
        profiler = SystemProfiler(config_dir)
        profiler.print_summary()
    else:
        click.echo("📊 System Profile:")
        with open(profile_file) as f:
            profile = yaml.safe_load(f)
        
        hw = profile.get('hardware', {})
        click.echo(f"\n   CPU: {hw.get('cpu_cores')} cores ({hw.get('cpu_logical')} logical)")
        click.echo(f"   Memory: {hw.get('memory_gb', 0):.1f} GB")
        click.echo(f"   Platform: {hw.get('platform')}")
        
        if hw.get('is_apple_silicon'):
            click.echo(f"   Apple Silicon: ✓")
        
        click.echo(f"\n   Recommended mode: {profile.get('recommended_mode')}")
        
        limits = profile.get('resource_limits', {})
        click.echo(f"   Max memory: {limits.get('max_memory_gb', 0):.1f} GB")
        click.echo(f"   Max workers: {limits.get('max_workers')}")


@config.command()
@click.argument('key')
@click.argument('value')
def set(key, value):
    """Set configuration value."""
    
    config_file = Path('config/pipeline_config.yaml')
    
    if not config_file.exists():
        click.echo("❌ Configuration file not found. Run 'quantmini config init' first.", err=True)
        return
    
    with open(config_file) as f:
        config = yaml.safe_load(f) or {}
    
    # Support dot notation (e.g., pipeline.mode)
    keys = key.split('.')
    current = config
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    
    # Try to infer type
    if value.lower() == 'true':
        value = True
    elif value.lower() == 'false':
        value = False
    elif value.isdigit():
        value = int(value)
    elif value.replace('.', '').isdigit():
        value = float(value)
    
    current[keys[-1]] = value
    
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    click.echo(f"✅ Set {key} = {value}")


@config.command()
@click.argument('key')
def get(key):
    """Get configuration value."""
    
    try:
        config = ConfigLoader()
        value = config.get(key)
        
        if value is not None:
            click.echo(f"{key} = {value}")
        else:
            click.echo(f"❌ Key not found: {key}", err=True)
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
