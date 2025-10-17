"""Polygon API data refresh commands."""

import click
import asyncio
from pathlib import Path
from datetime import datetime, date, timedelta

from src.core import ConfigLoader
from src.api import PolygonAPIClient, StocksAPIFetcher, OptionsAPIFetcher


@click.group()
def api():
    """Polygon API data operations (refresh real-time data)."""
    pass


@api.command()
@click.option('--tickers', '-t', multiple=True, required=True,
              help='Ticker symbols to fetch (can specify multiple, e.g., -t AAPL -t MSFT)')
@click.option('--start-date', '-s', required=True,
              help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e',
              help='End date (YYYY-MM-DD, defaults to today)')
@click.option('--output', '-o', type=click.Path(),
              help='Output directory (defaults to data/api/stocks_daily)')
@click.option('--adjusted/--no-adjusted', default=True,
              help='Adjust for splits (default: adjusted)')
def fetch_stocks_daily(tickers, start_date, end_date, output, adjusted):
    """
    Fetch daily stock bars from Polygon API.

    Example:
        quantmini api fetch-stocks-daily -t AAPL -t MSFT -s 2025-01-01
    """
    config = ConfigLoader()

    # Get API key from credentials
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 'api_key' not in polygon_creds:
        click.echo("❌ Error: Polygon API key not found.", err=True)
        click.echo("   Add 'api_key' under 'polygon' in config/credentials.yaml", err=True)
        click.echo("   Or set POLYGON_API_KEY environment variable", err=True)
        return

    api_key = polygon_creds['api_key']

    # Parse dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    else:
        end_dt = date.today()

    # Setup output directory
    if output:
        output_dir = Path(output)
    else:
        output_dir = config.get_data_root() / 'api' / 'stocks_daily'

    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"📡 Fetching daily bars from Polygon API...")
    click.echo(f"   Tickers: {', '.join(tickers)}")
    click.echo(f"   Date range: {start_dt} to {end_dt}")
    click.echo(f"   Adjusted: {adjusted}")

    async def fetch_data():
        api_config = config.pipeline_config.get('source', {}).get('api', {})

        async with PolygonAPIClient(
            api_key=api_key,
            max_retries=api_config.get('max_retries', 3),
            timeout=api_config.get('timeout_seconds', 30),
            rate_limit_calls=api_config.get('rate_limit_calls', 5),
            rate_limit_period=api_config.get('rate_limit_period', 1.0)
        ) as client:
            fetcher = StocksAPIFetcher(client)

            with click.progressbar(length=len(tickers), label='Fetching') as bar:
                df = await fetcher.fetch_daily_bars(
                    tickers=list(tickers),
                    from_date=start_dt,
                    to_date=end_dt,
                    adjusted=adjusted
                )
                bar.update(len(tickers))

            if df.is_empty():
                click.echo("\n❌ No data fetched", err=True)
                return

            # Save to parquet
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f"stocks_daily_{start_dt}_{end_dt}_{timestamp}.parquet"

            fetcher.save_to_parquet(df, output_file)

            click.echo(f"\n✅ Fetched {len(df)} bars")
            click.echo(f"   Unique tickers: {df['ticker'].n_unique()}")
            click.echo(f"   Saved to: {output_file}")

    asyncio.run(fetch_data())


@api.command()
@click.option('--tickers', '-t', multiple=True, required=True,
              help='Ticker symbols to fetch')
@click.option('--start-date', '-s', required=True,
              help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e',
              help='End date (YYYY-MM-DD, defaults to today)')
@click.option('--output', '-o', type=click.Path(),
              help='Output directory (defaults to data/api/stocks_minute)')
@click.option('--adjusted/--no-adjusted', default=True,
              help='Adjust for splits (default: adjusted)')
def fetch_stocks_minute(tickers, start_date, end_date, output, adjusted):
    """
    Fetch minute stock bars from Polygon API.

    Note: Minute data can be very large. Consider fetching smaller date ranges.

    Example:
        quantmini api fetch-stocks-minute -t AAPL -s 2025-01-15 -e 2025-01-15
    """
    config = ConfigLoader()

    # Get API key
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 'api_key' not in polygon_creds:
        click.echo("❌ Error: Polygon API key not found.", err=True)
        return

    api_key = polygon_creds['api_key']

    # Parse dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    else:
        end_dt = date.today()

    # Warn if date range is large
    days = (end_dt - start_dt).days + 1
    if days > 7:
        click.confirm(
            f"⚠️  You're fetching {days} days of minute data. This may be slow and use many API calls. Continue?",
            abort=True
        )

    # Setup output directory
    if output:
        output_dir = Path(output)
    else:
        output_dir = config.get_data_root() / 'api' / 'stocks_minute'

    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"📡 Fetching minute bars from Polygon API...")
    click.echo(f"   Tickers: {', '.join(tickers)}")
    click.echo(f"   Date range: {start_dt} to {end_dt}")

    async def fetch_data():
        api_config = config.pipeline_config.get('source', {}).get('api', {})

        async with PolygonAPIClient(
            api_key=api_key,
            max_retries=api_config.get('max_retries', 3),
            timeout=api_config.get('timeout_seconds', 30),
            rate_limit_calls=api_config.get('rate_limit_calls', 5),
            rate_limit_period=api_config.get('rate_limit_period', 1.0)
        ) as client:
            fetcher = StocksAPIFetcher(client)

            with click.progressbar(length=len(tickers), label='Fetching') as bar:
                df = await fetcher.fetch_minute_bars(
                    tickers=list(tickers),
                    from_date=start_dt,
                    to_date=end_dt,
                    adjusted=adjusted
                )
                bar.update(len(tickers))

            if df.is_empty():
                click.echo("\n❌ No data fetched", err=True)
                return

            # Save to parquet
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f"stocks_minute_{start_dt}_{end_dt}_{timestamp}.parquet"

            fetcher.save_to_parquet(df, output_file)

            click.echo(f"\n✅ Fetched {len(df)} minute bars")
            click.echo(f"   Unique tickers: {df['ticker'].n_unique()}")
            click.echo(f"   Saved to: {output_file}")

    asyncio.run(fetch_data())


@api.command()
@click.option('--underlying', '-u', multiple=True, required=True,
              help='Underlying ticker symbols (e.g., AAPL, MSFT)')
@click.option('--start-date', '-s', required=True,
              help='Start date for OHLCV data (YYYY-MM-DD)')
@click.option('--end-date', '-e',
              help='End date (YYYY-MM-DD, defaults to today)')
@click.option('--output', '-o', type=click.Path(),
              help='Output directory (defaults to data/api/options_daily)')
def fetch_options_daily(underlying, start_date, end_date, output):
    """
    Fetch daily options bars from Polygon API.

    This will:
    1. Discover all options contracts for the underlying tickers
    2. Fetch daily OHLCV data for those contracts

    Example:
        quantmini api fetch-options-daily -u AAPL -s 2025-01-01
    """
    config = ConfigLoader()

    # Get API key
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 'api_key' not in polygon_creds:
        click.echo("❌ Error: Polygon API key not found.", err=True)
        return

    api_key = polygon_creds['api_key']

    # Parse dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    else:
        end_dt = date.today()

    # Setup output directory
    if output:
        output_dir = Path(output)
    else:
        output_dir = config.get_data_root() / 'api' / 'options_daily'

    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"📡 Fetching options from Polygon API...")
    click.echo(f"   Underlyings: {', '.join(underlying)}")
    click.echo(f"   Date range: {start_dt} to {end_dt}")

    async def fetch_data():
        api_config = config.pipeline_config.get('source', {}).get('api', {})

        async with PolygonAPIClient(
            api_key=api_key,
            max_retries=api_config.get('max_retries', 3),
            timeout=api_config.get('timeout_seconds', 30),
            rate_limit_calls=api_config.get('rate_limit_calls', 5),
            rate_limit_period=api_config.get('rate_limit_period', 1.0)
        ) as client:
            fetcher = OptionsAPIFetcher(client)

            click.echo("   Discovering options contracts...")
            df = await fetcher.fetch_options_for_underlyings(
                underlying_tickers=list(underlying),
                from_date=start_dt,
                to_date=end_dt,
                timespan='day'
            )

            if df.is_empty():
                click.echo("\n❌ No data fetched", err=True)
                return

            # Save to parquet
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f"options_daily_{start_dt}_{end_dt}_{timestamp}.parquet"

            fetcher.save_to_parquet(df, output_file)

            click.echo(f"\n✅ Fetched {len(df)} option bars")
            click.echo(f"   Unique contracts: {df['ticker'].n_unique()}")
            click.echo(f"   Saved to: {output_file}")

    asyncio.run(fetch_data())


@api.command()
@click.option('--underlying', '-u', multiple=True, required=True,
              help='Underlying ticker symbols')
@click.option('--start-date', '-s', required=True,
              help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e',
              help='End date (YYYY-MM-DD, defaults to today)')
@click.option('--output', '-o', type=click.Path(),
              help='Output directory (defaults to data/api/options_minute)')
def fetch_options_minute(underlying, start_date, end_date, output):
    """
    Fetch minute options bars from Polygon API.

    Note: Options minute data can be extremely large.

    Example:
        quantmini api fetch-options-minute -u AAPL -s 2025-01-15 -e 2025-01-15
    """
    config = ConfigLoader()

    # Get API key
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 'api_key' not in polygon_creds:
        click.echo("❌ Error: Polygon API key not found.", err=True)
        return

    api_key = polygon_creds['api_key']

    # Parse dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    else:
        end_dt = date.today()

    # Warn if date range is large
    days = (end_dt - start_dt).days + 1
    if days > 3:
        click.confirm(
            f"⚠️  You're fetching {days} days of options minute data. This will be VERY slow. Continue?",
            abort=True
        )

    # Setup output directory
    if output:
        output_dir = Path(output)
    else:
        output_dir = config.get_data_root() / 'api' / 'options_minute'

    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"📡 Fetching options minute data from Polygon API...")
    click.echo(f"   Underlyings: {', '.join(underlying)}")
    click.echo(f"   Date range: {start_dt} to {end_dt}")

    async def fetch_data():
        api_config = config.pipeline_config.get('source', {}).get('api', {})

        async with PolygonAPIClient(
            api_key=api_key,
            max_retries=api_config.get('max_retries', 3),
            timeout=api_config.get('timeout_seconds', 30),
            rate_limit_calls=api_config.get('rate_limit_calls', 5),
            rate_limit_period=api_config.get('rate_limit_period', 1.0)
        ) as client:
            fetcher = OptionsAPIFetcher(client)

            click.echo("   Discovering options contracts...")
            df = await fetcher.fetch_options_for_underlyings(
                underlying_tickers=list(underlying),
                from_date=start_dt,
                to_date=end_dt,
                timespan='minute'
            )

            if df.is_empty():
                click.echo("\n❌ No data fetched", err=True)
                return

            # Save to parquet
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f"options_minute_{start_dt}_{end_dt}_{timestamp}.parquet"

            fetcher.save_to_parquet(df, output_file)

            click.echo(f"\n✅ Fetched {len(df)} option minute bars")
            click.echo(f"   Unique contracts: {df['ticker'].n_unique()}")
            click.echo(f"   Saved to: {output_file}")

    asyncio.run(fetch_data())


@api.command()
def test_connection():
    """
    Test Polygon API connection and credentials.

    This will fetch the latest trading day for SPY to verify the API is working.
    """
    config = ConfigLoader()

    # Get API key
    polygon_creds = config.get_credentials('polygon')
    if not polygon_creds or 'api_key' not in polygon_creds:
        click.echo("❌ Error: Polygon API key not found.", err=True)
        click.echo("   Add 'api_key' under 'polygon' in config/credentials.yaml", err=True)
        click.echo("   Or set POLYGON_API_KEY environment variable", err=True)
        return

    api_key = polygon_creds['api_key']

    click.echo("🔧 Testing Polygon API connection...")

    async def test():
        api_config = config.pipeline_config.get('source', {}).get('api', {})

        try:
            async with PolygonAPIClient(
                api_key=api_key,
                max_retries=api_config.get('max_retries', 3),
                timeout=api_config.get('timeout_seconds', 30),
                rate_limit_calls=api_config.get('rate_limit_calls', 5),
                rate_limit_period=api_config.get('rate_limit_period', 1.0)
            ) as client:
                fetcher = StocksAPIFetcher(client)

                latest_date = await fetcher.get_latest_trading_day()

                click.echo(f"✅ Connection successful!")
                click.echo(f"   Latest trading day: {latest_date}")
                click.echo(f"   Rate limit: {api_config.get('rate_limit_calls', 5)} calls/second")

        except Exception as e:
            click.echo(f"❌ Connection failed: {e}", err=True)

    asyncio.run(test())
