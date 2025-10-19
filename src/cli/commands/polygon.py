"""
CLI commands for Polygon REST API data downloads

Provides commands to download:
- Reference data (ticker types, related tickers)
- Corporate actions (dividends, splits, events)
- Fundamentals (balance sheets, income statements, cash flow)
- Short data (short interest, short volume)
- Economy data (treasury yields, inflation, inflation expectations)
"""

import click
import asyncio
from pathlib import Path
from datetime import date, timedelta
import logging

from ...core.config_loader import ConfigLoader
from ...download import (
    PolygonRESTClient,
    ReferenceDataDownloader,
    CorporateActionsDownloader,
    FundamentalsDownloader,
    FinancialRatiosDownloader,
    EconomyDataDownloader,
    AggregatesDownloader,
    SnapshotsDownloader,
    MarketStatusDownloader,
    TechnicalIndicatorsDownloader,
    OptionsDownloader,
    TradesQuotesDownloader,
    IndicesDownloader,
    NewsDownloader,
    ForexDownloader,
    CryptoDownloader
)

logger = logging.getLogger(__name__)


@click.group()
def polygon():
    """Polygon REST API data downloads"""
    pass


def _get_api_key(credentials: dict) -> str:
    """Extract API key from credentials (supports multiple formats)"""
    if not credentials:
        return None

    # Format 1: polygon.api_key
    if 'api_key' in credentials:
        return credentials['api_key']

    # Format 2: polygon.api.key
    if 'api' in credentials and isinstance(credentials['api'], dict):
        return credentials['api'].get('key')

    return None


@polygon.command()
@click.option('--asset-class', type=str, help='Filter by asset class (stocks, options, crypto, fx, indices)')
@click.option('--locale', type=str, help='Filter by locale (us, global)')
@click.option('--output-dir', type=Path, default='data/reference', help='Output directory')
def ticker_types(asset_class, locale, output_dir):
    """Download ticker types"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = ReferenceDataDownloader(client, output_dir)
            click.echo("📥 Downloading ticker types...")

            df = await downloader.download_ticker_types(asset_class, locale)
            click.echo(f"✅ Downloaded {len(df)} ticker types")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def related_tickers(tickers, output_dir):
    """Download related tickers for one or more tickers in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = ReferenceDataDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/related_tickers/")
            click.echo(f"📥 Downloading related tickers for {len(tickers)} tickers...")

            df = await downloader.download_related_tickers_batch(list(tickers))
            click.echo(f"✅ Downloaded {len(df)} related ticker relationships")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.option('--ticker', type=str, help='Filter by ticker symbol')
@click.option('--start-date', type=str, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', type=str, help='End-date (YYYY-MM-DD)')
@click.option('--include-ipos', is_flag=True, help='Include IPO data')
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def corporate_actions(ticker, start_date, end_date, include_ipos, output_dir):
    """Download corporate actions (dividends, splits, IPOs) in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = CorporateActionsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📥 Downloading corporate actions for {ticker or 'all tickers'}...")
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/")

            data = await downloader.download_all_corporate_actions(
                ticker,
                start_date,
                end_date,
                include_ipos=include_ipos
            )

            click.echo(f"✅ Downloaded corporate actions:")
            click.echo(f"   Dividends: {len(data['dividends'])} records")
            click.echo(f"   Splits: {len(data['splits'])} records")
            if include_ipos:
                click.echo(f"   IPOs: {len(data['ipos'])} records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def ticker_events(tickers, output_dir):
    """Download ticker events (symbol changes, rebranding) in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = CorporateActionsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/ticker_events/")
            click.echo(f"📥 Downloading ticker events for {len(tickers)} tickers...")

            df = await downloader.download_ticker_events_batch(list(tickers))
            click.echo(f"✅ Downloaded {len(df)} ticker event records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--timeframe', type=click.Choice(['annual', 'quarterly']), default='quarterly', help='Reporting period')
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def fundamentals(tickers, timeframe, output_dir):
    """Download fundamentals (balance sheets, income statements, cash flow) in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = FundamentalsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📥 Downloading {timeframe} fundamentals for {len(tickers)} tickers...")
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/")

            data = await downloader.download_financials_batch(list(tickers), timeframe)

            click.echo(f"✅ Downloaded fundamentals:")
            click.echo(f"   Balance sheets: {data['balance_sheets']} records")
            click.echo(f"   Cash flow: {data['cash_flow']} records")
            click.echo(f"   Income statements: {data['income_statements']} records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--input-dir', type=Path, default='data/partitioned_screener', help='Input directory with fundamentals data')
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
@click.option('--include-growth', is_flag=True, default=True, help='Include growth rate calculations')
def financial_ratios(tickers, input_dir, output_dir, include_growth):
    """Calculate financial ratios from fundamentals data in partitioned structure"""
    async def run():
        downloader = FinancialRatiosDownloader(
            input_dir,
            output_dir,
            use_partitioned_structure=True
        )
        click.echo(f"📂 Reading fundamentals from: {input_dir}/")
        click.echo(f"📂 Saving ratios to: {output_dir}/financial_ratios/")
        click.echo(f"📊 Calculating ratios for {len(tickers)} tickers...")

        ratios_by_ticker = await downloader.calculate_ratios_batch(
            list(tickers),
            include_growth=include_growth
        )

        # Count total records
        total_ratios = sum(len(df) for df in ratios_by_ticker.values())

        click.echo(f"✅ Calculated {total_ratios} ratio records")

        # Show per-ticker breakdown
        for ticker, df in ratios_by_ticker.items():
            if len(df) > 0:
                click.echo(f"   {ticker}: {len(df)} records")

        return 0

    return asyncio.run(run())


@polygon.command()
@click.option('--start-date', type=str, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', type=str, help='End date (YYYY-MM-DD)')
@click.option('--days', type=int, default=90, help='Number of days to download (default: 90)')
@click.option('--output-dir', type=Path, default='data/economy', help='Output directory')
def economy(start_date, end_date, days, output_dir):
    """Download economy data (treasury yields, inflation, expectations)"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        # Calculate date range
        nonlocal end_date, start_date
        if not end_date:
            end_date = date.today().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = EconomyDataDownloader(client, output_dir)
            click.echo(f"📥 Downloading economy data ({start_date} to {end_date})...")

            data = await downloader.download_all_economy_data(start_date, end_date)

            click.echo(f"✅ Downloaded economy data:")
            click.echo(f"   Treasury yields: {len(data['treasury_yields'])} records")
            click.echo(f"   Inflation: {len(data['inflation'])} records")
            click.echo(f"   Inflation expectations: {len(data['inflation_expectations'])} records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.option('--date', type=str, help='Date for yield curve (YYYY-MM-DD, default: today)')
@click.option('--output-dir', type=Path, default='data/economy', help='Output directory')
def yield_curve(date_str, output_dir):
    """Download full treasury yield curve for a specific date"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        # Use today if no date provided
        if not date_str:
            date_str = date.today().strftime('%Y-%m-%d')

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = EconomyDataDownloader(client, output_dir)
            click.echo(f"📥 Downloading yield curve for {date_str}...")

            df = await downloader.download_treasury_curve(date_str)

            click.echo(f"✅ Downloaded {len(df)} yield curve data points")

            if len(df) > 0:
                click.echo("\nYield Curve:")
                for row in df.iter_rows(named=True):
                    click.echo(f"   {row.get('ticker', 'N/A'):8s}: {row.get('value', 0):.2f}%")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def short_interest(tickers, output_dir):
    """Download short interest data for one or more tickers in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = FundamentalsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/short_interest/")
            click.echo(f"📥 Downloading short interest for {len(tickers)} tickers...")

            if len(tickers) == 1:
                # Single ticker
                df = await downloader.download_short_interest(tickers[0])
                click.echo(f"✅ Downloaded {len(df)} short interest records")
            else:
                # Batch download
                data = await downloader.download_short_data_batch(list(tickers))
                click.echo(f"✅ Downloaded {len(data['short_interest'])} short interest records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def short_volume(tickers, output_dir):
    """Download short volume data for one or more tickers in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = FundamentalsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/short_volume/")
            click.echo(f"📥 Downloading short volume for {len(tickers)} tickers...")

            if len(tickers) == 1:
                # Single ticker
                df = await downloader.download_short_volume(tickers[0])
                click.echo(f"✅ Downloaded {len(df)} short volume records")
            else:
                # Batch download
                data = await downloader.download_short_data_batch(list(tickers))
                click.echo(f"✅ Downloaded {len(data['short_volume'])} short volume records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def short_data(tickers, output_dir):
    """Download both short interest and short volume for one or more tickers in partitioned structure"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)

        if not api_key:
            click.echo("❌ API key not found. Please configure config/credentials.yaml", err=True)
            return 1

        async with PolygonRESTClient(
            api_key=api_key,
            max_concurrent=100,
            max_connections=200
        ) as client:
            downloader = FundamentalsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )
            click.echo(f"📂 Saving to partitioned structure: {output_dir}/")
            click.echo(f"📥 Downloading short data for {len(tickers)} tickers...")

            data = await downloader.download_short_data_batch(list(tickers))

            click.echo(f"✅ Downloaded short data:")
            click.echo(f"   Short interest: {len(data['short_interest'])} records")
            click.echo(f"   Short volume: {len(data['short_volume'])} records")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0

    return asyncio.run(run())


# ===== NEW COMMANDS - Phase 1, 2, 3 =====

@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--multiplier', type=int, default=1, help='Size of timespan multiplier')
@click.option('--timespan', type=click.Choice(['minute', 'hour', 'day', 'week', 'month']), default='day', help='Size of time window')
@click.option('--from-date', type=str, help='Start date (YYYY-MM-DD)')
@click.option('--to-date', type=str, help='End date (YYYY-MM-DD)')
@click.option('--output-dir', type=Path, default='data/bars', help='Output directory')
def bars(tickers, multiplier, timespan, from_date, to_date, output_dir):
    """Download aggregate bars (OHLCV) for one or more tickers"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = AggregatesDownloader(client, output_dir)
            click.echo(f"📥 Downloading {timespan} bars for {len(tickers)} tickers...")
            if len(tickers) == 1:
                df = await downloader.download_bars(tickers[0], multiplier, timespan, from_date, to_date)
                click.echo(f"✅ Downloaded {len(df)} bars")
            else:
                df = await downloader.download_bars_batch(list(tickers), multiplier, timespan, from_date, to_date)
                click.echo(f"✅ Downloaded {len(df)} total bars")
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics: {stats['total_requests']} requests, {stats['success_rate']:.1%} success")
            return 0
    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1, required=True)
@click.option('--output-dir', type=Path, default='data/snapshots', help='Output directory')
def snapshots(tickers, output_dir):
    """Download real-time snapshots for one or more tickers"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = SnapshotsDownloader(client, output_dir)
            click.echo(f"📥 Downloading snapshots for {len(tickers)} tickers...")
            df = await downloader.download_ticker_snapshots_batch(list(tickers))
            click.echo(f"✅ Downloaded {len(df)} snapshots")
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics: {stats['total_requests']} requests")
            return 0
    return asyncio.run(run())


@polygon.command()
@click.option('--output-dir', type=Path, default='data/market_status', help='Output directory')
def market_status(output_dir):
    """Download market status, holidays, and metadata"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = MarketStatusDownloader(client, output_dir)
            click.echo("📥 Downloading market metadata...")
            data = await downloader.download_all_market_metadata()
            click.echo(f"✅ Downloaded market metadata:")
            for key, df in data.items():
                click.echo(f"   {key}: {len(df)} records")
            return 0
    return asyncio.run(run())


@polygon.command()
@click.argument('ticker', required=True)
@click.option('--indicator', type=click.Choice(['sma', 'ema', 'macd', 'rsi', 'all']), default='all', help='Indicator type')
@click.option('--window', type=int, default=50, help='Window size (for SMA/EMA/RSI)')
@click.option('--output-dir', type=Path, default='data/indicators', help='Output directory')
def indicators(ticker, indicator, window, output_dir):
    """Download technical indicators for a ticker"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = TechnicalIndicatorsDownloader(client, output_dir)
            if indicator == 'all':
                click.echo(f"📥 Downloading all indicators for {ticker}...")
                data = await downloader.download_all_indicators(ticker)
                click.echo(f"✅ Downloaded indicators:")
                for key, df in data.items():
                    click.echo(f"   {key}: {len(df)} records")
            elif indicator == 'sma':
                df = await downloader.download_sma(ticker, window=window)
                click.echo(f"✅ Downloaded {len(df)} SMA({window}) records")
            elif indicator == 'ema':
                df = await downloader.download_ema(ticker, window=window)
                click.echo(f"✅ Downloaded {len(df)} EMA({window}) records")
            elif indicator == 'macd':
                df = await downloader.download_macd(ticker)
                click.echo(f"✅ Downloaded {len(df)} MACD records")
            elif indicator == 'rsi':
                df = await downloader.download_rsi(ticker, window=window)
                click.echo(f"✅ Downloaded {len(df)} RSI({window}) records")
            return 0
    return asyncio.run(run())


@polygon.command()
@click.option('--underlying', type=str, help='Underlying ticker')
@click.option('--expiration', type=str, help='Expiration date (YYYY-MM-DD)')
@click.option('--output-dir', type=Path, default='data/options', help='Output directory')
def options(underlying, expiration, output_dir):
    """Download options contracts and chains"""
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1
        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = OptionsDownloader(client, output_dir)
            if underlying:
                click.echo(f"📥 Downloading options chain for {underlying}...")
                df = await downloader.download_options_chain(underlying, expiration)
                click.echo(f"✅ Downloaded {len(df)} options contracts")
            else:
                click.echo("📥 Downloading all options contracts...")
                df = await downloader.download_contracts(expiration_date=expiration)
                click.echo(f"✅ Downloaded {len(df)} contracts")
            return 0
    return asyncio.run(run())


@polygon.command()
@click.argument('tickers', nargs=-1)
@click.option('--start-date', type=str, help='Start date for news (YYYY-MM-DD)')
@click.option('--end-date', type=str, help='End date for news (YYYY-MM-DD)')
@click.option('--days', type=int, default=30, help='Number of days to download (default: 30, used if dates not specified)')
@click.option('--limit', type=int, default=1000, help='Number of news articles per ticker (max 1000)')
@click.option('--output-dir', type=Path, default='data/partitioned_screener', help='Output directory (partitioned structure)')
def news(tickers, start_date, end_date, days, limit, output_dir):
    """Download news articles for one or more tickers in partitioned structure

    Examples:
      quantmini polygon news AAPL MSFT --days 30
      quantmini polygon news AAPL --start-date 2024-01-01 --end-date 2024-12-31
      quantmini polygon news --days 7  # All tickers from the last 7 days
    """
    async def run():
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')
        api_key = _get_api_key(credentials)
        if not api_key:
            click.echo("❌ API key not found", err=True)
            return 1

        # Calculate date range
        nonlocal end_date, start_date
        if not end_date:
            end_date = date.today().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')

        async with PolygonRESTClient(api_key=api_key, max_concurrent=100, max_connections=200) as client:
            downloader = NewsDownloader(
                client,
                output_dir,
                use_partitioned_structure=True
            )

            click.echo(f"📂 Saving to partitioned structure: {output_dir}/news/")
            click.echo(f"📅 Date range: {start_date} to {end_date}")

            if tickers:
                # Download for specific tickers
                click.echo(f"📥 Downloading news for {len(tickers)} tickers...")

                if len(tickers) == 1:
                    # Single ticker
                    df = await downloader.download_ticker_news(
                        ticker=tickers[0],
                        published_utc_gte=start_date,
                        published_utc_lte=end_date,
                        limit=limit
                    )
                    click.echo(f"✅ Downloaded {len(df)} news articles")
                else:
                    # Batch download
                    result = await downloader.download_news_batch(
                        tickers=list(tickers),
                        published_utc_gte=start_date,
                        published_utc_lte=end_date,
                        limit=limit
                    )
                    click.echo(f"✅ Downloaded {result['total_articles']} total news articles")
            else:
                # Download all news (no ticker filter)
                click.echo(f"📥 Downloading all news articles...")
                df = await downloader.download_ticker_news(
                    ticker=None,
                    published_utc_gte=start_date,
                    published_utc_lte=end_date,
                    limit=limit
                )
                click.echo(f"✅ Downloaded {len(df)} news articles")

            # Show statistics
            stats = client.get_statistics()
            click.echo(f"\n📊 Statistics:")
            click.echo(f"   Total requests: {stats['total_requests']}")
            click.echo(f"   Success rate: {stats['success_rate']:.1%}")

            return 0
    return asyncio.run(run())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    polygon()
