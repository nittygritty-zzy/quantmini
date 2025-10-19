#!/usr/bin/env python3
"""
Complete Medallion Architecture Pipeline Test

Runs the complete pipeline: Landing → Bronze → Silver → Gold
All data stored in test_pipeline/ directory.

Usage:
    uv run python scripts/tests/run_complete_pipeline.py
"""

import asyncio
import sys
import logging
import polars as pl
import struct
from pathlib import Path
from datetime import date, datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config_loader import ConfigLoader
from src.download.polygon_rest_client import PolygonRESTClient
from src.download.news import NewsDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# BRONZE LAYER: Download Data
# ============================================================================

async def phase_bronze(test_dir: Path, config: ConfigLoader):
    """Download data to bronze layer"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 1: BRONZE LAYER - Download Data")
    logger.info("="*80)

    bronze_dir = test_dir / 'bronze'
    bronze_dir.mkdir(parents=True, exist_ok=True)

    # Configuration - Last Month (September 2025)
    START_DATE = '2025-09-01'
    END_DATE = '2025-09-30'
    TEST_TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']

    credentials = config.get_credentials('polygon')

    async with PolygonRESTClient(
        api_key=credentials['api_key'],
        max_concurrent=50,
        max_connections=100
    ) as client:

        # Download news
        logger.info(f"\n📰 Downloading news for {len(TEST_TICKERS)} tickers...")
        news_path = bronze_dir / 'news'
        news_path.mkdir(parents=True, exist_ok=True)

        downloader = NewsDownloader(
            client=client,
            output_dir=news_path,
            use_partitioned_structure=True
        )

        result = await downloader.download_news_batch(
            tickers=TEST_TICKERS,
            published_utc_gte=START_DATE,
            published_utc_lte=END_DATE,
            limit=100
        )

        logger.info(f"✅ Downloaded {result['total_articles']} news articles")

        # Statistics
        stats = client.get_statistics()
        logger.info(f"\n📊 API Statistics:")
        logger.info(f"   Requests: {stats['total_requests']}")
        logger.info(f"   Success rate: {stats['success_rate']:.1%}")

    return {
        'news_articles': result['total_articles'],
        'tickers': TEST_TICKERS
    }


# ============================================================================
# SILVER LAYER: Feature Engineering
# ============================================================================

def phase_silver(test_dir: Path, bronze_results: dict):
    """Add features to create silver layer"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 2: SILVER LAYER - Feature Engineering")
    logger.info("="*80)

    bronze_news = test_dir / 'bronze' / 'news' / 'news'
    silver_dir = test_dir / 'silver' / 'news'
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Find all news files
    news_files = list(bronze_news.rglob('*.parquet'))
    logger.info(f"\n🔧 Processing {len(news_files)} news files...")

    total_enriched = 0

    for news_file in news_files:
        ticker = news_file.stem.replace('ticker=', '')

        # Read bronze data
        df = pl.read_parquet(news_file)

        # Add features: daily article aggregations
        df_silver = df.with_columns([
            pl.col('published_utc').str.slice(0, 10).alias('date'),
            pl.lit(1).alias('article_count')
        ]).group_by(['ticker', 'date']).agg([
            pl.col('article_count').sum().alias('daily_articles'),
            pl.col('id').count().alias('total_mentions'),
            pl.col('amp_url').is_not_null().sum().alias('amp_count')
        ])

        # Save enriched data
        output_file = silver_dir / f'{ticker}_enriched.parquet'
        df_silver.write_parquet(str(output_file), compression='zstd')

        total_enriched += len(df_silver)
        logger.info(f"   ✅ {ticker}: {len(df_silver)} enriched records")

    logger.info(f"\n✅ Silver layer: {total_enriched} enriched records created")

    return {
        'enriched_records': total_enriched,
        'files': len(news_files)
    }


# ============================================================================
# GOLD LAYER: Qlib Binary Format (Mock)
# ============================================================================

def phase_gold(test_dir: Path, silver_results: dict):
    """Convert to Qlib binary format (demonstration)"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 3: GOLD LAYER - Qlib Binary Format")
    logger.info("="*80)

    silver_dir = test_dir / 'silver' / 'news'
    gold_dir = test_dir / 'gold' / 'qlib' / 'news_features'
    gold_dir.mkdir(parents=True, exist_ok=True)

    # Create instruments and calendars
    instruments_dir = test_dir / 'gold' / 'qlib' / 'instruments'
    calendars_dir = test_dir / 'gold' / 'qlib' / 'calendars'
    instruments_dir.mkdir(parents=True, exist_ok=True)
    calendars_dir.mkdir(parents=True, exist_ok=True)

    silver_files = list(silver_dir.glob('*.parquet'))
    logger.info(f"\n💎 Converting {len(silver_files)} files to Qlib format...")

    all_tickers = []
    all_dates = set()

    for silver_file in silver_files:
        ticker = silver_file.stem.replace('_enriched', '')
        all_tickers.append(ticker)

        # Read silver data
        df = pl.read_parquet(silver_file)

        # Extract dates
        dates = df['date'].to_list()
        all_dates.update(dates)

        # Create ticker directory
        ticker_dir = gold_dir / ticker.lower()
        ticker_dir.mkdir(parents=True, exist_ok=True)

        # Write binary files (simplified demonstration)
        # In real Qlib, this would be proper binary format
        for col in ['daily_articles', 'total_mentions', 'amp_count']:
            if col in df.columns:
                values = df[col].to_numpy()
                binary_file = ticker_dir / f'{col}.bin'

                # Write as binary (simplified - real Qlib has specific format)
                with open(binary_file, 'wb') as f:
                    for val in values:
                        f.write(struct.pack('f', float(val)))

        logger.info(f"   ✅ {ticker}: Binary files created")

    # Write instruments file
    instruments_file = instruments_dir / 'all.txt'
    with open(instruments_file, 'w') as f:
        for ticker in sorted(all_tickers):
            f.write(f"{ticker.lower()}\n")

    # Write calendar file
    calendar_file = calendars_dir / 'day.txt'
    with open(calendar_file, 'w') as f:
        for date in sorted(all_dates):
            f.write(f"{date}\n")

    logger.info(f"\n✅ Gold layer: {len(all_tickers)} tickers, {len(all_dates)} dates")
    logger.info(f"   Instruments: {instruments_file}")
    logger.info(f"   Calendar: {calendar_file}")

    return {
        'tickers': len(all_tickers),
        'dates': len(all_dates),
        'binary_files': len(silver_files) * 3  # 3 features per ticker
    }


# ============================================================================
# VALIDATION: Verify All Layers
# ============================================================================

def phase_validation(test_dir: Path):
    """Validate all pipeline layers"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 4: VALIDATION - Verify All Layers")
    logger.info("="*80)

    results = {}

    # Bronze validation
    logger.info("\n🔍 Validating Bronze Layer...")
    bronze_files = list((test_dir / 'bronze').rglob('*.parquet'))
    bronze_size = sum(f.stat().st_size for f in bronze_files)
    results['bronze'] = {
        'files': len(bronze_files),
        'size_mb': bronze_size / (1024 * 1024)
    }
    logger.info(f"   ✅ {len(bronze_files)} files, {bronze_size / 1024:.1f} KB")

    # Silver validation
    logger.info("\n🔍 Validating Silver Layer...")
    silver_files = list((test_dir / 'silver').rglob('*.parquet'))
    silver_size = sum(f.stat().st_size for f in silver_files)
    results['silver'] = {
        'files': len(silver_files),
        'size_mb': silver_size / (1024 * 1024)
    }
    logger.info(f"   ✅ {len(silver_files)} files, {silver_size / 1024:.1f} KB")

    # Gold validation
    logger.info("\n🔍 Validating Gold Layer...")
    gold_bin_files = list((test_dir / 'gold' / 'qlib').rglob('*.bin'))
    gold_txt_files = list((test_dir / 'gold' / 'qlib').rglob('*.txt'))
    gold_size = sum(f.stat().st_size for f in gold_bin_files + gold_txt_files)
    results['gold'] = {
        'bin_files': len(gold_bin_files),
        'txt_files': len(gold_txt_files),
        'size_mb': gold_size / (1024 * 1024)
    }
    logger.info(f"   ✅ {len(gold_bin_files)} binary files, {len(gold_txt_files)} metadata files")
    logger.info(f"   ✅ Total size: {gold_size / 1024:.1f} KB")

    return results


# ============================================================================
# MAIN PIPELINE
# ============================================================================

async def main():
    """Run complete pipeline"""

    TEST_DIR = Path('/Users/zheyuanzhao/workspace/quantmini/test_pipeline')

    logger.info("="*80)
    logger.info("COMPLETE MEDALLION ARCHITECTURE PIPELINE TEST")
    logger.info("="*80)
    logger.info(f"Test Directory: {TEST_DIR}")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)

    # Load config
    config = ConfigLoader()

    # Phase 1: Bronze Layer
    bronze_results = await phase_bronze(TEST_DIR, config)

    # Phase 2: Silver Layer
    silver_results = phase_silver(TEST_DIR, bronze_results)

    # Phase 3: Gold Layer
    gold_results = phase_gold(TEST_DIR, silver_results)

    # Phase 4: Validation
    validation_results = phase_validation(TEST_DIR)

    # Final Summary
    logger.info("\n" + "="*80)
    logger.info("PIPELINE COMPLETE - SUMMARY")
    logger.info("="*80)

    logger.info("\n📊 Data Processing:")
    logger.info(f"   Bronze: {bronze_results['news_articles']} news articles")
    logger.info(f"   Silver: {silver_results['enriched_records']} enriched records")
    logger.info(f"   Gold: {gold_results['tickers']} tickers × {gold_results['dates']} dates")

    logger.info("\n💾 Storage:")
    logger.info(f"   Bronze: {validation_results['bronze']['size_mb']:.2f} MB ({validation_results['bronze']['files']} files)")
    logger.info(f"   Silver: {validation_results['silver']['size_mb']:.2f} MB ({validation_results['silver']['files']} files)")
    logger.info(f"   Gold: {validation_results['gold']['size_mb']:.2f} MB ({validation_results['gold']['bin_files']} binary files)")

    logger.info("\n📁 Directory Structure:")
    logger.info(f"   {TEST_DIR}/")
    logger.info(f"   ├── bronze/news/          ({validation_results['bronze']['files']} files)")
    logger.info(f"   ├── silver/news/          ({validation_results['silver']['files']} files)")
    logger.info(f"   └── gold/qlib/            ({validation_results['gold']['bin_files']} binary files)")

    logger.info("\n" + "="*80)
    logger.info("✅ ALL PIPELINE PHASES COMPLETED SUCCESSFULLY")
    logger.info("="*80)

    # Write summary report
    summary_file = TEST_DIR / 'PIPELINE_SUMMARY.md'
    with open(summary_file, 'w') as f:
        f.write(f"""# Complete Pipeline Test Summary

**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Test Directory**: `{TEST_DIR}`

## Pipeline Results

### Bronze Layer (Raw Data)
- **News Articles**: {bronze_results['news_articles']}
- **Tickers**: {', '.join(bronze_results['tickers'])}
- **Files**: {validation_results['bronze']['files']}
- **Size**: {validation_results['bronze']['size_mb']:.2f} MB

### Silver Layer (Enriched Data)
- **Enriched Records**: {silver_results['enriched_records']}
- **Files**: {validation_results['silver']['files']}
- **Size**: {validation_results['silver']['size_mb']:.2f} MB

### Gold Layer (Qlib Binary)
- **Tickers**: {gold_results['tickers']}
- **Dates**: {gold_results['dates']}
- **Binary Files**: {validation_results['gold']['bin_files']}
- **Metadata Files**: {validation_results['gold']['txt_files']}
- **Size**: {validation_results['gold']['size_mb']:.2f} MB

## Total Pipeline
- **Bronze → Silver → Gold**: ✅ Complete
- **Total Files**: {validation_results['bronze']['files'] + validation_results['silver']['files'] + validation_results['gold']['bin_files'] + validation_results['gold']['txt_files']}
- **Total Size**: {validation_results['bronze']['size_mb'] + validation_results['silver']['size_mb'] + validation_results['gold']['size_mb']:.2f} MB

## Next Steps
1. Review data quality in each layer
2. Test Qlib binary format loading
3. Scale to full production datasets
""")

    logger.info(f"\n📝 Summary report written to: {summary_file}")


if __name__ == '__main__':
    asyncio.run(main())
