#!/usr/bin/env python3
"""
Test Silver Layer - Feature Engineering

Demonstrates feature engineering on test data by adding technical indicators.

Usage:
    uv run python scripts/tests/test_silver_layer.py
"""

import sys
import logging
import polars as pl
from pathlib import Path
from datetime import date

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def calculate_technical_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate technical indicators for stock data.

    Adds simple moving averages and returns as a demonstration.
    """
    df = df.with_columns([
        # Simple Moving Averages (if we had OHLCV data)
        # For news data, we'll add aggregation features
        pl.col('published_utc').alias('published_date'),
        pl.lit(1).alias('article_count')
    ])

    # Group by date and ticker to count articles per day
    df_agg = df.group_by(['ticker', 'published_utc']).agg([
        pl.col('article_count').sum().alias('daily_article_count'),
        pl.col('id').count().alias('total_articles'),
        pl.col('amp_url').is_not_null().sum().alias('amp_available')
    ])

    return df_agg


def main():
    """Run Silver layer feature engineering test"""

    # Use project-relative path instead of absolute path
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    TEST_DIR = PROJECT_ROOT / 'test_pipeline'
    BRONZE_DIR = TEST_DIR / 'bronze' / 'news' / 'news'
    SILVER_DIR = TEST_DIR / 'silver' / 'news'

    logger.info("=" * 80)
    logger.info("SILVER LAYER TEST - Feature Engineering")
    logger.info("=" * 80)
    logger.info(f"Bronze input: {BRONZE_DIR}")
    logger.info(f"Silver output: {SILVER_DIR}")
    logger.info("=" * 80)

    # Create silver directory
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # Find all news parquet files
    news_files = list(BRONZE_DIR.rglob('*.parquet'))

    if not news_files:
        logger.error("❌ No news files found in bronze layer")
        logger.error(f"   Expected location: {BRONZE_DIR}")
        return

    logger.info(f"\n📊 Found {len(news_files)} news files in bronze layer")

    # Process each file
    total_articles = 0
    total_enriched = 0

    for news_file in news_files:
        ticker = news_file.stem.replace('ticker=', '')
        logger.info(f"\n🔧 Processing {ticker}...")

        # Read bronze data
        df_bronze = pl.read_parquet(news_file)
        articles_count = len(df_bronze)
        total_articles += articles_count

        logger.info(f"   Bronze: {articles_count} articles")

        # Add features
        df_silver = calculate_technical_indicators(df_bronze)
        enriched_count = len(df_silver)
        total_enriched += enriched_count

        logger.info(f"   Silver: {enriched_count} aggregated records")

        # Save to silver layer
        output_file = SILVER_DIR / f'{ticker}_enriched.parquet'
        df_silver.write_parquet(str(output_file), compression='zstd')

        logger.info(f"   ✅ Saved to {output_file.name}")

        # Show sample
        logger.info(f"\n   Sample enriched data for {ticker}:")
        logger.info(df_silver.head(3))

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("SILVER LAYER SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Bronze articles processed: {total_articles}")
    logger.info(f"Silver records created: {total_enriched}")
    logger.info(f"Enriched files: {len(news_files)}")
    logger.info(f"Output directory: {SILVER_DIR}")

    # Verify silver layer
    silver_files = list(SILVER_DIR.glob('*.parquet'))
    logger.info(f"\n✅ Silver layer files created: {len(silver_files)}")

    for sf in silver_files:
        size_kb = sf.stat().st_size / 1024
        logger.info(f"   - {sf.name} ({size_kb:.1f} KB)")

    logger.info("\n" + "=" * 80)
    logger.info("✅ SILVER LAYER TEST COMPLETE")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
