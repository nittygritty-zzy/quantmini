"""
Centralized path configuration for QuantMini.

This module provides a single source of truth for all data lake paths.
Configuration priority (highest to lowest):
1. config/paths.yaml (data_lake_root under active_environment) - ONLY SOURCE
2. Default: ~/quantlake (fallback only)

IMPORTANT: Environment variables are NOT used. All paths must be configured
in config/paths.yaml with the active_environment setting.
"""

import os
from pathlib import Path
from typing import Optional
import logging
import yaml

logger = logging.getLogger(__name__)


def get_quantlake_root() -> Path:
    """
    Get the root path for the quantlake data lake.

    This function resolves the quantlake root path using the following priority:
    1. config/paths.yaml (data_lake_root under active_environment)
    2. Default: ~/quantlake (fallback only if config not found)

    Returns:
        Path: Absolute path to the quantlake root directory

    Example:
        >>> from src.utils.paths import get_quantlake_root
        >>> root = get_quantlake_root()
        >>> print(root)
        PosixPath('/Volumes/990EVOPLUS/quantlake')

        >>> bronze_path = get_quantlake_root() / 'fundamentals' / 'financial_ratios'
        >>> silver_path = get_quantlake_root() / 'silver' / 'financial_ratios'
    """
    # Priority 1: config/paths.yaml (ONLY source of truth)
    try:
        # Find project root (walk up from this file)
        current_file = Path(__file__).resolve()
        project_root = current_file.parent.parent.parent  # src/utils/paths.py -> project root
        paths_config = project_root / 'config' / 'paths.yaml'

        if paths_config.exists():
            with open(paths_config) as f:
                config = yaml.safe_load(f)

            # Get active environment
            active_env = config.get('active_environment', 'production')

            # Get data_lake_root from active environment
            if active_env in config and 'data_lake_root' in config[active_env]:
                data_lake_root = config[active_env]['data_lake_root']
                path = Path(data_lake_root).expanduser().resolve()
                logger.debug(f"Using data_lake_root from config/paths.yaml ({active_env}): {path}")
                return path
            else:
                logger.error(f"Environment '{active_env}' or 'data_lake_root' not found in config/paths.yaml")
        else:
            logger.error(f"Config file not found: {paths_config}")
    except Exception as e:
        logger.error(f"Could not load config/paths.yaml: {e}")

    # Priority 2: Default fallback (should only happen if config is missing)
    default_path = Path.home() / 'quantlake'
    logger.warning(f"Using default quantlake root: {default_path}")
    logger.warning("IMPORTANT: Configure config/paths.yaml with proper data_lake_root")
    return default_path


def get_bronze_path(data_type: Optional[str] = None) -> Path:
    """
    Get bronze layer path, optionally for a specific data type.

    Args:
        data_type: Optional data type (e.g., 'fundamentals', 'corporate_actions', 'news')

    Returns:
        Path: Bronze layer path

    Example:
        >>> bronze = get_bronze_path()
        >>> fundamentals_bronze = get_bronze_path('fundamentals')
    """
    bronze = get_quantlake_root()
    return bronze / data_type if data_type else bronze


def get_silver_path(data_type: Optional[str] = None) -> Path:
    """
    Get silver layer path, optionally for a specific data type.

    Args:
        data_type: Optional data type (e.g., 'financial_ratios', 'corporate_actions')

    Returns:
        Path: Silver layer path

    Example:
        >>> silver = get_silver_path()
        >>> ratios_silver = get_silver_path('financial_ratios')
    """
    silver = get_quantlake_root() / 'silver'
    return silver / data_type if data_type else silver


def get_gold_path(data_type: Optional[str] = None) -> Path:
    """
    Get gold layer path, optionally for a specific data type.

    Args:
        data_type: Optional data type (e.g., 'qlib', 'enriched')

    Returns:
        Path: Gold layer path

    Example:
        >>> gold = get_gold_path()
        >>> qlib_gold = get_gold_path('qlib')
    """
    gold = get_quantlake_root() / 'gold'
    return gold / data_type if data_type else gold


def print_config():
    """Print current path configuration for debugging."""
    root = get_quantlake_root()
    print("=" * 80)
    print("QUANTMINI PATH CONFIGURATION")
    print("=" * 80)
    print(f"Quantlake Root: {root}")
    print(f"Bronze Layer:   {get_bronze_path()}")
    print(f"Silver Layer:   {get_silver_path()}")
    print(f"Gold Layer:     {get_gold_path()}")
    print("=" * 80)
    print("\nEnvironment Variables:")
    print(f"  QUANTLAKE_ROOT: {os.getenv('QUANTLAKE_ROOT', '(not set)')}")
    print(f"  DATA_ROOT:      {os.getenv('DATA_ROOT', '(not set)')}")
    print("=" * 80)


if __name__ == '__main__':
    # Command-line interface for debugging
    print_config()
