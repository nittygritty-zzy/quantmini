"""
Configuration Loader - Load and validate pipeline configuration

This module provides configuration management for the data pipeline,
including validation, defaults, and environment variable overrides.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Load and manage pipeline configuration

    Configuration hierarchy (highest priority first):
    1. Environment variables
    2. User config file (pipeline_config.yaml)
    3. System profile (system_profile.yaml)
    4. Default values
    """

    DEFAULT_CONFIG = {
        'data_root': Path('data'),  # Default data root
        'pipeline': {
            'mode': 'adaptive',
            'data_types': ['stocks_daily', 'stocks_minute', 'options_daily', 'options_minute'],
        },
        'source': {
            's3': {
                'endpoint': 'https://files.polygon.io',
                'bucket': 'flatfiles',
                'max_retries': 5,
                'retry_delay_seconds': 5,
                'timeout_seconds': 60,
            }
        },
        'processing': {
            'enable_validation': True,
            'enable_enrichment': True,
            'enable_binary_conversion': True,
            'gc_frequency': 5,
            'memory_check_interval': 100,
            'memory_threshold_percent': 80,
        },
        'parquet': {
            'compression': 'zstd',
            'compression_level': 3,
            'use_dictionary': True,
            'write_statistics': True,
        },
        'monitoring': {
            'profiling': {
                'enabled': False,
                'output_dir': 'logs/performance',
            },
            'health_check': {
                'enabled': True,
                'check_interval_minutes': 5,
                'data_freshness_threshold_hours': 48,
            }
        },
        'logging': {
            'level': 'INFO',
            'console': {
                'enabled': True,
                'level': 'INFO',
            }
        }
    }

    def __init__(self, config_dir: Path = None, environment: str = None):
        """
        Initialize configuration loader

        Args:
            config_dir: Directory containing config files (default: config/)
            environment: Environment to use ('production', 'test', or None for auto-detect)
        """
        self.config_dir = config_dir or Path('config')
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.environment = environment

        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from all sources

        Returns:
            Merged configuration dictionary
        """
        # Start with defaults
        config = self._deep_copy(self.DEFAULT_CONFIG)

        # Load paths configuration FIRST (highest priority for paths)
        paths_config_path = self.config_dir / 'paths.yaml'
        if paths_config_path.exists():
            with open(paths_config_path) as f:
                paths_config = yaml.safe_load(f)

                # Determine active environment
                active_env = self.environment or paths_config.get('active_environment', 'production')

                # Resolve environment aliases
                env_aliases = paths_config.get('environments', {})
                active_env = env_aliases.get(active_env, active_env)

                # Get environment-specific paths
                if active_env in paths_config:
                    env_paths = paths_config[active_env]
                    config.update(env_paths)
                    logger.info(f"Loaded {active_env} environment paths from {paths_config_path}")
                else:
                    logger.warning(f"Environment '{active_env}' not found in paths.yaml")
        else:
            logger.warning(f"Paths config not found at {paths_config_path}")

        # Load user config (lower priority than paths.yaml)
        pipeline_config_path = self.config_dir / 'pipeline_config.yaml'
        if pipeline_config_path.exists():
            with open(pipeline_config_path) as f:
                user_config = yaml.safe_load(f)
                # Merge but don't override paths from paths.yaml
                for key, value in user_config.items():
                    if key not in ['data_lake_root', 'bronze_path', 'silver_path', 'gold_path', 'metadata_path', 'logs_path']:
                        if key in config and isinstance(config[key], dict) and isinstance(value, dict):
                            config[key] = self._merge_dicts(config[key], value)
                        else:
                            config[key] = value
                logger.info(f"Loaded pipeline config from {pipeline_config_path}")
        else:
            logger.warning(f"Pipeline config not found at {pipeline_config_path}")

        # Load system profile (higher priority - overrides pipeline_config)
        system_profile_path = self.config_dir / 'system_profile.yaml'
        if system_profile_path.exists():
            with open(system_profile_path) as f:
                system_profile = yaml.safe_load(f)
                config['system_profile'] = system_profile
                # Override data_root from system_profile if present
                if 'data_root' in system_profile:
                    config['data_root'] = system_profile['data_root']
                    logger.info(f"Using data_root from system_profile: {system_profile['data_root']}")
                logger.info(f"Loaded system profile from {system_profile_path}")
        else:
            logger.warning(f"System profile not found at {system_profile_path}")

        # Load credentials
        credentials_path = self.config_dir / 'credentials.yaml'
        if credentials_path.exists():
            with open(credentials_path) as f:
                credentials = yaml.safe_load(f)
                config['credentials'] = credentials
                logger.info(f"Loaded credentials from {credentials_path}")
        else:
            logger.warning(f"Credentials not found at {credentials_path}")

        # Apply environment variable overrides
        config = self._apply_env_overrides(config)

        return config

    def _validate_config(self):
        """Validate configuration values"""
        # Check required fields
        if 'system_profile' not in self.config:
            logger.warning("System profile missing - run system profiler first")

        # Validate mode
        valid_modes = ['adaptive', 'streaming', 'batch', 'parallel']
        mode = self.config.get('pipeline', {}).get('mode', 'adaptive')
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode: {mode}. Must be one of {valid_modes}")

        # Validate compression
        valid_compression = ['snappy', 'zstd', 'gzip', 'lz4']
        compression = self.config.get('parquet', {}).get('compression', 'zstd')
        if compression not in valid_compression:
            raise ValueError(f"Invalid compression: {compression}. Must be one of {valid_compression}")

        logger.info("Configuration validation passed")

    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides

        Environment variables:
        - PIPELINE_MODE: Override processing mode
        - MAX_MEMORY_GB: Override max memory
        - LOG_LEVEL: Override log level
        - DATA_ROOT: Override data root path
        """
        # Pipeline mode
        if os.getenv('PIPELINE_MODE'):
            config['pipeline']['mode'] = os.getenv('PIPELINE_MODE')
            logger.info(f"Pipeline mode overridden by env: {os.getenv('PIPELINE_MODE')}")

        # Max memory
        if os.getenv('MAX_MEMORY_GB'):
            try:
                max_memory = float(os.getenv('MAX_MEMORY_GB'))
                if 'system_profile' in config and 'resource_limits' in config['system_profile']:
                    config['system_profile']['resource_limits']['max_memory_gb'] = max_memory
                logger.info(f"Max memory overridden by env: {max_memory}GB")
            except ValueError:
                logger.warning(f"Invalid MAX_MEMORY_GB value: {os.getenv('MAX_MEMORY_GB')}")

        # Log level
        if os.getenv('LOG_LEVEL'):
            config['logging']['level'] = os.getenv('LOG_LEVEL').upper()
            logger.info(f"Log level overridden by env: {os.getenv('LOG_LEVEL')}")

        # Data root
        if os.getenv('DATA_ROOT'):
            config['data_root'] = Path(os.getenv('DATA_ROOT'))
            logger.info(f"Data root overridden by env: {os.getenv('DATA_ROOT')}")

        return config

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value by dot-separated path

        Args:
            key_path: Dot-separated path (e.g., 'pipeline.mode')
            default: Default value if key not found

        Returns:
            Configuration value

        Example:
            mode = config.get('pipeline.mode')
            max_memory = config.get('system_profile.resource_limits.max_memory_gb')
        """
        keys = key_path.split('.')
        value = self.config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def get_resource_limits(self) -> Dict[str, Any]:
        """Get resource limits from system profile"""
        return self.get('system_profile.resource_limits', {})

    def get_recommended_mode(self) -> str:
        """Get recommended processing mode"""
        # Check if mode is overridden in pipeline config
        mode = self.get('pipeline.mode', 'adaptive')

        if mode == 'adaptive':
            # Use system profile recommendation
            return self.get('system_profile.recommended_mode', 'streaming')
        else:
            # Use explicit mode
            return mode

    def get_data_root(self) -> Path:
        """
        Get data root path (DEPRECATED - use get_bronze_path() for new code)

        This returns the legacy data_root for backward compatibility.
        New code should use Medallion Architecture paths:
        - get_bronze_path() for validated Parquet
        - get_silver_path() for enriched data
        - get_gold_path() for production formats

        Returns:
            Path to legacy data root directory
        """
        from src.utils.paths import get_quantlake_root

        # Check if data_root is explicitly configured
        configured_data_root = self.get('data_root')
        if configured_data_root:
            return Path(configured_data_root) if not isinstance(configured_data_root, Path) else configured_data_root

        # Fall back to centralized path configuration
        return get_quantlake_root()

    def get_bronze_path(self) -> Path:
        """
        Get bronze layer path (validated Parquet data)

        Returns:
            Path to bronze directory
        """
        from src.utils.paths import get_bronze_path as utils_get_bronze_path

        bronze_path = self.get('bronze_path')
        if bronze_path:
            return Path(bronze_path)
        # Use data_lake_root/bronze if configured
        data_lake_root = self.get('data_lake_root')
        if data_lake_root:
            return Path(data_lake_root) / 'bronze'
        # Fall back to environment-based path
        return utils_get_bronze_path()

    def get_silver_path(self) -> Path:
        """
        Get silver layer path (enriched data with features)

        Returns:
            Path to silver directory
        """
        from src.utils.paths import get_silver_path as utils_get_silver_path

        silver_path = self.get('silver_path')
        if silver_path:
            return Path(silver_path)
        # Use data_lake_root/silver if configured
        data_lake_root = self.get('data_lake_root')
        if data_lake_root:
            return Path(data_lake_root) / 'silver'
        # Fall back to environment-based path
        return utils_get_silver_path()

    def get_gold_path(self) -> Path:
        """
        Get gold layer path (production-ready ML formats)

        Returns:
            Path to gold directory
        """
        from src.utils.paths import get_gold_path as utils_get_gold_path

        gold_path = self.get('gold_path')
        if gold_path:
            return Path(gold_path)
        # Use data_lake_root/gold if configured
        data_lake_root = self.get('data_lake_root')
        if data_lake_root:
            return Path(data_lake_root) / 'gold'
        # Fall back to environment-based path
        return utils_get_gold_path()

    def get_metadata_path(self) -> Path:
        """
        Get metadata path for watermarks and lineage

        Returns:
            Path to metadata directory
        """
        data_lake_root = self.get('data_lake_root')
        if not data_lake_root:
            raise ValueError("data_lake_root not configured in pipeline_config.yaml")
        return Path(data_lake_root) / 'metadata'

    def get_credentials(self, service: str) -> Optional[Dict[str, Any]]:
        """
        Get credentials for a service

        Args:
            service: Service name (e.g., 'polygon')

        Returns:
            Credentials dictionary or None
        """
        return self.get(f'credentials.{service}')

    def get_environment(self) -> str:
        """
        Get the active environment name

        Returns:
            Environment name ('production', 'test', etc.)
        """
        paths_config_path = self.config_dir / 'paths.yaml'
        if paths_config_path.exists():
            with open(paths_config_path) as f:
                paths_config = yaml.safe_load(f)
                active_env = self.environment or paths_config.get('active_environment', 'production')
                env_aliases = paths_config.get('environments', {})
                return env_aliases.get(active_env, active_env)
        return self.environment or 'production'

    @staticmethod
    def _deep_copy(d: Dict) -> Dict:
        """Deep copy a dictionary"""
        import copy
        return copy.deepcopy(d)

    @staticmethod
    def _merge_dicts(base: Dict, override: Dict) -> Dict:
        """
        Recursively merge two dictionaries

        Args:
            base: Base dictionary
            override: Override dictionary

        Returns:
            Merged dictionary
        """
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigLoader._merge_dicts(result[key], value)
            else:
                result[key] = value

        return result

    def save_config(self, output_path: Path = None):
        """
        Save current configuration to file

        Args:
            output_path: Path to save config (default: config/current_config.yaml)
        """
        output_path = output_path or (self.config_dir / 'current_config.yaml')

        with open(output_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Configuration saved to {output_path}")

    def print_config(self):
        """Print human-readable configuration summary"""
        print("\n" + "="*70)
        print("CONFIGURATION SUMMARY")
        print("="*70)

        # Environment
        print(f"\n🌍 Environment: {self.get_environment().upper()}")
        print(f"  Data Lake: {self.get('data_lake_root', 'Not configured')}")

        # Pipeline settings
        print(f"\n⚙️  Pipeline:")
        print(f"  Mode: {self.get_recommended_mode().upper()}")
        print(f"  Data Types: {', '.join(self.get('pipeline.data_types', []))}")

        # System profile
        if 'system_profile' in self.config:
            limits = self.get_resource_limits()
            print(f"\n📊 Resources:")
            print(f"  Max Memory: {limits.get('max_memory_gb', 'N/A')} GB")
            print(f"  Max Workers: {limits.get('max_workers', 'N/A')}")
            print(f"  Chunk Size: {limits.get('chunk_size', 'N/A'):,}")

        # Processing
        print(f"\n🔧 Processing:")
        print(f"  Validation: {'✓' if self.get('processing.enable_validation') else '✗'}")
        print(f"  Enrichment: {'✓' if self.get('processing.enable_enrichment') else '✗'}")
        print(f"  Binary Conversion: {'✓' if self.get('processing.enable_binary_conversion') else '✗'}")

        # Storage
        print(f"\n💾 Storage:")
        print(f"  Compression: {self.get('parquet.compression').upper()}")
        print(f"  Compression Level: {self.get('parquet.compression_level')}")

        # Credentials
        has_credentials = 'credentials' in self.config
        print(f"\n🔐 Credentials:")
        print(f"  Loaded: {'✓' if has_credentials else '✗'}")

        print("\n" + "="*70 + "\n")


def main():
    """Command-line interface for config loader"""
    import sys

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )

    try:
        # Load configuration
        config = ConfigLoader()

        # Print summary
        config.print_config()

        # Test some gets
        print("Testing configuration access:")
        print(f"  Pipeline mode: {config.get_recommended_mode()}")
        print(f"  Max memory: {config.get('system_profile.resource_limits.max_memory_gb')} GB")
        print(f"  Parquet compression: {config.get('parquet.compression')}")

        print("\n✅ Configuration loaded successfully")

        # Optionally save merged config
        if '--save' in sys.argv:
            config.save_config()
            print("💾 Configuration saved to config/current_config.yaml")

    except Exception as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
