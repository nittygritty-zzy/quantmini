"""
Unit tests for SystemProfiler

Run with: pytest tests/unit/test_system_profiler.py
"""

import pytest
from pathlib import Path
import tempfile
import yaml
from src.core.system_profiler import SystemProfiler


def test_system_profiler_creation():
    """Test SystemProfiler initialization"""
    with tempfile.TemporaryDirectory() as tmpdir:
        profiler = SystemProfiler(config_dir=Path(tmpdir))

        # Check profile was created
        assert profiler.profile is not None
        assert 'hardware' in profiler.profile
        assert 'storage' in profiler.profile
        assert 'recommended_mode' in profiler.profile
        assert 'resource_limits' in profiler.profile


def test_hardware_detection():
    """Test hardware detection"""
    with tempfile.TemporaryDirectory() as tmpdir:
        profiler = SystemProfiler(config_dir=Path(tmpdir))
        hw = profiler.profile['hardware']

        # Check required fields
        assert 'cpu_cores' in hw
        assert 'cpu_threads' in hw
        assert 'memory_gb' in hw
        assert 'platform' in hw
        assert 'processor' in hw
        assert 'is_apple_silicon' in hw

        # Validate values
        assert hw['cpu_cores'] > 0
        assert hw['cpu_threads'] >= hw['cpu_cores']
        assert hw['memory_gb'] > 0


def test_mode_recommendation():
    """Test processing mode recommendation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        profiler = SystemProfiler(config_dir=Path(tmpdir))
        mode = profiler.profile['recommended_mode']

        # Should be one of the valid modes
        assert mode in ['streaming', 'batch', 'parallel']


def test_resource_limits():
    """Test resource limit calculation"""
    with tempfile.TemporaryDirectory() as tmpdir:
        profiler = SystemProfiler(config_dir=Path(tmpdir))
        limits = profiler.profile['resource_limits']

        # Check required fields
        assert 'max_memory_gb' in limits
        assert 'chunk_size' in limits
        assert 'max_workers' in limits
        assert 'max_concurrent_downloads' in limits
        assert 'parquet_row_group_size' in limits

        # Validate values
        assert limits['max_memory_gb'] > 0
        assert limits['chunk_size'] > 0
        assert limits['max_workers'] > 0


def test_profile_save():
    """Test profile is saved to YAML"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)
        profiler = SystemProfiler(config_dir=config_dir)

        profile_path = config_dir / 'system_profile.yaml'
        assert profile_path.exists()

        # Load and verify
        with open(profile_path) as f:
            loaded = yaml.safe_load(f)

        assert loaded == profiler.profile


def test_load_profile():
    """Test loading existing profile"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Create profile
        profiler1 = SystemProfiler(config_dir=config_dir)

        # Load profile
        loaded = SystemProfiler.load_profile(config_dir=config_dir)

        assert loaded == profiler1.profile


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
