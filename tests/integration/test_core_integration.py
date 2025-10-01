"""
Integration tests for core infrastructure

Tests the interaction between SystemProfiler, MemoryMonitor, and ConfigLoader

Run with: pytest tests/integration/test_core_integration.py -v
"""

import pytest
from pathlib import Path
import tempfile
from src.core.system_profiler import SystemProfiler
from src.core.memory_monitor import AdvancedMemoryMonitor
from src.core.config_loader import ConfigLoader


def test_profiler_to_config_integration():
    """Test SystemProfiler generates config that ConfigLoader can use"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Create system profile
        profiler = SystemProfiler(config_dir=config_dir)

        # Load configuration
        config = ConfigLoader(config_dir=config_dir)

        # Verify profile is loaded
        assert config.get('system_profile') is not None
        assert config.get('system_profile.hardware') is not None
        assert config.get('system_profile.recommended_mode') is not None


def test_profiler_to_memory_monitor():
    """Test SystemProfiler limits work with MemoryMonitor"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Create system profile
        profiler = SystemProfiler(config_dir=config_dir)
        limits = profiler.profile['resource_limits']

        # Create memory monitor with those limits
        monitor = AdvancedMemoryMonitor(limits)

        # Verify limits are applied
        assert monitor.max_memory_gb == limits['max_memory_gb']

        # Test memory check works
        status = monitor.check_and_wait()
        assert 'system_percent' in status


def test_config_to_memory_monitor():
    """Test ConfigLoader provides limits to MemoryMonitor"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Create system profile first
        profiler = SystemProfiler(config_dir=config_dir)

        # Load config
        config = ConfigLoader(config_dir=config_dir)
        limits = config.get_resource_limits()

        # Create monitor with config limits
        monitor = AdvancedMemoryMonitor(limits)

        # Verify integration
        assert monitor.max_memory_gb > 0
        assert monitor.check_count == 0

        # Run check
        status = monitor.check_and_wait()
        assert monitor.check_count == 1


def test_full_core_pipeline():
    """Test complete core infrastructure pipeline"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Step 1: Profile system
        profiler = SystemProfiler(config_dir=config_dir)
        assert profiler.profile is not None

        # Step 2: Load configuration
        config = ConfigLoader(config_dir=config_dir)
        assert config.get('system_profile') is not None

        # Step 3: Get recommended mode
        mode = config.get_recommended_mode()
        assert mode in ['streaming', 'batch', 'parallel']

        # Step 4: Get resource limits
        limits = config.get_resource_limits()
        assert limits['max_memory_gb'] > 0

        # Step 5: Create memory monitor
        monitor = AdvancedMemoryMonitor(limits)
        assert monitor.max_memory_gb == limits['max_memory_gb']

        # Step 6: Check memory
        status = monitor.check_and_wait()
        assert status['action'] in ['none', 'gc_gen0', 'gc_full']

        # Step 7: Get memory stats
        stats = monitor.get_memory_stats()
        assert stats['system']['total_gb'] > 0
        assert stats['process']['rss_gb'] >= 0


def test_mode_selection():
    """Test processing mode selection logic"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Create profile
        profiler = SystemProfiler(config_dir=config_dir)

        # Load config
        config = ConfigLoader(config_dir=config_dir)

        # Get mode
        recommended_mode = config.get('system_profile.recommended_mode')
        config_mode = config.get_recommended_mode()

        # In adaptive mode, should use system recommendation
        assert config_mode == recommended_mode


def test_memory_availability_check():
    """Test memory availability checking"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir)

        # Setup
        profiler = SystemProfiler(config_dir=config_dir)
        config = ConfigLoader(config_dir=config_dir)
        limits = config.get_resource_limits()
        monitor = AdvancedMemoryMonitor(limits)

        # Test small memory request (should be available)
        assert monitor.is_memory_available(0.1) is True

        # Test large memory request (may or may not be available)
        result = monitor.is_memory_available(100.0)
        assert isinstance(result, bool)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
