"""
Unit tests for AdvancedMemoryMonitor

Run with: pytest tests/unit/test_memory_monitor.py
"""

import pytest
from src.core.memory_monitor import AdvancedMemoryMonitor


def test_memory_monitor_creation():
    """Test AdvancedMemoryMonitor initialization"""
    limits = {'max_memory_gb': 10.0}
    monitor = AdvancedMemoryMonitor(limits)

    assert monitor.max_memory_gb == 10.0
    assert monitor.warning_threshold == 0.75
    assert monitor.critical_threshold == 0.85


def test_check_and_wait():
    """Test memory check"""
    limits = {'max_memory_gb': 100.0}  # High limit to avoid triggering
    monitor = AdvancedMemoryMonitor(limits)

    status = monitor.check_and_wait()

    assert 'system_percent' in status
    assert 'process_gb' in status
    assert 'available_gb' in status
    assert 'action' in status
    assert monitor.check_count == 1


def test_get_memory_stats():
    """Test memory statistics retrieval"""
    limits = {'max_memory_gb': 10.0}
    monitor = AdvancedMemoryMonitor(limits)

    stats = monitor.get_memory_stats()

    # Check structure
    assert 'system' in stats
    assert 'process' in stats
    assert 'limits' in stats
    assert 'statistics' in stats

    # Check system stats
    assert 'total_gb' in stats['system']
    assert 'available_gb' in stats['system']
    assert 'used_percent' in stats['system']

    # Check process stats
    assert 'rss_gb' in stats['process']
    assert 'percent' in stats['process']


def test_is_memory_available():
    """Test memory availability check"""
    limits = {'max_memory_gb': 100.0}
    monitor = AdvancedMemoryMonitor(limits)

    # Should have memory available for small requirement
    assert monitor.is_memory_available(1.0) is True

    # May or may not have memory for very large requirement
    result = monitor.is_memory_available(1000.0)
    assert isinstance(result, bool)


def test_reset_statistics():
    """Test statistics reset"""
    limits = {'max_memory_gb': 100.0}
    monitor = AdvancedMemoryMonitor(limits)

    # Generate some stats
    monitor.check_and_wait()
    monitor.check_and_wait()

    assert monitor.check_count == 2

    # Reset
    monitor.reset_statistics()

    assert monitor.check_count == 0
    assert monitor.gc_count == 0
    assert monitor.critical_count == 0


def test_get_system_memory_info():
    """Test static method for quick memory info"""
    info = AdvancedMemoryMonitor.get_system_memory_info()

    assert 'total_gb' in info
    assert 'available_gb' in info
    assert 'used_percent' in info

    assert info['total_gb'] > 0
    assert info['available_gb'] >= 0
    assert 0 <= info['used_percent'] <= 100


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
