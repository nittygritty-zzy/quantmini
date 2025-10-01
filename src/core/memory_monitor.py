"""
Advanced Memory Monitor - Proactive memory management with platform-specific optimizations

This module provides memory monitoring and pressure release for macOS and Linux,
with special optimizations for Apple Silicon.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import gc
import time
import psutil
import platform
import ctypes
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class AdvancedMemoryMonitor:
    """
    Proactive memory management with platform-specific optimizations

    Features:
    - Tiered memory pressure handling (warning/critical)
    - macOS-specific memory release (malloc_trim)
    - Process-level memory limits
    - Detailed memory statistics
    """

    def __init__(self, limits: Dict[str, Any]):
        """
        Initialize memory monitor

        Args:
            limits: Resource limits dict with 'max_memory_gb' key
        """
        self.max_memory_gb = limits.get('max_memory_gb', 14.0)
        self.max_memory_bytes = self.max_memory_gb * (1024**3)

        # Thresholds
        self.warning_threshold = 0.75  # 75% usage
        self.critical_threshold = 0.85  # 85% usage

        # Platform detection
        self.is_macos = platform.system() == 'Darwin'
        self.is_linux = platform.system() == 'Linux'

        # macOS-specific: libc for malloc_trim
        self.libc = None
        if self.is_macos:
            try:
                self.libc = ctypes.CDLL('libc.dylib')
                logger.info("macOS memory optimization enabled")
            except Exception as e:
                logger.warning(f"Could not load libc.dylib: {e}")

        # Statistics
        self.check_count = 0
        self.gc_count = 0
        self.critical_count = 0

    def check_and_wait(self) -> Dict[str, Any]:
        """
        Check memory and take action if needed

        Returns:
            Dictionary with memory status and action taken
        """
        self.check_count += 1

        mem = psutil.virtual_memory()
        process = psutil.Process()
        process_mem_gb = process.memory_info().rss / (1024**3)

        status = {
            'system_percent': mem.percent,
            'process_gb': process_mem_gb,
            'available_gb': mem.available / (1024**3),
            'action': 'none',
            'check_count': self.check_count,
        }

        # Warning level: soft garbage collection
        if mem.percent > (self.warning_threshold * 100):
            gc.collect(generation=0)  # Quick collection (young objects only)
            self.gc_count += 1
            status['action'] = 'gc_gen0'
            logger.debug(f"Memory warning: {mem.percent:.1f}% - Quick GC triggered")

        # Critical level: aggressive cleanup
        if mem.percent > (self.critical_threshold * 100):
            self.critical_count += 1

            # Full garbage collection
            gc.collect()  # All generations

            # macOS-specific memory release
            if self.is_macos and self.libc:
                try:
                    self.libc.malloc_trim(0)
                except Exception as e:
                    logger.warning(f"malloc_trim failed: {e}")

            time.sleep(0.5)  # Brief pause to let system reclaim memory
            status['action'] = 'gc_full'

            logger.warning(
                f"Memory critical: {mem.percent:.1f}% - Aggressive cleanup triggered "
                f"(count: {self.critical_count})"
            )

        # Process-level check
        if process_mem_gb > self.max_memory_gb:
            error_msg = (
                f"Process memory ({process_mem_gb:.1f}GB) "
                f"exceeds limit ({self.max_memory_gb}GB)"
            )
            logger.error(error_msg)
            raise MemoryError(error_msg)

        return status

    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get detailed memory statistics

        Returns:
            Dictionary with comprehensive memory info
        """
        mem = psutil.virtual_memory()
        process = psutil.Process()

        stats = {
            'system': {
                'total_gb': mem.total / (1024**3),
                'available_gb': mem.available / (1024**3),
                'used_gb': mem.used / (1024**3),
                'used_percent': mem.percent,
                'cached_gb': mem.cached / (1024**3) if hasattr(mem, 'cached') else 0,
            },
            'process': {
                'rss_gb': process.memory_info().rss / (1024**3),
                'vms_gb': process.memory_info().vms / (1024**3),
                'percent': process.memory_percent(),
            },
            'limits': {
                'max_memory_gb': self.max_memory_gb,
                'warning_threshold': self.warning_threshold,
                'critical_threshold': self.critical_threshold,
            },
            'statistics': {
                'check_count': self.check_count,
                'gc_count': self.gc_count,
                'critical_count': self.critical_count,
            }
        }

        return stats

    def print_stats(self):
        """Print human-readable memory statistics"""
        stats = self.get_memory_stats()

        print("\n" + "="*70)
        print("MEMORY STATISTICS")
        print("="*70)

        sys = stats['system']
        proc = stats['process']

        print(f"\n💾 System Memory:")
        print(f"  Total: {sys['total_gb']:.1f} GB")
        print(f"  Used: {sys['used_gb']:.1f} GB ({sys['used_percent']:.1f}%)")
        print(f"  Available: {sys['available_gb']:.1f} GB")

        print(f"\n📊 Process Memory:")
        print(f"  RSS: {proc['rss_gb']:.2f} GB")
        print(f"  VMS: {proc['vms_gb']:.2f} GB")
        print(f"  Percent: {proc['percent']:.1f}%")

        print(f"\n🎯 Limits:")
        print(f"  Max Process Memory: {self.max_memory_gb:.1f} GB")
        print(f"  Warning Threshold: {self.warning_threshold * 100:.0f}%")
        print(f"  Critical Threshold: {self.critical_threshold * 100:.0f}%")

        print(f"\n📈 Statistics:")
        print(f"  Checks: {self.check_count}")
        print(f"  GC Triggered: {self.gc_count}")
        print(f"  Critical Events: {self.critical_count}")

        print("\n" + "="*70 + "\n")

    def reset_statistics(self):
        """Reset monitoring statistics"""
        self.check_count = 0
        self.gc_count = 0
        self.critical_count = 0
        logger.info("Memory monitor statistics reset")

    def is_memory_available(self, required_gb: float) -> bool:
        """
        Check if enough memory is available

        Args:
            required_gb: Required memory in GB

        Returns:
            True if memory is available
        """
        mem = psutil.virtual_memory()
        available_gb = mem.available / (1024**3)
        process = psutil.Process()
        process_mem_gb = process.memory_info().rss / (1024**3)

        # Check both system and process limits
        system_ok = available_gb >= required_gb
        process_ok = (process_mem_gb + required_gb) <= self.max_memory_gb

        return system_ok and process_ok

    @staticmethod
    def get_system_memory_info() -> Dict[str, float]:
        """
        Get quick system memory info (static method)

        Returns:
            Dictionary with memory info in GB
        """
        mem = psutil.virtual_memory()
        return {
            'total_gb': mem.total / (1024**3),
            'available_gb': mem.available / (1024**3),
            'used_percent': mem.percent,
        }


class MemoryMonitor(AdvancedMemoryMonitor):
    """
    Alias for backward compatibility
    """
    pass


def main():
    """Command-line interface for memory monitor"""
    # Load system profile to get limits
    from .system_profiler import SystemProfiler

    profile = SystemProfiler.load_profile()
    limits = profile['resource_limits']

    # Create monitor
    monitor = AdvancedMemoryMonitor(limits)

    print(f"Memory Monitor Initialized")
    print(f"Max Process Memory: {monitor.max_memory_gb:.1f} GB")

    # Show current stats
    monitor.print_stats()

    # Test memory check
    print("Testing memory check...")
    status = monitor.check_and_wait()
    print(f"✅ Memory check passed: {status['action']}")

    # Show available memory
    required_gb = 5.0
    if monitor.is_memory_available(required_gb):
        print(f"✅ {required_gb:.1f}GB memory is available")
    else:
        print(f"⚠️  {required_gb:.1f}GB memory is NOT available")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
