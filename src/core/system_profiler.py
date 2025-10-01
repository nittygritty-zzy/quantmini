"""
System Profiler - Detect hardware capabilities and recommend processing mode

This module profiles the system hardware (CPU, memory, disk) and recommends
the optimal processing mode based on available resources.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import psutil
import platform
from pathlib import Path
from typing import Dict, Any
import yaml


class SystemProfiler:
    """
    Profile system capabilities and recommend processing mode

    Processing Modes:
    - streaming: < 32GB RAM (memory-safe, slower)
    - batch: 32-64GB RAM (moderate memory, moderate speed)
    - parallel: > 64GB RAM (high memory, fastest)
    """

    def __init__(self, config_dir: Path = None):
        """
        Initialize system profiler

        Args:
            config_dir: Directory to save profile (default: config/)
        """
        self.config_dir = config_dir or Path('config')
        self.config_dir.mkdir(parents=True, exist_ok=True)

        self.profile = self._profile_system()
        self._save_profile()

    def _profile_system(self) -> Dict[str, Any]:
        """
        Detect hardware capabilities

        Returns:
            Dictionary with hardware profile and recommendations
        """
        memory = psutil.virtual_memory()

        profile = {
            'hardware': {
                'cpu_cores': psutil.cpu_count(logical=False),
                'cpu_threads': psutil.cpu_count(logical=True),
                'memory_gb': memory.total / (1024**3),
                'available_memory_gb': memory.available / (1024**3),
                'platform': platform.system(),
                'processor': platform.processor(),
                'architecture': platform.machine(),
            },
            'storage': {
                'disk_free_gb': psutil.disk_usage('/').free / (1024**3),
                'disk_total_gb': psutil.disk_usage('/').total / (1024**3),
                'disk_type': self._detect_disk_type(),
            },
            'recommended_mode': self._recommend_mode(memory.total / (1024**3)),
            'resource_limits': self._calculate_limits(memory.total / (1024**3)),
        }

        # Add Apple Silicon detection
        profile['hardware']['is_apple_silicon'] = self._is_apple_silicon()

        return profile

    def _recommend_mode(self, memory_gb: float) -> str:
        """
        Recommend processing mode based on available memory

        Args:
            memory_gb: Total system memory in GB

        Returns:
            Recommended mode: 'streaming', 'batch', or 'parallel'
        """
        if memory_gb < 32:
            return 'streaming'
        elif memory_gb < 64:
            return 'batch'
        else:
            return 'parallel'

    def _calculate_limits(self, memory_gb: float) -> Dict[str, Any]:
        """
        Calculate safe resource limits based on available memory

        Args:
            memory_gb: Total system memory in GB

        Returns:
            Dictionary with resource limits for each mode
        """
        # Leave 20% for OS/other apps
        usable_memory = memory_gb * 0.8

        if memory_gb < 32:
            # Streaming mode limits (conservative)
            return {
                'max_memory_gb': min(14, usable_memory),
                'chunk_size': 10000,
                'max_workers': 2,
                'max_concurrent_downloads': 2,
                'parquet_row_group_size': 50000,
            }
        elif memory_gb < 64:
            # Batch mode limits (moderate)
            return {
                'max_memory_gb': min(40, usable_memory),
                'chunk_size': 50000,
                'max_workers': 4,
                'max_concurrent_downloads': 4,
                'parquet_row_group_size': 100000,
            }
        else:
            # Parallel mode limits (aggressive)
            return {
                'max_memory_gb': usable_memory,
                'chunk_size': 100000,
                'max_workers': min(16, psutil.cpu_count()),
                'max_concurrent_downloads': 8,
                'parquet_row_group_size': 200000,
            }

    def _detect_disk_type(self) -> str:
        """
        Detect if using SSD or HDD

        Returns:
            Disk type: 'SSD', 'HDD', or 'Unknown'
        """
        if platform.system() == 'Darwin':  # macOS
            # Most modern Macs have SSDs
            return 'SSD'
        elif platform.system() == 'Linux':
            # Try to detect via /sys filesystem
            try:
                import subprocess
                result = subprocess.run(
                    ['lsblk', '-d', '-o', 'name,rota'],
                    capture_output=True,
                    text=True
                )
                # ROTA=0 means SSD, ROTA=1 means HDD
                if '0' in result.stdout:
                    return 'SSD'
                elif '1' in result.stdout:
                    return 'HDD'
            except:
                pass

        return 'Unknown'

    def _is_apple_silicon(self) -> bool:
        """
        Detect if running on Apple Silicon (M1/M2/M3)

        Returns:
            True if Apple Silicon, False otherwise
        """
        if platform.system() != 'Darwin':
            return False

        # Check for ARM architecture
        machine = platform.machine().lower()
        processor = platform.processor().lower()

        return 'arm' in machine or 'arm' in processor

    def _save_profile(self):
        """Save profile to YAML file"""
        profile_path = self.config_dir / 'system_profile.yaml'

        with open(profile_path, 'w') as f:
            yaml.dump(self.profile, f, default_flow_style=False, sort_keys=False)

        print(f"✅ System profile saved to: {profile_path}")

    def print_summary(self):
        """Print human-readable summary of system profile"""
        hw = self.profile['hardware']
        storage = self.profile['storage']
        mode = self.profile['recommended_mode']
        limits = self.profile['resource_limits']

        print("\n" + "="*70)
        print("SYSTEM PROFILE")
        print("="*70)

        print(f"\n📊 Hardware:")
        print(f"  Platform: {hw['platform']} ({hw['architecture']})")
        print(f"  Processor: {hw['processor']}")
        if hw['is_apple_silicon']:
            print(f"  🍎 Apple Silicon: YES (M-series chip detected)")
        print(f"  CPU Cores: {hw['cpu_cores']} physical, {hw['cpu_threads']} logical")
        print(f"  Memory: {hw['memory_gb']:.1f} GB total, {hw['available_memory_gb']:.1f} GB available")

        print(f"\n💾 Storage:")
        print(f"  Disk Type: {storage['disk_type']}")
        print(f"  Free Space: {storage['disk_free_gb']:.1f} GB / {storage['disk_total_gb']:.1f} GB")

        print(f"\n⚡ Recommended Mode: {mode.upper()}")
        print(f"  Max Memory: {limits['max_memory_gb']:.1f} GB")
        print(f"  Chunk Size: {limits['chunk_size']:,} records")
        print(f"  Max Workers: {limits['max_workers']}")
        print(f"  Concurrent Downloads: {limits['max_concurrent_downloads']}")

        print("\n" + "="*70 + "\n")

    @staticmethod
    def load_profile(config_dir: Path = None) -> Dict[str, Any]:
        """
        Load existing system profile from file

        Args:
            config_dir: Directory containing profile (default: config/)

        Returns:
            Profile dictionary, or creates new one if not found
        """
        config_dir = config_dir or Path('config')
        profile_path = config_dir / 'system_profile.yaml'

        if profile_path.exists():
            with open(profile_path) as f:
                return yaml.safe_load(f)
        else:
            # Create new profile
            profiler = SystemProfiler(config_dir)
            return profiler.profile


def main():
    """Command-line interface for system profiler"""
    profiler = SystemProfiler()
    profiler.print_summary()


if __name__ == '__main__':
    main()
