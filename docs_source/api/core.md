# Core Module (`src.core`)

System profiling, memory monitoring, configuration management, and exception handling.

## ConfigLoader

**Module**: `src.core.config_loader`

Configuration manager with hierarchical loading from environment variables, user config, system profile, and defaults.

### Class: `ConfigLoader`

```python
ConfigLoader(config_dir: Path = None)
```

**Parameters:**
- `config_dir`: Configuration directory (default: `config/`)

**Key Methods:**

#### `get(key_path: str, default: Any = None) -> Any`
Get configuration value by dot-separated path.

```python
config = ConfigLoader()
mode = config.get('pipeline.mode', 'adaptive')
```

#### `get_resource_limits() -> Dict[str, Any]`
Get resource limits from system profile.

#### `get_recommended_mode() -> str`
Get recommended processing mode based on system resources.

Returns: `'streaming'`, `'batch'`, or `'parallel'`

#### `get_data_root() -> Path`
Get data root directory path.

#### `get_credentials(service: str) -> Optional[Dict[str, Any]]`
Get credentials for a service.

```python
polygon_creds = config.get_credentials('polygon')
# Returns: {'access_key_id': '...', 'secret_access_key': '...'}
```

#### `save_config(output_path: Path = None)`
Save current merged configuration to file.

#### `print_config()`
Print human-readable configuration summary.

**Configuration Hierarchy:**
1. Environment variables (highest priority)
2. User config (`config/pipeline_config.yaml`)
3. System profile (`config/system_profile.yaml`)
4. Built-in defaults (lowest priority)

---

## Memory Monitoring

**Module**: `src.core.memory_monitor`

Proactive memory management with platform-specific optimizations.

### Class: `AdvancedMemoryMonitor`

Alias: `MemoryMonitor`

```python
AdvancedMemoryMonitor(limits: Dict[str, Any])
```

**Parameters:**
- `limits`: Dictionary with `'max_memory_gb'` key

**Key Methods:**

#### `check_and_wait() -> Dict[str, Any]`
Check memory and take action if needed.

Returns status dictionary with action taken:
- `'none'`: No action needed
- `'gc'`: Garbage collection performed  
- `'wait'`: Waited for memory to free up

```python
monitor = AdvancedMemoryMonitor({'max_memory_gb': 24.0})
status = monitor.check_and_wait()
if status['action'] != 'none':
    print(f"Memory action: {status['action']}")
```

#### `get_memory_stats() -> Dict[str, Any]`
Get detailed memory statistics.

Returns:
- `system`: System memory info (total, available, percent)
- `process`: Process memory info (rss, vms, percent)
- `limits`: Configured limits
- `statistics`: Monitoring counters

#### `print_stats()`
Print human-readable memory statistics.

#### `reset_statistics()`
Reset monitoring statistics counters.

#### `is_memory_available(required_gb: float) -> bool`
Check if enough memory is available for operation.

**Static Methods:**

#### `get_system_memory_info() -> Dict[str, float]`
Get quick system memory info in GB.

**Memory Thresholds:**
- Warning: 75% usage → trigger garbage collection
- Critical: 85% usage → wait for memory to free up

**Platform Support:**
- macOS: Uses `malloc_trim` for efficient memory release
- Linux: Standard memory management
- Windows: Basic support

---

## System Profiling

**Module**: `src.core.system_profiler`

Detect hardware capabilities and recommend optimal processing mode.

### Class: `SystemProfiler`

```python
SystemProfiler(config_dir: Path = None)
```

**Parameters:**
- `config_dir`: Directory to save profile (default: `config/`)

**Key Methods:**

#### `print_summary()`
Print human-readable system profile summary.

**Static Methods:**

#### `load_profile(config_dir: Path = None) -> Dict[str, Any]`
Load existing profile or create new one.

```python
profile = SystemProfiler.load_profile()
print(f"Recommended mode: {profile['recommended_mode']}")
print(f"Memory: {profile['hardware']['memory_gb']:.1f} GB")
```

**Profile Structure:**
```python
{
    'hardware': {
        'cpu_cores': int,
        'cpu_logical': int,
        'memory_gb': float,
        'platform': str,
        'processor': str,
        'is_apple_silicon': bool
    },
    'storage': {
        'total_gb': float,
        'free_gb': float,
        'disk_type': str
    },
    'recommended_mode': str,  # 'streaming', 'batch', or 'parallel'
    'resource_limits': {
        'max_memory_gb': float,
        'max_workers': int
    }
}
```

**Processing Mode Recommendations:**
- **streaming**: < 32GB RAM (memory-safe, slower)
- **batch**: 32-64GB RAM (moderate speed)
- **parallel**: > 64GB RAM (fastest)

---

## Exceptions

**Module**: `src.core.exceptions`

Custom exceptions for error handling throughout the pipeline.

### Exception Hierarchy

```python
PipelineException (base)
├── ConfigurationError
├── MemoryLimitExceeded
├── S3DownloadError
├── DataValidationError
├── IngestionError
├── FeatureEngineeringError
├── BinaryConversionError
└── WatermarkError
```

**Usage:**
```python
from src.core.exceptions import ConfigurationError, MemoryLimitExceeded

try:
    if memory_usage > limit:
        raise MemoryLimitExceeded(f"Memory usage {memory_usage} exceeds limit {limit}")
except MemoryLimitExceeded as e:
    print(f"Error: {e}")
```

---

## Environment Variables

The following environment variables can override configuration:

- `PIPELINE_MODE`: Override processing mode (`streaming`, `batch`, `parallel`)
- `MAX_MEMORY_GB`: Override max memory limit
- `LOG_LEVEL`: Override log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `DATA_ROOT`: Override data root directory path

**Example:**
```bash
export PIPELINE_MODE=streaming
export MAX_MEMORY_GB=16
python scripts/run_pipeline.py
```
