# Utils Module (`src.utils`)

Utility functions and helpers.

## Gym Warnings Suppression

**Module**: `src.utils.suppress_gym_warnings`

Suppress Gym deprecation warnings by redirecting to Gymnasium.

### Functions

#### `patch_gym() -> None`
Redirect all 'gym' imports to 'gymnasium'.

```python
from src.utils.suppress_gym_warnings import patch_gym

# Call BEFORE importing qlib
patch_gym()

import qlib
# No more gym deprecation warnings!
```

**Usage:**
- Call once at the start of your script
- Must be called before importing qlib
- Safe to call multiple times
- Works with all qlib functionality

#### `is_patched() -> bool`
Check if gym has been patched.

```python
from src.utils.suppress_gym_warnings import is_patched

if is_patched():
    print("Gym is patched - no warnings!")
```

#### `get_effective_gym_version() -> Optional[str]`
Get version of effective gym module.

```python
from src.utils.suppress_gym_warnings import get_effective_gym_version

version = get_effective_gym_version()
print(f"Using gym version: {version}")
```

### Context Manager

#### `patch_gym_context()`
Context manager version of patch_gym().

```python
from src.utils.suppress_gym_warnings import patch_gym_context

with patch_gym_context():
    import qlib
    # Gym warnings suppressed within context
    qlib.init(...)
```

---

## Complete Example

### Qlib Script Without Warnings

```python
#!/usr/bin/env python3
"""
Example Qlib script with gym warnings suppressed.
"""

# IMPORTANT: Patch gym BEFORE importing qlib
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

import qlib
from qlib.data import D
from qlib.contrib.model.gbdt import LGBModel
import pandas as pd

# Initialize Qlib
qlib.init(provider_uri='data/qlib/stocks_daily', region='us')

# Create model
model = LGBModel(
    learning_rate=0.05,
    n_estimators=100,
    num_leaves=31
)

# Train model (no gym warnings!)
X_train = pd.DataFrame(...)  # Your features
y_train = pd.Series(...)      # Your targets

model.fit(X_train, y_train)

print("Model trained successfully - no warnings!")
```

---

## Why This Is Needed

Qlib uses Gym for its RL strategies, but Gym is deprecated in favor of Gymnasium. When importing qlib, you may see:

```
DeprecationWarning: The gym package is deprecated. Please use gymnasium instead.
```

The `suppress_gym_warnings` module redirects all `gym` imports to `gymnasium`, which is API-compatible and actively maintained.

---

## All Example Scripts Use This

All example scripts in the QuantMini project use this pattern:

```python
# examples/qlib_lgb_example.py
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

import qlib
# ... rest of example
```

This ensures clean output without deprecation warnings.

---

## Technical Details

**How It Works:**
1. Adds an import hook to `sys.meta_path`
2. Intercepts `import gym` statements
3. Redirects to `import gymnasium as gym`
4. Gymnasium is API-compatible with Gym

**Compatibility:**
- Works with all Qlib versions
- Compatible with Gym and Gymnasium
- No side effects on other imports
- Thread-safe

**Limitations:**
- Must be called before importing qlib
- Only affects Python import system
- Does not patch already-imported modules

---

## Related Documentation

See also:
- `docs/changelog/QLIB_GYM_WARNING.md` - Detailed explanation of the gym warning issue
- `examples/` - All examples use this pattern
