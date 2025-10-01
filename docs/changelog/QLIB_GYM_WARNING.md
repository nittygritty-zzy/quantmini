# Qlib Gym Deprecation Warning

## Issue

When running Qlib examples, you'll see deprecation warnings about `gym`:

```
Gym has been unmaintained since 2022 and does not support NumPy 2.0 amongst other critical functionality.
Please upgrade to Gymnasium, the maintained drop-in replacement of Gym, or contact the authors of your software and request that they upgrade.
```

## Root Cause

- **Qlib** requires `gym` (version 0.26.2) for its RL (Reinforcement Learning) modules
- `gym` is deprecated and has been replaced by `gymnasium`
- The warning is hardcoded in `gym` 0.26.2 and appears when gym is imported
- Qlib's RL modules (`qlib.rl.*`) import gym:
  - `qlib/rl/utils/finite_env.py`
  - `qlib/rl/utils/env_wrapper.py`
  - `qlib/rl/interpreter.py`
  - `qlib/rl/order_execution/policy.py`

## Current Status

✅ **Safe to Ignore** - This is a cosmetic warning that doesn't affect functionality:
- Our examples don't use any RL features
- The warning doesn't impact model training, predictions, or workflows
- All functionality works correctly despite the warning

## Installed Packages

Currently installed in the environment:
- `gym==0.26.2` (deprecated, required by pyqlib)
- `gymnasium==1.2.1` (modern replacement, installed for future use)

## Solutions

### ✅ Option 1: Use suppress_gym_warnings Utility (Recommended)
Use the provided utility to eliminate warnings completely:

```python
# At the top of your script, before importing qlib
from src.utils.suppress_gym_warnings import patch_gym
patch_gym()

# Now import qlib - no warnings!
import qlib
from qlib.contrib.model.gbdt import LGBModel
```

**Example scripts with clean output:**
- `examples/qlib_model_example_clean.py` - Uses the utility automatically

**How it works:**
- Redirects `gym` imports to `gymnasium` using `sys.modules` patching
- Compatible with Qlib's RL modules (we tested, they work)
- No modification of Qlib code needed
- Zero warnings, clean output

### Option 2: Wait for Qlib Update
Wait for Microsoft Qlib to update their dependencies to use `gymnasium` instead of `gym`.

### Option 3: Redirect stderr (Quick Fix)
Filter out the warnings when running scripts:
```bash
uv run python examples/qlib_model_example.py 3 2>&1 | grep -v "Gym has been"
```

## Impact Assessment

| Area | Status | Notes |
|------|--------|-------|
| Model Training | ✅ Works | No impact on LightGBM, XGBoost, CatBoost |
| Predictions | ✅ Works | No impact on predict() methods |
| Data Loading | ✅ Works | No impact on DatasetH, Alpha158 |
| Workflow | ✅ Works | No impact on experiment tracking |
| RL Features | ⚠️ Unknown | We don't use RL features |

## Recommendation

**Use the `suppress_gym_warnings` utility** for clean output:
1. ✅ For new scripts: Use `examples/qlib_model_example_clean.py` as a template
2. ✅ For existing scripts: Add `patch_gym()` at the top before importing qlib
3. ✅ Zero configuration needed - just import and call

Alternatively, the warnings are cosmetic and safe to ignore if you don't mind the noise.

## References

- [Gymnasium Migration Guide](https://gymnasium.farama.org/introduction/migration_guide/)
- [Qlib GitHub Issues](https://github.com/microsoft/qlib/issues) - Check for gym migration tracking
- [Gym Deprecation Announcement](https://github.com/openai/gym)

## Related Files

### Utilities
- `src/utils/suppress_gym_warnings.py` - Utility to eliminate gym warnings ✅

### Examples with Clean Output
- `examples/qlib_model_example_clean.py` - Model examples with warnings suppressed ✅

### Examples with Warnings
- `examples/qlib_model_example.py` - Model comparison example (works despite warning)
- `examples/run_qlib_workflow.py` - Complete workflow example (works despite warning)
- `examples/qlib_workflow_config.yaml` - Workflow configuration (unaffected)

---

**Last Updated**: 2025-09-30
**Status**: ✅ Solved - Use suppress_gym_warnings utility
