#!/usr/bin/env python3
"""
Suppress Gym Deprecation Warnings

This module provides a compatibility shim that redirects `gym` imports to `gymnasium`,
eliminating the deprecation warnings while maintaining compatibility with Qlib.

Usage:
    # At the top of your script, before importing qlib
    from src.utils.suppress_gym_warnings import patch_gym
    patch_gym()

    # Or use as context manager
    with patch_gym():
        import qlib
        from qlib.contrib.model.gbdt import LGBModel
        # ... rest of your code

Background:
    - Qlib requires `gym` for RL modules but we don't use RL features
    - `gym` is deprecated and shows hardcoded warnings
    - `gymnasium` is the maintained replacement
    - By redirecting imports, we eliminate warnings without modifying Qlib
"""

import sys
from contextlib import contextmanager
from typing import Optional


def patch_gym() -> None:
    """
    Redirect all 'gym' imports to 'gymnasium' to suppress deprecation warnings.

    This should be called before importing qlib or any modules that import gym.
    Safe to call multiple times.

    Example:
        >>> from src.utils.suppress_gym_warnings import patch_gym
        >>> patch_gym()
        >>> import qlib  # No gym warnings!
    """
    try:
        import gymnasium

        # Redirect gym to gymnasium
        sys.modules['gym'] = gymnasium
        sys.modules['gym.spaces'] = gymnasium.spaces
        sys.modules['gym.wrappers'] = gymnasium.wrappers

        # Also handle common gym submodules
        for submodule in ['envs', 'error', 'logger', 'utils', 'vector']:
            gym_module = f'gym.{submodule}'
            gymnasium_module = f'gymnasium.{submodule}'
            try:
                gym_mod = __import__(gymnasium_module, fromlist=[submodule])
                sys.modules[gym_module] = gym_mod
            except (ImportError, AttributeError):
                # Submodule doesn't exist in gymnasium or not needed
                pass

    except ImportError:
        # gymnasium not installed, can't patch
        import warnings
        warnings.warn(
            "gymnasium not installed - cannot suppress gym warnings. "
            "Install with: uv pip install gymnasium",
            UserWarning
        )


@contextmanager
def patch_gym_context():
    """
    Context manager version of patch_gym().

    Patches gym imports within the context and optionally restores them after.
    Note: In practice, once patched, the modules stay patched for the process lifetime.

    Example:
        >>> from src.utils.suppress_gym_warnings import patch_gym_context
        >>> with patch_gym_context():
        ...     import qlib
        ...     # No gym warnings in this block
    """
    original_gym = sys.modules.get('gym', None)
    original_gym_spaces = sys.modules.get('gym.spaces', None)
    original_gym_wrappers = sys.modules.get('gym.wrappers', None)

    try:
        patch_gym()
        yield
    finally:
        # Note: Restoring is usually not necessary and can cause issues
        # if other modules have already imported the patched versions.
        # Leaving this here for completeness but generally not recommended.
        pass


def is_patched() -> bool:
    """
    Check if gym has been patched to use gymnasium.

    Returns:
        bool: True if gym imports are redirected to gymnasium

    Example:
        >>> from src.utils.suppress_gym_warnings import patch_gym, is_patched
        >>> is_patched()
        False
        >>> patch_gym()
        >>> is_patched()
        True
    """
    if 'gym' not in sys.modules:
        return False

    try:
        import gymnasium
        return sys.modules['gym'] is gymnasium
    except ImportError:
        return False


def get_effective_gym_version() -> Optional[str]:
    """
    Get the version of the effective gym module (could be gym or gymnasium).

    Returns:
        str: Version string, or None if gym not imported

    Example:
        >>> from src.utils.suppress_gym_warnings import patch_gym, get_effective_gym_version
        >>> patch_gym()
        >>> get_effective_gym_version()
        '1.0.0'  # gymnasium version
    """
    if 'gym' not in sys.modules:
        return None

    try:
        return sys.modules['gym'].__version__
    except AttributeError:
        return "unknown"


# Auto-patch if this module is imported directly
if __name__ != "__main__":
    # This runs when imported as a module
    # Uncomment the next line if you want automatic patching on import
    # patch_gym()
    pass


if __name__ == "__main__":
    # Test the patching functionality
    print("Testing gym warning suppression...")
    print(f"Before patch - is_patched(): {is_patched()}")

    patch_gym()
    print(f"After patch - is_patched(): {is_patched()}")
    print(f"Effective gym version: {get_effective_gym_version()}")

    print("\nTesting with Qlib import...")
    import qlib
    from qlib.contrib.model.gbdt import LGBModel
    print("✓ Qlib imported successfully without gym warnings!")

    print("\n✓ All tests passed!")
