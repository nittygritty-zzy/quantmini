# Changelog Documentation

This directory contains change logs, update notes, and fix documentation for the QuantMini project.

## Files

### QLIB_BINARY_WRITER_UPDATES.md
Documents the 6 critical fixes made to `src/transform/qlib_binary_writer.py` for Qlib compatibility:
1. Null symbol filtering
2. Tab-separated instruments file format
3. Frequency metadata creation
4. macOS metadata cleanup
5. Date ranges in instruments file
6. Error handling improvements

### QLIB_GYM_WARNING.md
Explains and provides solutions for gym deprecation warnings in Qlib:
- Root cause: Qlib's dependency on deprecated gym library
- Solution: `src/utils/suppress_gym_warnings.py` utility
- Implementation: Monkeypatch sys.modules to redirect gym → gymnasium
- Usage pattern for all Qlib examples

## Purpose

These changelog documents serve as:
- Historical record of critical fixes
- Reference for understanding past decisions
- Documentation for similar issues in the future
- Onboarding material for new contributors

## When to Add New Changelog Files

Create a new changelog document when:
1. Major bug fixes that required significant investigation
2. Breaking changes or API updates
3. Critical compatibility fixes
4. Workarounds for third-party library issues
5. Data format or schema changes

## Naming Convention

Use descriptive names that indicate:
- Component affected: `COMPONENT_TYPE_CHANGE.md`
- Type of change: UPDATE, FIX, MIGRATION, DEPRECATION
- Examples:
  - `QLIB_BINARY_WRITER_UPDATES.md`
  - `QLIB_GYM_WARNING.md`
  - `POLYGON_API_MIGRATION.md` (future)
  - `DATABASE_SCHEMA_CHANGES.md` (future)
