# MyPy Strictness Guide

This guide helps you gradually increase MyPy strictness as your codebase becomes more type-safe.

## Current Configuration (Lenient Start)

We start with a lenient MyPy configuration that provides useful type checking without being overwhelming:

- ✅ **Enabled**: Basic type checking, error codes, pretty output
- ⚠️ **Lenient**: Allows untyped functions, missing imports, optional types
- 🔇 **Module Ignores**: All major modules start with `ignore_errors = true`

## Graduation Path to Strictness

### Phase 1: Foundation (Current)
**Goal**: Get basic type checking working without breaking the build

Current settings in `pyproject.toml`:
```toml
ignore_missing_imports = true
no_strict_optional = true
check_untyped_defs = true
disallow_untyped_calls = false
disallow_untyped_defs = false
```

### Phase 2: Module-by-Module Cleanup
**Goal**: Clean up one module at a time

1. **Pick a module** (start with smallest/newest)
2. **Remove module override**:
   ```toml
   # Remove or comment out:
   [[tool.mypy.overrides]]
   module = "utils.*"
   ignore_errors = true
   ```
3. **Fix errors** in that module
4. **Repeat** for next module

**Recommended order**: `utils` → `config` → `agents` → `registry` → `tools`

### Phase 3: Function-Level Strictness
**Goal**: Require type annotations for new functions

Enable one at a time:
```toml
disallow_untyped_defs = true      # Require type annotations
disallow_incomplete_defs = true   # Require complete type annotations
```

### Phase 4: Call-Site Strictness
**Goal**: Ensure type safety at call sites

```toml
disallow_untyped_calls = true     # No calling untyped functions
warn_return_any = true            # Warn when returning Any
```

### Phase 5: Advanced Strictness
**Goal**: Maximum type safety

Uncomment and enable:
```toml
disallow_any_generics = true      # Require specific generic types
disallow_subclassing_any = true   # No subclassing Any
strict_equality = true            # Strict equality checks
strict_concatenate = true         # Strict string/list operations
warn_unused_ignores = true        # Catch unnecessary # type: ignore
```

### Phase 6: Strict Mode
**Goal**: MyPy strict mode

Replace all individual settings with:
```toml
strict = true
```

## Migration Commands

### Check current status:
```bash
uv run mypy --config-file pyproject.toml .
```

### Test module-specific changes:
```bash
uv run mypy --config-file pyproject.toml utils/
```

### Run with stricter settings (testing):
```bash
uv run mypy --strict utils/
```

## Common Patterns & Fixes

### 1. Missing Type Annotations
**Before:**
```python
def process_data(data):
    return data.strip()
```

**After:**
```python
def process_data(data: str) -> str:
    return data.strip()
```

### 2. Optional Types
**Before:**
```python
def get_config(key):
    return config.get(key)  # Could return None
```

**After:**
```python
from typing import Optional

def get_config(key: str) -> Optional[str]:
    return config.get(key)
```

### 3. Generic Collections
**Before:**
```python
def process_items(items: list):
    return [item.upper() for item in items]
```

**After:**
```python
def process_items(items: list[str]) -> list[str]:
    return [item.upper() for item in items]
```

### 4. Any Type (Gradual Transition)
**Temporary fix:**
```python
from typing import Any

def legacy_function(data: Any) -> Any:  # TODO: Add proper types
    return process_legacy_data(data)
```

**Better:**
```python
from typing import Union

def legacy_function(data: Union[str, dict, int]) -> dict:
    return process_legacy_data(data)
```

## Tracking Progress

### Monitor MyPy Coverage
```bash
# Generate coverage report
uv run mypy --html-report mypy-report .

# View report
open mypy-report/index.html
```

### Metrics to Track
- **Modules** with `ignore_errors = true` (decrease over time)
- **Functions** without type annotations (decrease over time)
- **`Any` types** used (decrease over time)
- **MyPy errors** (should stay manageable as you tighten)

## Tips for Success

1. **Start Small**: Pick the easiest module first
2. **Use Type Comments**: For complex cases, add `# type: ignore[error-code]` with specific error codes
3. **Leverage Type Checkers**: Use IDE type checking alongside MyPy
4. **Regular Reviews**: Make type improvements part of code reviews
5. **Document Progress**: Update this guide with project-specific patterns

## Emergency Rollback

If MyPy becomes too strict too fast:

1. **Add module override**:
   ```toml
   [[tool.mypy.overrides]]
   module = "problematic_module.*"
   ignore_errors = true
   ```

2. **Disable specific checks**:
   ```toml
   disallow_untyped_defs = false  # Temporarily relax
   ```

3. **Add ignore comments**:
   ```python
   result = some_function()  # type: ignore[return-value]
   ```

Remember: The goal is gradual improvement, not perfect types overnight! 🎯
