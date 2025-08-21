#!/bin/bash

# MyPy Progress Checker
# Shows current MyPy status and helps track graduation progress

set -e

echo "🔍 MyPy Progress Report"
echo "======================"
echo

# Check which modules are currently ignored
echo "📋 Current Module Overrides (ignored modules):"
echo "----------------------------------------------"
grep -A 1 "\[\[tool\.mypy\.overrides\]\]" pyproject.toml | grep "module = " | sed 's/module = /  ❌ /' | tr -d '"'
echo

# Test clean modules
echo "✅ Clean Modules (ready for stricter checking):"
echo "-----------------------------------------------"
for module in config utils main; do
    if [ -d "$module" ] || [ -f "$module.py" ]; then
        if uv run mypy --config-file pyproject.toml "$module" --quiet 2>/dev/null; then
            echo "  ✅ $module/"
        else
            echo "  ⚠️  $module/ (has issues)"
        fi
    fi
done
echo

# Show current strictness level
echo "⚙️  Current Strictness Settings:"
echo "--------------------------------"
echo "  check_untyped_defs: $(grep "check_untyped_defs" pyproject.toml | cut -d'=' -f2 | xargs)"
echo "  disallow_untyped_calls: $(grep "disallow_untyped_calls" pyproject.toml | cut -d'=' -f2 | xargs)"
echo "  disallow_untyped_defs: $(grep "disallow_untyped_defs" pyproject.toml | cut -d'=' -f2 | xargs)"
echo "  warn_return_any: $(grep "warn_return_any" pyproject.toml | cut -d'=' -f2 | xargs)"
echo

# Show next steps
echo "🎯 Next Steps:"
echo "-------------"
echo "1. Pick a clean module from above"
echo "2. Remove its override from pyproject.toml"
echo "3. Run: uv run mypy --config-file pyproject.toml <module>/"
echo "4. Fix any issues that appear"
echo "5. Repeat with next module"
echo
echo "📖 See docs/mypy_strictness_guide.md for detailed progression plan"
