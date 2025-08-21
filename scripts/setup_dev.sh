#!/bin/bash

# Development setup script for NiFi to Databricks migration tool

set -e

echo "🚀 Setting up development environment..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Please install uv first:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

echo "📦 Installing dependencies..."
uv sync --group dev

echo "🔧 Installing pre-commit hooks..."
uv run pre-commit install

echo "✅ Development environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Run 'uv run pre-commit run --all-files' to check all files"
echo "2. Check MyPy progress: './scripts/mypy_progress.sh'"
echo "3. Start coding! Pre-commit hooks will run automatically on commits"
echo ""
echo "📖 See docs/mypy_strictness_guide.md for gradual type safety improvement"
