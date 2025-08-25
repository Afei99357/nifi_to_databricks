#!/usr/bin/env python3
"""
Test runner script for nifi_to_databricks project.
"""

import subprocess
import sys
from pathlib import Path


def run_tests():
    """Run all tests with appropriate options."""
    project_root = Path(__file__).parent

    # Change to project directory
    import os

    os.chdir(project_root)

    # Run pytest with coverage if available
    cmd = [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--color=yes"]

    try:
        # Try to run with coverage
        subprocess.run(
            [sys.executable, "-m", "pytest", "--version"],
            check=True,
            capture_output=True,
        )
        print("Running tests...")
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except FileNotFoundError:
        print("pytest not found. Please install test dependencies:")
        print("  uv add --dev pytest pytest-mock pytest-asyncio")
        return 1
    except subprocess.CalledProcessError:
        print("pytest not installed. Please install test dependencies:")
        print("  uv add --dev pytest pytest-mock pytest-asyncio")
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)
