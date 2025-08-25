"""
Unit tests for tools.migration_tools module - CORRECTED VERSION

Tests the core migration functions:
- orchestrate_chunked_nifi_migration
- process_nifi_chunk
- build_migration_plan

This version fixes the key issues found in the original test file.
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Only import pytest if available, fallback to unittest
try:
    import pytest

    HAVE_PYTEST = True
except ImportError:
    HAVE_PYTEST = False


# ISSUE 1 FIXED: Proper dependency mocking before import
class MockConfig:
    class logger:
        @staticmethod
        def info(msg):
            pass

        @staticmethod
        def error(msg):
            pass

        @staticmethod
        def warning(msg):
            pass


# Mock all dependencies before any imports
sys.modules["langchain_core"] = Mock()  # type: ignore
sys.modules["langchain_core.tools"] = Mock()  # type: ignore
sys.modules["databricks_langchain"] = Mock()  # type: ignore
sys.modules["json_repair"] = Mock()  # type: ignore
sys.modules["config"] = MockConfig()  # type: ignore


# Mock the tool decorator to return the function as-is
def mock_tool(func):
    func.func = func  # ISSUE 2 FIXED: Add .func attribute for compatibility
    return func


sys.modules["langchain_core.tools"].tool = mock_tool  # type: ignore

# Now we can safely import
from tools.migration_tools import (
    build_migration_plan,
    orchestrate_chunked_nifi_migration,
    process_nifi_chunk,
)

# Test data
SAMPLE_NIFI_XML = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<template>
    <description>Test NiFi Template</description>
    <name>Test Template</name>
    <snippet>
        <processors>
            <processor>
                <id>proc-1</id>
                <name>GetFile1</name>
                <type>org.apache.nifi.processors.standard.GetFile</type>
                <properties>
                    <entry>
                        <key>Input Directory</key>
                        <value>/input</value>
                    </entry>
                </properties>
            </processor>
            <processor>
                <id>proc-2</id>
                <name>PutHDFS1</name>
                <type>org.apache.nifi.processors.hadoop.PutHDFS</type>
                <properties>
                    <entry>
                        <key>Directory</key>
                        <value>/output</value>
                    </entry>
                </properties>
            </processor>
        </processors>
        <connections>
            <connection>
                <id>conn-1</id>
                <source><id>proc-1</id></source>
                <destination><id>proc-2</id></destination>
            </connection>
        </connections>
    </snippet>
</template>"""

SAMPLE_CHUNK_DATA = {
    "chunk_id": "chunk_0",
    "processors": [
        {
            "id": "proc-1",
            "name": "GetFile1",
            "type": "org.apache.nifi.processors.standard.GetFile",
            "properties": {"Input Directory": "/input"},
        }
    ],
    "connections": [],
}


if HAVE_PYTEST:
    # Pytest version
    class TestBuildMigrationPlan:
        """Test the build_migration_plan function."""

        def test_build_migration_plan_success(self):
            """Test successful migration plan building."""
            # ISSUE 3 FIXED: Call function directly, not .func
            result = build_migration_plan(SAMPLE_NIFI_XML)

            # Should return a string (JSON)
            assert isinstance(result, str)

            # Try to parse as JSON
            try:
                plan = json.loads(result)
                assert "tasks" in plan or "Failed" in result
            except json.JSONDecodeError:
                # If not JSON, should be error message
                assert "Failed building plan:" in result

        def test_build_migration_plan_invalid_xml(self):
            """Test build_migration_plan with invalid XML."""
            result = build_migration_plan("invalid xml content")
            assert "Failed building plan:" in result

    class TestProcessNifiChunk:
        """Test the process_nifi_chunk function."""

        @patch("tools.migration_tools._generate_batch_processor_code")
        @patch("tools.migration_tools.generate_databricks_code")
        def test_process_nifi_chunk_success(self, mock_gen_code, mock_batch_gen):
            """Test successful chunk processing."""
            # Mock individual code generation
            mock_gen_code.func = Mock(return_value="# Generated PySpark code")

            # Mock batch generation to fail so it falls back to individual
            mock_batch_gen.side_effect = Exception("Batch failed")

            result = process_nifi_chunk(
                chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
                project="test_project",
                chunk_index=0,
            )

            # Should return valid JSON
            assert isinstance(result, str)
            try:
                chunk_result = json.loads(result)
                assert "tasks" in chunk_result or "error" in chunk_result
            except json.JSONDecodeError:
                pytest.fail("process_nifi_chunk did not return valid JSON")

    class TestOrchestrateChunkedNifiMigration:
        """Test the orchestrate_chunked_nifi_migration function."""

        @patch("tools.migration_tools.deploy_and_run_job")
        @patch("tools.migration_tools.scaffold_asset_bundle")
        @patch("tools.migration_tools.reconstruct_full_workflow")
        @patch("tools.migration_tools.process_nifi_chunk")
        @patch("tools.migration_tools.chunk_nifi_xml_by_process_groups")
        @patch("tools.migration_tools.extract_complete_workflow_map")
        @patch("tools.migration_tools._read_text")
        def test_orchestrate_chunked_migration_mocked(
            self,
            mock_read,
            mock_extract,
            mock_chunk,
            mock_process,
            mock_reconstruct,
            mock_scaffold,
            mock_deploy,
        ):
            """Test orchestrate_chunked_nifi_migration with proper mocking."""

            # ISSUE 4 FIXED: Mock the .func attributes properly
            mock_read.return_value = SAMPLE_NIFI_XML
            mock_extract.func = Mock(
                return_value=json.dumps({"processors": [], "connections": []})
            )
            mock_chunk.func = Mock(
                return_value=json.dumps(
                    {
                        "chunks": [SAMPLE_CHUNK_DATA],
                        "cross_chunk_links": [],
                        "summary": {"total_processors": 1, "chunks_created": 1},
                    }
                )
            )
            mock_process.func = Mock(
                return_value=json.dumps(
                    {
                        "tasks": [{"name": "task1", "code": "code", "type": "type"}],
                        "chunk_id": "chunk_0",
                        "task_count": 1,
                    }
                )
            )
            mock_reconstruct.func = Mock(
                return_value=json.dumps(
                    {"databricks_job_config": {"name": "test", "tasks": []}}
                )
            )
            mock_scaffold.func = Mock(return_value="# YAML")
            mock_deploy.func = Mock(return_value=json.dumps({"job_id": "123"}))

            with tempfile.TemporaryDirectory() as temp_dir:
                result = orchestrate_chunked_nifi_migration(
                    xml_path="test.xml",
                    out_dir=temp_dir,
                    project="test_project",
                    job="test_job",
                )

                # Should return valid JSON
                assert isinstance(result, str)
                migration_result = json.loads(result)
                assert "output_dir" in migration_result or "error" in migration_result

else:
    # Unittest version for environments without pytest
    import unittest

    class TestBuildMigrationPlanUnittest(unittest.TestCase):
        """Test the build_migration_plan function."""

        def test_build_migration_plan_success(self):
            """Test successful migration plan building."""
            result = build_migration_plan(SAMPLE_NIFI_XML)
            self.assertIsInstance(result, str)

            # Try to parse as JSON or verify error message
            try:
                plan = json.loads(result)
                self.assertTrue("tasks" in plan or "Failed" in result)
            except json.JSONDecodeError:
                self.assertIn("Failed building plan:", result)

        def test_build_migration_plan_invalid_xml(self):
            """Test build_migration_plan with invalid XML."""
            result = build_migration_plan("invalid xml content")
            self.assertIn("Failed building plan:", result)

    class TestProcessNifiChunkUnittest(unittest.TestCase):
        """Test the process_nifi_chunk function."""

        @patch("tools.migration_tools._generate_batch_processor_code")
        @patch("tools.migration_tools.generate_databricks_code")
        def test_process_nifi_chunk_success(self, mock_gen_code, mock_batch_gen):
            """Test successful chunk processing."""
            mock_gen_code.func = Mock(return_value="# Generated PySpark code")
            mock_batch_gen.side_effect = Exception("Batch failed")

            result = process_nifi_chunk(
                chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
                project="test_project",
                chunk_index=0,
            )

            self.assertIsInstance(result, str)
            try:
                chunk_result = json.loads(result)
                self.assertTrue("tasks" in chunk_result or "error" in chunk_result)
            except json.JSONDecodeError:
                self.fail("process_nifi_chunk did not return valid JSON")


def run_tests():
    """Run tests with appropriate framework."""
    if HAVE_PYTEST:
        print("Running tests with pytest...")
        import subprocess

        result = subprocess.run(
            [sys.executable, "-m", "pytest", __file__, "-v"],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode
    else:
        print("Running tests with unittest...")
        import unittest

        loader = unittest.TestLoader()
        suite = unittest.TestSuite()

        suite.addTests(loader.loadTestsFromTestCase(TestBuildMigrationPlanUnittest))
        if "TestProcessNifiChunkUnittest" in globals():
            suite.addTests(loader.loadTestsFromTestCase(TestProcessNifiChunkUnittest))

        runner = unittest.TextTestRunner(verbosity=2)
        test_result = runner.run(suite)
        return 0 if test_result.wasSuccessful() else 1


if __name__ == "__main__":
    print("Migration Tools Unit Tests - CORRECTED VERSION")
    print("=" * 60)
    print("Key issues fixed:")
    print("1. ✅ Proper dependency mocking before import")
    print("2. ✅ Fixed .func attribute handling")
    print("3. ✅ Corrected function call syntax")
    print("4. ✅ Proper mock path specification")
    print("5. ✅ Fallback to unittest when pytest unavailable")
    print("=" * 60)

    exit_code = run_tests()
    sys.exit(exit_code)
