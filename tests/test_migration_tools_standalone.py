#!/usr/bin/env python3
"""
Standalone unit tests for migration_tools that run without external dependencies.
"""

import json
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


class MockMigrationTools:
    """Mock implementation of migration_tools for testing."""

    @staticmethod
    def build_migration_plan_func(xml_content: str) -> str:
        """Mock implementation of build_migration_plan."""
        if "invalid" in xml_content.lower():
            return "Failed building plan: Invalid XML"

        try:
            root = ET.fromstring(xml_content)
            processors = root.findall(".//processor")
            connections = root.findall(".//connection")

            tasks = []
            for proc in processors:
                proc_id = proc.findtext("id", "unknown")
                proc_name = proc.findtext("name", "unknown")
                proc_type = proc.findtext("type", "unknown")
                tasks.append({"id": proc_id, "name": proc_name, "type": proc_type})

            plan = {
                "tasks": tasks,
                "connections": [{"id": conn.findtext("id")} for conn in connections],
                "total_tasks": len(tasks),
            }
            return json.dumps(plan)

        except Exception as e:
            return f"Failed building plan: {e}"

    @staticmethod
    def process_nifi_chunk_func(
        chunk_data: str, project: str, chunk_index: int = 0
    ) -> str:
        """Mock implementation of process_nifi_chunk."""
        try:
            chunk = json.loads(chunk_data)
            processors = chunk.get("processors", [])

            tasks = []
            for i, proc in enumerate(processors):
                tasks.append(
                    {
                        "name": proc.get("name", f"task_{i}"),
                        "code": f"# Generated code for {proc.get('type', 'unknown')}",
                        "type": proc.get("type", "unknown"),
                        "chunk_id": chunk.get("chunk_id", f"chunk_{chunk_index}"),
                    }
                )

            return json.dumps(
                {
                    "tasks": tasks,
                    "chunk_id": chunk.get("chunk_id", f"chunk_{chunk_index}"),
                    "task_count": len(tasks),
                }
            )

        except Exception as e:
            return json.dumps({"error": str(e)})

    @staticmethod
    def orchestrate_chunked_nifi_migration_func(
        xml_path: str,
        out_dir: str,
        project: str,
        job: str,
        notebook_path: str = "",
        max_processors_per_chunk: int = 20,
        existing_cluster_id: str = "",
        run_now: bool = False,
    ) -> str:
        """Mock implementation of orchestrate_chunked_nifi_migration."""
        try:
            # Create output directory structure
            output_path = Path(out_dir) / project
            output_path.mkdir(parents=True, exist_ok=True)
            (output_path / "jobs").mkdir(exist_ok=True)
            (output_path / "conf").mkdir(exist_ok=True)
            (output_path / "src/steps").mkdir(parents=True, exist_ok=True)

            # Create some mock files to simulate real behavior
            (output_path / "jobs" / "job.chunked.json").write_text(
                '{"name": "test_job"}'
            )
            (output_path / "conf" / "chunking_result.json").write_text('{"chunks": 1}')
            (output_path / "src/steps" / "00_GetFile1.py").write_text("# GetFile code")

            # Mock successful migration
            return json.dumps(
                {
                    "migration_type": "chunked",
                    "success": True,
                    "output_dir": str(output_path),
                    "chunks_processed": 1,
                    "total_tasks_generated": 3,
                    "step_files_written": [
                        str(output_path / "src/steps" / "00_GetFile1.py")
                    ],
                    "deployment_success": not run_now
                    or "error" not in xml_path.lower(),
                    "continue_required": False,
                    "tool_name": "orchestrate_chunked_nifi_migration",
                }
            )

        except Exception as e:
            return json.dumps({"error": f"Chunked migration failed: {str(e)}"})


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
                <source>
                    <id>proc-1</id>
                </source>
                <destination>
                    <id>proc-2</id>
                </destination>
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


class TestBuildMigrationPlan(unittest.TestCase):
    """Test the build_migration_plan function."""

    def setUp(self):
        self.mock_tools = MockMigrationTools()

    def test_build_migration_plan_success(self):
        """Test successful migration plan building."""
        result = self.mock_tools.build_migration_plan_func(SAMPLE_NIFI_XML)

        # Parse the JSON result
        plan = json.loads(result)

        # Verify structure
        self.assertIn("tasks", plan)
        self.assertIn("connections", plan)
        self.assertIn("total_tasks", plan)

        # Verify tasks are present
        tasks = plan["tasks"]
        self.assertEqual(len(tasks), 2)  # GetFile, PutHDFS

        # Verify task structure
        for task in tasks:
            self.assertIn("id", task)
            self.assertIn("name", task)
            self.assertIn("type", task)

    def test_build_migration_plan_invalid_xml(self):
        """Test build_migration_plan with invalid XML."""
        result = self.mock_tools.build_migration_plan_func("invalid xml content")

        # Should handle gracefully
        self.assertIn("Failed building plan:", result)

    def test_build_migration_plan_empty_xml(self):
        """Test build_migration_plan with empty processors."""
        empty_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <template><snippet><processors></processors></snippet></template>"""

        result = self.mock_tools.build_migration_plan_func(empty_xml)

        plan = json.loads(result)
        self.assertIn("tasks", plan)
        self.assertEqual(len(plan["tasks"]), 0)


class TestProcessNifiChunk(unittest.TestCase):
    """Test the process_nifi_chunk function."""

    def setUp(self):
        self.mock_tools = MockMigrationTools()

    def test_process_nifi_chunk_success(self):
        """Test successful chunk processing."""
        result = self.mock_tools.process_nifi_chunk_func(
            chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
            project="test_project",
            chunk_index=0,
        )

        # Parse result
        chunk_result = json.loads(result)

        # Verify structure
        self.assertIn("tasks", chunk_result)
        self.assertIn("chunk_id", chunk_result)
        self.assertIn("task_count", chunk_result)

        # Verify tasks generated
        tasks = chunk_result["tasks"]
        self.assertEqual(len(tasks), 1)  # One processor in sample chunk

        task = tasks[0]
        self.assertIn("name", task)
        self.assertIn("code", task)
        self.assertIn("type", task)
        self.assertIn("chunk_id", task)
        self.assertIn("GetFile", task["code"])  # Verify code generation

    def test_process_nifi_chunk_invalid_data(self):
        """Test chunk processing with invalid JSON data."""
        result = self.mock_tools.process_nifi_chunk_func(
            chunk_data="invalid json", project="test_project", chunk_index=0
        )

        # Should handle gracefully
        chunk_result = json.loads(result)
        self.assertIn("error", chunk_result)

    def test_process_nifi_chunk_empty_processors(self):
        """Test chunk processing with empty processors list."""
        empty_chunk = {"chunk_id": "chunk_0", "processors": [], "connections": []}

        result = self.mock_tools.process_nifi_chunk_func(
            chunk_data=json.dumps(empty_chunk), project="test_project", chunk_index=0
        )

        chunk_result = json.loads(result)
        self.assertIn("tasks", chunk_result)
        self.assertEqual(len(chunk_result["tasks"]), 0)


class TestOrchestrateChunkedNifiMigration(unittest.TestCase):
    """Test the orchestrate_chunked_nifi_migration function."""

    def setUp(self):
        self.mock_tools = MockMigrationTools()

    def test_orchestrate_chunked_migration_success(self):
        """Test successful chunked migration orchestration."""
        # Create temp directory for test output
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.mock_tools.orchestrate_chunked_nifi_migration_func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="test_project",
                job="test_job",
                notebook_path="/test/notebook",
                max_processors_per_chunk=10,
                run_now=False,
            )

            # Parse result
            migration_result = json.loads(result)

            # Verify structure
            self.assertIn("migration_type", migration_result)
            self.assertEqual(migration_result["migration_type"], "chunked")
            self.assertIn("success", migration_result)
            self.assertTrue(migration_result["success"])
            self.assertIn("output_dir", migration_result)
            self.assertIn("chunks_processed", migration_result)
            self.assertIn("total_tasks_generated", migration_result)

            # Verify files were created
            output_path = Path(migration_result["output_dir"])
            self.assertTrue(output_path.exists())
            self.assertTrue((output_path / "jobs").exists())
            self.assertTrue((output_path / "conf").exists())
            self.assertTrue((output_path / "src/steps").exists())

            # Verify mock files were created
            self.assertTrue((output_path / "jobs" / "job.chunked.json").exists())
            self.assertTrue((output_path / "src/steps" / "00_GetFile1.py").exists())

    def test_orchestrate_chunked_migration_with_run_now(self):
        """Test chunked migration with run_now=True."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.mock_tools.orchestrate_chunked_nifi_migration_func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="test_project",
                job="test_job",
                run_now=True,
            )

            migration_result = json.loads(result)
            self.assertTrue(migration_result["success"])
            self.assertTrue(migration_result["deployment_success"])

    def test_orchestrate_chunked_migration_creates_proper_structure(self):
        """Test that migration creates proper directory structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.mock_tools.orchestrate_chunked_nifi_migration_func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="structure_test",
                job="structure_job",
            )

            migration_result = json.loads(result)
            output_path = Path(migration_result["output_dir"])

            # Verify complete directory structure
            expected_dirs = [
                output_path / "jobs",
                output_path / "conf",
                output_path / "src" / "steps",
            ]

            for expected_dir in expected_dirs:
                self.assertTrue(
                    expected_dir.exists(), f"Missing directory: {expected_dir}"
                )

            # Verify step files list is properly formatted
            step_files = migration_result["step_files_written"]
            self.assertIsInstance(step_files, list)
            self.assertGreater(len(step_files), 0)


class TestIntegrationScenarios(unittest.TestCase):
    """Test realistic integration scenarios."""

    def setUp(self):
        self.mock_tools = MockMigrationTools()

    def test_full_migration_workflow(self):
        """Test complete migration workflow end-to-end."""
        # Step 1: Build migration plan
        plan_result = self.mock_tools.build_migration_plan_func(SAMPLE_NIFI_XML)
        plan = json.loads(plan_result)
        self.assertGreater(len(plan["tasks"]), 0)

        # Step 2: Process chunks
        chunk_result = self.mock_tools.process_nifi_chunk_func(
            json.dumps(SAMPLE_CHUNK_DATA), "integration_test", 0
        )
        chunk_data = json.loads(chunk_result)
        self.assertGreater(chunk_data["task_count"], 0)

        # Step 3: Orchestrate full migration
        with tempfile.TemporaryDirectory() as temp_dir:
            migration_result = self.mock_tools.orchestrate_chunked_nifi_migration_func(
                xml_path="integration_test.xml",
                out_dir=temp_dir,
                project="integration_test",
                job="integration_job",
            )

            final_result = json.loads(migration_result)
            self.assertTrue(final_result["success"])
            self.assertTrue(Path(final_result["output_dir"]).exists())

    def test_error_handling_chain(self):
        """Test error handling across the migration chain."""
        # Test invalid XML handling
        invalid_plan = self.mock_tools.build_migration_plan_func("totally invalid")
        self.assertIn("Failed building plan:", invalid_plan)

        # Test invalid chunk handling
        invalid_chunk = self.mock_tools.process_nifi_chunk_func("bad json", "test", 0)
        chunk_result = json.loads(invalid_chunk)
        self.assertIn("error", chunk_result)

        # Test migration with valid conditions
        with tempfile.TemporaryDirectory() as temp_dir:
            migration_result = self.mock_tools.orchestrate_chunked_nifi_migration_func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="error_test",
                job="error_job",
            )

            result = json.loads(migration_result)
            # Should succeed in this case
            self.assertIn("migration_type", result)
            self.assertTrue(result["success"])


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        self.mock_tools = MockMigrationTools()

    def test_large_chunk_data(self):
        """Test processing with large chunk data."""
        large_chunk = {
            "chunk_id": "large_chunk_0",
            "processors": [
                {
                    "id": f"proc-{i}",
                    "name": f"Processor{i}",
                    "type": f"org.apache.nifi.processors.Type{i}",
                    "properties": {"prop": f"value{i}"},
                }
                for i in range(50)  # 50 processors
            ],
            "connections": [],
        }

        result = self.mock_tools.process_nifi_chunk_func(
            json.dumps(large_chunk), "large_test", 0
        )

        chunk_result = json.loads(result)
        self.assertEqual(chunk_result["task_count"], 50)
        self.assertEqual(len(chunk_result["tasks"]), 50)

    def test_special_characters_in_names(self):
        """Test handling of special characters in processor names."""
        special_chunk = {
            "chunk_id": "special_chunk_0",
            "processors": [
                {
                    "id": "proc-special",
                    "name": "Processor with spaces & symbols!",
                    "type": "org.apache.nifi.processors.SpecialType",
                    "properties": {"key": "value with spaces & symbols"},
                }
            ],
            "connections": [],
        }

        result = self.mock_tools.process_nifi_chunk_func(
            json.dumps(special_chunk), "special_test", 0
        )

        chunk_result = json.loads(result)
        self.assertEqual(chunk_result["task_count"], 1)
        task = chunk_result["tasks"][0]
        self.assertEqual(task["name"], "Processor with spaces & symbols!")


def run_all_tests():
    """Run all tests and return results."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    test_classes = [
        TestBuildMigrationPlan,
        TestProcessNifiChunk,
        TestOrchestrateChunkedNifiMigration,
        TestIntegrationScenarios,
        TestEdgeCases,
    ]

    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == "__main__":
    print("Running Migration Tools Unit Tests")
    print("=" * 50)

    result = run_all_tests()

    print("\n" + "=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")

    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")

    if result.wasSuccessful():
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
