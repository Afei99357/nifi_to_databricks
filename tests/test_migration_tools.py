"""
Unit tests for tools.migration_tools module.

Tests the core migration functions:
- orchestrate_chunked_nifi_migration
- process_nifi_chunk
- build_migration_plan
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Mock dependencies before importing
with patch.dict(
    "sys.modules",
    {
        "langchain_core.tools": Mock(),
        "databricks_langchain": Mock(),
        "json_repair": Mock(),
        "config": Mock(),
    },
):
    from tools.migration_tools import (
        build_migration_plan,
        orchestrate_chunked_nifi_migration,
        process_nifi_chunk,
    )


# Sample NiFi XML for testing
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
                    <entry>
                        <key>File Filter</key>
                        <value>.*\\.csv</value>
                    </entry>
                </properties>
            </processor>
            <processor>
                <id>proc-2</id>
                <name>UpdateAttribute1</name>
                <type>org.apache.nifi.processors.attributes.UpdateAttribute</type>
                <properties>
                    <entry>
                        <key>filename</key>
                        <value>processed_${filename}</value>
                    </entry>
                </properties>
            </processor>
            <processor>
                <id>proc-3</id>
                <name>PutHDFS1</name>
                <type>org.apache.nifi.processors.hadoop.PutHDFS</type>
                <properties>
                    <entry>
                        <key>Hadoop Configuration Resources</key>
                        <value>/etc/hadoop/conf/core-site.xml</value>
                    </entry>
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
                <name>GetFile to UpdateAttribute</name>
                <source>
                    <id>proc-1</id>
                </source>
                <destination>
                    <id>proc-2</id>
                </destination>
                <selectedRelationships>
                    <relationship>success</relationship>
                </selectedRelationships>
            </connection>
            <connection>
                <id>conn-2</id>
                <name>UpdateAttribute to PutHDFS</name>
                <source>
                    <id>proc-2</id>
                </source>
                <destination>
                    <id>proc-3</id>
                </destination>
                <selectedRelationships>
                    <relationship>success</relationship>
                </selectedRelationships>
            </connection>
        </connections>
    </snippet>
</template>"""

# Sample chunk data for testing process_nifi_chunk
SAMPLE_CHUNK_DATA = {
    "chunk_id": "chunk_0",
    "processors": [
        {
            "id": "proc-1",
            "name": "GetFile1",
            "type": "org.apache.nifi.processors.standard.GetFile",
            "properties": {"Input Directory": "/input", "File Filter": ".*\\.csv"},
        },
        {
            "id": "proc-2",
            "name": "UpdateAttribute1",
            "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
            "properties": {"filename": "processed_${filename}"},
        },
    ],
    "connections": [
        {
            "id": "conn-1",
            "source": {"id": "proc-1"},
            "destination": {"id": "proc-2"},
            "relationships": ["success"],
        }
    ],
}


class TestBuildMigrationPlan:
    """Test the build_migration_plan function."""

    def test_build_migration_plan_success(self):
        """Test successful migration plan building."""
        result = build_migration_plan.func(SAMPLE_NIFI_XML)

        # Parse the JSON result
        plan = json.loads(result)

        # Verify structure
        assert "tasks" in plan
        assert "connections" in plan
        assert "total_tasks" in plan

        # Verify tasks are present
        tasks = plan["tasks"]
        assert len(tasks) == 3  # GetFile, UpdateAttribute, PutHDFS

        # Verify task structure
        for task in tasks:
            assert "id" in task
            assert "name" in task
            assert "type" in task

        # Verify connections
        connections = plan["connections"]
        assert len(connections) == 2

    def test_build_migration_plan_empty_xml(self):
        """Test build_migration_plan with empty/invalid XML."""
        result = build_migration_plan.func("<invalid>xml</invalid>")

        # Should handle gracefully and return error or empty plan
        assert result  # Should not crash

    def test_build_migration_plan_no_processors(self):
        """Test build_migration_plan with XML containing no processors."""
        xml_no_processors = """<?xml version="1.0" encoding="UTF-8"?>
        <template>
            <snippet>
                <processors>
                </processors>
            </snippet>
        </template>"""

        result = build_migration_plan.func(xml_no_processors)
        plan = json.loads(result)

        assert "tasks" in plan
        assert len(plan["tasks"]) == 0


class TestProcessNifiChunk:
    """Test the process_nifi_chunk function."""

    @patch("tools.migration_tools.generate_databricks_code")
    def test_process_nifi_chunk_success(self, mock_generate_code):
        """Test successful chunk processing."""
        # Mock the code generation
        mock_generate_code.func.return_value = (
            "# Generated PySpark code\ndf = spark.read.csv('/input')"
        )

        result = process_nifi_chunk.func(
            chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
            project="test_project",
            chunk_index=0,
        )

        # Parse result
        chunk_result = json.loads(result)

        # Verify structure
        assert "tasks" in chunk_result
        assert "chunk_id" in chunk_result
        assert "task_count" in chunk_result

        # Verify tasks generated
        tasks = chunk_result["tasks"]
        assert len(tasks) == 2  # Two processors in sample chunk

        for task in tasks:
            assert "name" in task
            assert "code" in task
            assert "type" in task
            assert "chunk_id" in task

        # Verify code generation was called
        assert mock_generate_code.func.call_count == 2

    @patch("tools.migration_tools.generate_databricks_code")
    def test_process_nifi_chunk_with_batch_generation(self, mock_generate_code):
        """Test chunk processing with batch code generation."""
        # Mock batch code generation
        mock_batch_result = [
            {
                "id": "proc-1",
                "name": "GetFile1",
                "type": "org.apache.nifi.processors.standard.GetFile",
                "code": "# Batch generated GetFile code",
                "properties": {"Input Directory": "/input"},
                "chunk_id": "chunk_0",
                "processor_index": 0,
            },
            {
                "id": "proc-2",
                "name": "UpdateAttribute1",
                "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                "code": "# Batch generated UpdateAttribute code",
                "properties": {"filename": "processed_${filename}"},
                "chunk_id": "chunk_0",
                "processor_index": 1,
            },
        ]

        with patch(
            "tools.migration_tools._generate_batch_processor_code"
        ) as mock_batch:
            mock_batch.return_value = mock_batch_result

            result = process_nifi_chunk.func(
                chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
                project="test_project",
                chunk_index=0,
            )

            chunk_result = json.loads(result)
            assert len(chunk_result["tasks"]) == 2
            assert chunk_result["tasks"][0]["code"] == "# Batch generated GetFile code"

    def test_process_nifi_chunk_invalid_data(self):
        """Test chunk processing with invalid JSON data."""
        result = process_nifi_chunk.func(
            chunk_data="invalid json", project="test_project", chunk_index=0
        )

        # Should handle gracefully
        chunk_result = json.loads(result)
        assert "error" in chunk_result or "tasks" in chunk_result

    def test_process_nifi_chunk_empty_processors(self):
        """Test chunk processing with empty processors list."""
        empty_chunk = {"chunk_id": "chunk_0", "processors": [], "connections": []}

        result = process_nifi_chunk.func(
            chunk_data=json.dumps(empty_chunk), project="test_project", chunk_index=0
        )

        chunk_result = json.loads(result)
        assert "tasks" in chunk_result
        assert len(chunk_result["tasks"]) == 0


class TestOrchestrateChekedNifiMigration:
    """Test the orchestrate_chunked_nifi_migration function."""

    @patch("tools.migration_tools.deploy_and_run_job")
    @patch("tools.migration_tools.scaffold_asset_bundle")
    @patch("tools.migration_tools.reconstruct_full_workflow")
    @patch("tools.migration_tools.process_nifi_chunk")
    @patch("tools.migration_tools.chunk_nifi_xml_by_process_groups")
    @patch("tools.migration_tools.extract_complete_workflow_map")
    @patch("tools.migration_tools._read_text")
    def test_orchestrate_chunked_migration_success(
        self,
        mock_read_text,
        mock_extract_workflow,
        mock_chunk_xml,
        mock_process_chunk,
        mock_reconstruct,
        mock_scaffold_bundle,
        mock_deploy_job,
    ):
        """Test successful chunked migration orchestration."""

        # Mock file reading
        mock_read_text.return_value = SAMPLE_NIFI_XML

        # Mock workflow extraction
        mock_extract_workflow.func.return_value = json.dumps(
            {
                "processors": [{"id": "proc-1", "name": "GetFile1"}],
                "connections": [{"source": "proc-1", "destination": "proc-2"}],
            }
        )

        # Mock chunking
        mock_chunk_xml.func.return_value = json.dumps(
            {
                "chunks": [SAMPLE_CHUNK_DATA],
                "cross_chunk_links": [],
                "summary": {"total_processors": 2, "chunks_created": 1},
            }
        )

        # Mock chunk processing
        mock_process_chunk.func.return_value = json.dumps(
            {
                "tasks": [
                    {"name": "GetFile1", "code": "# GetFile code", "type": "GetFile"},
                    {
                        "name": "UpdateAttribute1",
                        "code": "# UpdateAttribute code",
                        "type": "UpdateAttribute",
                    },
                ],
                "chunk_id": "chunk_0",
                "task_count": 2,
            }
        )

        # Mock workflow reconstruction
        mock_reconstruct.func.return_value = json.dumps(
            {
                "databricks_job_config": {
                    "name": "test_job",
                    "tasks": [
                        {
                            "task_key": "task_1",
                            "notebook_task": {"notebook_path": "/test"},
                        },
                        {
                            "task_key": "task_2",
                            "notebook_task": {"notebook_path": "/test"},
                        },
                    ],
                }
            }
        )

        # Mock asset bundle
        mock_scaffold_bundle.func.return_value = "# Asset bundle YAML"

        # Mock deployment
        mock_deploy_job.func.return_value = json.dumps(
            {"job_id": "12345", "run_id": "67890"}
        )

        # Create temp directory for test output
        with tempfile.TemporaryDirectory() as temp_dir:
            result = orchestrate_chunked_nifi_migration.func(
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
            assert "migration_type" in migration_result
            assert migration_result["migration_type"] == "chunked"
            assert "success" in migration_result
            assert "output_dir" in migration_result
            assert "chunks_processed" in migration_result
            assert "total_tasks_generated" in migration_result

            # Verify files were created
            output_path = Path(migration_result["output_dir"])
            assert output_path.exists()
            assert (output_path / "jobs").exists()
            assert (output_path / "conf").exists()
            assert (output_path / "src/steps").exists()

    @patch("tools.migration_tools._read_text")
    def test_orchestrate_chunked_migration_file_not_found(self, mock_read_text):
        """Test orchestrate_chunked_migration with missing XML file."""
        mock_read_text.side_effect = FileNotFoundError("File not found")

        with tempfile.TemporaryDirectory() as temp_dir:
            result = orchestrate_chunked_nifi_migration.func(
                xml_path="nonexistent.xml",
                out_dir=temp_dir,
                project="test_project",
                job="test_job",
            )

            # Should handle error gracefully
            migration_result = json.loads(result)
            assert "error" in migration_result

    @patch("tools.migration_tools._read_text")
    @patch("tools.migration_tools.extract_complete_workflow_map")
    def test_orchestrate_chunked_migration_workflow_extraction_failure(
        self, mock_extract_workflow, mock_read_text
    ):
        """Test orchestrate_chunked_migration with workflow extraction failure."""
        mock_read_text.return_value = SAMPLE_NIFI_XML
        mock_extract_workflow.func.return_value = json.dumps(
            {"error": "Failed to extract workflow"}
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            result = orchestrate_chunked_nifi_migration.func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="test_project",
                job="test_job",
            )

            migration_result = json.loads(result)
            assert "error" in migration_result
            assert "Workflow map extraction failed" in migration_result["error"]

    @patch("tools.migration_tools.deploy_and_run_job")
    @patch("tools.migration_tools.scaffold_asset_bundle")
    @patch("tools.migration_tools.reconstruct_full_workflow")
    @patch("tools.migration_tools.process_nifi_chunk")
    @patch("tools.migration_tools.chunk_nifi_xml_by_process_groups")
    @patch("tools.migration_tools.extract_complete_workflow_map")
    @patch("tools.migration_tools._read_text")
    def test_orchestrate_chunked_migration_with_run_now(
        self,
        mock_read_text,
        mock_extract_workflow,
        mock_chunk_xml,
        mock_process_chunk,
        mock_reconstruct,
        mock_scaffold_bundle,
        mock_deploy_job,
    ):
        """Test chunked migration with run_now=True."""

        # Setup all mocks similar to success test
        mock_read_text.return_value = SAMPLE_NIFI_XML
        mock_extract_workflow.func.return_value = json.dumps(
            {"processors": [], "connections": []}
        )
        mock_chunk_xml.func.return_value = json.dumps(
            {
                "chunks": [SAMPLE_CHUNK_DATA],
                "cross_chunk_links": [],
                "summary": {"total_processors": 2, "chunks_created": 1},
            }
        )
        mock_process_chunk.func.return_value = json.dumps(
            {
                "tasks": [{"name": "task1", "code": "code", "type": "type"}],
                "chunk_id": "chunk_0",
                "task_count": 1,
            }
        )
        mock_reconstruct.func.return_value = json.dumps(
            {"databricks_job_config": {"name": "test_job", "tasks": []}}
        )
        mock_scaffold_bundle.func.return_value = "# YAML"
        mock_deploy_job.func.return_value = json.dumps(
            {"job_id": "12345", "run_id": "67890"}
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            result = orchestrate_chunked_nifi_migration.func(
                xml_path="test.xml",
                out_dir=temp_dir,
                project="test_project",
                job="test_job",
                run_now=True,
            )

            # Verify deploy_and_run_job was called with run_now=True
            mock_deploy_job.func.assert_called_once()
            call_args = mock_deploy_job.func.call_args
            assert call_args[1]["run_now"] is True


class TestEnvironmentVariables:
    """Test environment variable handling."""

    @patch.dict("os.environ", {"MAX_PROCESSORS_PER_CHUNK": "15"})
    def test_max_processors_per_chunk_env_override(self):
        """Test that environment variable overrides default chunk size."""
        # This would be tested through orchestrate_chunked_nifi_migration
        # The actual test would verify that the chunking uses the env value
        pass

    @patch.dict("os.environ", {"ENABLE_LLM_CODE_GENERATION": "false"})
    def test_llm_code_generation_disabled(self):
        """Test behavior when LLM code generation is disabled."""
        # This would test the fallback to individual processor generation
        pass


class TestErrorHandling:
    """Test error handling in migration tools."""

    def test_invalid_xml_handling(self):
        """Test handling of completely invalid XML."""
        invalid_xml = "not xml at all"
        result = build_migration_plan.func(invalid_xml)

        # Should handle gracefully without crashing
        assert "Failed building plan:" in result or json.loads(result)

    @patch("tools.migration_tools.generate_databricks_code")
    def test_code_generation_failure_handling(self, mock_generate_code):
        """Test handling when code generation fails."""
        mock_generate_code.func.side_effect = Exception("Code generation failed")

        result = process_nifi_chunk.func(
            chunk_data=json.dumps(SAMPLE_CHUNK_DATA),
            project="test_project",
            chunk_index=0,
        )

        # Should handle error gracefully
        chunk_result = json.loads(result)
        # Either contains error or fallback behavior
        assert "error" in chunk_result or "tasks" in chunk_result


if __name__ == "__main__":
    pytest.main([__file__])
