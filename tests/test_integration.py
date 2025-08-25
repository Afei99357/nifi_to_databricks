"""
Integration tests for migration tools using real NiFi XML files.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tools.migration_tools import build_migration_plan


@pytest.mark.integration
class TestIntegrationWithRealFiles:
    """Integration tests using actual NiFi XML files from the project."""

    def test_build_migration_plan_with_real_xml(self):
        """Test build_migration_plan with actual project XML files."""
        project_root = Path(__file__).parent.parent
        nifi_files = list((project_root / "nifi_pipeline_file").glob("*.xml"))

        if not nifi_files:
            pytest.skip("No NiFi XML files found in nifi_pipeline_file/")

        # Test with the first available XML file
        xml_file = nifi_files[0]

        with open(xml_file, "r", encoding="utf-8") as f:
            xml_content = f.read()

        # Run the migration plan builder
        result = build_migration_plan.func(xml_content)

        # Should not crash and should return valid JSON
        assert result
        plan = json.loads(result)

        # Basic structure validation
        assert "tasks" in plan
        assert "connections" in plan
        assert isinstance(plan["tasks"], list)
        assert isinstance(plan["connections"], list)

        print(f"✅ Successfully processed {xml_file.name}")
        print(f"   Tasks found: {len(plan['tasks'])}")
        print(f"   Connections: {len(plan['connections'])}")

    def test_all_project_xml_files(self):
        """Test that all project XML files can be parsed successfully."""
        project_root = Path(__file__).parent.parent
        xml_files = list((project_root / "nifi_pipeline_file").glob("*.xml"))

        if not xml_files:
            pytest.skip("No NiFi XML files found")

        for xml_file in xml_files:
            with open(xml_file, "r", encoding="utf-8") as f:
                xml_content = f.read()

            try:
                result = build_migration_plan.func(xml_content)
                plan = json.loads(result)

                # Basic validation
                assert "tasks" in plan
                print(f"✅ {xml_file.name}: {len(plan['tasks'])} tasks")

            except Exception as e:
                pytest.fail(f"Failed to process {xml_file.name}: {e}")


@pytest.mark.slow
class TestPerformance:
    """Performance tests for migration tools."""

    def test_large_xml_processing_time(self):
        """Test processing time for larger XML files."""
        import time

        project_root = Path(__file__).parent.parent
        xml_files = list((project_root / "nifi_pipeline_file").glob("*.xml"))

        if not xml_files:
            pytest.skip("No XML files for performance testing")

        # Find the largest XML file
        largest_file = max(xml_files, key=lambda f: f.stat().st_size)

        with open(largest_file, "r", encoding="utf-8") as f:
            xml_content = f.read()

        start_time = time.time()
        result = build_migration_plan.func(xml_content)
        end_time = time.time()

        processing_time = end_time - start_time

        # Should complete within reasonable time (adjust threshold as needed)
        assert (
            processing_time < 30.0
        ), f"Processing took too long: {processing_time:.2f}s"

        plan = json.loads(result)
        print(f"Processed {len(plan['tasks'])} tasks in {processing_time:.2f}s")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
