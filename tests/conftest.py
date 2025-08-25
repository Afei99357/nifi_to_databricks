"""
Shared pytest fixtures for nifi_to_databricks tests.
"""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_nifi_xml():
    """Sample NiFi XML template for testing."""
    return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
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
                <selectedRelationships>
                    <relationship>success</relationship>
                </selectedRelationships>
            </connection>
        </connections>
    </snippet>
</template>"""


@pytest.fixture
def sample_chunk_data():
    """Sample chunk data for testing process_nifi_chunk."""
    return {
        "chunk_id": "chunk_0",
        "processors": [
            {
                "id": "proc-1",
                "name": "GetFile1",
                "type": "org.apache.nifi.processors.standard.GetFile",
                "properties": {"Input Directory": "/input", "File Filter": ".*\\.csv"},
            }
        ],
        "connections": [],
    }


@pytest.fixture(autouse=True)
def mock_environment_vars(monkeypatch):
    """Set up common environment variables for tests."""
    monkeypatch.setenv("MAX_PROCESSORS_PER_CHUNK", "20")
    monkeypatch.setenv("ENABLE_LLM_CODE_GENERATION", "true")
    monkeypatch.setenv("LLM_SUB_BATCH_SIZE", "10")
