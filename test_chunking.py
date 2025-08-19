#!/usr/bin/env python3
"""
Test script for chunked NiFi XML processing functionality.
This creates a sample large NiFi XML and tests the chunking tools.
"""

import json
import tempfile
from pathlib import Path

def create_sample_large_nifi_xml() -> str:
    """Create a sample large NiFi XML file for testing."""
    
    # Generate a large XML with multiple process groups and processors
    processors_xml = []
    connections_xml = []
    
    # Process Group 1 - Data Ingestion (15 processors)
    for i in range(15):
        processor_id = f"proc_1_{i:03d}"
        processors_xml.append(f"""
        <processors>
            <id>{processor_id}</id>
            <name>GetFile_{i:03d}</name>
            <type>org.apache.nifi.processors.standard.GetFile</type>
            <properties>
                <entry>
                    <key>Input Directory</key>
                    <value>/data/input_{i}</value>
                </entry>
                <entry>
                    <key>File Filter</key>
                    <value>.*\.csv</value>
                </entry>
            </properties>
        </processors>""")
        
        # Connect to next processor
        if i < 14:
            connections_xml.append(f"""
            <connections>
                <source><id>{processor_id}</id></source>
                <destination><id>proc_1_{i+1:03d}</id></destination>
                <selectedRelationships>success</selectedRelationships>
            </connections>""")
    
    # Process Group 2 - Data Transformation (20 processors)
    for i in range(20):
        processor_id = f"proc_2_{i:03d}"
        processors_xml.append(f"""
        <processors>
            <id>{processor_id}</id>
            <name>ConvertRecord_{i:03d}</name>
            <type>org.apache.nifi.processors.standard.ConvertRecord</type>
            <properties>
                <entry>
                    <key>Record Reader</key>
                    <value>csv-reader</value>
                </entry>
                <entry>
                    <key>Record Writer</key>
                    <value>parquet-writer</value>
                </entry>
            </properties>
        </processors>""")
        
        if i < 19:
            connections_xml.append(f"""
            <connections>
                <source><id>{processor_id}</id></source>
                <destination><id>proc_2_{i+1:03d}</id></destination>
                <selectedRelationships>success</selectedRelationships>
            </connections>""")
    
    # Process Group 3 - Data Output (25 processors)
    for i in range(25):
        processor_id = f"proc_3_{i:03d}"
        processors_xml.append(f"""
        <processors>
            <id>{processor_id}</id>
            <name>PutHDFS_{i:03d}</name>
            <type>org.apache.nifi.processors.hadoop.PutHDFS</type>
            <properties>
                <entry>
                    <key>Hadoop Configuration Resources</key>
                    <value>/etc/hadoop/conf/core-site.xml</value>
                </entry>
                <entry>
                    <key>Directory</key>
                    <value>/output/data_{i}</value>
                </entry>
            </properties>
        </processors>""")
        
        if i < 24:
            connections_xml.append(f"""
            <connections>
                <source><id>{processor_id}</id></source>
                <destination><id>proc_3_{i+1:03d}</id></destination>
                <selectedRelationships>success</selectedRelationships>
            </connections>""")
    
    # Cross-group connections
    connections_xml.append(f"""
    <connections>
        <source><id>proc_1_014</id></source>
        <destination><id>proc_2_000</id></destination>
        <selectedRelationships>success</selectedRelationships>
    </connections>""")
    
    connections_xml.append(f"""
    <connections>
        <source><id>proc_2_019</id></source>
        <destination><id>proc_3_000</id></destination>
        <selectedRelationships>success</selectedRelationships>
    </connections>""")
    
    # Assemble the complete XML
    xml_content = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<template>
    <description>Large test NiFi template with 60 processors across 3 process groups</description>
    <name>Large Test Template</name>
    <snippet>
        <processGroups>
            <id>group_1</id>
            <name>Data Ingestion</name>
        </processGroups>
        <processGroups>
            <id>group_2</id>
            <name>Data Transformation</name>
        </processGroups>
        <processGroups>
            <id>group_3</id>
            <name>Data Output</name>
        </processGroups>
        {''.join(processors_xml)}
        {''.join(connections_xml)}
    </snippet>
</template>"""
    
    return xml_content

def test_chunking_functionality():
    """Test the chunking functionality with a sample large XML."""
    print("Testing NiFi XML chunking functionality...")
    
    # Create sample XML
    xml_content = create_sample_large_nifi_xml()
    print(f"Created sample XML with ~60 processors")
    
    # Import the chunking tools
    try:
        from tools.chunking_tools import (
            chunk_nifi_xml_by_process_groups, 
            estimate_chunk_size,
            reconstruct_full_workflow
        )
        print("✓ Successfully imported chunking tools")
    except ImportError as e:
        print(f"✗ Failed to import chunking tools: {e}")
        return False
    
    # Test chunking
    try:
        chunking_result = chunk_nifi_xml_by_process_groups.func(
            xml_content=xml_content,
            max_processors_per_chunk=25
        )
        
        result = json.loads(chunking_result)
        
        if "error" in result:
            print(f"✗ Chunking failed: {result['error']}")
            return False
        
        chunks = result["chunks"]
        cross_chunk_links = result["cross_chunk_links"]
        summary = result["summary"]
        
        print(f"✓ Chunking successful:")
        print(f"  - Total processors: {summary['total_processors']}")
        print(f"  - Total connections: {summary['total_connections']}")
        print(f"  - Chunks created: {summary['chunk_count']}")
        print(f"  - Cross-chunk links: {summary['cross_chunk_links_count']}")
        
        # Verify chunk details
        for i, chunk in enumerate(chunks):
            print(f"  - Chunk {i}: {chunk['processor_count']} processors, "
                  f"{chunk['internal_connection_count']} internal connections")
        
        return True
        
    except Exception as e:
        print(f"✗ Chunking test failed: {e}")
        return False

def test_migration_tools():
    """Test that the new migration tools can be imported."""
    print("\nTesting migration tools import...")
    
    try:
        from tools.migration_tools import (
            orchestrate_chunked_nifi_migration,
            process_nifi_chunk
        )
        print("✓ Successfully imported chunked migration tools")
        return True
    except ImportError as e:
        print(f"✗ Failed to import migration tools: {e}")
        return False

def test_agent_integration():
    """Test that the agent has the chunking tools available."""
    print("\nTesting agent integration...")
    
    try:
        from tools import TOOLS
        tool_names = [tool.name for tool in TOOLS]
        
        required_tools = [
            "chunk_nifi_xml_by_process_groups",
            "orchestrate_chunked_nifi_migration", 
            "process_nifi_chunk",
            "reconstruct_full_workflow"
        ]
        
        missing_tools = [tool for tool in required_tools if tool not in tool_names]
        
        if missing_tools:
            print(f"✗ Missing tools from agent: {missing_tools}")
            return False
        
        print("✓ All chunking tools available to agent")
        print(f"  - Total tools available: {len(TOOLS)}")
        print(f"  - Chunking tools: {[tool for tool in required_tools if tool in tool_names]}")
        
        return True
        
    except Exception as e:
        print(f"✗ Agent integration test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("NiFi Chunking Implementation Test Suite")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 3
    
    if test_chunking_functionality():
        tests_passed += 1
    
    if test_migration_tools():
        tests_passed += 1
        
    if test_agent_integration():
        tests_passed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("✓ All tests passed! Chunking implementation is ready.")
        print("\nNext steps:")
        print("1. Test with a real large NiFi XML file")
        print("2. Use the agent with: 'Run orchestrate_chunked_nifi_migration ...'")
        print("3. Verify the generated multi-task Databricks job works correctly")
    else:
        print("✗ Some tests failed. Please check the implementation.")
    
    print("=" * 60)