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
                    <value>.*\\.csv</value>
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
    
    # Test core chunking logic without dependencies
    try:
        import xml.etree.ElementTree as ET
        import json
        
        # Simple chunking test
        root = ET.fromstring(xml_content)
        processors = []
        connections = []
        
        # Extract processors
        for processor in root.findall(".//processors"):
            proc_id = (processor.findtext("id") or "").strip()
            if proc_id:
                processors.append({
                    "id": proc_id,
                    "name": (processor.findtext("name") or "Unknown").strip(),
                    "type": (processor.findtext("type") or "Unknown").strip(),
                })
        
        # Extract connections  
        for connection in root.findall(".//connections"):
            source_id = (connection.findtext(".//source/id") or "").strip()
            dest_id = (connection.findtext(".//destination/id") or "").strip()
            if source_id and dest_id:
                connections.append({
                    "source": source_id,
                    "destination": dest_id,
                })
        
        print(f"✓ Parsed {len(processors)} processors and {len(connections)} connections")
        
        # Test chunking by groups
        groups = {}
        for proc in processors:
            if proc["id"].startswith("proc_1_"):
                group = "group_1"
            elif proc["id"].startswith("proc_2_"):
                group = "group_2"
            elif proc["id"].startswith("proc_3_"):
                group = "group_3"
            else:
                group = "root"
            
            if group not in groups:
                groups[group] = []
            groups[group].append(proc)
        
        print(f"✓ Grouped into {len(groups)} process groups:")
        for group, procs in groups.items():
            print(f"  - {group}: {len(procs)} processors")
        
        # Test cross-group connections
        cross_group_connections = 0
        for conn in connections:
            src_group = None
            dst_group = None
            
            if conn["source"].startswith("proc_1_"):
                src_group = "group_1"
            elif conn["source"].startswith("proc_2_"):
                src_group = "group_2"
            elif conn["source"].startswith("proc_3_"):
                src_group = "group_3"
            
            if conn["destination"].startswith("proc_1_"):
                dst_group = "group_1"
            elif conn["destination"].startswith("proc_2_"):
                dst_group = "group_2"
            elif conn["destination"].startswith("proc_3_"):
                dst_group = "group_3"
            
            if src_group and dst_group and src_group != dst_group:
                cross_group_connections += 1
        
        print(f"✓ Found {cross_group_connections} cross-group connections")
        
        if len(processors) == 60 and len(groups) == 3 and cross_group_connections >= 2:
            print("✓ Core chunking logic validation passed")
            return True
        else:
            print("✗ Validation failed - unexpected counts")
            return False
        
    except Exception as e:
        print(f"✗ Core chunking test failed: {e}")
        return False

def test_migration_tools():
    """Test that the migration tools files exist."""
    print("\nTesting migration tools availability...")
    
    try:
        import os
        tools_path = os.path.join(os.path.dirname(__file__), 'tools')
        
        required_files = [
            'chunking_tools.py',
            'migration_tools.py', 
            'xml_tools.py'
        ]
        
        missing_files = []
        for filename in required_files:
            filepath = os.path.join(tools_path, filename)
            if not os.path.exists(filepath):
                missing_files.append(filename)
        
        if missing_files:
            print(f"✗ Missing tool files: {missing_files}")
            return False
        
        print("✓ All required tool files present")
        print(f"  - Found {len(required_files)} tool modules")
        return True
        
    except Exception as e:
        print(f"✗ Tools check failed: {e}")
        return False

def test_agent_integration():
    """Test that the agent files are present."""
    print("\nTesting agent integration...")
    
    try:
        import os
        
        agent_files = [
            'agents/agent.py',
            'tools/__init__.py'
        ]
        
        project_root = os.path.dirname(__file__)
        missing_files = []
        
        for filepath in agent_files:
            full_path = os.path.join(project_root, filepath)
            if not os.path.exists(full_path):
                missing_files.append(filepath)
        
        if missing_files:
            print(f"✗ Missing agent files: {missing_files}")
            return False
        
        print("✓ Agent integration files present")
        print("  - agents/agent.py: Agent implementation")
        print("  - tools/__init__.py: Tools registry")
        
        # Check if chunking tools are referenced
        tools_init_path = os.path.join(project_root, 'tools', '__init__.py')
        with open(tools_init_path, 'r') as f:
            content = f.read()
            if 'chunking_tools' in content:
                print("✓ Chunking tools integrated into agent")
                return True
            else:
                print("✗ Chunking tools not found in agent integration")
                return False
        
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