#!/usr/bin/env python3
"""
Test XML parsing without LLM to verify structure extraction works
"""

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List


def extract_processors_from_xml(xml_content: str) -> List[Dict[str, Any]]:
    """Extract processor information from NiFi XML"""
    try:
        root = ET.fromstring(xml_content)
        processors = []

        for processor in root.findall(".//processors"):
            proc_id = (processor.findtext("id") or "").strip()
            proc_name = (processor.findtext("name") or "Unknown").strip()
            proc_type = (processor.findtext("type") or "").strip()

            # Extract properties
            properties = {}
            config = processor.find("config")
            if config is not None:
                props_elem = config.find("properties")
                if props_elem is not None:
                    for entry in props_elem.findall("entry"):
                        key_elem = entry.find("key")
                        value_elem = entry.find("value")
                        if key_elem is not None and value_elem is not None:
                            key = key_elem.text or ""
                            value = value_elem.text or ""
                            if value:  # Only store non-empty values
                                properties[key] = value

            processors.append(
                {
                    "id": proc_id,
                    "name": proc_name,
                    "processor_type": proc_type,
                    "properties": properties,
                }
            )

        return processors

    except Exception as e:
        print(f"❌ XML parsing failed: {e}")
        return []


def test_xml_structure():
    """Test XML structure extraction on both workflows"""
    print("=== TESTING XML STRUCTURE EXTRACTION ===\n")

    # Test simple workflow
    print("🔍 SIMPLE WORKFLOW (nifi_pipeline_eric_1.xml):")
    try:
        with open("nifi_pipeline_file/nifi_pipeline_eric_1.xml", "r") as f:
            simple_xml = f.read()

        simple_processors = extract_processors_from_xml(simple_xml)
        print(f"📊 Found {len(simple_processors)} processors:")

        for i, proc in enumerate(simple_processors, 1):
            proc_class = (
                proc["processor_type"].split(".")[-1]
                if "." in proc["processor_type"]
                else proc["processor_type"]
            )
            print(f'  {i}. {proc["name"]} ({proc_class})')
            print(f'     Properties: {len(proc["properties"])} configured')

            # Show key properties for analysis
            key_props: list[str] = []
            for key, value in proc["properties"].items():
                if len(key_props) < 3:  # Show first 3 properties
                    key_props.append(f"{key}={value}")
            if key_props:
                print(f'     Key props: {"; ".join(key_props)}')

    except Exception as e:
        print(f"❌ Simple workflow test failed: {e}")

    print("\n" + "=" * 60 + "\n")

    # Test complex workflow
    print("🔍 COMPLEX WORKFLOW (ICN8_BRS_Feedback.xml):")
    try:
        with open("nifi_pipeline_file/ICN8_BRS_Feedback.xml", "r") as f:
            complex_xml = f.read()

        complex_processors = extract_processors_from_xml(complex_xml)
        print(f"📊 Found {len(complex_processors)} processors:")

        # Count processor types
        type_counts: dict[str, int] = {}
        for proc in complex_processors:
            proc_class = (
                proc["processor_type"].split(".")[-1]
                if "." in proc["processor_type"]
                else proc["processor_type"]
            )
            type_counts[proc_class] = type_counts.get(proc_class, 0) + 1

        print("📈 Processor type breakdown:")
        for proc_type, count in type_counts.items():
            print(f"   {proc_type}: {count}")

        # Show first few processors with details
        print("\n🔍 Sample processors:")
        for i, proc in enumerate(complex_processors[:5], 1):
            proc_class = (
                proc["processor_type"].split(".")[-1]
                if "." in proc["processor_type"]
                else proc["processor_type"]
            )
            print(f'  {i}. {proc["name"]} ({proc_class})')
            print(f'     Properties: {len(proc["properties"])} configured')

        if len(complex_processors) > 5:
            print(f"   ... and {len(complex_processors) - 5} more processors")

    except Exception as e:
        print(f"❌ Complex workflow test failed: {e}")

    print("\n" + "=" * 60)
    print("✅ XML STRUCTURE EXTRACTION COMPLETE!")
    print("🧠 Ready to test LLM intelligence on extracted processor data")


if __name__ == "__main__":
    test_xml_structure()
