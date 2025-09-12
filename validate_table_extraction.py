#!/usr/bin/env python3
"""
Validation script to show WHERE tables are extracted from in the NiFi XML
and help correlate with the actual NiFi canvas
"""

import xml.etree.ElementTree as ET

from tools.simple_table_lineage import (
    _extract_tables_from_processor,
    _strip_namespaces,
    _txt,
)


def validate_table_extraction(xml_path: str, target_table: str = None):
    """Show exactly where each table is extracted from in the NiFi XML"""

    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    root = ET.fromstring(xml_content)
    _strip_namespaces(root)

    print(f"🔍 VALIDATION: Table Extraction Sources")
    print(f"📄 File: {xml_path}")
    if target_table:
        print(f"🎯 Target: {target_table}")
    print("=" * 80)

    found_processors = []

    # Analyze each processor
    for i, proc_container in enumerate(root.findall(".//processors")):
        pid = _txt(proc_container, "id")
        ptype = _txt(proc_container, "type")
        pname = _txt(proc_container, "name")

        # Extract properties
        props = {}
        config_elem = proc_container.find("config")
        if config_elem is not None:
            props_elem = config_elem.find("properties")
            if props_elem is not None:
                for entry in props_elem.findall("entry"):
                    key = _txt(entry, "key")
                    value = _txt(entry, "value")
                    if key:
                        props[key] = value

        # Extract tables using our algorithm
        reads, writes, _ = _extract_tables_from_processor(
            ptype, props, strict_sql_only=True
        )

        # Check if this processor has the target table or any tables
        if target_table:
            if target_table in reads or target_table in writes:
                found_processors.append(
                    (proc_container, pid, ptype, pname, props, reads, writes)
                )
        elif reads or writes:
            found_processors.append(
                (proc_container, pid, ptype, pname, props, reads, writes)
            )

    # Display results
    if not found_processors:
        print("❌ No processors found with table references")
        return

    for i, (proc_elem, pid, ptype, pname, props, reads, writes) in enumerate(
        found_processors
    ):
        print(f"\n📋 PROCESSOR #{i+1}")
        print(f"   🆔 ID: {pid}")
        print(f"   🔧 Type: {ptype}")
        print(f"   📛 Name: {pname}")
        print(f"   📊 Reads: {sorted(reads)}")
        print(f"   📤 Writes: {sorted(writes)}")

        # Show the EXACT properties where tables were found
        print(f"   🔍 EVIDENCE - Properties containing table references:")

        # Re-analyze to show exactly where each table came from
        if "updateattribute" in ptype.lower():
            print(
                f"      📝 UpdateAttribute processor - checking explicit table properties:"
            )
            for key, value in props.items():
                if not isinstance(value, str) or not value.strip():
                    continue
                key_lower = key.lower()
                if any(
                    pattern in key_lower
                    for pattern in [
                        "table",
                        "prod_table",
                        "staging_table",
                        "external_table",
                        "source_table",
                        "target_table",
                        "destination_table",
                    ]
                ):
                    print(f"         ✅ {key} = {value}")

        elif any(
            db_type in ptype.lower()
            for db_type in ["executesql", "putsql", "executestreamcommand"]
        ):
            print(f"      🎯 Database processor - checking SQL and command properties:")
            for key, value in props.items():
                if not isinstance(value, str) or len(value) < 5:
                    continue
                kl = key.lower()
                if any(
                    x in kl for x in ("sql", "query", "statement", "hql", "command")
                ):
                    # Show first 200 chars of SQL/command
                    preview = value[:200] + ("..." if len(value) > 200 else "")
                    print(f"         📜 {key}: {preview}")

        # Show position info to help locate in canvas
        position = proc_elem.find("position")
        if position is not None:
            x = _txt(position, "x")
            y = _txt(position, "y")
            print(f"   📍 Canvas Position: ({x}, {y})")

        print(f"   🔗 XML Path: .//processors[id='{pid}']")


if __name__ == "__main__":
    import sys

    xml_path = "/home/eric/Projects/nifi_to_databricks/nifi_pipeline_file/ICN8_Track-out_time_based_loading.xml"

    if len(sys.argv) > 1:
        target_table = sys.argv[1]
        print(f"🎯 Looking specifically for table: {target_table}")
        validate_table_extraction(xml_path, target_table)
    else:
        print("📋 Showing ALL processors with table references:")
        validate_table_extraction(xml_path)

        print(f"\n💡 USAGE:")
        print(
            f"   python validate_table_extraction.py 'mfg_icn8_staging.nifi_imp_diamond_components'"
        )
        print(
            f"   python validate_table_extraction.py 'mfg_icn8_data.nifi_diamond_components'"
        )
