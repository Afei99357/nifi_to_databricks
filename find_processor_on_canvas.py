#!/usr/bin/env python3
"""
Quick tool to find processors on your NiFi canvas by table name or processor name
"""

import xml.etree.ElementTree as ET

from tools.simple_table_lineage import (
    _extract_tables_from_processor,
    _strip_namespaces,
    _txt,
)


def find_processor_on_canvas(xml_path: str, search_term: str):
    """Find processors on NiFi canvas by table name or processor name"""

    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    root = ET.fromstring(xml_content)
    _strip_namespaces(root)

    print(f"🔍 FINDING: '{search_term}' in NiFi canvas")
    print("=" * 60)

    matches = []

    for proc_container in root.findall(".//processors"):
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

        # Extract tables
        reads, writes, _ = _extract_tables_from_processor(
            ptype, props, strict_sql_only=True
        )
        all_tables = reads | writes

        # Check if matches search term
        match_found = False
        match_reason = []

        # Check processor name
        if search_term.lower() in pname.lower():
            match_found = True
            match_reason.append(f"Processor name contains '{search_term}'")

        # Check table names
        for table in all_tables:
            if search_term.lower() in table.lower():
                match_found = True
                match_reason.append(f"Contains table '{table}'")

        if match_found:
            position = proc_container.find("position")
            x = _txt(position, "x") if position is not None else "unknown"
            y = _txt(position, "y") if position is not None else "unknown"

            matches.append(
                {
                    "name": pname,
                    "type": ptype.split(".")[-1],  # Just the class name
                    "id": pid,
                    "position": (x, y),
                    "tables": sorted(all_tables),
                    "reason": match_reason,
                }
            )

    if not matches:
        print(f"❌ No processors found containing '{search_term}'")
        return

    for i, match in enumerate(matches):
        print(f"\n🎯 MATCH #{i+1}")
        print(f"   📛 Processor: {match['name']}")
        print(f"   🔧 Type: {match['type']}")
        print(
            f"   📍 Canvas Position: ({match['position'][0]}, {match['position'][1]})"
        )
        print(f"   🆔 ID: {match['id']}")
        print(f"   📊 Tables: {match['tables']}")
        print(f"   ✅ Match reason: {', '.join(match['reason'])}")

    print(f"\n💡 TO FIND ON CANVAS:")
    print(f"   1. Look for processor at coordinates shown above")
    print(f"   2. Or search by processor name in NiFi")
    print(f"   3. Right-click → Configure → Properties to verify tables")


if __name__ == "__main__":
    import sys

    xml_path = "/home/eric/Projects/nifi_to_databricks/nifi_pipeline_file/ICN8_Track-out_time_based_loading.xml"

    if len(sys.argv) > 1:
        search_term = sys.argv[1]
        find_processor_on_canvas(xml_path, search_term)
    else:
        print("💡 USAGE EXAMPLES:")
        print("   python find_processor_on_canvas.py 'diamond_components'")
        print("   python find_processor_on_canvas.py 'Diamond Components'")
        print("   python find_processor_on_canvas.py 'mfg_icn8_data'")
        print("   python find_processor_on_canvas.py 'Add configuration'")
