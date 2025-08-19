import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

def summarize_nifi_template(xml_path: str) -> str:
    """
    Summarize a NiFi template XML into a digest of process groups, processors, 
    controller services, and connections.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Extract high-level name
    template_name = root.findtext("name", default="(Unnamed Template)").strip()

    # Processor counts
    processors = root.findall(".//processors/processor/type")
    proc_counter = Counter(p.text for p in processors if p is not None)

    # Controller services
    controller_services = root.findall(".//controllerServices/type")
    ctrl_counter = Counter(c.text for c in controller_services if c is not None)

    # Process groups
    groups = root.findall(".//processGroups/processGroup/name")
    group_names = [g.text.strip() for g in groups if g is not None]

    # Connections
    connections = root.findall(".//connections/connection")
    conn_count = len(connections)

    # Build summary
    lines = [f"Process Group: {template_name}"]
    if group_names:
        lines.append(f"Nested Groups ({len(group_names)}): {', '.join(group_names[:10])}" +
                     ("..." if len(group_names) > 10 else ""))
    if proc_counter:
        lines.append("Processors:")
        for proc, count in proc_counter.most_common():
            lines.append(f"  - {proc}: {count}")
    if ctrl_counter:
        lines.append("Controller Services:")
        for svc, count in ctrl_counter.most_common():
            lines.append(f"  - {svc}: {count}")
    lines.append(f"Connections: {conn_count}")

    return "\n".join(lines)
