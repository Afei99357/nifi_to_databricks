#!/usr/bin/env python3
"""
Simple but useful NiFi table lineage (no NetworkX).
- Handles namespaces
- Parses processors, properties, and connections
- Extracts read/write tables across common DB processors
- Builds directional table->table chains (intra- and inter-processor)
"""

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Set, Tuple

# ---------- XML helpers ----------


def _strip_namespaces(root: ET.Element):
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def _txt(elem: ET.Element, *paths: str) -> str:
    for p in paths:
        n = elem.find(p)
        if n is not None:
            t = (n.text or "").strip()
            if t:
                return t
    return ""


def _extract_properties(proc: ET.Element) -> Dict[str, str]:
    """Handle both component-scoped and direct properties, and entry[@key]."""
    props: Dict[str, str] = {}
    props_node = (
        proc.find("./component/properties")
        or proc.find("./properties")
        or proc.find(".//properties")
    )
    if props_node is None:
        return props
    for entry in props_node.findall("./entry"):
        k = (
            entry.findtext("key") or entry.get("key") or entry.findtext("name") or ""
        ).strip()
        v = (entry.findtext("value") or "").strip()
        if k:
            props[k] = v
    return props


# ---------- Table extraction ----------

# Accept db.table or db.schema.table, allow quoted/backticked/bracketed parts
_IDENT = r'(?:[`"\[]?[A-Za-z_][\w$]*[`"\]]?)'
TABLE_IDENT = rf"(?:{_IDENT}\.)?{_IDENT}(?:\.{_IDENT})?"  # up to 3-part

_READ_PATTERNS = [
    rf"\bfrom\s+({TABLE_IDENT})",
    rf"\bjoin\s+({TABLE_IDENT})",
    rf"\bmerge\s+into\s+({TABLE_IDENT})",  # merge reads target too
]
_WRITE_PATTERNS = [
    rf"\binsert\s+(?:into|overwrite\s+table)\s+({TABLE_IDENT})",
    rf"\bcreate\s+(?:or\s+replace\s+)?table\s+({TABLE_IDENT})",
    rf"\btruncate\s+table\s+({TABLE_IDENT})",
    rf"\brefresh\s+({TABLE_IDENT})",
    rf"\balter\s+table\s+({TABLE_IDENT})",
    rf"\bmerge\s+into\s+({TABLE_IDENT})",
]


def _clean_table(name: str) -> str:
    return name.strip('`"[]').lower()


def _is_false_positive_table_ref(name: str) -> bool:
    if not name:
        return True
    n = name.lower()

    # Filter out single-letter aliases (p.cnt, t.before, y.w01, etc.)
    parts = n.split(".")
    if len(parts) >= 2:
        # Check if first part is a single letter (common alias pattern)
        if len(parts[0]) == 1 and parts[0].isalpha():
            return True
        # Check for common alias patterns like p., t., y., etc.
        if len(parts[0]) <= 2 and parts[0] in (
            "p",
            "t",
            "y",
            "s",
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
        ):
            return True

    # Filter out column-like patterns (before, after, cnt, avg, sum, etc.)
    if len(parts) >= 2:
        # Common column names that shouldn't be tables
        column_patterns = {
            "cnt",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "before",
            "after",
            "date",
            "time",
            "timestamp",
            "id",
            "name",
            "value",
            "status",
            "type",
            "code",
            "desc",
            "description",
            "create",
            "update",
            "delete",
            "insert",
            "w01",
            "w02",
            "w03",
            "w04",
            "w05",
            "absolute",
            "path",
            "file",
            "row",
            "col",
        }
        if parts[-1] in column_patterns:
            return True

    # obvious non-table substrings
    for pat in (
        ".sh",
        ".py",
        ".jar",
        ".class",
        ".xml",
        ".keytab",
        ".com",
        ".error",
        ".log",
        ".txt",
        ".csv",
    ):
        if pat in n:
            return True
    # guard against keywords accidentally captured
    for kw in (
        "select",
        "where",
        "order",
        "group",
        "by",
        "join",
        "from",
        "when",
        "case",
        "then",
        "else",
        "end",
    ):
        if n == kw:
            return True
    # skip variable references and multi-line content
    if any(pattern in n for pattern in ["${", "}", "\n", "\r", "--", "/*"]):
        return True
    # skip obvious paths or long descriptive content
    if len(n) > 100 or n.count("/") > 2:
        return True
    return False


def _extract_sql_tables(sql_text: str) -> Tuple[Set[str], Set[str]]:
    reads, writes = set(), set()
    if not sql_text or len(sql_text) < 5:
        return reads, writes
    for pat in _READ_PATTERNS:
        for m in re.finditer(pat, sql_text, flags=re.IGNORECASE):
            t = _clean_table(m.group(1))
            if not _is_false_positive_table_ref(t):
                reads.add(t)
    for pat in _WRITE_PATTERNS:
        for m in re.finditer(pat, sql_text, flags=re.IGNORECASE):
            t = _clean_table(m.group(1))
            if not _is_false_positive_table_ref(t):
                writes.add(t)
    return reads, writes


def _sql_snippets(ptype: str, props: Dict[str, str]) -> List[str]:
    """Extract SQL snippets from processor properties"""
    p = (ptype or "").lower()
    sqls = []
    if "executesql" in p:
        sqls += [props.get("SQL select query") or props.get("SQL Query") or ""]
    if "putsql" in p or "puthiveql" in p:
        sqls += [
            props.get("SQL statement") or props.get("sql") or props.get("HiveQL") or ""
        ]
    if "executestreamcommand" in p:
        cmd = f"{props.get('Command Path', '')} {props.get('Command Arguments', '')}"
        # prefer quoted chunks; fall back if we see SQL verbs
        chunks = re.findall(r'"([^"]+)"|\'([^\']+)\'', cmd)
        sqls += [c[0] or c[1] for c in chunks if (c[0] or c[1])]
        if not sqls and re.search(r"\b(select|insert|merge|create)\b", cmd, re.I):
            sqls += [cmd]
        # any explicit sql/* keys
        for k, v in props.items():
            if (
                isinstance(v, str)
                and len(v) > 10
                and any(w in k.lower() for w in ("sql", "query", "statement", "hql"))
            ):
                sqls.append(v)
    return [s for s in sqls if s and s.strip()]


def _pairs_from_sql(sql: str) -> Set[Tuple[str, str]]:
    """Extract read→write pairs from SQL"""
    r, w = _extract_sql_tables(sql)
    return {(rr, ww) for rr in r for ww in w if rr != ww}


def _extract_tables_from_processor(
    ptype: str, props: Dict[str, Any], strict_sql_only: bool = True
) -> Tuple[Set[str], Set[str], Set[Tuple[str, str]]]:
    """Return (reads, writes) tables for a processor."""
    p = (ptype or "").lower()
    reads, writes = set(), set()

    # UpdateAttribute processors (NiFi configuration processors)
    if "updateattribute" in p:
        # These processors define table mappings in their properties
        for key, value in props.items():
            if not isinstance(value, str) or not value.strip():
                continue

            key_lower = key.lower()
            # Look for explicit table definition properties
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
                table = _clean_table(value)
                if not _is_false_positive_table_ref(table) and "." in table:
                    if any(
                        s in key_lower
                        for s in ("prod", "target", "destination", "output")
                    ):
                        writes.add(table)
                    elif any(s in key_lower for s in ("external", "source", "input")):
                        reads.add(table)
                    elif "staging" in key_lower:
                        # Staging tables are typically intermediate (both read and written)
                        reads.add(table)
                        writes.add(table)
                    else:
                        # Default to both if unclear
                        reads.add(table)
                        writes.add(table)

    # Common DB processors
    # ExecuteSQL (reads)
    if "executesql" in p:
        sql = props.get("SQL select query") or props.get("SQL Query") or ""
        r, _ = _extract_sql_tables(sql)
        reads |= r

    # PutSQL / PutHiveQL (writes)
    if "putsql" in p or "puthiveql" in p:
        sql = (
            props.get("SQL statement") or props.get("sql") or props.get("HiveQL") or ""
        )
        r, w = _extract_sql_tables(sql)
        # PutSQL often has only INSERT/CREATE etc.
        reads |= r
        writes |= w
        # also honor explicit table name when present
        tname = (props.get("Table Name") or props.get("table.name") or "").strip()
        if tname:
            writes.add(_clean_table(tname))

    # PutDatabaseRecord (writes)
    if "putdatabaserecord" in p:
        tname = (props.get("Table Name") or props.get("table.name") or "").strip()
        if tname:
            writes.add(_clean_table(tname))

    # QueryDatabaseTable / GenerateTableFetch (reads)
    if "querydatabasetable" in p or "generatetablefetch" in p:
        tname = (props.get("Table Name") or props.get("table.name") or "").strip()
        if tname:
            reads.add(_clean_table(tname))

    # ExecuteStreamCommand (reads/writes inferred from embedded SQL and Command Arguments)
    if "executestreamcommand" in p:
        cmd = f"{props.get('Command Path', '')} {props.get('Command Arguments', '')}"
        r, w = _extract_sql_tables(cmd)
        reads |= r
        writes |= w
        # also scan any property that looks like SQL or contains table references
        for k, v in props.items():
            if not isinstance(v, str) or len(v) < 5:
                continue
            kl = k.lower()
            if any(x in kl for x in ("sql", "query", "statement", "hql")):
                r2, w2 = _extract_sql_tables(v)
                reads |= r2
                writes |= w2
            else:
                # Look for direct table references in Command Arguments
                for m in re.finditer(rf"\b({_IDENT}\.{_IDENT}(?:\.{_IDENT})?)\b", v):
                    t = _clean_table(m.group(1))
                    if not _is_false_positive_table_ref(t):
                        # ExecuteStreamCommand typically both reads and writes
                        reads.add(t)
                        writes.add(t)

    # Generic schema.table mentions in properties (for any other processor types)
    # Skip this noisy pattern matching if strict_sql_only is True
    if not strict_sql_only:
        for k, v in props.items():
            if not isinstance(v, str):
                continue
            for m in re.finditer(rf"\b({_IDENT}\.{_IDENT}(?:\.{_IDENT})?)\b", v):
                t = _clean_table(m.group(1))
                if _is_false_positive_table_ref(t):
                    continue
                kl = k.lower()
                if any(s in kl for s in ("output", "target", "destination", "prod")):
                    writes.add(t)
                elif any(s in kl for s in ("input", "source", "external")):
                    reads.add(t)
                elif "staging" in kl:
                    # Staging context suggests intermediate processing
                    reads.add(t)
                    writes.add(t)
                else:
                    # Conservative: if we can't determine direction, include both
                    reads.add(t)
                    writes.add(t)

    # Extract evidence-based pairs from SQL snippets
    sqls = _sql_snippets(ptype, props)
    pairs = set()
    for sql in sqls:
        pairs |= _pairs_from_sql(sql)

    return reads, writes, pairs


# ---------- Main: parse & lineage ----------


def analyze_nifi_table_lineage(
    xml_content: str, max_depth: int = 4, use_statement_pairs: bool = True
) -> Dict[str, Any]:
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        return {"error": f"Failed to parse XML: {e}"}
    _strip_namespaces(root)

    # Processors (NiFi templates use <processors> containers, not <processor> elements)
    procs: Dict[str, Dict[str, Any]] = {}
    for proc_container in root.findall(".//processors"):
        pid = _txt(proc_container, "id")
        if not pid:
            continue
        ptype = _txt(proc_container, "type")
        pname = _txt(proc_container, "name") or f"processor-{pid[:8]}"

        # Extract properties from the container
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

        reads, writes, pairs = _extract_tables_from_processor(
            ptype, props, strict_sql_only=True
        )
        procs[pid] = {
            "id": pid,
            "name": pname,
            "type": ptype,
            "reads": reads,
            "writes": writes,
            "pairs": pairs,
        }

    # Connections (processor graph) - NiFi templates use <connections> containers
    adj: Dict[str, List[str]] = {}
    for conn_container in root.findall(".//connections"):
        src = _txt(conn_container, "source/id")
        dst = _txt(conn_container, "destination/id")
        if not src or not dst or src not in procs or dst not in procs:
            continue
        adj.setdefault(src, []).append(dst)

    # Collect all initial tables (before schema filtering)
    all_initial_tables: Set[str] = set()
    for p in procs.values():
        all_initial_tables |= p["reads"] | p["writes"]

    # Dynamic schema allowlist - count schema frequency
    schema_counts: Dict[str, int] = {}
    for table in all_initial_tables:
        if "." in table:
            schema = table.split(".")[0]
            schema_counts[schema] = schema_counts.get(schema, 0) + 1

    # Build allowlist of schemas that appear at least twice (minimum frequency)
    # This helps filter out one-off alias captures while keeping real schemas
    min_schema_frequency = 2
    schema_allowlist = {
        schema
        for schema, count in schema_counts.items()
        if count >= min_schema_frequency
    }

    # Seed known good schemas so we don't drop legitimate one-offs
    schema_allowlist |= {
        "bq",
        "bqa",
        "bws",
        "dia",
        "e3s",
        "e3u",
        "edc",
        "pcm",
        "ps",
        "ovl",
        "nts",
        "proc_bws",
        "mfg_icn8_data",
        "mfg_icn8_staging",
        "mfg_icn8_apps",
        "mfg_icn8_temp",
    }

    # Apply schema filtering to processor tables
    def _filter_table_by_schema(table: str) -> bool:
        if "." not in table:
            return False  # Single-part names are likely not real tables
        schema = table.split(".")[0]
        return schema in schema_allowlist

    # Re-filter processor tables based on schema allowlist
    for p in procs.values():
        p["reads"] = {t for t in p["reads"] if _filter_table_by_schema(t)}
        p["writes"] = {t for t in p["writes"] if _filter_table_by_schema(t)}

    # Collect final filtered tables
    all_tables: Set[str] = set()
    for p in procs.values():
        all_tables |= p["reads"] | p["writes"]

    # Build OPTION 3: Variable Resolution chains with processor references
    chains: List[Dict[str, Any]] = []

    # Step 1: Build variable resolution map and identify config processors
    config_processors = {}  # pid -> processor info for UpdateAttribute
    for pid, p in procs.items():
        if "updateattribute" in p["type"].lower():
            config_processors[pid] = p

    print(f"🔧 Found {len(config_processors)} UpdateAttribute config processors")

    # Step 2: Get processing processors
    processing_procs = {
        pid: p
        for pid, p in procs.items()
        if not any(
            config_type in p["type"].lower()
            for config_type in [
                "updateattribute",
                "logmessage",
                "wait",
                "routeonattribute",
            ]
        )
        and (p["reads"] or p["writes"])
    }

    print(f"🔧 Found {len(processing_procs)} actual processing processors")

    # Step 3: Build processor numbering for chain display
    processor_numbers = {}
    counter = 1
    for pid, p in procs.items():
        processor_numbers[pid] = counter
        counter += 1

    # Step 4: Variable resolution function
    def resolve_variables_in_text(text: str, connected_configs: List[str]) -> str:
        """Resolve ${variable} references using connected UpdateAttribute processors"""
        resolved = text
        for config_pid in connected_configs:
            config_proc = procs.get(config_pid, {})
            # Get properties, handling the nested structure
            config_props = {}
            config_elem = None
            for proc_container in root.findall(".//processors"):
                if _txt(proc_container, "id") == config_pid:
                    config_elem = proc_container.find("config")
                    break

            if config_elem is not None:
                props_elem = config_elem.find("properties")
                if props_elem is not None:
                    for entry in props_elem.findall("entry"):
                        key = _txt(entry, "key")
                        value = _txt(entry, "value")
                        if key and value:
                            config_props[key] = value

            # Replace variables
            for prop_key, prop_value in config_props.items():
                if isinstance(prop_value, str) and prop_value.strip():
                    resolved = resolved.replace(f"${{{prop_key}}}", prop_value)

        return resolved

    # Step 5: Build chains with variable resolution
    for pid, p in processing_procs.items():
        # Find connected UpdateAttribute processors (via NiFi connections)
        connected_configs = []
        for src_pid, dst_pids in adj.items():
            if pid in dst_pids and src_pid in config_processors:
                connected_configs.append(src_pid)

        processor_props = p.get("properties", {})

        # Extract properties directly from XML for this processor
        direct_props = {}
        for proc_container in root.findall(".//processors"):
            if _txt(proc_container, "id") == pid:
                config_elem = proc_container.find("config")
                if config_elem is not None:
                    props_elem = config_elem.find("properties")
                    if props_elem is not None:
                        for entry in props_elem.findall("entry"):
                            key = _txt(entry, "key")
                            value = _txt(entry, "value")
                            if key and value:
                                direct_props[key] = value
                break

        # Process ExecuteStreamCommand with variable resolution
        if "executestreamcommand" in p["type"].lower():
            cmd_path = direct_props.get("Command Path", "")
            cmd_args = direct_props.get("Command Arguments", "")
            full_command = f"{cmd_path} {cmd_args}"

            # Resolve variables in command
            resolved_command = resolve_variables_in_text(
                full_command, connected_configs
            )

            # Extract tables from resolved command
            resolved_reads, resolved_writes = _extract_sql_tables(resolved_command)

            # Filter by schema allowlist
            resolved_reads = {t for t in resolved_reads if _filter_table_by_schema(t)}
            resolved_writes = {t for t in resolved_writes if _filter_table_by_schema(t)}

            # Create resolved chains
            if resolved_reads or resolved_writes:
                # If both reads and writes, create transformation chain
                if resolved_reads and resolved_writes:
                    for r_table in resolved_reads:
                        for w_table in resolved_writes:
                            if r_table != w_table:
                                proc_ref = processor_numbers[pid]
                                chains.append(
                                    {
                                        "source_table": r_table,
                                        "target_table": w_table,
                                        "processors": [
                                            {
                                                "id": pid,
                                                "name": p["name"],
                                                "number": proc_ref,
                                            }
                                        ],
                                        "processor_count": 1,
                                        "kind": "resolved-processing",
                                        "processor_type": p["type"].split(".")[-1],
                                        "resolved_sql": (
                                            resolved_command[:150] + "..."
                                            if len(resolved_command) > 150
                                            else resolved_command
                                        ),
                                        "config_processors": [
                                            {
                                                "id": cp,
                                                "number": processor_numbers[cp],
                                                "name": config_processors[cp]["name"],
                                            }
                                            for cp in connected_configs
                                        ],
                                    }
                                )
                # If only writes (like REFRESH), create self-referencing chain
                elif resolved_writes:
                    for w_table in resolved_writes:
                        proc_ref = processor_numbers[pid]
                        chains.append(
                            {
                                "source_table": w_table,
                                "target_table": w_table,
                                "processors": [
                                    {"id": pid, "name": p["name"], "number": proc_ref}
                                ],
                                "processor_count": 1,
                                "kind": "resolved-processing",
                                "processor_type": p["type"].split(".")[-1],
                                "resolved_sql": (
                                    resolved_command[:150] + "..."
                                    if len(resolved_command) > 150
                                    else resolved_command
                                ),
                                "config_processors": [
                                    {
                                        "id": cp,
                                        "number": processor_numbers[cp],
                                        "name": config_processors[cp]["name"],
                                    }
                                    for cp in connected_configs
                                ],
                            }
                        )

        # Also handle direct SQL processors with variable resolution
        elif any(
            sql_type in p["type"].lower() for sql_type in ["executesql", "putsql"]
        ):
            for prop_key, prop_value in direct_props.items():
                if isinstance(prop_value, str) and any(
                    sql_term in prop_key.lower()
                    for sql_term in ["sql", "query", "statement"]
                ):
                    resolved_sql = resolve_variables_in_text(
                        prop_value, connected_configs
                    )
                    resolved_reads, resolved_writes = _extract_sql_tables(resolved_sql)

                    resolved_reads = {
                        t for t in resolved_reads if _filter_table_by_schema(t)
                    }
                    resolved_writes = {
                        t for t in resolved_writes if _filter_table_by_schema(t)
                    }

                    for r_table in resolved_reads:
                        for w_table in resolved_writes:
                            if r_table != w_table:
                                proc_ref = processor_numbers[pid]
                                chains.append(
                                    {
                                        "source_table": r_table,
                                        "target_table": w_table,
                                        "processors": [
                                            {
                                                "id": pid,
                                                "name": p["name"],
                                                "number": proc_ref,
                                            }
                                        ],
                                        "processor_count": 1,
                                        "kind": "resolved-processing",
                                        "processor_type": p["type"].split(".")[-1],
                                        "resolved_sql": (
                                            resolved_sql[:150] + "..."
                                            if len(resolved_sql) > 150
                                            else resolved_sql
                                        ),
                                        "config_processors": [
                                            {
                                                "id": cp,
                                                "number": processor_numbers[cp],
                                                "name": config_processors[cp]["name"],
                                            }
                                            for cp in connected_configs
                                        ],
                                    }
                                )

    # De-dup chains by (src,tgt,proc_ids)
    def key(c):
        return (
            c["source_table"],
            c["target_table"],
            tuple(p["id"] for p in c["processors"]),
        )

    uniq = {}
    for c in chains:
        uniq[key(c)] = c
    chains = list(uniq.values())

    # Critical tables by connectivity
    table_usage: Dict[str, Dict[str, Any]] = {}
    for p in procs.values():
        for t in p["reads"]:
            table_usage.setdefault(t, {"readers": set(), "writers": set()})
            table_usage[t]["readers"].add(p["name"])
        for t in p["writes"]:
            table_usage.setdefault(t, {"readers": set(), "writers": set()})
            table_usage[t]["writers"].add(p["name"])
    critical = []
    for t, u in table_usage.items():
        connectivity = len(u["readers"]) + len(u["writers"])
        critical.append(
            {
                "table": t,
                "connectivity": connectivity,
                "in_degree": len(u["readers"]),
                "out_degree": len(u["writers"]),
                "processors": sorted(list(u["readers"] | u["writers"])),
            }
        )
    critical.sort(key=lambda x: x["connectivity"], reverse=True)

    return {
        "table_lineage": {
            "total_tables": len(all_tables),
            "total_files": 0,  # For compatibility with old interface
            "lineage_chains": sorted(
                chains,
                key=lambda c: (
                    c["processor_count"],
                    c["source_table"],
                    c["target_table"],
                ),
            ),
            "critical_tables": critical[:10],
            "all_tables": sorted(all_tables),
            "all_files": [],  # For compatibility with old interface
        },
        "debug": {
            "processor_count": len(procs),
            "connection_count": sum(len(v) for v in adj.values()),
            "procs_with_reads": sum(1 for p in procs.values() if p["reads"]),
            "procs_with_writes": sum(1 for p in procs.values() if p["writes"]),
        },
    }


# ---------- Compatibility functions for existing code ----------


def build_complete_nifi_graph_with_tables(xml_content: str):
    """Compatibility function - returns a mock graph object"""

    class MockGraph:
        def __init__(self, analysis):
            self.analysis = analysis

        def number_of_nodes(self):
            return self.analysis.get("debug", {}).get("processor_count", 0)

        def number_of_edges(self):
            return self.analysis.get("debug", {}).get("connection_count", 0)

    analysis = analyze_nifi_table_lineage(xml_content)
    return MockGraph(analysis)


def analyze_complete_workflow_with_tables(graph, k=10):
    """Compatibility function - extracts analysis from mock graph"""
    return graph.analysis


def generate_table_lineage_report(analysis: Dict[str, Any]) -> str:
    """Generate a detailed markdown report from table lineage analysis with processor numbers"""
    if "table_lineage" not in analysis:
        return "# Table Lineage Report\n\nNo table lineage data available."

    data = analysis["table_lineage"]
    lines = [
        "# 🗄️ Table Lineage Analysis Report",
        "",
        f"**Analysis Summary:**",
        f"- **Total Tables**: {data['total_tables']}",
        f"- **Resolved Chains**: {len(data['lineage_chains'])}",
        f"- **Critical Tables**: {len(data['critical_tables'])}",
        "",
    ]

    # All Tables
    if data["all_tables"]:
        lines.extend(
            [
                "## 📋 Tables Found",
                "",
            ]
        )
        for table in sorted(data["all_tables"]):
            lines.append(f"- `{table}`")
        lines.append("")

    # Resolved lineage chains with processor numbers
    if data["lineage_chains"]:
        lines.extend(
            [
                "## 🔄 Resolved Data Processing Chains",
                "",
                "End-to-end table lineage with resolved SQL operations:",
                "",
            ]
        )
        for i, chain in enumerate(data["lineage_chains"], 1):
            proc_info = chain["processors"][0]
            proc_type = chain.get("processor_type", "unknown")

            # Format: table → Processor Name [Type] → table (much clearer than meaningless numbers)
            chain_text = f"{chain['source_table']} → {proc_info['name']} [{proc_type}] → {chain['target_table']}"
            lines.append(f"{i}. {chain_text}")

            # Add resolved SQL if available
            if "resolved_sql" in chain and chain["resolved_sql"]:
                sql_preview = (
                    chain["resolved_sql"][:100] + "..."
                    if len(chain["resolved_sql"]) > 100
                    else chain["resolved_sql"]
                )
                lines.append(f"   - **Resolved SQL**: `{sql_preview}`")

            # Add config processors if available
            if "config_processors" in chain and chain["config_processors"]:
                config_info = ", ".join(
                    [
                        f"{cp['name']} ({cp['number']})"
                        for cp in chain["config_processors"]
                    ]
                )
                lines.append(f"   - **Configuration**: {config_info}")

            lines.append(
                f"   - **Complexity**: {chain['processor_count']} processing steps"
            )
            lines.append("")

    return "\n".join(lines)


def generate_simple_lineage_report(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a compatible structure for migration orchestrator"""
    report_text = generate_table_lineage_report(analysis)

    # Return a structure compatible with the old NetworkX version
    if "table_lineage" in analysis:
        data = analysis["table_lineage"]
        return {
            "connection_analysis": report_text,
            "connection_summary": {
                "total_tables": data.get("total_tables", 0),
                "lineage_chains": len(data.get("lineage_chains", [])),
                "critical_tables": len(data.get("critical_tables", [])),
                "complexity_reduction": f"{data.get('total_tables', 0)} tables analyzed",
            },
        }
    else:
        return {
            "connection_analysis": report_text,
            "connection_summary": {
                "total_tables": 0,
                "lineage_chains": 0,
                "critical_tables": 0,
                "complexity_reduction": "No analysis available",
            },
        }
