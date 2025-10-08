#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NiFi table lineage (no NetworkX, no external deps).

What it does
------------
1) Parses NiFi template XML (using <processors> containers)
2) Resolves ${vars} from upstream UpdateAttribute processors
3) Extracts tables only from real SQL/HQL (ExecuteSQL, PutSQL, ExecuteStreamCommand)
4) Builds atomic table->table edges per processor (reads x writes), optionally inter-processor chains
5) Filters noise (aliases like p.cnt, file-ish tokens, long strings, ${...}, etc.)
6) Applies a dynamic schema allowlist (seeded with known-good schemas)
7) Writes two CSVs:
   - all_chains_*.csv         (e.g., 130 for your flow)
   - domain_only_chains_*.csv (e.g., 26 for your flow; excludes housekeeping schemas)
8) Reports longest composed table paths (graph over the atomic edges)

Usage
-----
python nifi_table_lineage.py ICN8_Track-out_time_based_loading.xml --outdir out \
    --write-inter-chains=0
"""

import argparse
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Set, Tuple

from .table_extraction import extract_all_tables_from_nifi_xml
from .xml_tools import parse_nifi_template_impl

# ---------------- Table extraction helpers ----------------

# Accept db.table or db.schema.table, allow quoted/backticked/bracketed parts
_IDENT = r"(?:[`\"\[]?[A-Za-z_][\w$]*[`\"\]]?)"
TABLE_IDENT = rf"(?:{_IDENT}\.)?{_IDENT}(?:\.{_IDENT})?"  # up to 3-part

_READ_PATTERNS = [
    rf"\bfrom\s+({TABLE_IDENT})",
    rf"\bjoin\s+({TABLE_IDENT})",
    rf"\bmerge\s+into\s+({TABLE_IDENT})",  # MERGE reads target too
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

    # Allow more permissive extraction like GPT - only filter very obvious non-tables
    parts = n.split(".")
    if len(parts) >= 2:
        # Only filter single letters that are obviously not schemas (like p.cnt, t.before)
        if (
            len(parts[0]) == 1 and parts[0] in "abcdefghijkmpqrtuvwxyz"
        ):  # exclude common aliases like 'nll', 'nlt', 'scp'
            return True

    # Be much more permissive - only filter the most obvious non-tables like GPT does
    if len(parts) >= 2:
        # Only filter very obvious file/path patterns
        obvious_files = {"path", "file", "absolute", "sh", "py", "jar", "xml"}
        if parts[-1] in obvious_files:
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

    # exact SQL keywords occasionally captured
    if n in {
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
    }:
        return True

    # variables / comments / too long / path-ish
    if any(pat in n for pat in ("${", "}", "\n", "\r", "--", "/*")):
        return True
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
    """Pull SQL/HQL-like chunks from relevant processors & properties."""
    p = (ptype or "").lower()
    sqls: List[str] = []

    if "executesql" in p:
        sqls += [props.get("SQL select query") or props.get("SQL Query") or ""]

    if "putsql" in p or "puthiveql" in p:
        sqls += [
            props.get("SQL statement") or props.get("sql") or props.get("HiveQL") or ""
        ]

    if "executestreamcommand" in p:
        cmd = f"{props.get('Command Path', '')} {props.get('Command Arguments', '')}"

        # 1) quoted chunks (fixed regex)
        chunks = re.findall(r'"([^"]+)"|\'([^\']+)\'', cmd)
        sqls += [c[0] or c[1] for c in chunks if (c[0] or c[1])]

        # 2) -e / -q one-liners (beeline/impala-shell)
        for flag in ("-e", "-q", "--execute"):
            m = re.search(rf"{flag}\s+([\"'])(.+?)\1", cmd)
            if m:
                sqls.append(m.group(2))

        # 3) obvious raw SQL fallback
        if not sqls and re.search(
            r"\b(select|insert|merge|create|alter|refresh)\b", cmd, re.I
        ):
            sqls += [cmd]

        # 4) explicit sql-like props
        for k, v in props.items():
            if (
                isinstance(v, str)
                and len(v) > 10
                and any(w in k.lower() for w in ("sql", "query", "statement", "hql"))
            ):
                sqls.append(v)

    # Trim empties
    return [s for s in sqls if s and s.strip()]


# ---------------- Core extraction ----------------

KNOWN_GOOD_SCHEMAS = {
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
    "nll",
    "nlt",
}

CONTROL_SCHEMAS = {"root", "absolute", "file", "text", "impala"}

SKIP_PROCESSOR_TYPES = ("updateattribute", "logmessage", "wait", "routeonattribute")


def build_connections_from_xml_tools(
    connections_data: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    """Convert xml_tools connection format to adjacency dictionary."""
    adj: Dict[str, List[str]] = defaultdict(list)
    for conn in connections_data:
        src = conn.get("source")
        dst = conn.get("destination")
        if src and dst and src != "Unknown" and dst != "Unknown":
            adj[src].append(dst)
    return dict(adj)


def upstream_updateattributes(
    adj: Dict[str, List[str]], procs: Dict[str, Dict[str, Any]], pid: str
) -> List[str]:
    """List pids of UpdateAttribute that have a direct edge into `pid`."""
    cfgs = []
    for src, outs in adj.items():
        if (
            pid in outs
            and "updateattribute" in procs.get(src, {}).get("type", "").lower()
        ):
            cfgs.append(src)
    return cfgs


def resolve_from_updateattrs(
    text: str, cfg_pids: List[str], procs: Dict[str, Dict[str, Any]]
) -> str:
    """Replace ${var} using properties of connected UpdateAttribute processors."""
    if not text or "${" not in text:
        return text
    resolved = text
    # build a single map of var->val from all upstream UpdateAttribute processors
    varmap = {}
    for cp in cfg_pids:
        varmap.update({k: v for k, v in procs[cp]["props"].items() if v})

    # do a couple of passes to catch simple nesting
    for _ in range(2):
        changed = False
        for k, v in varmap.items():
            needle = f"${{{k}}}"
            if needle in resolved:
                resolved = resolved.replace(needle, v)
                changed = True
        if not changed:
            break
    return resolved


def dynamic_schema_allowlist(procs: Dict[str, Dict[str, Any]]) -> Set[str]:
    counts = Counter()
    for p in procs.values():
        for t in p["reads"] | p["writes"]:
            if "." in t:
                counts[t.split(".", 1)[0]] += 1
    allow = {s for s, c in counts.items() if c >= 2}
    allow |= KNOWN_GOOD_SCHEMAS
    return allow


def filter_tables_by_schema(tables: Iterable[str], allow: Set[str]) -> Set[str]:
    out = set()
    for t in tables:
        if "." not in t:
            continue
        schema = t.split(".", 1)[0]
        if schema in allow and not _is_false_positive_table_ref(t):
            out.add(t)
    return out


def extract_atomic_chains(
    procs: Dict[str, Dict[str, Any]],
    adj: Dict[str, List[str]],
    make_inter_chains: bool = False,
):
    """
    Returns:
      chains: list of (src_table, tgt_table, [processor_ids])
    """
    allow = dynamic_schema_allowlist(procs)

    # Re-resolve tables with variable substitution for SQL-bearing processors
    for pid, p in procs.items():
        if any(
            s in p["type"].lower()
            for s in ("executesql", "putsql", "executestreamcommand", "puthiveql")
        ):
            cfgs = upstream_updateattributes(adj, procs, pid)
            # resolve SQL-bearing property values
            resolved_reads, resolved_writes = set(), set()
            for sql in _sql_snippets(p["type"], p["props"]):
                sql_resolved = resolve_from_updateattrs(sql, cfgs, procs)
                r, w = _extract_sql_tables(sql_resolved)
                resolved_reads |= r
                resolved_writes |= w
            p["reads"] = filter_tables_by_schema(resolved_reads or p["reads"], allow)
            p["writes"] = filter_tables_by_schema(resolved_writes or p["writes"], allow)
        else:
            p["reads"] = filter_tables_by_schema(p["reads"], allow)
            p["writes"] = filter_tables_by_schema(p["writes"], allow)

    # Atomic chains per processor (1-hop edges)
    chains = []
    for pid, p in procs.items():
        if any(s in p["type"].lower() for s in SKIP_PROCESSOR_TYPES):
            continue
        if not (p["reads"] and p["writes"]):
            continue
        for r in p["reads"]:
            for w in p["writes"]:
                if r != w:
                    chains.append((r, w, (pid,)))

    # Optional: inter-processor two-hop chains (src writes X, dst reads X)
    if make_inter_chains:
        for src_pid, outs in adj.items():
            sp = procs.get(src_pid)
            if not sp or not sp.get("writes"):
                continue
            if any(s in sp["type"].lower() for s in SKIP_PROCESSOR_TYPES):
                continue
            for dst_pid in outs:
                dp = procs.get(dst_pid)
                if not dp or not dp.get("reads"):
                    continue
                if any(s in dp["type"].lower() for s in SKIP_PROCESSOR_TYPES):
                    continue
                common = sp["writes"] & dp["reads"]
                if not common:
                    continue
                for x in common:
                    for w in dp.get("writes", set()) or {x}:
                        if w != x:
                            chains.append((x, w, (src_pid, dst_pid)))
    # De-dup
    uniq = {}
    for c in chains:
        uniq[(c[0], c[1], tuple(c[2]))] = c
    chains = list(uniq.values())
    return chains


# ---------------- Reporting / CSV ----------------


def schema_of(table: str) -> str:
    return table.split(".", 1)[0] if "." in table else table


def domain_only(
    chains: List[Tuple[str, str, Tuple[str, ...]]],
) -> List[Tuple[str, str, Tuple[str, ...]]]:
    out = []
    for s, t, pids in chains:
        if schema_of(s) in CONTROL_SCHEMAS:
            continue
        if schema_of(t) in CONTROL_SCHEMAS:
            continue
        out.append((s, t, pids))
    return out


def write_csv(
    path: str,
    chains: List[Tuple[str, str, Tuple[str, ...]]],
    procs: Dict[str, Dict[str, Any]],
):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(
            [
                "source_table",
                "target_table",
                "processor_ids",
                "processors",
                "chain_type",
                "hop_count",
            ]
        )
        for s, t, pids in sorted(chains, key=lambda x: (x[0], x[1], len(x[2]))):
            names = " -> ".join(procs[p]["name"] if p in procs else p for p in pids)
            w.writerow(
                [
                    s,
                    t,
                    " -> ".join(pids),
                    names,
                    "inter" if len(pids) > 1 else "intra",
                    len(pids),
                ]
            )


def longest_paths(edges: Dict[str, Set[str]], max_examples: int = 5):
    """DFS longest simple paths on table graph."""
    best_len, best_paths = 0, []
    nodes = set(edges.keys())
    for vs in edges.values():
        nodes |= vs

    def dfs(node, visited, path):
        nonlocal best_len, best_paths
        visited.add(node)
        path.append(node)
        outs = edges.get(node, set())
        extended = False
        for nxt in outs:
            if nxt not in visited:
                extended = True
                dfs(nxt, visited, path)
        if not extended:
            if len(path) > best_len:
                best_len, best_paths = len(path), [path.copy()]
            elif len(path) == best_len and len(best_paths) < max_examples:
                best_paths.append(path.copy())
        path.pop()
        visited.remove(node)

    for n in nodes:
        dfs(n, set(), [])
    return best_len, best_paths


def analyze_nifi_table_lineage(
    xml_path: str,
    outdir: str = ".",
    write_inter_chains: bool = True,
    table_results: List[Dict[str, Any]] | None = None,
):
    """
    Analyze NiFi table lineage and generate CSV reports.

    Args:
        xml_path: Path to NiFi XML template file
        outdir: Output directory for CSV files
        write_inter_chains: Whether to include inter-processor chains

    Returns:
        Dictionary containing analysis results and file paths
    """
    # Use xml_tools for consistent XML parsing
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    xml_data = parse_nifi_template_impl(xml_content)

    if table_results is None:
        table_results = extract_all_tables_from_nifi_xml(xml_path)

    table_index: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "reads": set(),
            "writes": set(),
            "unknown": set(),
            "raw": [],
            "processor_type": None,
        }
    )
    for entry in table_results:
        pid = entry.get("processor_id")
        ptype = entry.get("processor_type")
        table_name = entry.get("table_name")
        if not pid or not table_name:
            continue
        table = _clean_table(table_name)
        io_type = (entry.get("io_type") or "unknown").lower()
        if io_type == "write":
            table_index[pid]["writes"].add(table)
        elif io_type == "read":
            table_index[pid]["reads"].add(table)
        else:
            table_index[pid]["unknown"].add(table)
        table_index[pid]["raw"].append(entry)
        if ptype:
            table_index[pid]["processor_type"] = ptype

    procs = {}
    for proc in xml_data["processors"]:
        pid = proc["id"]
        base_tables = table_index.get(
            pid, {"reads": set(), "writes": set(), "unknown": set()}
        )
        reads = set(base_tables.get("reads", set()))
        writes = set(base_tables.get("writes", set()))
        unknown = set(base_tables.get("unknown", set()))
        if unknown:
            reads |= unknown

        procs[pid] = {
            "id": pid,
            "type": proc["type"],
            "name": proc["name"],
            "props": proc["properties"],
            "reads": reads,
            "writes": writes,
            "raw_tables": table_index.get(pid, {}).get("raw", []),
            "table_source_type": table_index.get(pid, {}).get("processor_type"),
        }

    # Convert xml_tools connections to adjacency dictionary
    adj = build_connections_from_xml_tools(xml_data["connections"])

    chains = extract_atomic_chains(procs, adj, make_inter_chains=write_inter_chains)
    table_lineage_links: List[Dict[str, str]] = []
    if write_inter_chains:
        for src_pid, targets in adj.items():
            src_proc = procs.get(src_pid)
            if not src_proc:
                continue
            writes = src_proc.get("writes") or set()
            if not writes:
                continue
            for dst_pid in targets:
                dst_proc = procs.get(dst_pid)
                if not dst_proc:
                    continue
                reads = dst_proc.get("reads") or set()
                overlap = writes & reads
                if not overlap:
                    continue
                for table in overlap:
                    table_lineage_links.append(
                        {
                            "table": table,
                            "writer_id": src_pid,
                            "writer_name": src_proc.get("name"),
                            "reader_id": dst_pid,
                            "reader_name": dst_proc.get("name"),
                        }
                    )

    dom_chains = domain_only(chains)

    # Write CSVs
    os.makedirs(outdir, exist_ok=True)
    all_csv = os.path.join(outdir, f"all_chains_{len(chains)}.csv")
    dom_csv = os.path.join(outdir, f"domain_only_chains_{len(dom_chains)}.csv")
    write_csv(all_csv, chains, procs)
    write_csv(dom_csv, dom_chains, procs)

    # Longest composed paths (build table graph and DFS)
    edges_all = defaultdict(set)
    edges_dom = defaultdict(set)
    for s, t, _ in chains:
        edges_all[s].add(t)
    for s, t, _ in dom_chains:
        edges_dom[s].add(t)

    la, pa = longest_paths(edges_all, max_examples=3)
    ld, pd = longest_paths(edges_dom, max_examples=3)

    # Extract processor names for connection display
    processor_names = {pid: proc["name"] for pid, proc in procs.items()}

    return {
        "processors": len(procs),
        "connections": sum(len(v) for v in adj.values()),
        "all_chains": len(chains),
        "domain_chains": len(dom_chains),
        "all_chains_csv": all_csv,
        "domain_chains_csv": dom_csv,
        "longest_path_all": {"length": la, "paths": pa},
        "longest_path_domain": {"length": ld, "paths": pd},
        "chains_data": chains,
        "domain_chains_data": dom_chains,
        "connections_data": adj,
        "processor_names": processor_names,
        "processor_tables": {
            pid: {
                "reads": sorted(info.get("reads", set())),
                "writes": sorted(info.get("writes", set())),
                "raw": info.get("raw_tables", []),
                "processor_type": info.get("type"),
                "table_source_type": info.get("table_source_type"),
                "processor_name": info.get("name"),
            }
            for pid, info in procs.items()
        },
        "table_lineage_links": table_lineage_links,
    }


def main():
    ap = argparse.ArgumentParser(
        description="NiFi table lineage (atomic edges + domain-only subset)"
    )
    ap.add_argument("xml", help="NiFi template XML file")
    ap.add_argument("--outdir", default=".", help="Output directory for CSV files")
    ap.add_argument(
        "--write-inter-chains",
        type=int,
        default=0,
        help="Also derive 2-hop inter-processor chains (default 0)",
    )
    args = ap.parse_args()

    result = analyze_nifi_table_lineage(
        args.xml, args.outdir, bool(args.write_inter_chains)
    )

    # Report
    print(f"Processors: {result['processors']}  Connections: {result['connections']}")
    print(f"Atomic chains (all): {result['all_chains']}")
    print(f"Domain-only chains:  {result['domain_chains']}")
    print(
        f"CSV written:\n  {result['all_chains_csv']}\n  {result['domain_chains_csv']}"
    )

    print(f"\nLongest composed path (ALL): {result['longest_path_all']['length']} hops")
    for p in result["longest_path_all"]["paths"]:
        print("  - " + " → ".join(p))
    print(
        f"\nLongest composed path (DOMAIN): {result['longest_path_domain']['length']} hops"
    )
    for p in result["longest_path_domain"]["paths"]:
        print("  - " + " → ".join(p))


if __name__ == "__main__":
    sys.exit(main())
