"""
External Script Extraction for NiFi workflows.
Extracts script files, executable paths, and external dependencies that need migration planning.
Based on asset_extraction.py but focused specifically on scripts and executables.
"""

import base64
import json
import re
import shlex
from typing import Any, Dict, List, Set

# Enhanced patterns for improved detection
EL_PATTERN = re.compile(r"\$\{[^}]+\}")
QUERY_REF_RE = re.compile(r"\$\{(?P<name>query_\d+(?:_[A-Za-z][A-Za-z0-9_]*)?)\}")

# Processor types that commonly contain scripts
SCRIPTY_PROCESSOR_HINTS = [
    "executescript",
    "invokescriptedprocessor",
    "executestreamcommand",
    "scriptedlookupservice",
    "scriptedrecordreader",
    "scriptedrecordsetwriter",
    "jolttransformjson",
    "executegroovyscript",
    "updateattribute",
]

# Property name patterns that likely contain inline code
SCRIPT_PROPERTY_HINTS = [
    "script",
    "script body",
    "script text",
    "script code",
    "query",
    "sql",
    "statement",
    "hql",
    "command",
    "command arguments",
    "args",
    "groovy",
    "jython",
    "python",
    "javascript",
    "inline",
    "expression",
    "spec",
    "query_1",
    "query_2",
    "query_3",
    "query_4",
    "query_5",
]

# Language detection patterns
LANG_SIGS = [
    (
        "sql",
        [
            r"\bSELECT\b.*\bFROM\b",
            r"\bINSERT\s+INTO\b",
            r"\bUPDATE\s+.*\bSET\b",
            r"\bCREATE\s+TABLE\b",
            r"\bALTER\s+TABLE\b",
            r"\bDROP\s+.*\bPARTITION\b",
            r"\bTRUNCATE\s+TABLE\b",
            r"\bREFRESH\b",
            r"\bWITH\b.*\bAS\s*\(",
        ],
    ),
    (
        "bash",
        [
            r"^\s*#!/bin/(ba)?sh",
            r"\b(echo|grep|awk|sed)\b",
            r"\|\s*(grep|awk|sed)\b",
            r"impala-shell.*-q",
            r"-[A-Za-z]+;",
        ],
    ),
    (
        "python",
        [
            r"\bdef\s+\w+\s*\(",
            r"\bimport\s+\w+",
            r"\bfrom\s+.*\bimport\b",
            r"if\s+__name__\s*==",
        ],
    ),
    (
        "groovy",
        [
            r"@Grab\(",
            r"\bprintln\s*\(",
            r"groovy\.",
        ],
    ),
    (
        "jolt",
        [
            r"^\s*\[?\s*\{",
            r'"operation"\s*:',
            r'"shift"',
        ],
    ),
]

# Secret redaction patterns
SECRET_PATS = [
    (
        re.compile(
            r"(?i)\b(password|passwd|pwd|secret|token|apikey)\s*=\s*['\"]?([^'\"\\s]+)['\"]?"
        ),
        r"\1='***'",
    ),
    (
        re.compile(r"(?i)aws_secret_access_key\s*=\s*['\"]?[^'\"\\s]+['\"]?"),
        "aws_secret_access_key='***'",
    ),
    (re.compile(r"(?i)jdbc:[^\\s]+:([^@]+)@"), r"jdbc:***:***@"),
]

# Script and executable patterns
SCRIPT_EXTENSIONS = [
    ".sh",
    ".py",
    ".sql",
    ".jar",
    ".class",
    ".scala",
    ".R",
    ".pl",
    ".rb",
]
SCRIPT_PATH_PATTERN = re.compile(r"/[\w/.-]+\.(?:sh|py|sql|jar|class|scala|R|pl|rb)")
EXECUTABLE_COMMANDS = re.compile(
    r"\b(?:bash|sh|python|java|scala|spark-submit|hdfs|impala-shell|beeline|hive)\b"
)
HOST_PATTERN = re.compile(r"\b(?:[a-zA-Z0-9-]+\.){1,}[a-zA-Z]{2,}\b")


def _normalize_el(text: str, replacement: str = "ELVAR") -> str:
    """Replace ${variables} with placeholder for script detection."""
    return EL_PATTERN.sub(replacement, text)


def _maybe_base64_decode(s: str) -> str:
    """Try to decode base64 content if it looks encoded."""
    if not s:
        return None
    t = s.strip()
    if len(t) < 32 or len(t) % 4 != 0 or re.search(r"[^A-Za-z0-9+/=\s]", t):
        return None
    try:
        raw = base64.b64decode(t, validate=True)
        if not raw:
            return None
        txt = raw.decode("utf-8", errors="ignore")
        # Require some letters and whitespace to consider it "text"
        if sum(c.isalpha() for c in txt) > 10 and ("\n" in txt or " " in txt):
            return txt
    except Exception:
        return None
    return None


def extract_query_refs(text: str) -> List[str]:
    """Extract query property references from text like ${query_5_analyzeStaging}"""
    return list({m.group("name") for m in QUERY_REF_RE.finditer(text or "")})


def build_query_index(processors: List[Dict[str, Any]]) -> Dict[str, Dict[str, tuple]]:
    """Build index of query properties for cross-referencing"""
    idx = {}
    for p in processors:
        group = p.get("parentGroupName", "Root")
        props = p.get("properties", {})
        for k, v in props.items():
            if isinstance(k, str) and re.match(r"query_\d+", k.lower()):
                idx.setdefault(group, {})[k] = (p.get("id"), p.get("name"), k, v)
    return idx


def _dedupe_inline(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate inline scripts based on processor+property+content"""
    seen = set()
    out = []
    for it in items:
        # Use first 120 chars of normalized content as key
        content_key = _normalize_el(it.get("content", ""))[:120]
        key = (
            it.get("processor_id", ""),
            it.get("property_name", "").lower().replace(" (decoded)", ""),
            content_key,
        )
        if key not in seen:
            out.append(it)
            seen.add(key)
    return out


def split_command_args(cmd: str, args: str) -> List[str]:
    """Safely parse command and arguments, handling shell quoting"""
    try:
        tokens = []
        if cmd:
            tokens.extend(shlex.split(cmd))
        if args:
            # Handle semicolon-separated args (common in NiFi)
            if ";" in args and '"' not in args:
                tokens.extend(args.split(";"))
            else:
                tokens.extend(shlex.split(args))
        return tokens
    except Exception:
        # Fallback to simple split
        return ((cmd or "") + " " + (args or "")).split()


def extract_hosts_from_impala(tokens: List[str]) -> List[str]:
    """Extract host:port from impala-shell command arguments"""
    hosts = []
    for i, t in enumerate(tokens):
        if t in ("-i", "--impalad") and i + 1 < len(tokens):
            hosts.append(tokens[i + 1])
    return hosts


def redact_secrets(text: str) -> str:
    """Redact sensitive information from script content"""
    if not text:
        return text
    for pat, repl in SECRET_PATS:
        text = pat.sub(repl, text)
    return text


def guess_script_type(text: str) -> tuple[str, float]:
    """Detect script language with confidence score."""
    if not text:
        return ("unknown", 0.3)

    t = text if len(text) < 20000 else text[:20000]
    score_map = {}
    for lang, pats in LANG_SIGS:
        score = 0
        for p in pats:
            if re.search(p, t, re.IGNORECASE | re.MULTILINE):
                score += 1
        score_map[lang] = score

    if not score_map or all(v == 0 for v in score_map.values()):
        return ("unknown", 0.3)

    lang, score = max(score_map.items(), key=lambda kv: kv[1])
    conf = min(0.95, 0.25 + 0.15 * score)
    return (lang if score > 0 else "unknown", conf)


def looks_like_code(prop_name: str, value: str, processor_type: str) -> bool:
    """Determine if property contains code/script content."""
    if not value or not isinstance(value, str):
        return False

    # Normalize NiFi variables for detection
    v = _normalize_el(value)

    # Special case: query properties always considered code
    if re.match(r"query_\d+", prop_name.lower()):
        return True

    # Empty but named like script
    if len(v.strip()) < 2:
        return any(h in prop_name.lower() for h in SCRIPT_PROPERTY_HINTS)

    # Check for code patterns
    longish = len(v) >= 40 or "\n" in v
    token_hits = sum(
        bool(re.search(p, v, re.IGNORECASE))
        for p in [
            r"\b(def|function|class)\b",
            r"[{};]",
            r"\b(import|from)\b",
            r"#!/",
            r"\bSELECT\b",
            r"\bINSERT\b",
            r"\bALTER\b",
            r"\|\s*grep\b",
            r"\bawk\b",
            r"\bsed\b",
        ]
    )

    name_hint = any(h in prop_name.lower() for h in SCRIPT_PROPERTY_HINTS)
    type_hint = any(h in processor_type.lower() for h in SCRIPTY_PROCESSOR_HINTS)

    # ExecuteStreamCommand special case
    is_exec_stream = "executestreamcommand" in processor_type.lower()

    return (
        (longish and (token_hits >= 1 or name_hint or type_hint))
        or is_exec_stream
        or name_hint
    )


def _is_script_false_positive(value: str) -> bool:
    """Filter out non-script patterns and obvious false positives."""
    if not value or not isinstance(value, str):
        return True

    value = value.strip()

    # Skip empty or very short values
    if len(value) < 3:
        return True

    # Skip regex patterns
    if value.startswith("^") or value.endswith("$") or ".*" in value or "\\." in value:
        return True

    # Skip variable references without actual paths
    if "${" in value and "}" in value and not value.startswith("/"):
        return True

    # Skip wildcards and glob patterns
    if "*" in value or "?" in value or "[" in value:
        return True

    # Skip multiple items in one value
    if ";" in value and "/" not in value:  # Allow shell commands with semicolons
        return True

    # Skip error messages or descriptive text
    error_indicators = [
        "failed",
        "error",
        "exception",
        "unable to",
        "cannot",
        "not found",
    ]
    if any(indicator in value.lower() for indicator in error_indicators):
        return True

    # Skip configuration-like values
    config_indicators = ["true", "false", "null", "none", "auto", "default"]
    if value.lower() in config_indicators:
        return True

    return False


def _extract_scripts_from_command(command_text: str) -> Set[str]:
    """Extract script paths from command line text."""
    scripts = set()

    if not command_text or _is_script_false_positive(command_text):
        return scripts

    # Look for script file paths in command arguments
    script_matches = SCRIPT_PATH_PATTERN.findall(command_text)
    for script_path in script_matches:
        if not _is_script_false_positive(script_path):
            scripts.add(script_path.strip())

    # Look for relative script names that might be in working directory
    lines = command_text.split("\n")
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):  # Skip comments
            continue

        # Split by spaces to find script arguments
        tokens = line.split()
        for token in tokens:
            token = token.strip("\"'`;")  # Remove quotes and separators
            if any(token.endswith(ext) for ext in SCRIPT_EXTENSIONS):
                if not _is_script_false_positive(token):
                    scripts.add(token)

    return scripts


def _classify_script_type(script_path: str) -> str:
    """Classify script by file extension and naming patterns."""
    script_lower = script_path.lower()

    if script_lower.endswith(".sh"):
        return "shell_script"
    elif script_lower.endswith(".py"):
        return "python_script"
    elif script_lower.endswith(".sql"):
        return "sql_script"
    elif script_lower.endswith(".jar"):
        return "java_application"
    elif script_lower.endswith(".class"):
        return "java_class"
    elif script_lower.endswith(".scala"):
        return "scala_script"
    elif script_lower.endswith(".R"):
        return "r_script"
    elif script_lower.endswith(".pl"):
        return "perl_script"
    elif script_lower.endswith(".rb"):
        return "ruby_script"
    else:
        return "unknown_script"


def _detect_inline_script(prop_name: str, prop_value: str) -> Dict[str, Any]:
    """Detect if a property contains inline script content rather than a file path."""
    if not prop_value or len(prop_value.strip()) < 5:  # Reduced minimum length
        return None

    prop_name_lower = prop_name.lower()

    # Property names that typically contain inline scripts
    inline_script_properties = [
        "script body",
        "script_body",
        "scriptbody",
        "script content",
        "script_content",
        "scriptcontent",
        "script text",
        "script_text",
        "scripttext",
        "script code",
        "script_code",
        "scriptcode",
        "query",
        "sql",
        "statement",
        "sql statement",
        "sql_statement",
        "sqlstatement",
        "sql select query",
        "sql select statement",
        "command body",
        "command_body",
        "commandbody",
        "python script",
        "python_script",
        "pythonscript",
        "shell script",
        "shell_script",
        "shellscript",
        "groovy script",
        "groovy_script",
        "groovyscript",
        "hql statement",
        "hql_statement",
        "hqlstatement",
        "batch statement",
        "batch_statement",
        "batchstatement",
        # Common NiFi property names
        "query timeout",
        "sql pre query",
        "sql post query",
        "pre-query",
        "post-query",
    ]

    # Check if property name suggests inline script
    has_script_property_name = any(
        keyword in prop_name_lower for keyword in inline_script_properties
    )

    # Detect script patterns in content
    script_indicators = [
        # Python patterns
        r"import\s+\w+",
        r"from\s+\w+\s+import",
        r"def\s+\w+\s*\(",
        r"if\s+__name__\s*==",
        # Shell patterns
        r"#!/bin/(bash|sh)",
        r"echo\s+",
        r"for\s+\w+\s+in",
        r"if\s*\[\s*.+\s*\]",
        # SQL patterns
        r"SELECT\s+",
        r"INSERT\s+INTO",
        r"UPDATE\s+",
        r"CREATE\s+TABLE",
        r"FROM\s+\w+",
        # Java/Groovy patterns
        r"public\s+class",
        r"import\s+java\.",
        r"System\.out\.print",
        # General programming patterns
        r"\{\s*\n.*\n.*\}",
        r"=\s*function\s*\(",
        r"var\s+\w+\s*=",
    ]

    has_script_patterns = any(
        re.search(pattern, prop_value, re.IGNORECASE | re.MULTILINE)
        for pattern in script_indicators
    )

    # Multi-line content is likely script code, but SQL can be single line too
    has_multiple_lines = "\n" in prop_value.strip() and len(prop_value.split("\n")) > 1

    # For SQL, even single line statements can be important
    is_sql_like = any(
        keyword in prop_name_lower for keyword in ["sql", "query", "statement", "hql"]
    )

    # Not a file path (doesn't start with / or contain common path patterns)
    is_not_file_path = not (
        prop_value.startswith("/")
        or prop_value.startswith("./")
        or any(ext in prop_value for ext in SCRIPT_EXTENSIONS)
    )

    # Determine if this looks like inline script content
    if (
        (
            has_script_property_name
            or has_script_patterns
            or (is_sql_like and len(prop_value) > 20)
        )
        and (has_multiple_lines or is_sql_like)
        and is_not_file_path
    ):
        # Determine script type
        script_type = "unknown"
        if re.search(r"import\s+\w+|def\s+\w+|python", prop_value, re.IGNORECASE):
            script_type = "python"
        elif re.search(
            r"#!/bin/(bash|sh)|echo\s+|bash|shell", prop_value, re.IGNORECASE
        ):
            script_type = "shell"
        elif re.search(r"SELECT|INSERT|UPDATE|CREATE|sql", prop_value, re.IGNORECASE):
            script_type = "sql"
        elif re.search(
            r"import\s+java|public\s+class|groovy", prop_value, re.IGNORECASE
        ):
            script_type = "java_groovy"

        return {
            "property_name": prop_name,
            "script_type": script_type,
            "content_preview": (
                prop_value[:200] + "..." if len(prop_value) > 200 else prop_value
            ),
            "content_length": len(prop_value),
            "line_count": len(prop_value.split("\n")),
        }

    return None


def _extract_external_hosts_from_scripts(script_content: str) -> Set[str]:
    """Extract external host dependencies from script content."""
    hosts = set()

    if not script_content or _is_script_false_positive(script_content):
        return hosts

    # Extract URLs and hosts
    try:
        # Look for URLs
        url_pattern = re.compile(r"https?://([^/\s]+)")
        url_matches = url_pattern.findall(script_content)
        for host in url_matches:
            hosts.add(host.lower())

        # Look for host patterns (domains)
        host_matches = HOST_PATTERN.findall(script_content)
        for host in host_matches:
            # Filter out obvious non-hosts
            if not any(
                skip in host.lower()
                for skip in ["example.com", "localhost", "test.com"]
            ):
                hosts.add(host.lower())

    except Exception:
        pass  # Continue if regex fails

    return hosts


def extract_scripts_from_processor(
    processor: Dict[str, Any], query_index: Dict = None
) -> Dict[str, Any]:
    """Extract scripts with cross-referencing and improved detection"""
    processor_id = processor.get("id", "")
    processor_name = processor.get("name", "")
    processor_type = processor.get("type", "")
    processor_group = processor.get("parentGroupName", "Root")
    properties = processor.get("properties", {})

    result = {
        "processor_id": processor_id,
        "processor_name": processor_name,
        "processor_type": (
            processor_type.split(".")[-1] if processor_type else "Unknown"
        ),
        "processor_group": processor_group,
        "inline_scripts": [],
        "external_scripts": [],
        "external_hosts": [],
        "script_count": 0,
        "has_external_dependencies": False,
        "all_properties": properties,  # Keep for debugging
    }

    inline_scripts = []
    external_hosts = []
    external_scripts = []

    # Special ExecuteStreamCommand handling
    if "executestreamcommand" in processor_type.lower():
        cmd = properties.get("Command Path", "")
        args = properties.get("Command Arguments", "")

        if cmd or args:
            combined = f"{cmd} {args}".strip()
            tokens = split_command_args(cmd, args)

            # Extract query references
            refs = extract_query_refs(combined)
            resolved_queries = []

            # Resolve query references if index provided
            if query_index and refs:
                group_idx = query_index.get(processor_group, {})
                for ref in refs:
                    if ref in group_idx:
                        proc_id, proc_name, prop_name, value = group_idx[ref]
                        resolved_queries.append(
                            {
                                "name": ref,
                                "processor": proc_name,
                                "value": value[:200] if value else "(empty)",
                            }
                        )

            # Detect language
            normalized = _normalize_el(combined)
            lang, conf = guess_script_type(normalized)

            # Override if referencing SQL queries
            if refs:
                lang = "sql"
                conf = 0.85

            # Extract external hosts for impala-shell
            if "impala-shell" in cmd:
                hosts = extract_hosts_from_impala(tokens)
                external_hosts.extend(hosts)

            inline_scripts.append(
                {
                    "property_name": "Command+Args",
                    "script_type": lang,
                    "confidence": conf,
                    "content": combined,  # Original with variables
                    "content_preview": redact_secrets(combined[:800]),
                    "line_count": combined.count("\n") + 1,
                    "referenced_queries": refs,
                    "resolved_queries": resolved_queries,
                }
            )

    # Process all properties (including query_* properties)
    for prop_name, prop_value in properties.items():
        # Handle empty query properties
        if re.match(r"query_\d+", prop_name.lower()):
            if not prop_value or len(str(prop_value).strip()) <= 1:
                inline_scripts.append(
                    {
                        "property_name": prop_name,
                        "script_type": "sql",
                        "confidence": 0.3,
                        "content": prop_value or "(empty)",
                        "content_preview": "(empty)",
                        "line_count": 0,
                    }
                )
            else:
                # Process non-empty query property
                lang, conf = guess_script_type(prop_value)
                inline_scripts.append(
                    {
                        "property_name": prop_name,
                        "script_type": "sql",  # Force SQL for query properties
                        "confidence": max(0.7, conf),
                        "content": prop_value,
                        "content_preview": redact_secrets(prop_value[:800]),
                        "line_count": prop_value.count("\n") + 1,
                    }
                )
            continue

        # Regular property processing
        if (
            prop_value
            and isinstance(prop_value, str)
            and looks_like_code(prop_name, prop_value, processor_type)
        ):
            lang, conf = guess_script_type(prop_value)
            inline_scripts.append(
                {
                    "property_name": prop_name,
                    "script_type": lang,
                    "confidence": conf,
                    "content": prop_value,
                    "content_preview": redact_secrets(prop_value[:800]),
                    "line_count": prop_value.count("\n") + 1,
                }
            )

        # Base64 detection
        decoded = _maybe_base64_decode(prop_value)
        if decoded and looks_like_code(prop_name, decoded, processor_type):
            lang, conf = guess_script_type(decoded)
            inline_scripts.append(
                {
                    "property_name": f"{prop_name} (decoded)",
                    "script_type": lang,
                    "confidence": max(0.6, conf),
                    "content": decoded,
                    "content_preview": redact_secrets(decoded[:800]),
                    "line_count": decoded.count("\n") + 1,
                }
            )

        # External script file paths
        if prop_value and isinstance(prop_value, str):
            # Direct script file paths
            if (
                any(prop_value.endswith(ext) for ext in SCRIPT_EXTENSIONS)
                and "/" in prop_value
                and not _is_script_false_positive(prop_value)
            ):
                external_scripts.append(
                    {
                        "path": prop_value,
                        "type": _classify_script_type(prop_value),
                        "property_source": prop_name,
                    }
                )

            # Script paths using regex
            script_matches = SCRIPT_PATH_PATTERN.findall(prop_value)
            for script_match in script_matches:
                if not _is_script_false_positive(script_match):
                    external_scripts.append(
                        {
                            "path": script_match,
                            "type": _classify_script_type(script_match),
                            "property_source": prop_name,
                        }
                    )

            # External host dependencies
            script_hosts = _extract_external_hosts_from_scripts(prop_value)
            external_hosts.extend(script_hosts)

    # Deduplicate inline scripts
    result["inline_scripts"] = _dedupe_inline(inline_scripts)
    result["external_scripts"] = external_scripts
    result["external_hosts"] = list(set(external_hosts))
    result["script_count"] = len(result["inline_scripts"]) + len(external_scripts)
    result["has_external_dependencies"] = bool(external_hosts)

    return result


def extract_all_scripts_from_nifi_xml(xml_path: str) -> List[Dict[str, Any]]:
    """
    Extract all script references from NiFi XML file with cross-referencing.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        List of script extraction results per processor
    """
    from tools.xml_tools import parse_nifi_template_impl

    # Parse processors from XML
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data.get("processors", [])

    # Build query index first for cross-referencing
    query_index = build_query_index(processors)

    all_script_results = []

    for processor in processors:
        script_result = extract_scripts_from_processor(processor, query_index)

        # Only include processors that have scripts or external dependencies
        if (
            script_result["script_count"] > 0
            or script_result["has_external_dependencies"]
        ):
            all_script_results.append(script_result)

    return all_script_results


def generate_script_migration_summary(script_results: List[Dict[str, Any]]) -> str:
    """Generate a migration-focused summary of script dependencies."""

    total_processors = len(script_results)
    total_scripts = sum(result["script_count"] for result in script_results)

    # Categorize scripts by type
    script_types = {}
    high_priority_scripts = []
    external_dependencies = set()

    for result in script_results:
        for script in result["scripts"]:
            script_type = script["type"]
            script_types[script_type] = script_types.get(script_type, 0) + 1

            if script["migration_priority"] == "high":
                high_priority_scripts.append(
                    {
                        "path": script["path"],
                        "processor": result["processor_name"],
                        "type": script_type,
                    }
                )

        external_dependencies.update(result["external_hosts"])

    # Generate markdown report
    lines = [
        "# NiFi Script Migration Analysis",
        "",
        "## Summary",
        f"- **Processors with Scripts**: {total_processors}",
        f"- **Total Scripts Found**: {total_scripts}",
        f"- **External Dependencies**: {len(external_dependencies)}",
        "",
    ]

    if script_types:
        lines.extend(["## Scripts by Type", ""])
        for script_type, count in sorted(script_types.items()):
            lines.append(f"- **{script_type.replace('_', ' ').title()}**: {count}")
        lines.append("")

    if high_priority_scripts:
        lines.extend(
            [
                "## High Priority Scripts (Absolute Paths)",
                "",
                "These scripts require immediate attention for migration:",
                "",
            ]
        )
        for script in high_priority_scripts[:20]:  # Limit to first 20
            lines.append(
                f"- `{script['path']}` ({script['type']}) - Used in: {script['processor']}"
            )

        if len(high_priority_scripts) > 20:
            lines.append(f"- ... and {len(high_priority_scripts) - 20} more")
        lines.append("")

    if external_dependencies:
        lines.extend(
            [
                "## External Host Dependencies",
                "",
                "External systems that scripts depend on:",
                "",
            ]
        )
        for host in sorted(external_dependencies)[:15]:  # Limit to first 15
            lines.append(f"- `{host}`")

        if len(external_dependencies) > 15:
            lines.append(f"- ... and {len(external_dependencies) - 15} more")
        lines.append("")

    lines.extend(
        [
            "## Migration Recommendations",
            "",
            "1. **High Priority Scripts**: Migrate absolute path scripts first",
            "2. **External Dependencies**: Ensure external hosts are accessible from Databricks",
            "3. **Script Types**: Consider converting shell scripts to Databricks notebooks",
            "4. **JAR Files**: Upload to Databricks libraries or DBFS",
            "5. **SQL Scripts**: Convert to Databricks SQL notebooks or jobs",
            "",
        ]
    )

    return "\n".join(lines)


def extract_scripts_summary_json(xml_path: str) -> str:
    """
    Extract scripts and return JSON summary suitable for API responses.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        JSON string with script extraction summary
    """
    try:
        script_results = extract_all_scripts_from_nifi_xml(xml_path)

        summary = {
            "total_processors_with_scripts": len(script_results),
            "total_scripts": sum(result["script_count"] for result in script_results),
            "script_types": {},
            "high_priority_scripts": [],
            "external_dependencies": [],
            "processors": script_results,
        }

        # Aggregate data
        external_deps = set()
        for result in script_results:
            for script in result["scripts"]:
                script_type = script["type"]
                summary["script_types"][script_type] = (
                    summary["script_types"].get(script_type, 0) + 1
                )

                if script["migration_priority"] == "high":
                    summary["high_priority_scripts"].append(
                        {
                            "path": script["path"],
                            "processor_name": result["processor_name"],
                            "processor_id": result["processor_id"],
                            "type": script_type,
                        }
                    )

            external_deps.update(result["external_hosts"])

        summary["external_dependencies"] = list(external_deps)

        return json.dumps(summary, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to extract scripts: {str(e)}",
                "total_processors_with_scripts": 0,
                "total_scripts": 0,
            }
        )
