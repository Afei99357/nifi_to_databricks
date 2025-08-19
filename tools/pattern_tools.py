# tools/pattern_tools.py
# Pattern lookups and code-generation utilities that use the UC-backed registry.

from __future__ import annotations

import json
from typing import Any, Dict

from langchain_core.tools import tool

# Import UC-backed registry (aliased for clarity)
from registry import PatternRegistryUC as _UCRegistry

# Lazy-initialized registry instance
_registry = None

def _get_registry():
    """Get the pattern registry, initializing it lazily."""
    global _registry
    if _registry is None:
        try:
            _registry = _UCRegistry()
        except RuntimeError as e:
            # If Unity Catalog/Spark is not available, use a fallback
            if "SparkSession not available" in str(e):
                _registry = _FallbackRegistry()
            else:
                raise
    return _registry

class _FallbackRegistry:
    """Fallback registry for when Unity Catalog is not available."""
    def __init__(self):
        self._patterns = {}
    
    def get_pattern(self, processor_class: str):
        return self._patterns.get(processor_class)
    
    def add_pattern(self, processor_class: str, pattern: dict):
        self._patterns[processor_class] = pattern

__all__ = [
    "generate_databricks_code",
    "get_migration_pattern",
    "suggest_autoloader_options",
]


def _render_pattern(processor_class: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Look up a migration pattern from UC; if missing, auto-register a stub
    so you can fill it in later. Returns a rendered dictionary.
    """
    registry = _get_registry()
    pattern = registry.get_pattern(processor_class) or {}

    if not pattern:
        lc = processor_class.lower()
        if "getfile" in lc or "listfile" in lc:
            pattern = {
                "databricks_equivalent": "Auto Loader",
                "description": "File ingestion via Auto Loader.",
                "best_practices": [
                    "Use schemaLocation for schema tracking",
                    "Enable includeExistingFiles for initial backfill",
                    "Use cleanSource after successful processing",
                ],
                "code_template": (
                    "from pyspark.sql.functions import *\n"
                    "df = (spark.readStream\n"
                    "      .format('cloudFiles')\n"
                    "      .option('cloudFiles.format', '{format}')\n"
                    "      .option('cloudFiles.inferColumnTypes', 'true')\n"
                    "      .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')\n"
                    "      .load('{path}'))"
                ),
                "last_seen_properties": properties or {},
            }
        elif "puthdfs" in lc or "putfile" in lc:
            pattern = {
                "databricks_equivalent": "Delta Lake",
                "description": "Transactional storage in Delta.",
                "best_practices": [
                    "Partition by frequently filtered columns when useful",
                    "Compact small files (OPTIMIZE / auto-opt)",
                    "Consider Z-ORDER for skewed query patterns",
                ],
                "code_template": "df.write.format('delta').mode('{mode}').save('{path}')",
                "last_seen_properties": properties or {},
            }
        else:
            pattern = {
                "databricks_equivalent": "Unknown",
                "description": "",
                "best_practices": [],
                "code_template": (
                    "# TODO: Implement conversion for {processor_class}\n"
                    "# Properties seen: {properties}\n"
                    "df = spark.read.format('delta').load('/path/to/input')\n"
                    "# ... your logic here ...\n"
                    "df.write.format('delta').mode('append').save('/path/to/output')"
                ),
                "last_seen_properties": properties or {},
            }

        # Persist new stub into UC (and the in-memory cache)
        registry.add_pattern(processor_class, pattern)

    # Render code with injected placeholders when present
    code = None
    if "code_template" in pattern:
        code = pattern["code_template"]
        injections = {
            "processor_class": processor_class,
            "properties": json.dumps(properties or {}, indent=2),
            **{k: v for k, v in (properties or {}).items()},
        }
        for k, v in injections.items():
            code = code.replace(f"{{{k}}}", str(v))

    return {
        "equivalent": pattern.get("databricks_equivalent", "Unknown"),
        "description": pattern.get("description", ""),
        "best_practices": pattern.get("best_practices", []),
        "code": code,
    }


@tool
def generate_databricks_code(processor_type: str, properties: str = "{}") -> str:
    """
    Generate equivalent Databricks/PySpark code for a NiFi processor type.
    Returns a Python code string with best-practice comments when available.
    """
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except Exception:
            properties = {}

    processor_class = processor_type.split(".")[-1] if "." in processor_type else processor_type
    rendered = _render_pattern(processor_class, properties)

    if rendered["code"]:
        code = f"# {processor_class} → {rendered['equivalent']}\n"
        if rendered["description"]:
            code += f"# {rendered['description']}\n"
        code += f"\n{rendered['code']}"
        if rendered["best_practices"]:
            code += "\n\n# Best Practices:\n" + "\n".join([f"# - {bp}" for bp in rendered["best_practices"]])
        return code

    return (
        f"# Generic processor conversion for: {processor_type}\n"
        f"# Properties: {json.dumps(properties, indent=2)}\n\n"
        "# TODO: Implement specific logic for this processor\n"
        "df = spark.read.format('delta').load('/path/to/data')\n"
        "# ... your transformations ...\n"
    )


@tool
def get_migration_pattern(nifi_component: str, properties: str = "{}") -> str:
    """
    Return a human-readable description of the migration pattern for a NiFi component.
    Includes best practices and a code template when available.
    """
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except Exception:
            properties = {}

    rendered = _render_pattern(nifi_component, properties)
    if rendered["equivalent"] != "Unknown":
        out = [
            f"**Migration Pattern: {nifi_component} → {rendered['equivalent']}**",
            "",
            rendered["description"],
        ]
        if rendered["best_practices"]:
            out += ["", "Best Practices:"]
            out += [f"- {bp}" for bp in rendered["best_practices"]]
        if rendered["code"]:
            out += ["", "Code Template:", "```python", rendered["code"], "```"]
        return "\n".join(out)

    return (
        f"**General Migration Guidelines for {nifi_component}**\n"
        "1. Identify the data flow pattern\n"
        "2. Map to equivalent Databricks components\n"
        "3. Implement error handling and monitoring\n"
        "4. Test with sample data\n"
        "5. Optimize for performance\n"
        "6. Document the migration approach\n"
    )


@tool
def suggest_autoloader_options(properties: str = "{}") -> str:
    """
    Given NiFi GetFile/ListFile-like properties, suggest Auto Loader code & tips.
    Returns JSON with keys: code, tips.
    """
    try:
        props = json.loads(properties) if properties else {}
    except Exception:
        props = {}

    path = props.get("Input Directory") or props.get("Directory") or "/mnt/raw"
    fmt_guess = (props.get("File Filter") or "*.json").split(".")[-1].lower()
    if fmt_guess in ["csv"]:
        fmt = "csv"
    elif fmt_guess in ["json"]:
        fmt = "json"
    else:
        fmt = "parquet"

    code = (
        'from pyspark.sql.functions import *\n'
        'df = (spark.readStream\n'
        '      .format("cloudFiles")\n'
        f'      .option("cloudFiles.format", "{fmt}")\n'
        '      .option("cloudFiles.inferColumnTypes", "true")\n'
        '      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\n'
        f'      .load("{path}"))'
    )
    tips = [
        "Use cloudFiles.schemaLocation for checkpoint/schema tracking.",
        "Use cloudFiles.includeExistingFiles=true to backfill once.",
        "Set cloudFiles.validateOptions for strictness; cleanSource MOVE/DELETE for hygiene.",
    ]

    return json.dumps({"code": code, "tips": tips}, indent=2)
