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
            "properties": _format_properties_as_comments(properties or {}),
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

    # Pattern not found in UC table - use LLM generation
    return _generate_with_llm(processor_class, properties)


def _generate_with_llm(processor_class: str, properties: dict) -> str:
    """
    Generate processor-specific PySpark code using LLM when pattern is not in UC table.
    
    Args:
        processor_class: NiFi processor class name (e.g., "ControlRate", "ValidateRecord")
        properties: Processor configuration properties
        
    Returns:
        Generated PySpark code with comments and logic
    """
    try:
        # Import here to avoid circular dependencies
        from langchain_databricks import ChatDatabricks
        import os
        
        # Get the model endpoint from environment
        model_endpoint = os.environ.get("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
        llm = ChatDatabricks(endpoint=model_endpoint)
        
        # Create a comprehensive prompt for code generation
        prompt = f"""You are a NiFi to Databricks migration expert. Generate specific PySpark code for the NiFi processor: {processor_class}

Processor: {processor_class}
Properties: {json.dumps(properties, indent=2)}

Requirements:
1. Research what this NiFi processor does based on its name and properties
2. Generate equivalent PySpark/Databricks code that performs the same function
3. Include detailed comments explaining the logic
4. Use proper Databricks patterns (Delta Lake, Structured Streaming when appropriate)
5. Handle the specific properties provided
6. Add error handling where relevant
7. Return ONLY the Python code, no markdown or explanations

Example format:
```python
# {processor_class} → [Databricks equivalent]
# [Description of what this processor does]

# Extract configuration from properties
property_1 = {properties.get('Property1', 'default_value')}

# [Implementation logic]
df = spark.read.format('delta').load('/path/to/input')

# [Processor-specific transformations based on properties]

# Write output
df.write.format('delta').mode('append').save('/path/to/output')
```

Generate the code:"""

        # Call the LLM to generate code
        response = llm.invoke(prompt)
        generated_code = response.content.strip()
        
        # Clean up the response - remove markdown if present
        if generated_code.startswith('```python'):
            generated_code = generated_code.replace('```python\n', '').replace('\n```', '')
        elif generated_code.startswith('```'):
            generated_code = generated_code.replace('```\n', '').replace('\n```', '')
        
        # Add header comment
        header = f"# {processor_class} → LLM Generated Code\n# Generated based on processor properties and NiFi documentation\n\n"
        
        # Optionally save the generated pattern to UC table for future use
        _save_generated_pattern(processor_class, properties, generated_code)
        
        return header + generated_code
        
    except Exception as e:
        # Fallback to improved generic template if LLM fails
        return f"""# {processor_class} → LLM Generation Failed
# Error: {str(e)}
# Properties: {json.dumps(properties, indent=2)}

# Fallback implementation - please customize based on processor functionality
df = spark.read.format('delta').load('/path/to/input')

# TODO: Implement {processor_class} logic based on properties:
{_generate_property_comments(properties)}

# Basic passthrough - customize based on {processor_class} behavior
df_processed = df  # Add your transformations here

df_processed.write.format('delta').mode('append').save('/path/to/output')
"""


def _format_properties_as_comments(properties: dict) -> str:
    """Format properties dictionary as properly commented Python code."""
    if not properties:
        return "# No properties configured"
    
    # Generate properly commented JSON-like format
    lines = ["# {"]
    for key, value in properties.items():
        if isinstance(value, str):
            lines.append(f'#   "{key}": "{value}",')
        elif value is None:
            lines.append(f'#   "{key}": null,')
        else:
            lines.append(f'#   "{key}": {value},')
    lines.append("# }")
    
    return "\n".join(lines)


def _generate_property_comments(properties: dict) -> str:
    """Generate helpful comments about processor properties."""
    if not properties:
        return "# No properties configured"
    
    comments = []
    for key, value in properties.items():
        if value is not None:
            comments.append(f"# - {key}: {value}")
        else:
            comments.append(f"# - {key}: (not set)")
    
    return "\n".join(comments)


def _save_generated_pattern(processor_class: str, properties: dict, generated_code: str) -> None:
    """
    Optionally save the LLM-generated pattern to UC table for future reuse.
    This builds up the pattern registry over time.
    """
    try:
        from registry.pattern_registry import PatternRegistryUC
        
        # Create a pattern from the generated code
        pattern = {
            "category": "llm_generated",
            "databricks_equivalent": "LLM Generated Solution",
            "description": f"Auto-generated pattern for {processor_class} based on properties analysis",
            "code_template": generated_code,
            "best_practices": [
                "Review and customize the generated code",
                "Test thoroughly before production use",
                "Consider processor-specific optimizations"
            ],
            "generated_from_properties": properties,
            "generation_source": "llm_hybrid_approach"
        }
        
        # Save to UC table
        registry = PatternRegistryUC()
        registry.add_pattern(processor_class, pattern)
        
    except Exception:
        # Silent fail - saving is optional
        pass


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
