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
            # Pattern not found - don't create stub, let LLM generation handle it
            return {
                "equivalent": "Unknown",
                "description": "",
                "best_practices": [],
                "code": None,  # This will trigger LLM generation
            }

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
def generate_databricks_code(processor_type: str, properties: str = "{}", force_regenerate: bool = False) -> str:
    """
    Generate equivalent Databricks/PySpark code for a NiFi processor type.
    Returns a Python code string with best-practice comments when available.
    
    Args:
        processor_type: NiFi processor class name
        properties: JSON string of processor properties  
        force_regenerate: If True, skip UC table lookup and force LLM generation
    """
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except Exception:
            properties = {}

    processor_class = processor_type.split(".")[-1] if "." in processor_type else processor_type
    
    # If force_regenerate is True, skip UC table lookup and use LLM directly
    if force_regenerate:
        return _generate_with_llm(processor_class, properties)
    
    rendered = _render_pattern(processor_class, properties)

    if rendered["code"]:
        code = f"# {processor_class} → {rendered['equivalent']}\n"
        if rendered["description"]:
            code += f"# {rendered['description']}\n"
        code += f"\n{rendered['code']}"
        if rendered["best_practices"]:
            code += "\n\n# Best Practices:\n" + "\n".join([f"# - {bp}" for bp in rendered["best_practices"]])
        return code

    # Pattern not found in UC table - check if LLM generation is enabled
    import os
    enable_llm_generation = os.environ.get("ENABLE_LLM_CODE_GENERATION", "false").lower() == "true"
    
    if enable_llm_generation:
        return _generate_with_llm(processor_class, properties)
    else:
        # Use simple fallback template to avoid excessive LLM calls
        return f"""# {processor_class} → Fallback Template (LLM generation disabled)
# Properties: {json.dumps(properties, indent=2)}
# To enable LLM generation, set ENABLE_LLM_CODE_GENERATION=true in .env

# TODO: Implement {processor_class} logic based on properties
df = spark.read.format('delta').load('/path/to/input')

# Add your {processor_class} transformations here
df_processed = df  # Customize based on {processor_class} behavior

df_processed.write.format('delta').mode('append').save('/path/to/output')
"""


def _get_processor_specific_guidance(processor_class: str, properties: dict) -> str:
    """Get specific guidance for common NiFi processors."""
    guidance_map = {
        "EvaluateJsonPath": """
PROCESSOR GUIDANCE - EvaluateJsonPath:
This processor extracts values from JSON using JSONPath expressions and adds them as flowfile attributes.

Key Functionality:
- Takes JSON content as input
- Uses JSONPath expressions (like $.host, $.level) to extract specific values
- Destination: 'flowfile-attribute' means extract values and add as DataFrame columns
- Return Type: 'auto-detect' means automatically determine data types
- Path Not Found Behavior: 'ignore' means don't fail if path doesn't exist
- Multiple JSONPath expressions can extract different fields simultaneously

PySpark Implementation:
- Use from_json() to parse JSON strings
- Use json functions like get_json_object() or json_tuple()
- Extract multiple fields in one operation
- Handle missing paths gracefully
""",
        "ControlRate": """
PROCESSOR GUIDANCE - ControlRate:
This processor controls the rate at which flowfiles pass through the processor.

Key Functionality:
- Limits throughput to specified rate (records per time period)
- Can group by attribute values
- Acts as a throttling mechanism

PySpark Implementation:
- Use DataFrame.limit() for simple rate limiting  
- Use window functions for time-based rate control
- Consider using Structured Streaming rate limiting options
""",
        "RouteOnAttribute": """
PROCESSOR GUIDANCE - RouteOnAttribute:
This processor routes flowfiles to different relationships based on attribute values.

Key Functionality:
- Evaluates conditions against flowfile attributes
- Routes data to different output paths based on conditions
- Can have multiple routing rules

PySpark Implementation:
- Use DataFrame.filter() with conditions
- Create multiple DataFrames for different routes
- Use when/otherwise for conditional logic
""",
        "ExecuteSQL": """
PROCESSOR GUIDANCE - ExecuteSQL:
This processor executes SQL queries against a database.

Key Functionality:
- Connects to database via JDBC
- Executes provided SQL query
- Results become flowfile content

PySpark Implementation:
- Use spark.sql() for Spark SQL
- Use DataFrame.jdbc() for external database connections
- Handle connection properties and authentication
"""
    }
    
    return guidance_map.get(processor_class, f"""
PROCESSOR GUIDANCE - {processor_class}:
Research the NiFi {processor_class} processor functionality based on its name and properties.
Generate equivalent PySpark code that performs the same data processing operations.
Focus on the specific properties provided to customize the implementation.
""")


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
        from databricks_langchain import ChatDatabricks
        import os
        
        # Get the model endpoint from environment
        model_endpoint = os.environ.get("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
        llm = ChatDatabricks(endpoint=model_endpoint)
        
        # Create processor-specific prompt with detailed guidance
        processor_guidance = _get_processor_specific_guidance(processor_class, properties)
        
        prompt = f"""You are a NiFi to Databricks migration expert. Generate specific PySpark code for the NiFi processor: {processor_class}

{processor_guidance}

Properties: {json.dumps(properties, indent=2)}

Requirements:
1. Generate equivalent PySpark/Databricks code that performs the exact same function as this NiFi processor
2. Handle ALL the provided properties appropriately in your implementation
3. Include detailed comments explaining the logic
4. Use proper Databricks patterns (Delta Lake, DataFrame operations)
5. Add error handling where relevant
6. Return ONLY the Python code, no markdown or explanations
7. Make the code functional and ready to use

Example format:
```python
# {processor_class} → [Databricks equivalent]
# [Description of what this processor does]

# Extract configuration from properties
# Handle the specific properties provided

df = spark.read.format('delta').load('/path/to/input')

# [Processor-specific transformations implementing the NiFi processor logic]

df.write.format('delta').mode('append').save('/path/to/output')
```

Generate the working PySpark code that implements {processor_class} functionality:"""

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
        # Track fallback usage for maintenance review
        _track_fallback_processor(processor_class, properties, str(e))
        
        # Fallback to improved generic template if LLM fails
        return f"""# {processor_class} → LLM Generation Failed
# Error: {str(e)}
{_format_properties_as_comments(properties)}

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


def _track_fallback_processor(processor_class: str, properties: dict, error: str) -> None:
    """Track processors that fell back to generic implementation for maintenance review."""
    try:
        import json
        import os
        from datetime import datetime
        
        # Create fallback tracking record
        fallback_record = {
            "processor_class": processor_class,
            "properties": properties,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "status": "fallback_used"
        }
        
        # Write to fallback tracking file in output directory
        # This will help maintainers identify which processors need attention
        fallback_file = "fallback_processors.jsonl"
        
        # Try to write to current working directory or temp
        try:
            with open(fallback_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(fallback_record) + "\n")
        except Exception:
            # Fallback: try to write to temp directory
            import tempfile
            fallback_file = os.path.join(tempfile.gettempdir(), "nifi_migration_fallbacks.jsonl")
            with open(fallback_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(fallback_record) + "\n")
                
        print(f"⚠️  Fallback used for {processor_class}. Tracked in: {fallback_file}")
        
    except Exception as track_error:
        print(f"Warning: Could not track fallback for {processor_class}: {track_error}")


def _save_generated_pattern(processor_class: str, properties: dict, generated_code: str) -> None:
    """
    Optionally save the LLM-generated pattern to UC table for future reuse.
    This builds up the pattern registry over time.
    """
    try:
        # Use the global registry to ensure tables are created once
        registry = _get_registry()
        
        # Debug: Show what type of registry we got
        print(f"🔍 [DEBUG] Registry type: {type(registry).__name__}")
        
        # Only save if we have a UC registry (not fallback)
        if hasattr(registry, 'add_pattern') and hasattr(registry, 'spark'):
            if registry.spark:
                try:
                    app_name = registry.spark.sparkContext.appName
                    print(f"🔍 [DEBUG] SparkSession available: {app_name}")
                except Exception:
                    print(f"🔍 [DEBUG] SparkSession available (Spark Connect mode)")
                
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
                registry.add_pattern(processor_class, pattern)
                print(f"💾 [PATTERN SAVED] {processor_class} → UC table")
            else:
                print(f"⚠️  [DEBUG] No SparkSession - UC tables cannot be created")
        else:
            print(f"⚠️  [DEBUG] Using fallback registry - no UC tables created")
        
    except Exception as e:
        print(f"❌ [DEBUG] Pattern save error: {e}")
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

    result = {"code": code, "tips": tips, "continue_required": False, "tool_name": "suggest_autoloader_options"}
    return json.dumps(result, indent=2)
