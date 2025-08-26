# tools/generator_tools.py
# Code generation utilities with LLM-powered PySpark code creation.

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from typing import Any, Dict

from databricks_langchain import ChatDatabricks
from langchain_core.tools import tool

# Registry functionality removed - generate fresh every time


__all__ = [
    "generate_databricks_code",
    "get_migration_pattern",
    "suggest_autoloader_options",
]


def _get_context_aware_input_dataframe(
    processor_class: str, context: Dict[str, Any]
) -> str:
    """
    Generate context-aware input DataFrame name based on previous processors in workflow.
    """
    previous_processors = context.get("previous_processors", [])
    processor_index = context.get("processor_index", 0)

    if not previous_processors:
        # No previous processors, this is a source processor
        return "df"

    # Find the most recent previous processor that would output data
    for prev_proc in reversed(previous_processors):
        prev_type = prev_proc.get("type", "").lower()
        prev_name = prev_proc.get("name", "").lower()

        # Source processors create DataFrames
        if any(
            src in prev_type
            for src in ["getfile", "listfile", "consumekafka", "executesql"]
        ):
            safe_name = prev_name.replace(" ", "_").replace("-", "_")[:20]
            return f"df_{safe_name}"

    # Fallback: use generic df name
    return "df"


def _get_intermediate_table_name(processor_class: str, context: Dict[str, Any]) -> str:
    """
    Generate intermediate Delta table name for passing data between job tasks.
    """
    processor_name = context.get("processor_name", processor_class).lower()
    processor_index = context.get("processor_index", 0)
    project = context.get("project", "migration")
    chunk_id = context.get("chunk_id", "chunk_0")

    # Clean names for table naming
    safe_name = processor_name.replace(" ", "_").replace("-", "_")[:15]
    safe_project = project.replace(" ", "_").replace("-", "_")[:15]

    return f"temp_{safe_project}_{chunk_id}_{processor_index:02d}_{safe_name}"


def _generate_data_passing_strategy(
    processor_class: str, context: Dict[str, Any]
) -> Dict[str, str]:
    """
    Use LLM to analyze workflow context and generate appropriate data passing strategy.
    Returns data passing code snippets and TODO comments for this specific workflow.
    """
    try:
        import os

        from databricks_langchain import ChatDatabricks

        model_endpoint = os.environ.get(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1)

        previous_processors = context.get("previous_processors", [])
        processor_index = context.get("processor_index", 0)

        # Create workflow context summary for LLM
        workflow_summary = {
            "current_processor": {"type": processor_class, "index": processor_index},
            "previous_processors": [
                {"type": p.get("type", ""), "name": p.get("name", "")}
                for p in previous_processors
            ],
            "total_processors": processor_index + 1,
            "execution_context": "Databricks Jobs - each processor runs as separate Python task",
        }

        prompt = f"""You are a Databricks migration expert. Analyze this NiFi-to-Databricks workflow and generate data passing strategy.

WORKFLOW CONTEXT:
{json.dumps(workflow_summary, indent=2)}

CHALLENGE: Each processor runs as a separate Databricks job task (separate Python process), so variables can't be passed directly.

MIGRATION CONTEXT: This is a BATCH ETL workflow (not streaming). NiFi processors typically:
- Process files once, then move/delete them
- Execute commands and process results
- Handle discrete data batches, not continuous streams
- Use GetFile/ListFile for one-time file processing (not continuous monitoring)

TASK: Generate specific data passing strategy for processor "{processor_class}" at index {processor_index}.

Consider these approaches:
1. **Intermediate Delta Tables**: Save/read via temp tables (RECOMMENDED for batch ETL)
2. **Unity Catalog Tables**: Use persistent catalog tables
3. **Cloud Storage**: Save/read via Delta Lake files
4. **Job Parameters**: Pass metadata/paths as job parameters
5. **Batch DataFrames**: Use regular spark.read (NOT spark.readStream) for file ingestion

REQUIREMENTS:
1. Analyze the specific processor types in this workflow
2. Recommend the BEST data passing approach for this combination
3. Generate code snippets with proper error handling
4. Include TODO comments for manual configuration
5. Consider data volume, real-time needs, and persistence requirements

RESPONSE FORMAT (JSON):
{{
    "strategy": "brief description of chosen approach",
    "input_code": "Python code to read data from previous processor (or null if source)",
    "output_code": "Python code to save data for next processor (or null if sink)",
    "todo_comments": ["List of TODO items for manual review"],
    "reasoning": "Why this approach is best for this workflow"
}}

Generate the optimal data passing strategy:"""

        response = llm.invoke(prompt)

        # Parse LLM response
        try:
            import json

            strategy = json.loads(response.content.strip())
            return strategy
        except json.JSONDecodeError:
            # Fallback if JSON parsing fails
            return {
                "strategy": "Intermediate Delta Tables (fallback)",
                "input_code": (
                    f"# Read from previous processor\ndf = spark.read.format('delta').table('temp_workflow_{processor_index-1:02d}')"
                    if processor_index > 0
                    else None
                ),
                "output_code": f"# Save for next processor\ndf.write.format('delta').mode('overwrite').saveAsTable('temp_workflow_{processor_index:02d}')",
                "todo_comments": [
                    "Review table names and permissions",
                    "Consider data retention policies for temp tables",
                    "Verify error handling and checkpointing",
                ],
                "reasoning": "LLM response parsing failed, using safe default approach",
            }

    except Exception as e:
        # Complete fallback if LLM fails
        return {
            "strategy": "Basic Delta Tables (error fallback)",
            "input_code": (
                f"df = spark.read.format('delta').table('temp_{processor_index-1:02d}')"
                if processor_index > 0
                else None
            ),
            "output_code": f"df.write.format('delta').mode('overwrite').saveAsTable('temp_{processor_index:02d}')",
            "todo_comments": [
                f"LLM data passing analysis failed: {str(e)}",
                "Manually implement appropriate data passing strategy",
                "Consider: Delta tables, job parameters, or streaming approaches",
            ],
            "reasoning": "Error occurred during LLM analysis, using minimal fallback",
        }


def _get_template_property_mappings(
    processor_class: str, properties: Dict[str, Any]
) -> Dict[str, str]:
    """
    Map NiFi processor properties to template placeholder names.
    This ensures templates like '{path}' get replaced with actual property values.
    """
    mappings = {}
    lc = processor_class.lower()

    if "getfile" in lc or "listfile" in lc:
        # Map GetFile properties to template placeholders
        input_directory = properties.get("Input Directory", "/path/to/input")
        file_filter = properties.get("File Filter", ".*")

        # Determine format from file filter
        format_type = "json"  # default
        if "csv" in file_filter.lower():
            format_type = "csv"
        elif "parquet" in file_filter.lower():
            format_type = "parquet"
        elif "avro" in file_filter.lower():
            format_type = "avro"

        mappings.update(
            {
                "path": input_directory,
                "format": format_type,
            }
        )

    elif "puthdfs" in lc or "putfile" in lc:
        # Map PutHDFS properties
        directory = properties.get("Directory", "/path/to/output")
        mappings.update(
            {
                "Directory": directory,
                "mode": "append",  # default mode
            }
        )

    elif "consumekafka" in lc:
        # Map ConsumeKafka properties
        topics = properties.get("Topic Name(s)", "topic")
        bootstrap_servers = properties.get("Kafka Brokers", "localhost:9092")
        mappings.update(
            {
                "topics": topics,
                "bootstrap_servers": bootstrap_servers,
            }
        )

    return mappings


def _get_previous_processor_table(context: Dict[str, Any]) -> str:
    """
    Get the intermediate table name from the previous processor in the workflow.
    """
    previous_processors = context.get("previous_processors", [])

    if not previous_processors:
        return None

    # Find the most recent previous processor
    prev_proc = previous_processors[-1]
    prev_context = {
        "processor_name": prev_proc.get("name", ""),
        "processor_index": len(previous_processors) - 1,
        "project": context.get("project", "migration"),
        "chunk_id": context.get("chunk_id", "chunk_0"),
    }

    return _get_intermediate_table_name(prev_proc.get("type", "Unknown"), prev_context)


def _get_context_aware_output_dataframe(
    processor_class: str, context: Dict[str, Any]
) -> str:
    """
    Generate context-aware output DataFrame name for this processor.
    """
    processor_name = context.get("processor_name", processor_class).lower()
    processor_index = context.get("processor_index", 0)

    # Clean the processor name
    safe_name = processor_name.replace(" ", "_").replace("-", "_")[:20]

    # For source processors, use descriptive names
    if any(src in processor_class.lower() for src in ["getfile", "listfile"]):
        return f"df_{safe_name}"
    elif "consumekafka" in processor_class.lower():
        return f"df_kafka_{safe_name}"
    elif "executesql" in processor_class.lower():
        return f"df_sql_{safe_name}"
    else:
        # For processing processors, use the processor type
        return f"df_{processor_class.lower()[:10]}"


def _get_builtin_pattern(
    processor_class: str,
    properties: Dict[str, Any],
    workflow_context: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    LLM-driven pattern generation - no hardcoded templates.
    Returns indication to use LLM for all processors.
    """
    # Let LLM handle ALL processors - no hardcoded patterns needed
    # The LLM can analyze ANY processor type and generate appropriate code
    return {
        "equivalent": "LLM-Generated",
        "description": f"AI-generated code for {processor_class} based on workflow context",
        "best_practices": [],
        "code": None,  # This will trigger LLM generation
    }


@tool
def generate_databricks_code(
    processor_type: str,
    properties: str = "{}",
    force_regenerate: bool = False,
    workflow_context: str = "{}",
) -> str:
    """
    Generate equivalent Databricks/PySpark code for a NiFi processor type.
    Returns a Python code string with best-practice comments when available.

    Args:
        processor_type: NiFi processor class name
        properties: JSON string of processor properties
        force_regenerate: If True, skip builtin patterns and force LLM generation
        workflow_context: JSON string containing workflow context (previous processors, data flow)
    """
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except Exception:
            properties = {}

    # Parse workflow context
    context = {}
    if isinstance(workflow_context, str):
        try:
            context = json.loads(workflow_context)
        except Exception:
            context = {}

    processor_class = (
        processor_type.split(".")[-1] if "." in processor_type else processor_type
    )

    # If force_regenerate is True, skip builtin patterns and use LLM directly
    if force_regenerate:
        return _generate_with_llm(processor_class, properties)

    # Check for builtin patterns first with workflow context
    rendered = _get_builtin_pattern(processor_class, properties, context)

    if rendered["code"]:
        code = f"# {processor_class} → {rendered['equivalent']}\n"
        if rendered["description"]:
            code += f"# {rendered['description']}\n"
        code += f"\n{rendered['code']}"
        if rendered["best_practices"]:
            code += "\n\n# Best Practices:\n" + "\n".join(
                [f"# - {bp}" for bp in rendered["best_practices"]]
            )
        return code

    # No builtin pattern found - check if LLM generation is enabled
    enable_llm_generation = (
        os.environ.get("ENABLE_LLM_CODE_GENERATION", "false").lower() == "true"
    )

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
""",
    }

    return guidance_map.get(
        processor_class,
        f"""
PROCESSOR GUIDANCE - {processor_class}:
Research the NiFi {processor_class} processor functionality based on its name and properties.
Generate equivalent PySpark code that performs the same data processing operations.
Focus on the specific properties provided to customize the implementation.
""",
    )


def _generate_with_llm(processor_class: str, properties: dict) -> str:
    """
    Generate processor-specific PySpark code using LLM for unknown processors.

    Args:
        processor_class: NiFi processor class name (e.g., "ControlRate", "ValidateRecord")
        properties: Processor configuration properties

    Returns:
        Generated PySpark code with comments and logic
    """
    try:
        # Get the model endpoint from environment
        model_endpoint = os.environ.get(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        llm = ChatDatabricks(endpoint=model_endpoint)

        # Create processor-specific prompt with detailed guidance
        processor_guidance = _get_processor_specific_guidance(
            processor_class, properties
        )

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
        if generated_code.startswith("```python"):
            generated_code = generated_code.replace("```python\n", "").replace(
                "\n```", ""
            )
        elif generated_code.startswith("```"):
            generated_code = generated_code.replace("```\n", "").replace("\n```", "")

        # Add header comment
        header = f"# {processor_class} → LLM Generated Code\n# Generated based on processor properties and NiFi documentation\n\n"

        # Pattern generated fresh each time - no registry saving

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


def _track_fallback_processor(
    processor_class: str, properties: dict, error: str
) -> None:
    """Track processors that fell back to generic implementation for maintenance review."""
    try:
        # Create fallback tracking record
        fallback_record = {
            "processor_class": processor_class,
            "properties": properties,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "status": "fallback_used",
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
            fallback_file = os.path.join(
                tempfile.gettempdir(), "nifi_migration_fallbacks.jsonl"
            )
            with open(fallback_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(fallback_record) + "\n")

        print(f"⚠️  Fallback used for {processor_class}. Tracked in: {fallback_file}")

    except Exception as track_error:
        print(f"Warning: Could not track fallback for {processor_class}: {track_error}")


# Pattern saving removed - generate fresh each time


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

    rendered = _get_builtin_pattern(nifi_component, properties)
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
        "from pyspark.sql.functions import *\n"
        "df = (spark.readStream\n"
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

    result = {
        "code": code,
        "tips": tips,
        "continue_required": False,
        "tool_name": "suggest_autoloader_options",
    }
    return json.dumps(result, indent=2)
