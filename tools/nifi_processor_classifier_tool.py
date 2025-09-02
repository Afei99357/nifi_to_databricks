# tools/nifi_processor_classifier_tool.py
# Comprehensive NiFi processor classification tools using hybrid rule-based + LLM analysis

import json
import os
from datetime import datetime
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks
from json_repair import repair_json

from utils.workflow_summary import (
    print_and_save_workflow_summary,
    print_workflow_summary_from_data,
)

from .xml_tools import parse_nifi_template

# Removed langchain_core.tools import - no longer using # Removed @tool decorator - direct function call approach decorator


# Hybrid approach: Rule-based + LLM intelligence

# Rule-based classifications for obvious cases
INFRASTRUCTURE_PROCESSORS = {
    "LogMessage",
    "LogAttribute",
    "MonitorActivity",
    "Notify",
    "Wait",
    "ControlRate",
    "ThrottleRate",
    "HandleHttpRequest",
    "HandleHttpResponse",
    "RouteOnAttribute",
    "RouteOnContent",
    "DistributeLoad",
    "MergeContent",
    # Most UpdateAttribute and GenerateFlowFile are infrastructure
    "UpdateAttribute",  # 90% are just metadata (priorities, retries, mem_limit)
    "GenerateFlowFile",  # Usually just creates trigger/placeholder flowfiles
}

DATA_MOVEMENT_PROCESSORS = {
    "GetFile",
    "ListFile",
    "FetchFile",
    "PutFile",
    "GetHDFS",
    "PutHDFS",
    "FetchHDFS",
    "ListHDFS",
    "GetFTP",
    "PutFTP",
    "GetSFTP",
    "PutSFTP",
    "ConsumeKafka",
    "PublishKafka",
    "GetJMSQueue",
    "PutJMS",
}

DATA_TRANSFORMATION_PROCESSORS = {
    "EvaluateJsonPath",
    "EvaluateXPath",
    "EvaluateXQuery",
    "ConvertRecord",
    "ConvertAvroToJSON",
    "ConvertJSONToAvro",
    "SplitJson",
    "SplitXml",
    "SplitText",
    "SplitAvro",
    "ReplaceText",
    "ReplaceTextWithMapping",
    "TransformXml",
    "CalculateRecordStats",
    "LookupRecord",
    "EnrichRecord",
}

# Processors that need LLM analysis (only truly ambiguous cases)
LLM_ANALYSIS_NEEDED = {
    "ExecuteStreamCommand",  # Could be REFRESH TABLE vs SHOW TABLES vs full ETL
    "ExecuteScript",  # Depends on script content
    "ExecuteSQL",  # Could be DML vs DDL vs SELECT
    "PutSQL",  # Usually data transformation but context matters
    "GenerateTableFetch",  # Usually data movement but could be transformation
    "QueryDatabaseTable",  # Usually data movement but could transform
    "InvokeHTTP",  # Could be data API vs health check
    "PostHTTP",  # Could be data submission vs notifications
    "ExecuteProcess",  # Depends on process being executed
}


def _is_sql_generating_updateattribute(properties: Dict[str, Any], name: str) -> bool:
    """
    Check if UpdateAttribute generates SQL or processing logic (rare case).
    Most UpdateAttribute just sets metadata, but some generate SQL for downstream use.
    Focus on properties content (reliable) with name as hint only.
    """
    # PRIMARY: Check properties for actual SQL construction
    primary_sql_keywords = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "REFRESH",
        "INVALIDATE",
    ]

    sql_structure_indicators = [
        "FROM",
        "WHERE",
        "JOIN",
        "GROUP BY",
        "ORDER BY",
        "HAVING",
        "SET",
        "VALUES",
        "TABLE",
    ]

    # Look for substantial SQL statements in properties
    for key, value in properties.items():
        if isinstance(value, str) and len(value) > 30:
            value_upper = value.upper()

            # Must have both SQL keywords AND structure indicators
            has_sql_keyword = any(
                keyword in value_upper for keyword in primary_sql_keywords
            )
            has_sql_structure = any(
                indicator in value_upper for indicator in sql_structure_indicators
            )

            if has_sql_keyword and has_sql_structure:
                # Further validation: must be substantial multi-word SQL
                if len(value) > 50 and value.count(" ") > 5:
                    return True

    # SECONDARY: Name as hint for extra validation (not decision maker)
    # If name suggests infrastructure operation, be extra cautious
    name_upper = name.upper() if name else ""
    infrastructure_name_hints = [
        "FILENAME",
        "FILE NAME",
        "PATH",
        "DIRECTORY",
        "FOLDER",
        "PARAMETER",
        "VARIABLE",
        "ATTRIBUTE",
        "PROPERTY",
        "CONFIG",
        "COUNTER",
        "COUNT",
        "INDEX",
        "ID",
        "TIMESTAMP",
        "DATE",
        "STATUS",
        "FLAG",
        "MARKER",
        "TAG",
        "LABEL",
    ]

    # If name suggests infrastructure AND no clear SQL found, be more conservative
    if any(hint in name_upper for hint in infrastructure_name_hints):
        # For infrastructure-named processors, require even more explicit SQL evidence
        for key, value in properties.items():
            if isinstance(value, str) and len(value) > 100:  # Higher threshold
                value_upper = value.upper()
                # Must contain complete SQL with multiple clauses
                if (
                    any(
                        keyword in value_upper
                        for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE"]
                    )
                    and any(
                        clause in value_upper
                        for clause in ["FROM", "WHERE", "SET", "VALUES"]
                    )
                    and value.count(" ") > 10
                ):  # Very substantial SQL
                    return True
        # If no substantial SQL found despite infrastructure name hints, default to false
        return False

    return False


def _is_data_generating_generateflowfile(properties: Dict[str, Any], name: str) -> bool:
    """
    Check if GenerateFlowFile creates actual business data (rare case).
    Most GenerateFlowFile just creates trigger/control flowfiles.
    """
    # Check for business data indicators in properties
    data_indicators = ["CUSTOMER", "ORDER", "RECORD", "DATA", "CONTENT", "PAYLOAD"]

    for key, value in properties.items():
        if isinstance(value, str):
            value_upper = value.upper()
            if any(indicator in value_upper for indicator in data_indicators):
                return True

    # Check processor name for data-related terms
    name_upper = name.upper() if name else ""
    if any(indicator in name_upper for indicator in data_indicators):
        return True

    return False


def _is_sql_operation_executecommand(properties: Dict[str, Any], name: str) -> bool:
    """
    Check if ExecuteStreamCommand is performing SQL operations (data transformation).
    Focus on command properties (reliable) with name as supporting hint only.
    """
    # PRIMARY: Check properties for actual SQL/database operations
    sql_operation_keywords = [
        "REFRESH TABLE",
        "REFRESH",
        "INVALIDATE METADATA",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "UPSERT",
        "CREATE TABLE",
        "DROP TABLE",
        "ALTER TABLE",
        "SELECT",
        "IMPALA",
        "HIVE",
        "SPARK-SQL",
        "BEELINE",
        "EXECUTE QUERY",
        "RUN QUERY",
        "SQL SCRIPT",
    ]

    # Database command line tools and flags
    db_command_indicators = [
        "IMPALA-SHELL",
        "HIVE-CLI",
        "BEELINE",
        "SPARK-SQL",
        "SQLPLUS",
        "--query",
        "-q",
        "-e",
        "--execute",
        "--file",
        "-f",
    ]

    # Check all properties for SQL/database operations
    for key, value in properties.items():
        if isinstance(value, str) and len(value) > 5:  # Skip trivial values
            value_upper = value.upper()

            # Check for SQL operations or database tools
            if any(keyword in value_upper for keyword in sql_operation_keywords):
                return True
            if any(cmd in value_upper for cmd in db_command_indicators):
                return True

    # SECONDARY: Name as supporting evidence (not primary decision)
    # Only use name if properties are ambiguous
    name_upper = name.upper() if name else ""

    # Strong SQL indicators in name
    strong_sql_name_patterns = [
        "EXECUTE QUERY",
        "RUN QUERY",
        "EXECUTE SQL",
        "RUN SQL",
        "IMPALA QUERY",
        "HIVE QUERY",
        "REFRESH TABLE",
        "INVALIDATE METADATA",
        "SQL SCRIPT",
    ]

    if any(pattern in name_upper for pattern in strong_sql_name_patterns):
        return True

    # Moderate SQL indicators (need both action + context)
    sql_actions = ["EXECUTE", "RUN", "REFRESH", "CREATE", "DROP"]
    sql_contexts = ["QUERY", "SQL", "TABLE", "IMPALA", "HIVE", "METADATA"]

    has_sql_action = any(action in name_upper for action in sql_actions)
    has_sql_context = any(context in name_upper for context in sql_contexts)

    # Only trust name-based classification if both action and context are present
    return has_sql_action and has_sql_context


def _is_file_management_executecommand(properties: Dict[str, Any], name: str) -> bool:
    """
    Check if ExecuteStreamCommand is doing file management (not data transformation).
    Focus on command properties (reliable) with name as supporting hint only.
    """
    # PRIMARY: Check properties for file management commands
    file_mgmt_keywords = [
        "RM ",
        "DELETE ",
        "REMOVE ",
        "CLEAR ",
        "CLEANUP",
        "MKDIR",
        "RMDIR",
        "CHMOD",
        "CHOWN",
        "MOVE ",
        "COPY ",
        "CP ",
        "MV ",
        "FIND ",
        "LS ",
        "TAR ",
        "GZIP ",
        "UNZIP ",
    ]

    # File system paths and operations
    file_path_indicators = [
        "/TMP/",
        "/TEMP/",
        "/VAR/",
        "/HOME/",
        "/DATA/",
        "/LOG/",
        "*.TXT",
        "*.LOG",
        "*.CSV",
        "*.JSON",
        "*.XML",
        "FILESYSTEM",
        "HDFS://",
        "S3://",
        "FILE://",
    ]

    # Check all properties for file operations
    for key, value in properties.items():
        if isinstance(value, str) and len(value) > 3:
            value_upper = value.upper()

            # Check for file management commands
            if any(keyword in value_upper for keyword in file_mgmt_keywords):
                return True
            # Check for file system paths/operations
            if any(path in value_upper for path in file_path_indicators):
                return True

    # SECONDARY: Name as supporting evidence (not primary decision)
    name_upper = name.upper() if name else ""
    file_mgmt_name_indicators = [
        "REMOVE",
        "DELETE",
        "CLEAR",
        "CLEANUP",
        "FILE MANAGEMENT",
        "CLEANUP FILES",
        "DELETE FILES",
        "REMOVE FILES",
    ]

    # Only use name as hint, not definitive
    if any(indicator in name_upper for indicator in file_mgmt_name_indicators):
        return True

    return False


def _determine_impact_level(
    processor_type: str, manipulation_type: str, name: str, properties: Dict[str, Any]
) -> str:
    """
    Determine data impact level based on processor type and characteristics.
    More conservative approach to avoid over-classifying UpdateAttribute as high-impact.
    """
    short_type = (
        processor_type.split(".")[-1] if "." in processor_type else processor_type
    )

    # HIGH IMPACT: Only true data transformation processors
    if manipulation_type == "data_transformation":
        # ExecuteStreamCommand with actual SQL operations
        if short_type == "ExecuteStreamCommand":
            return "high"
        # Other data transformation processors that actually transform content
        if short_type in DATA_TRANSFORMATION_PROCESSORS:
            return "high"
        # LLM-classified as data transformation - still high but less likely to be UpdateAttribute
        return "high"

    # MEDIUM IMPACT: External processing and significant data movement
    if manipulation_type == "external_processing":
        return "medium"

    # UpdateAttribute should MAX OUT at medium, never high (even with SQL generation)
    # Most UpdateAttribute should be "none" unless it's truly generating substantial SQL
    if short_type == "UpdateAttribute":
        if (
            manipulation_type == "data_transformation"
            and _is_sql_generating_updateattribute(properties, name)
        ):
            return "medium"  # Only if it's actually generating SQL
        else:
            return "none"  # Default for UpdateAttribute is no impact

    # LOW IMPACT: Data movement
    if manipulation_type == "data_movement":
        return "low"

    # NO IMPACT: Infrastructure only
    if manipulation_type == "infrastructure_only":
        return "none"

    # Default for unknown
    return "low"


def classify_processor_hybrid(
    processor_type: str, properties: Dict[str, Any], name: str, proc_id: str
) -> Dict[str, Any]:
    """
    Type-first classification: Reliable processor type + properties, name as hint only.

    Classification Strategy:
    1. PRIMARY: Processor type (org.apache.nifi.processors.*) - defines behavior class
    2. SECONDARY: Properties content - what it actually does
    3. TERTIARY: Name as hint only - user labels can be misleading

    Args:
        processor_type: Full processor class name (reliable behavior indicator)
        properties: Processor configuration properties (actual configuration)
        name: Processor display name (user-defined label - hint only)
        proc_id: Processor ID

    Returns:
        Classification result dictionary
    """
    # Extract short processor name (remove package prefix)
    short_type = (
        processor_type.split(".")[-1] if "." in processor_type else processor_type
    )

    # 1. RULE-BASED: Infrastructure processors (never transform data content)
    if short_type in INFRASTRUCTURE_PROCESSORS:
        manipulation_type = "infrastructure_only"
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": manipulation_type,
            "actual_data_processing": f"Infrastructure work: {_get_infrastructure_description(short_type)}",
            "transforms_data_content": False,
            "business_purpose": f"System operation: {name}",
            "data_impact_level": _determine_impact_level(
                processor_type, manipulation_type, name, properties
            ),
            "key_operations": [_get_key_operation(short_type)],
            "analysis_method": "rule_based_infrastructure",
        }

    # 2. RULE-BASED: Data movement processors (move data without transformation)
    if short_type in DATA_MOVEMENT_PROCESSORS:
        manipulation_type = "data_movement"
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": manipulation_type,
            "actual_data_processing": f"Data movement: {_get_movement_description(short_type)}",
            "transforms_data_content": False,
            "business_purpose": f"Data transfer: {name}",
            "data_impact_level": _determine_impact_level(
                processor_type, manipulation_type, name, properties
            ),
            "key_operations": [_get_key_operation(short_type)],
            "analysis_method": "rule_based_movement",
        }

    # 3. RULE-BASED: Data transformation processors (always transform content)
    if short_type in DATA_TRANSFORMATION_PROCESSORS:
        manipulation_type = "data_transformation"
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": manipulation_type,
            "actual_data_processing": f"Data transformation: {_get_transformation_description(short_type)}",
            "transforms_data_content": True,
            "business_purpose": f"Data processing: {name}",
            "data_impact_level": _determine_impact_level(
                processor_type, manipulation_type, name, properties
            ),
            "key_operations": [_get_key_operation(short_type)],
            "analysis_method": "rule_based_transformation",
        }

    # 4. LLM ANALYSIS: For ambiguous processors or unknown types
    return _analyze_with_enhanced_llm(
        processor_type, properties, name, proc_id, short_type
    )


def _get_infrastructure_description(short_type: str) -> str:
    """Get description for infrastructure processors."""
    descriptions = {
        "LogMessage": "logging and monitoring",
        "ControlRate": "rate limiting and flow control",
        "RouteOnAttribute": "routing decisions based on attributes",
        "Wait": "timing delays",
        "Notify": "notifications and alerts",
    }
    return descriptions.get(short_type, "system operations")


def _get_movement_description(short_type: str) -> str:
    """Get description for data movement processors."""
    descriptions = {
        "GetFile": "reading files from file system",
        "PutFile": "writing files to file system",
        "GetHDFS": "reading files from HDFS",
        "PutHDFS": "writing files to HDFS",
        "ConsumeKafka": "reading messages from Kafka",
        "PublishKafka": "publishing messages to Kafka",
    }
    return descriptions.get(short_type, "data transfer operations")


def _get_transformation_description(short_type: str) -> str:
    """Get description for data transformation processors."""
    descriptions = {
        "EvaluateJsonPath": "extracting fields from JSON data",
        "ConvertRecord": "converting between data formats",
        "SplitJson": "splitting JSON arrays into individual records",
        "ReplaceText": "modifying text content using regex",
        "EvaluateXPath": "extracting data from XML documents",
    }
    return descriptions.get(short_type, "data content transformation")


def _get_key_operation(short_type: str) -> str:
    """Get key operation for processor type."""
    operations = {
        # Infrastructure
        "LogMessage": "logging",
        "ControlRate": "rate_limiting",
        "RouteOnAttribute": "routing",
        # Movement
        "GetFile": "file_reading",
        "PutFile": "file_writing",
        "ConsumeKafka": "message_consumption",
        # Transformation
        "EvaluateJsonPath": "json_field_extraction",
        "ConvertRecord": "format_conversion",
        "SplitJson": "data_splitting",
        "ReplaceText": "text_replacement",
    }
    return operations.get(short_type, "processing")


def _analyze_with_enhanced_llm(
    processor_type: str,
    properties: Dict[str, Any],
    name: str,
    proc_id: str,
    short_type: str,
) -> Dict[str, Any]:
    """
    Enhanced LLM analysis for ambiguous processors with stricter prompt.
    """
    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    try:
        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1)

        # Enhanced context for ambiguous processors
        enhanced_prompt = f"""You are a NiFi expert. Analyze this AMBIGUOUS processor that requires careful inspection.

PROCESSOR DETAILS:
- Type: {processor_type}
- Name: {name}
- Configuration: {json.dumps(properties, indent=2)}

STRICT CLASSIFICATION RULES - Focus ONLY on actual data record/file content changes:

**DATA TRANSFORMATION** (transforms_data_content: true):
- Changes ACTUAL CONTENT of data records/files (parse JSON, execute SQL DML, modify field values)
- Extracts/modifies fields from data records
- Applies business logic to data values
- EXCLUDES: FlowFile attributes, metadata, priorities, logging

**EXTERNAL PROCESSING** (high impact):
- Calls external systems that modify actual data (SQL INSERT/UPDATE/DELETE, web APIs that transform)
- Database operations that change data content
- EXCLUDES: Connection testing, health checks, SHOW/DESCRIBE commands

**DATA MOVEMENT** (no content transformation):
- Reads/writes files/messages without changing content
- EXCLUDES: Logging about the transfer

**INFRASTRUCTURE ONLY** (transforms_data_content: false):
- Logging, monitoring, alerting
- FlowFile routing, priorities, attributes, counters, retries
- Configuration, memory settings, timeouts
- File management (rm, delete, clear, cleanup, mkdir)
- ANY UpdateAttribute that only sets metadata/routing info
- SQL queries that only check status (SHOW TABLES, health checks)

STRICT EXCLUSIONS - These are NEVER data transformation:
- UpdateAttribute setting priorities, retries, memory limits, counters
- ExecuteStreamCommand doing file cleanup (rm, delete, clear folders)
- GenerateFlowFile creating control/timing flowfiles without business data
- Any operation that only changes FlowFile metadata, not record content

SPECIFIC GUIDANCE FOR AMBIGUOUS TYPES:
- UpdateAttribute: Only transformation if properties contain substantial SQL (>20 chars with SELECT/INSERT/etc)
- ExecuteStreamCommand: Only transformation if command modifies actual data tables (not file management)
- GenerateFlowFile: Only transformation if creating records with business content (not triggers/timers)

EXAMPLES:
- UpdateAttribute(priority=high): infrastructure_only, just metadata
- UpdateAttribute(mem_limit=2GB): infrastructure_only, just configuration
- UpdateAttribute(retry_count=${{retry_count:plus(1)}}): infrastructure_only, just counter
- UpdateAttribute(sql=SELECT customer_id, amount FROM orders WHERE date > ${{date}}): data_transformation, substantial SQL
- ExecuteStreamCommand("rm /tmp/old_files"): infrastructure_only, file cleanup
- ExecuteStreamCommand("REFRESH TABLE orders"): external_processing, updates data
- ExecuteStreamCommand("SHOW TABLES"): infrastructure_only, just checking
- GenerateFlowFile(empty with timer attributes): infrastructure_only, just triggers workflow
- GenerateFlowFile(customer_data with records): data_transformation, creates business records

BE STRICT: Only classify as transformation if it changes actual data record/file CONTENT.

Return ONLY a JSON object:
{{
  "data_manipulation_type": "data_transformation | data_movement | infrastructure_only | external_processing",
  "actual_data_processing": "Detailed description of what happens to data content",
  "transforms_data_content": true/false,
  "business_purpose": "What this accomplishes in business terms",
  "data_impact_level": "high | medium | low | none",
  "key_operations": ["list", "of", "key", "operations"]
}}"""

        print(f"🧠 [HYBRID LLM] Analyzing ambiguous processor: {name} ({short_type})")
        response = llm.invoke(enhanced_prompt)

        # Parse LLM response with recovery strategies
        try:
            analysis_result = json.loads(response.content.strip())
        except json.JSONDecodeError:
            content = response.content.strip()
            analysis_result = None
            # Recovery strategies
            recovery_strategies = [
                ("json-repair", lambda c: json.loads(repair_json(c))),
                (
                    "markdown extraction",
                    lambda c: (
                        json.loads(
                            repair_json(c.split("```json")[1].split("```")[0].strip())
                        )
                        if "```json" in c
                        else None
                    ),
                ),
                (
                    "boundary detection",
                    lambda c: (
                        json.loads(repair_json(c[c.find("{") : c.rfind("}") + 1]))
                        if c.find("{") >= 0 and c.rfind("}") > c.find("{")
                        else None
                    ),
                ),
            ]

            for strategy_name, strategy_func in recovery_strategies:
                try:
                    repaired_content = strategy_func(content)
                    if repaired_content:
                        analysis_result = repaired_content
                        print(f"🔧 [HYBRID LLM] Recovered JSON using {strategy_name}")
                        break
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue

            if analysis_result is None:
                raise ValueError(
                    "All JSON recovery attempts failed for enhanced LLM analysis"
                )

        # Add processor metadata and override impact level with conservative rules
        manipulation_type = analysis_result.get("data_manipulation_type", "unknown")

        analysis_result.update(
            {
                "processor_type": processor_type,
                "properties": properties,
                "id": proc_id,
                "name": name,
                "analysis_method": "enhanced_llm_hybrid",
            }
        )

        # Override LLM's impact level with conservative rules
        analysis_result["data_impact_level"] = _determine_impact_level(
            processor_type, manipulation_type, name, properties
        )

        print(
            f"✅ [HYBRID LLM] Successfully analyzed {name}: {analysis_result.get('data_manipulation_type')}"
        )
        return analysis_result

    except Exception as e:
        print(f"❌ [HYBRID LLM] Analysis failed for {name}: {str(e)}")
        # Fallback to conservative classification
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": "unknown",
            "actual_data_processing": f"Enhanced LLM analysis failed: {str(e)}",
            "transforms_data_content": False,
            "business_purpose": f"Unknown processor: {name}",
            "data_impact_level": "unknown",
            "key_operations": ["analysis_failed"],
            "analysis_method": "hybrid_llm_fallback",
            "error": str(e),
        }


# Removed @tool decorator - direct function call approach
def analyze_processors_batch(
    processors: List[Dict[str, Any]], max_batch_size: int = None
) -> str:
    """
    Hybrid batch analysis: Use rule-based classification for obvious cases,
    then batch LLM analysis for ambiguous processors only.

    Args:
        processors: List of processor data to analyze
        max_batch_size: Maximum processors per batch for LLM analysis

    Returns:
        JSON string with analysis results for all processors
    """
    if not processors:
        return json.dumps([])

    print(
        f"🔄 [HYBRID ANALYSIS] Starting hybrid analysis for {len(processors)} processors..."
    )

    # Step 1: Separate processors by classification approach needed
    rule_based_results = []
    llm_needed_processors = []

    for proc in processors:
        processor_type = proc.get("processor_type", "")
        properties = proc.get("properties", {})
        name = proc.get("name", "")
        proc_id = proc.get("id", "")

        short_type = (
            processor_type.split(".")[-1] if "." in processor_type else processor_type
        )

        # Check if we can classify with rules (with smart exceptions)
        if (
            short_type in DATA_MOVEMENT_PROCESSORS
            or short_type in DATA_TRANSFORMATION_PROCESSORS
        ):
            # Always use rules for clear data movement/transformation processors
            result = classify_processor_hybrid(
                processor_type, properties, name, proc_id
            )
            rule_based_results.append(result)
            print(
                f"📋 [RULE-BASED] {name} ({short_type}) → {result['data_manipulation_type']}"
            )

        elif short_type in INFRASTRUCTURE_PROCESSORS:
            # Smart handling for UpdateAttribute and GenerateFlowFile
            if short_type == "UpdateAttribute" and _is_sql_generating_updateattribute(
                properties, name
            ):
                # Rare case: UpdateAttribute generates SQL - needs LLM analysis
                print(f"🔍 [SMART DETECTION] {name} generates SQL - sending to LLM")
                llm_needed_processors.append(proc)
            elif (
                short_type == "GenerateFlowFile"
                and _is_data_generating_generateflowfile(properties, name)
            ):
                # Rare case: GenerateFlowFile creates business data - needs LLM analysis
                print(f"🔍 [SMART DETECTION] {name} generates data - sending to LLM")
                llm_needed_processors.append(proc)
            else:
                # Normal case: Infrastructure processor
                result = classify_processor_hybrid(
                    processor_type, properties, name, proc_id
                )
                rule_based_results.append(result)
                print(
                    f"📋 [RULE-BASED] {name} ({short_type}) → {result['data_manipulation_type']}"
                )

        else:
            # Handle ExecuteStreamCommand with smart detection
            if short_type == "ExecuteStreamCommand":
                if _is_sql_operation_executecommand(properties, name):
                    # SQL operations should be data transformation with high impact
                    result = {
                        "processor_type": processor_type,
                        "properties": properties,
                        "id": proc_id,
                        "name": name,
                        "data_manipulation_type": "data_transformation",
                        "actual_data_processing": f"SQL operation: {name}",
                        "transforms_data_content": True,
                        "business_purpose": f"Database operation: {name}",
                        "data_impact_level": "high",
                        "key_operations": ["sql_execution"],
                        "analysis_method": "smart_detection_sql_operation",
                    }
                    rule_based_results.append(result)
                    print(
                        f"🔍 [SMART DETECTION] {name} is SQL operation - classified as data transformation"
                    )
                elif _is_file_management_executecommand(properties, name):
                    # File management commands should be infrastructure, not data transformation
                    result = {
                        "processor_type": processor_type,
                        "properties": properties,
                        "id": proc_id,
                        "name": name,
                        "data_manipulation_type": "infrastructure_only",
                        "actual_data_processing": f"File management: {_get_infrastructure_description('ExecuteStreamCommand')}",
                        "transforms_data_content": False,
                        "business_purpose": f"File system operation: {name}",
                        "data_impact_level": "none",
                        "key_operations": ["file_management"],
                        "analysis_method": "smart_detection_file_mgmt",
                    }
                    rule_based_results.append(result)
                    print(
                        f"🔍 [SMART DETECTION] {name} is file management - classified as infrastructure"
                    )
                else:
                    # Ambiguous ExecuteStreamCommand needs LLM analysis
                    llm_needed_processors.append(proc)
            else:
                # Truly ambiguous processors need LLM analysis
                llm_needed_processors.append(proc)

    print(
        f"✅ [RULE-BASED] Classified {len(rule_based_results)} processors using rules"
    )
    print(f"🧠 [LLM NEEDED] {len(llm_needed_processors)} processors need LLM analysis")

    # Step 2: Batch analyze the ambiguous processors with LLM
    llm_results = []
    if llm_needed_processors:
        if max_batch_size is None:
            max_batch_size = int(os.environ.get("MAX_PROCESSORS_PER_CHUNK", "20"))

        # Process ambiguous processors in batches
        if len(llm_needed_processors) <= max_batch_size:
            llm_results = _analyze_hybrid_llm_batch(llm_needed_processors)
        else:
            print(
                f"📦 [LLM CHUNKING] Splitting {len(llm_needed_processors)} ambiguous processors into batches..."
            )

            for i in range(0, len(llm_needed_processors), max_batch_size):
                batch = llm_needed_processors[i : i + max_batch_size]
                batch_num = (i // max_batch_size) + 1
                total_batches = (
                    len(llm_needed_processors) + max_batch_size - 1
                ) // max_batch_size

                print(
                    f"🧠 [LLM BATCH {batch_num}/{total_batches}] Analyzing {len(batch)} ambiguous processors..."
                )
                batch_results = _analyze_hybrid_llm_batch(batch)
                llm_results.extend(batch_results)
                print(f"✅ [LLM BATCH {batch_num}/{total_batches}] Completed")

    # Step 3: Combine results
    all_results = rule_based_results + llm_results

    # Statistics
    rule_count = len(rule_based_results)
    llm_count = len(llm_results)
    total_count = len(all_results)

    print(f"🎉 [HYBRID COMPLETE] Analysis finished:")
    print(f"    📋 Rule-based: {rule_count} processors")
    print(f"    🧠 LLM analysis: {llm_count} processors")
    print(f"    🏆 Total: {total_count} processors")

    # Show efficiency gains
    if total_count > 0:
        efficiency = (rule_count / total_count) * 100
        print(f"    ⚡ Efficiency: {efficiency:.1f}% classified without LLM")

    return json.dumps(all_results, indent=2)


def _analyze_hybrid_llm_batch(processors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Batch analyze ambiguous processors that need LLM inspection.
    Uses single LLM call to analyze multiple processors at once.
    """
    if not processors:
        return []

    print(
        f"🧠 [HYBRID BATCH] Processing {len(processors)} ambiguous processors in single LLM call..."
    )

    # Use the existing proven batch analysis function for ambiguous processors
    # This reuses the efficient batch LLM approach
    results = _analyze_single_batch(processors)

    # Update analysis method to indicate hybrid batch processing
    for result in results:
        if result.get("analysis_method") == "llm_batch_intelligent":
            result["analysis_method"] = "hybrid_batch_llm"

    return results


def _analyze_single_batch(processors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze a single batch of processors in one LLM call.
    This is separated to handle both single and chunked batch scenarios.
    """
    if not processors:
        return []

    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    try:
        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1)

        # Prepare batch analysis prompt
        processors_summary = []
        for i, proc in enumerate(processors):
            processors_summary.append(
                f"""
PROCESSOR {i+1}:
- Name: {proc.get('name', 'unnamed')}
- Type: {proc.get('processor_type', 'unknown')}
- Properties: {json.dumps(proc.get('properties', {}), indent=2)}
"""
            )

        batch_prompt = f"""You are a NiFi data engineering expert. Analyze these {len(processors)} processors in batch to understand what each ACTUALLY does with data.

PROCESSORS TO ANALYZE:
{''.join(processors_summary)}

For each processor, focus on DATA MANIPULATION vs INFRASTRUCTURE:
- Does this processor TRANSFORM the actual data content? (change structure, extract fields, convert formats)
- Does this processor just MOVE data from A to B? (file ingestion, storage, network transfer)
- Does this processor do INFRASTRUCTURE work? (logging, routing, delays, authentication)

Return ONLY a JSON array with one object per processor:
[
  {{
    "processor_index": 0,
    "data_manipulation_type": "data_transformation | data_movement | infrastructure_only | external_processing",
    "actual_data_processing": "Detailed description of what happens to the data content",
    "transforms_data_content": true/false,
    "business_purpose": "What this accomplishes in business terms",
    "data_impact_level": "high | medium | low | none",
    "key_operations": ["list", "of", "key", "operations"]
  }},
  ...
]

Examples:
- GetFile: moves files, data_movement, no content transformation
- EvaluateJsonPath: extracts JSON fields, data_transformation, changes data structure
- LogMessage: pure logging, infrastructure_only, no data impact
- ExecuteStreamCommand with SQL: transforms data, external_processing, high impact

Be specific about what happens to the actual data content, not just metadata or routing."""

        print(
            f"🧠 [BATCH ANALYSIS] Analyzing {len(processors)} processors in single LLM call..."
        )
        response = llm.invoke(batch_prompt)

        # Parse LLM response with JSON recovery strategies
        try:
            batch_results = json.loads(response.content.strip())
        except json.JSONDecodeError:
            content = response.content.strip()
            batch_results = None
            # Try various recovery strategies in order (same as migration_tools.py)
            recovery_strategies = [
                ("json-repair", lambda c: json.loads(repair_json(c))),
                (
                    "markdown extraction",
                    lambda c: (
                        json.loads(
                            repair_json(c.split("```json")[1].split("```")[0].strip())
                        )
                        if "```json" in c
                        else None
                    ),
                ),
                (
                    "boundary detection",
                    lambda c: (
                        json.loads(repair_json(c[c.find("[") : c.rfind("]") + 1]))
                        if c.find("[") >= 0 and c.rfind("]") > c.find("[")
                        else None
                    ),
                ),
            ]

            for strategy_name, strategy_func in recovery_strategies:
                try:
                    repaired_content = strategy_func(content)
                    if repaired_content:
                        batch_results = repaired_content
                        print(f"🔧 [LLM BATCH] Recovered JSON using {strategy_name}")
                        break
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue

            if batch_results is None:
                raise ValueError("All JSON recovery attempts failed")

        # Combine results with processor metadata and conservative impact levels
        results = []
        for i, proc in enumerate(processors):
            if i < len(batch_results):
                analysis = batch_results[i]
                manipulation_type = analysis.get("data_manipulation_type", "unknown")

                analysis.update(
                    {
                        "processor_type": proc.get("processor_type", ""),
                        "properties": proc.get("properties", {}),
                        "id": proc.get("id", ""),
                        "name": proc.get("name", ""),
                        "analysis_method": "llm_batch_intelligent",
                    }
                )

                # Override LLM's impact level with conservative rules
                analysis["data_impact_level"] = _determine_impact_level(
                    proc.get("processor_type", ""),
                    manipulation_type,
                    proc.get("name", ""),
                    proc.get("properties", {}),
                )

                results.append(analysis)
            else:
                # Fallback if batch didn't return enough results
                results.append(
                    {
                        "processor_type": proc.get("processor_type", ""),
                        "properties": proc.get("properties", {}),
                        "id": proc.get("id", ""),
                        "name": proc.get("name", ""),
                        "data_manipulation_type": "unknown",
                        "actual_data_processing": "Batch analysis incomplete",
                        "transforms_data_content": False,
                        "business_purpose": "Analysis failed",
                        "data_impact_level": "unknown",
                        "key_operations": ["batch_analysis_failed"],
                        "analysis_method": "fallback_batch_incomplete",
                    }
                )

        print(f"✅ [BATCH ANALYSIS] Successfully analyzed {len(results)} processors")
        return results

    except Exception as e:
        print(f"❌ [BATCH ANALYSIS] Batch analysis failed: {str(e)}")
        # Sub-batch fallback to reduce per-processor LLM calls (same as migration_tools.py)
        sub_batch_size = int(os.environ.get("LLM_SUB_BATCH_SIZE", "10"))
        if len(processors) > sub_batch_size:
            print(
                f"🔄 [SUB-BATCH FALLBACK] Trying smaller batches of {sub_batch_size} processors..."
            )
            results = []
            for start in range(0, len(processors), sub_batch_size):
                subset = processors[start : start + sub_batch_size]
                batch_num = (start // sub_batch_size) + 1
                total_batches = (len(processors) + sub_batch_size - 1) // sub_batch_size
                print(
                    f"🧠 [SUB-BATCH {batch_num}/{total_batches}] Analyzing {len(subset)} processors..."
                )

                try:
                    # Reuse batch function for each sub-batch
                    sub_results = _analyze_single_batch(subset)
                    results.extend(sub_results)
                    print(f"✅ [SUB-BATCH {batch_num}/{total_batches}] Success!")
                except Exception:
                    # If sub-batch still fails, fall back to per-processor for this subset only
                    print(
                        f"⚠️ [SUB-BATCH {batch_num}/{total_batches}] Failed, processing individually..."
                    )
                    for idx, proc in enumerate(subset):
                        try:
                            # Single processor batch
                            single_result = _analyze_single_batch([proc])
                            if single_result:
                                results.extend(single_result)
                            else:
                                raise ValueError(
                                    "Single processor batch returned empty"
                                )
                        except Exception as proc_error:
                            print(
                                f"❌ [INDIVIDUAL] Failed for {proc.get('name', 'unnamed')}: {proc_error}"
                            )
                            # Create error result for failed processor
                            results.append(
                                {
                                    "processor_type": proc.get("processor_type", ""),
                                    "properties": proc.get("properties", {}),
                                    "id": proc.get("id", ""),
                                    "name": proc.get("name", ""),
                                    "data_manipulation_type": "unknown",
                                    "actual_data_processing": f"Analysis failed: {str(proc_error)}",
                                    "transforms_data_content": False,
                                    "business_purpose": "Analysis failed",
                                    "data_impact_level": "unknown",
                                    "key_operations": ["analysis_failed"],
                                    "analysis_method": "fallback_failed",
                                    "error": str(proc_error),
                                }
                            )
            return results
        else:
            # If batch size is already small, fall back to individual processing
            print("🔄 [INDIVIDUAL FALLBACK] Processing each processor individually...")
            results = []
            for proc in processors:
                try:
                    single_result = _analyze_single_batch([proc])
                    if single_result:
                        results.extend(single_result)
                    else:
                        raise ValueError("Single processor batch returned empty")
                except Exception as proc_error:
                    print(
                        f"❌ [INDIVIDUAL] Failed for {proc.get('name', 'unnamed')}: {proc_error}"
                    )
                    results.append(
                        {
                            "processor_type": proc.get("processor_type", ""),
                            "properties": proc.get("properties", {}),
                            "id": proc.get("id", ""),
                            "name": proc.get("name", ""),
                            "data_manipulation_type": "unknown",
                            "actual_data_processing": f"Analysis failed: {str(proc_error)}",
                            "transforms_data_content": False,
                            "business_purpose": "Analysis failed",
                            "data_impact_level": "unknown",
                            "key_operations": ["analysis_failed"],
                            "analysis_method": "fallback_failed",
                            "error": str(proc_error),
                        }
                    )
            return results


# Removed @tool decorator - direct function call approach
def analyze_workflow_patterns(
    xml_path: str, save_markdown: bool = True, output_dir: str = None
) -> str:
    """
    Comprehensive workflow analysis with automatic markdown report generation.

    Args:
        xml_path: Path to the NiFi XML template file
        save_markdown: Whether to save a markdown analysis report (default: True)
        output_dir: Optional output directory for analysis files

    Returns:
        JSON string with complete workflow analysis results
    """
    print(f"🔍 [WORKFLOW ANALYSIS] Starting analysis of: {xml_path}")

    # Read XML file
    with open(xml_path, "r") as f:
        xml_content = f.read()

    # Extract processors
    template_data = json.loads(parse_nifi_template.func(xml_content))
    processors = template_data["processors"]
    print(f"📊 [WORKFLOW ANALYSIS] Found {len(processors)} processors")

    # Analyze processors using hybrid approach
    analysis_results = json.loads(
        analyze_processors_batch.func(processors, max_batch_size=20)
    )

    # Create complete analysis data
    workflow_analysis = {
        "workflow_metadata": {
            "filename": os.path.basename(xml_path),
            "xml_path": xml_path,
            "total_processors": len(processors),
            "analysis_timestamp": f"{datetime.now().isoformat()}",
        },
        "classification_results": analysis_results,
        "total_processors": len(analysis_results),
        "workflow_characteristics": {
            "classification_breakdown": _get_classification_breakdown(analysis_results),
            "impact_analysis": _get_impact_breakdown(analysis_results),
        },
    }

    # Determine output paths
    if output_dir is None:
        output_dir = os.path.dirname(xml_path)

    base_filename = os.path.splitext(os.path.basename(xml_path))[0]
    json_path = os.path.join(output_dir, f"{base_filename}_workflow_analysis.json")

    # Save JSON analysis
    with open(json_path, "w") as f:
        json.dump(workflow_analysis, f, indent=2)
    print(f"💾 [WORKFLOW ANALYSIS] Analysis saved to: {json_path}")

    # Print summary to console and save markdown if requested
    if save_markdown:
        # Use unified function that prints AND saves markdown
        markdown_path = print_and_save_workflow_summary(
            json_path, save_markdown=True, output_path=None
        )
        print(f"📄 [WORKFLOW ANALYSIS] Markdown report: {markdown_path}")
    else:
        # Just print to console
        print_workflow_summary_from_data(workflow_analysis)

    return json.dumps(workflow_analysis, indent=2)


def _get_classification_breakdown(
    analysis_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Get breakdown of processor classifications."""
    breakdown = {
        "data_transformation": 0,
        "data_movement": 0,
        "infrastructure_only": 0,
        "external_processing": 0,
        "unknown": 0,
    }

    for result in analysis_results:
        classification = result.get("data_manipulation_type", "unknown")
        if classification in breakdown:
            breakdown[classification] += 1
        else:
            breakdown["unknown"] += 1

    return breakdown


def _get_impact_breakdown(analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get breakdown of processor impact levels."""
    breakdown = {"high": 0, "medium": 0, "low": 0, "none": 0}

    for result in analysis_results:
        impact = result.get("data_impact_level", "low")
        if impact in breakdown:
            breakdown[impact] += 1
        else:
            breakdown["low"] += 1

    return breakdown
