# utils/nifi_analysis_utils.py
# Utility functions for NiFi workflow analysis and understanding

import json
import os
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks
from json_repair import repair_json

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
    More strict criteria to avoid false positives.
    """
    # Primary SQL keywords that indicate actual query generation
    primary_sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER"]

    # Secondary SQL keywords (must appear with primary or have substantial content)
    secondary_sql_keywords = ["FROM", "WHERE", "JOIN", "REFRESH", "TABLE"]

    sql_keyword_found = False
    substantial_sql_content = False

    for key, value in properties.items():
        if (
            isinstance(value, str) and len(value) > 10
        ):  # Must be substantial content, not just metadata
            value_upper = value.upper()

            # Check for primary SQL keywords
            if any(keyword in value_upper for keyword in primary_sql_keywords):
                sql_keyword_found = True

            # Check for secondary keywords with substantial content
            if (
                any(keyword in value_upper for keyword in secondary_sql_keywords)
                and len(value) > 20
            ):
                substantial_sql_content = True

    # Must have either primary SQL keyword OR secondary with substantial content
    if sql_keyword_found or substantial_sql_content:
        return True

    # Check processor name for clear SQL generation indicators (stricter)
    name_upper = name.upper() if name else ""
    clear_sql_indicators = ["DERIVE", "GENERATE", "BUILD", "CONSTRUCT", "CREATE"]
    sql_terms = ["SQL", "QUERY", "STATEMENT"]

    # Must have both a generation verb AND SQL term in name
    has_generation_verb = any(
        indicator in name_upper for indicator in clear_sql_indicators
    )
    has_sql_term = any(term in name_upper for term in sql_terms)

    return has_generation_verb and has_sql_term


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


def _is_file_management_executecommand(properties: Dict[str, Any], name: str) -> bool:
    """
    Check if ExecuteStreamCommand is doing file management (not data transformation).
    File management should be classified as infrastructure_only, not data transformation.
    """
    # File management command keywords
    file_mgmt_keywords = [
        "RM",
        "DELETE",
        "REMOVE",
        "CLEAR",
        "CLEANUP",
        "MKDIR",
        "RMDIR",
        "CHMOD",
        "CHOWN",
        "MOVE",
        "COPY",
        "CP",
        "MV",
    ]

    # Check command properties for file management operations
    for key, value in properties.items():
        if isinstance(value, str):
            value_upper = value.upper()
            if any(keyword in value_upper for keyword in file_mgmt_keywords):
                return True

    # Check processor name for file management terms
    name_upper = name.upper() if name else ""
    file_mgmt_name_indicators = ["REMOVE", "DELETE", "CLEAR", "CLEANUP", "FILE"]
    if any(indicator in name_upper for indicator in file_mgmt_name_indicators):
        return True

    return False


def classify_processor_hybrid(
    processor_type: str, properties: Dict[str, Any], name: str, proc_id: str
) -> Dict[str, Any]:
    """
    Hybrid classification: Use rules for obvious cases, LLM for ambiguous ones.

    Args:
        processor_type: Full processor class name
        properties: Processor configuration properties
        name: Processor display name
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
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": "infrastructure_only",
            "actual_data_processing": f"Infrastructure work: {_get_infrastructure_description(short_type)}",
            "transforms_data_content": False,
            "business_purpose": f"System operation: {name}",
            "data_impact_level": "none",
            "key_operations": [_get_key_operation(short_type)],
            "analysis_method": "rule_based_infrastructure",
        }

    # 2. RULE-BASED: Data movement processors (move data without transformation)
    if short_type in DATA_MOVEMENT_PROCESSORS:
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": "data_movement",
            "actual_data_processing": f"Data movement: {_get_movement_description(short_type)}",
            "transforms_data_content": False,
            "business_purpose": f"Data transfer: {name}",
            "data_impact_level": "low",
            "key_operations": [_get_key_operation(short_type)],
            "analysis_method": "rule_based_movement",
        }

    # 3. RULE-BASED: Data transformation processors (always transform content)
    if short_type in DATA_TRANSFORMATION_PROCESSORS:
        return {
            "processor_type": processor_type,
            "properties": properties,
            "id": proc_id,
            "name": name,
            "data_manipulation_type": "data_transformation",
            "actual_data_processing": f"Data transformation: {_get_transformation_description(short_type)}",
            "transforms_data_content": True,
            "business_purpose": f"Data processing: {name}",
            "data_impact_level": "high",
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

        # Add processor metadata
        analysis_result.update(
            {
                "processor_type": processor_type,
                "properties": properties,
                "id": proc_id,
                "name": name,
                "analysis_method": "enhanced_llm_hybrid",
            }
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


def analyze_processors_batch(
    processors: List[Dict[str, Any]], max_batch_size: int = None
) -> List[Dict[str, Any]]:
    """
    Hybrid batch analysis: Use rule-based classification for obvious cases,
    then batch LLM analysis for ambiguous processors only.

    Args:
        processors: List of processor data to analyze
        max_batch_size: Maximum processors per batch for LLM analysis

    Returns:
        List of analysis results for all processors
    """
    if not processors:
        return []

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
            # Handle ExecuteStreamCommand with smart detection for file management
            if (
                short_type == "ExecuteStreamCommand"
                and _is_file_management_executecommand(properties, name)
            ):
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

    return all_results


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

        # Combine results with processor metadata
        results = []
        for i, proc in enumerate(processors):
            if i < len(batch_results):
                analysis = batch_results[i]
                analysis.update(
                    {
                        "processor_type": proc.get("processor_type", ""),
                        "properties": proc.get("properties", {}),
                        "id": proc.get("id", ""),
                        "name": proc.get("name", ""),
                        "analysis_method": "llm_batch_intelligent",
                    }
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
