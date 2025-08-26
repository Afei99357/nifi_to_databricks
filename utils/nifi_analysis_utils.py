# utils/nifi_analysis_utils.py
# Utility functions for NiFi workflow analysis and understanding

import json
import os
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks

# Removed hardcoded knowledge base - using pure LLM intelligence instead


def analyze_processors_batch(
    processors: List[Dict[str, Any]], max_batch_size: int = None
) -> List[Dict[str, Any]]:
    """
    Batch analyze multiple processors with chunking to avoid context length limits.
    Uses multiple batches if needed to handle large workflows efficiently.

    Args:
        processors: List of processor data to analyze
        max_batch_size: Maximum processors per batch to avoid context limits

    Returns:
        List of analysis results for all processors
    """
    if not processors:
        return []

    # Get batch size from environment or use default
    if max_batch_size is None:
        max_batch_size = int(os.environ.get("MAX_PROCESSORS_PER_CHUNK", "20"))

    # If small enough, analyze all in one batch
    if len(processors) <= max_batch_size:
        return _analyze_single_batch(processors)

    # For large workflows, split into chunks
    print(f"🔄 [CHUNKED BATCH] Large workflow detected: {len(processors)} processors")
    print(
        f"📦 [CHUNKED BATCH] Splitting into batches of {max_batch_size} processors..."
    )

    all_results = []
    for i in range(0, len(processors), max_batch_size):
        batch = processors[i : i + max_batch_size]
        batch_num = (i // max_batch_size) + 1
        total_batches = (len(processors) + max_batch_size - 1) // max_batch_size

        print(
            f"🧠 [BATCH {batch_num}/{total_batches}] Analyzing {len(batch)} processors..."
        )

        batch_results = _analyze_single_batch(batch)
        all_results.extend(batch_results)

        print(
            f"✅ [BATCH {batch_num}/{total_batches}] Successfully analyzed {len(batch_results)} processors"
        )

    print(
        f"🎉 [CHUNKED BATCH] All {len(all_results)} processors analyzed successfully!"
    )
    return all_results


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

        # Parse LLM response
        try:
            batch_results = json.loads(response.content.strip())
        except json.JSONDecodeError:
            # Try to extract JSON from response if it's wrapped in text
            content = response.content.strip()
            if "[" in content and "]" in content:
                start = content.find("[")
                end = content.rfind("]") + 1
                batch_results = json.loads(content[start:end])
            else:
                raise ValueError("Could not parse JSON from LLM batch response")

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
        # Fallback to individual analysis for critical failures
        print("🔄 [FALLBACK] Using individual processor analysis...")
        results = []
        for proc in processors:
            try:
                individual_result = analyze_processor_properties(
                    proc.get("processor_type", ""), proc.get("properties", {})
                )
                individual_result.update(
                    {
                        "id": proc.get("id", ""),
                        "name": proc.get("name", ""),
                    }
                )
                results.append(individual_result)
            except Exception as individual_error:
                print(
                    f"⚠️ [FALLBACK] Individual analysis also failed for {proc.get('name', 'unnamed')}: {individual_error}"
                )
                results.append(
                    {
                        "processor_type": proc.get("processor_type", ""),
                        "properties": proc.get("properties", {}),
                        "id": proc.get("id", ""),
                        "name": proc.get("name", ""),
                        "data_manipulation_type": "unknown",
                        "actual_data_processing": "Both batch and individual analysis failed",
                        "transforms_data_content": False,
                        "business_purpose": "Analysis completely failed",
                        "data_impact_level": "unknown",
                        "key_operations": ["analysis_failed"],
                        "analysis_method": "fallback_failed",
                        "error": str(individual_error),
                    }
                )

        return results


def analyze_processor_properties(
    processor_type: str, properties: Dict[str, Any]
) -> Dict[str, Any]:
    """Use pure LLM intelligence to analyze what this processor actually does with data."""

    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    try:
        llm = ChatDatabricks(
            endpoint=model_endpoint, temperature=0.1
        )  # Low temp for consistent analysis

        analysis_prompt = f"""You are a NiFi data engineering expert. Analyze this processor to understand what it ACTUALLY does with data.

PROCESSOR TYPE: {processor_type}
CONFIGURATION: {json.dumps(properties, indent=2)}

Focus on DATA MANIPULATION vs INFRASTRUCTURE:
- Does this processor TRANSFORM the actual data content? (change structure, extract fields, convert formats)
- Does this processor just MOVE data from A to B? (file ingestion, storage, network transfer)
- Does this processor do INFRASTRUCTURE work? (logging, routing, delays, authentication)

Return ONLY a JSON object:
{{
  "data_manipulation_type": "data_transformation | data_movement | infrastructure_only | external_processing",
  "actual_data_processing": "Detailed description of what happens to the data content",
  "transforms_data_content": true/false,
  "business_purpose": "What this accomplishes in business terms",
  "data_impact_level": "high | medium | low | none",
  "key_operations": ["list", "of", "key", "operations"]
}}

Examples:
- GetFile: moves files, data_movement, no content transformation
- EvaluateJsonPath: extracts JSON fields, data_transformation, changes data structure
- LogMessage: pure logging, infrastructure_only, no data impact
- UpdateAttribute: metadata only, infrastructure_only, doesn't change core data
- ExecuteStreamCommand with SQL: transforms data, external_processing, high impact
- RouteOnAttribute: routes data, infrastructure_only, no content change

Be specific about what happens to the actual data content, not just metadata or routing."""

        response = llm.invoke(analysis_prompt)

        # Parse LLM response
        try:
            analysis_result = json.loads(response.content.strip())
        except json.JSONDecodeError:
            # Try to extract JSON from response if it's wrapped in text
            content = response.content.strip()
            if "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                analysis_result = json.loads(content[start:end])
            else:
                raise ValueError("Could not parse JSON from LLM response")

        # Add processor info and metadata
        analysis_result.update(
            {
                "processor_type": processor_type,
                "properties": properties,
                "analysis_method": "llm_intelligent",
            }
        )

        return analysis_result

    except Exception as e:
        # Fallback to basic analysis if LLM fails
        return {
            "processor_type": processor_type,
            "properties": properties,
            "data_manipulation_type": "unknown",
            "actual_data_processing": f"LLM analysis failed: {str(e)}. Manual analysis needed.",
            "transforms_data_content": False,
            "business_purpose": f"Unknown processor: {processor_type}",
            "data_impact_level": "unknown",
            "key_operations": ["analysis_failed"],
            "analysis_method": "fallback_basic",
            "error": str(e),
        }
