"""
Databricks LLM Enhancement for DDL Conversion

Uses Databricks Foundation Model API (same as AI Assistant page)
to validate and enhance DDL conversions.
"""

import json
from typing import Dict, List, Optional

from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client


def query_databricks_llm(
    endpoint_name: str,
    prompt: str,
    max_tokens: int = 2000,
) -> str:
    """
    Query Databricks LLM endpoint (same method as AI assistant).

    Args:
        endpoint_name: Name of Databricks serving endpoint
        prompt: The prompt to send to the LLM
        max_tokens: Maximum tokens in response

    Returns:
        LLM response text
    """
    messages = [{"role": "user", "content": prompt}]

    response = get_deploy_client("databricks").predict(
        endpoint=endpoint_name,
        inputs={"messages": messages, "max_tokens": max_tokens},
    )

    # Extract content from response
    if "messages" in response:
        return response["messages"][-1]["content"]

    if "choices" in response:
        choice_message = response["choices"][0]["message"]
        content = choice_message.get("content")

        if isinstance(content, list):
            return "".join(
                part.get("text", "") for part in content if part.get("type") == "text"
            )

        if isinstance(content, str):
            return content

    raise Exception("Unexpected response format from Databricks LLM")


def validate_ddl_conversion(
    hive_ddl: str,
    databricks_ddl: str,
    endpoint_name: str = "databricks-meta-llama-3-1-70b-instruct",
) -> Dict:
    """
    Use Databricks LLM to validate DDL conversion.

    Args:
        hive_ddl: Original Hive DDL
        databricks_ddl: Converted Databricks DDL
        endpoint_name: Databricks serving endpoint name

    Returns:
        Dictionary with validation results
    """
    prompt = f"""You are a database migration expert. Review this DDL conversion from Hive to Databricks.

ORIGINAL HIVE DDL:
```sql
{hive_ddl}
```

CONVERTED DATABRICKS DDL:
```sql
{databricks_ddl}
```

Please analyze:
1. Is the conversion correct?
2. Are column types properly converted (especially STRING to TIMESTAMP)?
3. Is the partition strategy preserved?
4. Is the storage location correctly converted?
5. Are there any potential issues?

Respond in JSON format:
{{
    "status": "PASS" or "FAIL",
    "issues": ["list of issues, empty if none"],
    "suggestions": ["list of optimization suggestions"],
    "confidence": "HIGH" or "MEDIUM" or "LOW"
}}
"""

    try:
        response_text = query_databricks_llm(endpoint_name, prompt, max_tokens=1000)

        # Parse JSON response
        # Extract JSON from response (might have markdown formatting)
        if "```json" in response_text:
            json_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            json_text = response_text.split("```")[1].split("```")[0].strip()
        else:
            json_text = response_text.strip()

        result = json.loads(json_text)
        return result

    except Exception as e:
        return {
            "status": "ERROR",
            "issues": [f"LLM validation failed: {str(e)}"],
            "suggestions": [],
            "confidence": "LOW",
        }


def suggest_optimizations(
    databricks_ddl: str,
    endpoint_name: str = "databricks-meta-llama-3-1-70b-instruct",
) -> List[str]:
    """
    Get optimization suggestions for Databricks DDL.

    Args:
        databricks_ddl: Databricks DDL to optimize
        endpoint_name: Databricks serving endpoint name

    Returns:
        List of optimization suggestions
    """
    prompt = f"""You are a Databricks optimization expert. Review this Delta table DDL and suggest optimizations.

DATABRICKS DDL:
```sql
{databricks_ddl}
```

Suggest optimizations for:
1. Partitioning strategy
2. Data types
3. Table properties
4. Performance tuning

Return a JSON array of suggestions:
["suggestion 1", "suggestion 2", ...]
"""

    try:
        response_text = query_databricks_llm(endpoint_name, prompt, max_tokens=1000)

        # Extract JSON array
        if "[" in response_text and "]" in response_text:
            start = response_text.index("[")
            end = response_text.rindex("]") + 1
            json_text = response_text[start:end]
            return json.loads(json_text)
        else:
            return [response_text]

    except Exception as e:
        return [f"Error getting suggestions: {str(e)}"]


def enhance_ddl_with_comments(
    hive_ddl: str,
    databricks_ddl: str,
    endpoint_name: str = "databricks-meta-llama-3-1-70b-instruct",
) -> str:
    """
    Add helpful comments to Databricks DDL using LLM.

    Args:
        hive_ddl: Original Hive DDL
        databricks_ddl: Converted Databricks DDL
        endpoint_name: Databricks serving endpoint name

    Returns:
        Enhanced DDL with comments
    """
    prompt = f"""Add helpful inline comments to this Databricks DDL based on the original Hive DDL.

ORIGINAL HIVE DDL:
```sql
{hive_ddl}
```

DATABRICKS DDL:
```sql
{databricks_ddl}
```

Add comments explaining:
1. Type conversions (e.g., STRING to TIMESTAMP)
2. Path changes (HDFS to DBFS)
3. Format changes (PARQUET to DELTA)
4. Any important differences

Return ONLY the enhanced DDL with comments, no other text.
"""

    try:
        enhanced = query_databricks_llm(endpoint_name, prompt, max_tokens=2000)

        # Extract SQL from response
        if "```sql" in enhanced:
            enhanced = enhanced.split("```sql")[1].split("```")[0].strip()
        elif "```" in enhanced:
            enhanced = enhanced.split("```")[1].split("```")[0].strip()

        return enhanced

    except Exception as e:
        return f"-- Error enhancing DDL: {str(e)}\n\n{databricks_ddl}"


def convert_with_llm_validation(
    hive_ddl: str,
    databricks_ddl: str,
    endpoint_name: str = "databricks-meta-llama-3-1-70b-instruct",
    verbose: bool = True,
) -> Dict:
    """
    Complete workflow: validate, enhance, and provide suggestions.

    Args:
        hive_ddl: Original Hive DDL
        databricks_ddl: Rule-based converted DDL
        endpoint_name: Databricks serving endpoint name
        verbose: Print progress messages

    Returns:
        Dictionary with all results
    """
    if verbose:
        print("🤖 Validating conversion with Databricks LLM...")

    validation = validate_ddl_conversion(hive_ddl, databricks_ddl, endpoint_name)

    if verbose:
        if validation["status"] == "PASS":
            print(
                f"✅ Validation: {validation['status']} (Confidence: {validation.get('confidence', 'UNKNOWN')})"
            )
        else:
            print(f"⚠️  Validation: {validation['status']}")
            if validation.get("issues"):
                print("   Issues found:")
                for issue in validation["issues"]:
                    print(f"   - {issue}")

    if verbose:
        print("💡 Getting optimization suggestions...")

    suggestions = suggest_optimizations(databricks_ddl, endpoint_name)

    if verbose:
        if suggestions:
            print(f"   Found {len(suggestions)} suggestions:")
            for i, suggestion in enumerate(suggestions[:3], 1):
                print(f"   {i}. {suggestion}")
            if len(suggestions) > 3:
                print(f"   ... and {len(suggestions) - 3} more")

    return {
        "validation": validation,
        "suggestions": suggestions,
        "original_ddl": databricks_ddl,
    }


# Example usage
if __name__ == "__main__":
    example_hive = """
    CREATE EXTERNAL TABLE sales.orders (
      order_id INT,
      customer_name STRING,
      order_timestamp_ts STRING,
      total_amount DOUBLE
    )
    PARTITIONED BY (year INT, month INT, day INT)
    STORED AS PARQUET
    LOCATION 'hdfs://namenode/warehouse/sales.db/orders'
    """

    example_databricks = """
    CREATE TABLE sales.orders (
      order_id INT,
      customer_name STRING,
      order_timestamp_ts TIMESTAMP,
      total_amount DOUBLE,
      year INT,
      month INT,
      day INT
    )
    USING DELTA
    PARTITIONED BY (year, month, day)
    LOCATION 'dbfs:/mnt/warehouse/warehouse/sales.db/orders'
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact' = 'true'
    );
    """

    print("=" * 80)
    print("Testing Databricks LLM Enhancement")
    print("=" * 80)

    # This will only work when running inside Databricks with proper authentication
    try:
        result = convert_with_llm_validation(
            example_hive, example_databricks, verbose=True
        )
        print("\n" + "=" * 80)
        print("RESULTS")
        print("=" * 80)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"\n⚠️  Cannot run - requires Databricks authentication: {e}")
        print("\nThis tool must be run inside Databricks (notebook or job)")
