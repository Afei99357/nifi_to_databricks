#!/usr/bin/env python3
"""
Script to clear cached patterns from the Delta table to force LLM regeneration.
Run this in Databricks to remove specific processor patterns.
"""

from registry.pattern_registry import PatternRegistryUC

def clear_pattern(processor_name: str):
    """Clear a specific processor pattern from the Delta table."""
    try:
        registry = PatternRegistryUC()
        
        # Check if pattern exists
        existing_pattern = registry.get_pattern(processor_name)
        if existing_pattern:
            print(f"Found existing pattern for {processor_name}:")
            print(f"  Equivalent: {existing_pattern.get('databricks_equivalent', 'Unknown')}")
            
            # Remove the pattern by deleting the row
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            catalog = registry.catalog
            schema = registry.schema  
            table = registry.table
            full_table_name = f"{catalog}.{schema}.{table}"
            
            # Delete the specific pattern
            spark.sql(f"""
                DELETE FROM {full_table_name} 
                WHERE processor_class = '{processor_name}'
            """)
            
            print(f"✓ Cleared pattern for {processor_name}")
            print("  LLM generation will now be triggered for this processor")
            
        else:
            print(f"No existing pattern found for {processor_name}")
            print("  LLM generation should already be triggered")
            
    except Exception as e:
        print(f"Error clearing pattern: {e}")

if __name__ == "__main__":
    # Clear the EvaluateJsonPath pattern
    clear_pattern("EvaluateJsonPath")
    
    # You can add other processors here if needed
    # clear_pattern("ControlRate")
    # clear_pattern("SomeOtherProcessor")