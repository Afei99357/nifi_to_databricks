#!/usr/bin/env python3
"""
Test script to verify LLM generation for EvaluateJsonPath processor.
Run this in Databricks to test the LLM generation functionality.
"""

import json
from tools.pattern_tools import generate_databricks_code

def test_evaluate_json_path():
    """Test LLM generation for EvaluateJsonPath processor."""
    
    # Test properties from the user's example
    properties = {
        "Destination": "flowfile-attribute",
        "Return Type": "auto-detect", 
        "Path Not Found Behavior": "ignore",
        "Null Value Representation": "empty string",
        "log.host": "$.host",
        "log.level": "$.level", 
        "log.message": "$.message",
        "log.service": "$.service",
        "log.timestamp": "$.timestamp"
    }
    
    print("Testing EvaluateJsonPath code generation...")
    print("=" * 50)
    
    # Test 1: Normal generation (may use cached pattern)
    print("\n1. Normal generation (may use cached pattern):")
    code1 = generate_databricks_code.func(
        processor_type="EvaluateJsonPath",
        properties=json.dumps(properties),
        force_regenerate=False
    )
    print(code1)
    
    print("\n" + "=" * 50)
    
    # Test 2: Force LLM regeneration (bypass cache)
    print("\n2. Force LLM regeneration (bypass cache):")
    code2 = generate_databricks_code.func(
        processor_type="EvaluateJsonPath", 
        properties=json.dumps(properties),
        force_regenerate=True
    )
    print(code2)
    
    print("\n" + "=" * 50)
    
    # Compare results
    if code1 == code2:
        print("\n✓ Both methods returned the same code")
    else:
        print("\n! Different code generated - cached pattern may exist")
        print("\nCached version length:", len(code1))
        print("LLM version length:", len(code2))

if __name__ == "__main__":
    test_evaluate_json_path()