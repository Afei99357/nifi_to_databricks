#!/usr/bin/env python3
"""
Test script for LLM-powered NiFi intelligence analysis
"""

import json
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def test_simple_workflow():
    """Test intelligence analysis on simple workflow"""
    from tools.nifi_intelligence import analyze_nifi_workflow_intelligence

    print("=== TESTING LLM-POWERED NIFI INTELLIGENCE ===\n")

    # Test with simple workflow first
    print("🔍 Testing with SIMPLE workflow (nifi_pipeline_eric_1.xml)")
    try:
        with open("nifi_pipeline_file/nifi_pipeline_eric_1.xml", "r") as f:
            simple_xml = f.read()

        result = analyze_nifi_workflow_intelligence.func(simple_xml)
        analysis = json.loads(result)

        print("✅ Simple workflow analysis completed")
        print(f'📊 Total processors: {analysis.get("total_processors", "unknown")}')

        # Show workflow intelligence
        workflow_intel = analysis.get("workflow_intelligence", {})
        print(
            f'🎯 Business purpose: {workflow_intel.get("business_purpose", "unknown")}'
        )
        print(
            f'🔄 Data transformation summary: {workflow_intel.get("data_transformation_summary", "unknown")}'
        )
        print(
            f'⚖️ Infrastructure vs processing: {workflow_intel.get("infrastructure_vs_processing", "unknown")}'
        )

        # Show key processors by type
        core_processors = workflow_intel.get("core_data_processors", [])
        infra_processors = workflow_intel.get("infrastructure_processors", [])
        print(f"🔧 Core data processors: {core_processors}")
        print(f"🏗️ Infrastructure processors: {infra_processors}")

        # Show individual processor analysis
        print("\n📋 INDIVIDUAL PROCESSOR ANALYSIS:")
        processors = analysis.get("processors_analysis", [])
        for proc in processors:
            name = proc.get("name", "unnamed")
            proc_type = proc.get("processor_type", "unknown").split(".")[
                -1
            ]  # Get class name only
            manipulation_type = proc.get("data_manipulation_type", "unknown")
            transforms_data = proc.get("transforms_data_content", False)
            business_purpose = proc.get("business_purpose", "unknown")

            icon = "🔧" if transforms_data else "🏗️"
            print(
                f"{icon} {name} ({proc_type}): {manipulation_type} - {business_purpose}"
            )

        return analysis

    except Exception as e:
        print(f"❌ Simple workflow test failed: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_complex_workflow():
    """Test intelligence analysis on complex workflow"""
    from tools.nifi_intelligence import analyze_nifi_workflow_intelligence

    print("\n" + "=" * 80 + "\n")
    print("🔍 Testing with COMPLEX workflow (ICN8_BRS_Feedback.xml)")

    try:
        with open("nifi_pipeline_file/ICN8_BRS_Feedback.xml", "r") as f:
            complex_xml = f.read()

        result = analyze_nifi_workflow_intelligence.func(complex_xml)
        analysis = json.loads(result)

        print("✅ Complex workflow analysis completed")
        print(f'📊 Total processors: {analysis.get("total_processors", "unknown")}')

        # Show workflow intelligence
        workflow_intel = analysis.get("workflow_intelligence", {})
        print(
            f'🎯 Business purpose: {workflow_intel.get("business_purpose", "unknown")}'
        )
        print(
            f'🔄 Data transformation summary: {workflow_intel.get("data_transformation_summary", "unknown")}'
        )
        print(
            f'⚖️ Infrastructure vs processing: {workflow_intel.get("infrastructure_vs_processing", "unknown")}'
        )

        # Show key processors by type
        core_processors = workflow_intel.get("core_data_processors", [])
        infra_processors = workflow_intel.get("infrastructure_processors", [])
        print(f"🔧 Core data processors ({len(core_processors)}): {core_processors}")
        print(
            f"🏗️ Infrastructure processors ({len(infra_processors)}): {infra_processors}"
        )

        # Show key insights
        insights = workflow_intel.get("key_insights", [])
        print(f"💡 Key insights: {insights}")

        # Count processor types
        processors = analysis.get("processors_analysis", [])
        data_transform_count = sum(
            1
            for p in processors
            if p.get("data_manipulation_type") == "data_transformation"
        )
        data_movement_count = sum(
            1 for p in processors if p.get("data_manipulation_type") == "data_movement"
        )
        infrastructure_count = sum(
            1
            for p in processors
            if p.get("data_manipulation_type") == "infrastructure_only"
        )

        print(f"\n📈 PROCESSOR BREAKDOWN:")
        print(f"🔧 Data transformation: {data_transform_count}")
        print(f"📦 Data movement: {data_movement_count}")
        print(f"🏗️ Infrastructure only: {infrastructure_count}")

        return analysis

    except Exception as e:
        print(f"❌ Complex workflow test failed: {e}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run tests
    simple_result = test_simple_workflow()
    complex_result = test_complex_workflow()

    print("\n" + "=" * 80)
    print("🎉 INTELLIGENCE TESTING COMPLETE!")

    if simple_result and complex_result:
        print("✅ Both simple and complex workflow analysis succeeded")
        print("🧠 LLM-powered intelligence is working correctly!")

        # Summary comparison
        simple_intel = simple_result.get("workflow_intelligence", {})
        complex_intel = complex_result.get("workflow_intelligence", {})

        print(f"\n📊 COMPARISON:")
        print(
            f'Simple workflow: {simple_intel.get("infrastructure_vs_processing", "unknown")}'
        )
        print(
            f'Complex workflow: {complex_intel.get("infrastructure_vs_processing", "unknown")}'
        )

    else:
        print("❌ Some tests failed - check error output above")
