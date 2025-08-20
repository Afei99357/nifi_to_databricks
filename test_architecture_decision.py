#!/usr/bin/env python3
"""
Test script for the new architecture decision system.
Tests the XML analysis and recommendation logic with existing NiFi files.
"""

import json
from pathlib import Path

def test_architecture_decision_system():
    """Test the architecture decision system with existing NiFi files."""
    
    # Test files
    test_files = [
        "nifi_pipeline_file/nifi_pipeline_eric_embed_groups.xml",  # Mixed batch+streaming
        "nifi_pipeline_file/nifi_pipeline_eric_1.xml",             # Unknown structure  
        "nifi_pipeline_file/json_log_process_pipeline.xml"         # JSON processing
    ]
    
    print("🔍 Testing Architecture Decision System")
    print("=" * 60)
    
    for xml_file in test_files:
        if not Path(xml_file).exists():
            print(f"❌ File not found: {xml_file}")
            continue
            
        print(f"\n📄 Analyzing: {xml_file}")
        print("-" * 40)
        
        try:
            # Read XML content
            with open(xml_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # Test architecture analysis
            from tools.xml_tools import analyze_nifi_architecture_requirements, recommend_databricks_architecture
            
            print("🔎 Running architecture analysis...")
            analysis_result = analyze_nifi_architecture_requirements.func(xml_content)
            analysis = json.loads(analysis_result)
            
            print("📊 Analysis Results:")
            print(f"  • Total processors: {analysis['processor_analysis']['total_count']}")
            print(f"  • Complexity level: {analysis['complexity_level']}")
            print(f"  • Feature flags:")
            for flag, value in analysis['feature_flags'].items():
                if value:
                    print(f"    ✓ {flag}")
            
            print(f"  • Processor breakdown:")
            for category in ['sources', 'transforms', 'sinks']:
                processors = analysis['processor_analysis'][category]
                if processors:
                    print(f"    - {category.title()}: {len(processors)}")
                    for proc in processors[:3]:  # Show first 3
                        print(f"      • {proc['name']} ({proc['type']})")
                    if len(processors) > 3:
                        print(f"      • ... and {len(processors)-3} more")
            
            print("\n🎯 Getting architecture recommendation...")
            recommendation_result = recommend_databricks_architecture.func(xml_content)
            recommendation = json.loads(recommendation_result)
            
            print("💡 Recommendation:")
            print(f"  • Architecture: {recommendation['recommendation']}")
            print(f"  • Confidence: {recommendation['confidence']}")
            print(f"  • Reasoning:")
            for reason in recommendation['reasoning']:
                print(f"    - {reason}")
                
            if recommendation['alternative_options']:
                print(f"  • Alternatives:")
                for alt in recommendation['alternative_options']:
                    print(f"    - {alt['option']}: {alt['reason']}")
            
            print("✅ Analysis completed successfully!")
            
        except Exception as e:
            print(f"❌ Error analyzing {xml_file}: {e}")
    
    print(f"\n🎉 Architecture decision system testing completed!")
    
    # Test the intelligent migration function (without actual execution)
    print(f"\n🤖 Testing intelligent migration function...")
    try:
        from tools.migration_tools import orchestrate_intelligent_nifi_migration
        print("✅ Intelligent migration function is available!")
        print("   Use it like this:")
        print("   orchestrate_intelligent_nifi_migration(")
        print("       xml_path='path/to/nifi.xml',")
        print("       out_dir='output_results/',")
        print("       project='my_project'")
        print("   )")
        
    except Exception as e:
        print(f"❌ Error importing intelligent migration: {e}")

if __name__ == "__main__":
    test_architecture_decision_system()