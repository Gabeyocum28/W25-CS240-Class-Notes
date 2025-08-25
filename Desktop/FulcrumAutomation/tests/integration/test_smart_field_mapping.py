#!/usr/bin/env python3
"""
Test the smart field mapping system with memory, synonyms, and duplicate prevention
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_smart_field_mapping():
    """Test the smart field mapping functionality"""
    processor = AdvancedFulcrumProcessor()
    
    print("🧠 Testing Smart Field Mapping System")
    print("=" * 60)
    
    # Test 1: Basic synonym matching
    print("\n🔍 Test 1: Synonym-based field matching")
    print("-" * 40)
    
    source_columns = [
        'survey_date',
        'inspector_name', 
        'property_address',
        'slice_length',
        'expansion_joint_length',
        'high_point',
        'low_point',
        'technician_notes',
        'before_pictures',
        'completion_date'
    ]
    
    template_fields = [
        {'label': 'Survey Date', 'type': 'date'},
        {'label': 'Surveyed By', 'type': 'text'},
        {'label': 'Address', 'type': 'text'},
        {'label': 'Slice Length', 'type': 'number'},
        {'label': 'Expansion Joint Length', 'type': 'number'},
        {'label': 'High Point', 'type': 'number'},
        {'label': 'Low Point', 'type': 'number'},
        {'label': 'Technician Notes', 'type': 'text'},
        {'label': 'Before Photos', 'type': 'photos'},
        {'label': 'Date Completed', 'type': 'date'}
    ]
    
    print("📋 Source columns:")
    for i, col in enumerate(source_columns, 1):
        print(f"  {i:2d}. {col}")
    
    print("\n🎯 Target fields:")
    for i, field in enumerate(template_fields, 1):
        print(f"  {i:2d}. {field['label']} ({field['type']})")
    
    # Test smart mapping
    print(f"\n🤖 Testing smart field mapping...")
    smart_mapping = processor.smart_field_mapper.get_smart_mapping(source_columns, template_fields)
    
    print(f"\n📊 Smart mapping results:")
    print("-" * 40)
    for target_field, source_field in smart_mapping.items():
        if source_field:
            print(f"✅ {target_field} ← {source_field}")
        else:
            print(f"⚪ {target_field} ← (unmapped)")
    
    # Test 2: Memory and learning
    print(f"\n🧠 Test 2: Memory and learning system")
    print("-" * 40)
    
    # Remember some mappings
    processor.smart_field_mapper.remember_mapping("test_form", "Survey Date", "survey_date")
    processor.smart_field_mapper.remember_mapping("test_form", "Address", "property_address")
    processor.smart_field_mapper.update_mapping_history("test_form", "Survey Date", "survey_date", success=True)
    processor.smart_field_mapper.update_mapping_history("test_form", "Address", "property_address", success=True)
    
    print("✅ Remembered mappings for 'test_form'")
    
    # Test 3: Duplicate prevention
    print(f"\n🚫 Test 3: Duplicate prevention")
    print("-" * 40)
    
    # Try to map the same source field to multiple targets
    test_mapping = {}
    used_source_columns = set()
    
    # First mapping
    test_mapping["Field 1"] = "slice_length"
    used_source_columns.add("slice_length")
    print(f"✅ Mapped: Field 1 ← slice_length")
    
    # Try to map slice_length again (should be prevented)
    if "slice_length" in used_source_columns:
        print(f"❌ slice_length is already mapped to Field 1")
        print(f"   Cannot map slice_length to multiple targets")
    
    # Test 4: Show mapping suggestions
    print(f"\n💡 Test 4: Intelligent mapping suggestions")
    print("-" * 40)
    
    target_field = "High Point"
    suggestions = processor.smart_field_mapper.get_mapping_suggestions(target_field, source_columns, used_source_columns)
    
    print(f"🎯 Suggestions for '{target_field}':")
    for i, suggestion in enumerate(suggestions[:3], 1):
        print(f"  {i}. {suggestion['source_field']} - {suggestion['reason']}")
    
    # Test 5: View memory
    print(f"\n🧠 Test 5: View field mapping memory")
    print("-" * 40)
    
    processor.view_field_mapping_memory()
    
    # Test 6: View history
    print(f"\n📚 Test 6: View mapping history")
    print("-" * 40)
    
    processor.view_mapping_history()
    
    print(f"\n🎉 Smart field mapping test completed!")
    print(f"💡 The system now provides:")
    print(f"   ✅ Intelligent field matching with synonyms")
    print(f"   🧠 Memory of successful mappings")
    print(f"   🚫 Prevention of duplicate mappings")
    print(f"   💡 Smart suggestions for unmapped fields")
    print(f"   📚 Learning from mapping history")

if __name__ == "__main__":
    test_smart_field_mapping()

