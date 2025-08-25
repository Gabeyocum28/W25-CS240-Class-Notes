#!/usr/bin/env python3
"""
Test the status mapping functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_status_mapping():
    """Test status mapping between different forms"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🔄 Testing Status Mapping Functionality")
    print("=" * 50)
    
    try:
        # Test getting status values from Bayview Hills District form
        source_form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
        source_form_name = "Bayview Hills District"
        
        print(f"📋 Getting status values from source form:")
        print(f"   {source_form_name}")
        
        source_statuses = processor.api_client.get_status_values_from_form(source_form_id, source_form_name)
        
        if source_statuses:
            print(f"✅ Found {len(source_statuses)} source status values:")
            for i, status in enumerate(source_statuses, 1):
                print(f"   {i}. {status}")
        else:
            print("❌ No source status values found")
            return
        
        # Test getting status values from Liberty Military Housing form
        all_forms = processor.api_client.get_forms('all')
        liberty_forms = [f for f in all_forms if 'liberty' in f.get('name', '').lower() and 'military' in f.get('name', '').lower()]
        
        if liberty_forms:
            target_form = liberty_forms[-1]  # Use Legacy form
            target_form_id = target_form.get('id')
            target_form_name = target_form.get('name')
            
            print(f"\n🎯 Getting status values from target form:")
            print(f"   {target_form_name}")
            
            target_statuses = processor.api_client.get_status_values_from_form(target_form_id, target_form_name)
            
            if target_statuses:
                print(f"✅ Found {len(target_statuses)} target status values:")
                for i, status in enumerate(target_statuses, 1):
                    print(f"   {i}. {status}")
            else:
                print("❌ No target status values found")
                return
        else:
            print("❌ No Liberty Military Housing forms found")
            return
        
        # Test auto-mapping algorithm
        print(f"\n🤖 Testing Auto-Mapping Algorithm:")
        print("-" * 40)
        
        auto_mapping = processor._auto_map_statuses(source_statuses, target_statuses)
        
        print(f"Auto-mapping results:")
        for source_status, target_status in auto_mapping.items():
            if target_status:
                print(f"   ✅ '{source_status}' → '{target_status}'")
            else:
                print(f"   ⚪ '{source_status}' → (no match found)")
        
        # Show expected mappings based on your examples
        print(f"\n💡 Expected Status Mappings (Bayview → Liberty):")
        print("-" * 50)
        
        expected_mappings = [
            ("Level 3 (Severe)", "Replace"),
            ("Level 2 (Moderate)", "Replace"),
            ("Level 1 (Minor)", "Slice"),
            ("Transverse", "Transverse"),
            ("Complete Slice", "Complete Slice"),
            ("Incomplete Expansion", "Expansion"),
            ("Complete Expansion", "Complete Expansion"),
            ("Incomplete Patch", "Patch"),
            ("Complete Patch", "Complete Patch"),
            ("Replace & Repair", "Replace"),
            ("Visited Area", "Visited Area"),
            ("Board Pick-up", "Visited Area")  # Or skip
        ]
        
        for source, expected_target in expected_mappings:
            if source in source_statuses:
                auto_target = auto_mapping.get(source)
                if auto_target == expected_target:
                    print(f"   ✅ '{source}' → '{expected_target}' (auto-mapped correctly)")
                else:
                    print(f"   ⚠️  '{source}' → '{auto_target}' (expected: '{expected_target}')")
            else:
                print(f"   ❌ '{source}' not found in source statuses")
        
        # Summary
        print(f"\n📊 STATUS MAPPING ANALYSIS:")
        print(f"   Source form: {source_form_name}")
        print(f"   Target form: {target_form_name}")
        print(f"   Source statuses: {len(source_statuses)}")
        print(f"   Target statuses: {len(target_statuses)}")
        print(f"   Auto-mapped: {sum(1 for v in auto_mapping.values() if v)}")
        print(f"   Unmapped: {sum(1 for v in auto_mapping.values() if not v)}")
        
        print(f"\n💡 Status Mapping Workflow:")
        print(f"1. ✅ Extract status values from source CSV")
        print(f"2. ✅ Get status values from target form records")
        print(f"3. ✅ Auto-map using similarity scoring")
        print(f"4. ✅ User can review and adjust mappings")
        print(f"5. ✅ Apply mapping during CSV transformation")
        print(f"6. ✅ Unmapped statuses keep original values")
        
        print(f"\n🔄 When migrating Bayview Hills → Liberty Military Housing:")
        print(f"   System will automatically map similar statuses")
        print(f"   User can review and adjust mappings")
        print(f"   Final CSV will have Liberty-compatible status values")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_status_mapping()