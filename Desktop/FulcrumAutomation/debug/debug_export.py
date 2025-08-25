#!/usr/bin/env python3
"""
Debug script to test column name mapping
"""

import sys
import os
# Add the project root to the path (one level up from debug/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def debug_export():
    """Debug the column name mapping"""
    processor = AdvancedFulcrumProcessor()
    
    # Check if API credentials are set up
    if not processor.api_client:
        print("❌ API credentials not found. Please run the main script and do option 1 first")
        return
    
    # Test form ID for Bayview Hills District
    form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
    form_name = "Bayview Hills District"
    
    print(f"🔍 Debugging column names for: {form_name}")
    print(f"📋 Form ID: {form_id}")
    
    try:
        # Get form schema directly to see what's available
        print("\n📋 Getting form schema...")
        form_schema = processor.api_client.get_form_schema(form_id)
        
        print(f"\n🔍 Form Elements:")
        
        def print_elements(elements, level=0):
            indent = "  " * level
            for i, element in enumerate(elements, 1):
                data_name = element.get('data_name')
                key = element.get('key')
                label = element.get('label')
                element_type = element.get('type')
                print(f"{indent}{i}. {key} ({data_name}) → '{label}' ({element_type})")
                
                # Print nested elements
                if 'elements' in element:
                    print(f"{indent}  📁 Contains {len(element['elements'])} nested elements:")
                    print_elements(element['elements'], level + 2)
                    
        print_elements(form_schema.get('elements', []))
            
        # Get a sample record to see field keys
        print(f"\n📊 Getting sample records...")
        import requests
        response = requests.get(
            f"{processor.api_client.base_url}/records",
            headers=processor.api_client.headers,
            params={'form_id': form_id, 'per_page': 1},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            records = data.get('records', [])
            
            if records:
                sample_record = records[0]
                form_values = sample_record.get('form_values', {})
                
                print(f"\n🔍 Sample Record Field Keys:")
                for i, (key, value) in enumerate(form_values.items(), 1):
                    value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    print(f"  {i}. '{key}' = {value_preview}")
            else:
                print("❌ No records found")
        else:
            print(f"❌ Failed to get records: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Debug failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_export()