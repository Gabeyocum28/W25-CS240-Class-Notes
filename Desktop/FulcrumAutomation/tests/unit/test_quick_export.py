#!/usr/bin/env python3
"""
Quick test of the export with just a few records
"""

import sys
import os
# Add the project root to the path (two levels up from tests/unit/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def quick_test():
    """Test the export with Replace & Repair status only"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
    form_name = "Bayview Hills District"
    
    print(f"🧪 Quick export test for: {form_name}")
    
    try:
        import requests
        import pandas as pd
        from pathlib import Path
        from datetime import datetime
        
        # Get records with "Replace & Repair" status only
        response = requests.get(
            f"{processor.api_client.base_url}/records",
            headers=processor.api_client.headers,
            params={'form_id': form_id},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Failed to get records")
            return
            
        all_records = response.json().get('records', [])
        filtered_records = [r for r in all_records if r.get('status') == 'Replace & Repair']
        
        print(f"📊 Found {len(filtered_records)} Replace & Repair records")
        
        # Get form schema and create field mapping
        form_schema = processor.api_client.get_form_schema(form_id)
        field_mapping = {}
        
        def extract_fields(elements, level=0):
            for element in elements:
                key = element.get('key')
                data_name = element.get('data_name')
                label = element.get('label', data_name or key)
                
                if key:
                    field_mapping[key] = label
                if data_name and data_name != key:
                    field_mapping[data_name] = label
                
                if 'elements' in element:
                    extract_fields(element['elements'], level + 1)
        
        extract_fields(form_schema.get('elements', []))
        print(f"📊 Created field mapping with {len(field_mapping)} entries")
        
        # Process records
        csv_data = []
        for record in filtered_records[:3]:  # Just first 3 records
            row = {
                'id': record.get('id', ''),
                'status': record.get('status', ''),
                'created_at': record.get('created_at', ''),
                'updated_at': record.get('updated_at', ''),
                'latitude': record.get('latitude', ''),
                'longitude': record.get('longitude', ''),
            }
            
            # Add form field values with proper column names
            form_values = record.get('form_values', {})
            for field_key, value in form_values.items():
                column_name = field_mapping.get(field_key, field_key)
                
                # Clean up values
                if isinstance(value, dict):
                    if 'choice_values' in value:
                        row[column_name] = ', '.join(value['choice_values'])
                    elif 'thoroughfare' in value or 'sub_thoroughfare' in value:
                        address_parts = []
                        if value.get('sub_thoroughfare'):
                            address_parts.append(value['sub_thoroughfare'])
                        if value.get('thoroughfare'):
                            address_parts.append(value['thoroughfare'])
                        if value.get('locality'):
                            address_parts.append(value['locality'])
                        if value.get('admin_area'):
                            address_parts.append(value['admin_area'])
                        if value.get('postal_code'):
                            address_parts.append(value['postal_code'])
                        row[column_name] = ', '.join(filter(None, address_parts))
                    else:
                        row[column_name] = str(value)
                elif isinstance(value, list):
                    if all(isinstance(item, dict) and 'photo_id' in item for item in value):
                        photo_ids = [item['photo_id'] for item in value]
                        row[column_name] = ', '.join(photo_ids)
                    else:
                        row[column_name] = ', '.join(str(item) for item in value)
                else:
                    row[column_name] = str(value) if value is not None else ''
            
            csv_data.append(row)
        
        # Create DataFrame and save
        df = pd.DataFrame(csv_data)
        
        # Show column names
        print(f"\n📋 Column Names ({len(df.columns)} total):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        # Save test file
        cache_dir = Path("cached")
        cache_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_file = cache_dir / f"test_export_{timestamp}.csv"
        df.to_csv(test_file, index=False)
        
        print(f"\n✅ Test export saved: {test_file}")
        print(f"📊 Records: {len(df)}")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_test()