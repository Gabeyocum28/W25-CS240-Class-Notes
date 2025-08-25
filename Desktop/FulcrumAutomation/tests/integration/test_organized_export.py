#!/usr/bin/env python3
"""
Test the new organized folder structure
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_organized_export():
    """Test the organized export with simulated user input"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
    form_name = "Bayview Hills District"
    
    print(f"🧪 Testing organized export structure for: {form_name}")
    
    try:
        import requests
        import pandas as pd
        from pathlib import Path
        from datetime import datetime
        
        # Get some records to test with
        response = requests.get(
            f"{processor.api_client.base_url}/records",
            headers=processor.api_client.headers,
            params={'form_id': form_id},
            timeout=30
        )
        
        all_records = response.json().get('records', [])
        # Get just Replace & Repair records for testing
        filtered_records = [r for r in all_records if r.get('status') == 'Replace & Repair'][:3]  # Just 3 for testing
        
        print(f"📊 Using {len(filtered_records)} test records")
        
        # Simulate the organized folder creation
        cache_dir = Path("cached")
        cache_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_form_name = "".join(c for c in form_name if c.isalnum() or c in (' ', '-', '_')).rstrip().replace(' ', '_')
        status_summary = "ReplaceandRepair"
        
        # Create property folder with timestamp
        property_folder_name = f"{safe_form_name}_{status_summary}_{timestamp}"
        property_folder = cache_dir / property_folder_name
        property_folder.mkdir(exist_ok=True)
        
        print(f"📁 Created property folder: {property_folder}")
        
        # Create CSV in the property folder
        csv_filename = f"{safe_form_name}_data.csv"
        csv_path = property_folder / csv_filename
        
        # Create simple test CSV
        test_data = []
        for record in filtered_records:
            test_data.append({
                'id': record.get('id', ''),
                'status': record.get('status', ''),
                'created_at': record.get('created_at', ''),
                'latitude': record.get('latitude', ''),
                'longitude': record.get('longitude', ''),
            })
        
        df = pd.DataFrame(test_data)
        df.to_csv(csv_path, index=False)
        
        print(f"📄 Created CSV: {csv_path}")
        
        # Create photos directory
        photos_dir = property_folder / "photos"
        photos_dir.mkdir(exist_ok=True)
        
        # Create test photo index
        photo_index_data = [{
            'photo_filename': 'test_photo.jpg',
            'photo_id': 'test-id-123',
            'record_id': 'test-record-456',
            'field_name': 'before_photos'
        }]
        
        photo_index_df = pd.DataFrame(photo_index_data)
        photo_index_path = photos_dir / "photo_index.csv"
        photo_index_df.to_csv(photo_index_path, index=False)
        
        print(f"📸 Created photos directory: {photos_dir}")
        print(f"📋 Created photo index: {photo_index_path}")
        
        # Show the final structure
        print(f"\n🎉 ORGANIZED EXPORT STRUCTURE:")
        print(f"📁 {property_folder_name}/")
        print(f"   📄 {csv_filename}")
        print(f"   📁 photos/")
        print(f"      📋 photo_index.csv")
        print(f"      📸 (downloaded photos would be here)")
        
        print(f"\n✅ Test completed! Check the folder structure in: cached/{property_folder_name}")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_organized_export()