#!/usr/bin/env python3
"""
Test the data source generation functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_data_source_generation():
    """Test the data source generation with property name and form ID"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("📊 Testing Data Source Generation")
    print("=" * 50)
    
    try:
        # Test with different folder names
        test_folders = [
            "Bayview_Hills_District_ReplaceandRepair_20250822_143702",
            "Liberty_Base_CompleteSlice_20250822_140000",
            "Catalina_Heights_data",
            "Edson_District_20250101_120000"
        ]
        
        print(f"🧪 Testing data source generation with example folders:")
        
        for i, folder_name in enumerate(test_folders, 1):
            print(f"\n{i}. Testing folder: {folder_name}")
            print("-" * 40)
            
            # Extract property name (same logic as in function)
            search_name = folder_name.replace('_data', '').replace('_migrated', '')
            parts = search_name.split('_')
            clean_parts = []
            for part in parts:
                if part.isdigit() and len(part) == 8:
                    continue
                if part.isdigit() and len(part) == 6:
                    continue
                if part.lower() in ['replace', 'repair', 'complete', 'slice', 'incomplete', 'patch', 'replaceandrepair']:
                    continue
                clean_parts.append(part)
            
            property_name = ' '.join(clean_parts).replace('_', ' ').strip()
            print(f"   🏠 Extracted property name: '{property_name}'")
            
            # Search for matching forms
            all_forms = processor.api_client.get_forms('all')
            property_words = property_name.lower().split()
            
            matching_forms = []
            for form in all_forms:
                form_name = form.get('name', '').lower()
                matches = sum(1 for word in property_words if word in form_name)
                if matches >= len(property_words) * 0.5:
                    matching_forms.append({
                        'form': form,
                        'name': form.get('name', 'Unknown'),
                        'id': form.get('id', ''),
                        'matches': matches
                    })
            
            if matching_forms:
                # Sort by best matches
                matching_forms.sort(key=lambda x: x['matches'], reverse=True)
                
                print(f"   📋 Found {len(matching_forms)} matching forms:")
                for j, match in enumerate(matching_forms[:3], 1):  # Show top 3
                    print(f"     {j}. {match['name']} (ID: {match['id'][:8]}...)")
                
                if len(matching_forms) > 3:
                    print(f"     ... and {len(matching_forms) - 3} more matches")
                
                # Show what the data source would be
                best_match = matching_forms[0]
                data_source = f"{property_name} ({best_match['id']})"
                print(f"   ✅ Data source would be: '{data_source}'")
                
            else:
                print(f"   ⚠️ No matching forms found")
                print(f"   ✅ Data source would be: '{property_name}'")
        
        # Test the actual function with one example
        print(f"\n🔧 Testing actual function:")
        test_folder = "Bayview_Hills_District_ReplaceandRepair_20250822_143702"
        
        # This would normally be called during CSV transformation
        data_source_value = processor._get_data_source_value(test_folder)
        
        print(f"\n📊 DATA SOURCE WORKFLOW SUMMARY:")
        print(f"=" * 50)
        print(f"1. 📁 Folder: {test_folder}")
        print(f"2. 🏠 Property: Bayview Hills District") 
        print(f"3. 🔍 Search for matching forms")
        print(f"4. 📋 Find source form with similar name")
        print(f"5. 🆔 Get form ID")
        print(f"6. 📊 Format: 'Property Name (form-id-123)'")
        print(f"7. ✅ Result: '{data_source_value}'")
        
        print(f"\n💡 When migrating data:")
        print(f"   • If target form has 'data_source' field → auto-populate")
        print(f"   • If target form doesn't have it → offer to add column")
        print(f"   • Value format: 'Property Name (form-id)' or just 'Property Name'")
        print(f"   • Every row gets the same data source value")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_data_source_generation()