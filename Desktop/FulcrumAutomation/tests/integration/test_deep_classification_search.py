#!/usr/bin/env python3
"""
Test the deep classification search functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_deep_classification_search():
    """Test deep classification search with various search terms"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🔍 Testing Deep Classification Search")
    print("=" * 50)
    
    try:
        # Get classification sets
        classification_sets = processor.api_client.get_classification_sets()
        print(f"📋 Found {len(classification_sets)} classification sets")
        
        # Test search terms
        test_searches = [
            "bayview hills district",
            "liberty",
            "military",
            "navy",
            "marines",
            "housing",
            "hills",
            "bayview"
        ]
        
        for search_term in test_searches:
            print(f"\n🔍 Testing search: '{search_term}'")
            print("-" * 40)
            
            matches = []
            
            for cls_set in classification_sets:
                cls_name = cls_set.get('name', '')
                
                # Use the deep search function
                set_matches = processor._search_classification_items(
                    cls_set.get('items', []), 
                    search_term, 
                    [cls_name],
                    debug=(cls_name.upper() == 'LMH' and search_term == "bayview hills district")
                )
                
                for match in set_matches:
                    matches.append({
                        'set_name': cls_name,
                        'path': match['formatted_path'],
                        'score': match['score'],
                        'exact_words': match['exact_words'],
                        'partial_words': match['partial_words']
                    })
            
            if matches:
                # Sort by score
                matches.sort(key=lambda x: (x['exact_words'], x['score']), reverse=True)
                print(f"✅ Found {len(matches)} matches:")
                for i, match in enumerate(matches[:5], 1):  # Show top 5
                    print(f"   {i}. {match['path']} (Set: {match['set_name']}, Score: {match['score']:.2f})")
                if len(matches) > 5:
                    print(f"   ... and {len(matches) - 5} more matches")
            else:
                print(f"❌ No matches found")
        
        # Test the actual district property function
        print(f"\n🧪 Testing actual _get_district_property_value function:")
        print("=" * 60)
        
        test_folders = [
            "Bayview_Hills_District_CompleteSlice_20250822_143702",
            "Liberty_Military_Housing_20250822_143702",
            "Catalina_Heights_data",
        ]
        
        for folder in test_folders:
            print(f"\n📁 Testing folder: {folder}")
            result = processor._get_district_property_value(folder)
            print(f"   Result: '{result}'")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_deep_classification_search()