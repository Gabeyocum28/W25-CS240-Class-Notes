#!/usr/bin/env python3
"""
Test the enhanced deep classification search functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_enhanced_classification_search():
    """Test enhanced deep classification search with better depth tracking"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🔍 Testing Enhanced Deep Classification Search")
    print("=" * 60)
    
    try:
        # Test the enhanced search with a specific folder name
        test_folder = "Bayview_Hills_District_CompleteSlice_ReplaceandRepair_20250823_155720"
        print(f"\n📁 Testing enhanced search for folder: {test_folder}")
        print("-" * 60)
        
        # This will use the enhanced _get_district_property_value function
        result = processor._get_district_property_value(test_folder)
        print(f"\n🎯 Final result: '{result}'")
        
        # Test the new exploration function
        print(f"\n🔍 Testing Classification Structure Explorer")
        print("=" * 60)
        
        # Explore LMH classification set specifically
        print("\n📁 Exploring LMH classification structure:")
        processor.explore_classification_structure("LMH", max_depth=6)
        
        # Test deep search with debug enabled
        print(f"\n🧪 Testing Deep Search with Debug Mode")
        print("=" * 60)
        
        # Get classification sets
        classification_sets = processor.api_client.get_classification_sets()
        
        # Find LMH set
        lmh_set = None
        for cls_set in classification_sets:
            if cls_set.get('name', '').upper() == 'LMH':
                lmh_set = cls_set
                break
        
        if lmh_set:
            print(f"\n🔍 Deep searching LMH classification with debug enabled:")
            search_terms = "bayview hills district"
            
            matches = processor._search_classification_items(
                lmh_set.get('items', []), 
                search_terms, 
                ['LMH'], 
                debug=True,
                max_depth=8
            )
            
            print(f"\n📊 Search Results Summary:")
            print(f"Total matches found: {len(matches)}")
            
            if matches:
                # Group by depth
                matches_by_depth = {}
                for match in matches:
                    depth = match['depth']
                    if depth not in matches_by_depth:
                        matches_by_depth[depth] = []
                    matches_by_depth[depth].append(match)
                
                print(f"\n🎯 Matches by depth:")
                for depth in sorted(matches_by_depth.keys()):
                    depth_matches = matches_by_depth[depth]
                    print(f"\n  Depth {depth}: {len(depth_matches)} matches")
                    for match in depth_matches[:3]:  # Show first 3 at each depth
                        print(f"    ✅ {match['formatted_path']} (score: {match['score']:.2f})")
                    if len(depth_matches) > 3:
                        print(f"    ... and {len(depth_matches) - 3} more")
        else:
            print("❌ LMH classification set not found")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_enhanced_classification_search()

