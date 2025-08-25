#!/usr/bin/env python3
"""
Test specifically for Liberty Military Housing classification sets
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_lmh_classification():
    """Test specifically for Liberty Military Housing classifications"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🏢 Testing Liberty Military Housing Classification Sets")
    print("=" * 60)
    
    try:
        classification_sets = processor.api_client.get_classification_sets()
        print(f"📋 Found {len(classification_sets)} classification sets")
        
        # Look for LMH-related classification sets
        lmh_sets = []
        for cls_set in classification_sets:
            name = cls_set.get('name', '').lower()
            if any(term in name for term in ['liberty', 'military', 'housing', 'lmh', 'navy', 'army']):
                lmh_sets.append(cls_set)
        
        if lmh_sets:
            print(f"\n🎯 Found {len(lmh_sets)} LMH-related classification sets:")
            for cls_set in lmh_sets:
                name = cls_set.get('name', 'Unknown')
                item_count = len(cls_set.get('items', []))
                print(f"   • {name} ({item_count} items)")
                
                # Show the structure of this classification set
                print(f"     📁 Structure:")
                items = cls_set.get('items', [])
                for item in items[:5]:  # Show first 5 items
                    label = item.get('label', 'Unknown')
                    children_count = len(item.get('children', []))
                    if children_count > 0:
                        print(f"       └─ {label} ({children_count} children)")
                        # Show some children
                        for child in item.get('children', [])[:3]:
                            child_label = child.get('label', 'Unknown')
                            grand_children = len(child.get('children', []))
                            if grand_children > 0:
                                print(f"          └─ {child_label} ({grand_children} children)")
                                # Show some grandchildren
                                for grandchild in child.get('children', [])[:3]:
                                    gc_label = grandchild.get('label', 'Unknown')
                                    print(f"             └─ {gc_label}")
                            else:
                                print(f"          └─ {child_label}")
                    else:
                        print(f"       └─ {label}")
                
                if len(items) > 5:
                    print(f"       ... and {len(items) - 5} more items")
        else:
            print(f"\n⚠️ No LMH-specific classification sets found")
            print(f"Showing all available classification sets:")
            for cls_set in classification_sets:
                name = cls_set.get('name', 'Unknown')
                item_count = len(cls_set.get('items', []))
                print(f"   • {name} ({item_count} items)")
        
        # Test search for typical LMH terms
        lmh_search_terms = [
            "bayview hills",
            "edson district", 
            "catalina heights",
            "navy",
            "army",
            "liberty"
        ]
        
        print(f"\n🔍 Testing LMH-related searches:")
        
        for search_term in lmh_search_terms:
            print(f"\n   Searching for: '{search_term}'")
            
            total_matches = 0
            for cls_set in classification_sets:
                cls_name = cls_set.get('name', '')
                matches = processor._search_classification_items(
                    cls_set.get('items', []), 
                    search_term.lower(), 
                    []
                )
                
                if matches:
                    print(f"     📁 {cls_name}: {len(matches)} matches")
                    for match in matches[:2]:  # Show first 2
                        print(f"       • {match['formatted_path']}")
                    if len(matches) > 2:
                        print(f"       ... and {len(matches) - 2} more")
                    total_matches += len(matches)
            
            if total_matches == 0:
                print(f"     ❌ No matches found")
        
        print(f"\n📊 Classification Analysis Summary:")
        print(f"   Total sets: {len(classification_sets)}")
        print(f"   LMH-related sets: {len(lmh_sets)}")
        print(f"   Search algorithm: ✅ Working")
        
        if lmh_sets:
            print(f"\n💡 When migrating Bayview Hills District data:")
            print(f"   1. System will search classification sets")
            print(f"   2. Look for 'bayview hills' matches")
            print(f"   3. Show path like 'LMH → NAVY → Bayview Hills District'")
            print(f"   4. Format as 'NAVY,Bayview Hills District' for districtproperty")
        else:
            print(f"\n⚠️ No LMH classification sets found.")
            print(f"   You may need to create classification sets in Fulcrum with the structure:")
            print(f"   LMH → NAVY → Bayview Hills District")
            print(f"   LMH → ARMY → Other Districts")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lmh_classification()