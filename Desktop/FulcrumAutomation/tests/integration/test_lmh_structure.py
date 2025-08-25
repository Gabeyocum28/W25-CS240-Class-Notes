#!/usr/bin/env python3
"""
Test the LMH classification structure in detail
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_lmh_structure():
    """Test the LMH classification structure in detail"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🏢 Detailed LMH Classification Structure Analysis")
    print("=" * 60)
    
    try:
        classification_sets = processor.api_client.get_classification_sets()
        
        # Find the LMH classification set
        lmh_set = None
        for cls_set in classification_sets:
            if cls_set.get('name', '').upper() == 'LMH':
                lmh_set = cls_set
                break
        
        if not lmh_set:
            print("❌ LMH classification set not found")
            return
        
        print(f"📋 Found LMH Classification Set")
        print(f"   Items: {len(lmh_set.get('items', []))}")
        
        def print_tree(items, level=0, path=[]):
            """Print the classification tree structure"""
            indent = "  " * level
            for item in items:
                label = item.get('label', 'Unknown')
                current_path = path + [label]
                children = item.get('children', [])
                
                if children:
                    print(f"{indent}📁 {label} ({len(children)} children)")
                    print_tree(children, level + 1, current_path)
                else:
                    print(f"{indent}📄 {label}")
                    # Show the full path for leaf nodes
                    full_path = ' → '.join(current_path)
                    formatted_path = ','.join(current_path[1:]) if len(current_path) > 1 else current_path[0]
                    print(f"{indent}   Path: {full_path}")
                    print(f"{indent}   Formatted: '{formatted_path}'")
        
        print(f"\n🌳 LMH Classification Tree:")
        print_tree(lmh_set.get('items', []))
        
        # Test the search algorithm with this structure
        print(f"\n🔍 Testing Search Algorithm:")
        
        test_terms = ["bayview", "hills", "district", "navy", "marines"]
        
        for term in test_terms:
            print(f"\n   Searching for: '{term}'")
            matches = processor._search_classification_items(
                lmh_set.get('items', []), 
                term.lower(), 
                []
            )
            
            if matches:
                print(f"     ✅ Found {len(matches)} matches:")
                for match in matches:
                    print(f"       • {match['formatted_path']} (score: {match['score']:.2f})")
            else:
                print(f"     ❌ No matches found")
        
        # Show what the districtproperty workflow would look like
        print(f"\n💡 DISTRICTPROPERTY WORKFLOW EXAMPLE:")
        print(f"=" * 50)
        
        example_folder = "Bayview_Hills_District_ReplaceandRepair_20250822_143702"
        print(f"1. Folder name: {example_folder}")
        
        # Extract search terms
        search_name = example_folder.replace('_data', '').replace('_migrated', '')
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
        
        search_terms = ' '.join(clean_parts).replace('_', ' ').strip().lower()
        print(f"2. Extracted terms: '{search_terms}'")
        
        print(f"3. Search results:")
        all_matches = []
        for cls_set in classification_sets:
            matches = processor._search_classification_items(
                cls_set.get('items', []), 
                search_terms, 
                []
            )
            for match in matches:
                all_matches.append({
                    'set_name': cls_set.get('name', ''),
                    'formatted_path': match['formatted_path'],
                    'score': match['score']
                })
        
        if all_matches:
            # Sort by score
            all_matches.sort(key=lambda x: x['score'], reverse=True)
            print(f"   Found {len(all_matches)} matches:")
            for match in all_matches[:5]:  # Top 5
                print(f"   • {match['set_name']}: {match['formatted_path']} (score: {match['score']:.2f})")
        else:
            print(f"   No matches found")
            print(f"   ⚠️ This means the classification set needs to have:")
            print(f"      LMH → NAVY → Bayview Hills District")
            print(f"      LMH → NAVY → Other districts...")
        
        print(f"\n📋 To make this work fully, the LMH classification should have:")
        print(f"   LMH")
        print(f"   ├── NAVY")
        print(f"   │   ├── Bayview Hills District")
        print(f"   │   ├── Edson District") 
        print(f"   │   └── Other Navy Districts...")
        print(f"   └── ARMY")
        print(f"       ├── Fort Liberty Housing")
        print(f"       └── Other Army Districts...")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lmh_structure()