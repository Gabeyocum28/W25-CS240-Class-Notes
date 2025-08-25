#!/usr/bin/env python3
"""
Complete demo of the migration workflow with classification sets
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def demo_complete_migration():
    """Demo the complete migration workflow including classification"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🎬 COMPLETE MIGRATION WORKFLOW DEMO")
    print("=" * 60)
    print("This shows the full end-to-end process:")
    print("1. ✅ Export data from source form → CSV + photos")
    print("2. ✅ Select target form (Liberty Military Housing)")  
    print("3. ✅ Extract target form template")
    print("4. ✅ Map fields between source and target")
    print("5. ✅ Search classification sets for districtproperty")
    print("6. ✅ Generate migrated CSV with proper districtproperty")
    print("=" * 60)
    
    try:
        # Step 1: Find Liberty Military Housing forms
        print(f"\n1️⃣ FINDING TARGET FORMS")
        print("-" * 40)
        
        all_forms = processor.api_client.get_forms('all')
        liberty_forms = [f for f in all_forms if 'liberty' in f.get('name', '').lower() and 'military' in f.get('name', '').lower()]
        
        if liberty_forms:
            print(f"✅ Found {len(liberty_forms)} Liberty Military Housing forms")
            target_form = liberty_forms[0]
            print(f"   Target: {target_form.get('name')}")
        else:
            print("❌ No Liberty Military Housing forms found")
            return
        
        # Step 2: Get template
        print(f"\n2️⃣ EXTRACTING FORM TEMPLATE")
        print("-" * 40)
        
        template_fields = processor._get_form_template(target_form['id'], target_form['name'])
        
        if template_fields:
            print(f"✅ Template ready with {len(template_fields)} fields")
            
            # Look for districtproperty field
            district_field = None
            for field in template_fields:
                if field['label'].lower() == 'districtproperty':
                    district_field = field
                    break
            
            if district_field:
                print(f"🏢 Found districtproperty field: {district_field['label']} ({district_field['type']})")
            else:
                print(f"⚠️ No districtproperty field found in template")
        else:
            print("❌ Could not extract template")
            return
        
        # Step 3: Test classification search
        print(f"\n3️⃣ TESTING CLASSIFICATION SETS")
        print("-" * 40)
        
        classification_sets = processor.api_client.get_classification_sets()
        lmh_set = None
        for cls_set in classification_sets:
            if cls_set.get('name', '').upper() == 'LMH':
                lmh_set = cls_set
                break
        
        if lmh_set:
            print(f"✅ Found LMH classification set")
            items = lmh_set.get('items', [])
            print(f"   Available branches: {[item.get('label', 'Unknown') for item in items]}")
        else:
            print(f"⚠️ No LMH classification set found")
        
        # Step 4: Simulate the districtproperty workflow
        print(f"\n4️⃣ SIMULATING DISTRICTPROPERTY WORKFLOW")
        print("-" * 40)
        
        test_folder = "Bayview_Hills_District_ReplaceandRepair_20250822_143702"
        print(f"Test folder: {test_folder}")
        
        # This would normally be called during CSV transformation
        print(f"🔍 Classification search process:")
        print(f"   1. Extract 'bayview hills district' from folder name")
        print(f"   2. Search classification sets for matches")
        print(f"   3. If no exact match:")
        print(f"      → Show LMH branches (NAVY, MARINES)")
        print(f"      → User selects branch")
        print(f"      → User enters district name")
        print(f"      → Format as 'NAVY,Bayview Hills District'")
        
        # Step 5: Show the complete workflow
        print(f"\n5️⃣ COMPLETE WORKFLOW SUMMARY")
        print("-" * 40)
        
        print(f"📋 When you export 'Bayview Hills District' data and migrate to")
        print(f"   'Liberty Military Housing - Master', the system will:")
        
        print(f"\n🔄 FIELD MAPPING:")
        common_mappings = [
            ("Survey Date", "Survey Date"),
            ("Surveyed By", "Surveyed By"), 
            ("High Point", "High Point"),
            ("Low Point", "Low Point"),
            ("Before Photos", "Before Photos"),
            ("Address", "Address")
        ]
        
        for source, target in common_mappings:
            print(f"   ✅ {source} → {target}")
        
        print(f"\n🏢 DISTRICTPROPERTY HANDLING:")
        print(f"   1. Search for 'bayview hills district' in classification sets")
        print(f"   2. No exact match found")
        print(f"   3. Show LMH branches: NAVY, MARINES")
        print(f"   4. User selects: NAVY")
        print(f"   5. User enters: Bayview Hills District")
        print(f"   6. Result: 'NAVY,Bayview Hills District' in every row")
        
        print(f"\n📁 OUTPUT STRUCTURE:")
        print(f"   PropertyFolder_20250822_143702/")
        print(f"   ├── Bayview_Hills_District_data.csv         # Original export")
        print(f"   ├── Liberty_Military_Housing_migrated.csv   # Mapped to target form")
        print(f"   └── photos/                                 # All associated photos")
        print(f"       ├── photo_index.csv")
        print(f"       └── *.jpg files")
        
        print(f"\n🎯 READY TO USE!")
        print(f"   Run: python fulcrum_processor.py")
        print(f"   Choose option 3: Filter and export")
        print(f"   When asked to migrate → 'y'")
        print(f"   Search for 'liberty' → Select form")
        print(f"   Choose 'auto' mapping → Review if needed")
        print(f"   Classification search → Select NAVY + district name")
        print(f"   Get perfectly formatted CSV for import!")
        
    except Exception as e:
        print(f"❌ Demo failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    demo_complete_migration()