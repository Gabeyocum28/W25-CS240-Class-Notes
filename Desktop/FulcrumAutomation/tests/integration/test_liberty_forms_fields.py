#!/usr/bin/env python3
"""
Check all Liberty Military Housing forms for districtproperty field
"""

import sys
import os
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def check_liberty_forms():
    """Check all Liberty Military Housing forms for fields"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    print("🔍 Checking Liberty Military Housing Forms for Fields")
    print("=" * 60)
    
    try:
        all_forms = processor.api_client.get_forms('all')
        liberty_forms = [f for f in all_forms if 'liberty' in f.get('name', '').lower() and 'military' in f.get('name', '').lower()]
        
        print(f"📋 Found {len(liberty_forms)} Liberty Military Housing forms:")
        
        for i, form in enumerate(liberty_forms, 1):
            name = form.get('name', 'Unknown')
            status = form.get('status', 'Unknown')
            print(f"\n{i}. {name} ({status})")
            print("-" * 50)
            
            try:
                template_fields = processor._get_form_template(form['id'], name)
                
                if template_fields:
                    print(f"📊 {len(template_fields)} fields found:")
                    
                    # Look for districtproperty or similar fields
                    district_fields = []
                    property_fields = []
                    
                    for field in template_fields:
                        label = field['label'].lower()
                        if 'district' in label:
                            district_fields.append(field)
                        elif 'property' in label:
                            property_fields.append(field)
                    
                    if district_fields:
                        print(f"🏢 District-related fields:")
                        for field in district_fields:
                            req = " *" if field['required'] else ""
                            print(f"   • {field['label']}{req} ({field['type']})")
                    
                    if property_fields:
                        print(f"🏠 Property-related fields:")
                        for field in property_fields:
                            req = " *" if field['required'] else ""
                            print(f"   • {field['label']}{req} ({field['type']})")
                    
                    if not district_fields and not property_fields:
                        print(f"⚠️ No district/property fields found")
                    
                    # Show all fields for the first form as example
                    if i == 1:
                        print(f"\n📋 All fields in '{name}':")
                        for field in template_fields:
                            req = " *" if field['required'] else ""
                            print(f"   • {field['label']}{req} ({field['type']})")
                
                else:
                    print(f"❌ Could not extract template")
                    
            except Exception as e:
                print(f"❌ Error processing form: {str(e)}")
        
        # Also check for forms with "legacy" in the name
        print(f"\n🔍 Checking for 'legacy' forms...")
        legacy_forms = [f for f in all_forms if 'legacy' in f.get('name', '').lower()]
        
        if legacy_forms:
            print(f"📋 Found {len(legacy_forms)} forms with 'legacy':")
            
            for form in legacy_forms[:5]:  # Check first 5
                name = form.get('name', 'Unknown')
                status = form.get('status', 'Unknown')
                print(f"\n📄 {name} ({status})")
                
                try:
                    template_fields = processor._get_form_template(form['id'], name)
                    
                    if template_fields:
                        # Look for districtproperty
                        district_property_fields = [f for f in template_fields if 'districtproperty' in f['label'].lower().replace(' ', '')]
                        
                        if district_property_fields:
                            print(f"🎯 FOUND districtproperty field!")
                            for field in district_property_fields:
                                req = " *" if field['required'] else ""
                                print(f"   • {field['label']}{req} ({field['type']})")
                        else:
                            # Show similar fields
                            similar_fields = [f for f in template_fields if 'district' in f['label'].lower() or 'property' in f['label'].lower()]
                            if similar_fields:
                                print(f"   Similar fields:")
                                for field in similar_fields:
                                    req = " *" if field['required'] else ""
                                    print(f"   • {field['label']}{req} ({field['type']})")
                            else:
                                print(f"   No district/property fields")
                        
                except Exception as e:
                    print(f"❌ Error: {str(e)}")
        else:
            print(f"⚠️ No forms with 'legacy' found")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_liberty_forms()