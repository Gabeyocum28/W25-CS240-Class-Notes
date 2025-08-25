#!/usr/bin/env python3
"""
Fully Automated Fulcrum Processing Script
Run this script for completely hands-off processing with pre-configured settings
"""

from fulcrum_processor import AdvancedFulcrumProcessor
import sys

def automated_processing():
    """Run fully automated processing with pre-configured settings"""
    
    # Initialize processor
    processor = AdvancedFulcrumProcessor()
    
    # Check if API is configured
    if not processor.api_client:
        print("❌ API credentials not found!")
        print("Please run the main script first to setup credentials:")
        print("python fulcrum_processor.py")
        return False
    
    # Check if target form is configured
    if not processor.config.get('fulcrum', {}).get('target_form_id'):
        print("❌ Target form not configured!")
        print("Please run the main script first to setup target form:")
        print("python fulcrum_processor.py")
        return False
    
    # Configuration - CUSTOMIZE THESE VALUES
    properties_to_process = [
        "bayview hills",
        "catalina heights",
        "liberty base"
    ]
    
    source_form_name = "Property Survey Form"  # Set your source form name here
    
    print("🚀 AUTOMATED FULCRUM PROCESSING")
    print("=" * 50)
    print(f"Properties to process: {len(properties_to_process)}")
    print(f"Source form: {source_form_name}")
    print(f"Target form: {processor.config['fulcrum']['target_form_name']}")
    print("=" * 50)
    
    success_count = 0
    total_records = 0
    
    for i, property_name in enumerate(properties_to_process, 1):
        print(f"\n🏗️  PROCESSING {i}/{len(properties_to_process)}: {property_name}")
        print("-" * 60)
        
        try:
            # Process with auto-import enabled
            result = processor.process_property(
                property_name=property_name,
                form_name=source_form_name,
                import_data=True
            )
            
            success_count += 1
            print(f"✅ {property_name} completed successfully!")
            
        except Exception as e:
            print(f"❌ {property_name} failed: {str(e)}")
            # Continue with next property instead of stopping
            continue
    
    # Final summary
    print(f"\n🎯 AUTOMATION COMPLETE!")
    print("=" * 50)
    print(f"✅ Successfully processed: {success_count}/{len(properties_to_process)}")
    print(f"❌ Failed: {len(properties_to_process) - success_count}")
    print(f"📊 All successful data has been imported to Fulcrum")
    
    if success_count == len(properties_to_process):
        print("🎉 All properties processed successfully!")
        return True
    else:
        print("⚠️  Some properties failed - check logs above")
        return False

def scheduled_processing():
    """Version for scheduled/cron job execution"""
    try:
        success = automated_processing()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Critical error in automated processing: {str(e)}")
        sys.exit(2)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--scheduled":
        # For cron jobs or scheduled execution
        scheduled_processing()
    else:
        # Interactive mode
        automated_processing()
