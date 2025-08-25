#!/usr/bin/env python3
"""
Test script to demonstrate the fixed CSV export functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/unit/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_export():
    """Test the export functionality with simulated responses"""
    processor = AdvancedFulcrumProcessor()
    
    # Check if API credentials are set up
    if not processor.api_client:
        print("❌ API credentials not found. Please run setup first (option 1 in main script)")
        return
    
    # Test form ID for Bayview Hills District
    form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
    form_name = "Bayview Hills District"
    
    print(f"🧪 Testing CSV export for: {form_name}")
    print(f"📋 Form ID: {form_id}")
    
    try:
        # Call the filter and export function directly
        result = processor.filter_and_export_by_status(form_id, form_name)
        
        if result:
            print(f"\n✅ Export test completed successfully!")
            print(f"📁 File saved: {result}")
            
            # Read first few lines to show the improved format
            import pandas as pd
            df = pd.read_csv(result)
            
            print(f"\n📊 CSV Preview:")
            print(f"Columns: {len(df.columns)}")
            print(f"Rows: {len(df)}")
            print(f"\nColumn names:")
            for i, col in enumerate(df.columns[:10], 1):
                print(f"  {i}. {col}")
            if len(df.columns) > 10:
                print(f"  ... and {len(df.columns) - 10} more columns")
                
        else:
            print(f"\n❌ Export test failed")
            
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")

if __name__ == "__main__":
    test_export()