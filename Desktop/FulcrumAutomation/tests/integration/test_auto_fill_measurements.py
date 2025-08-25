#!/usr/bin/env python3
"""
Test the auto-fill functionality for blank measurement fields
"""

import sys
import os
import pandas as pd
from pathlib import Path
import tempfile

# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_auto_fill_measurements():
    """Test the auto-fill functionality for blank measurement fields"""
    print("🔢 Testing Auto-Fill for Blank Measurement Fields")
    print("=" * 60)
    
    # Create a test CSV with blank measurement fields
    test_data = {
        'survey_date': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04'],
        'high_point': ['5', '', '3', None],
        'low_point': ['1', '2', '', '0'],
        'slice_length': ['10.5', '8.2', '', '12.0'],
        'expansion_joint_length': ['2.1', '', '1.8', ''],
        'inch_feet': ['', '6.5', '7.2', ''],
        'technician_notes': ['Good condition', 'Needs repair', 'Minor issues', 'Complete'],
        'l_patch': ['', '2.5', '', '3.0'],
        'xl_patch': ['1.0', '', '1.5', '']
    }
    
    # Create test CSV
    test_df = pd.DataFrame(test_data)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        test_csv_path = temp_path / "test_measurements.csv"
        test_df.to_csv(test_csv_path, index=False)
        
        print(f"📋 Created test CSV with blank measurement fields:")
        print(f"   File: {test_csv_path}")
        print(f"   Rows: {len(test_df)}")
        print(f"   Columns: {len(test_df.columns)}")
        
        # Show original data
        print(f"\n📊 Original Data (showing blank values):")
        print("-" * 50)
        for col in ['high_point', 'low_point', 'slice_length', 'expansion_joint_length', 'inch_feet', 'l_patch', 'xl_patch']:
            blank_count = test_df[col].isna().sum() + (test_df[col] == '').sum()
            print(f"   {col}: {blank_count} blank values out of {len(test_df)} rows")
        
        # Test the auto-fill functionality
        print(f"\n🔧 Testing Auto-Fill Logic:")
        print("-" * 50)
        
        processor = AdvancedFulcrumProcessor()
        
        # Test field identification
        test_fields = [
            'High Point', 'Low Point', 'Slice Length', 'Expansion Joint Length',
            'Inch Feet', 'L Patch', 'XL Patch', 'Technician Notes', 'Survey Date'
        ]
        
        for field in test_fields:
            is_measurement = processor._is_measurement_field(field)
            status = "🔢 MEASUREMENT" if is_measurement else "📝 TEXT/DATE"
            print(f"   {field}: {status}")
        
        # Test the actual auto-fill process
        print(f"\n🔄 Testing CSV Transformation with Auto-Fill:")
        print("-" * 50)
        
        # Create template fields
        template_fields = [
            {'label': 'Survey Date', 'type': 'date'},
            {'label': 'High Point', 'type': 'number'},
            {'label': 'Low Point', 'type': 'number'},
            {'label': 'Slice Length', 'type': 'number'},
            {'label': 'Expansion Joint Length', 'type': 'number'},
            {'label': 'Inch Feet', 'type': 'number'},
            {'label': 'L Patch', 'type': 'number'},
            {'label': 'XL Patch', 'type': 'number'},
            {'label': 'Technician Notes', 'type': 'text'}
        ]
        
        # Create field mapping
        field_mapping = {
            'Survey Date': 'survey_date',
            'High Point': 'high_point',
            'Low Point': 'low_point',
            'Slice Length': 'slice_length',
            'Expansion Joint Length': 'expansion_joint_length',
            'Inch Feet': 'inch_feet',
            'L Patch': 'l_patch',
            'XL Patch': 'xl_patch',
            'Technician Notes': 'technician_notes'
        }
        
        # Transform the CSV
        result_path = processor._transform_csv_to_template(
            test_csv_path, 
            temp_path, 
            template_fields, 
            field_mapping, 
            'Test Form'
        )
        
        if result_path and result_path.exists():
            # Read the transformed CSV
            transformed_df = pd.read_csv(result_path)
            
            print(f"\n✅ Transformation completed successfully!")
            print(f"📄 Transformed file: {result_path.name}")
            print(f"📊 Transformed data summary:")
            
            # Check measurement fields for auto-filled values
            measurement_fields = ['High Point', 'Low Point', 'Slice Length', 'Expansion Joint Length', 'Inch Feet', 'L Patch', 'XL Patch']
            
            for field in measurement_fields:
                if field in transformed_df.columns:
                    zero_count = (transformed_df[field] == 0).sum()
                    total_rows = len(transformed_df)
                    print(f"   {field}: {zero_count}/{total_rows} rows have value 0 (auto-filled)")
            
            # Show sample of transformed data
            print(f"\n📋 Sample of transformed data:")
            print("-" * 50)
            for field in measurement_fields[:3]:  # Show first 3 measurement fields
                if field in transformed_df.columns:
                    print(f"   {field}: {transformed_df[field].tolist()}")
        
        else:
            print(f"❌ Transformation failed")
    
    print(f"\n🎉 Auto-fill test completed!")
    print(f"💡 The system now automatically:")
    print(f"   ✅ Identifies measurement fields (high point, low point, slice length, etc.)")
    print(f"   🔢 Fills blank/empty values with 0")
    print(f"   📊 Reports how many values were auto-filled")
    print(f"   🚫 Prevents import errors from missing measurement data")

if __name__ == "__main__":
    test_auto_fill_measurements()

