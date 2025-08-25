#!/usr/bin/env python3
"""
Test script to verify photo download functionality
"""

import sys
import os
# Add the project root to the path (two levels up from tests/unit/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_photo_download():
    """Test downloading a single photo"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    # Test with a known photo ID from the test export
    test_photo_id = "c6a09574-922a-4dd4-ab1f-1124cb0d3f17"
    
    print(f"🧪 Testing photo download...")
    print(f"📸 Photo ID: {test_photo_id}")
    
    try:
        from pathlib import Path
        
        # Create test directory
        test_dir = Path("test_photos")
        test_dir.mkdir(exist_ok=True)
        
        # Test getting photo info first
        print(f"\n📋 Getting photo metadata...")
        photo_info = processor.api_client.get_photo_info(test_photo_id)
        print(f"   Photo created: {photo_info.get('created_at', 'Unknown')}")
        print(f"   File size: {photo_info.get('file_size', 'Unknown')} bytes")
        print(f"   Has thumbnail: {'thumbnail' in photo_info}")
        print(f"   Has large: {'large' in photo_info}")
        print(f"   Has original: {'original' in photo_info}")
        
        # Test downloading thumbnail
        print(f"\n📥 Downloading thumbnail...")
        thumbnail_path = test_dir / f"{test_photo_id}_thumbnail.jpg"
        processor.api_client.download_photo(test_photo_id, thumbnail_path, size="thumbnail")
        
        if thumbnail_path.exists():
            file_size = thumbnail_path.stat().st_size
            print(f"✅ Thumbnail downloaded: {thumbnail_path} ({file_size} bytes)")
        else:
            print(f"❌ Thumbnail download failed")
            
        # Test downloading large version
        print(f"\n📥 Downloading large version...")
        large_path = test_dir / f"{test_photo_id}_large.jpg"
        processor.api_client.download_photo(test_photo_id, large_path, size="large")
        
        if large_path.exists():
            file_size = large_path.stat().st_size
            print(f"✅ Large photo downloaded: {large_path} ({file_size} bytes)")
        else:
            print(f"❌ Large photo download failed")
            
        print(f"\n🎉 Photo download test completed!")
        print(f"📁 Check the 'test_photos' folder to see the downloaded images")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_photo_download()