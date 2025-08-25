#!/usr/bin/env python3
"""
Test script to demonstrate concurrent photo downloads
"""

import sys
import os
import time
# Add the project root to the path (two levels up from tests/integration/)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fulcrum_processor import AdvancedFulcrumProcessor

def test_concurrent_download():
    """Test concurrent photo download speed"""
    processor = AdvancedFulcrumProcessor()
    
    if not processor.api_client:
        print("❌ API credentials not found")
        return
    
    # Get some photos to test with
    test_photo_ids = [
        "c6a09574-922a-4dd4-ab1f-1124cb0d3f17",
        "0d98a256-ce6a-4e68-a3ff-5e5825fe8fe5",
        "5ae20b37-704b-4ad4-97c9-03552c9fa940",
        "7c9f8444-ce0f-4b8c-9e11-e3b5a56436aa",
        "83b55139-dffc-4d85-a52a-39146c81f300"
    ]
    
    print(f"🚀 Testing concurrent download speed with {len(test_photo_ids)} photos")
    
    try:
        from pathlib import Path
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        # Create test directory
        test_dir = Path("test_concurrent")
        test_dir.mkdir(exist_ok=True)
        
        # Test 1: Sequential download (old way)
        print(f"\n⏳ Test 1: Sequential downloads...")
        start_time = time.time()
        
        for i, photo_id in enumerate(test_photo_ids, 1):
            try:
                photo_path = test_dir / f"sequential_{photo_id}.jpg"
                processor.api_client.download_photo(photo_id, photo_path, size="thumbnail")
                print(f"  📥 Sequential {i}/{len(test_photo_ids)}: {photo_id[:8]}...")
            except Exception as e:
                print(f"  ❌ Failed {photo_id}: {str(e)}")
        
        sequential_time = time.time() - start_time
        print(f"⏱️  Sequential time: {sequential_time:.2f} seconds")
        
        # Test 2: Concurrent download (new way)
        print(f"\n🚀 Test 2: Concurrent downloads...")
        start_time = time.time()
        
        def download_single_photo(photo_id):
            try:
                photo_path = test_dir / f"concurrent_{photo_id}.jpg"
                processor.api_client.download_photo(photo_id, photo_path, size="thumbnail")
                return {'success': True, 'photo_id': photo_id}
            except Exception as e:
                return {'success': False, 'photo_id': photo_id, 'error': str(e)}
        
        concurrent_success = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_photo = {executor.submit(download_single_photo, photo_id): photo_id 
                             for photo_id in test_photo_ids}
            
            for future in as_completed(future_to_photo):
                result = future.result()
                if result['success']:
                    concurrent_success += 1
                    print(f"  ✅ Concurrent: {result['photo_id'][:8]}...")
                else:
                    print(f"  ❌ Failed {result['photo_id']}: {result['error']}")
        
        concurrent_time = time.time() - start_time
        print(f"⏱️  Concurrent time: {concurrent_time:.2f} seconds")
        
        # Results
        speedup = sequential_time / concurrent_time if concurrent_time > 0 else 0
        print(f"\n🎉 SPEED IMPROVEMENT RESULTS:")
        print(f"   Sequential: {sequential_time:.2f} seconds")
        print(f"   Concurrent: {concurrent_time:.2f} seconds")
        print(f"   Speedup: {speedup:.1f}x faster! 🚀")
        print(f"   Time saved: {sequential_time - concurrent_time:.2f} seconds")
        
        # Cleanup
        import shutil
        shutil.rmtree(test_dir)
        print(f"\n🧹 Cleaned up test files")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_concurrent_download()