# Unit Tests

This folder contains unit tests for individual components of the Fulcrum processor.

## Test Files

- `test_export.py` - Test CSV export functionality with proper column mapping
- `test_quick_export.py` - Quick test with a small dataset 
- `test_photo_download.py` - Test individual photo download functionality

## Running Tests

From the project root directory:

```bash
# Run individual tests
python tests/unit/test_export.py
python tests/unit/test_quick_export.py
python tests/unit/test_photo_download.py
```

## Requirements

- Valid Fulcrum API credentials configured
- Internet connection for API calls
- Write permissions for cached/ directory