# Debug Tools

This folder contains debugging utilities for development and troubleshooting.

## Debug Files

- `debug_export.py` - Debug form schema and field mapping issues
- `test_photos/` - Sample downloaded photos for testing

## Running Debug Tools

From the project root directory:

```bash
# Debug form field mapping
python debug/debug_export.py
```

## What debug_export.py Does

- Fetches form schema and shows field structure
- Maps form field keys to human-readable labels  
- Shows sample record data to verify mapping
- Helps troubleshoot column name issues

## Usage

Use these tools when:
- Column names aren't showing correctly
- Need to understand form structure
- Debugging field mapping issues
- Investigating API responses

## Requirements

- Valid Fulcrum API credentials configured
- Internet connection for API calls