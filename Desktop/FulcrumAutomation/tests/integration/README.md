# Integration Tests

This folder contains integration tests that test complete workflows and system interactions.

## Test Files

- `test_organized_export.py` - Test the complete organized folder structure creation
- `test_concurrent_download.py` - Test concurrent photo download performance
- `test_form_migration.py` - Test the form migration workflow components
- `test_migration_demo.py` - Demo the complete migration workflow
- `test_classification_search.py` - Test classification set search functionality
- `test_lmh_classification.py` - Test Liberty Military Housing classification sets
- `test_lmh_structure.py` - Test detailed LMH classification structure
- `test_liberty_forms_fields.py` - Check Liberty forms for districtproperty field
- `test_complete_migration_demo.py` - Complete end-to-end migration demo
- `test_data_source_generation.py` - Test data source field with form ID
- `test_final_complete_workflow.py` - Final comprehensive workflow demo

## Running Tests

From the project root directory:

```bash
# Run individual integration tests
python tests/integration/test_organized_export.py
python tests/integration/test_concurrent_download.py
```

## What These Tests Do

### test_organized_export.py
- Creates the full property folder structure
- Tests CSV export and photos folder creation
- Verifies proper organization

### test_concurrent_download.py  
- Compares sequential vs concurrent download speeds
- Demonstrates 4-5x speed improvement
- Tests error handling in concurrent downloads

## Requirements

- Valid Fulcrum API credentials configured
- Internet connection for API calls
- Write permissions for cached/ directory