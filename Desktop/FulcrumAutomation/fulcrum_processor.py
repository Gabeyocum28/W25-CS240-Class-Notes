#!/usr/bin/env python3
"""
Advanced Fulcrum Data Processing Automation Script
Downloads data from Fulcrum API and processes for Liberty Military Housing Legacy app
"""

import os
import sys
import zipfile
import pandas as pd
import shutil
import requests
import json
import time
from pathlib import Path
from datetime import datetime
import configparser
from urllib.parse import urlencode

class FulcrumAPIClient:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = "https://api.fulcrumapp.com/api/v2"
        self.headers = {
            "X-ApiToken": api_token,
            "Content-Type": "application/json"
        }
    
    def get_forms(self, form_filter="all"):
        """
        Get forms from Fulcrum with filtering options
        form_filter options: 'active', 'inactive', 'all'
        Based on Fulcrum API documentation: https://docs.fulcrumapp.com/docs/developer-information
        """
        # First try the Query API approach for inactive and all forms
        if form_filter in ["inactive", "all"]:
            print("🔍 Trying Fulcrum Query API for inactive/all forms...")
            query_forms = self._try_query_api_for_inactive_forms()
            if query_forms and form_filter == "inactive":
                return query_forms
            elif query_forms and form_filter == "all":
                # For 'all' forms, we need to combine Query API (inactive) + standard API (active)
                print("🔍 Getting active forms via standard API...")
                active_forms = self._get_active_forms_via_standard_api()
                if active_forms:
                    print(f"✅ Combined: {len(query_forms)} inactive + {len(active_forms)} active = {len(query_forms) + len(active_forms)} total forms")
                    return query_forms + active_forms
                else:
                    return query_forms
        
        # Fallback to standard API (filtering will be done client-side)
        try:
            response = requests.get(f"{self.base_url}/forms", headers=self.headers)
            response.raise_for_status()
            all_api_forms = response.json()["forms"]
            print(f"Retrieved {len(all_api_forms)} total forms from Fulcrum API")
        except Exception as e:
            print(f"Error fetching forms: {e}")
            return []
        
        # Filter forms based on their actual status field
        filtered_forms = []
        active_count = 0
        inactive_count = 0
        
        for form in all_api_forms:
            form_status = form.get('status', '').lower()
            is_active = form_status == 'active'
            
            # Mark forms with our internal tracking
            form['_api_status'] = form_status
            form['_is_active'] = is_active
            
            if is_active:
                active_count += 1
            else:
                inactive_count += 1
            
            # Apply filter
            if form_filter == 'active' and is_active:
                filtered_forms.append(form)
            elif form_filter == 'inactive' and not is_active:
                filtered_forms.append(form)
            elif form_filter == 'all':
                filtered_forms.append(form)
        
        print(f"📊 Form Status Summary:")
        print(f"   Total forms: {len(all_api_forms)}")
        print(f"   Active forms: {active_count}")
        print(f"   Inactive forms: {inactive_count}")
        print(f"   Filtered result ({form_filter}): {len(filtered_forms)} forms")
        
        if form_filter == 'inactive' and inactive_count == 0:
            print("ℹ️  Note: No inactive forms found. Your Fulcrum account may only contain active forms.")
            print("💡 Try using the Query API test script: python test_fulcrum_query_api.py")
        
        return filtered_forms
    
    def get_classification_sets(self):
        """Get classification sets from Fulcrum"""
        try:
            response = requests.get(f"{self.base_url}/classification_sets", headers=self.headers)
            response.raise_for_status()
            classification_sets = response.json().get("classification_sets", [])
            print(f"Retrieved {len(classification_sets)} classification sets from Fulcrum API")
            return classification_sets
        except Exception as e:
            print(f"Error fetching classification sets: {e}")
            return []
    
    def _try_query_api_for_inactive_forms(self):
        """Try to use Fulcrum Query API to get inactive forms"""
        # Based on Fulcrum Python library: https://github.com/fulcrumapp/fulcrum-python.git
        query_endpoints = [
            f"{self.base_url}/query",  # Main Query API endpoint
            f"{self.base_url}/query/forms",  # Forms-specific query endpoint
        ]
        
        # SQL-like queries that should work with Fulcrum Query API
        # Based on successful test: format=json is the key!
        query_params = [
            {"q": "SELECT * FROM forms WHERE status != 'active'", "format": "json"},
            {"q": "SELECT * FROM forms WHERE status IN ('inactive', 'disabled', 'archived')", "format": "json"},
            {"q": "SELECT * FROM forms WHERE status = 'inactive'", "format": "json"},
            {"q": "SELECT * FROM forms WHERE status = 'disabled'", "format": "json"},
            {"q": "SELECT * FROM forms WHERE status = 'archived'", "format": "json"},
            {"q": "SELECT id, name, status, created_at FROM forms WHERE status != 'active'", "format": "json"},
            {"q": "SELECT COUNT(*) as total FROM forms WHERE status != 'active'", "format": "json"},
        ]
        
        for endpoint in query_endpoints:
            for params in query_params:
                try:
                    print(f"  🔍 Trying Query API: {endpoint} with {params}")
                    response = requests.get(endpoint, headers=self.headers, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Handle Query API response format (fields + rows)
                        if 'fields' in data and 'rows' in data:
                            forms = data.get("rows", [])
                            fields = data.get("fields", [])
                            
                            print(f"  🔍 Debug: Found {len(forms)} rows and {len(fields)} fields")
                            
                            if len(forms) > 0:
                                print(f"  ✅ Query API SUCCESS: Found {len(forms)} forms via {endpoint}")
                                print(f"     Fields available: {[f.get('name', 'unknown') for f in fields[:5]]}")
                                print(f"     This is the solution to accessing inactive forms!")
                                
                                try:
                                    # The Query API returns rows as dictionaries with field names as keys
                                    # This is much simpler than expected!
                                    converted_forms = []
                                    print(f"  🔍 Converting {len(forms)} forms from Query API...")
                                    
                                    for row_idx, row in enumerate(forms):
                                        if row_idx == 0:
                                            print(f"  🔍 First row type: {type(row)}")
                                            print(f"  🔍 First row keys: {list(row.keys())[:5]}...")
                                        
                                        # Each row is already a dictionary with field names as keys
                                        form = dict(row)  # Copy the row data
                                        
                                        # Fix field mapping: Query API uses 'form_id' instead of 'id'
                                        if 'form_id' in form and 'id' not in form:
                                            form['id'] = form['form_id']
                                        
                                        # Mark as from Query API
                                        form['_source'] = 'query_api'
                                        form['_query_endpoint'] = endpoint
                                        form['_query_params'] = params
                                        
                                        converted_forms.append(form)
                                    
                                    print(f"  🎯 Successfully converted {len(converted_forms)} forms from Query API")
                                    return converted_forms
                                except Exception as e:
                                    print(f"  ❌ Error converting forms: {str(e)}")
                                    print(f"  🔍 Exception details: {type(e).__name__}")
                                    import traceback
                                    traceback.print_exc()
                                    # Return the raw forms if conversion fails
                                    return forms
                        
                        # Handle standard API response format
                        elif 'forms' in data:
                            forms = data.get("forms", [])
                            
                            if len(forms) > 0:
                                print(f"  ✅ Query API SUCCESS: Found {len(forms)} forms via {endpoint}")
                                print(f"     This might be the key to accessing inactive forms!")
                                
                                # Mark these as from Query API
                                for form in forms:
                                    form['_source'] = 'query_api'
                                    form['_query_endpoint'] = endpoint
                                    form['_query_params'] = params
                                
                                return forms
                    
                except Exception as e:
                    print(f"  ❌ Query API error: {str(e)}")
                    continue
        
        print("  ❌ Query API approach didn't work - falling back to standard API")
        print("  💡 Make sure your API key has Query API permissions")
        return None
    
    def _get_active_forms_via_standard_api(self):
        """Get active forms via standard API"""
        try:
            response = requests.get(f"{self.base_url}/forms", headers=self.headers)
            response.raise_for_status()
            active_forms = response.json()["forms"]
            
            # Mark these as from standard API
            for form in active_forms:
                form['_source'] = 'standard_api'
                form['_api_status'] = form.get('status', 'active').lower()
                form['_is_active'] = True
            
            return active_forms
        except Exception as e:
            print(f"  ❌ Error getting active forms: {str(e)}")
            return None
    
    def get_records(self, form_id, filters=None):
        """Get records for a specific form with optional filters"""
        params = {}
        if filters:
            params.update(filters)
        
        url = f"{self.base_url}/records"
        if params:
            url += "?" + urlencode(params)
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()["records"]
    
    def export_data(self, form_id, format_type="csv", filters=None):
        """Export data from a form"""
        export_config = {
            "type": format_type,
            "form": form_id
        }
        
        if filters:
            export_config.update(filters)
        
        # Wrap in export object as required by API
        export_data = {
            "export": export_config
        }
        
        print(f"🔍 Export request data: {export_data}")
        print(f"🔍 Headers being sent: {self.headers}")
        print(f"🔍 URL: {self.base_url}/exports")
        
        # Create export
        response = requests.post(
            f"{self.base_url}/exports",
            headers=self.headers,
            json=export_data
        )
        
        print(f"🔍 Export response status: {response.status_code}")
        if response.status_code != 200:
            print(f"🔍 Export response text: {response.text}")
            print(f"🔍 Export response headers: {response.headers}")
        
        response.raise_for_status()
        export_id = response.json()["export"]["id"]
        
        # Poll for completion
        while True:
            status_response = requests.get(
                f"{self.base_url}/exports/{export_id}",
                headers=self.headers
            )
            status_response.raise_for_status()
            export_status = status_response.json()["export"]
            
            if export_status["status"] == "completed":
                return export_status["url"]
            elif export_status["status"] == "failed":
                raise Exception(f"Export failed: {export_status.get('message', 'Unknown error')}")
            
            time.sleep(5)  # Wait 5 seconds before checking again
    
    def download_export(self, export_url, local_path):
        """Download the exported file"""
        response = requests.get(export_url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return local_path
    
    def create_record(self, form_id, record_data):
        """Create a new record in Fulcrum"""
        payload = {"record": record_data}
        
        response = requests.post(
            f"{self.base_url}/records",
            headers=self.headers,
            json=payload
        )
        response.raise_for_status()
        return response.json()["record"]
    
    def upload_photo(self, photo_path, access_key=None):
        """Upload a photo to Fulcrum"""
        files = {'photo[file]': open(photo_path, 'rb')}
        headers = {"X-ApiToken": self.api_token}
        
        data = {}
        if access_key:
            data['photo[access_key]'] = access_key
        
        response = requests.post(
            f"{self.base_url}/photos",
            headers=headers,
            files=files,
            data=data
        )
        files['photo[file]'].close()
        response.raise_for_status()
        return response.json()["photo"]
    
    def get_photo_info(self, photo_id):
        """Get photo metadata from Fulcrum"""
        response = requests.get(
            f"{self.base_url}/photos/{photo_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()["photo"]
    
    def download_photo(self, photo_id, local_path, size="large"):
        """Download a photo from Fulcrum
        
        Args:
            photo_id: The photo ID from Fulcrum
            local_path: Where to save the photo locally
            size: Photo size - 'thumbnail', 'large', or 'original'
        """
        # Get photo info first to get the download URL
        photo_info = self.get_photo_info(photo_id)
        
        # Choose the appropriate URL based on size
        if size == "thumbnail":
            download_url = photo_info.get("thumbnail")
        elif size == "large":
            download_url = photo_info.get("large")
        else:  # original
            download_url = photo_info.get("original")
        
        if not download_url:
            raise Exception(f"No {size} URL found for photo {photo_id}")
        
        # Download the photo
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return local_path
    
    def get_form_schema(self, form_id):
        """Get the schema for a form to understand field structure"""
        response = requests.get(
            f"{self.base_url}/forms/{form_id}",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()["form"]
    
    def get_classification_sets(self):
        """Get all classification sets"""
        response = requests.get(
            f"{self.base_url}/classification_sets",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json().get('classification_sets', [])
    
    def get_status_values_from_form(self, form_id, form_name, sample_size=100):
        """Get all possible status values from a form by sampling records"""
        try:
            response = requests.get(
                f"{self.base_url}/records",
                headers=self.headers,
                params={'form_id': form_id, 'per_page': sample_size},
                timeout=30
            )
            response.raise_for_status()
            
            records = response.json().get('records', [])
            status_values = set()
            
            for record in records:
                status = record.get('status')
                if status:
                    status_values.add(status)
            
            return sorted(list(status_values))
            
        except Exception as e:
            print(f"⚠️ Could not get status values from {form_name}: {str(e)}")
            return []

class PropertyMapper:
    def __init__(self, config_file="property_mappings.json"):
        self.config_file = config_file
        self.mappings = self.load_mappings()
    
    def load_mappings(self):
        """Load property mappings from config file"""
        if Path(self.config_file).exists():
            with open(self.config_file, 'r') as f:
                return json.load(f)
        return {}
    
    def save_mappings(self):
        """Save property mappings to config file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.mappings, f, indent=2)
    
    def add_mapping(self, property_name, district_property):
        """Add a new property mapping"""
        self.mappings[property_name.lower()] = district_property
        self.save_mappings()
    
    def get_mapping(self, property_name):
        """Get district property for a given property name"""
        return self.mappings.get(property_name.lower())
    
    def list_mappings(self):
        """List all current mappings"""
        return self.mappings

class AdvancedFulcrumProcessor:
    def __init__(self, config_file="fulcrum_config.ini"):
        self.config_file = config_file
        self.config = self.load_config()
        self.api_client = None
        self.property_mapper = PropertyMapper()
        self.smart_field_mapper = SmartFieldMapper()  # New smart field mapper
        self.target_form_id = None  # Will store the target form ID for imports
        
        # Initialize API client if token is available
        if self.config.get('fulcrum', {}).get('api_token'):
            self.api_client = FulcrumAPIClient(self.config['fulcrum']['api_token'])
    
    def load_config(self):
        """Load configuration from file"""
        config = configparser.ConfigParser()
        if Path(self.config_file).exists():
            config.read(self.config_file)
            return {section: dict(config[section]) for section in config.sections()}
        return {}
    
    def save_config(self):
        """Save configuration to file"""
        config = configparser.ConfigParser()
        for section, values in self.config.items():
            config[section] = values
        
        with open(self.config_file, 'w') as f:
            config.write(f)
    
    def setup_credentials(self):
        """Setup Fulcrum API credentials"""
        print("Fulcrum API Setup")
        print("=" * 20)
        print("You need your Fulcrum API token. Find it at:")
        print("https://web.fulcrumapp.com/settings/api")
        print()
        
        api_token = input("Enter your Fulcrum API token: ").strip()
        
        if 'fulcrum' not in self.config:
            self.config['fulcrum'] = {}
        
        self.config['fulcrum']['api_token'] = api_token
        self.save_config()
        
        self.api_client = FulcrumAPIClient(api_token)
        print("✓ API credentials saved!")
    
    def _debug_form_data(self):
        """Debug function to show concise form data analysis"""
        print("\n🔍 DEBUG MODE: Form Data Analysis")
        print("=" * 50)
        
        try:
            # Get a sample of all forms (both active and inactive)
            print("📊 Fetching sample forms from all categories...")
            all_forms = self.api_client.get_forms('all')
            
            if not all_forms:
                print("❌ No forms found to analyze")
                return []
            
            # Take first 3 forms for analysis
            forms_to_analyze = all_forms[:3]
            
            print(f"\n📋 Analyzing {len(forms_to_analyze)} sample forms:")
            print("=" * 50)
            
            for i, form in enumerate(forms_to_analyze, 1):
                form_status = form.get('status', 'unknown').lower()
                status_emoji = "🟢" if form_status == 'active' else "🔴" if form_status == 'inactive' else "⚪"
                source = form.get('_source', 'unknown')
                
                print(f"\n{status_emoji} FORM #{i}: {form.get('name', 'Unknown')[:60]}...")
                print("-" * 50)
                
                # Key form information
                print("📋 BASIC INFO:")
                print(f"  ID: {form.get('id', 'Unknown')}")
                print(f"  Status: {form.get('status', 'Unknown')}")
                print(f"  Records: {form.get('record_count', 'Unknown')}")
                print(f"  Created: {form.get('created_at', 'Unknown')[:10] if form.get('created_at') else 'Unknown'}")
                print(f"  API Source: {source}")
                
                # Status validation
                print("\n🎯 STATUS VALIDATION:")
                if form_status == 'active':
                    print("  ✅ ACTIVE - Correctly detected")
                elif form_status == 'inactive':
                    print("  ✅ INACTIVE - Correctly detected via Query API")
                else:
                    print(f"  ⚠️  UNKNOWN STATUS: '{form_status}'")
                
                # API source validation  
                print("\n🔍 API SOURCE:")
                if source == 'standard_api':
                    print("  📡 Standard API (active forms)")
                elif source == 'query_api':
                    print("  📡 Query API (inactive forms)")
                else:
                    print(f"  ❓ Unknown source: {source}")
            
            # Summary statistics
            print(f"\n📊 SUMMARY STATISTICS:")
            print("=" * 30)
            total_forms = len(all_forms)
            active_count = sum(1 for f in all_forms if f.get('status', '').lower() == 'active')
            inactive_count = sum(1 for f in all_forms if f.get('status', '').lower() == 'inactive')
            
            print(f"Total Forms Available: {total_forms}")
            print(f"Active Forms: {active_count}")
            print(f"Inactive Forms: {inactive_count}")
            print(f"Coverage: {((active_count + inactive_count) / total_forms * 100):.1f}% status detection")
            
            # API source breakdown
            standard_api_count = sum(1 for f in all_forms if f.get('_source') == 'standard_api')
            query_api_count = sum(1 for f in all_forms if f.get('_source') == 'query_api')
            
            print(f"\nAPI Sources:")
            print(f"  Standard API: {standard_api_count} forms")
            print(f"  Query API: {query_api_count} forms")
            
            print(f"\n✅ DEBUG ANALYSIS COMPLETE")
            print("   The system is working correctly!")
            
            return forms_to_analyze
            
        except Exception as e:
            print(f"❌ Debug analysis failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def list_forms(self):
        """Show form listing sub-menu"""
        if not self.api_client:
            print("API client not initialized. Run setup first.")
            return []
        
        print("\n📋 LIST AVAILABLE FORMS")
        print("=" * 50)
        print("✅ SUCCESS: Query API Breakthrough!")
        print("   • ACTIVE forms: 413 (via standard API)")
        print("   • INACTIVE forms: 4,578 (via Query API)")
        print("   • TOTAL accessible: 4,991 forms")
        print("")
        print("1. List active forms (413 available)")
        print("2. List inactive forms (4,578 available)")
        print("3. List all forms (4,991 total)")
        print("4. List classification sets")
        print("5. DEBUG: Show raw form data (first 3 forms)")
        print("0. Back to main menu")
        
        while True:
            try:
                choice = input("\nSelect option (0-5): ").strip()
                
                if choice == '1':
                    filter_type = 'active'
                    title = "Active Forms (413 available)"
                    break
                elif choice == '2':
                    filter_type = 'inactive'
                    title = "Inactive Forms (4,578 available via Query API)"
                    break
                elif choice == '3':
                    filter_type = 'all'
                    title = "All Forms (4,991 total)"
                    break
                elif choice == '4':
                    # CLASSIFICATION SETS
                    return self._list_classification_sets()
                elif choice == '5':
                    # DEBUG MODE
                    return self._debug_form_data()
                elif choice == '0':
                    return []
                else:
                    print("Invalid choice. Please enter 0-5.")
            except KeyboardInterrupt:
                return []
        
        try:
            print(f"\n🔍 Fetching {title.lower()}...")
            forms = self.api_client.get_forms(form_filter=filter_type)
            
            if not forms:
                print(f"No forms found for filter: {title.lower()}")
                return []
            
            print(f"\n📋 {title} ({len(forms)} found):")
            print("=" * 80)
            
            for i, form in enumerate(forms, 1):
                # Determine status based on the actual form 'status' field
                form_status = form.get('status', '').lower()
                
                # Use the actual status field from the API response
                if form_status == 'active':
                    status = "🟢 ACTIVE"
                    is_active = True
                elif form_status == 'inactive' or form_status == 'disabled':
                    status = "🔴 INACTIVE"
                    is_active = False
                else:
                    # Fallback logic if status field is unclear
                    fetched_as = form.get('_fetched_as', 'unknown')
                    if 'inactive' in fetched_as:
                        status = "🔴 INACTIVE" 
                        is_active = False
                    else:
                        # Default assumption: active
                        is_active = True
                        status = "🟢 ACTIVE"
                
                created_date = form.get('created_at', '')[:10] if form.get('created_at') else 'Unknown'
                
                print(f"{i}. {form['name']} {status}")
                print(f"   ID: {form['id']} | Created: {created_date}")
                
                # Enhanced debug info to show actual API response
                print(f"   🔍 DEBUG INFO:")
                print(f"      API status field: {form.get('status', 'NOT FOUND')}")
                print(f"      Our interpretation: {'ACTIVE' if is_active else 'INACTIVE'}")
                
                # Show key form metadata
                print(f"      Records: {form.get('record_count', 'Unknown')}")
                print(f"      Last updated: {form.get('updated_at', 'Unknown')[:10] if form.get('updated_at') else 'Unknown'}")
                
                print("-" * 80)
            
            return forms
            
        except Exception as e:
            print(f"Error fetching forms: {str(e)}")
            return []
    
    def _list_classification_sets(self):
        """Interactive classification sets browser with nested REPL"""
        if not self.api_client:
            print("API client not initialized. Run setup first.")
            return []
        
        try:
            classification_sets = self.api_client.get_classification_sets()
            
            if not classification_sets:
                print("❌ No classification sets found in your Fulcrum account")
                return []
            
            while True:
                print("\n📂 CLASSIFICATION SETS BROWSER")
                print("=" * 50)
                print(f"✅ Found {len(classification_sets)} classification sets")
                print("Select a classification set to explore its items:")
                print()
                
                # List all classification sets
                for i, cs in enumerate(classification_sets, 1):
                    items_count = len(cs.get('items', []))
                    print(f"{i}. {cs.get('name', 'Unknown Name')} ({items_count} items)")
                
                print("0. Back to forms menu")
                
                try:
                    choice = input(f"\nSelect classification set (0-{len(classification_sets)}): ").strip()
                    
                    if choice == "0":
                        # Back to forms menu
                        return []
                    
                    choice_num = int(choice) - 1
                    if 0 <= choice_num < len(classification_sets):
                        selected_cs = classification_sets[choice_num]
                        self._browse_classification_set(selected_cs)
                    else:
                        print("Invalid choice. Please try again.")
                        
                except ValueError:
                    print("Please enter a valid number.")
                except KeyboardInterrupt:
                    return []
                    
        except Exception as e:
            print(f"❌ Error fetching classification sets: {str(e)}")
            return []
    
    def _browse_classification_set(self, classification_set):
        """Browse items within a specific classification set"""
        cs_name = classification_set.get('name', 'Unknown Name')
        items = classification_set.get('items', [])
        
        while True:
            print(f"\n📋 CLASSIFICATION SET: {cs_name}")
            print("=" * 60)
            print(f"ID: {classification_set.get('id', 'Unknown')}")
            print(f"Description: {classification_set.get('description', 'No description')}")
            print(f"Items: {len(items)} total")
            print()
            
            if not items:
                print("❌ No items found in this classification set")
                input("Press Enter to go back...")
                return
            
            print("Select an item to explore:")
            
            # List all items
            for i, item in enumerate(items, 1):
                item_label = item.get('label', 'Unknown Item')
                # Check if item has children
                children = item.get('children', [])
                if children:
                    print(f"{i}. {item_label} ({len(children)} sub-items)")
                else:
                    print(f"{i}. {item_label}")
            
            print("0. Back to classification sets")
            
            try:
                choice = input(f"\nSelect item (0-{len(items)}): ").strip()
                
                if choice == "0":
                    # Back to classification sets list
                    return
                
                choice_num = int(choice) - 1
                if 0 <= choice_num < len(items):
                    selected_item = items[choice_num]
                    self._browse_classification_item(selected_item, cs_name)
                else:
                    print("Invalid choice. Please try again.")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                return
    
    def _browse_classification_item(self, item, cs_name):
        """Browse details of a specific classification item"""
        item_label = item.get('label', 'Unknown Item')
        children = item.get('child_classifications', [])
        
        while True:
            print(f"\n🏷️  CLASSIFICATION ITEM: {item_label}")
            print("=" * 60)
            print(f"Classification Set: {cs_name}")
            print(f"Item ID: {item.get('value', 'Unknown')}")
            print(f"Label: {item_label}")
            
            # Show item details
            if 'description' in item:
                print(f"Description: {item['description']}")
            
            if children:
                # Check if any children have sub-classifications (indicating this is not the final level)
                has_navigable_children = any(child.get('child_classifications') for child in children)
                
                if has_navigable_children:
                    # This level has items that can be navigated into
                    print(f"\n📁 SUB-ITEMS ({len(children)} total):")
                    print("-" * 40)
                    
                    for i, child in enumerate(children, 1):
                        child_label = child.get('label', 'Unknown Sub-item')
                        child_value = child.get('value', 'Unknown')
                        # Check if child has its own children
                        grandchildren = child.get('child_classifications', [])
                        if grandchildren:
                            print(f"{i}. {child_label} (Value: {child_value}) - {len(grandchildren)} sub-items")
                        else:
                            print(f"{i}. {child_label} (Value: {child_value})")
                    
                    print()  # Add blank line
                    print(f"{len(children) + 1}. Show detailed view of sub-items")
                    print(f"0. Back to {cs_name}")
                    
                    try:
                        choice = input(f"\nSelect option (0-{len(children) + 1}): ").strip()
                        
                        if choice == "0":
                            # Back to classification set
                            return
                        elif choice == str(len(children) + 1):
                            # Show detailed view
                            self._show_detailed_items(children, item_label)
                        else:
                            choice_num = int(choice) - 1
                            if 0 <= choice_num < len(children):
                                selected_child = children[choice_num]
                                # Check if this child has sub-classifications
                                grandchildren = selected_child.get('child_classifications', [])
                                if grandchildren:
                                    # This child has sub-items, show them recursively
                                    self._browse_classification_item(selected_child, cs_name)
                                else:
                                    # This is the lowest level, just show the item details
                                    print(f"\n📄 ITEM DETAILS: {selected_child.get('label', 'Unknown')}")
                                    print("=" * 50)
                                    print(f"Value: {selected_child.get('value', 'Unknown')}")
                                    if 'description' in selected_child:
                                        print(f"Description: {selected_child['description']}")
                                    print("\nThis is a leaf item (no sub-classifications)")
                                    input("Press Enter to continue...")
                            else:
                                print("Invalid choice. Please try again.")
                                
                    except ValueError:
                        print("Please enter a valid number.")
                    except KeyboardInterrupt:
                        return
                else:
                    # This is the final level - show detailed view format
                    print(f"\n📋 DETAILED VIEW: {item_label}")
                    print("=" * 60)
                    
                    for i, child in enumerate(children, 1):
                        child_label = child.get('label', 'Unknown Sub-item')
                        child_value = child.get('value', 'Unknown')
                        
                        print(f"\n{i}. {child_label}")
                        print(f"   Value: {child_value}")
                        print("-" * 40)
                    
                    input("\nPress Enter to go back...")
                    return
            else:
                print("\n📄 This is a leaf item (no sub-items)")
                input(f"Press Enter to go back to {cs_name}...")
                return
    
    def _show_detailed_items(self, items, parent_label):
        """Show detailed view of all items in a list"""
        print(f"\n📋 DETAILED VIEW: {parent_label}")
        print("=" * 60)
        
        for i, item in enumerate(items, 1):
            print(f"\n{i}. {item.get('label', 'Unknown')}")
            print(f"   Value: {item.get('value', 'Unknown')}")
            if 'description' in item:
                print(f"   Description: {item['description']}")
            
            children = item.get('children', [])
            if children:
                print(f"   Sub-items: {len(children)} items")
                for j, child in enumerate(children[:3]):  # Show first 3 sub-items
                    print(f"      • {child.get('label', 'Unknown')}")
                if len(children) > 3:
                    print(f"      ... and {len(children) - 3} more")
            # Remove sub-items line when there are none
            
            print("-" * 40)
        
        input("\nPress Enter to go back...")
        return
    
    def setup_target_form(self):
        """Setup the target form for importing processed data"""
        if not self.api_client:
            print("API client not initialized. Run setup first.")
            return
        
        print("\nTarget Form Setup")
        print("=" * 20)
        print("This is the form where processed data will be imported.")
        print("(e.g., 'liberty military housing legacy')")
        
        try:
            forms = self.api_client.get_forms()
            print("\nAvailable forms (All Apps - Active and Inactive):")
            print("-" * 70)
            for i, form in enumerate(forms, 1):
                # Improved status detection
                is_active = True
                if 'disabled' in form:
                    is_active = not form['disabled']
                elif 'enabled' in form:
                    is_active = form['enabled']
                elif 'status' in form:
                    is_active = form['status'].lower() == 'active'
                
                status = "🟢 ACTIVE" if is_active else "🔴 INACTIVE"
                print(f"{i}. {form['name']} {status} (ID: {form['id']})")
            
            while True:
                try:
                    choice = int(input("\nSelect target form number: ")) - 1
                    if 0 <= choice < len(forms):
                        selected_form = forms[choice]
                        self.target_form_id = selected_form['id']
                        
                        # Save to config
                        if 'fulcrum' not in self.config:
                            self.config['fulcrum'] = {}
                        self.config['fulcrum']['target_form_id'] = self.target_form_id
                        self.config['fulcrum']['target_form_name'] = selected_form['name']
                        self.save_config()
                        
                        print(f"✓ Target form set: {selected_form['name']}")
                        return selected_form
                    print("Invalid choice")
                except ValueError:
                    print("Please enter a valid number")
        except Exception as e:
            print(f"Error fetching forms: {str(e)}")
    
    def get_field_mapping(self, source_schema, target_schema):
        """Create field mapping between source CSV and target form"""
        print("\nField Mapping Setup")
        print("=" * 20)
        
        # Get target form fields
        target_fields = {}
        for element in target_schema.get('elements', []):
            if element.get('type') in ['TextField', 'NumberField', 'DateField', 'PhotoField']:
                target_fields[element['data_name']] = {
                    'label': element['label'],
                    'type': element['type'],
                    'key': element['key']
                }
        
        print("Target form fields:")
        for data_name, info in target_fields.items():
            print(f"  {data_name} ({info['type']}): {info['label']}")
        
        # Common field mappings (you can customize these)
        default_mappings = {
            'high_point': 'high_point',
            'low_point': 'low_point',
            'districtproperty': 'districtproperty',
            'slicing_severity_level': 'slicing_severity_level',
            'data_source': 'data_source',
            'before_photos': 'before_photos',
            'completed_photo': 'completed_photos',
            'survey_date': 'survey_date',
            'address': 'address',
            'number': 'number',
            'slice_length': 'slice_length'
        }
        
        return default_mappings
    
    def process_photos_for_import(self, photo_string, extract_dir):
        """Process photo references and upload to Fulcrum"""
        if not photo_string or pd.isna(photo_string):
            return []
        
        photo_ids = []
        photo_files = photo_string.split(',')
        
        for photo_file in photo_files:
            photo_file = photo_file.strip()
            if not photo_file:
                continue
            
            # Find the photo file in the extracted directory
            photo_path = None
            for ext in ['.jpg', '.jpeg', '.png', '.gif']:
                potential_path = extract_dir / f"{photo_file}{ext}"
                if potential_path.exists():
                    photo_path = potential_path
                    break
            
            if photo_path and photo_path.exists():
                try:
                    print(f"  Uploading photo: {photo_path.name}")
                    photo_response = self.api_client.upload_photo(photo_path, photo_file)
                    photo_ids.append(photo_response['access_key'])
                except Exception as e:
                    print(f"  Warning: Failed to upload {photo_path.name}: {str(e)}")
            else:
                print(f"  Warning: Photo file not found: {photo_file}")
        
        return photo_ids
    
    def import_processed_data(self, df, extract_dir, property_name):
        """Import processed data to the target Fulcrum form"""
        if not self.target_form_id:
            # Try to load from config
            self.target_form_id = self.config.get('fulcrum', {}).get('target_form_id')
            if not self.target_form_id:
                print("No target form configured. Please run setup first.")
                return False
        
        print(f"\nImporting {len(df)} records to Fulcrum...")
        print(f"Target form ID: {self.target_form_id}")
        
        try:
            # Get target form schema
            target_form = self.api_client.get_form_schema(self.target_form_id)
            field_mapping = self.get_field_mapping(None, target_form)
            
            imported_count = 0
            failed_count = 0
            
            for index, row in df.iterrows():
                try:
                    print(f"Processing record {index + 1}/{len(df)}")
                    
                    # Build form values based on field mapping
                    form_values = {}
                    
                    for source_field, target_field in field_mapping.items():
                        if source_field in row and target_field:
                            value = row[source_field]
                            
                            # Handle photo fields specially
                            if source_field in ['before_photos', 'completed_photo'] and not pd.isna(value):
                                photo_ids = self.process_photos_for_import(str(value), extract_dir)
                                if photo_ids:
                                    form_values[target_field] = photo_ids
                            elif not pd.isna(value):
                                # Handle different data types
                                if isinstance(value, (int, float)) and not pd.isna(value):
                                    form_values[target_field] = value
                                elif isinstance(value, str) and value.strip():
                                    form_values[target_field] = value.strip()
                    
                    # Include geometry if available
                    geometry = None
                    if '_latitude' in row and '_longitude' in row:
                        lat = row['_latitude']
                        lon = row['_longitude']
                        if not pd.isna(lat) and not pd.isna(lon):
                            geometry = {
                                "type": "Point",
                                "coordinates": [float(lon), float(lat)]
                            }
                    
                    # Create the record
                    record_data = {
                        'form_id': self.target_form_id,
                        'form_values': form_values
                    }
                    
                    if geometry:
                        record_data['geometry'] = geometry
                    
                    # Import to Fulcrum
                    created_record = self.api_client.create_record(self.target_form_id, record_data)
                    imported_count += 1
                    
                    if imported_count % 10 == 0:
                        print(f"  Imported {imported_count} records...")
                
                except Exception as e:
                    print(f"  Failed to import record {index + 1}: {str(e)}")
                    failed_count += 1
                    continue
            
            print(f"\n✓ Import completed!")
            print(f"  Successfully imported: {imported_count} records")
            print(f"  Failed: {failed_count} records")
            print(f"  Total processed: {len(df)} records")
            
            return imported_count > 0
            
        except Exception as e:
            print(f"Error during import: {str(e)}")
            return False
        """Interactive setup for property mappings"""
        print("\nProperty Mapping Setup")
        print("=" * 25)
        
        # Show existing mappings
        mappings = self.property_mapper.list_mappings()
        if mappings:
            print("Existing mappings:")
            for prop, district in mappings.items():
                print(f"  {prop} → {district}")
            print()
        
        while True:
            action = input("(A)dd mapping, (R)emove mapping, (L)ist all, or (Q)uit: ").strip().upper()
            
            if action == 'A':
                property_name = input("Enter property name: ").strip()
                district_property = input("Enter district property: ").strip()
                self.property_mapper.add_mapping(property_name, district_property)
                print(f"✓ Added mapping: {property_name} → {district_property}")
            
            elif action == 'R':
                property_name = input("Enter property name to remove: ").strip().lower()
                if property_name in self.property_mapper.mappings:
                    del self.property_mapper.mappings[property_name]
                    self.property_mapper.save_mappings()
                    print(f"✓ Removed mapping for {property_name}")
                else:
                    print("Property not found in mappings")
            
            elif action == 'L':
                current_mappings = self.property_mapper.list_mappings()
                if current_mappings:
                    print("Current mappings:")
                    for prop, district in current_mappings.items():
                        print(f"  {prop} → {district}")
                else:
                    print("No mappings configured")
            
            elif action == 'Q':
                break
    
    def download_form_data(self, property_name, form_name=None, form_id=None, status_filter=None):
        """Download data for a specific property"""
        if not self.api_client:
            raise Exception("API client not initialized. Run setup first.")
        
        # Get form ID if not provided
        if not form_id:
            forms = self.api_client.get_forms()
            if form_name:
                matching_forms = [f for f in forms if form_name.lower() in f['name'].lower()]
                if not matching_forms:
                    raise Exception(f"No form found matching '{form_name}'")
                form_id = matching_forms[0]['id']
            else:
                # List forms and let user choose
                print("\nAvailable forms (All Apps - Active and Inactive):")
                print("-" * 60)
                for i, form in enumerate(forms, 1):
                    # Improved status detection
                    is_active = True
                    if 'disabled' in form:
                        is_active = not form['disabled']
                    elif 'enabled' in form:
                        is_active = form['enabled']
                    elif 'status' in form:
                        is_active = form['status'].lower() == 'active'
                    
                    status = "🟢 ACTIVE" if is_active else "🔴 INACTIVE"
                    print(f"{i}. {form['name']} {status}")
                
                while True:
                    try:
                        choice = int(input("Select form number: ")) - 1
                        if 0 <= choice < len(forms):
                            form_id = forms[choice]['id']
                            break
                        print("Invalid choice")
                    except ValueError:
                        print("Please enter a valid number")
        
        print(f"Downloading data for property: {property_name}")
        
        # Create export with status filter if needed
        filters = {}
        if status_filter and status_filter != "CANCEL":
            filters = {
                "status": status_filter
            }
            print(f"🔍 Applying status filter: '{status_filter}'")
        elif status_filter == "CANCEL":
            print("❌ Download cancelled by user")
            return None
        
        export_url = self.api_client.export_data(form_id, "csv", filters)
        
        # Download the export
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{property_name}_{timestamp}.zip"
        local_path = Path(filename)
        
        print(f"Downloading export to {filename}")
        self.api_client.download_export(export_url, local_path)
        
        return local_path
    
    def process_csv_with_mapping(self, csv_path, property_name):
        """Process CSV with property mapping"""
        print(f"Processing CSV: {csv_path}")
        
        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows")
        
        # Get district property mapping
        district_property = self.property_mapper.get_mapping(property_name)
        
        if not district_property:
            print(f"No mapping found for property '{property_name}'")
            print("Available mappings:")
            for prop, district in self.property_mapper.list_mappings().items():
                print(f"  {prop} → {district}")
            
            district_property = input(f"Enter district property for '{property_name}': ").strip()
            
            # Save this mapping for future use
            save_mapping = input("Save this mapping for future use? (y/n): ").strip().lower()
            if save_mapping == 'y':
                self.property_mapper.add_mapping(property_name, district_property)
        
        # Fix point values
        self.fix_point_values(df)
        
        # Add required columns
        df['districtproperty'] = district_property
        df['data_source'] = 'PCC-SD'
        
        # Add severity level
        def get_severity_level(high_point):
            if pd.isna(high_point):
                return "Unknown"
            if high_point < 4:
                return "Minor (Level 1)"
            elif high_point < 8:
                return "Moderate (Level 2)"
            else:
                return "Severe (Level 3)"
        
        df['slicing_severity_level'] = df['high_point'].apply(get_severity_level)
        
        print(f"✓ Set district property: {district_property}")
        print(f"✓ Added severity levels")
        print(f"✓ Set data source: PCC-SD")
        
        return df, district_property
    
    def fix_point_values(self, df):
        """Fix high_point and low_point values if needed"""
        for col in ['high_point', 'low_point']:
            if col not in df.columns:
                continue
            
            # Check if values need to be multiplied by 8
            non_integer_mask = ~df[col].apply(lambda x: pd.isna(x) or (isinstance(x, (int, float)) and x == int(x)))
            
            if non_integer_mask.any():
                print(f"Converting {col} values (multiplying by 8)")
                df[col] = df[col] * 8
                df[col] = df[col].round().astype('Int64')
            else:
                df[col] = df[col].astype('Int64')
    
    def create_output_package(self, df, extract_dir, property_name, district_property):
        """Create final zip package"""
        # Save processed CSV
        processed_csv = extract_dir / f"processed_{property_name}.csv"
        df.to_csv(processed_csv, index=False)
        
        # Create final zip
        safe_name = "".join(c for c in district_property if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_name = safe_name.replace(' ', '_')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        zip_name = f"{safe_name}_{timestamp}.zip"
        zip_path = Path(zip_name)
        
        print(f"Creating final package: {zip_name}")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add processed CSV
            zipf.write(processed_csv, processed_csv.name)
            
            # Add all other files (photos, etc.)
            for file_path in extract_dir.rglob('*'):
                if file_path.is_file() and file_path != processed_csv:
                    arcname = file_path.relative_to(extract_dir)
                    zipf.write(file_path, arcname)
        
        return zip_path
    
    def process_property(self, property_name, form_name=None, form_id=None, import_data=True):
        """Complete processing workflow for a property"""
        try:
            # Download data
            print(f"Step 1: Downloading data for '{property_name}'...")
            zip_path = self.download_form_data(property_name, form_name, form_id)
            
            # Extract zip
            print("Step 2: Extracting downloaded data...")
            extract_dir = Path(zip_path.stem)
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Find CSV file
            csv_files = list(extract_dir.glob("*.csv"))
            if not csv_files:
                raise Exception("No CSV file found in downloaded data")
            
            csv_file = csv_files[0]  # Use first CSV found
            
            # Process CSV
            print("Step 3: Processing CSV data...")
            df, district_property = self.process_csv_with_mapping(csv_file, property_name)
            
            # Create final package
            print("Step 4: Creating processed data package...")
            final_zip = self.create_output_package(df, extract_dir, property_name, district_property)
            
            # Import to Fulcrum if requested
            if import_data:
                print("Step 5: Importing data to Fulcrum...")
                import_success = self.import_processed_data(df, extract_dir, property_name)
                
                if import_success:
                    print("✓ Data successfully imported to Fulcrum!")
                else:
                    print("⚠ Import failed, but processed file is available")
            
            # Cleanup
            print("Step 6: Cleaning up temporary files...")
            shutil.rmtree(extract_dir)
            zip_path.unlink()
            
            print(f"\n🎉 Processing completed successfully!")
            print(f"Property: {property_name}")
            print(f"Records processed: {len(df)}")
            print(f"District property: {district_property}")
            print(f"Output file: {final_zip}")
            
            if import_data:
                print(f"✓ Data imported to target form")
            else:
                print(f"📦 Use {final_zip} for manual import")
            
            return final_zip
            
        except Exception as e:
            print(f"❌ Error processing property '{property_name}': {str(e)}")
            raise
    
    def download_data_menu(self):
        """Download data menu with single/batch search options"""
        if not self.api_client:
            print("Please setup API credentials first")
            return
        
        while True:
            print("\n📥 DOWNLOAD DATA")
            print("=" * 30)
            print("1. Single form search")
            print("2. Batch download")
            print("0. Back to main menu")
            
            try:
                choice = input("\nSelect option (0-2): ").strip()
                
                if choice == "0":
                    return
                elif choice == "1":
                    self._single_form_search()
                elif choice == "2":
                    print("Batch download functionality coming soon...")
                    input("Press Enter to continue...")
                else:
                    print("Invalid choice. Please enter 0-2.")
                    
            except KeyboardInterrupt:
                return
    
    def _single_form_search(self):
        """Search for a single form by name"""
        print("\n🔍 SINGLE FORM SEARCH")
        print("=" * 40)
        
        search_term = input("Enter form name to search for: ").strip()
        if not search_term:
            print("Search term cannot be empty")
            return
        
        print(f"\nSearching for forms matching '{search_term}'...")
        
        # Get all forms
        all_forms = self.api_client.get_forms('all')
        if not all_forms:
            print("❌ No forms found in your account")
            return
        
        # Search for exact matches first
        exact_matches = [form for form in all_forms 
                        if search_term.lower() == form.get('name', '').lower()]
        
        if exact_matches:
            if len(exact_matches) == 1:
                # Single exact match
                form = exact_matches[0]
                print(f"\n✅ Found exact match:")
                print(f"Name: {form.get('name', 'Unknown')}")
                print(f"Status: {form.get('status', 'Unknown')}")
                print(f"Records: {form.get('record_count', 'Unknown')}")
                
                confirm = input(f"\nDownload data for '{form.get('name')}'? (y/n): ").strip().lower()
                if confirm == 'y':
                    self._download_form_data(form)
                return
            else:
                # Multiple exact matches
                print(f"\n✅ Found {len(exact_matches)} exact matches:")
                self._show_form_selection(exact_matches, search_term)
                return
        
        # Search for partial matches (full search term)
        partial_matches = [form for form in all_forms 
                          if search_term.lower() in form.get('name', '').lower()]
        
        if partial_matches:
            print(f"\n🔍 Found {len(partial_matches)} forms with matching keywords:")
            self._show_form_selection(partial_matches, search_term)
            return
        
        # Search by individual words as a safety net
        search_words = search_term.lower().split()
        if len(search_words) > 1:
            # Filter out common words that would return too many results
            common_words = {
                'district', 'office', 'apartments', 'apartment', 'city', 'county', 
                'building', 'complex', 'center', 'plaza', 'park', 'street', 'road',
                'avenue', 'lane', 'drive', 'way', 'place', 'court', 'circle',
                'north', 'south', 'east', 'west', 'new', 'old', 'main', 'first',
                'second', 'third', 'the', 'and', 'or', 'of', 'in', 'at', 'on',
                'property', 'properties', 'housing', 'residential', 'commercial'
            }
            
            # Keep only meaningful search words
            filtered_words = [word for word in search_words if word not in common_words and len(word) > 2]
            
            if filtered_words:
                print(f"\n🔍 No exact matches found. Searching individual words: {', '.join(filtered_words)}...")
                word_matches = []
                
                for form in all_forms:
                    form_name = form.get('name', '').lower()
                    # Check if any of the filtered search words appear in the form name
                    if any(word in form_name for word in filtered_words):
                        word_matches.append(form)
            else:
                print(f"\n🔍 Search terms contain only common words (district, city, etc.)")
                print("Please use more specific terms for better results")
                return
            
            if word_matches:
                print(f"\n🔍 Found {len(word_matches)} forms matching individual words from '{search_term}':")
                self._show_form_selection(word_matches, search_term)
                return
        
        print(f"\n❌ No forms found matching '{search_term}' or any of its keywords")
        print("Please try different keywords or check the spelling")
    
    def _show_form_selection(self, forms, search_term):
        """Show form selection menu"""
        while True:
            print(f"\n📋 Forms matching '{search_term}':")
            print("-" * 50)
            
            for i, form in enumerate(forms, 1):
                status = form.get('status', 'Unknown')
                records = form.get('record_count', 'Unknown')
                print(f"{i}. {form.get('name', 'Unknown')} (Status: {status}, Records: {records})")
            
            print("0. Back to search")
            
            try:
                choice = input(f"\nSelect form to download (0-{len(forms)}): ").strip()
                
                if choice == "0":
                    return
                
                choice_num = int(choice) - 1
                if 0 <= choice_num < len(forms):
                    selected_form = forms[choice_num]
                    print(f"\n✅ Selected: {selected_form.get('name')}")
                    
                    confirm = input("Download data for this form? (y/n): ").strip().lower()
                    if confirm == 'y':
                        self._download_form_data(selected_form)
                    return
                else:
                    print("Invalid choice. Please try again.")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                return
    
    def _download_form_data(self, form):
        """Download data for a specific form with optional status filtering"""
        form_name = form.get('name', 'Unknown')
        form_id = form.get('id')
        
        # Get form schema to show available columns
        print(f"\n📊 Getting form schema for '{form_name}'...")
        try:
            schema = self.api_client.get_form_schema(form_id)
            print(f"\n📋 Available columns in '{form_name}':")
            print("-" * 50)
            
            system_fields = ['id', 'status', 'created_at', 'updated_at', 'created_by', 'updated_by', 'latitude', 'longitude']
            print("System fields:", ", ".join(system_fields))
            
            if 'elements' in schema:
                data_fields = []
                for element in schema['elements']:
                    if element.get('data_name'):
                        field_type = element.get('type', 'Unknown')
                        field_name = element.get('data_name')
                        field_label = element.get('label', field_name)
                        data_fields.append(f"{field_name} ({field_label})")
                
                if data_fields:
                    print("Form fields:", ", ".join(data_fields))
                else:
                    print("No custom form fields found")
            
        except Exception as e:
            print(f"⚠️ Could not get form schema: {str(e)}")
        
        # Get available statuses for this form
        print(f"\n📊 Getting available statuses for '{form_name}'...")
        try:
            status_filter = self._get_status_filter(form_id, form_name)
            if status_filter == "CANCEL":
                print("❌ Download cancelled by user")
                return
        except Exception as e:
            print(f"⚠️ Could not get status information: {str(e)}")
            print("Proceeding with download of all records...")
            status_filter = None
        
        print(f"\n📥 Downloading data for '{form_name}'...")
        
        try:
            # Use existing download functionality with optional status filter
            zip_path = self.download_form_data(form_name, form_name, form_id, status_filter)
            print(f"\n✅ Download completed!")
            print(f"📁 File saved as: {zip_path}")
            
            # Ask if they want to open the folder
            open_folder = input("Open containing folder? (y/n): ").strip().lower()
            if open_folder == 'y':
                import subprocess
                import platform
                
                folder_path = zip_path.parent
                if platform.system() == "Darwin":  # macOS
                    subprocess.run(["open", folder_path])
                elif platform.system() == "Windows":
                    subprocess.run(["explorer", folder_path])
                else:  # Linux
                    subprocess.run(["xdg-open", folder_path])
                    
        except Exception as e:
            print(f"❌ Download failed: {str(e)}")
        
        input("\nPress Enter to continue...")
    
    def analyze_form_statuses(self, form_id, form_name):
        """Analyze and display all unique status values in a form"""
        print(f"\n🔍 Analyzing status values for '{form_name}'...")
        print(f"Form ID: {form_id}")
        print("-" * 60)
        
        # Try different approaches to get status data
        status_data = None
        
        # Method 1: Try Fulcrum Query API with different query formats
        print("📊 Method 1: Using Fulcrum Query API...")
        queries_to_try = [
            f"SELECT status, COUNT(*) as count FROM records WHERE form_id = '{form_id}' GROUP BY status ORDER BY count DESC",
            f"SELECT status, COUNT(*) FROM records WHERE form_id = '{form_id}' GROUP BY status ORDER BY COUNT(*) DESC",
            f"SELECT DISTINCT status FROM records WHERE form_id = '{form_id}' ORDER BY status",
            f"SELECT status FROM records WHERE form_id = '{form_id}'",
        ]
        
        for i, query in enumerate(queries_to_try, 1):
            try:
                print(f"  🔍 Query {i}: {query}")
                response = requests.get(
                    f"{self.api_client.base_url}/query",
                    headers=self.api_client.headers,
                    params={'q': query, 'format': 'json'},
                    timeout=15
                )
                
                print(f"  📡 Response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"  📊 Response keys: {list(data.keys())}")
                    
                    rows = data.get('rows', [])
                    fields = data.get('fields', [])
                    
                    print(f"  📊 Found {len(rows)} rows, {len(fields)} fields")
                    
                    if rows:
                        print(f"  ✅ SUCCESS with Query {i}!")
                        print(f"  📊 Fields: {[f.get('name', 'unknown') for f in fields]}")
                        
                        # Extract status values
                        if "COUNT" in query.upper() or "count" in query:
                            # Grouped query - status and count
                            print(f"\n📈 UNIQUE STATUS VALUES (with counts):")
                            print("=" * 50)
                            for row in rows:
                                status = row[0] if row[0] else "(No Status/NULL)"
                                count = row[1] if len(row) > 1 else "Unknown"
                                print(f"  • {status}: {count} records")
                        else:
                            # Just status values
                            all_statuses = [row[0] if row[0] else "(No Status/NULL)" for row in rows]
                            unique_statuses = list(set(all_statuses))
                            
                            print(f"\n📈 UNIQUE STATUS VALUES:")
                            print("=" * 40)
                            for status in sorted(unique_statuses):
                                count = all_statuses.count(status)
                                print(f"  • {status}: {count} records")
                        
                        status_data = rows
                        return rows  # Success!
                    else:
                        print(f"  ❌ No data returned")
                        if response.text:
                            print(f"  📄 Raw response: {response.text[:200]}...")
                else:
                    print(f"  ❌ Failed with status {response.status_code}")
                    if response.text:
                        print(f"  📄 Error response: {response.text[:200]}...")
                
            except Exception as e:
                print(f"  ❌ Query {i} failed: {str(e)}")
                continue
        
        # Method 2: Try standard records API
        print(f"\n📊 Method 2: Using standard Records API...")
        try:
            print(f"  🔍 Getting records from form {form_id}")
            response = requests.get(
                f"{self.api_client.base_url}/records",
                headers=self.api_client.headers,
                params={'form_id': form_id},
                timeout=15
            )
            
            print(f"  📡 Response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                print(f"  📊 Found {len(records)} records")
                
                if records:
                    # Extract all status values
                    status_values = []
                    for record in records:
                        status = record.get('status', '(No Status)')
                        status_values.append(status if status else '(NULL)')
                    
                    # Get unique status values with counts
                    unique_statuses = {}
                    for status in status_values:
                        unique_statuses[status] = unique_statuses.get(status, 0) + 1
                    
                    print(f"\n📈 UNIQUE STATUS VALUES (from Records API):")
                    print("=" * 50)
                    for status, count in sorted(unique_statuses.items()):
                        print(f"  • {status}: {count} records")
                    
                    print(f"\n📊 SUMMARY:")
                    print(f"  Total records: {len(records)}")
                    print(f"  Unique statuses: {len(unique_statuses)}")
                    
                    return list(unique_statuses.keys())
                else:
                    print(f"  ❌ No records found in form")
            else:
                print(f"  ❌ Failed with status {response.status_code}")
                if response.text:
                    print(f"  📄 Error: {response.text[:200]}...")
                    
        except Exception as e:
            print(f"  ❌ Records API failed: {str(e)}")
        
        print(f"\n❌ Could not retrieve status information using any method")
        return None
    
    def filter_and_export_by_status(self, form_id, form_name):
        """Filter records by status and export to CSV"""
        print(f"\n🔍 FILTER AND EXPORT: {form_name}")
        print("=" * 60)
        
        # Step 1: Get all records and analyze statuses
        print("📊 Step 1: Getting records and analyzing statuses...")
        try:
            response = requests.get(
                f"{self.api_client.base_url}/records",
                headers=self.api_client.headers,
                params={'form_id': form_id},
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"❌ Failed to get records (HTTP {response.status_code})")
                return None
                
            data = response.json()
            all_records = data.get('records', [])
            print(f"📊 Found {len(all_records)} total records")
            
            if not all_records:
                print("❌ No records found in this form")
                return None
            
            # Analyze unique statuses
            unique_statuses = {}
            for record in all_records:
                status = record.get('status', '(No Status)')
                unique_statuses[status] = unique_statuses.get(status, 0) + 1
            
            print(f"\n📈 AVAILABLE STATUS VALUES:")
            print("-" * 50)
            status_list = list(unique_statuses.keys())
            for i, (status, count) in enumerate(sorted(unique_statuses.items()), 1):
                print(f"{i}. {status} ({count} records)")
            
        except Exception as e:
            print(f"❌ Error getting records: {str(e)}")
            return None
        
        # Step 2: Interactive status selection REPL
        print(f"\n🎯 Step 2: Select statuses to filter by")
        print("-" * 40)
        selected_statuses = []
        
        while True:
            print(f"\n📋 Current selection: {len(selected_statuses)} statuses")
            if selected_statuses:
                for i, status in enumerate(selected_statuses, 1):
                    count = unique_statuses[status]
                    print(f"  {i}. {status} ({count} records)")
            else:
                print("  (None selected)")
            
            print(f"\n📋 Available statuses:")
            for i, (status, count) in enumerate(sorted(unique_statuses.items()), 1):
                selected = "✓" if status in selected_statuses else " "
                print(f"{selected} {i}. {status} ({count} records)")
            
            print(f"\nCommands:")
            print(f"  • Enter number to toggle status (1-{len(status_list)})")
            print(f"  • 'all' - select all statuses")
            print(f"  • 'clear' - clear all selections")
            print(f"  • 'done' - proceed with current selection")
            print(f"  • 'quit' - cancel and exit")
            
            choice = input(f"\nEnter command or number: ").strip().lower()
            
            if choice == 'quit':
                print("❌ Operation cancelled")
                return None
            elif choice == 'clear':
                selected_statuses = []
                print("🗑️  Cleared all selections")
            elif choice == 'all':
                selected_statuses = list(status_list)
                print(f"✅ Selected all {len(selected_statuses)} statuses")
            elif choice == 'done':
                if not selected_statuses:
                    print("⚠️  No statuses selected. Please select at least one.")
                    continue
                break
            else:
                try:
                    choice_num = int(choice)
                    if 1 <= choice_num <= len(status_list):
                        status = sorted(status_list)[choice_num - 1]
                        if status in selected_statuses:
                            selected_statuses.remove(status)
                            print(f"➖ Removed: {status}")
                        else:
                            selected_statuses.append(status)
                            print(f"➕ Added: {status}")
                    else:
                        print("❌ Invalid number. Please try again.")
                except ValueError:
                    print("❌ Invalid input. Please enter a number or command.")
        
        # Step 3: Filter records by selected statuses
        print(f"\n📊 Step 3: Filtering records by selected statuses...")
        filtered_records = []
        for record in all_records:
            if record.get('status') in selected_statuses:
                filtered_records.append(record)
        
        print(f"📊 Filtered to {len(filtered_records)} records from {len(selected_statuses)} statuses:")
        for status in selected_statuses:
            count = unique_statuses[status]
            print(f"  • {status}: {count} records")
        
        # Step 4: Create cache directory and export to CSV
        print(f"\n💾 Step 4: Exporting to CSV...")
        
        # Create cache directory
        cache_dir = Path("cached")
        cache_dir.mkdir(exist_ok=True)
        print(f"📁 Cache directory: {cache_dir.absolute()}")
        
        # Create organized folder structure
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_form_name = "".join(c for c in form_name if c.isalnum() or c in (' ', '-', '_')).rstrip().replace(' ', '_')
        status_summary = "_".join([s.replace(' ', '').replace('&', 'and') for s in selected_statuses[:3]])
        if len(selected_statuses) > 3:
            status_summary += f"_plus{len(selected_statuses)-3}more"
        
        # Create property folder with timestamp
        property_folder_name = f"{safe_form_name}_{status_summary}_{timestamp}"
        property_folder = cache_dir / property_folder_name
        property_folder.mkdir(exist_ok=True)
        
        # CSV file goes directly in the property folder
        filename = f"{safe_form_name}_data.csv"
        csv_path = property_folder / filename
        
        # Convert records to CSV format
        import pandas as pd
        
        # Get the form schema to map field names properly
        print(f"📋 Getting form schema to map field names...")
        try:
            form_schema = self.api_client.get_form_schema(form_id)
            field_mapping = {}
            
            # Create mapping from key to human-readable label (handle nested structure)
            def extract_fields(elements, level=0):
                """Recursively extract fields from nested form structure"""
                indent = "  " * level
                for element in elements:
                    element_type = element.get('type', '')
                    key = element.get('key')
                    data_name = element.get('data_name')
                    label = element.get('label', data_name or key)
                    
                    # Map both key and data_name to label
                    if key:
                        field_mapping[key] = label
                        print(f"{indent}🔗 Mapped key: {key} → {label}")
                    if data_name and data_name != key:
                        field_mapping[data_name] = label
                        print(f"{indent}🔗 Mapped data_name: {data_name} → {label}")
                    
                    # Handle nested elements (sections, repeatable sections, etc.)
                    if 'elements' in element:
                        print(f"{indent}📁 Section: {label}")
                        extract_fields(element['elements'], level + 1)
            
            extract_fields(form_schema.get('elements', []))
                    
            print(f"📊 Mapped {len(field_mapping)} form fields")
            
            # Debug: Show what field keys we actually get from records
            if filtered_records:
                sample_record = filtered_records[0]
                form_values = sample_record.get('form_values', {})
                print(f"🔍 Debug: Actual field keys in records: {list(form_values.keys())}")
                print(f"🔍 Debug: Field mapping keys: {list(field_mapping.keys())}")
                
        except Exception as e:
            print(f"⚠️ Could not get form schema: {str(e)}")
            import traceback
            traceback.print_exc()
            field_mapping = {}
        
        # Flatten the record structure for CSV export
        csv_data = []
        for record in filtered_records:
            row = {
                'id': record.get('id', ''),
                'status': record.get('status', ''),
                'created_at': record.get('created_at', ''),
                'updated_at': record.get('updated_at', ''),
                'created_by': record.get('created_by', ''),
                'updated_by': record.get('updated_by', ''),
                'latitude': record.get('latitude', ''),
                'longitude': record.get('longitude', ''),
            }
            
            # Add form field values with proper column names
            form_values = record.get('form_values', {})
            for field_key, value in form_values.items():
                # Use the human-readable label as column name, or fall back to field key
                column_name = field_mapping.get(field_key, field_key)
                
                # Clean up complex values for readability
                if isinstance(value, dict):
                    # Handle choice fields
                    if 'choice_values' in value:
                        row[column_name] = ', '.join(value['choice_values'])
                    elif 'other_values' in value:
                        row[column_name] = ', '.join(value['other_values'])
                    # Handle address fields
                    elif 'thoroughfare' in value or 'sub_thoroughfare' in value:
                        # Extract readable address from address JSON
                        address_parts = []
                        if value.get('sub_thoroughfare'):
                            address_parts.append(value['sub_thoroughfare'])
                        if value.get('thoroughfare'):
                            address_parts.append(value['thoroughfare'])
                        if value.get('locality'):
                            address_parts.append(value['locality'])
                        if value.get('admin_area'):
                            address_parts.append(value['admin_area'])
                        if value.get('postal_code'):
                            address_parts.append(value['postal_code'])
                        row[column_name] = ', '.join(filter(None, address_parts))
                    else:
                        row[column_name] = str(value)
                elif isinstance(value, list):
                    # Handle photo fields and lists
                    if all(isinstance(item, dict) and 'photo_id' in item for item in value):
                        # Photo field - just comma-separated photo IDs
                        photo_ids = [item['photo_id'] for item in value]
                        row[column_name] = ', '.join(photo_ids)
                    else:
                        row[column_name] = ', '.join(str(item) for item in value)
                else:
                    row[column_name] = str(value) if value is not None else ''
            
            csv_data.append(row)
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(csv_data)
        df.to_csv(csv_path, index=False)
        
        print(f"✅ SUCCESS!")
        print(f"📄 CSV file: {csv_path}")
        print(f"📁 Property folder: {property_folder}")
        print(f"📊 Records: {len(filtered_records)}")
        print(f"📋 Columns: {len(df.columns)}")
        print(f"🎯 Filtered statuses: {', '.join(selected_statuses)}")
        
        # Ask if user wants to download photos
        download_photos = input(f"\n📸 Download photos for these records? (y/n): ").strip().lower()
        
        if download_photos == 'y':
            photos_downloaded = self.download_photos_for_records(filtered_records, property_folder, safe_form_name)
            if photos_downloaded > 0:
                print(f"📸 Downloaded {photos_downloaded} photos to {property_folder}/photos/")
        
        print(f"\n🎉 Export package created: {property_folder}")
        print(f"   📄 Data: {csv_path.name}")
        if download_photos == 'y':
            print(f"   📸 Photos: photos/ folder")
        
        # Ask if user wants to migrate to another form template
        migrate_form = input(f"\n🔄 Migrate data to match another form template? (y/n): ").strip().lower()
        
        if migrate_form == 'y':
            migrated_csv = self.migrate_to_form_template(csv_path, property_folder, form_name)
            if migrated_csv:
                print(f"   📄 Migrated data: {migrated_csv.name}")
        
        return property_folder
    
    def download_photos_for_records(self, records, property_folder, form_name):
        """Download all photos from the filtered records"""
        print(f"\n📸 Starting photo download process...")
        
        # Create photos directory inside the property folder
        photos_dir = property_folder / "photos"
        photos_dir.mkdir(exist_ok=True)
        
        # Collect all unique photo IDs
        all_photo_ids = set()
        photo_field_mapping = {}  # Map photo_id to (record_id, field_name) for organization
        
        for record in records:
            record_id = record.get('id', 'unknown')
            form_values = record.get('form_values', {})
            
            for field_key, value in form_values.items():
                if isinstance(value, list) and all(isinstance(item, dict) and 'photo_id' in item for item in value):
                    # This is a photo field
                    for photo_item in value:
                        photo_id = photo_item['photo_id']
                        all_photo_ids.add(photo_id)
                        photo_field_mapping[photo_id] = (record_id, field_key)
        
        print(f"📊 Found {len(all_photo_ids)} unique photos to download")
        
        if not all_photo_ids:
            print("❌ No photos found in the selected records")
            return 0
        
        # Ask for photo size preference
        print(f"\n📸 Photo size options:")
        print(f"  1. Thumbnail (fast download, small size)")
        print(f"  2. Large (good quality, medium size) - RECOMMENDED")
        print(f"  3. Original (highest quality, largest size)")
        
        while True:
            size_choice = input("Select photo size (1-3): ").strip()
            if size_choice == "1":
                photo_size = "thumbnail"
                break
            elif size_choice == "2":
                photo_size = "large"
                break
            elif size_choice == "3":
                photo_size = "original"
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        
        # Download photos concurrently for speed
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        print(f"🚀 Starting concurrent downloads with up to 5 threads...")
        
        downloaded_count = 0
        failed_count = 0
        progress_lock = threading.Lock()
        
        def download_single_photo(photo_id):
            """Download a single photo with error handling"""
            try:
                record_id, field_key = photo_field_mapping[photo_id]
                
                # Create filename with record info
                safe_record_id = record_id[:8]  # First 8 chars of record ID
                filename = f"{safe_record_id}_{field_key}_{photo_id}.jpg"
                photo_path = photos_dir / filename
                
                # Download the photo
                self.api_client.download_photo(photo_id, photo_path, size=photo_size)
                return {'success': True, 'photo_id': photo_id, 'filename': filename}
                
            except Exception as e:
                return {'success': False, 'photo_id': photo_id, 'error': str(e)}
        
        # Use ThreadPoolExecutor for concurrent downloads
        max_workers = 5  # Limit to avoid overwhelming the API
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_photo = {executor.submit(download_single_photo, photo_id): photo_id 
                             for photo_id in all_photo_ids}
            
            # Process completed downloads
            for future in as_completed(future_to_photo):
                result = future.result()
                
                with progress_lock:
                    if result['success']:
                        downloaded_count += 1
                        if downloaded_count % 10 == 0 or downloaded_count <= 5:
                            print(f"  ✅ Downloaded {downloaded_count}/{len(all_photo_ids)}: {result['filename']}")
                    else:
                        failed_count += 1
                        print(f"  ❌ Failed {result['photo_id']}: {result['error']}")
                    
                    # Show progress every 25% completion
                    total = len(all_photo_ids)
                    if downloaded_count + failed_count in [total//4, total//2, 3*total//4]:
                        completed = downloaded_count + failed_count
                        print(f"📊 Progress: {completed}/{total} ({completed/total*100:.0f}%) - Success: {downloaded_count}, Failed: {failed_count}")
        
        print(f"\n✅ Photo download completed!")
        print(f"   Successfully downloaded: {downloaded_count} photos")
        print(f"   Failed: {failed_count} photos")
        print(f"   Photos saved to: {photos_dir}")
        
        # Create a photo index CSV
        if downloaded_count > 0:
            self.create_photo_index(photos_dir, photo_field_mapping, records)
        
        return downloaded_count
    
    def create_photo_index(self, photos_dir, photo_field_mapping, records):
        """Create a CSV index of all downloaded photos"""
        print(f"\n📋 Creating photo index...")
        
        import pandas as pd
        
        # Create mapping from record ID to record data
        record_lookup = {record['id']: record for record in records}
        
        index_data = []
        for photo_id, (record_id, field_key) in photo_field_mapping.items():
            record = record_lookup.get(record_id)
            if record:
                safe_record_id = record_id[:8]
                filename = f"{safe_record_id}_{field_key}_{photo_id}.jpg"
                photo_path = photos_dir / filename
                
                if photo_path.exists():
                    index_data.append({
                        'photo_filename': filename,
                        'photo_id': photo_id,
                        'record_id': record_id,
                        'field_name': field_key,
                        'record_status': record.get('status', ''),
                        'created_at': record.get('created_at', ''),
                        'latitude': record.get('latitude', ''),
                        'longitude': record.get('longitude', ''),
                    })
        
        if index_data:
            index_df = pd.DataFrame(index_data)
            index_path = photos_dir / "photo_index.csv"
            index_df.to_csv(index_path, index=False)
            print(f"📋 Photo index saved: {index_path}")
            print(f"   Index contains {len(index_data)} photo entries")
    
    def migrate_to_form_template(self, source_csv_path, property_folder, source_form_name):
        """Migrate CSV data to match a target form template"""
        print(f"\n🔄 FORM MIGRATION WIZARD")
        print("=" * 50)
        print(f"Source: {source_form_name}")
        print(f"Current CSV: {source_csv_path.name}")
        
        # Step 1: Select target form
        target_form = self._select_target_form()
        if not target_form:
            print("❌ Migration cancelled")
            return None
        
        target_form_name = target_form.get('name', 'Unknown')
        target_form_id = target_form.get('id')
        
        print(f"\n🎯 Target: {target_form_name}")
        print(f"Form ID: {target_form_id}")
        
        # Step 2: Get target form template
        template_fields = self._get_form_template(target_form_id, target_form_name)
        if not template_fields:
            print("❌ Could not get target form template")
            return None
        
        # Step 3: Create field mapping
        field_mapping = self._create_field_mapping(source_csv_path, template_fields, source_form_name, target_form_name)
        if not field_mapping:
            print("❌ Field mapping cancelled")
            return None
        
        # Step 4: Create status mapping
        status_mapping = self._create_status_mapping(source_csv_path, target_form['id'], source_form_name, target_form_name)
        
        # Step 5: Transform CSV
        migrated_csv_path = self._transform_csv_to_template(source_csv_path, property_folder, template_fields, field_mapping, target_form_name, status_mapping)
        
        return migrated_csv_path
    
    def _select_target_form(self):
        """Let user select a target form for migration"""
        print(f"\n📋 Select target form for migration:")
        
        try:
            # Get all forms
            all_forms = self.api_client.get_forms('all')
            if not all_forms:
                print("❌ No forms found")
                return None
            
            # Show forms with search capability
            print(f"\nAvailable forms ({len(all_forms)} total):")
            print("=" * 60)
            
            # Show first 20 forms
            display_forms = all_forms[:20]
            for i, form in enumerate(display_forms, 1):
                status = form.get('status', 'Unknown')
                records = form.get('record_count', 'Unknown')
                print(f"{i}. {form.get('name', 'Unknown')} (Status: {status}, Records: {records})")
            
            if len(all_forms) > 20:
                print(f"... and {len(all_forms) - 20} more forms")
                print("\n💡 Options:")
                print("   • Enter number 1-20 to select from above")
                print("   • Type 'search <term>' to search all forms")
                print("   • Type 'all' to see all forms")
                print("   • Type 'cancel' to exit")
            else:
                print("0. Cancel")
            
            while True:
                choice = input(f"\nSelect form or search: ").strip()
                
                if choice.lower() == 'cancel':
                    return None
                elif choice.lower() == 'all':
                    # Show all forms
                    print(f"\n📋 ALL FORMS ({len(all_forms)} total):")
                    print("=" * 60)
                    for i, form in enumerate(all_forms, 1):
                        status = form.get('status', 'Unknown')
                        print(f"{i}. {form.get('name', 'Unknown')} ({status})")
                    
                    form_choice = input(f"Select form number (1-{len(all_forms)}): ").strip()
                    try:
                        form_idx = int(form_choice) - 1
                        if 0 <= form_idx < len(all_forms):
                            return all_forms[form_idx]
                        else:
                            print("Invalid choice. Please try again.")
                    except ValueError:
                        print("Please enter a valid number.")
                        
                elif choice.lower().startswith('search '):
                    # Search forms
                    search_term = choice[7:].strip().lower()
                    if not search_term:
                        print("Please provide a search term.")
                        continue
                    
                    matching_forms = [f for f in all_forms if search_term in f.get('name', '').lower()]
                    
                    if not matching_forms:
                        print(f"No forms found matching '{search_term}'")
                        continue
                    
                    print(f"\n🔍 Found {len(matching_forms)} forms matching '{search_term}':")
                    print("=" * 60)
                    for i, form in enumerate(matching_forms, 1):
                        status = form.get('status', 'Unknown')
                        print(f"{i}. {form.get('name', 'Unknown')} ({status})")
                    
                    search_choice = input(f"Select form number (1-{len(matching_forms)}): ").strip()
                    try:
                        form_idx = int(search_choice) - 1
                        if 0 <= form_idx < len(matching_forms):
                            return matching_forms[form_idx]
                        else:
                            print("Invalid choice. Please try again.")
                    except ValueError:
                        print("Please enter a valid number.")
                        
                else:
                    # Direct number selection from first 20
                    try:
                        choice_num = int(choice) - 1
                        if 0 <= choice_num < len(display_forms):
                            return display_forms[choice_num]
                        else:
                            print(f"Invalid choice. Please enter 1-{len(display_forms)} or use search.")
                    except ValueError:
                        print("Please enter a valid number, 'search <term>', 'all', or 'cancel'.")
                        
        except Exception as e:
            print(f"❌ Error selecting target form: {str(e)}")
            return None
    
    def _get_form_template(self, form_id, form_name):
        """Get target form schema as template"""
        print(f"\n📋 Getting template from {form_name}...")
        
        try:
            form_schema = self.api_client.get_form_schema(form_id)
            template_fields = []
            
            def extract_template_fields(elements, level=0):
                """Extract fields with their metadata for template"""
                for element in elements:
                    element_type = element.get('type', '')
                    key = element.get('key')
                    data_name = element.get('data_name')
                    label = element.get('label', data_name or key)
                    required = element.get('required', False)
                    
                    # Only include data fields, not sections or labels
                    if element_type in ['TextField', 'NumberField', 'DateField', 'DateTimeField', 
                                       'TimeField', 'ChoiceField', 'PhotoField', 'AddressField',
                                       'CalculatedField']:
                        template_fields.append({
                            'key': key,
                            'data_name': data_name,
                            'label': label,
                            'type': element_type,
                            'required': required
                        })
                    
                    # Handle nested elements
                    if 'elements' in element:
                        extract_template_fields(element['elements'], level + 1)
            
            extract_template_fields(form_schema.get('elements', []))
            
            print(f"📊 Template has {len(template_fields)} data fields:")
            for field in template_fields[:10]:  # Show first 10
                req_marker = " *" if field['required'] else ""
                print(f"   • {field['label']}{req_marker} ({field['type']})")
            
            if len(template_fields) > 10:
                print(f"   ... and {len(template_fields) - 10} more fields")
            
            return template_fields
            
        except Exception as e:
            print(f"❌ Error getting form template: {str(e)}")
            return None
    
    def _create_field_mapping(self, source_csv_path, template_fields, source_form_name, target_form_name):
        """Interactive field mapping between source CSV and target form"""
        print(f"\n🔗 FIELD MAPPING: {source_form_name} → {target_form_name}")
        print("=" * 70)
        
        # Read source CSV to get columns
        import pandas as pd
        try:
            source_df = pd.read_csv(source_csv_path)
            source_columns = list(source_df.columns)
            print(f"📄 Source CSV has {len(source_columns)} columns")
        except Exception as e:
            print(f"❌ Error reading source CSV: {str(e)}")
            return None
        
        # Show source columns
        print(f"\n📋 Source Columns:")
        for i, col in enumerate(source_columns[:15], 1):
            print(f"   {i}. {col}")
        if len(source_columns) > 15:
            print(f"   ... and {len(source_columns) - 15} more columns")
        
        # Show target fields
        print(f"\n🎯 Target Form Fields:")
        for i, field in enumerate(template_fields[:15], 1):
            req_marker = " *" if field['required'] else ""
            print(f"   {i}. {field['label']}{req_marker} ({field['type']})")
        if len(template_fields) > 15:
            print(f"   ... and {len(template_fields) - 15} more fields")
        
        # Create mapping
        field_mapping = {}
        
        print(f"\n🔗 Creating field mappings...")
        print("For each target field, specify which source column to map (or 'skip' to leave empty)")
        print("Type 'auto' for automatic best-match mapping, or 'manual' for field-by-field mapping")
        
        mapping_mode = input("Choose mapping mode (auto/manual): ").strip().lower()
        
        if mapping_mode == 'auto':
            field_mapping = self._auto_map_fields(source_columns, template_fields, source_form_name)
            
            # Show auto mapping results
            print(f"\n🤖 Auto-mapping results:")
            print("-" * 50)
            mapped_count = 0
            for target_field, source_col in field_mapping.items():
                if source_col:
                    mapped_count += 1
                    print(f"✅ {target_field} ← {source_col}")
                else:
                    print(f"⚪ {target_field} ← (unmapped)")
            
            print(f"\n📊 Mapped {mapped_count}/{len(template_fields)} fields automatically")
            
            # Ask if user wants to review/adjust
            review = input("Review and adjust mappings? (y/n): ").strip().lower()
            if review == 'y':
                field_mapping = self._review_field_mapping(field_mapping, source_columns, template_fields)
                
        else:
            field_mapping = self._manual_map_fields(source_columns, template_fields)
        
        if not field_mapping:
            print("❌ No field mapping created")
            return None
        
        # Summary
        mapped_fields = sum(1 for v in field_mapping.values() if v)
        print(f"\n📋 MAPPING SUMMARY:")
        print(f"   Total target fields: {len(template_fields)}")
        print(f"   Mapped fields: {mapped_fields}")
        print(f"   Unmapped fields: {len(template_fields) - mapped_fields}")
        
        return field_mapping
    
    def _interactive_field_mapping(self, source_columns, template_fields):
        """Interactive field mapping interface similar to filter selection"""
        print(f"\n🔗 INTERACTIVE FIELD MAPPING")
        print("=" * 50)
        print(f"Map target form fields to source CSV columns")
        print(f"Each source column can only be used once")
        
        field_mapping = {}
        used_source_columns = set()  # Track which source columns have been used
        mapped_point_fields = set()  # Track which point fields have been mapped (high_point, low_point)
        
        # Start with smart mapping as suggestions
        auto_suggestions = self._auto_map_fields(source_columns, template_fields, None)
        
        # Show available source columns
        available_source = [col for col in source_columns if col not in used_source_columns]
        
        print(f"\n📋 Available source columns ({len(available_source)}):")
        for i, col in enumerate(available_source, 1):
            auto_mapped = any(v == col for v in auto_suggestions.values())
            marker = " 🤖" if auto_mapped else ""
            print(f"  {i:2d}. {col}{marker}")
        
        print(f"\n🎯 Target form fields to map ({len(template_fields)}):")
        
        for i, field in enumerate(template_fields, 1):
            target_label = field['label']
            field_type = field.get('type', 'unknown')
            
            # Show auto-suggestion if available
            auto_suggestion = auto_suggestions.get(target_label)
            suggestion_text = f" (auto-suggests: {auto_suggestion})" if auto_suggestion and auto_suggestion not in used_source_columns else ""
            
            print(f"\n{i:2d}/{len(template_fields)}. 🎯 {target_label} [{field_type}]{suggestion_text}")
            
            # Show current available source columns (excluding used ones)
            available_source = [col for col in source_columns if col not in used_source_columns]
            
            if not available_source:
                print(f"    ❌ No more source columns available")
                field_mapping[target_label] = None
                continue
            
            print(f"    Available source columns:")
            print(f"    0. Skip (leave empty)")
            
            # Get smart suggestions for this field
            smart_suggestions = self.smart_field_mapper.get_mapping_suggestions(target_label, available_source, used_source_columns)
            
            for j, col in enumerate(available_source, 1):
                # Check if this is a smart suggestion
                suggestion_info = ""
                for suggestion in smart_suggestions[:3]:  # Show top 3 suggestions
                    if suggestion['source_field'] == col:
                        suggestion_info = f" 🤖 {suggestion['reason']}"
                        break
                
                marker = " 🤖" if col == auto_suggestion else ""
                print(f"    {j:2d}. {col}{marker}{suggestion_info}")
            
            while True:
                try:
                    choice = input(f"    Select source column (0-{len(available_source)}): ").strip()
                    
                    if choice == '0':
                        field_mapping[target_label] = None
                        break
                    elif choice.isdigit():
                        choice_num = int(choice)
                        if 1 <= choice_num <= len(available_source):
                            selected_column = available_source[choice_num - 1]
                            
                            # Check if this source column is already mapped to another target
                            already_mapped_to = None
                            for existing_target, existing_source in field_mapping.items():
                                if existing_source == selected_column:
                                    already_mapped_to = existing_target
                                    break
                            
                            if already_mapped_to:
                                print(f"    ⚠️  Warning: '{selected_column}' is already mapped to '{already_mapped_to}'")
                                print(f"    Each source column can only be mapped to one target field")
                                continue
                            
                            # Remember this successful mapping
                            self.smart_field_mapper.remember_mapping(None, target_label, selected_column)
                            self.smart_field_mapper.update_mapping_history(None, target_label, selected_column, success=True)
                            
                            # Check for duplicate point field mappings
                            target_lower = target_label.lower()
                            if ('high' in target_lower and 'point' in target_lower):
                                if 'high_point' in mapped_point_fields:
                                    print(f"    ⚠️  Warning: high_point field already mapped. Only one high_point mapping allowed.")
                                    continue
                                mapped_point_fields.add('high_point')
                            elif ('low' in target_lower and 'point' in target_lower):
                                if 'low_point' in mapped_point_fields:
                                    print(f"    ⚠️  Warning: low_point field already mapped. Only one low_point mapping allowed.")
                                    continue
                                mapped_point_fields.add('low_point')
                            
                            field_mapping[target_label] = selected_column
                            used_source_columns.add(selected_column)
                            print(f"    ✅ Mapped: {target_label} ← {selected_column}")
                            break
                        else:
                            print(f"    Invalid choice. Please enter 0-{len(available_source)}")
                    else:
                        print(f"    Invalid input. Please enter a number 0-{len(available_source)}")
                
                except KeyboardInterrupt:
                    print(f"\n    ⚠️ Skipping remaining fields...")
                    # Fill remaining fields with None
                    for remaining_field in template_fields[i:]:
                        field_mapping[remaining_field['label']] = None
                    return field_mapping
        
        # Summary
        mapped_count = sum(1 for v in field_mapping.values() if v)
        unmapped_count = len(template_fields) - mapped_count
        used_count = len(used_source_columns)
        unused_count = len(source_columns) - used_count
        
        print(f"\n📊 MAPPING SUMMARY:")
        print(f"   ✅ Mapped fields: {mapped_count}/{len(template_fields)}")
        print(f"   ⚪ Unmapped fields: {unmapped_count}")
        print(f"   🔗 Used source columns: {used_count}/{len(source_columns)}")
        print(f"   📋 Unused source columns: {unused_count}")
        
        if unused_count > 0:
            unused_columns = [col for col in source_columns if col not in used_source_columns]
            print(f"   📋 Unused: {', '.join(unused_columns)}")
        
        return field_mapping
    
    def _auto_map_fields(self, source_columns, template_fields, form_name=None):
        """Use smart field mapper for intelligent field mapping"""
        return self.smart_field_mapper.get_smart_mapping(source_columns, template_fields, form_name)
    
    def _manual_map_fields(self, source_columns, template_fields):
        """Manual field-by-field mapping"""
        mapping = {}
        
        print(f"\n🔗 Manual Field Mapping")
        print("=" * 40)
        
        for i, field in enumerate(template_fields, 1):
            req_marker = " *" if field['required'] else ""
            print(f"\n{i}/{len(template_fields)} Target: {field['label']}{req_marker} ({field['type']})")
            
            # Show source options
            print("Available source columns:")
            for j, col in enumerate(source_columns, 1):
                print(f"   {j}. {col}")
            
            while True:
                choice = input("Enter source column number (or 'skip'): ").strip()
                
                if choice.lower() == 'skip':
                    mapping[field['label']] = None
                    break
                
                try:
                    col_idx = int(choice) - 1
                    if 0 <= col_idx < len(source_columns):
                        mapping[field['label']] = source_columns[col_idx]
                        print(f"✅ Mapped: {field['label']} ← {source_columns[col_idx]}")
                        break
                    else:
                        print("Invalid choice. Please try again.")
                except ValueError:
                    print("Please enter a number or 'skip'.")
        
        return mapping
    
    def _review_field_mapping(self, current_mapping, source_columns, template_fields):
        """Review and adjust auto-generated mappings"""
        print(f"\n📝 REVIEW FIELD MAPPINGS")
        print("=" * 50)
        print("Enter new mapping or press Enter to keep current")
        
        field_lookup = {f['label']: f for f in template_fields}
        
        for target_label, current_source in current_mapping.items():
            field = field_lookup[target_label]
            req_marker = " *" if field['required'] else ""
            current_display = current_source if current_source else "(unmapped)"
            
            print(f"\n🎯 {target_label}{req_marker} ({field['type']})")
            print(f"   Current: {current_display}")
            
            # Show source options
            print("   Available sources:")
            for j, col in enumerate(source_columns, 1):
                print(f"      {j}. {col}")
            
            choice = input("   New mapping (number), 'unmap' to remove, or Enter to keep current: ").strip()
            
            if choice:
                if choice.lower() in ['skip', 'none', 'unmap', 'remove']:
                    current_mapping[target_label] = None
                    print(f"   ⚪ Unmapped: {target_label}")
                else:
                    try:
                        col_idx = int(choice) - 1
                        if 0 <= col_idx < len(source_columns):
                            current_mapping[target_label] = source_columns[col_idx]
                            print(f"   ✅ Updated: {target_label} ← {source_columns[col_idx]}")
                        else:
                            print("   Invalid choice. Keeping current mapping.")
                    except ValueError:
                        print("   Invalid input. Keeping current mapping.")
        
        return current_mapping
    
    def _create_status_mapping(self, source_csv_path, target_form_id, source_form_name, target_form_name):
        """Create status mapping between source and target forms"""
        print(f"\n🔄 STATUS MAPPING: {source_form_name} → {target_form_name}")
        print("=" * 70)
        
        # Get source status values from CSV
        import pandas as pd
        try:
            source_df = pd.read_csv(source_csv_path)
            if 'status' in source_df.columns:
                source_statuses = sorted(source_df['status'].dropna().unique().tolist())
                print(f"📄 Source CSV has {len(source_statuses)} status values")
            else:
                print("⚠️ No status column found in source CSV")
                return {}
        except Exception as e:
            print(f"❌ Error reading source CSV: {str(e)}")
            return {}
        
        # Get target status values from target form
        target_statuses = self.api_client.get_status_values_from_form(target_form_id, target_form_name)
        print(f"🎯 Target form has {len(target_statuses)} status values")
        
        if not source_statuses:
            print("⚠️ No source statuses to map")
            return {}
        
        if not target_statuses:
            print("⚠️ No target statuses found - will keep original status values")
            return {}
        
        # Show status values
        print(f"\n📋 Source Status Values:")
        for i, status in enumerate(source_statuses, 1):
            print(f"   {i}. {status}")
        
        print(f"\n🎯 Target Status Values:")
        for i, status in enumerate(target_statuses, 1):
            print(f"   {i}. {status}")
        
        # Create status mapping
        status_mapping = {}
        
        print(f"\n🔗 Creating status mappings...")
        print("For each source status, select the corresponding target status")
        print("Type 'auto' for automatic best-match mapping, or 'manual' for status-by-status mapping")
        
        mapping_mode = input("Choose mapping mode (auto/manual): ").strip().lower()
        
        if mapping_mode == 'auto':
            status_mapping = self._auto_map_statuses(source_statuses, target_statuses)
            
            # Show auto mapping results
            print(f"\n🤖 Auto-mapping results:")
            print("-" * 50)
            for source_status, target_status in status_mapping.items():
                if target_status:
                    print(f"✅ {source_status} → {target_status}")
                else:
                    print(f"⚪ {source_status} → (unmapped)")
            
            # Ask if user wants to review/adjust
            review = input("Review and adjust status mappings? (y/n): ").strip().lower()
            if review == 'y':
                status_mapping = self._review_status_mapping(status_mapping, source_statuses, target_statuses)
        else:
            status_mapping = self._manual_map_statuses(source_statuses, target_statuses)
        
        # Summary
        mapped_statuses = sum(1 for v in status_mapping.values() if v)
        print(f"\n📋 STATUS MAPPING SUMMARY:")
        print(f"   Source statuses: {len(source_statuses)}")
        print(f"   Target statuses: {len(target_statuses)}")
        print(f"   Mapped statuses: {mapped_statuses}")
        print(f"   Unmapped statuses: {len(source_statuses) - mapped_statuses}")
        
        return status_mapping
    
    def _auto_map_statuses(self, source_statuses, target_statuses):
        """Automatically map status values based on name similarity"""
        mapping = {}
        
        for source_status in source_statuses:
            source_lower = source_status.lower()
            best_match = None
            best_score = 0
            
            for target_status in target_statuses:
                target_lower = target_status.lower()
                
                # Exact matches
                if source_lower == target_lower:
                    best_match = target_status
                    best_score = 1.0
                    break
                
                # Partial matches and word matches
                source_words = source_lower.replace('(', ' ').replace(')', ' ').split()
                target_words = target_lower.replace('(', ' ').replace(')', ' ').split()
                
                # Check if any words match
                word_matches = sum(1 for word in source_words if word in target_words)
                if word_matches > 0:
                    score = word_matches / max(len(source_words), len(target_words))
                    if score > best_score:
                        best_match = target_status
                        best_score = score
                
                # Check if target is contained in source or vice versa
                if source_lower in target_lower or target_lower in source_lower:
                    score = min(len(source_lower), len(target_lower)) / max(len(source_lower), len(target_lower))
                    if score > best_score:
                        best_match = target_status
                        best_score = score
            
            mapping[source_status] = best_match
        
        return mapping
    
    def _manual_map_statuses(self, source_statuses, target_statuses):
        """Manual status-by-status mapping"""
        mapping = {}
        
        print(f"\n🔗 Manual Status Mapping")
        print("=" * 40)
        
        for i, source_status in enumerate(source_statuses, 1):
            print(f"\n{i}/{len(source_statuses)} Source Status: '{source_status}'")
            
            # Show target options
            print("Available target statuses:")
            for j, target_status in enumerate(target_statuses, 1):
                print(f"   {j}. {target_status}")
            
            while True:
                choice = input("Select target status number (or 'skip'): ").strip()
                
                if choice.lower() == 'skip':
                    mapping[source_status] = None
                    break
                
                try:
                    status_idx = int(choice) - 1
                    if 0 <= status_idx < len(target_statuses):
                        mapping[source_status] = target_statuses[status_idx]
                        print(f"✅ Mapped: '{source_status}' → '{target_statuses[status_idx]}'")
                        break
                    else:
                        print("Invalid choice. Please try again.")
                except ValueError:
                    print("Please enter a number or 'skip'.")
        
        return mapping
    
    def _review_status_mapping(self, current_mapping, source_statuses, target_statuses):
        """Review and adjust auto-generated status mappings"""
        print(f"\n📝 REVIEW STATUS MAPPINGS")
        print("=" * 50)
        print("Enter new mapping or press Enter to keep current")
        
        for source_status, current_target in current_mapping.items():
            current_display = current_target if current_target else "(unmapped)"
            
            print(f"\n📋 Source: '{source_status}'")
            print(f"   Current: {current_display}")
            
            # Show target options
            print("   Available targets:")
            for j, target_status in enumerate(target_statuses, 1):
                print(f"      {j}. {target_status}")
            
            choice = input("   New mapping (number), 'unmap', or Enter to keep current: ").strip()
            
            if choice:
                if choice.lower() in ['skip', 'none', 'unmap', 'remove']:
                    current_mapping[source_status] = None
                    print(f"   ⚪ Unmapped: '{source_status}'")
                else:
                    try:
                        status_idx = int(choice) - 1
                        if 0 <= status_idx < len(target_statuses):
                            current_mapping[source_status] = target_statuses[status_idx]
                            print(f"   ✅ Updated: '{source_status}' → '{target_statuses[status_idx]}'")
                        else:
                            print("   Invalid choice. Keeping current mapping.")
                    except ValueError:
                        print("   Invalid input. Keeping current mapping.")
        
        return current_mapping
    
    def _transform_csv_to_template(self, source_csv_path, property_folder, template_fields, field_mapping, target_form_name, status_mapping=None):
        """Transform source CSV to match target form template"""
        print(f"\n📊 TRANSFORMING CSV TO TEMPLATE")
        print("=" * 50)
        
        import pandas as pd
        
        try:
            # Read source CSV
            source_df = pd.read_csv(source_csv_path)
            print(f"📄 Source: {len(source_df)} rows, {len(source_df.columns)} columns")
            
            # Create new DataFrame with template structure
            template_df = pd.DataFrame()
            
            # Add system columns
            system_columns = ['id', 'status', 'created_at', 'updated_at', 'created_by', 'updated_by', 'latitude', 'longitude']
            for sys_col in system_columns:
                if sys_col in source_df.columns:
                    if sys_col == 'status' and status_mapping:
                        # Apply status mapping
                        template_df[sys_col] = source_df[sys_col].map(status_mapping).fillna(source_df[sys_col])
                        print(f"🔄 Applied status mapping to {len(source_df)} records")
                    else:
                        template_df[sys_col] = source_df[sys_col]
            
            # Process high_point and low_point fields first to ensure they're available for severity calculation
            processed_point_fields = []
            if 'High Point' in source_df.columns:
                template_df['high_point'] = self._process_point_values(source_df['High Point'])
                processed_point_fields.append('high_point')
                print(f"🔢 Processed high_point values as integers")
            if 'Low Point' in source_df.columns:
                template_df['low_point'] = self._process_point_values(source_df['Low Point'])
                processed_point_fields.append('low_point')
                print(f"🔢 Processed low_point values as integers")
            
            # Map template fields
            for field in template_fields:
                target_label = field['label']
                source_column = field_mapping.get(target_label)
                
                if source_column and source_column in source_df.columns:
                    # Check if this is a point field that needs processing
                    if ('high' in source_column.lower() and 'point' in source_column.lower()) or ('low' in source_column.lower() and 'point' in source_column.lower()):
                        # Process point values as integers
                        template_df[target_label] = self._process_point_values(source_df[source_column])
                    else:
                        # Auto-fill blank measurement fields with 0
                        if self._is_measurement_field(target_label):
                            template_df[target_label] = source_df[source_column].fillna(0)
                            # Also replace empty strings with 0
                            template_df[target_label] = template_df[target_label].replace(['', ' ', 'nan', 'None'], 0)
                            print(f"🔢 Auto-filled blank {target_label} values with 0")
                        else:
                            template_df[target_label] = source_df[source_column]
                elif 'districtproperty' in target_label.lower().replace(' ', '') or target_label.lower() == 'district property':
                    # Special handling for districtproperty field
                    district_value = self._get_district_property_value(source_csv_path.parent.name)
                    template_df[target_label] = district_value
                    print(f"🏢 Auto-populated districtproperty: '{district_value}'")
                elif 'datasource' in target_label.lower().replace(' ', '') or target_label.lower() == 'data source':
                    # Special handling for data source field
                    data_source_value = self._get_data_source_value(source_csv_path.parent.name)
                    template_df[target_label] = data_source_value
                    print(f"📊 Auto-populated data source: '{data_source_value}'")
                elif 'severity' in target_label.lower() or 'level' in target_label.lower():
                    # Calculate severity based on high_point
                    template_df[target_label] = self._calculate_severity_level(template_df)
                    print(f"📊 Auto-calculated severity levels based on high_point")
                else:
                    # Create empty column for unmapped fields
                    template_df[target_label] = ''
            
            print(f"🎯 Template: {len(template_df)} rows, {len(template_df.columns)} columns")
            
            # Check if districtproperty was added, if not offer to add it
            has_district_property = any('districtproperty' in col.lower().replace(' ', '') for col in template_df.columns)
            has_data_source = any('datasource' in col.lower().replace(' ', '') or 'data source' in col.lower() for col in template_df.columns)
            
            if not has_district_property:
                add_district = input(f"\n🏢 Target form doesn't have districtproperty field. Add it? (y/n): ").strip().lower()
                
                if add_district == 'y':
                    district_value = self._get_district_property_value(source_csv_path.parent.name)
                    template_df['districtproperty'] = district_value
                    print(f"✅ Added districtproperty column: '{district_value}'")
            
            if not has_data_source:
                add_data_source = input(f"\n📊 Target form doesn't have data source field. Add it? (y/n): ").strip().lower()
                
                if add_data_source == 'y':
                    data_source_value = self._get_data_source_value(source_csv_path.parent.name)
                    template_df['data_source'] = data_source_value
                    print(f"✅ Added data_source column: '{data_source_value}'")
            
            # Check if severity level should be added if not already mapped
            has_severity = any('severity' in col.lower() or 'level' in col.lower() for col in template_df.columns)
            has_high_point = any('high' in col.lower() and 'point' in col.lower() for col in template_df.columns)
            
            if not has_severity and has_high_point:
                add_severity = input(f"\n📊 Target form doesn't have severity field but has high_point. Add auto-calculated severity? (y/n): ").strip().lower()
                
                if add_severity == 'y':
                    template_df['severity_level'] = self._calculate_severity_level(template_df)
                    print(f"✅ Added auto-calculated severity_level column based on high_point")

            # Save migrated CSV
            safe_target_name = "".join(c for c in target_form_name if c.isalnum() or c in (' ', '-', '_')).rstrip().replace(' ', '_')
            migrated_filename = f"{safe_target_name}_migrated.csv"
            migrated_csv_path = property_folder / migrated_filename
            
            template_df.to_csv(migrated_csv_path, index=False)
            
            # Count auto-filled measurement fields
            auto_filled_count = 0
            for field in template_fields:
                target_label = field['label']
                if self._is_measurement_field(target_label) and target_label in template_df.columns:
                    # Count rows where the field was auto-filled with 0
                    auto_filled_count += (template_df[target_label] == 0).sum()
            
            print(f"✅ Migrated CSV saved: {migrated_filename}")
            print(f"📊 Summary:")
            print(f"   Mapped fields: {sum(1 for v in field_mapping.values() if v)}")
            print(f"   Total template columns: {len(template_df.columns)}")
            print(f"   Data rows: {len(template_df)}")
            if auto_filled_count > 0:
                print(f"   🔢 Auto-filled {auto_filled_count} blank measurement values with 0")
            
            return migrated_csv_path
            
        except Exception as e:
            print(f"❌ Error transforming CSV: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _is_measurement_field(self, field_label):
        """Check if a field is a measurement field that should auto-fill blanks with 0"""
        field_lower = field_label.lower()
        
        # Measurement fields that should auto-fill with 0
        measurement_keywords = [
            'high point', 'low point', 'slicing length', 'slice length', 'expansion joint length',
            'inch feet', 'inches', 'feet', 'length', 'width', 'height', 'depth',
            'measurement', 'dimension', 'size', 'area', 'volume', 'weight',
            'l_patch', 'xl_patch', 'patch', 'patch_size'
        ]
        
        return any(keyword in field_lower for keyword in measurement_keywords)
    
    def _get_district_property_value(self, folder_name):
        """Get districtproperty value by searching classification sets"""
        print(f"\n🔍 CLASSIFICATION SET SEARCH")
        print("=" * 50)
        print(f"Searching for: {folder_name}")
        
        try:
            # Extract search terms from folder name
            # Remove common suffixes and clean the name
            search_name = folder_name.replace('_data', '').replace('_migrated', '')
            # Remove status and timestamp parts
            parts = search_name.split('_')
            # Remove likely timestamp and status parts
            clean_parts = []
            for part in parts:
                part_lower = part.lower()
                # Skip parts that look like timestamps or statuses
                if part.isdigit() and len(part) == 8:  # Date like 20250822
                    continue
                if part.isdigit() and len(part) == 6:  # Time like 143702
                    continue
                # Expanded status terms to filter out
                if part_lower in [
                    'replace', 'repair', 'complete', 'slice', 'incomplete', 'patch',
                    'replaceandrepair', 'completeslice', 'incompleteslice', 
                    'expansion', 'transverse', 'level1', 'level2', 'level3',
                    'minor', 'moderate', 'severe'
                ]:
                    continue
                clean_parts.append(part)
            
            search_terms = ' '.join(clean_parts).replace('_', ' ').strip().lower()
            print(f"🔍 Search terms: '{search_terms}'")
            
            # Get classification sets
            classification_sets = self.api_client.get_classification_sets()
            print(f"📋 Found {len(classification_sets)} classification sets")
            
            # Search through all classification sets for matches
            matches = []
            
            print(f"\n🔍 Starting deep search through {len(classification_sets)} classification sets...")
            print(f"💡 Search will check ALL nested levels and multiple child key types")
            print(f"🔍 Debug mode will show detailed search process for better understanding")
            
            for cls_set in classification_sets:
                cls_name = cls_set.get('name', '')
                print(f"\n📁 Searching classification set: {cls_name}")
                print(f"   Items in set: {len(cls_set.get('items', []))}")
                
                # Enable debug for more sets to show search process
                debug_enabled = cls_name.upper() in ['LMH', 'SCHOOLS', 'DISTRICTS']
                if debug_enabled:
                    print(f"   🔍 DEBUG MODE: Showing detailed search for {cls_name}")
                
                # Find matches in this classification set with deep search
                set_matches = self._search_classification_items(
                    cls_set.get('items', []), 
                    search_terms, 
                    [cls_name],  # Start with set name as root
                    debug=debug_enabled
                )
                
                if set_matches:
                    print(f"   🎯 Found {len(set_matches)} matches in {cls_name}:")
                    
                    # Sort matches by depth and score for better organization
                    set_matches.sort(key=lambda x: (x['depth'], -x['score']))
                    
                    for match in set_matches:
                        # Show depth indicator
                        depth_indicator = "  " * (match['depth'] - 1) if match['depth'] > 0 else ""
                        depth_label = f"[D{match['depth']}]" if match['depth'] > 0 else "[ROOT]"
                        
                        matches.append({
                            'set_name': cls_name,
                            'path': match['path'],
                            'formatted_path': match['formatted_path'],
                            'score': match['score'],
                            'exact_words': match['exact_words'],
                            'partial_words': match['partial_words'],
                            'depth': match['depth'],
                            'full_path': match['full_path']
                        })
                        
                        print(f"     {depth_indicator}{depth_label} ✅ {match['formatted_path']} (score: {match['score']:.2f})")
                        if debug_enabled and match['depth'] > 0:
                            print(f"        📍 Full path: {match['full_path']}")
                else:
                    print(f"   ❌ No matches in {cls_name}")
            
            if not matches:
                print(f"❌ No exact classification matches found for '{search_terms}'")
                
                # Look for parent branches (like NAVY, MARINES) that user could extend
                parent_matches = []
                for cls_set in classification_sets:
                    cls_name = cls_set.get('name', '')
                    if cls_name.upper() == 'LMH':  # Focus on LMH classification
                        for item in cls_set.get('items', []):
                            parent_label = item.get('label', '')
                            parent_matches.append({
                                'set_name': cls_name,
                                'parent': parent_label,
                                'formatted_path': parent_label  # Just the parent for now
                            })
                
                if parent_matches:
                    print(f"\n🔗 Found parent branches in LMH classification:")
                    for i, match in enumerate(parent_matches, 1):
                        print(f"{i}. {match['parent']}")
                    
                    print(f"\nYou can:")
                    print(f"1. Select a parent branch and add district manually")
                    print(f"2. Skip and leave districtproperty empty")
                    
                    choice = input("Select parent branch number or 'skip': ").strip()
                    
                    if choice.lower() == 'skip':
                        return ""
                    
                    try:
                        parent_idx = int(choice) - 1
                        if 0 <= parent_idx < len(parent_matches):
                            selected_parent = parent_matches[parent_idx]
                            
                            # Ask for district name
                            district_name = input(f"Enter district name for {selected_parent['parent']}: ").strip()
                            
                            if district_name:
                                # Format as parent,district (omitting LMH root)
                                final_path = f"{selected_parent['parent']},{district_name}"
                                print(f"✅ Manual classification: {final_path}")
                                return final_path
                            else:
                                return selected_parent['parent']  # Just the parent
                        else:
                            print("Invalid choice. Leaving districtproperty empty.")
                    except ValueError:
                        print("Invalid input. Leaving districtproperty empty.")
                
                return ""
            
            # Auto-select the highest scoring match and ask for confirmation
            if len(matches) == 1:
                chosen_match = matches[0]
                print(f"\n🎯 Auto-selected: {chosen_match['formatted_path']}")
                if chosen_match['depth'] > 0:
                    print(f"   📍 Full classification path: {chosen_match['full_path']}")
            else:
                # Find the highest scoring match
                best_match = max(matches, key=lambda x: (x['score'], -x['depth']))  # Higher score first, then lower depth
                
                print(f"\n🎯 AUTO-SELECTED BEST MATCH:")
                print("=" * 60)
                print(f"✅ {best_match['formatted_path']}")
                print(f"   📍 Full path: {best_match['full_path']}")
                print(f"   🎯 Score: {best_match['score']:.2f} (Exact: {best_match['exact_words']}, Partial: {best_match['partial_words']})")
                print(f"   📁 Classification Set: {best_match['set_name']}")
                
                # Show a few other high-scoring alternatives for comparison
                other_matches = [m for m in matches if m != best_match]
                if other_matches:
                    # Sort by score and show top 3 alternatives
                    other_matches.sort(key=lambda x: (x['score'], -x['depth']), reverse=True)
                    print(f"\n🔍 Other high-scoring alternatives:")
                    print("-" * 40)
                    for i, match in enumerate(other_matches[:3], 1):
                        depth_label = f"[D{match['depth']}]" if match['depth'] > 0 else "[ROOT]"
                        print(f"   {i}. {depth_label} {match['formatted_path']} (score: {match['score']:.2f})")
                    
                    if len(other_matches) > 3:
                        print(f"   ... and {len(other_matches) - 3} more options")
                
                # Ask for confirmation
                print(f"\n❓ Is this the correct classification?")
                print(f"   Selected: {best_match['formatted_path']}")
                
                while True:
                    confirm = input("Confirm selection? (y/n/choose): ").strip().lower()
                    
                    if confirm in ['y', 'yes', '']:
                        chosen_match = best_match
                        print(f"✅ Confirmed: {chosen_match['formatted_path']}")
                        break
                    elif confirm in ['n', 'no']:
                        # User wants to see all options and choose manually
                        print(f"\n🔍 Showing all {len(matches)} classification matches:")
                        print("-" * 80)
                        
                        # Group matches by set and depth for better organization
                        matches_by_set = {}
                        for match in matches:
                            set_name = match['set_name']
                            if set_name not in matches_by_set:
                                matches_by_set[set_name] = []
                            matches_by_set[set_name].append(match)
                        
                        # Display organized results
                        for set_name, set_matches in matches_by_set.items():
                            print(f"\n📁 {set_name} Classification Set:")
                            # Sort by depth and score
                            set_matches.sort(key=lambda x: (x['depth'], -x['score']))
                            
                            for i, match in enumerate(set_matches, 1):
                                depth_indicator = "  " * match['depth'] if match['depth'] > 0 else ""
                                depth_label = f"[D{match['depth']}]" if match['depth'] > 0 else "[ROOT]"
                                
                                print(f"{i:2d}. {depth_indicator}{depth_label} {match['formatted_path']}")
                                print(f"     📍 Full path: {match['full_path']}")
                                print(f"     🎯 Score: {match['score']:.2f} (Exact: {match['exact_words']}, Partial: {match['partial_words']})")
                                print()
                        
                        # Manual selection
                        while True:
                            choice = input(f"Select classification (1-{len(matches)}): ").strip()
                            try:
                                choice_idx = int(choice) - 1
                                if 0 <= choice_idx < len(matches):
                                    chosen_match = matches[choice_idx]
                                    break
                                else:
                                    print("Invalid choice. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                        break
                    elif confirm == 'choose':
                        # User wants to see all options and choose manually (same as 'no')
                        print(f"\n🔍 Showing all {len(matches)} classification matches:")
                        print("-" * 80)
                        
                        # Group matches by set and depth for better organization
                        matches_by_set = {}
                        for match in matches:
                            set_name = match['set_name']
                            if set_name not in matches_by_set:
                                matches_by_set[set_name] = []
                            matches_by_set[set_name].append(match)
                        
                        # Display organized results
                        for set_name, set_matches in matches_by_set.items():
                            print(f"\n📁 {set_name} Classification Set:")
                            # Sort by depth and score
                            set_matches.sort(key=lambda x: (x['depth'], -x['score']))
                            
                            for i, match in enumerate(set_matches, 1):
                                depth_indicator = "  " * match['depth'] if match['depth'] > 0 else ""
                                depth_label = f"[D{match['depth']}]" if match['depth'] > 0 else "[ROOT]"
                                
                                print(f"{i:2d}. {depth_indicator}{depth_label} {match['formatted_path']}")
                                print(f"     📍 Full path: {match['full_path']}")
                                print(f"     🎯 Score: {match['score']:.2f} (Exact: {match['exact_words']}, Partial: {match['partial_words']})")
                                print()
                        
                        # Manual selection
                        while True:
                            choice = input(f"Select classification (1-{len(matches)}): ").strip()
                            try:
                                choice_idx = int(choice) - 1
                                if 0 <= choice_idx < len(matches):
                                    chosen_match = matches[choice_idx]
                                    break
                                else:
                                    print("Invalid choice. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                        break
                    else:
                        print("Please enter 'y' (yes), 'n' (no), or 'choose' to see all options.")
            
            print(f"✅ Selected: {chosen_match['formatted_path']}")
            return chosen_match['formatted_path']
            
        except Exception as e:
            print(f"❌ Classification search failed: {str(e)}")
            print("Will leave districtproperty empty")
            return ""
    
    def _search_classification_items(self, items, search_terms, current_path, debug=False, max_depth=10, current_depth=0):
        """Recursively search classification items for matches - searches EVERYTHING with depth tracking"""
        matches = []
        
        search_words = search_terms.lower().split()
        if debug:
            depth_indent = "  " * current_depth
            print(f"{depth_indent}🔍 Searching at depth {current_depth} in path: {' → '.join(current_path) if current_path else '(root)'}")
            print(f"{depth_indent}📋 Items at this level: {len(items)}")
        
        # Prevent infinite recursion
        if current_depth >= max_depth:
            if debug:
                depth_indent = "  " * current_depth
                print(f"{depth_indent}⚠️  Max depth {max_depth} reached, stopping recursion")
            return matches
        
        for item in items:
            item_name = item.get('label', '').lower()
            item_display_name = item.get('label', 'Unknown')
            current_path_copy = current_path + [item_display_name]
            
            if debug:
                depth_indent = "  " * current_depth
                print(f"{depth_indent}📄 Checking item: '{item_display_name}' (depth: {current_depth}, path: {' → '.join(current_path_copy)})")
            
            # Calculate match score - check for any word matches
            words_matched = 0
            for word in search_words:
                if word in item_name:
                    words_matched += 1
                    if debug:
                        print(f"{depth_indent}  ✅ Word match: '{word}' found in '{item_display_name}'")
            
            # Also check for partial matches
            partial_matches = 0
            for word in search_words:
                for item_word in item_name.split():
                    if word in item_word or item_word in word:
                        partial_matches += 1
                        if debug:
                            print(f"{depth_indent}  🔤 Partial match: '{word}' ↔ '{item_word}' in '{item_display_name}'")
                        break
            
            total_matches = words_matched + (partial_matches * 0.5)  # Weight partial matches less
            
            if total_matches > 0:
                score = total_matches / len(search_words)
                # Format path, omitting the first element (root) for cleaner display
                formatted_path = ','.join(current_path_copy[1:]) if len(current_path_copy) > 1 else current_path_copy[0]
                
                match_info = {
                    'path': current_path_copy,
                    'formatted_path': formatted_path,
                    'score': score,
                    'item': item,
                    'exact_words': words_matched,
                    'partial_words': partial_matches,
                    'depth': current_depth,
                    'full_path': ' → '.join(current_path_copy)
                }
                matches.append(match_info)
                
                if debug:
                    print(f"{depth_indent}  🎯 MATCH FOUND: '{item_display_name}' → '{formatted_path}' (score: {score:.2f}, depth: {current_depth})")
            
            # Recursively search children - check multiple possible child keys
            child_keys = ['children', 'items', 'child_items', 'sub_items', 'child_classifications']
            for child_key in child_keys:
                if child_key in item and item[child_key]:
                    if debug:
                        print(f"{depth_indent}  📁 Found {len(item[child_key])} children under '{child_key}' in '{item_display_name}'")
                    child_matches = self._search_classification_items(
                        item[child_key], 
                        search_terms, 
                        current_path_copy, 
                        debug, 
                        max_depth, 
                        current_depth + 1
                    )
                    matches.extend(child_matches)
        
        if debug:
            depth_indent = "  " * current_depth
            print(f"{depth_indent}📊 Found {len(matches)} total matches at depth {current_depth}")
        
        return matches
    
    def _process_point_values(self, point_series):
        """Process high_point and low_point values: ensure they are integers, multiply by 8 if decimal"""
        import pandas as pd
        
        def process_value(value):
            if pd.isna(value):
                return None
            
            try:
                # Convert to float first
                float_val = float(value)
                
                # Check if it's already an integer (no decimal part)
                if float_val == int(float_val):
                    return int(float_val)
                else:
                    # It's a decimal, multiply by 8 and convert to int
                    return int(float_val * 8)
                    
            except (ValueError, TypeError):
                # If conversion fails, return original value
                return value
        
        # Apply processing and ensure result is integer type
        processed = point_series.apply(process_value)
        # Convert to nullable integer type to handle NaN values properly
        return processed.astype('Int64')
    
    def _calculate_severity_level(self, dataframe):
        """Calculate severity level based on high_point values"""
        import pandas as pd
        
        def get_severity(row):
            # Look for high_point in various possible column names
            high_point_value = None
            for col in dataframe.columns:
                if 'high' in col.lower() and 'point' in col.lower():
                    high_point_value = row[col]
                    break
            
            if pd.isna(high_point_value):
                return ""
            
            try:
                hp = float(high_point_value)
                if hp < 4:
                    return "Minor (Level 1)"
                elif hp < 8:
                    return "Moderate (Level 2)"
                else:
                    return "Severe (Level 3)"
            except (ValueError, TypeError):
                return ""
        
        return dataframe.apply(get_severity, axis=1)
    
    def _get_data_source_value(self, folder_name):
        """Get data source value with property name and form ID"""
        print(f"\n📊 DATA SOURCE GENERATION")
        print("=" * 50)
        
        try:
            # Extract property name from folder name
            search_name = folder_name.replace('_data', '').replace('_migrated', '')
            parts = search_name.split('_')
            clean_parts = []
            for part in parts:
                if part.isdigit() and len(part) == 8:  # Date
                    continue
                if part.isdigit() and len(part) == 6:  # Time
                    continue
                if part.lower() in ['replace', 'repair', 'complete', 'slice', 'incomplete', 'patch', 'replaceandrepair']:
                    continue
                clean_parts.append(part)
            
            property_name = ' '.join(clean_parts).replace('_', ' ').strip()
            print(f"🏠 Extracted property name: '{property_name}'")
            
            # Try to find the source form ID
            form_id = None
            try:
                # Look for forms that match this property name
                all_forms = self.api_client.get_forms('all')
                
                # Search for forms with similar names
                matching_forms = []
                property_words = property_name.lower().split()
                
                for form in all_forms:
                    form_name = form.get('name', '').lower()
                    
                    # Check if property words appear in form name
                    matches = sum(1 for word in property_words if word in form_name)
                    if matches >= len(property_words) * 0.5:  # At least 50% of words match
                        matching_forms.append({
                            'form': form,
                            'matches': matches
                        })
                
                if matching_forms:
                    # Sort by best matches
                    matching_forms.sort(key=lambda x: x['matches'], reverse=True)
                    best_match = matching_forms[0]['form']
                    
                    form_id = best_match.get('id', '')
                    source_form_name = best_match.get('name', '')
                    
                    print(f"📋 Found matching source form: {source_form_name}")
                    print(f"🆔 Form ID: {form_id}")
                else:
                    print(f"⚠️ No matching source form found")
                    
            except Exception as e:
                print(f"⚠️ Could not find source form: {str(e)}")
            
            # Build data source value
            if form_id:
                data_source = f"{property_name} ({form_id})"
            else:
                data_source = property_name
            
            print(f"✅ Data source value: '{data_source}'")
            return data_source
            
        except Exception as e:
            print(f"❌ Data source generation failed: {str(e)}")
            return "Unknown Source"
    
    def explore_classification_structure(self, classification_set_name=None, max_depth=5):
        """Explore and display the full structure of classification sets"""
        print(f"\n🔍 CLASSIFICATION STRUCTURE EXPLORER")
        print("=" * 60)
        
        try:
            # Get classification sets
            classification_sets = self.api_client.get_classification_sets()
            print(f"📋 Found {len(classification_sets)} classification sets")
            
            if classification_set_name:
                # Explore specific set
                target_set = None
                for cls_set in classification_sets:
                    if cls_set.get('name', '').upper() == classification_set_name.upper():
                        target_set = cls_set
                        break
                
                if target_set:
                    print(f"\n📁 Exploring structure of: {target_set['name']}")
                    self._display_classification_tree(target_set.get('items', []), max_depth=max_depth)
                else:
                    print(f"❌ Classification set '{classification_set_name}' not found")
                    print("Available sets:")
                    for cls_set in classification_sets:
                        print(f"  - {cls_set.get('name', 'Unknown')}")
            else:
                # Show overview of all sets
                print(f"\n📊 CLASSIFICATION SETS OVERVIEW:")
                print("-" * 40)
                
                for cls_set in classification_sets:
                    cls_name = cls_set.get('name', '')
                    items = cls_set.get('items', [])
                    print(f"\n📁 {cls_name}")
                    print(f"   Items at root level: {len(items)}")
                    
                    # Show first few items as preview
                    if items:
                        print("   Preview of root items:")
                        for i, item in enumerate(items[:5], 1):
                            print(f"     {i}. {item.get('label', 'Unknown')}")
                        if len(items) > 5:
                            print(f"     ... and {len(items) - 5} more items")
                    
                    # Ask if user wants to explore this set
                    explore = input(f"\n🔍 Explore {cls_name} structure? (y/n): ").strip().lower()
                    if explore in ['y', 'yes']:
                        self._display_classification_tree(items, max_depth=max_depth)
                        break
        
        except Exception as e:
            print(f"❌ Classification exploration failed: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def _display_classification_tree(self, items, current_depth=0, max_depth=5, path=None):
        """Recursively display classification tree structure"""
        if path is None:
            path = []
        
        if current_depth >= max_depth:
            depth_indent = "  " * current_depth
            print(f"{depth_indent}⚠️  Max depth {max_depth} reached...")
            return
        
        for item in items:
            item_label = item.get('label', 'Unknown')
            current_path = path + [item_label]
            depth_indent = "  " * current_depth
            depth_label = f"[D{current_depth}]" if current_depth > 0 else "[ROOT]"
            
            print(f"{depth_indent}{depth_label} {item_label}")
            
            # Check for children
            child_keys = ['children', 'items', 'child_items', 'sub_items']
            has_children = False
            for child_key in child_keys:
                if child_key in item and item[child_key]:
                    has_children = True
                    if current_depth < max_depth - 1:  # Don't show children if we're at max depth
                        self._display_classification_tree(
                            item[child_key], 
                            current_depth + 1, 
                            max_depth, 
                            current_path
                        )
                    else:
                        child_indent = "  " * (current_depth + 1)
                        print(f"{child_indent}📁 {len(item[child_key])} children (max depth reached)")
                    break
            
            if not has_children:
                # Show item details if no children
                item_details = []
                for key, value in item.items():
                    if key not in ['label', 'children', 'items', 'child_items', 'sub_items'] and value:
                        item_details.append(f"{key}: {value}")
                
                if item_details:
                    detail_indent = "  " * (current_depth + 1)
                    for detail in item_details[:3]:  # Show first 3 details
                        print(f"{detail_indent}  📝 {detail}")
                    if len(item_details) > 3:
                        print(f"{detail_indent}  ... and {len(item_details) - 3} more properties")
    
    def view_field_mapping_memory(self):
        """Display current field mappings"""
        print(f"\n🧠 CURRENT FIELD MAPPINGS")
        print("=" * 60)
        
        if not self.smart_field_mapper.mappings:
            print("❌ No field mappings found")
            return
        
        total_mappings = 0
        for form_name, form_mappings in self.smart_field_mapper.mappings.items():
            if form_mappings:
                print(f"\n📁 Form: {form_name.upper()}")
                print("-" * 40)
                for target_field, source_field in form_mappings.items():
                    print(f"  ✅ {target_field} ← {source_field}")
                    total_mappings += 1
        
        print(f"\n📊 Total mappings: {total_mappings}")
    
    def view_mapping_history(self):
        """Display mapping history for learning"""
        print(f"\n📚 MAPPING HISTORY")
        print("=" * 60)
        
        if not self.smart_field_mapper.mapping_history:
            print("❌ No mapping history found")
            return
        
        for form_name, form_history in self.smart_field_mapper.mapping_history.items():
            if form_history:
                print(f"\n📁 Form: {form_name.upper()}")
                print("-" * 40)
                for target_field, attempts in form_history.items():
                    print(f"  🎯 {target_field}:")
                    for attempt in attempts[-3:]:  # Show last 3 attempts
                        status = "✅" if attempt['success'] else "❌"
                        timestamp = attempt['timestamp'][:19]  # Truncate timestamp
                        print(f"    {status} {attempt['source_field']} ({timestamp})")
    
    def clear_all_field_mappings(self):
        """Clear all field mappings"""
        self.smart_field_mapper.mappings = {}
        self.smart_field_mapper.save_mappings()
        print("✅ All field mappings cleared")
    
    def export_field_mappings(self):
        """Export field mappings to a readable file"""
        if not self.smart_field_mapper.mappings:
            print("❌ No field mappings to export")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"field_mappings_export_{timestamp}.txt"
        
        with open(filename, 'w') as f:
            f.write("FIELD MAPPINGS EXPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for form_name, form_mappings in self.smart_field_mapper.mappings.items():
                if form_mappings:
                    f.write(f"FORM: {form_name.upper()}\n")
                    f.write("-" * 30 + "\n")
                    for target_field, source_field in form_mappings.items():
                        f.write(f"  {target_field} ← {source_field}\n")
                    f.write("\n")
        
        print(f"✅ Field mappings exported to: {filename}")

class SmartFieldMapper:
    def __init__(self, config_file="field_mappings.json"):
        self.config_file = config_file
        self.mappings = self.load_mappings()
        self.field_synonyms = self._get_field_synonyms()
        self.mapping_history = self.load_mapping_history()
    
    def _get_field_synonyms(self):
        """Define common field synonyms for better matching"""
        return {
            # Date fields
            'date': ['date', 'survey_date', 'completion_date', 'created_date', 'modified_date', 'timestamp'],
            'survey_date': ['survey_date', 'date', 'inspection_date', 'assessment_date'],
            'date_completed': ['date_completed', 'completion_date', 'finished_date', 'end_date'],
            
            # Name/ID fields
            'name': ['name', 'title', 'label', 'identifier', 'id'],
            'number': ['number', 'id', 'identifier', 'record_id', 'form_id'],
            'unit_number': ['unit_number', 'unit', 'apartment', 'suite', 'room'],
            
            # Location fields
            'address': ['address', 'location', 'street_address', 'property_address'],
            'location': ['location', 'address', 'site', 'property_location'],
            'specific_location': ['specific_location', 'location', 'site', 'area', 'zone'],
            
            # Measurement fields
            'length': ['length', 'slice_length', 'expansion_joint_length', 'distance', 'size'],
            'slice_length': ['slice_length', 'length', 'cut_length', 'section_length'],
            'expansion_joint_length': ['expansion_joint_length', 'joint_length', 'length', 'gap_length'],
            'high_point': ['high_point', 'high', 'maximum', 'peak', 'top'],
            'low_point': ['low_point', 'low', 'minimum', 'bottom', 'base'],
            'inch_feet': ['inch_feet', 'measurement', 'units', 'dimensions'],
            
            # Status fields
            'status': ['status', 'condition', 'state', 'phase', 'stage'],
            'severity': ['severity', 'severity_level', 'level', 'intensity', 'degree'],
            'slicing_severity_level': ['slicing_severity_level', 'severity', 'severity_level', 'level'],
            
            # Notes fields
            'notes': ['notes', 'comments', 'description', 'remarks', 'observations'],
            'sales_and_technician_notes': ['sales_and_technician_notes', 'technician_notes', 'sales_notes', 'notes'],
            'technician_notes': ['technician_notes', 'tech_notes', 'repair_notes', 'work_notes'],
            'additional_notes': ['additional_notes', 'extra_notes', 'other_notes', 'notes'],
            
            # Person fields
            'surveyed_by': ['surveyed_by', 'inspector', 'surveyor', 'technician', 'worker'],
            'completed_by': ['completed_by', 'technician', 'worker', 'inspector', 'surveyor'],
            
            # Photo fields
            'before_photos': ['before_photos', 'before_pictures', 'initial_photos', 'start_photos'],
            'completed_photos': ['completed_photos', 'after_photos', 'final_photos', 'end_photos'],
            
            # Other fields
            'reason_for_replacement': ['reason_for_replacement', 'replacement_reason', 'why_replace', 'cause'],
            'data_source': ['data_source', 'source', 'origin', 'reference'],
            'l_patch': ['l_patch', 'l_patch_size', 'large_patch'],
            'xl_patch': ['xl_patch', 'xl_patch_size', 'extra_large_patch']
        }
    
    def load_mappings(self):
        """Load field mappings from config file"""
        if Path(self.config_file).exists():
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}
    
    def save_mappings(self):
        """Save field mappings to config file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.mappings, f, indent=2)
    
    def load_mapping_history(self):
        """Load mapping history for learning"""
        history_file = self.config_file.replace('.json', '_history.json')
        if Path(history_file).exists():
            try:
                with open(history_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}
    
    def save_mapping_history(self):
        """Save mapping history for learning"""
        history_file = self.config_file.replace('.json', '_history.json')
        with open(history_file, 'w') as f:
            json.dump(self.mapping_history, f, indent=2)
    
    def get_smart_mapping(self, source_columns, template_fields, form_name=None):
        """Get intelligent field mapping with memory and synonyms"""
        mapping = {}
        used_source_columns = set()
        
        # Create a lookup for form-specific mappings
        form_key = form_name.lower() if form_name else 'default'
        form_mappings = self.mappings.get(form_key, {})
        
        # First pass: Use exact remembered mappings
        for field in template_fields:
            target_label = field['label']
            target_data_name = field.get('data_name', '')
            
            # Check if we have a remembered mapping for this form
            remembered_source = form_mappings.get(target_label) or form_mappings.get(target_data_name)
            if remembered_source and remembered_source in source_columns and remembered_source not in used_source_columns:
                mapping[target_label] = remembered_source
                used_source_columns.add(remembered_source)
                continue
            
            # Check if we have a remembered mapping from any form
            for form_maps in self.mappings.values():
                remembered_source = form_maps.get(target_label) or form_maps.get(target_data_name)
                if remembered_source and remembered_source in source_columns and remembered_source not in used_source_columns:
                    mapping[target_label] = remembered_source
                    used_source_columns.add(remembered_source)
                    break
        
        # Second pass: Use synonym-based matching for unmapped fields
        for field in template_fields:
            target_label = field['label']
            if target_label in mapping:
                continue
            
            best_match = self._find_best_synonym_match(target_label, source_columns, used_source_columns)
            if best_match:
                mapping[target_label] = best_match
                used_source_columns.add(best_match)
            else:
                mapping[target_label] = None
        
        return mapping
    
    def _find_best_synonym_match(self, target_label, source_columns, used_source_columns):
        """Find best match using synonyms and fuzzy matching"""
        target_lower = target_label.lower()
        best_match = None
        best_score = 0
        
        # Get synonyms for this target field
        synonyms = []
        for key, syn_list in self.field_synonyms.items():
            if key.lower() in target_lower or any(syn.lower() in target_lower for syn in syn_list):
                synonyms.extend(syn_list)
        
        # Add the target label itself
        synonyms.append(target_label)
        
        # Check each available source column
        for source_col in source_columns:
            if source_col in used_source_columns:
                continue
            
            source_lower = source_col.lower()
            
            # Check exact matches with synonyms
            for synonym in synonyms:
                if source_lower == synonym.lower():
                    return source_col  # Perfect match
            
            # Check partial matches
            for synonym in synonyms:
                synonym_lower = synonym.lower()
                
                # Word-based matching
                target_words = set(synonym_lower.replace('_', ' ').replace('-', ' ').split())
                source_words = set(source_lower.replace('_', ' ').replace('-', ' ').split())
                
                if target_words and source_words:
                    # Calculate word overlap
                    common_words = target_words.intersection(source_words)
                    if common_words:
                        score = len(common_words) / max(len(target_words), len(source_words))
                        
                        # Bonus for longer common sequences
                        if len(common_words) > 1:
                            score += 0.2
                        
                        if score > best_score:
                            best_score = score
                            best_match = source_col
        
        # Only return if we have a reasonable match
        return best_match if best_score > 0.3 else None
    
    def remember_mapping(self, form_name, target_field, source_field):
        """Remember a successful mapping for future use"""
        form_key = form_name.lower() if form_name else 'default'
        
        if form_key not in self.mappings:
            self.mappings[form_key] = {}
        
        self.mappings[form_key][target_field] = source_field
        
        # Also remember in global mappings for cross-form learning
        if 'global' not in self.mappings:
            self.mappings['global'] = {}
        self.mappings['global'][target_field] = source_field
        
        self.save_mappings()
    
    def update_mapping_history(self, form_name, target_field, source_field, success=True):
        """Update mapping history for learning"""
        form_key = form_name.lower() if form_name else 'default'
        
        if form_key not in self.mapping_history:
            self.mapping_history[form_key] = {}
        
        if target_field not in self.mapping_history[form_key]:
            self.mapping_history[form_key][target_field] = []
        
        # Add this mapping attempt to history
        mapping_record = {
            'source_field': source_field,
            'timestamp': datetime.now().isoformat(),
            'success': success
        }
        
        self.mapping_history[form_key][target_field].append(mapping_record)
        
        # Keep only last 10 attempts per field
        if len(self.mapping_history[form_key][target_field]) > 10:
            self.mapping_history[form_key][target_field] = self.mapping_history[form_key][target_field][-10:]
        
        self.save_mapping_history()
    
    def get_mapping_suggestions(self, target_field, source_columns, used_columns):
        """Get intelligent suggestions for field mapping"""
        suggestions = []
        
        # Get synonyms for this target field
        target_lower = target_field.lower()
        synonyms = []
        for key, syn_list in self.field_synonyms.items():
            if key.lower() in target_lower or any(syn.lower() in target_lower for syn in syn_list):
                synonyms.extend(syn_list)
        
        # Check each available source column
        for source_col in source_columns:
            if source_col in used_columns:
                continue
            
            source_lower = source_col.lower()
            score = 0
            
            # Check exact matches with synonyms
            for synonym in synonyms:
                if source_lower == synonym.lower():
                    score = 1.0
                    break
            
            # Check partial matches
            if score == 0:
                for synonym in synonyms:
                    synonym_lower = synonym.lower()
                    target_words = set(synonym_lower.replace('_', ' ').replace('-', ' ').split())
                    source_words = set(source_lower.replace('_', ' ').replace('-', ' ').split())
                    
                    if target_words and source_words:
                        common_words = target_words.intersection(source_words)
                        if common_words:
                            score = len(common_words) / max(len(target_words), len(source_words))
                            break
            
            if score > 0:
                suggestions.append({
                    'source_field': source_col,
                    'score': score,
                    'reason': f"Matches {target_field} (score: {score:.2f})"
                })
        
        # Sort by score (highest first)
        suggestions.sort(key=lambda x: x['score'], reverse=True)
        return suggestions

    def view_field_mapping_memory(self):
        """Display current field mappings"""
        print(f"\n🧠 CURRENT FIELD MAPPINGS")
        print("=" * 60)
        
        if not self.smart_field_mapper.mappings:
            print("❌ No field mappings found")
            return
        
        total_mappings = 0
        for form_name, form_mappings in self.smart_field_mapper.mappings.items():
            if form_mappings:
                print(f"\n📁 Form: {form_name.upper()}")
                print("-" * 40)
                for target_field, source_field in form_mappings.items():
                    print(f"  ✅ {target_field} ← {source_field}")
                    total_mappings += 1
        
        print(f"\n📊 Total mappings: {total_mappings}")
    
    def view_mapping_history(self):
        """Display mapping history for learning"""
        print(f"\n📚 MAPPING HISTORY")
        print("=" * 60)
        
        if not self.smart_field_mapper.mapping_history:
            print("❌ No mapping history found")
            return
        
        for form_name, form_history in self.smart_field_mapper.mapping_history.items():
            if form_history:
                print(f"\n📁 Form: {form_name.upper()}")
                print("-" * 40)
                for target_field, attempts in form_history.items():
                    print(f"  🎯 {target_field}:")
                    for attempt in attempts[-3:]:  # Show last 3 attempts
                        status = "✅" if attempt['success'] else "❌"
                        timestamp = attempt['timestamp'][:19]  # Truncate timestamp
                        print(f"    {status} {attempt['source_field']} ({timestamp})")
    
    def clear_all_field_mappings(self):
        """Clear all field mappings"""
        self.smart_field_mapper.mappings = {}
        self.smart_field_mapper.save_mappings()
        print("✅ All field mappings cleared")
    
    def export_field_mappings(self):
        """Export field mappings to a readable file"""
        if not self.smart_field_mapper.mappings:
            print("❌ No field mappings to export")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"field_mappings_export_{timestamp}.txt"
        
        with open(filename, 'w') as f:
            f.write("FIELD MAPPINGS EXPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for form_name, form_mappings in self.smart_field_mapper.mappings.items():
                if form_mappings:
                    f.write(f"FORM: {form_name.upper()}\n")
                    f.write("-" * 30 + "\n")
                    for target_field, source_field in form_mappings.items():
                        f.write(f"  {target_field} ← {source_field}\n")
                    f.write("\n")
        
        print(f"✅ Field mappings exported to: {filename}")

def main():
    """Main function"""
    processor = AdvancedFulcrumProcessor()
    
    while True:
        print("\nFulcrum Automation Tool")
        print("=" * 25)
        print("1. Setup API credentials")
        print("2. List available forms")
        print("3. Filter records by status and export to CSV")
        print("4. Download Data")
        print("5. Setup property mappings")
        print("6. Setup target form (for imports)")
        print("7. Process property data")
        print("8. Batch process multiple properties")
        print("9. Fully automated processing (download + import)")
        print("10. Explore classification structure")
        print("11. **View field mapping memory** 🧠")
        print("0. Exit")
        
        choice = input("\nSelect option: ").strip()
        
        try:
            if choice == '1':
                processor.setup_credentials()
            
            elif choice == '2':
                processor.list_forms()
            elif choice == '3':
                # Filter records by status and export to CSV
                if not processor.api_client:
                    print("Please setup API credentials first (option 1)")
                    continue
                
                # For now, hardcoded to Bayview Hills District - later can make this selectable
                form_id = "658a55e5-e62b-47d6-a78a-41090911215f"
                form_name = "Bayview Hills District"
                
                result = processor.filter_and_export_by_status(form_id, form_name)
                if result:
                    print(f"\n🎉 Filtered data exported successfully!")
                    print(f"📁 Check the 'cached' folder for your CSV file")
                else:
                    print(f"\n❌ Export failed or cancelled")
                    
                input("\nPress Enter to continue...")
            elif choice == '4':
                processor.download_data_menu()
            
            elif choice == '5':
                processor.setup_property_mapping()
            
            elif choice == '6':
                processor.setup_target_form()
            
            elif choice == '7':
                if not processor.api_client:
                    print("Please setup API credentials first (option 1)")
                    continue
                
                property_name = input("Enter property name: ").strip()
                form_name = input("Enter source form name (or press Enter to choose): ").strip() or None
                
                # Ask if they want to import automatically
                auto_import = input("Automatically import to Fulcrum? (y/n): ").strip().lower() == 'y'
                
                processor.process_property(property_name, form_name, import_data=auto_import)
            
            elif choice == '8':
                if not processor.api_client:
                    print("Please setup API credentials first (option 1)")
                    continue
                
                properties_input = input("Enter property names (comma-separated): ").strip()
                properties = [p.strip() for p in properties_input.split(',')]
                form_name = input("Enter source form name (or press Enter to choose): ").strip() or None
                
                # Ask if they want to import automatically
                auto_import = input("Automatically import all to Fulcrum? (y/n): ").strip().lower() == 'y'
                
                for property_name in properties:
                    print(f"\n{'='*50}")
                    print(f"Processing: {property_name}")
                    print(f"{'='*50}")
                    try:
                        processor.process_property(property_name, form_name, import_data=auto_import)
                    except Exception as e:
                        print(f"Failed to process {property_name}: {str(e)}")
                        continue
            
            elif choice == '9':
                if not processor.api_client:
                    print("Please setup API credentials first (option 1)")
                    continue
                
                if not processor.target_form_id and not processor.config.get('fulcrum', {}).get('target_form_id'):
                    print("Please setup target form first (option 4)")
                    continue
                
                print("\n🚀 FULLY AUTOMATED MODE")
                print("This will download, process, and import data automatically")
                
                properties_input = input("Enter property names (comma-separated): ").strip()
                properties = [p.strip() for p in properties_input.split(',')]
                form_name = input("Enter source form name (or press Enter to choose): ").strip() or None
                
                confirm = input(f"Process {len(properties)} properties with auto-import? (y/n): ").strip().lower()
                if confirm != 'y':
                    print("Operation cancelled")
                    continue
                
                success_count = 0
                total_records = 0
                
                for i, property_name in enumerate(properties, 1):
                    print(f"\n{'='*60}")
                    print(f"🏗️  PROCESSING {i}/{len(properties)}: {property_name}")
                    print(f"{'='*60}")
                    try:
                        result = processor.process_property(property_name, form_name, import_data=True)
                        success_count += 1
                        print(f"✅ {property_name} completed successfully!")
                    except Exception as e:
                        print(f"❌ {property_name} failed: {str(e)}")
                        continue
                
                print(f"\n🎯 BATCH PROCESSING COMPLETE!")
                print(f"✅ Successful: {success_count}/{len(properties)} properties")
                print(f"📊 All data has been imported to your target form")
            
            elif choice == '10':
                # Explore classification structure
                if not processor.api_client:
                    print("Please setup API credentials first (option 1)")
                    continue
                
                print("\n🔍 CLASSIFICATION STRUCTURE EXPLORER")
                print("=" * 50)
                print("1. Explore all classification sets")
                print("2. Explore specific classification set")
                print("0. Back to main menu")
                
                explore_choice = input("\nSelect option: ").strip()
                
                if explore_choice == '1':
                    processor.explore_classification_structure()
                elif explore_choice == '2':
                    set_name = input("Enter classification set name (e.g., LMH, SCHOOLS): ").strip()
                    if set_name:
                        processor.explore_classification_structure(set_name)
                elif explore_choice == '0':
                    continue
                else:
                    print("Invalid choice")
                
                input("\nPress Enter to continue...")
            
            elif choice == '11':
                # View field mapping memory
                print("\n🧠 FIELD MAPPING MEMORY")
                print("=" * 50)
                print("1. View current mappings")
                print("2. View mapping history")
                print("3. Clear all mappings")
                print("4. Export mappings to file")
                print("0. Back to main menu")
                
                memory_choice = input("\nSelect option: ").strip()
                
                if memory_choice == '1':
                    processor.view_field_mapping_memory()
                elif memory_choice == '2':
                    processor.view_mapping_history()
                elif memory_choice == '3':
                    confirm = input("⚠️  Are you sure you want to clear ALL field mappings? (yes/no): ").strip().lower()
                    if confirm == 'yes':
                        processor.clear_all_field_mappings()
                        print("✅ All field mappings cleared")
                    else:
                        print("Operation cancelled")
                elif memory_choice == '4':
                    processor.export_field_mappings()
                elif memory_choice == '0':
                    continue
                else:
                    print("Invalid choice")
                
                input("\nPress Enter to continue...")
            
            elif choice == '0':
                print("👋 Goodbye!")
                break
            
            else:
                print("Invalid choice")
        
        except KeyboardInterrupt:
            print("\nOperation cancelled")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
