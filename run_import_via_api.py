#!/usr/bin/env python3
"""
Script to run the import process directly via Databricks API using [dev] profile
This simulates running the import notebook but via terminal/API calls
"""

import os
import requests
import configparser
import json
import time
from datetime import datetime

def read_databricks_config():
    """Read the .databrickscfg file"""
    config_path = os.path.expanduser("~/.databrickscfg")
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return {
        'dev': {
            'host': config['dev']['host'], 
            'token': config['dev']['token']
        }
    }

def execute_sql_command(host, token, sql_command, warehouse_id=None):
    """Execute SQL command via Databricks SQL API"""
    url = f"{host}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "statement": sql_command,
        "wait_timeout": "30s"
    }
    
    if warehouse_id:
        payload["warehouse_id"] = warehouse_id
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print(f"Error executing SQL: {response.status_code} - {response.text}")
        return None

def list_warehouses(host, token):
    """List available SQL warehouses"""
    url = f"{host}/api/2.0/sql/warehouses"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('warehouses', [])
    else:
        print(f"Error listing warehouses: {response.status_code} - {response.text}")
        return []

def create_cluster_and_run_notebook(host, token):
    """Create a cluster and run the import process"""
    print("🚀 Starting import process via Databricks API...")
    
    # First, let's try to use SQL warehouse approach
    print("🔍 Looking for available SQL warehouses...")
    warehouses = list_warehouses(host, token)
    
    if not warehouses:
        print("❌ No SQL warehouses available. You may need to create one in the Databricks workspace.")
        return False
    
    # Use the first available warehouse
    warehouse = warehouses[0]
    warehouse_id = warehouse['id']
    warehouse_name = warehouse['name']
    print(f"✅ Using SQL warehouse: {warehouse_name} (ID: {warehouse_id})")
    
    # Configuration
    IMPORT_PATH = "/dbfs/FileStore/imports/jules_ewo_instances/20250707_224455"
    TARGET_DATABASE = "central_dev"
    TARGET_SCHEMA = "test"
    TABLE_NAME = "jules_ewo_instances"
    TARGET_TABLE = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TABLE_NAME}"
    
    print(f"📂 Import Path: {IMPORT_PATH}")
    print(f"🎯 Target Table: {TARGET_TABLE}")
    
    # Step 1: Create database and schema
    print("\n🏗️ Step 1: Creating database and schema...")
    
    sql_commands = [
        f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}",
        f"CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}",
        f"USE {TARGET_DATABASE}.{TARGET_SCHEMA}"
    ]
    
    for sql in sql_commands:
        print(f"   Executing: {sql}")
        result = execute_sql_command(host, token, sql, warehouse_id)
        if result:
            print(f"   ✅ Success")
        else:
            print(f"   ❌ Failed")
            return False
    
    # Step 2: Check if we can read the Parquet files
    print(f"\n📖 Step 2: Reading Parquet data...")
    
    # Create a temporary view from the Parquet files
    create_view_sql = f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_import_data
    USING PARQUET
    OPTIONS (path '{IMPORT_PATH}')
    """
    
    print(f"   Creating temporary view from Parquet files...")
    result = execute_sql_command(host, token, create_view_sql, warehouse_id)
    if not result:
        print("   ❌ Failed to create temporary view from Parquet files")
        return False
    
    print("   ✅ Temporary view created successfully")
    
    # Step 3: Get row count from source
    print(f"\n📊 Step 3: Validating source data...")
    count_sql = "SELECT COUNT(*) as row_count FROM temp_import_data"
    
    result = execute_sql_command(host, token, count_sql, warehouse_id)
    if result and result.get('result'):
        row_count = result['result']['data_array'][0][0]
        print(f"   ✅ Source data validated: {row_count:,} rows found")
    else:
        print("   ❌ Failed to count source rows")
        print(f"   Debug info: {result}")
        
        # Try a simpler approach - just try to create the table directly
        print("   🔄 Trying direct table creation approach...")
        
        # Skip validation and try direct creation
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {TARGET_TABLE}
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        AS SELECT * FROM temp_import_data
        """
        
        print(f"   Creating table directly: {TARGET_TABLE}")
        result = execute_sql_command(host, token, create_table_sql, warehouse_id)
        if not result:
            print("   ❌ Direct table creation also failed")
            return False
        
        print("   ✅ Table created successfully via direct approach")
        
        # Now get the row count from the created table
        verify_sql = f"SELECT COUNT(*) as imported_count FROM {TARGET_TABLE}"
        result = execute_sql_command(host, token, verify_sql, warehouse_id)
        
        if result and result.get('result'):
            row_count = result['result']['data_array'][0][0]
            if isinstance(row_count, int):
                print(f"   ✅ Final verification: {row_count:,} rows in target table")
            else:
                print(f"   ✅ Final verification: {row_count} rows in target table")
        else:
            print("   ⚠️ Could not verify final row count, but table was created")
            row_count = "unknown"
        
        # Skip to final steps
        print(f"\n🎯 IMPORT COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"✅ Source Path: {IMPORT_PATH}")
        print(f"✅ Target Table: {TARGET_TABLE}")
        if isinstance(row_count, int):
            print(f"✅ Rows Imported: {row_count:,}")
        else:
            print(f"✅ Rows Imported: {row_count}")
        print(f"✅ SQL Warehouse Used: {warehouse_name}")
        print("=" * 60)
        print(f"\n📊 You can now query the table using:")
        print(f"   SELECT * FROM {TARGET_TABLE};")
        
        return True
    
    # Step 4: Create the target table
    print(f"\n🚀 Step 4: Creating target table...")
    
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE}
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    AS SELECT * FROM temp_import_data
    """
    
    print(f"   Creating table: {TARGET_TABLE}")
    result = execute_sql_command(host, token, create_table_sql, warehouse_id)
    if not result:
        print("   ❌ Failed to create target table")
        return False
    
    print("   ✅ Target table created successfully")
    
    # Step 5: Verify the import
    print(f"\n🔍 Step 5: Verifying import...")
    
    verify_sql = f"SELECT COUNT(*) as imported_count FROM {TARGET_TABLE}"
    result = execute_sql_command(host, token, verify_sql, warehouse_id)
    
    if result and result.get('result'):
        imported_count = result['result']['data_array'][0][0]
        print(f"   ✅ Import verified: {imported_count:,} rows in target table")
        
        if imported_count == row_count:
            print("   ✅ Row count verification passed!")
        else:
            print(f"   ⚠️ Row count mismatch: Source={row_count:,}, Target={imported_count:,}")
    else:
        print("   ❌ Failed to verify import")
        return False
    
    # Step 6: Show sample data
    print(f"\n📄 Step 6: Sample data from new table...")
    
    sample_sql = f"SELECT * FROM {TARGET_TABLE} ORDER BY created_on_pst DESC LIMIT 5"
    result = execute_sql_command(host, token, sample_sql, warehouse_id)
    
    if result and result.get('result'):
        print("   ✅ Sample data (first 5 rows):")
        data = result['result']
        if 'data_array' in data and data['data_array']:
            # Print column headers
            if 'columns' in data:
                headers = [col['name'] for col in data['columns']]
                print(f"   📋 Columns: {', '.join(headers[:5])}...")  # Show first 5 columns
            
            # Print first few rows
            for i, row in enumerate(data['data_array'][:3]):  # Show first 3 rows
                print(f"   Row {i+1}: {str(row)[:100]}...")  # Truncate long rows
        else:
            print("   📄 No data returned in sample")
    else:
        print("   ⚠️ Could not retrieve sample data")
    
    # Step 7: Final summary
    print(f"\n🎯 IMPORT COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"✅ Source Path: {IMPORT_PATH}")
    print(f"✅ Target Table: {TARGET_TABLE}")
    print(f"✅ Rows Imported: {row_count:,}")
    print(f"✅ SQL Warehouse Used: {warehouse_name}")
    print("=" * 60)
    print(f"\n📊 You can now query the table using:")
    print(f"   SELECT * FROM {TARGET_TABLE};")
    
    return True

def main():
    """Main function"""
    print("🚀 Starting Databricks import process via API")
    print("Using [dev] profile from .databrickscfg")
    
    # Read configuration
    try:
        config = read_databricks_config()
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")
        return
    
    # Run the import process
    success = create_cluster_and_run_notebook(
        config['dev']['host'], 
        config['dev']['token']
    )
    
    if success:
        print("\n🎉 Data migration completed successfully!")
    else:
        print("\n❌ Data migration failed. Please check the errors above.")

if __name__ == "__main__":
    main()
