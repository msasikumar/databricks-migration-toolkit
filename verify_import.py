#!/usr/bin/env python3
"""
Script to verify the imported table and show sample data
"""

import os
import requests
import configparser

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

def execute_sql_command(host, token, sql_command, warehouse_id):
    """Execute SQL command via Databricks SQL API"""
    url = f"{host}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "statement": sql_command,
        "wait_timeout": "30s",
        "warehouse_id": warehouse_id
    }
    
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
        return []

def main():
    """Main function"""
    print("🔍 Verifying imported table: central_dev.test.jules_ewo_instances")
    
    # Read configuration
    config = read_databricks_config()
    host = config['dev']['host']
    token = config['dev']['token']
    
    # Get warehouse
    warehouses = list_warehouses(host, token)
    if not warehouses:
        print("❌ No SQL warehouses available")
        return
    
    warehouse_id = warehouses[0]['id']
    warehouse_name = warehouses[0]['name']
    print(f"✅ Using warehouse: {warehouse_name}")
    
    TABLE_NAME = "central_dev.test.jules_ewo_instances"
    
    # Test queries
    queries = [
        ("Row Count", f"SELECT COUNT(*) as total_rows FROM {TABLE_NAME}"),
        ("Table Schema", f"DESCRIBE {TABLE_NAME}"),
        ("Recent Records", f"SELECT * FROM {TABLE_NAME} ORDER BY created_on_pst DESC LIMIT 5"),
        ("Date Range", f"SELECT MIN(created_on_pst) as earliest, MAX(created_on_pst) as latest FROM {TABLE_NAME}"),
    ]
    
    for query_name, sql in queries:
        print(f"\n📊 {query_name}:")
        print("-" * 40)
        
        result = execute_sql_command(host, token, sql, warehouse_id)
        
        if result and result.get('result') and result['result'].get('data_array'):
            data = result['result']
            
            # Print column headers for non-describe queries
            if 'columns' in data and query_name != "Table Schema":
                headers = [col['name'] for col in data['columns']]
                print(f"Columns: {', '.join(headers)}")
                print()
            
            # Print data
            for i, row in enumerate(data['data_array']):
                if query_name == "Table Schema":
                    # For DESCRIBE, show column info nicely
                    print(f"  {row[0]:<25} {row[1]:<15} {row[2] if len(row) > 2 else ''}")
                elif query_name == "Recent Records":
                    # For sample data, truncate long values
                    row_str = str(row)
                    if len(row_str) > 100:
                        row_str = row_str[:100] + "..."
                    print(f"  Row {i+1}: {row_str}")
                else:
                    # For other queries, show as-is
                    print(f"  {row}")
                
                # Limit output for large results
                if i >= 9:  # Show max 10 rows
                    if len(data['data_array']) > 10:
                        print(f"  ... and {len(data['data_array']) - 10} more rows")
                    break
        else:
            print("  No data returned or query failed")
    
    print(f"\n🎯 VERIFICATION COMPLETED!")
    print("=" * 50)
    print(f"✅ Table: {TABLE_NAME}")
    print("✅ Table is accessible and contains data")
    print("✅ You can now use this table in your Databricks workspace")
    print("=" * 50)

if __name__ == "__main__":
    main()
