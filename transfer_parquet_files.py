#!/usr/bin/env python3
"""
Script to transfer Parquet files from stage to dev environment using Databricks API
"""

import os
import requests
import configparser
import base64
import json
from pathlib import Path

def read_databricks_config():
    """Read the .databrickscfg file"""
    config_path = os.path.expanduser("~/.databrickscfg")
    config = configparser.ConfigParser()
    config.read(config_path)
    
    return {
        'stage': {
            'host': config['stage']['host'],
            'token': config['stage']['token']
        },
        'dev': {
            'host': config['dev']['host'], 
            'token': config['dev']['token']
        }
    }

def list_dbfs_files(host, token, path):
    """List files in DBFS path"""
    url = f"{host}/api/2.0/dbfs/list"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"path": path}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error listing files: {response.status_code} - {response.text}")
        return None

def read_dbfs_file(host, token, path):
    """Read a file from DBFS"""
    url = f"{host}/api/2.0/dbfs/read"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"path": path}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()['data']
    else:
        print(f"Error reading file {path}: {response.status_code} - {response.text}")
        return None

def write_dbfs_file(host, token, path, data):
    """Write a file to DBFS"""
    # First, create the file
    url = f"{host}/api/2.0/dbfs/create"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "path": path,
        "overwrite": True
    }
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"Error creating file {path}: {response.status_code} - {response.text}")
        return False
    
    handle = response.json()['handle']
    
    # Add data to the file
    url = f"{host}/api/2.0/dbfs/add-block"
    payload = {
        "handle": handle,
        "data": data
    }
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"Error writing data to {path}: {response.status_code} - {response.text}")
        return False
    
    # Close the file
    url = f"{host}/api/2.0/dbfs/close"
    payload = {"handle": handle}
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        print(f"Error closing file {path}: {response.status_code} - {response.text}")
        return False
    
    return True

def create_dbfs_directory(host, token, path):
    """Create a directory in DBFS"""
    url = f"{host}/api/2.0/dbfs/mkdirs"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"path": path}
    
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return True
    else:
        print(f"Error creating directory {path}: {response.status_code} - {response.text}")
        return False

def transfer_files_recursive(stage_config, dev_config, source_path, dest_path):
    """Transfer files recursively from stage to dev"""
    print(f"📂 Transferring from {source_path} to {dest_path}")
    
    # List files in source
    files_info = list_dbfs_files(stage_config['host'], stage_config['token'], source_path)
    if not files_info:
        return False
    
    # Create destination directory
    create_dbfs_directory(dev_config['host'], dev_config['token'], dest_path)
    
    success_count = 0
    total_files = len(files_info.get('files', []))
    
    for file_info in files_info.get('files', []):
        file_path = file_info['path']
        file_name = os.path.basename(file_path)
        dest_file_path = f"{dest_path}/{file_name}"
        
        if file_info['is_dir']:
            # Recursively transfer directory
            print(f"📁 Processing directory: {file_name}")
            if transfer_files_recursive(stage_config, dev_config, file_path, dest_file_path):
                success_count += 1
        else:
            # Transfer file
            print(f"📄 Transferring file: {file_name} ({file_info['file_size']} bytes)")
            
            # Read file from stage
            file_data = read_dbfs_file(stage_config['host'], stage_config['token'], file_path)
            if file_data:
                # Write file to dev
                if write_dbfs_file(dev_config['host'], dev_config['token'], dest_file_path, file_data):
                    print(f"✅ Successfully transferred: {file_name}")
                    success_count += 1
                else:
                    print(f"❌ Failed to transfer: {file_name}")
            else:
                print(f"❌ Failed to read: {file_name}")
    
    print(f"📊 Transfer summary: {success_count}/{total_files} files transferred successfully")
    return success_count == total_files

def main():
    """Main function"""
    print("🚀 Starting Parquet file transfer from stage to dev environment")
    
    # Read configuration
    try:
        config = read_databricks_config()
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")
        return
    
    # Define paths
    source_path = "/dbfs/FileStore/exports/jules_ewo_instances/20250707_224455"
    dest_path = "/dbfs/FileStore/imports/jules_ewo_instances/20250707_224455"
    
    print(f"📂 Source: {source_path}")
    print(f"📂 Destination: {dest_path}")
    
    # Test connectivity to both environments
    print("\n🔍 Testing connectivity...")
    
    stage_test = list_dbfs_files(config['stage']['host'], config['stage']['token'], "/FileStore")
    if stage_test:
        print("✅ Stage environment: Connected")
    else:
        print("❌ Stage environment: Connection failed")
        return
    
    dev_test = list_dbfs_files(config['dev']['host'], config['dev']['token'], "/FileStore")
    if dev_test:
        print("✅ Dev environment: Connected")
    else:
        print("❌ Dev environment: Connection failed")
        return
    
    # Check if source exists
    print(f"\n🔍 Checking source path: {source_path}")
    source_files = list_dbfs_files(config['stage']['host'], config['stage']['token'], source_path)
    if not source_files:
        print("❌ Source path not found or empty")
        return
    
    file_count = len(source_files.get('files', []))
    print(f"✅ Found {file_count} files/folders in source")
    
    # Start transfer
    print(f"\n🔄 Starting transfer...")
    success = transfer_files_recursive(config['stage'], config['dev'], source_path, dest_path)
    
    if success:
        print("\n🎯 TRANSFER COMPLETED SUCCESSFULLY!")
        print(f"📁 Files are now available at: {dest_path}")
        print("🔄 You can now run the import notebook with this path")
    else:
        print("\n❌ Transfer completed with errors")

if __name__ == "__main__":
    main()
