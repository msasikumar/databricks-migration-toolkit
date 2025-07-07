# Databricks Table Migration Guide

This guide provides complete instructions for migrating data between Databricks environments using Python scripts and the Databricks API.

## Overview

This solution migrates data from a source Databricks table to a target environment by:
1. Exporting data as Parquet files
2. Transferring files between environments
3. Importing data to create a new table
4. Verifying the migration

## Prerequisites

- Python 3.x installed
- Databricks CLI configured with `.databrickscfg` file
- Access to both source and target Databricks environments
- SQL Warehouse available in target environment

## Files Included

| File | Purpose |
|------|---------|
| `databricks_export_to_parquet.py` | Export notebook - exports table data to Parquet files |
| `transfer_parquet_files.py` | Transfer script - moves files between environments |
| `run_import_via_api.py` | Import script - creates table from Parquet files |
| `verify_import.py` | Verification script - confirms successful migration |

## Step-by-Step Instructions

### Step 1: Configure Databricks Access

Ensure your `.databrickscfg` file is properly configured with both environments:

```ini
[stage]
host=https://your-stage-workspace.cloud.databricks.com/
token=dapi...

[dev]
host=https://your-dev-workspace.cloud.databricks.com/
token=dapi...
```

**Location**: `~/.databrickscfg`

### Step 2: Export Data from Source Environment

Run the export notebook in your **source environment** (e.g., stage):

```python
# In Databricks notebook or via API
python3 databricks_export_to_parquet.py
```

**What it does:**
- Queries the source table: `ops_common.jules_ewo_instances`
- Exports data as Parquet files to: `/dbfs/FileStore/exports/jules_ewo_instances/YYYYMMDD_HHMMSS/`
- Creates timestamped directory for organization
- Includes data validation and error handling

**Expected Output:**
```
✅ Export completed successfully!
📁 Files saved to: /dbfs/FileStore/exports/jules_ewo_instances/20250707_224455
📊 Total rows exported: 1,846
```

### Step 3: Transfer Files Between Environments

Run the transfer script from your **local terminal**:

```bash
python3 transfer_parquet_files.py
```

**What it does:**
- Reads your `.databrickscfg` file automatically
- Connects to both [stage] and [dev] environments
- Transfers Parquet files from stage to dev environment
- Creates destination directory structure

**Expected Output:**
```
🚀 Starting Parquet file transfer from stage to dev environment
✅ Stage environment: Connected
✅ Dev environment: Connected
📄 Transferring file: part-00000-tid-xxx.snappy.parquet (90MB)
✅ Successfully transferred: part-00000-tid-xxx.snappy.parquet
📊 Transfer summary: 4/6 files transferred successfully
🎯 TRANSFER COMPLETED SUCCESSFULLY!
```

### Step 4: Import Data to Target Environment

Run the import script from your **local terminal**:

```bash
python3 run_import_via_api.py
```

**What it does:**
- Uses [dev] profile from `.databrickscfg`
- Connects to available SQL Warehouse
- Creates database and schema: `central_dev.test`
- Creates table: `central_dev.test.jules_ewo_instances`
- Imports all Parquet data

**Expected Output:**
```
🚀 Starting import process via Databricks API...
✅ Using SQL warehouse: $$$$ DQ_Warehouse
🏗️ Step 1: Creating database and schema...
✅ Success
📖 Step 2: Reading Parquet data...
✅ Temporary view created successfully
🚀 Step 4: Creating target table...
✅ Table created successfully via direct approach
✅ Final verification: 1,846 rows in target table
🎯 IMPORT COMPLETED SUCCESSFULLY!
```

### Step 5: Verify Migration

Run the verification script to confirm success:

```bash
python3 verify_import.py
```

**What it does:**
- Connects to target environment
- Runs validation queries on the new table
- Shows row count, schema, and sample data
- Confirms table accessibility

**Expected Output:**
```
🔍 Verifying imported table: central_dev.test.jules_ewo_instances
📊 Row Count: 1,846
📊 Table Schema: [shows all columns]
✅ Table is accessible and contains data
```

## Configuration Options

### Customizing Paths

Edit the scripts to change default paths:

```python
# In transfer_parquet_files.py
source_path = "/dbfs/FileStore/exports/jules_ewo_instances/YYYYMMDD_HHMMSS"
dest_path = "/dbfs/FileStore/imports/jules_ewo_instances/YYYYMMDD_HHMMSS"

# In run_import_via_api.py
IMPORT_PATH = "/dbfs/FileStore/imports/jules_ewo_instances/YYYYMMDD_HHMMSS"
TARGET_DATABASE = "central_dev"
TARGET_SCHEMA = "test"
TABLE_NAME = "jules_ewo_instances"
```

### Customizing Source Query

Edit the export notebook to change the source query:

```python
# In databricks_export_to_parquet.py
SOURCE_QUERY = """
    SELECT * FROM your_database.your_schema.your_table 
    ORDER BY created_on_pst DESC
"""
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify `.databrickscfg` file exists and has correct tokens
   - Check token permissions for both environments

2. **Path Not Found Errors**
   - Ensure export completed successfully before transfer
   - Check timestamp in path matches actual export timestamp

3. **SQL Warehouse Issues**
   - Ensure SQL Warehouse is running in target environment
   - Check warehouse permissions

4. **Transfer Failures**
   - Large files may timeout - script handles retries automatically
   - Empty files (_SUCCESS, _started_*) may fail but are not critical

### Debug Commands

Check file paths:
```bash
# List exports directory
python3 -c "
import requests, configparser, os
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.databrickscfg'))
host = config['stage']['host']
token = config['stage']['token']
response = requests.get(f'{host}/api/2.0/dbfs/list', 
                       headers={'Authorization': f'Bearer {token}'}, 
                       params={'path': '/dbfs/FileStore/exports'})
print(response.json())
"
```

Check table exists:
```bash
# Verify table in dev environment
python3 -c "
import requests, configparser, os
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.databrickscfg'))
host = config['dev']['host']
token = config['dev']['token']
# Add SQL query to check table
"
```

## Success Criteria

✅ **Export**: Parquet files created in source environment  
✅ **Transfer**: Files copied to target environment  
✅ **Import**: Table created with all data  
✅ **Verification**: Table accessible and queryable  

## Final Query

After successful migration, query your data:

```sql
-- Basic count
SELECT COUNT(*) FROM central_dev.test.jules_ewo_instances;

-- Recent records
SELECT * FROM central_dev.test.jules_ewo_instances 
ORDER BY created_on_pst DESC LIMIT 10;

-- Data quality check
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT workorder_instance_id) as unique_ids,
       MIN(created_on_pst) as earliest_date,
       MAX(created_on_pst) as latest_date
FROM central_dev.test.jules_ewo_instances;
```

## Notes

- All scripts use your existing `.databrickscfg` profiles
- No manual file uploads required
- Process can be automated for regular migrations
- Timestamps ensure no data conflicts
- Delta Lake format provides ACID transactions

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify all prerequisites are met
3. Run verification script to confirm current state
4. Check Databricks workspace logs for detailed errors

---

**Migration completed successfully!** 🎉

Your data is now available in the target environment and ready for use.
