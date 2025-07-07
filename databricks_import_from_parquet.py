# Databricks notebook source
# MAGIC %md
# MAGIC # Import Parquet Data and Create Table
# MAGIC 
# MAGIC This notebook imports Parquet data from S3 and creates the table `central_dev.test.jules_ewo_instances`.
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Source: S3 Parquet files
# MAGIC - S3 Path: `s3://bdf-cape-dtlk-stage/warehouse/sandbox_silver/transfer/jules_ewo_instances/`
# MAGIC - Destination Table: `central_dev.test.jules_ewo_instances`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

# Configuration parameters - modify these as needed
# UPDATE THIS PATH with the actual import path after transferring from stage to dev
IMPORT_PATH = "/dbfs/FileStore/imports/jules_ewo_instances/20250707_224455"  # Path in dev environment after transfer

# Alternative paths (uncomment the one you need):
# IMPORT_PATH = "s3://bdf-cape-dtlk-stage/warehouse/sandbox_silver/transfer/jules_ewo_instances/20250707_224455"  # S3 path
# IMPORT_PATH = "/dbfs/FileStore/exports/jules_ewo_instances/20250707_224455"  # Direct from stage (if same cluster)

TABLE_NAME = "jules_ewo_instances"
TARGET_DATABASE = "central_dev"
TARGET_SCHEMA = "test"
TARGET_TABLE = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TABLE_NAME}"

print(f"Target Table: {TARGET_TABLE}")
print(f"Import Path: {IMPORT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Initialize Spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("Import_Jules_EWO_Instances_from_Parquet") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

print("Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Import Path

# COMMAND ----------

# Extract timestamp from path for logging
import re
timestamp_match = re.search(r'(\d{8}_\d{6})', IMPORT_PATH)
actual_timestamp = timestamp_match.group(1) if timestamp_match else "unknown"

print(f"📂 Using Import Path: {IMPORT_PATH}")
print(f"⏰ Detected Timestamp: {actual_timestamp}")

# Verify the path exists by trying to list files
try:
    if IMPORT_PATH.startswith('s3://'):
        # For S3 paths, try to list files
        test_files = dbutils.fs.ls(IMPORT_PATH)
        print(f"✅ Path verified - found {len(test_files)} files/folders")
    else:
        # For DBFS paths, try to list files
        test_files = dbutils.fs.ls(IMPORT_PATH.replace('/dbfs', 'dbfs:'))
        print(f"✅ Path verified - found {len(test_files)} files/folders")
except Exception as e:
    print(f"⚠️ Could not verify path: {e}")
    print("📝 Please ensure the path is correct and accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Validate Parquet Data

# COMMAND ----------

try:
    print(f"📖 Reading Parquet data from: {IMPORT_PATH}")
    
    # Read Parquet files from the specified path
    df = spark.read.parquet(IMPORT_PATH)
    
    # Cache the dataframe for multiple operations
    df.cache()
    
    # Get basic statistics
    row_count = df.count()
    column_count = len(df.columns)
    
    print(f"✅ Data loaded successfully!")
    print(f"📊 Row count: {row_count:,}")
    print(f"📊 Column count: {column_count}")
    
    # Display schema
    print("\n📋 Schema:")
    df.printSchema()
    
    # Show sample data
    print("\n📄 Sample data (first 5 rows):")
    df.show(5, truncate=False)
    
    if row_count == 0:
        raise ValueError("No data found in Parquet files!")
        
    # Validate data quality
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print("\n🔍 Null value counts per column:")
    null_counts.show()
        
except Exception as e:
    logger.error(f"❌ Error reading Parquet data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database and Schema if Not Exists

# COMMAND ----------

try:
    # Create database if it doesn't exist
    print(f"🏗️ Creating database '{TARGET_DATABASE}' if not exists...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DATABASE}")
    print(f"✅ Database '{TARGET_DATABASE}' ready")
    
    # Create schema if it doesn't exist (for 3-level namespace)
    print(f"🏗️ Creating schema '{TARGET_DATABASE}.{TARGET_SCHEMA}' if not exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_DATABASE}.{TARGET_SCHEMA}")
    print(f"✅ Schema '{TARGET_DATABASE}.{TARGET_SCHEMA}' ready")
    
    # Verify we can access the schema
    spark.sql(f"USE {TARGET_DATABASE}.{TARGET_SCHEMA}")
    print(f"✅ Successfully switched to schema '{TARGET_DATABASE}.{TARGET_SCHEMA}'")
    
except Exception as e:
    logger.error(f"❌ Error creating database/schema: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if Target Table Exists

# COMMAND ----------

def table_exists(database, schema, table):
    """Check if a table exists"""
    try:
        spark.sql(f"DESCRIBE TABLE {database}.{schema}.{table}")
        return True
    except:
        return False

# Check if target table exists
table_exists_flag = table_exists(TARGET_DATABASE, TARGET_SCHEMA, TABLE_NAME)

if table_exists_flag:
    print(f"⚠️ Table '{TARGET_TABLE}' already exists")
    
    # Get existing table info
    existing_df = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_TABLE}")
    existing_count = existing_df.collect()[0]['count']
    print(f"📊 Existing table row count: {existing_count:,}")
    
    # Show existing schema
    print(f"\n📋 Existing table schema:")
    spark.sql(f"DESCRIBE {TARGET_TABLE}").show(truncate=False)
    
else:
    print(f"✅ Table '{TARGET_TABLE}' does not exist - will create new table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Replace Table with Imported Data

# COMMAND ----------

try:
    print(f"🚀 Creating table '{TARGET_TABLE}' from Parquet data...")
    
    # Create a temporary view from the DataFrame
    temp_view_name = "temp_jules_ewo_instances_import"
    df.createOrReplaceTempView(temp_view_name)
    
    # Create table using CREATE TABLE AS SELECT (CTAS)
    # This approach automatically infers schema from the Parquet data
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE}
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    AS SELECT * FROM {temp_view_name}
    """
    
    print("📝 Executing CREATE TABLE statement...")
    spark.sql(create_table_sql)
    
    print("✅ Table created successfully!")
    
    # Verify the table creation
    verification_df = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_TABLE}")
    imported_row_count = verification_df.collect()[0]['count']
    
    print(f"🔍 Verification:")
    print(f"   Source rows: {row_count:,}")
    print(f"   Imported rows: {imported_row_count:,}")
    
    if row_count == imported_row_count:
        print("✅ Row count verification passed!")
    else:
        raise ValueError(f"Row count mismatch! Source: {row_count}, Imported: {imported_row_count}")
        
except Exception as e:
    logger.error(f"❌ Error creating table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Information and Statistics

# COMMAND ----------

try:
    print("📊 Final Table Information:")
    print("=" * 60)
    
    # Table details
    print(f"Table Name: {TARGET_TABLE}")
    print(f"Source Path: {IMPORT_PATH}")
    print(f"Import Timestamp: {actual_timestamp}")
    
    # Row count
    final_count = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_TABLE}").collect()[0]['count']
    print(f"Total Rows: {final_count:,}")
    
    # Schema information
    print(f"\n📋 Table Schema:")
    spark.sql(f"DESCRIBE {TARGET_TABLE}").show(truncate=False)
    
    # Table properties
    print(f"\n🏷️ Table Properties:")
    try:
        spark.sql(f"SHOW TBLPROPERTIES {TARGET_TABLE}").show(truncate=False)
    except:
        print("Table properties not available")
    
    # Sample data from the new table
    print(f"\n📄 Sample data from new table (first 5 rows):")
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
    
    # Basic statistics
    print(f"\n📈 Basic Statistics:")
    try:
        # Get min/max for date columns if they exist
        date_columns = [field.name for field in df.schema.fields 
                       if 'date' in field.name.lower() or 'time' in field.name.lower()]
        
        if date_columns:
            for col_name in date_columns[:3]:  # Limit to first 3 date columns
                try:
                    stats = spark.sql(f"SELECT MIN({col_name}) as min_val, MAX({col_name}) as max_val FROM {TARGET_TABLE}").collect()[0]
                    print(f"   {col_name}: {stats['min_val']} to {stats['max_val']}")
                except:
                    pass
    except:
        print("   Statistics not available")
    
    print("=" * 60)
    
    # Log completion
    logger.info(f"Import completed successfully. {final_count:,} rows imported to {TARGET_TABLE}")
    
except Exception as e:
    logger.warning(f"Could not retrieve table statistics: {str(e)}")
    print("✅ Import completed, but statistics unavailable")

finally:
    # Unpersist cached dataframe to free memory
    df.unpersist()
    print("🧹 Cleanup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Queries

# COMMAND ----------

print("🔍 Running validation queries...")

# Query 1: Basic count and recent records
print("\n1️⃣ Recent records (top 10 by created_on_pst):")
try:
    spark.sql(f"""
        SELECT * FROM {TARGET_TABLE} 
        ORDER BY created_on_pst DESC 
        LIMIT 10
    """).show(truncate=False)
except Exception as e:
    print(f"Could not run recent records query: {e}")

# Query 2: Data distribution by date
print("\n2️⃣ Data distribution by date:")
try:
    spark.sql(f"""
        SELECT DATE(created_on_pst) as date, COUNT(*) as record_count
        FROM {TARGET_TABLE}
        GROUP BY DATE(created_on_pst)
        ORDER BY date DESC
        LIMIT 10
    """).show()
except Exception as e:
    print(f"Could not run date distribution query: {e}")

# Query 3: Verify the table is accessible
print("\n3️⃣ Table accessibility test:")
try:
    test_count = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE}").collect()[0][0]
    print(f"✅ Table is accessible with {test_count:,} records")
except Exception as e:
    print(f"❌ Table accessibility issue: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎯 IMPORT COMPLETED SUCCESSFULLY!")
print("=" * 50)
print(f"✅ Source: {IMPORT_PATH}")
print(f"✅ Target Table: {TARGET_TABLE}")
print(f"✅ Rows Imported: {row_count:,}")
print(f"✅ Import Timestamp: {actual_timestamp}")
print("=" * 50)
print("\n🔄 Data migration completed successfully!")
print(f"📊 You can now query the table using: SELECT * FROM {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Verify Data**: Run queries against the new table to ensure data integrity
# MAGIC 2. **Performance**: Consider adding indexes or partitioning if needed for large datasets
# MAGIC 3. **Permissions**: Set up appropriate access permissions for the new table
# MAGIC 4. **Documentation**: Update your data catalog with the new table information
# MAGIC 
# MAGIC **Sample Queries to Test:**
# MAGIC ```sql
# MAGIC -- Basic count
# MAGIC SELECT COUNT(*) FROM central_dev.test.jules_ewo_instances;
# MAGIC 
# MAGIC -- Recent records
# MAGIC SELECT * FROM central_dev.test.jules_ewo_instances 
# MAGIC ORDER BY created_on_pst DESC LIMIT 10;
# MAGIC 
# MAGIC -- Data quality check
# MAGIC SELECT COUNT(*) as total_records,
# MAGIC        COUNT(DISTINCT id) as unique_ids,
# MAGIC        MIN(created_on_pst) as earliest_date,
# MAGIC        MAX(created_on_pst) as latest_date
# MAGIC FROM central_dev.test.jules_ewo_instances;
