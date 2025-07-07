# Databricks notebook source
# MAGIC %md
# MAGIC # Export Data to Parquet
# MAGIC 
# MAGIC This notebook exports data from `ops_common.jules_ewo_instances` table to S3 as Parquet format.
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Source Table: `ops_common.jules_ewo_instances`
# MAGIC - Destination: S3 Parquet files
# MAGIC - S3 Path: `s3://bdf-cape-dtlk-stage/warehouse/sandbox_silver/transfer/jules_ewo_instances/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

# Configuration parameters - modify these as needed
SOURCE_TABLE = "ops_common.jules_ewo_instances"
TABLE_NAME = "jules_ewo_instances"

# Generate timestamp for versioning
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Use DBFS path instead of direct S3 access
DBFS_EXPORT_PATH = f"/dbfs/FileStore/exports/{TABLE_NAME}/{timestamp}"
S3_TARGET_PATH = f"s3://bdf-cape-dtlk-stage/warehouse/sandbox_silver/transfer/{TABLE_NAME}/{timestamp}"

print(f"Source Table: {SOURCE_TABLE}")
print(f"DBFS Export Path: {DBFS_EXPORT_PATH}")
print(f"Target S3 Path: {S3_TARGET_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Initialize Spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("Export_Jules_EWO_Instances_to_Parquet") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

print("Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Source Query and Validate Data

# COMMAND ----------

try:
    # Execute the source query
    source_query = f"SELECT * FROM {SOURCE_TABLE} ORDER BY created_on_pst DESC"
    print(f"Executing query: {source_query}")
    
    # Read data from source table
    df = spark.sql(source_query)
    
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
        raise ValueError("No data found in source table!")
        
except Exception as e:
    logger.error(f"❌ Error reading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Data to DBFS as Parquet

# COMMAND ----------

try:
    print(f"🚀 Starting export to DBFS: {DBFS_EXPORT_PATH}")
    
    # Write data to DBFS as Parquet
    # Using coalesce to optimize file size and reduce small files
    # Adjust the number based on your data size (rule of thumb: aim for 100-200MB per file)
    num_partitions = 1 if row_count < 100000 else row_count // 100000  # Adjust based on your data size
    
    df.coalesce(num_partitions) \
      .write \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(DBFS_EXPORT_PATH)
    
    print("✅ Export to DBFS completed successfully!")
    
    # Verify the export by reading back and counting
    verification_df = spark.read.parquet(DBFS_EXPORT_PATH)
    exported_row_count = verification_df.count()
    
    print(f"🔍 Verification:")
    print(f"   Original rows: {row_count:,}")
    print(f"   Exported rows: {exported_row_count:,}")
    
    if row_count == exported_row_count:
        print("✅ Row count verification passed!")
    else:
        raise ValueError(f"Row count mismatch! Original: {row_count}, Exported: {exported_row_count}")
        
except Exception as e:
    logger.error(f"❌ Error during export: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy Files to S3 (Optional - if you have S3 access)

# COMMAND ----------

# Uncomment and run this section if you have S3 access configured
# This will copy the files from DBFS to S3

try:
    print(f"🔄 Attempting to copy files to S3: {S3_TARGET_PATH}")
    
    # Try to copy to S3 using dbutils
    dbutils.fs.cp(DBFS_EXPORT_PATH, S3_TARGET_PATH, recurse=True)
    
    print("✅ Files successfully copied to S3!")
    
    # Verify S3 copy
    s3_files = dbutils.fs.ls(S3_TARGET_PATH)
    print(f"📁 Files in S3: {len(s3_files)} files")
    
    # Update the export path for import notebook
    FINAL_EXPORT_PATH = S3_TARGET_PATH
    
except Exception as e:
    print(f"⚠️ Could not copy to S3: {str(e)}")
    print("📁 Files remain in DBFS - you can download them manually")
    print(f"📂 DBFS Location: {DBFS_EXPORT_PATH}")
    
    # Use DBFS path for import
    FINAL_EXPORT_PATH = DBFS_EXPORT_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Summary and Cleanup

# COMMAND ----------

try:
    # Get file information from the export location
    files_df = spark.read.format("binaryFile").load(FINAL_EXPORT_PATH)
    file_count = files_df.count()
    total_size_bytes = files_df.agg(sum("length")).collect()[0][0]
    total_size_mb = total_size_bytes / (1024 * 1024)
    
    print("📈 Export Summary:")
    print("=" * 50)
    print(f"Source Table: {SOURCE_TABLE}")
    print(f"Export Location: {FINAL_EXPORT_PATH}")
    print(f"Export Timestamp: {timestamp}")
    print(f"Total Rows Exported: {row_count:,}")
    print(f"Total Columns: {column_count}")
    print(f"Number of Parquet Files: {file_count}")
    print(f"Total Size: {total_size_mb:.2f} MB ({total_size_bytes:,} bytes)")
    print(f"Average File Size: {total_size_mb/file_count:.2f} MB")
    print("=" * 50)
    
    # Log completion
    logger.info(f"Export completed successfully. {row_count:,} rows exported to {FINAL_EXPORT_PATH}")
    
except Exception as e:
    logger.warning(f"Could not retrieve file statistics: {str(e)}")
    print("✅ Export completed, but file statistics unavailable")

finally:
    # Unpersist cached dataframe to free memory
    df.unpersist()
    print("🧹 Cleanup completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Verify Export**: Check the export location to ensure files are created
# MAGIC 2. **Run Import Notebook**: Use the companion import notebook to load data into the target cluster
# MAGIC 3. **Path for Import**: Use the final export path shown below in the import notebook
# MAGIC 
# MAGIC **Manual Transfer Options:**
# MAGIC - If files are in DBFS, you can download them from the Databricks UI under Data > FileStore > exports
# MAGIC - If files were copied to S3, use the S3 path in the import notebook

# COMMAND ----------

print(f"🎯 EXPORT COMPLETED SUCCESSFULLY!")
print(f"📁 Files location: {FINAL_EXPORT_PATH}")
print(f"📊 Total rows exported: {row_count:,}")
print(f"⏰ Export timestamp: {timestamp}")
print(f"\n📂 Export Path for Import Notebook:")
print(f"   {FINAL_EXPORT_PATH}")
print("\n🔄 Ready for import to target cluster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Download Instructions (if needed)
# MAGIC 
# MAGIC If the files are in DBFS and you need to transfer them manually:
# MAGIC 
# MAGIC 1. **Download from Databricks UI:**
# MAGIC    - Go to Data > FileStore in your Databricks workspace
# MAGIC    - Navigate to exports > jules_ewo_instances > [timestamp]
# MAGIC    - Download the Parquet files
# MAGIC 
# MAGIC 2. **Upload to Target Cluster:**
# MAGIC    - Upload files to the target cluster's DBFS
# MAGIC    - Update the import notebook with the new path
# MAGIC 
# MAGIC 3. **Use Databricks CLI with Profiles (Recommended):**
# MAGIC    ```bash
# MAGIC    # Step 1: Download from stage environment to local
# MAGIC    databricks fs cp -r dbfs:/FileStore/exports/jules_ewo_instances/20250707_224455 ./local_export --profile stage --overwrite
# MAGIC    
# MAGIC    # Step 2: Upload from local to dev environment
# MAGIC    databricks fs cp -r ./local_export dbfs:/FileStore/imports/jules_ewo_instances/20250707_224455 --profile dev --overwrite
# MAGIC    
# MAGIC    # Step 3: Verify upload to dev
# MAGIC    databricks fs ls dbfs:/FileStore/imports/jules_ewo_instances/20250707_224455 --profile dev
# MAGIC    ```
# MAGIC    
# MAGIC 4. **Alternative - List and Verify Commands:**
# MAGIC    ```bash
# MAGIC    # List available exports in stage
# MAGIC    databricks fs ls dbfs:/FileStore/exports/jules_ewo_instances/ --profile stage
# MAGIC    
# MAGIC    # Check if import directory exists in dev
# MAGIC    databricks fs ls dbfs:/FileStore/imports/ --profile dev
# MAGIC    
# MAGIC    # Create import directory if needed
# MAGIC    databricks fs mkdirs dbfs:/FileStore/imports/jules_ewo_instances --profile dev
# MAGIC    ```
