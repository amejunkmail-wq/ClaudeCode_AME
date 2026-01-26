# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Autoloader Pipeline with SCD Type 2
# MAGIC This pipeline:
# MAGIC 1. Uses Autoloader to stream CSV files from a source location into a Bronze layer
# MAGIC 2. Transforms Bronze data into a Silver layer using SCD Type 2 pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    when, coalesce, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from delta.tables import DeltaTable

# COMMAND ----------

# Configuration - Update these values for your environment
CONFIG = {
    # Source configuration (FTP site mounted or accessible location)
    "source_path": "/mnt/ftp_landing/csv_files/",  # Mount point for FTP site
    "source_format": "csv",

    # Bronze layer configuration
    "bronze_checkpoint_path": "/mnt/checkpoints/bronze/",
    "bronze_table_path": "/mnt/delta/bronze/raw_data/",
    "bronze_table_name": "bronze.raw_data",

    # Silver layer configuration
    "silver_table_path": "/mnt/delta/silver/dim_data/",
    "silver_table_name": "silver.dim_data",

    # Schema configuration
    "schema_location": "/mnt/schemas/autoloader/",

    # Primary key columns for SCD2 (comma-separated)
    "primary_keys": ["id"],

    # Columns to track for changes (exclude metadata columns)
    "tracked_columns": ["name", "email", "status", "amount"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Source Schema
# MAGIC Define the expected schema for incoming CSV files.
# MAGIC Adjust this based on your actual data structure.

# COMMAND ----------

# Define the schema for incoming CSV files
# Update this schema to match your CSV file structure
source_schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("created_date", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Autoloader Ingestion
# MAGIC Stream CSV files from source location using Autoloader with schema inference/evolution

# COMMAND ----------

def create_bronze_stream(spark, config, schema=None):
    """
    Create a streaming DataFrame using Autoloader to read CSV files.

    Autoloader (cloudFiles) provides:
    - Incremental file processing
    - Schema inference and evolution
    - Exactly-once processing guarantees
    - Automatic checkpoint management
    """

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config["source_format"])

        # Schema handling options
        .option("cloudFiles.schemaLocation", config["schema_location"])
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")

        # CSV-specific options
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")

        # Error handling
        .option("badRecordsPath", f"{config['bronze_checkpoint_path']}/bad_records/")
        .option("rescuedDataColumn", "_rescued_data")
    )

    # Apply schema if provided, otherwise rely on schema inference
    if schema:
        reader = reader.schema(schema)

    return reader.load(config["source_path"])


def write_bronze_stream(df, config):
    """
    Write streaming DataFrame to Bronze Delta table.

    Adds metadata columns:
    - _ingestion_timestamp: When the record was ingested
    - _source_file: Original source file path
    """

    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["bronze_checkpoint_path"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # Process all available files, then stop
        # Use .trigger(processingTime="1 minute") for continuous streaming
        .toTable(config["bronze_table_name"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - SCD Type 2 Implementation
# MAGIC Implement Slowly Changing Dimension Type 2 to track historical changes

# COMMAND ----------

def generate_hash_key(df, columns):
    """
    Generate a hash key from specified columns for change detection.
    """
    return df.withColumn(
        "_hash_key",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in columns]), 256)
    )


def initialize_silver_table(spark, config):
    """
    Initialize the Silver SCD2 table if it doesn't exist.
    """

    # Check if table exists
    if not DeltaTable.isDeltaTable(spark, config["silver_table_path"]):
        # Create empty Delta table with SCD2 schema
        schema = StructType([
            StructField("_surrogate_key", StringType(), False),
            StructField("id", StringType(), False),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("status", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("created_date", StringType(), True),
            StructField("_hash_key", StringType(), False),
            StructField("_effective_from", TimestampType(), False),
            StructField("_effective_to", TimestampType(), True),
            StructField("_is_current", BooleanType(), False),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ])

        empty_df = spark.createDataFrame([], schema)
        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .option("path", config["silver_table_path"])
            .saveAsTable(config["silver_table_name"])
        )

        print(f"Initialized Silver table: {config['silver_table_name']}")


def apply_scd2_merge(spark, config):
    """
    Apply SCD Type 2 merge from Bronze to Silver layer.

    SCD Type 2 tracks historical changes by:
    1. Keeping all historical versions of a record
    2. Using effective dates to show when each version was valid
    3. Marking the current active record with _is_current = True

    Process:
    1. Read new/changed records from Bronze layer
    2. Compare with existing Silver records using hash key
    3. Close out changed records (set _effective_to and _is_current = False)
    4. Insert new versions of changed records and new records
    """

    primary_keys = config["primary_keys"]
    tracked_columns = config["tracked_columns"]

    # Read current Bronze data (incremental - only new records since last run)
    bronze_df = spark.read.table(config["bronze_table_name"])

    # Get the latest record for each primary key from Bronze
    # (in case of duplicates in the same batch)
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(primary_keys).orderBy(col("_ingestion_timestamp").desc())

    bronze_latest = (
        bronze_df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # Generate hash key for change detection
    bronze_with_hash = generate_hash_key(bronze_latest, tracked_columns)

    # Add SCD2 metadata columns
    current_time = current_timestamp()

    bronze_prepared = (
        bronze_with_hash
        .withColumn("_surrogate_key",
            sha2(concat_ws("||", *[col(k) for k in primary_keys], current_time), 256))
        .withColumn("_effective_from", current_time)
        .withColumn("_effective_to", lit(None).cast(TimestampType()))
        .withColumn("_is_current", lit(True))
    )

    # Get Silver Delta table
    silver_table = DeltaTable.forPath(spark, config["silver_table_path"])

    # Build merge condition on primary keys and current flag
    merge_condition = " AND ".join([
        f"silver.{pk} = bronze.{pk}" for pk in primary_keys
    ]) + " AND silver._is_current = true"

    # Perform SCD2 merge
    (
        silver_table.alias("silver")
        .merge(
            bronze_prepared.alias("bronze"),
            merge_condition
        )
        # When matched and hash key is different (record changed)
        # Close out the old record
        .whenMatchedUpdate(
            condition="silver._hash_key != bronze._hash_key",
            set={
                "_effective_to": current_time,
                "_is_current": lit(False)
            }
        )
        # Insert new records (both new primary keys and new versions of changed records)
        .whenNotMatchedInsert(
            values={
                "_surrogate_key": col("bronze._surrogate_key"),
                "id": col("bronze.id"),
                "name": col("bronze.name"),
                "email": col("bronze.email"),
                "status": col("bronze.status"),
                "amount": col("bronze.amount"),
                "created_date": col("bronze.created_date"),
                "_hash_key": col("bronze._hash_key"),
                "_effective_from": col("bronze._effective_from"),
                "_effective_to": col("bronze._effective_to"),
                "_is_current": col("bronze._is_current"),
                "_ingestion_timestamp": col("bronze._ingestion_timestamp"),
            }
        )
        .execute()
    )

    # Insert new versions of changed records
    # (The merge above only closed out old records, we need to insert the new versions)
    changed_records = (
        bronze_prepared.alias("bronze")
        .join(
            silver_table.toDF().alias("silver"),
            [col(f"bronze.{pk}") == col(f"silver.{pk}") for pk in primary_keys],
            "inner"
        )
        .filter(
            (col("silver._is_current") == False) &
            (col("silver._effective_to") == current_time) &
            (col("silver._hash_key") != col("bronze._hash_key"))
        )
        .select("bronze.*")
    )

    if changed_records.count() > 0:
        (
            changed_records.write
            .format("delta")
            .mode("append")
            .option("path", config["silver_table_path"])
            .saveAsTable(config["silver_table_name"])
        )

    print(f"SCD2 merge completed for {config['silver_table_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Streaming SCD2 with foreachBatch
# MAGIC For real-time SCD2 processing using structured streaming

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

def process_scd2_microbatch(batch_df, batch_id, spark, config):
    """
    Process each micro-batch with SCD2 logic.
    Used with foreachBatch for streaming SCD2.
    """

    if batch_df.isEmpty():
        return

    primary_keys = config["primary_keys"]
    tracked_columns = config["tracked_columns"]
    current_time = current_timestamp()

    # Deduplicate batch - keep latest record per primary key
    window_spec = Window.partitionBy(primary_keys).orderBy(col("_ingestion_timestamp").desc())

    batch_deduped = (
        batch_df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # Generate hash and prepare for merge
    batch_with_hash = generate_hash_key(batch_deduped, tracked_columns)

    batch_prepared = (
        batch_with_hash
        .withColumn("_surrogate_key",
            sha2(concat_ws("||", *[col(k) for k in primary_keys], current_time), 256))
        .withColumn("_effective_from", current_time)
        .withColumn("_effective_to", lit(None).cast(TimestampType()))
        .withColumn("_is_current", lit(True))
        .withColumn("_merge_key", concat_ws("||", *[col(k) for k in primary_keys]))
    )

    # Get Silver table
    silver_table = DeltaTable.forPath(spark, config["silver_table_path"])

    # Build merge condition
    merge_condition = " AND ".join([
        f"silver.{pk} = updates.{pk}" for pk in primary_keys
    ]) + " AND silver._is_current = true"

    # Perform merge
    (
        silver_table.alias("silver")
        .merge(
            batch_prepared.alias("updates"),
            merge_condition
        )
        .whenMatchedUpdate(
            condition="silver._hash_key != updates._hash_key",
            set={
                "_effective_to": current_time,
                "_is_current": lit(False)
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_streaming_scd2_pipeline(spark, config):
    """
    Run end-to-end streaming pipeline with SCD2.
    """

    # Initialize Silver table if needed
    initialize_silver_table(spark, config)

    # Create Bronze stream
    bronze_stream = create_bronze_stream(spark, config)

    # Add ingestion metadata
    bronze_with_metadata = (
        bronze_stream
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Write to Silver with SCD2 using foreachBatch
    return (
        bronze_with_metadata.writeStream
        .foreachBatch(lambda df, batch_id: process_scd2_microbatch(df, batch_id, spark, config))
        .option("checkpointLocation", f"{config['bronze_checkpoint_path']}/scd2_streaming/")
        .trigger(availableNow=True)
        .start()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution
# MAGIC Choose between batch or streaming execution mode

# COMMAND ----------

def run_batch_pipeline(spark, config):
    """
    Run the pipeline in batch mode:
    1. Ingest new files to Bronze using Autoloader
    2. Apply SCD2 merge to Silver
    """

    print("Starting batch pipeline...")

    # Initialize Silver table
    initialize_silver_table(spark, config)

    # Step 1: Bronze ingestion with Autoloader
    print("Step 1: Ingesting to Bronze layer...")
    bronze_stream = create_bronze_stream(spark, config, source_schema)
    bronze_query = write_bronze_stream(bronze_stream, config)
    bronze_query.awaitTermination()
    print("Bronze ingestion complete.")

    # Step 2: SCD2 merge to Silver
    print("Step 2: Applying SCD2 merge to Silver...")
    apply_scd2_merge(spark, config)
    print("Silver SCD2 merge complete.")

    print("Batch pipeline completed successfully!")


def run_continuous_pipeline(spark, config):
    """
    Run the pipeline in continuous streaming mode.
    """

    print("Starting continuous streaming pipeline...")

    # Initialize Silver table
    initialize_silver_table(spark, config)

    # Run streaming SCD2 pipeline
    query = run_streaming_scd2_pipeline(spark, config)

    # For continuous mode, use:
    # query.awaitTermination()

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline
# MAGIC Uncomment the appropriate execution mode below

# COMMAND ----------

# Get or create Spark session (already available in Databricks)
# spark = SparkSession.builder.getOrCreate()

# Run batch pipeline (process available files, then stop)
# run_batch_pipeline(spark, CONFIG)

# OR run continuous streaming pipeline
# query = run_continuous_pipeline(spark, CONFIG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions for Data Quality and Monitoring

# COMMAND ----------

def get_silver_record_history(spark, config, primary_key_values):
    """
    Get the full history of a record from Silver table.

    Args:
        primary_key_values: Dict of primary key column names and values
    """

    silver_df = spark.read.table(config["silver_table_name"])

    filter_condition = None
    for pk, value in primary_key_values.items():
        condition = col(pk) == value
        filter_condition = condition if filter_condition is None else filter_condition & condition

    return (
        silver_df
        .filter(filter_condition)
        .orderBy(col("_effective_from").desc())
    )


def get_current_silver_snapshot(spark, config):
    """
    Get current state of all records (only active versions).
    """

    return (
        spark.read.table(config["silver_table_name"])
        .filter(col("_is_current") == True)
    )


def get_silver_stats(spark, config):
    """
    Get statistics about the Silver SCD2 table.
    """

    silver_df = spark.read.table(config["silver_table_name"])

    stats = {
        "total_records": silver_df.count(),
        "current_records": silver_df.filter(col("_is_current") == True).count(),
        "historical_records": silver_df.filter(col("_is_current") == False).count(),
        "unique_entities": silver_df.select(CONFIG["primary_keys"]).distinct().count(),
    }

    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: View Silver Table Stats
# MAGIC Uncomment to run after pipeline execution

# COMMAND ----------

# stats = get_silver_stats(spark, CONFIG)
# print(f"Total records: {stats['total_records']}")
# print(f"Current records: {stats['current_records']}")
# print(f"Historical records: {stats['historical_records']}")
# print(f"Unique entities: {stats['unique_entities']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: View Record History
# MAGIC Uncomment to view history of a specific record

# COMMAND ----------

# history = get_silver_record_history(spark, CONFIG, {"id": "12345"})
# display(history)
