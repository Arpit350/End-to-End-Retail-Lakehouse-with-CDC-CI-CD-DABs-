# Databricks notebook source
# =============================================================================
# BRONZE LAYER: bronze_customers
# -----------------------------------------------------------------------------
# Ingests raw customer CSV files landed in ADLS Gen2 using Auto Loader
# (cloudFiles). Schema is inferred and evolved automatically, and every
# record is stamped with lineage metadata before landing in the Bronze
# Delta table. This is a batch/near-real-time Auto Loader pattern, distinct
# from the streaming CDC pattern used for orders in silver_orders_cdc.py.
# =============================================================================

import dlt
from pyspark.sql import functions as f

# -----------------------------------------------------------------------------
# Widgets / configuration (overridden via databricks.yml job parameters)
# -----------------------------------------------------------------------------
RAW_CUSTOMERS_PATH = spark.conf.get(
    "bronze.customers.source_path",
    "abfss://raw@lakehouse_sample.dfs.core.windows.net/retail/customers/"
)

SCHEMA_LOCATION = spark.conf.get(
    "bronze.customers.schema_location",
    "abfss://raw@lakehouse_sample.dfs.core.windows.net/_schemas/bronze_customers/"
)


@dlt.table(
    name="bronze_customers",
    comment="Raw customer records ingested via Auto Loader (cloudFiles) from ADLS Gen2. "
            "No transformations applied — append-only landing zone.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_CUSTOMERS_PATH)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
