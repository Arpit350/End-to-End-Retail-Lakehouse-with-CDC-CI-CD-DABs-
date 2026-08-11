# Databricks notebook source
# =============================================================================
# SILVER LAYER: silver_orders_cdc
# -----------------------------------------------------------------------------
# Consumes a streaming Change Data Capture (CDC) feed of order events
# (INSERT / UPDATE / DELETE operations captured from an upstream OLTP
# source and landed as JSON in ADLS Gen2). Uses DLT's apply_changes() /
# AUTO CDC API to merge the change stream into a clean, deduplicated
# Silver table using SCD Type 1 semantics (latest state wins, no history
# retained).
#
# This is the core pattern that demonstrates production-grade CDC handling:
#   - out-of-order event handling via sequence_by
#   - deletes honored via apply_as_deletes
#   - duplicate/idempotent events collapsed automatically
# =============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

CDC_SOURCE_PATH = spark.conf.get(
    "silver.orders_cdc.source_path",
    "abfss://raw@<your_storage_account>.dfs.core.windows.net/retail/orders_cdc/"
)

SCHEMA_LOCATION = spark.conf.get(
    "silver.orders_cdc.schema_location",
    "abfss://raw@<your_storage_account>.dfs.core.windows.net/_schemas/silver_orders_cdc/"
)

# Explicit schema for the raw CDC JSON events (recommended for streaming
# sources so schema inference doesn't stall the stream).
cdc_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("op", StringType(), True),              # "INSERT" | "UPDATE" | "DELETE"
    StructField("sequence_num", TimestampType(), True),  # source commit/change timestamp
])


# -----------------------------------------------------------------------------
# 1. Raw CDC staging table (Bronze-ish landing for the change stream)
# -----------------------------------------------------------------------------
@dlt.table(
    name="bronze_orders_cdc_raw",
    comment="Raw CDC change events (insert/update/delete) for orders, ingested via Auto Loader.",
    table_properties={"quality": "bronze"}
)
def bronze_orders_cdc_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .schema(cdc_schema)
        .load(CDC_SOURCE_PATH)
    )


# -----------------------------------------------------------------------------
# 2. Target streaming table that apply_changes will materialize into
# -----------------------------------------------------------------------------
dlt.create_streaming_table(
    name="silver_orders",
    comment="Current-state orders table (SCD Type 1) produced by merging the "
            "CDC stream via apply_changes. Reflects the latest known state "
            "of each order — updates overwrite, deletes remove the row.",
    table_properties={"quality": "silver"}
)

# -----------------------------------------------------------------------------
# 3. apply_changes: the CDC merge logic itself
# -----------------------------------------------------------------------------
dlt.apply_changes(
    target="silver_orders",
    source="bronze_orders_cdc_raw",
    keys=["order_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "sequence_num"],
    stored_as_scd_type=1
)
