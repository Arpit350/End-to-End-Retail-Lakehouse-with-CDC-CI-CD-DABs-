# Databricks notebook source
# =============================================================================
# GOLD LAYER: gold_daily_sales
# -----------------------------------------------------------------------------
# Business-level aggregation joining the clean Silver orders (post-CDC
# merge, current state only) with Silver/Bronze customer dimension data,
# rolled up to a daily grain for BI consumption (Power BI / Databricks
# AI/BI dashboards).
# =============================================================================

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales aggregated by order date and customer region — "
            "the BI-ready reporting mart built on top of the CDC-merged "
            "silver_orders table.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "order_date"
    }
)
def gold_daily_sales():
    orders = dlt.read("silver_orders")
    customers = dlt.read("bronze_customers")

    return (
        orders
        .join(customers, on="customer_id", how="left")
        .withColumn("order_date_only", F.to_date("order_date"))
        .groupBy("order_date_only", F.col("region"))
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
            F.countDistinct("customer_id").alias("distinct_customers")
        )
        .withColumnRenamed("order_date_only", "order_date")
        .orderBy(F.desc("order_date"))
    )
