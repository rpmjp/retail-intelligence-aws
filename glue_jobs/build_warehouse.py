"""Glue PySpark job: build pre-aggregated warehouse tables from the curated zone.

Reads cleaned Parquet from the curated zone and writes three aggregation
tables under warehouse/:
  - daily_revenue   (time-series facts for trend dashboards)
  - customer_rfm    (recency/frequency/monetary segmentation features)
  - top_products    (product performance ranking)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "CURATED_PATH", "WAREHOUSE_PATH"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Read the curated Parquet (already cleaned and partitioned)
orders = spark.read.parquet(args["CURATED_PATH"])
print(f"Curated row count: {orders.count()}")

# --- 1. Daily revenue (time-series facts) ---
daily_revenue = (
    orders.withColumn("order_date", F.to_date("InvoiceDate"))
    .groupBy("order_date")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("total_revenue"),
        F.countDistinct("Invoice").alias("num_orders"),
        F.countDistinct("CustomerID").alias("num_customers"),
        F.sum("Quantity").alias("total_units"),
    )
    .orderBy("order_date")
)

(daily_revenue.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}daily_revenue/"))
print(f"daily_revenue rows: {daily_revenue.count()}")

# --- 2. Customer RFM features (segmentation inputs) ---
# Reference date: day after the latest transaction in the data
max_date_row = orders.agg(F.max("InvoiceDate").alias("max_date")).collect()[0]
reference_date = max_date_row["max_date"]

rfm = orders.groupBy("CustomerID").agg(
    F.datediff(F.lit(reference_date), F.max("InvoiceDate")).alias("recency_days"),
    F.countDistinct("Invoice").alias("frequency"),
    F.round(F.sum("Revenue"), 2).alias("monetary"),
)

(rfm.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}customer_rfm/"))
print(f"customer_rfm rows: {rfm.count()}")

# --- 3. Top products (performance ranking) ---
top_products = (
    orders.groupBy("StockCode")
    .agg(
        F.first("Description", ignorenulls=True).alias("description"),
        F.round(F.sum("Revenue"), 2).alias("total_revenue"),
        F.sum("Quantity").alias("total_units"),
        F.countDistinct("Invoice").alias("num_orders"),
    )
    .orderBy(F.col("total_revenue").desc())
)

(top_products.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}top_products/"))
print(f"top_products rows: {top_products.count()}")

print("Warehouse build complete.")
job.commit()
