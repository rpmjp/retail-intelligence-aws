"""Glue PySpark job: build pre-aggregated warehouse tables from the curated zone.

Transformation logic lives in `transformations.py` so it can be unit-tested
without a Glue runtime.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Glue runtime imports `transformations` from the same script bundle.
from transformations import (  # noqa: E402
    build_customer_rfm,
    build_daily_revenue,
    build_top_products,
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "CURATED_PATH", "WAREHOUSE_PATH"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

orders = spark.read.parquet(args["CURATED_PATH"])
print(f"Curated row count: {orders.count()}")

daily_revenue = build_daily_revenue(orders)
(daily_revenue.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}daily_revenue/"))
print(f"daily_revenue rows: {daily_revenue.count()}")

reference_date = orders.agg(F.max("InvoiceDate").alias("max_date")).collect()[0]["max_date"]
rfm = build_customer_rfm(orders, reference_date)
(rfm.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}customer_rfm/"))
print(f"customer_rfm rows: {rfm.count()}")

top_products = build_top_products(orders)
(top_products.write.mode("overwrite").parquet(f"{args['WAREHOUSE_PATH']}top_products/"))
print(f"top_products rows: {top_products.count()}")

print("Warehouse build complete.")
job.commit()
