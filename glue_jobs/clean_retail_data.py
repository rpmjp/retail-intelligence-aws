"""Glue PySpark job: clean Online Retail II data and write partitioned Parquet."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Read raw CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv(args["RAW_PATH"])

print(f"Raw row count: {df.count()}")

# Standardize column names (remove spaces)
df = df.withColumnRenamed("Customer ID", "CustomerID").withColumnRenamed("Price", "UnitPrice")

# Clean
clean = (
    df.filter(F.col("CustomerID").isNotNull())
    .filter(~F.col("Invoice").startswith("C"))
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)
)

# Add revenue + date partitions
clean = (
    clean.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
    .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
    .withColumn("year", F.year("InvoiceDate"))
    .withColumn("month", F.month("InvoiceDate"))
)

print(f"Clean row count: {clean.count()}")

# Write partitioned Parquet to curated zone
(clean.write.mode("overwrite").partitionBy("year", "month").parquet(args["CURATED_PATH"]))

print("Write complete.")
job.commit()
