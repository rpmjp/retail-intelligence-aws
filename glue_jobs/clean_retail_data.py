"""Glue PySpark job: clean Online Retail II and write partitioned Parquet.

Bookmark-aware: only processes files added since the last successful run.
Schema-evolution tolerant: handles both the original schema and an extended
schema with a `loyalty_tier` column. Missing columns are filled with NULL
so the curated table stays consistent.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Canonical curated schema. Any new optional fields added here will be
# backfilled as NULL for files that pre-date them.
CURATED_OPTIONAL_COLUMNS = {
    "loyalty_tier": StringType(),
}

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [args["RAW_PATH"]],
        "recurse": True,
    },
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="raw_orders_dyf",
)

if dyf.count() == 0:
    print("No new files since last bookmark. Nothing to process.")
else:
    df = dyf.toDF()
    print(f"Raw row count this run: {df.count()}")
    print(f"Columns in this batch: {df.columns}")

    df = df.withColumnRenamed("Customer ID", "CustomerID").withColumnRenamed("Price", "UnitPrice")

    # Schema evolution: ensure every optional column exists, NULL if absent
    for col_name, col_type in CURATED_OPTIONAL_COLUMNS.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
            print(f"Optional column '{col_name}' missing, backfilled as NULL")

    df = (
        df.withColumn("Quantity", F.col("Quantity").cast("int"))
        .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
        .withColumn("CustomerID", F.col("CustomerID").cast("double"))
    )

    clean = (
        df.filter(F.col("CustomerID").isNotNull())
        .filter(~F.col("Invoice").startswith("C"))
        .filter(F.col("Quantity") > 0)
        .filter(F.col("UnitPrice") > 0)
    )

    clean = (
        clean.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
        .withColumn("year", F.year("InvoiceDate"))
        .withColumn("month", F.month("InvoiceDate"))
    )

    print(f"Clean row count this run: {clean.count()}")

    (clean.write.mode("append").partitionBy("year", "month").parquet(args["CURATED_PATH"]))

    print("Write complete.")

job.commit()
