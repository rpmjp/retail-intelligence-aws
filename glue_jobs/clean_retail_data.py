"""Glue PySpark job: clean Online Retail II and write partitioned Parquet.

Bookmark-aware: only processes files added since the last successful run.
Schema-evolution tolerant: handles both the original schema and an extended
schema with a `loyalty_tier` column. Missing columns are filled with NULL
so the curated table stays consistent.

Transformation logic lives in `transformations.py` so it can be unit-tested
without a Glue runtime.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Glue runtime imports `transformations` from the same script bundle.
from transformations import clean_retail_orders  # noqa: E402

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

    clean = clean_retail_orders(df)
    print(f"Clean row count this run: {clean.count()}")

    (clean.write.mode("append").partitionBy("year", "month").parquet(args["CURATED_PATH"]))

    print("Write complete.")

job.commit()
