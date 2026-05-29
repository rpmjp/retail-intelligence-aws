"""Pure PySpark transformations for the retail pipeline.

These functions take a DataFrame in and return one out. No Glue context,
no I/O, no module-level side effects. This is what gets unit tested.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Optional columns the curated zone supports. Missing columns in any batch
# are backfilled as NULL so old and new schemas coexist in the same table.
CURATED_OPTIONAL_COLUMNS = {
    "loyalty_tier": StringType(),
}


def clean_retail_orders(df: DataFrame) -> DataFrame:
    """Apply the batch cleaning rules and derive partition columns.

    Steps:
      1. Standardize column names (`Customer ID` -> `CustomerID`, `Price` -> `UnitPrice`)
      2. Backfill optional columns as NULL if absent (schema evolution)
      3. Cast Quantity/UnitPrice/CustomerID to numeric types
      4. Filter: non-null CustomerID, non-cancelled invoice, positive Quantity + UnitPrice
      5. Derive Revenue, parse InvoiceDate, extract year/month for partitioning
    """
    df = df.withColumnRenamed("Customer ID", "CustomerID").withColumnRenamed("Price", "UnitPrice")

    for col_name, col_type in CURATED_OPTIONAL_COLUMNS.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

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

    return clean


def build_daily_revenue(orders: DataFrame) -> DataFrame:
    """Aggregate cleaned orders to daily revenue facts."""
    return (
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


def build_customer_rfm(orders: DataFrame, reference_date) -> DataFrame:
    """Build per-customer Recency / Frequency / Monetary features."""
    return orders.groupBy("CustomerID").agg(
        F.datediff(F.lit(reference_date), F.max("InvoiceDate")).alias("recency_days"),
        F.countDistinct("Invoice").alias("frequency"),
        F.round(F.sum("Revenue"), 2).alias("monetary"),
    )


def build_top_products(orders: DataFrame) -> DataFrame:
    """Rank products by total revenue."""
    return (
        orders.groupBy("StockCode")
        .agg(
            F.first("Description", ignorenulls=True).alias("description"),
            F.round(F.sum("Revenue"), 2).alias("total_revenue"),
            F.sum("Quantity").alias("total_units"),
            F.countDistinct("Invoice").alias("num_orders"),
        )
        .orderBy(F.col("total_revenue").desc())
    )
