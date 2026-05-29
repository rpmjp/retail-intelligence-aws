"""Unit tests for Glue PySpark transformations.

Uses local-mode PySpark, no Glue runtime needed. Verifies cleaning rules,
schema evolution, and warehouse aggregations against synthetic DataFrames.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Make transformations.py importable
GLUE_DIR = Path(__file__).parent.parent / "glue_jobs"
sys.path.insert(0, str(GLUE_DIR))

from transformations import (  # noqa: E402
    build_customer_rfm,
    build_daily_revenue,
    build_top_products,
    clean_retail_orders,
)


@pytest.fixture(scope="session")
def spark():
    """One SparkSession per test session, in local mode."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("retail-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _sample_raw_rows():
    """Synthetic raw rows mimicking Online Retail II columns and dirtiness."""
    return [
        # Valid
        (
            "536365",
            "85123A",
            "WHITE HEART",
            "6",
            "2010-12-01 08:26:00",
            "2.55",
            "17850",
            "United Kingdom",
        ),
        (
            "536366",
            "22633",
            "HAND WARMER",
            "12",
            "2010-12-01 08:30:00",
            "1.85",
            "17851",
            "United Kingdom",
        ),
        # Cancelled invoice (starts with C)
        (
            "C536367",
            "84879",
            "BIRD ORNAMENT",
            "5",
            "2010-12-01 09:00:00",
            "1.69",
            "17852",
            "France",
        ),
        # Null customer
        (
            "536368",
            "21730",
            "T-LIGHT HOLDER",
            "8",
            "2010-12-01 09:15:00",
            "4.25",
            None,
            "United Kingdom",
        ),
        # Negative quantity
        ("536369", "22112", "BAD ROW", "-3", "2010-12-01 10:00:00", "1.95", "17853", "Germany"),
        # Zero price
        ("536370", "22113", "FREEBIE", "5", "2010-12-01 10:30:00", "0", "17854", "Spain"),
    ]


_RAW_COLS = [
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
]


def test_clean_filters_invalid_rows(spark):
    """Cleaning should drop cancelled, null-customer, negative-qty, zero-price rows."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    result = clean_retail_orders(df).collect()
    invoices = [r["Invoice"] for r in result]
    assert invoices == ["536365", "536366"]


def test_clean_derives_revenue_and_partitions(spark):
    """Revenue, year, and month columns are derived correctly."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    result = clean_retail_orders(df).collect()
    first = result[0]
    assert first["Revenue"] == pytest.approx(6 * 2.55, rel=1e-6)
    assert first["year"] == 2010
    assert first["month"] == 12


def test_clean_backfills_optional_loyalty_tier(spark):
    """When loyalty_tier is missing, the cleaned frame still has the column as NULL."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    result = clean_retail_orders(df)
    assert "loyalty_tier" in result.columns
    assert all(r["loyalty_tier"] is None for r in result.collect())


def test_clean_preserves_loyalty_tier_when_present(spark):
    """When loyalty_tier is provided, it carries through cleaning unchanged."""
    rows = [r + ("Gold",) for r in _sample_raw_rows()[:2]]  # only valid rows
    cols = _RAW_COLS + ["loyalty_tier"]
    df = spark.createDataFrame(rows, cols)
    result = clean_retail_orders(df).collect()
    assert all(r["loyalty_tier"] == "Gold" for r in result)


def test_clean_handles_renames(spark):
    """`Customer ID` becomes `CustomerID`, `Price` becomes `UnitPrice`."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    result = clean_retail_orders(df)
    assert "CustomerID" in result.columns
    assert "UnitPrice" in result.columns
    assert "Customer ID" not in result.columns
    assert "Price" not in result.columns


def test_daily_revenue_aggregates_correctly(spark):
    """Daily revenue rolls up to the right totals per day."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    clean = clean_retail_orders(df)
    daily = build_daily_revenue(clean).collect()
    assert len(daily) == 1  # all valid rows are 2010-12-01
    day = daily[0]
    expected_revenue = round(6 * 2.55 + 12 * 1.85, 2)
    assert day["total_revenue"] == pytest.approx(expected_revenue, rel=1e-6)
    assert day["num_orders"] == 2
    assert day["num_customers"] == 2
    assert day["total_units"] == 18


def test_customer_rfm_computes_features(spark):
    """RFM features compute recency, frequency, and monetary per customer."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    clean = clean_retail_orders(df)
    reference_date = datetime(2011, 1, 1)
    rfm = build_customer_rfm(clean, reference_date).collect()
    by_customer = {r["CustomerID"]: r for r in rfm}
    assert 17850.0 in by_customer
    r = by_customer[17850.0]
    assert r["recency_days"] == 31
    assert r["frequency"] == 1
    assert r["monetary"] == pytest.approx(15.3, rel=1e-6)


def test_top_products_ranks_by_revenue(spark):
    """Top products is ordered by total_revenue descending."""
    df = spark.createDataFrame(_sample_raw_rows(), _RAW_COLS)
    clean = clean_retail_orders(df)
    products = build_top_products(clean).collect()
    assert len(products) == 2
    # 85123A has higher revenue (6 * 2.55 = 15.30) vs 22633 (12 * 1.85 = 22.20)
    # Actually 22633 wins. Verify ordering.
    revenues = [p["total_revenue"] for p in products]
    assert revenues == sorted(revenues, reverse=True)
