"""Data quality checks for the curated retail zone.

Runs after the Glue ETL completes. Queries Athena to assert:
  - Row count above a minimum threshold
  - Null rate on critical columns below threshold
  - Revenue totals are positive and reasonable
  - All expected year partitions present

Returns a structured result. Raises QualityCheckFailed on any failure
so Step Functions routes to PipelineFailed via Catch.
"""

import os
import time

import boto3

athena = boto3.client("athena")

DATABASE = os.environ.get("DATABASE", "retail_intelligence")
TABLE = os.environ.get("TABLE", "online_retail")


def _output_location() -> str:
    return os.environ["ATHENA_OUTPUT_LOCATION"]


# Thresholds: tunable per project
MIN_ROW_COUNT = 700_000
MAX_NULL_RATE = 0.0
MIN_TOTAL_REVENUE = 1_000_000.0
EXPECTED_YEARS = {"2009", "2010", "2011"}


class QualityCheckFailed(Exception):
    """Raised when one or more data quality checks fail."""


def _run_query(sql: str) -> list[dict]:
    """Run an Athena query synchronously and return result rows as dicts."""
    start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": _output_location()},
    )
    qid = start["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query failed: {reason}")
        time.sleep(1)

    results = athena.get_query_results(QueryExecutionId=qid)
    rows = results["ResultSet"]["Rows"]
    if not rows:
        return []
    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    return [dict(zip(headers, [d.get("VarCharValue") for d in r["Data"]])) for r in rows[1:]]


def _check_row_count() -> tuple[bool, str]:
    rows = _run_query(f"SELECT COUNT(*) AS n FROM {TABLE}")
    n = int(rows[0]["n"])
    if n < MIN_ROW_COUNT:
        return False, f"Row count {n} below minimum {MIN_ROW_COUNT}"
    return True, f"Row count {n} OK"


def _check_critical_nulls() -> tuple[bool, str]:
    rows = _run_query(f"""
        SELECT
            SUM(CASE WHEN invoice IS NULL THEN 1 ELSE 0 END) AS null_invoice,
            SUM(CASE WHEN customerid IS NULL THEN 1 ELSE 0 END) AS null_customer,
            SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue,
            COUNT(*) AS total
        FROM {TABLE}
        """)
    r = rows[0]
    total = int(r["total"])
    nulls = {
        "invoice": int(r["null_invoice"]),
        "customerid": int(r["null_customer"]),
        "revenue": int(r["null_revenue"]),
    }
    failures = [
        f"{col} null rate {nulls[col] / total:.4f}"
        for col in nulls
        if nulls[col] / total > MAX_NULL_RATE
    ]
    if failures:
        return False, "; ".join(failures)
    return True, f"Null rates within threshold (max allowed {MAX_NULL_RATE})"


def _check_revenue_sanity() -> tuple[bool, str]:
    rows = _run_query(f"SELECT SUM(revenue) AS total FROM {TABLE}")
    total = float(rows[0]["total"])
    if total < MIN_TOTAL_REVENUE:
        return False, f"Total revenue {total:,.2f} below minimum {MIN_TOTAL_REVENUE:,.2f}"
    return True, f"Total revenue {total:,.2f} OK"


def _check_partitions() -> tuple[bool, str]:
    rows = _run_query(f"SELECT DISTINCT year FROM {TABLE}")
    years = {r["year"] for r in rows}
    missing = EXPECTED_YEARS - years
    if missing:
        return False, f"Missing year partitions: {sorted(missing)}"
    return True, f"All expected year partitions present: {sorted(years)}"


def lambda_handler(event: dict, context: object) -> dict:
    checks = {
        "row_count": _check_row_count,
        "critical_nulls": _check_critical_nulls,
        "revenue_sanity": _check_revenue_sanity,
        "partitions": _check_partitions,
    }
    results = {}
    failures = []
    for name, fn in checks.items():
        passed, message = fn()
        results[name] = {"passed": passed, "message": message}
        if not passed:
            failures.append(f"{name}: {message}")

    if failures:
        raise QualityCheckFailed("; ".join(failures))

    return {"status": "PASSED", "checks": results}
