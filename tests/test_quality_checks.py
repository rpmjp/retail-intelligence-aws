"""Unit tests for the quality-checks Lambda handler.

Mocks the Athena client so tests run offline. Verifies that each check
correctly passes or fails based on synthetic Athena results.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make the lambda module importable
LAMBDA_DIR = Path(__file__).parent.parent / "lambdas" / "quality_checks"
sys.path.insert(0, str(LAMBDA_DIR))


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("DATABASE", "retail_intelligence")
    monkeypatch.setenv("TABLE", "online_retail")
    monkeypatch.setenv("ATHENA_OUTPUT_LOCATION", "s3://bucket/athena-results/")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _mock_athena_rows(headers: list[str], rows: list[list[str]]):
    """Build a mock Athena query-results response."""
    header_row = {"Data": [{"VarCharValue": h} for h in headers]}
    data_rows = [{"Data": [{"VarCharValue": v} for v in row]} for row in rows]
    return {"ResultSet": {"Rows": [header_row] + data_rows}}


def _patch_athena(athena_responses: list[dict]):
    """Patch boto3 Athena client to return preset query results in order."""
    mock = MagicMock()
    mock.start_query_execution.side_effect = [
        {"QueryExecutionId": f"q{i}"} for i in range(len(athena_responses))
    ]
    mock.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}
    mock.get_query_results.side_effect = athena_responses
    return mock


def test_all_checks_pass(env):
    """When all metrics meet thresholds, handler returns PASSED."""
    if "handler" in sys.modules:
        del sys.modules["handler"]
    import handler

    responses = [
        _mock_athena_rows(["n"], [["805549"]]),
        _mock_athena_rows(
            ["null_invoice", "null_customer", "null_revenue", "total"],
            [["0", "0", "0", "805549"]],
        ),
        _mock_athena_rows(["total"], [["17743429.18"]]),
        _mock_athena_rows(["year"], [["2009"], ["2010"], ["2011"]]),
    ]
    with patch.object(handler, "athena", _patch_athena(responses)):
        result = handler.lambda_handler({}, None)

    assert result["status"] == "PASSED"
    assert all(c["passed"] for c in result["checks"].values())


def test_row_count_failure_raises(env):
    """Row count below threshold should raise QualityCheckFailed."""
    if "handler" in sys.modules:
        del sys.modules["handler"]
    import handler

    responses = [
        _mock_athena_rows(["n"], [["100"]]),
        _mock_athena_rows(
            ["null_invoice", "null_customer", "null_revenue", "total"],
            [["0", "0", "0", "100"]],
        ),
        _mock_athena_rows(["total"], [["17743429.18"]]),
        _mock_athena_rows(["year"], [["2009"], ["2010"], ["2011"]]),
    ]
    with patch.object(handler, "athena", _patch_athena(responses)):
        with pytest.raises(handler.QualityCheckFailed, match="row_count"):
            handler.lambda_handler({}, None)


def test_missing_partition_raises(env):
    """Missing year partition should raise QualityCheckFailed."""
    if "handler" in sys.modules:
        del sys.modules["handler"]
    import handler

    responses = [
        _mock_athena_rows(["n"], [["805549"]]),
        _mock_athena_rows(
            ["null_invoice", "null_customer", "null_revenue", "total"],
            [["0", "0", "0", "805549"]],
        ),
        _mock_athena_rows(["total"], [["17743429.18"]]),
        _mock_athena_rows(["year"], [["2009"], ["2010"]]),  # missing 2011
    ]
    with patch.object(handler, "athena", _patch_athena(responses)):
        with pytest.raises(handler.QualityCheckFailed, match="partitions"):
            handler.lambda_handler({}, None)


def test_null_critical_field_raises(env):
    """Any nulls in a critical column should raise QualityCheckFailed."""
    if "handler" in sys.modules:
        del sys.modules["handler"]
    import handler

    responses = [
        _mock_athena_rows(["n"], [["805549"]]),
        _mock_athena_rows(
            ["null_invoice", "null_customer", "null_revenue", "total"],
            [["5", "0", "0", "805549"]],
        ),
        _mock_athena_rows(["total"], [["17743429.18"]]),
        _mock_athena_rows(["year"], [["2009"], ["2010"], ["2011"]]),
    ]
    with patch.object(handler, "athena", _patch_athena(responses)):
        with pytest.raises(handler.QualityCheckFailed, match="critical_nulls"):
            handler.lambda_handler({}, None)
