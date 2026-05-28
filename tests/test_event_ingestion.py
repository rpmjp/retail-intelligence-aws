"""Unit tests for the event ingestion Lambda handler.

Uses moto to mock S3 so tests run fast, deterministic, and offline.
Covers: validation rules, partial-failure handling, idempotency,
and the unrecoverable-failure path.
"""

import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Make the lambda module importable without installing it
sys.path.insert(0, str(Path(__file__).parent.parent))


RAW_BUCKET = "test-raw-bucket"
INCOMING_KEY = "incoming/test_orders.json"


@pytest.fixture
def aws_env(monkeypatch):
    """Provide environment + mocked S3 with the raw bucket created."""
    monkeypatch.setenv("RAW_BUCKET", RAW_BUCKET)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=RAW_BUCKET)
        yield s3


def _put_incoming(s3, body: str, key: str = INCOMING_KEY) -> None:
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))


def _s3_event(key: str = INCOMING_KEY) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": RAW_BUCKET},
                    "object": {"key": key},
                }
            }
        ]
    }


def _valid_order(invoice: str = "536365") -> dict:
    return {
        "invoice": invoice,
        "stockcode": "85123A",
        "description": "TEST ITEM",
        "quantity": 6,
        "price": 2.55,
        "customerid": 17850,
        "country": "United Kingdom",
    }


def _read_streaming_key(s3, key: str) -> str:
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")


def _find_streaming_key(s3) -> str:
    """Return the single key under streaming/."""
    listing = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="streaming/")
    keys = [o["Key"] for o in listing.get("Contents", [])]
    assert len(keys) == 1, f"Expected one streaming key, got {keys}"
    return keys[0]


def test_happy_path_writes_all_valid_events(aws_env):
    """Three valid events should all be written to the streaming zone."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    body = "\n".join(json.dumps(_valid_order(inv)) for inv in ["536365", "536366", "536367"])
    _put_incoming(s3, body)

    result = handler.lambda_handler(_s3_event(), context=None)

    assert result["events_written"] == 3
    out_key = _find_streaming_key(s3)
    assert out_key.startswith("streaming/year=")
    assert "/month=" in out_key
    out_body = _read_streaming_key(s3, out_key)
    assert len(out_body.splitlines()) == 3


def test_missing_required_field_is_skipped(aws_env):
    """A record missing required fields is skipped, valid ones still processed."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    valid = _valid_order("536400")
    invalid = {"invoice": "536401", "stockcode": "X", "quantity": 1, "price": 1.0}
    # ^ missing customerid
    body = "\n".join(json.dumps(r) for r in [valid, invalid])
    _put_incoming(s3, body)

    result = handler.lambda_handler(_s3_event(), context=None)

    assert result["events_written"] == 1


def test_negative_quantity_is_skipped(aws_env):
    """Records with non-positive quantity fail validation and are skipped."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    valid = _valid_order("536500")
    invalid = _valid_order("536501")
    invalid["quantity"] = -5
    body = "\n".join(json.dumps(r) for r in [valid, invalid])
    _put_incoming(s3, body)

    result = handler.lambda_handler(_s3_event(), context=None)

    assert result["events_written"] == 1


def test_malformed_json_line_is_skipped(aws_env):
    """A line of broken JSON is skipped, valid records continue."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    valid = _valid_order("536600")
    body = json.dumps(valid) + "\n{not valid json at all}"
    _put_incoming(s3, body)

    result = handler.lambda_handler(_s3_event(), context=None)

    assert result["events_written"] == 1


def test_zero_valid_events_raises(aws_env):
    """A file with no salvageable records raises so the async path hits the DLQ."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    body = json.dumps({"garbage": "no fields"})
    _put_incoming(s3, body)

    with pytest.raises(handler.ValidationError, match="No valid events"):
        handler.lambda_handler(_s3_event(), context=None)


def test_idempotent_output_key(aws_env):
    """Reprocessing the same file overwrites rather than creating duplicates."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    body = json.dumps(_valid_order("536700"))
    _put_incoming(s3, body)

    handler.lambda_handler(_s3_event(), context=None)
    first_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="streaming/").get("Contents", [])
    ]

    handler.lambda_handler(_s3_event(), context=None)
    second_keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="streaming/").get("Contents", [])
    ]

    assert first_keys == second_keys
    assert len(second_keys) == 1


def test_url_encoded_key_is_decoded(aws_env):
    """S3 event keys arrive URL-encoded; the handler must decode them."""
    from lambdas.event_ingestion import handler

    s3 = aws_env
    actual_key = "incoming/test orders.json"  # space in filename
    body = json.dumps(_valid_order("536800"))
    _put_incoming(s3, body, key=actual_key)

    # S3 events encode spaces as '+'
    event = _s3_event(key="incoming/test+orders.json")
    result = handler.lambda_handler(event, context=None)

    assert result["events_written"] == 1
