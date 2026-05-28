"""Event-driven ingestion consumer.

Triggered by S3 ObjectCreated events when a JSON order file lands in
the raw/incoming/ prefix. Validates each order event, writes valid
events to raw/streaming/ partitioned by ingest date, and raises on
unrecoverable errors so the async invocation routes to the DLQ.
"""

import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

STREAMING_PREFIX = "streaming"


def _raw_bucket() -> str:
    return os.environ["RAW_BUCKET"]


# Fields every valid order event must contain
REQUIRED_FIELDS = ("invoice", "stockcode", "quantity", "price", "customerid")


class ValidationError(Exception):
    """Raised when an event fails schema validation."""


def validate_event(event: dict) -> None:
    """Validate a single order event. Raises ValidationError if invalid."""
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")

    if not isinstance(event["quantity"], (int, float)) or event["quantity"] <= 0:
        raise ValidationError(f"Invalid quantity: {event['quantity']}")

    if not isinstance(event["price"], (int, float)) or event["price"] <= 0:
        raise ValidationError(f"Invalid price: {event['price']}")


def process_file(bucket: str, key: str) -> int:
    """Read a newline-delimited JSON file, validate, write valid events.

    Returns the count of valid events written. Idempotent: output key is
    derived from the source key, so reprocessing the same file overwrites
    rather than duplicates.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")

    valid_events = []
    for line_num, line in enumerate(body.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            validate_event(event)
            valid_events.append(event)
        except (json.JSONDecodeError, ValidationError) as exc:
            # Poison record: log and skip. The file still processes;
            # a fully unreadable file raises below and hits the DLQ.
            logger.warning("Skipping bad record on line %d: %s", line_num, exc)

    if not valid_events:
        raise ValidationError(f"No valid events in {key}")

    # Partition output by ingest date (UTC)
    now = datetime.now(timezone.utc)
    source_name = key.split("/")[-1].replace(".json", "")
    out_key = f"{STREAMING_PREFIX}/" f"year={now.year}/month={now.month}/" f"{source_name}.json"

    out_body = "\n".join(json.dumps(e) for e in valid_events)
    s3.put_object(Bucket=_raw_bucket(), Key=out_key, Body=out_body.encode("utf-8"))

    logger.info(
        "Wrote %d valid events to s3://%s/%s",
        len(valid_events),
        _raw_bucket(),
        out_key,
    )
    return len(valid_events)


def lambda_handler(event: dict, context: object) -> dict:
    """S3 event entrypoint. Processes every record in the event."""
    total = 0
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing s3://%s/%s", bucket, key)
        total += process_file(bucket, key)

    return {"events_written": total}
