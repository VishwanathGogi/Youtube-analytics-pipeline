"""Normalize YouTube category metadata from raw S3 JSON into Parquet."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import unquote_plus

import awswrangler as wr
import pandas as pd

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def _required_env(primary: str, legacy: str | None = None) -> str:
    """Return a required environment value, supporting one legacy name."""
    value = os.getenv(primary)
    if value is None and legacy:
        value = os.getenv(legacy)
    if not value:
        names = f"{primary} or {legacy}" if legacy else primary
        raise RuntimeError(f"Missing required environment variable: {names}")
    return value


S3_CLEANSED_PATH = _required_env("S3_CLEANSED_PATH", "s3_cleansed_layer")
GLUE_DB_NAME = _required_env("GLUE_DB_NAME", "glue_catalog_db_name")
GLUE_TABLE_NAME = _required_env("GLUE_TABLE_NAME", "glue_catalog_table_name")
WRITE_MODE = os.getenv("WRITE_MODE", os.getenv("write_data_operation", "overwrite"))
ALLOWED_SOURCE_BUCKET = os.getenv("ALLOWED_SOURCE_BUCKET")


def _source_from_event(event: dict[str, Any]) -> tuple[str, str]:
    """Extract and validate the source bucket and key from an S3 event."""
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"], encoding="utf-8")
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("Expected an S3 object-created event") from exc

    if ALLOWED_SOURCE_BUCKET and bucket != ALLOWED_SOURCE_BUCKET:
        raise ValueError(f"Unexpected source bucket: {bucket}")
    if not key.lower().endswith(".json"):
        raise ValueError(f"Unsupported source object type: {key}")

    return bucket, key


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process one category JSON object referenced by an S3 event."""
    bucket, key = _source_from_event(event)
    source_uri = f"s3://{bucket}/{key}"

    LOGGER.info("Processing category metadata", extra={"bucket": bucket, "key": key})

    try:
        raw_frame = wr.s3.read_json(source_uri)
        if "items" not in raw_frame.columns:
            raise ValueError(f"Source object does not contain an items field: {key}")

        flattened = pd.json_normalize(raw_frame["items"].explode().dropna())
        if flattened.empty:
            raise ValueError(f"Source object contains no category records: {key}")

        result = wr.s3.to_parquet(
            df=flattened,
            path=S3_CLEANSED_PATH,
            dataset=True,
            database=GLUE_DB_NAME,
            table=GLUE_TABLE_NAME,
            mode=WRITE_MODE,
        )
        LOGGER.info(
            "Category metadata processed",
            extra={"bucket": bucket, "key": key, "rows": len(flattened)},
        )
        return result
    except Exception:
        LOGGER.exception(
            "Category metadata processing failed",
            extra={"bucket": bucket, "key": key},
        )
        raise
