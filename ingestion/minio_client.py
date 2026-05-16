# ingestion/minio_client.py
import json
import gzip
import logging
from datetime import datetime, timezone
from typing import Any
import boto3
from botocore.client import Config
import os

logger = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET          = os.getenv("MINIO_BUCKET", "crypto-lake")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_json_gz(
    data: Any,
    layer: str,          # bronze | silver | gold
    source: str,         # coingecko | yfinance | fear_greed
    entity: str,         # ohlcv | market | sentiment | metadata
    partition_date: str = None,  # YYYY-MM-DD
) -> str:
    """
    Upload data as gzipped JSON to MinIO.
    Returns the S3 key of uploaded object.

    Path pattern:
    {layer}/{source}/{entity}/date={partition_date}/{timestamp}.json.gz
    """
    s3 = get_s3_client()

    if partition_date is None:
        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{layer}/{source}/{entity}/date={partition_date}/{timestamp}.json.gz"

    # Serialize + compress
    json_bytes  = json.dumps(data, ensure_ascii=False).encode("utf-8")
    gz_bytes    = gzip.compress(json_bytes)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=gz_bytes,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    logger.info(f"Uploaded → s3://{BUCKET}/{key}  ({len(gz_bytes):,} bytes)")
    return f"s3://{BUCKET}/{key}"


def download_json_gz(key: str) -> Any:
    """Download and decompress a JSON.gz object from MinIO."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=BUCKET, Key=key)
    gz_bytes = response["Body"].read()
    json_bytes = gzip.decompress(gz_bytes)
    return json.loads(json_bytes.decode("utf-8"))


def list_objects(prefix: str) -> list[str]:
    """List all object keys under a prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys