# ingestion/minio_client.py
import json
import gzip
import logging
import re
from datetime import datetime, timezone
from typing import Any
import boto3
from botocore.client import Config
import os

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET         = os.getenv("MINIO_BUCKET", "crypto-lake")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _sanitize_key(value: str) -> str:
    """
    ทำให้ string ปลอดภัยสำหรับใช้เป็น S3 key
    ลบ/แทนที่ตัวอักษรที่ MinIO ไม่รองรับ
    """
    # แทนที่ + ด้วย _
    value = value.replace("+", "_")
    # แทนที่ : ด้วย -
    value = value.replace(":", "-")
    # ลบตัวอักษรอื่นที่ไม่ปลอดภัย (เก็บแค่ a-z, A-Z, 0-9, -, _, ., /)
    value = re.sub(r"[^\w\-_./]", "_", value)
    return value


def upload_json_gz(
    data: Any,
    layer: str,
    source: str,
    entity: str,
    partition_date: str = None,
) -> str:
    """
    Upload data as gzipped JSON to MinIO
    Returns the S3 key of uploaded object
    """
    s3 = get_s3_client()

    if partition_date is None:
        partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ใช้ strftime แทน isoformat() เพื่อหลีกเลี่ยง + ใน timezone
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # sanitize ทุก component ก่อนสร้าง key
    layer          = _sanitize_key(layer)
    source         = _sanitize_key(source)
    entity         = _sanitize_key(entity)
    partition_date = _sanitize_key(partition_date)

    key = f"{layer}/{source}/{entity}/date={partition_date}/{timestamp}.json.gz"

    logger.info(f"Uploading to key: {key}")

    json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    gz_bytes   = gzip.compress(json_bytes)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=gz_bytes,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    logger.info(f"Uploaded → s3://{BUCKET}/{key} ({len(gz_bytes):,} bytes)")
    return key          # ← return แค่ key ไม่ใช่ full URL


def download_json_gz(key: str) -> Any:
    """Download and decompress a JSON.gz object from MinIO"""
    s3 = get_s3_client()
    response   = s3.get_object(Bucket=BUCKET, Key=key)
    gz_bytes   = response["Body"].read()
    json_bytes = gzip.decompress(gz_bytes)
    return json.loads(json_bytes.decode("utf-8"))


def list_objects(prefix: str) -> list[str]:
    """List all object keys under a prefix"""
    s3        = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys      = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys