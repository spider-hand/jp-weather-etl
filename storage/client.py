"""Shared S3 client configuration."""

import os

import boto3
from botocore.config import Config

RAW_BUCKET = "raw"
PROCESSED_BUCKET = "processed"
BUCKETS = (RAW_BUCKET, PROCESSED_BUCKET)

_REQUIRED_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_ENDPOINT_URL",
)


def create_s3_client():
    """Create a path-style S3 client for the configured endpoint."""
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"Missing required storage environment variables: {names}")

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
