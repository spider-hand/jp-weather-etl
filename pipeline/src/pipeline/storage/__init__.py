"""S3-compatible storage configuration for the pipeline."""

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
    """Create a path-style S3 client from the project's environment variables."""
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            "Missing required storage environment variables: " + ", ".join(missing)
        )

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
