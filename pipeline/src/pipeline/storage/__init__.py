"""S3-compatible storage configuration for the pipeline."""

import os
from hashlib import sha256

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


def upload_verified_payload(
    client, *, bucket: str, object_key: str, payload: bytes, content_type: str
) -> dict[str, str | int]:
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=payload,
        ContentType=content_type,
    )

    stored_body = client.get_object(Bucket=bucket, Key=object_key)["Body"]
    try:
        stored = stored_body.read()
    finally:
        stored_body.close()

    if stored != payload:
        raise RuntimeError(
            f"Uploaded bytes do not match payload for s3://{bucket}/{object_key}"
        )

    return {
        "destination_bucket": bucket,
        "destination_object_key": object_key,
        "destination_uri": f"s3://{bucket}/{object_key}",
        "file_size_bytes": len(payload),
        "sha256": sha256(payload).hexdigest(),
    }
