"""S3-compatible storage configuration shared by the project."""

import os
from dataclasses import dataclass
from hashlib import sha256
from urllib.parse import urlsplit

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


@dataclass(frozen=True)
class StorageSettings:
    """Validated S3-compatible storage settings."""

    access_key_id: str
    secret_access_key: str
    region: str
    endpoint_url: str

    @classmethod
    def from_env(cls) -> StorageSettings:
        values = {name: os.environ.get(name) for name in _REQUIRED_ENV}
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise RuntimeError(
                "Missing required storage environment variables: " + ", ".join(missing)
            )

        endpoint_url = values["S3_ENDPOINT_URL"]
        endpoint = urlsplit(endpoint_url)
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
            raise ValueError("S3_ENDPOINT_URL must be an http:// or https:// URL")

        return cls(
            access_key_id=values["AWS_ACCESS_KEY_ID"],
            secret_access_key=values["AWS_SECRET_ACCESS_KEY"],
            region=values["AWS_DEFAULT_REGION"],
            endpoint_url=endpoint_url,
        )

    @property
    def endpoint(self) -> str:
        return urlsplit(self.endpoint_url).netloc

    @property
    def use_ssl(self) -> bool:
        return urlsplit(self.endpoint_url).scheme == "https"


def create_s3_client(settings: StorageSettings | None = None):
    """Create a path-style S3 client from the project's environment variables."""
    settings = settings or StorageSettings.from_env()

    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
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
