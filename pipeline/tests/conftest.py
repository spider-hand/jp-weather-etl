import boto3
import pytest
from moto import mock_aws

from pipeline.defs.assets import cleaned, processed, raw
from pipeline.storage import PROCESSED_BUCKET, RAW_BUCKET


@pytest.fixture
def s3_client(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=RAW_BUCKET)
        client.create_bucket(Bucket=PROCESSED_BUCKET)
        monkeypatch.setattr(raw, "create_s3_client", lambda: client)
        monkeypatch.setattr(cleaned, "create_s3_client", lambda: client)
        monkeypatch.setattr(processed, "create_s3_client", lambda: client)
        yield client
