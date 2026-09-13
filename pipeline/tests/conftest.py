import boto3
import pytest
from moto import mock_aws

from pipeline.defs import raw_ingestion
from pipeline.storage import RAW_BUCKET


@pytest.fixture
def s3_client(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=RAW_BUCKET)
        monkeypatch.setattr(raw_ingestion, "create_s3_client", lambda: client)
        yield client
