import json
from unittest.mock import Mock

import pytest

from jp_weather_etl.storage import StorageSettings
from jp_weather_etl.storage.setup import setup_storage


def test_setup_storage_allows_browsers_to_read_processed_objects(monkeypatch):
    client = Mock()
    client.list_buckets.return_value = {
        "Buckets": [{"Name": "raw"}, {"Name": "processed"}]
    }
    monkeypatch.setattr("jp_weather_etl.storage.setup.create_s3_client", lambda: client)

    setup_storage()

    policy = json.loads(client.put_bucket_policy.call_args.kwargs["Policy"])
    assert client.put_bucket_policy.call_args.kwargs["Bucket"] == "processed"
    assert policy["Statement"] == [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::processed/*",
        }
    ]
    client.put_bucket_cors.assert_called_once_with(
        Bucket="processed",
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedOrigins": ["*"],
                }
            ]
        },
    )


def test_storage_settings_load_valid_environment(monkeypatch):
    # Arrange
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://storage.example.com:9000")

    # Act
    settings = StorageSettings.from_env()

    # Assert
    assert settings == StorageSettings(
        access_key_id="access-key",
        secret_access_key="secret-key",
        region="us-east-1",
        endpoint_url="https://storage.example.com:9000",
    )
    assert settings.endpoint == "storage.example.com:9000"
    assert settings.use_ssl is True


def test_storage_settings_report_missing_environment_variable(monkeypatch):
    # Arrange
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)

    # Act and Assert
    with pytest.raises(
        RuntimeError,
        match="Missing required storage environment variables: S3_ENDPOINT_URL",
    ):
        StorageSettings.from_env()


def test_storage_settings_reject_invalid_endpoint_url(monkeypatch):
    # Arrange
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", "storage.example.com:9000")

    # Act and Assert
    with pytest.raises(
        ValueError,
        match="S3_ENDPOINT_URL must be an http:// or https:// URL",
    ):
        StorageSettings.from_env()
