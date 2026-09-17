import pytest

from storage import StorageSettings


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
