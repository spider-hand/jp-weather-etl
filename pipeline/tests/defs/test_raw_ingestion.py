from datetime import datetime
from io import BytesIO

import pytest
from pipeline.defs.assets import raw
from pipeline.storage import RAW_BUCKET


class FrozenDateTime:
    @classmethod
    def now(cls, timezone):
        assert timezone is raw.JST
        return datetime(2024, 1, 2, 23, 30, tzinfo=timezone)


def test_ingestion_preserves_bytes_and_uses_jst_date(monkeypatch, s3_client):
    payload = b"\x8a\xcf\x91\xaa,\x93\x8c\x8b\x9e\r\n"
    monkeypatch.setattr(
        raw,
        "_download_bytes",
        lambda _url: (payload, "text/csv"),
    )
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)

    result = raw._ingest_raw_file(
        "https://example.test/source.csv",
        "precipitation.csv",
    )

    stored = s3_client.get_object(
        Bucket=RAW_BUCKET,
        Key="20240102/precipitation.csv",
    )
    assert stored["Body"].read() == payload
    assert stored["ContentType"] == "text/csv"
    assert result.metadata == {
        "source_url": "https://example.test/source.csv",
        "destination_bucket": "raw",
        "destination_object_key": "20240102/precipitation.csv",
        "destination_uri": "s3://raw/20240102/precipitation.csv",
        "downloaded_at_jst": "2024-01-02T23:30:00+09:00",
        "file_size_bytes": len(payload),
        "sha256": "1d8e4e62eba71b0b04fdea0c88888adb111c5d5b440a191b9f970bb855211542",
        "response_content_type": "text/csv",
    }


def test_ingestion_fails_when_stored_bytes_differ(monkeypatch, s3_client):
    monkeypatch.setattr(raw, "_download_bytes", lambda _url: (b"original", ""))
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)
    monkeypatch.setattr(
        s3_client,
        "get_object",
        lambda **_kwargs: {"Body": BytesIO(b"changed")},
    )

    with pytest.raises(RuntimeError, match="Uploaded bytes do not match download"):
        raw._ingest_raw_file(
            "https://example.test/source.csv",
            "precipitation.csv",
        )


def test_raw_asset_keys_match_files():
    assert {asset.key.to_user_string() for asset in raw.RAW_ASSETS} == set(
        raw.RAW_FILES
    )
