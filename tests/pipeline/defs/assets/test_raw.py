import json
from datetime import date, datetime
from io import BytesIO
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest
from dagster import build_asset_context

from jp_weather_etl.pipeline.defs.assets import raw
from jp_weather_etl.storage import RAW_BUCKET


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

    with pytest.raises(RuntimeError, match="Uploaded bytes do not match payload"):
        raw._ingest_raw_file(
            "https://example.test/source.csv",
            "precipitation.csv",
        )


def test_raw_asset_keys_match_files():
    assert {asset.key.to_user_string() for asset in raw.RAW_ASSETS} == set(
        raw.RAW_FILES
    ) | {"pollen_raw"}


def _pollen_response(day=2):
    return {
        "regionCode": "JP",
        "dailyInfo": [
            {
                "date": {"year": 2024, "month": 1, "day": day},
                "pollenTypeInfo": [{"code": "TREE", "displayName": "樹木"}],
            }
        ],
    }


def _wmo_stations():
    return pl.DataFrame(
        {
            "station_id": ["A", "B"],
            "wmo_station_id": ["WMO-A", "WMO-B"],
            "station_name": ["Alpha", "Beta"],
            "latitude": [35.0, 36.0],
            "longitude": [139.0, 140.0],
        }
    )


def test_pollen_request_parameters(monkeypatch):
    captured = {}
    response = BytesIO(json.dumps(_pollen_response(), ensure_ascii=False).encode())

    def fake_urlopen(url, timeout):
        captured.update(url=url, timeout=timeout)
        return response

    monkeypatch.setattr(raw, "urlopen", fake_urlopen)

    assert raw._download_pollen_response(35.0, 139.0, "secret") == _pollen_response()
    query = parse_qs(urlparse(captured["url"]).query)
    assert query == {
        "key": ["secret"],
        "location.latitude": ["35.0"],
        "location.longitude": ["139.0"],
        "days": ["1"],
        "languageCode": ["ja"],
    }
    assert captured["timeout"] == raw.DOWNLOAD_TIMEOUT_SECONDS


def test_pollen_request_rejects_invalid_json(monkeypatch):
    monkeypatch.setattr(
        raw,
        "urlopen",
        lambda *_args, **_kwargs: BytesIO(b"not json"),
    )

    with pytest.raises(json.JSONDecodeError):
        raw._download_pollen_response(35.0, 139.0, "secret")


def test_download_pollen_results_reports_progress(monkeypatch):
    messages = []
    monkeypatch.setattr(
        raw,
        "_download_pollen_response",
        lambda *_args: _pollen_response(),
    )

    results = raw._download_pollen_results(
        _wmo_stations(),
        "secret",
        date(2024, 1, 2),
        messages.append,
    )

    assert [result["station_id"] for result in results] == ["A", "B"]
    assert messages == [
        "Requesting pollen forecasts for 2 stations",
        "Requesting pollen forecast 1/2 for station 'A'",
        "Retrieved pollen forecast 1/2 for station 'A'",
        "Requesting pollen forecast 2/2 for station 'B'",
        "Retrieved pollen forecast 2/2 for station 'B'",
    ]


def test_pollen_raw_aggregates_and_stores_json(monkeypatch, s3_client):
    requests = []

    def download(latitude, longitude, api_key):
        requests.append((latitude, longitude, api_key))
        return _pollen_response()

    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "secret")
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)
    monkeypatch.setattr(raw, "_download_pollen_response", download)

    result = raw.pollen_raw(build_asset_context(), _wmo_stations())

    assert requests == [(35.0, 139.0, "secret"), (36.0, 140.0, "secret")]
    stored = s3_client.get_object(Bucket=RAW_BUCKET, Key="20240102/pollen.json")
    payload = stored["Body"].read()
    assert stored["ContentType"] == "application/json"
    assert json.loads(payload) == {
        "results": [
            {
                "station_id": "A",
                "latitude": 35.0,
                "longitude": 139.0,
                "response": _pollen_response(),
            },
            {
                "station_id": "B",
                "latitude": 36.0,
                "longitude": 140.0,
                "response": _pollen_response(),
            },
        ]
    }
    assert result.metadata["source_url"] == raw.POLLEN_FORECAST_URL
    assert result.metadata["station_count"] == 2


def test_pollen_raw_date_mismatch_does_not_upload(monkeypatch, s3_client):
    responses = iter([_pollen_response(), _pollen_response(day=3)])
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "secret")
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)
    monkeypatch.setattr(
        raw,
        "_download_pollen_response",
        lambda *_args: next(responses),
    )

    with pytest.raises(ValueError, match="Unexpected pollen forecast date"):
        raw.pollen_raw(build_asset_context(), _wmo_stations())

    assert "Contents" not in s3_client.list_objects_v2(Bucket=RAW_BUCKET)


def test_pollen_raw_requires_api_key(monkeypatch):
    monkeypatch.delenv("GOOGLE_MAPS_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Missing Google Maps API key"):
        raw.pollen_raw(build_asset_context(), _wmo_stations().clear())


@pytest.mark.parametrize("daily_info", [None, [], [{}, {}]])
def test_pollen_date_requires_exactly_one_daily_record(daily_info):
    with pytest.raises(ValueError, match="exactly one pollen forecast day"):
        raw._validate_pollen_date(
            {"dailyInfo": daily_info},
            date(2024, 1, 2),
            "A",
        )
