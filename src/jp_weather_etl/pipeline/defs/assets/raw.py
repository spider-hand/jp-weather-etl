"""Copy source data to raw storage with minimal transformation."""

import json
import os
from collections.abc import Callable
from datetime import date, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import polars as pl
from dagster import AssetExecutionContext, MaterializeResult, asset

from jp_weather_etl.storage import (
    RAW_BUCKET,
    create_s3_client,
    upload_verified_payload,
)

JST = ZoneInfo("Asia/Tokyo")
DOWNLOAD_TIMEOUT_SECONDS = 30
POLLEN_FORECAST_URL = "https://pollen.googleapis.com/v1/forecast:lookup"

RAW_FILES = {
    "precipitation_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/pre_rct/alltable/predaily00_rct.csv",
        "precipitation.csv",
    ),
    "max_temperature_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/tem_rct/alltable/mxtemsadext00_rct.csv",
        "max_temperature.csv",
    ),
    "min_temperature_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/tem_rct/alltable/mntemsadext00_rct.csv",
        "min_temperature.csv",
    ),
    "max_wind_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/wind_rct/alltable/mxwsp00_rct.csv",
        "max_wind.csv",
    ),
    "max_gust_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/wind_rct/alltable/gust00_rct.csv",
        "max_gust.csv",
    ),
}


def _download_bytes(source_url: str) -> tuple[bytes, str]:
    with urlopen(source_url, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
        return response.read(), response.headers.get("Content-Type", "")


def _store_raw_payload(
    payload: bytes,
    filename: str,
    content_type: str,
    timestamp: datetime,
    **metadata: Any,
) -> MaterializeResult:
    object_key = f"{timestamp:%Y%m%d}/{filename}"
    storage_metadata = upload_verified_payload(
        create_s3_client(),
        bucket=RAW_BUCKET,
        object_key=object_key,
        payload=payload,
        content_type=content_type,
    )

    return MaterializeResult(
        metadata={
            **metadata,
            **storage_metadata,
            "downloaded_at_jst": timestamp.isoformat(),
            "response_content_type": content_type,
        }
    )


def _ingest_raw_file(
    source_url: str,
    filename: str,
) -> MaterializeResult:
    payload, content_type = _download_bytes(source_url)
    return _store_raw_payload(
        payload,
        filename,
        content_type,
        datetime.now(JST),
        source_url=source_url,
    )


def _download_pollen_response(
    latitude: float,
    longitude: float,
    api_key: str,
) -> dict[str, Any]:
    query = urlencode(
        {
            "key": api_key,
            "location.latitude": latitude,
            "location.longitude": longitude,
            "days": 1,
            "languageCode": "ja",
        }
    )
    try:
        with urlopen(
            f"{POLLEN_FORECAST_URL}?{query}",
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        ) as response:
            result = json.loads(response.read())
    except HTTPError as error:
        raise RuntimeError(
            f"Pollen API request failed with HTTP status {error.code}"
        ) from None
    except URLError:
        raise RuntimeError("Pollen API request failed") from None

    if not isinstance(result, dict):
        raise TypeError("Pollen API response must be a JSON object")
    return result


def _validate_pollen_date(
    response: dict[str, Any], expected_date: date, station_id: str
) -> None:
    daily_info = response.get("dailyInfo")
    if not isinstance(daily_info, list) or len(daily_info) != 1:
        raise ValueError(
            f"Expected exactly one pollen forecast day for station {station_id!r}"
        )

    response_date = (
        daily_info[0].get("date") if isinstance(daily_info[0], dict) else None
    )
    expected = {
        "year": expected_date.year,
        "month": expected_date.month,
        "day": expected_date.day,
    }
    if response_date != expected:
        raise ValueError(
            f"Unexpected pollen forecast date for station {station_id!r}: "
            f"expected {expected!r}, got {response_date!r}"
        )


def _download_pollen_results(
    wmo_stations: pl.DataFrame,
    api_key: str,
    expected_date: date,
    log_info: Callable[[str], None],
) -> list[dict[str, Any]]:
    station_count = wmo_stations.height
    log_info(f"Requesting pollen forecasts for {station_count} stations")
    results = []
    for index, station in enumerate(wmo_stations.iter_rows(named=True), start=1):
        station_id = station["station_id"]
        latitude = station["latitude"]
        longitude = station["longitude"]
        log_info(
            f"Requesting pollen forecast {index}/{station_count} "
            f"for station {station_id!r}"
        )
        response = _download_pollen_response(latitude, longitude, api_key)
        _validate_pollen_date(response, expected_date, station_id)
        results.append(
            {
                "station_id": station_id,
                "latitude": latitude,
                "longitude": longitude,
                "response": response,
            }
        )
        log_info(
            f"Retrieved pollen forecast {index}/{station_count} "
            f"for station {station_id!r}"
        )
    return results


@asset(group_name="raw")
def precipitation_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["precipitation_raw"])


@asset(group_name="raw")
def max_temperature_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_temperature_raw"])


@asset(group_name="raw")
def min_temperature_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["min_temperature_raw"])


@asset(group_name="raw")
def max_wind_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_wind_raw"])


@asset(group_name="raw")
def max_gust_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_gust_raw"])


@asset(group_name="raw")
def pollen_raw(
    context: AssetExecutionContext, wmo_stations: pl.DataFrame
) -> MaterializeResult:
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise RuntimeError("Missing Google Maps API key")

    timestamp = datetime.now(JST)
    results = _download_pollen_results(
        wmo_stations,
        api_key,
        timestamp.date(),
        context.log.info,
    )

    payload = json.dumps(
        {"results": results}, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return _store_raw_payload(
        payload,
        "pollen.json",
        "application/json",
        timestamp,
        source_url=POLLEN_FORECAST_URL,
        station_count=len(results),
    )


RAW_ASSETS = [
    precipitation_raw,
    max_temperature_raw,
    min_temperature_raw,
    max_wind_raw,
    max_gust_raw,
    pollen_raw,
]
