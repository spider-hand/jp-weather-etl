"""Publish final processed weather conditions."""

import json
from io import BytesIO
from typing import Final

import polars as pl
from dagster import MaterializeResult, asset

from pipeline.defs.assets.cleaned import POLLEN_INFO_COLUMNS, POLLEN_INFO_SCHEMA
from pipeline.defs.assets.weather import DAILY_WEATHER_SCHEMA
from storage import (
    PROCESSED_BUCKET,
    create_s3_client,
    upload_verified_payload,
)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
GEOJSON_CONTENT_TYPE = "application/geo+json"

DAILY_WEATHER_CONDITIONS_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "date": pl.Date,
        "observed_at": pl.Datetime("us", time_zone="Asia/Tokyo"),
        "station_name": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        **{
            name: dtype
            for name, dtype in DAILY_WEATHER_SCHEMA.items()
            if name not in ("station_id", "wmo_station_id", "date", "observed_at")
        },
        **{column: POLLEN_INFO_SCHEMA for column in POLLEN_INFO_COLUMNS.values()},
    }
)


def _merge_daily_weather_conditions(
    daily_weather: pl.DataFrame,
    wmo_stations: pl.DataFrame,
    pollen_cleaned: pl.DataFrame,
) -> pl.DataFrame:
    result = daily_weather.join(
        wmo_stations,
        on=["station_id", "wmo_station_id"],
        how="inner",
        validate="1:1",
    ).join(
        pollen_cleaned,
        on=["station_id", "date"],
        how="inner",
        validate="1:1",
    )
    if not (
        result.height
        == daily_weather.height
        == wmo_stations.height
        == pollen_cleaned.height
    ):
        raise ValueError("daily weather condition station IDs or dates do not match")
    if result.is_empty():
        raise ValueError("daily weather conditions cannot be empty")

    return (
        result.select(DAILY_WEATHER_CONDITIONS_SCHEMA.names())
        .cast(DAILY_WEATHER_CONDITIONS_SCHEMA, strict=True)
        .sort(["station_id", "date", "observed_at"])
    )


def _geojson_payload(frame: pl.DataFrame) -> bytes:
    features = []
    for row in json.loads(frame.write_json()):
        longitude = row.pop("longitude")
        latitude = row.pop("latitude")
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": row,
            }
        )
    return json.dumps(
        {"type": "FeatureCollection", "features": features},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


@asset(group_name="processed")
def daily_weather_conditions(
    daily_weather: pl.DataFrame,
    wmo_stations: pl.DataFrame,
    pollen_cleaned: pl.DataFrame,
) -> MaterializeResult:
    result = _merge_daily_weather_conditions(
        daily_weather,
        wmo_stations,
        pollen_cleaned,
    )
    observation_date = result["date"][0]
    observed_at = result["observed_at"][0]
    buffer = BytesIO()
    result.write_parquet(buffer)
    parquet_payload = buffer.getvalue()
    geojson_payload = _geojson_payload(result)
    object_prefix = f"{observation_date:%Y%m%d}/daily_weather_conditions"
    client = create_s3_client()
    storage_metadata = upload_verified_payload(
        client,
        bucket=PROCESSED_BUCKET,
        object_key=f"{object_prefix}.parquet",
        payload=parquet_payload,
        content_type=PARQUET_CONTENT_TYPE,
    )
    geojson_metadata = upload_verified_payload(
        client,
        bucket=PROCESSED_BUCKET,
        object_key=f"{object_prefix}.geojson",
        payload=geojson_payload,
        content_type=GEOJSON_CONTENT_TYPE,
    )
    return MaterializeResult(
        metadata={
            **storage_metadata,
            **{f"geojson_{key}": value for key, value in geojson_metadata.items()},
            "observation_date": observation_date.isoformat(),
            "observed_at": observed_at.isoformat(),
            "row_count": result.height,
            "column_count": result.width,
        }
    )
