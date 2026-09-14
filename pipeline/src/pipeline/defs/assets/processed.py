"""Publish final processed weather conditions."""

from datetime import date, datetime
from io import BytesIO
from typing import Final
from zoneinfo import ZoneInfo

import polars as pl
from dagster import MaterializeResult, asset

from pipeline.defs.assets.cleaned import POLLEN_INFO_COLUMNS, POLLEN_INFO_SCHEMA
from pipeline.defs.assets.weather import DAILY_WEATHER_SCHEMA
from pipeline.storage import (
    PROCESSED_BUCKET,
    create_s3_client,
    upload_verified_payload,
)

JST = ZoneInfo("Asia/Tokyo")
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"

DAILY_WEATHER_CONDITIONS_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "date": pl.Date,
        "station_name": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        **{
            name: dtype
            for name, dtype in DAILY_WEATHER_SCHEMA.items()
            if name not in ("station_id", "date")
        },
        **{column: POLLEN_INFO_SCHEMA for column in POLLEN_INFO_COLUMNS.values()},
    }
)


def _observation_date(
    daily_weather: pl.DataFrame, pollen_cleaned: pl.DataFrame
) -> date:
    dates = (
        pl.concat(
            [
                daily_weather.select("date"),
                pollen_cleaned.select("date"),
            ]
        )["date"]
        .unique()
        .to_list()
    )
    if not dates:
        return datetime.now(JST).date()
    if len(dates) != 1 or dates[0] is None:
        raise ValueError(
            "daily_weather_conditions requires one shared observation date"
        )
    return dates[0]


def _merge_daily_weather_conditions(
    daily_weather: pl.DataFrame,
    active_stations: pl.DataFrame,
    pollen_cleaned: pl.DataFrame,
) -> tuple[pl.DataFrame, date]:
    observation_date = _observation_date(daily_weather, pollen_cleaned)
    result = daily_weather.join(
        active_stations,
        on="station_id",
        how="inner",
        validate="1:1",
    ).join(
        pollen_cleaned.drop("date"),
        on="station_id",
        how="inner",
        validate="1:1",
    )
    if not (
        result.height
        == daily_weather.height
        == active_stations.height
        == pollen_cleaned.height
    ):
        raise ValueError("daily weather condition station IDs do not match")

    return (
        result.select(DAILY_WEATHER_CONDITIONS_SCHEMA.names())
        .cast(DAILY_WEATHER_CONDITIONS_SCHEMA, strict=True)
        .sort("station_id"),
        observation_date,
    )


@asset(group_name="processed")
def daily_weather_conditions(
    daily_weather: pl.DataFrame,
    active_stations: pl.DataFrame,
    pollen_cleaned: pl.DataFrame,
) -> MaterializeResult:
    result, observation_date = _merge_daily_weather_conditions(
        daily_weather,
        active_stations,
        pollen_cleaned,
    )
    buffer = BytesIO()
    result.write_parquet(buffer)
    object_key = f"{observation_date:%Y%m%d}/daily_weather_conditions.parquet"
    storage_metadata = upload_verified_payload(
        create_s3_client(),
        bucket=PROCESSED_BUCKET,
        object_key=object_key,
        payload=buffer.getvalue(),
        content_type=PARQUET_CONTENT_TYPE,
    )
    return MaterializeResult(
        metadata={
            **storage_metadata,
            "observation_date": observation_date.isoformat(),
            "row_count": result.height,
            "column_count": result.width,
        }
    )
