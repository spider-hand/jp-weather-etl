"""Combine the cleaned JMA observations into one daily weather asset."""

from typing import Final

import polars as pl
from dagster import AssetExecutionContext, asset

DAILY_WEATHER_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "date": pl.Date,
        "precipitation_mm": pl.Float64,
        "precipitation_quality": pl.Int64,
        "max_temperature_c": pl.Float64,
        "max_temperature_quality": pl.Int64,
        "min_temperature_c": pl.Float64,
        "min_temperature_quality": pl.Int64,
        "max_wind_speed_ms": pl.Float64,
        "max_wind_quality": pl.Int64,
        "max_wind_direction": pl.String,
        "max_wind_direction_quality": pl.Int64,
        "max_gust_speed_ms": pl.Float64,
        "max_gust_quality": pl.Int64,
        "max_gust_direction": pl.String,
        "max_gust_direction_quality": pl.Int64,
    }
)

JOIN_KEYS = ["station_id", "date"]


def _merge_daily_weather(
    precipitation_cleaned: pl.DataFrame,
    max_temperature_cleaned: pl.DataFrame,
    min_temperature_cleaned: pl.DataFrame,
    max_wind_cleaned: pl.DataFrame,
    max_gust_cleaned: pl.DataFrame,
) -> pl.DataFrame:
    result = precipitation_cleaned
    for frame in (
        max_temperature_cleaned,
        min_temperature_cleaned,
        max_wind_cleaned,
        max_gust_cleaned,
    ):
        result = result.join(frame, on=JOIN_KEYS, how="full", coalesce=True)

    return (
        result.select(DAILY_WEATHER_SCHEMA.names())
        .cast(DAILY_WEATHER_SCHEMA, strict=True)
        .sort(JOIN_KEYS)
    )


@asset(group_name="daily")
def daily_weather(
    context: AssetExecutionContext,
    precipitation_cleaned: pl.DataFrame,
    max_temperature_cleaned: pl.DataFrame,
    min_temperature_cleaned: pl.DataFrame,
    max_wind_cleaned: pl.DataFrame,
    max_gust_cleaned: pl.DataFrame,
) -> pl.DataFrame:
    result = _merge_daily_weather(
        precipitation_cleaned,
        max_temperature_cleaned,
        min_temperature_cleaned,
        max_wind_cleaned,
        max_gust_cleaned,
    )
    context.add_output_metadata(
        {
            "row_count": result.height,
            "column_count": result.width,
        }
    )
    return result
