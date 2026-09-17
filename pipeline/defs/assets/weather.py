"""Combine the cleaned JMA observations into one daily weather asset."""

from typing import Final

import polars as pl
from dagster import AssetExecutionContext, asset

DAILY_WEATHER_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "wmo_station_id": pl.String,
        "date": pl.Date,
        "observed_at": pl.Datetime("us", time_zone="Asia/Tokyo"),
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

JOIN_KEYS = ["station_id", "wmo_station_id", "date"]


def _merge_daily_weather(
    precipitation_cleaned: pl.DataFrame,
    max_temperature_cleaned: pl.DataFrame,
    min_temperature_cleaned: pl.DataFrame,
    max_wind_cleaned: pl.DataFrame,
    max_gust_cleaned: pl.DataFrame,
) -> pl.DataFrame:
    frames = (
        precipitation_cleaned,
        max_temperature_cleaned,
        min_temperature_cleaned,
        max_wind_cleaned,
        max_gust_cleaned,
    )
    if len({frame["date"][0] for frame in frames}) != 1:
        raise ValueError("daily_weather requires one shared observation date")
    observed_at = min(frame["observed_at"][0] for frame in frames)
    frames = tuple(
        frame.filter(pl.col("wmo_station_id").is_not_null()) for frame in frames
    )
    result = frames[0].drop("observed_at")
    for frame in frames[1:]:
        result = result.join(
            frame.drop("observed_at"), on=JOIN_KEYS, how="full", coalesce=True
        )

    conflicting_ids = (
        result.group_by("station_id", "date")
        .len()
        .filter(pl.col("len") > 1)["station_id"]
        .sort()
        .to_list()
    )
    if conflicting_ids:
        raise ValueError(
            f"Conflicting WMO station IDs for station IDs: {conflicting_ids!r}"
        )

    return (
        result.with_columns(
            pl.lit(observed_at, dtype=DAILY_WEATHER_SCHEMA["observed_at"]).alias(
                "observed_at"
            )
        )
        .select(DAILY_WEATHER_SCHEMA.names())
        .cast(DAILY_WEATHER_SCHEMA, strict=True)
        .sort(["station_id", "date", "observed_at"])
    )


@asset(group_name="weather")
def daily_weather(
    context: AssetExecutionContext,
    precipitation_cleaned: pl.DataFrame,
    max_temperature_cleaned: pl.DataFrame,
    min_temperature_cleaned: pl.DataFrame,
    max_wind_cleaned: pl.DataFrame,
    max_gust_cleaned: pl.DataFrame,
) -> pl.DataFrame:
    frames = (
        precipitation_cleaned,
        max_temperature_cleaned,
        min_temperature_cleaned,
        max_wind_cleaned,
        max_gust_cleaned,
    )
    result = _merge_daily_weather(
        *frames,
    )
    source_hours = {
        "precipitation": precipitation_cleaned["observed_at"][0],
        "max_temperature": max_temperature_cleaned["observed_at"][0],
        "min_temperature": min_temperature_cleaned["observed_at"][0],
        "max_wind": max_wind_cleaned["observed_at"][0],
        "max_gust": max_gust_cleaned["observed_at"][0],
    }
    observation_date = result["date"][0]
    observed_at = result["observed_at"][0]
    if len(set(source_hours.values())) > 1:
        context.log.warning(
            "JMA source observed_at values differ; using the oldest hour: "
            f"{observed_at.isoformat()}"
        )
    context.add_output_metadata(
        {
            "row_count": result.height,
            "column_count": result.width,
            "observation_date": observation_date.isoformat(),
            "observed_at": observed_at.isoformat(),
            **{
                f"{name}_observed_at": timestamp.isoformat()
                for name, timestamp in source_hours.items()
            },
        }
    )
    return result
