"""Parse raw source objects into stable, typed DataFrames."""

import json
import re
from collections.abc import Callable
from datetime import date
from enum import Enum
from io import StringIO
from typing import Any, Final

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    asset,
    asset_check,
)

from pipeline.storage import RAW_BUCKET, create_s3_client

COMMON_COLUMN_MAPPING: Final = {
    "観測所番号": "station_id",
}

# JMA column names use the observation day in place of ``{day}``.
# ``date`` is derived from the Raw object key rather than a JMA CSV column.
COLUMN_MAPPINGS: Final = {
    "precipitation_cleaned": {
        "{day}日の値(mm)": "precipitation_mm",
        "{day}日の値の品質情報": "precipitation_quality",
    },
    "max_temperature_cleaned": {
        "{day}日の最高気温(℃)": "max_temperature_c",
        "{day}日の最高気温の品質情報": "max_temperature_quality",
    },
    "min_temperature_cleaned": {
        "{day}日の最低気温(℃)": "min_temperature_c",
        "{day}日の最低気温の品質情報": "min_temperature_quality",
    },
    "max_wind_cleaned": {
        "{day}日の最大値(m/s)": "max_wind_speed_ms",
        "{day}日の最大値の品質情報": "max_wind_quality",
        "{day}日の最大値観測時の風向": "max_wind_direction",
        "{day}日の最大値観測時の風向の品質情報": "max_wind_direction_quality",
    },
    "max_gust_cleaned": {
        "{day}日の最大値(m/s)": "max_gust_speed_ms",
        "{day}日の最大値の品質情報": "max_gust_quality",
        "{day}日の最大値観測時の風向": "max_gust_direction",
        "{day}日の最大値観測時の風向の品質情報": "max_gust_direction_quality",
    },
}

COMMON_SCHEMA: Final = {
    "station_id": pl.String,
    "date": pl.Date,
}


# @see: https://developers.google.com/maps/documentation/pollen/reference/rest/v1/forecast/lookup#PollenType
class PollenType(str, Enum):
    POLLEN_TYPE_UNSPECIFIED = "POLLEN_TYPE_UNSPECIFIED"
    GRASS = "GRASS"
    TREE = "TREE"
    WEED = "WEED"


# @see: https://developers.google.com/maps/documentation/pollen/reference/rest/v1/forecast/lookup#Plant
class Plant(str, Enum):
    PLANT_UNSPECIFIED = "PLANT_UNSPECIFIED"
    ALDER = "ALDER"
    ASH = "ASH"
    BIRCH = "BIRCH"
    COTTONWOOD = "COTTONWOOD"
    ELM = "ELM"
    MAPLE = "MAPLE"
    OLIVE = "OLIVE"
    JUNIPER = "JUNIPER"
    OAK = "OAK"
    PINE = "PINE"
    CYPRESS_PINE = "CYPRESS_PINE"
    HAZEL = "HAZEL"
    GRAMINALES = "GRAMINALES"
    RAGWEED = "RAGWEED"
    MUGWORT = "MUGWORT"


POLLEN_TYPES: Final = tuple(
    item.value for item in PollenType if item is not PollenType.POLLEN_TYPE_UNSPECIFIED
)
PLANTS: Final = tuple(
    item.value for item in Plant if item is not Plant.PLANT_UNSPECIFIED
)
PRECIPITATION_SCHEMA: Final = pl.Schema(
    COMMON_SCHEMA
    | {
        "precipitation_mm": pl.Float64,
        "precipitation_quality": pl.Int64,
    }
)
MAX_TEMPERATURE_SCHEMA: Final = pl.Schema(
    COMMON_SCHEMA
    | {
        "max_temperature_c": pl.Float64,
        "max_temperature_quality": pl.Int64,
    }
)
MIN_TEMPERATURE_SCHEMA: Final = pl.Schema(
    COMMON_SCHEMA
    | {
        "min_temperature_c": pl.Float64,
        "min_temperature_quality": pl.Int64,
    }
)
MAX_WIND_SCHEMA: Final = pl.Schema(
    COMMON_SCHEMA
    | {
        "max_wind_speed_ms": pl.Float64,
        "max_wind_quality": pl.Int64,
        "max_wind_direction": pl.String,
        "max_wind_direction_quality": pl.Int64,
    }
)
MAX_GUST_SCHEMA: Final = pl.Schema(
    COMMON_SCHEMA
    | {
        "max_gust_speed_ms": pl.Float64,
        "max_gust_quality": pl.Int64,
        "max_gust_direction": pl.String,
        "max_gust_direction_quality": pl.Int64,
    }
)
POLLEN_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "date": pl.Date,
        "pollen_code": pl.Enum(POLLEN_TYPES),
        "pollen_value": pl.Int64,
        "plant_code": pl.Enum(PLANTS),
        "plant_value": pl.Int64,
    }
)
CLEANED_SCHEMAS: Final = {
    "precipitation_cleaned": PRECIPITATION_SCHEMA,
    "max_temperature_cleaned": MAX_TEMPERATURE_SCHEMA,
    "min_temperature_cleaned": MIN_TEMPERATURE_SCHEMA,
    "max_wind_cleaned": MAX_WIND_SCHEMA,
    "max_gust_cleaned": MAX_GUST_SCHEMA,
    "pollen_cleaned": POLLEN_SCHEMA,
}


def find_column(columns: list[str], pattern: str) -> str:
    matches = [column for column in columns if re.fullmatch(pattern, column)]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one JMA column matching {pattern!r}, found {matches!r}. "
            f"Available columns: {columns!r}"
        )
    return matches[0]


def _source_object(
    context: AssetExecutionContext, raw_asset: str, filename: str
) -> tuple[str, date]:
    records = context.instance.get_records_for_run(
        context.run.run_id,
        of_type=DagsterEventType.ASSET_MATERIALIZATION,
    ).records
    materializations = [
        record.event_log_entry.dagster_event.event_specific_data.materialization
        for record in records
        if record.event_log_entry.dagster_event.asset_key == AssetKey(raw_asset)
    ]
    if len(materializations) != 1:
        raise RuntimeError(
            f"Expected exactly one {raw_asset} materialization in run {context.run.run_id}, "
            f"found {len(materializations)}"
        )

    metadata_value = materializations[0].metadata.get("destination_object_key")
    object_key = getattr(metadata_value, "value", None)
    match = re.fullmatch(rf"(?P<date>\d{{8}})/{re.escape(filename)}", object_key or "")
    if not match:
        raise RuntimeError(
            f"Invalid destination_object_key for {raw_asset}: {object_key!r}; "
            f"expected YYYYMMDD/{filename}"
        )
    raw_date = match.group("date")
    return object_key, date.fromisoformat(
        f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    )


def _read_raw_csv(object_key: str) -> pl.DataFrame:
    body = create_s3_client().get_object(Bucket=RAW_BUCKET, Key=object_key)["Body"]
    try:
        payload = body.read()
    finally:
        body.close()
    return pl.read_csv(StringIO(payload.decode("shift_jis")), infer_schema=False)


def _read_raw_json(object_key: str) -> dict[str, Any]:
    body = create_s3_client().get_object(Bucket=RAW_BUCKET, Key=object_key)["Body"]
    try:
        payload = json.load(body)
    finally:
        body.close()
    if not isinstance(payload, dict):
        raise TypeError("Raw JSON payload must be an object")
    return payload


def _text(source: str, target: str) -> pl.Expr:
    return pl.col(source).str.strip_chars().replace("", None).alias(target)


def _common(observation_date: date) -> list[pl.Expr]:
    return [
        *(_text(source, target) for source, target in COMMON_COLUMN_MAPPING.items()),
        pl.lit(observation_date, dtype=pl.Date).alias("date"),
    ]


def _clean_precipitation(frame: pl.DataFrame, observation_date: date) -> pl.DataFrame:
    value = find_column(frame.columns, r"\d{1,2}日の値\(mm\)")
    quality = find_column(frame.columns, r"\d{1,2}日の値の品質情報")
    return frame.select(
        *_common(observation_date),
        _text(value, "precipitation_mm"),
        _text(quality, "precipitation_quality"),
    ).cast(PRECIPITATION_SCHEMA, strict=True)


def _clean_max_temperature(frame: pl.DataFrame, observation_date: date) -> pl.DataFrame:
    value = find_column(frame.columns, r"\d{1,2}日の最高気温\(℃\)")
    quality = find_column(frame.columns, r"\d{1,2}日の最高気温の品質情報")
    return frame.select(
        *_common(observation_date),
        _text(value, "max_temperature_c"),
        _text(quality, "max_temperature_quality"),
    ).cast(MAX_TEMPERATURE_SCHEMA, strict=True)


def _clean_min_temperature(frame: pl.DataFrame, observation_date: date) -> pl.DataFrame:
    value = find_column(frame.columns, r"\d{1,2}日の最低気温\(℃\)")
    quality = find_column(frame.columns, r"\d{1,2}日の最低気温の品質情報")
    return frame.select(
        *_common(observation_date),
        _text(value, "min_temperature_c"),
        _text(quality, "min_temperature_quality"),
    ).cast(MIN_TEMPERATURE_SCHEMA, strict=True)


def _clean_max_wind(frame: pl.DataFrame, observation_date: date) -> pl.DataFrame:
    value = find_column(frame.columns, r"\d{1,2}日の最大値\(m/s\)")
    quality = find_column(frame.columns, r"\d{1,2}日の最大値の品質情報")
    direction = find_column(frame.columns, r"\d{1,2}日の最大値観測時の風向")
    direction_quality = find_column(
        frame.columns, r"\d{1,2}日の最大値観測時の風向の品質情報"
    )
    return frame.select(
        *_common(observation_date),
        _text(value, "max_wind_speed_ms"),
        _text(quality, "max_wind_quality"),
        _text(direction, "max_wind_direction"),
        _text(direction_quality, "max_wind_direction_quality"),
    ).cast(MAX_WIND_SCHEMA, strict=True)


def _clean_max_gust(frame: pl.DataFrame, observation_date: date) -> pl.DataFrame:
    value = find_column(frame.columns, r"\d{1,2}日の最大値\(m/s\)")
    quality = find_column(frame.columns, r"\d{1,2}日の最大値の品質情報")
    direction = find_column(frame.columns, r"\d{1,2}日の最大値観測時の風向")
    direction_quality = find_column(
        frame.columns, r"\d{1,2}日の最大値観測時の風向の品質情報"
    )
    return frame.select(
        *_common(observation_date),
        _text(value, "max_gust_speed_ms"),
        _text(quality, "max_gust_quality"),
        _text(direction, "max_gust_direction"),
        _text(direction_quality, "max_gust_direction_quality"),
    ).cast(MAX_GUST_SCHEMA, strict=True)


def _clean_pollen(payload: dict[str, Any], observation_date: date) -> pl.DataFrame:
    results = payload.get("results")
    if not isinstance(results, list):
        raise TypeError("Pollen payload must contain a results list")

    rows = []
    for result in results:
        station_id = result["station_id"]
        response = result["response"]
        daily_info = response.get("dailyInfo")
        if not isinstance(daily_info, list) or len(daily_info) != 1:
            raise ValueError(
                f"Expected exactly one pollen forecast day for station {station_id!r}"
            )

        day = daily_info[0]
        # Ignore unknown and unspecified pollen codes; keep missing values as null.
        pollen_values = {
            item["code"]: (item.get("indexInfo") or {}).get("value")
            for item in day.get("pollenTypeInfo", [])
            if item.get("code") in POLLEN_TYPES
        }
        codes_with_plants = set()
        for plant in day.get("plantInfo", []):
            plant_code = plant.get("code")
            description = plant.get("plantDescription") or {}
            pollen_code = description.get("type")
            # A plant needs both a documented code and a documented pollen group.
            if plant_code not in PLANTS or pollen_code not in POLLEN_TYPES:
                continue
            codes_with_plants.add(pollen_code)
            rows.append(
                {
                    "station_id": station_id,
                    "date": observation_date,
                    "pollen_code": pollen_code,
                    "pollen_value": pollen_values.get(pollen_code),
                    "plant_code": plant_code,
                    "plant_value": (plant.get("indexInfo") or {}).get("value"),
                }
            )

        # Preserve a valid type-level value even when it has no valid plant rows.
        for pollen_code, pollen_value in pollen_values.items():
            if pollen_code not in codes_with_plants:
                rows.append(
                    {
                        "station_id": station_id,
                        "date": observation_date,
                        "pollen_code": pollen_code,
                        "pollen_value": pollen_value,
                        "plant_code": None,
                        "plant_value": None,
                    }
                )

    return pl.DataFrame(rows, schema=POLLEN_SCHEMA).sort(
        "station_id", "pollen_code", "plant_code", nulls_last=True
    )


def _materialize_cleaned(
    context: AssetExecutionContext,
    raw_asset: str,
    filename: str,
    transform: Callable[[pl.DataFrame, date], pl.DataFrame],
    measurement: str,
) -> pl.DataFrame:
    object_key, observation_date = _source_object(context, raw_asset, filename)
    cleaned = transform(_read_raw_csv(object_key), observation_date)
    context.add_output_metadata(
        {
            "row_count": cleaned.height,
            "column_count": cleaned.width,
            "source_object_key": object_key,
            "observation_date": observation_date.isoformat(),
            "null_count": cleaned[measurement].null_count(),
        }
    )
    return cleaned


@asset(group_name="cleaned", deps=[AssetKey("precipitation_raw")])
def precipitation_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    return _materialize_cleaned(
        context,
        "precipitation_raw",
        "precipitation.csv",
        _clean_precipitation,
        "precipitation_mm",
    )


@asset(group_name="cleaned", deps=[AssetKey("max_temperature_raw")])
def max_temperature_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    return _materialize_cleaned(
        context,
        "max_temperature_raw",
        "max_temperature.csv",
        _clean_max_temperature,
        "max_temperature_c",
    )


@asset(group_name="cleaned", deps=[AssetKey("min_temperature_raw")])
def min_temperature_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    return _materialize_cleaned(
        context,
        "min_temperature_raw",
        "min_temperature.csv",
        _clean_min_temperature,
        "min_temperature_c",
    )


@asset(group_name="cleaned", deps=[AssetKey("max_wind_raw")])
def max_wind_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    return _materialize_cleaned(
        context,
        "max_wind_raw",
        "max_wind.csv",
        _clean_max_wind,
        "max_wind_speed_ms",
    )


@asset(group_name="cleaned", deps=[AssetKey("max_gust_raw")])
def max_gust_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    return _materialize_cleaned(
        context,
        "max_gust_raw",
        "max_gust.csv",
        _clean_max_gust,
        "max_gust_speed_ms",
    )


@asset(group_name="cleaned", deps=[AssetKey("pollen_raw")])
def pollen_cleaned(context: AssetExecutionContext) -> pl.DataFrame:
    object_key, observation_date = _source_object(context, "pollen_raw", "pollen.json")
    cleaned = _clean_pollen(_read_raw_json(object_key), observation_date)
    context.add_output_metadata(
        {
            "row_count": cleaned.height,
            "column_count": cleaned.width,
            "source_object_key": object_key,
            "observation_date": observation_date.isoformat(),
            "null_pollen_value_count": cleaned["pollen_value"].null_count(),
            "null_plant_value_count": cleaned["plant_value"].null_count(),
        }
    )
    return cleaned


CLEANED_ASSETS = [
    precipitation_cleaned,
    max_temperature_cleaned,
    min_temperature_cleaned,
    max_wind_cleaned,
    max_gust_cleaned,
    pollen_cleaned,
]

JOIN_KEYS = ["station_id", "date"]


def _valid_daily_weather_key(frame: pl.DataFrame) -> AssetCheckResult:
    null_key_row_count = frame.filter(
        pl.any_horizontal(*(pl.col(column).is_null() for column in JOIN_KEYS))
    ).height
    duplicate_key_count = (
        frame.group_by(JOIN_KEYS).len().filter(pl.col("len") > 1).height
    )
    return AssetCheckResult(
        passed=null_key_row_count == 0 and duplicate_key_count == 0,
        metadata={
            "null_key_row_count": null_key_row_count,
            "duplicate_key_count": duplicate_key_count,
        },
    )


def _valid_pollen_values(frame: pl.DataFrame) -> AssetCheckResult:
    invalid_pollen_value_count = frame.filter(
        pl.col("pollen_value").is_not_null() & ~pl.col("pollen_value").is_between(0, 5)
    ).height
    invalid_plant_value_count = frame.filter(
        pl.col("plant_value").is_not_null() & ~pl.col("plant_value").is_between(0, 5)
    ).height
    return AssetCheckResult(
        passed=invalid_pollen_value_count == 0 and invalid_plant_value_count == 0,
        metadata={
            "invalid_pollen_value_count": invalid_pollen_value_count,
            "invalid_plant_value_count": invalid_plant_value_count,
        },
    )


@asset_check(
    asset=precipitation_cleaned,
    name="valid_daily_weather_key",
    blocking=True,
)
def precipitation_cleaned_valid_daily_weather_key(
    precipitation_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_daily_weather_key(precipitation_cleaned)


@asset_check(
    asset=max_temperature_cleaned,
    name="valid_daily_weather_key",
    blocking=True,
)
def max_temperature_cleaned_valid_daily_weather_key(
    max_temperature_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_daily_weather_key(max_temperature_cleaned)


@asset_check(
    asset=min_temperature_cleaned,
    name="valid_daily_weather_key",
    blocking=True,
)
def min_temperature_cleaned_valid_daily_weather_key(
    min_temperature_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_daily_weather_key(min_temperature_cleaned)


@asset_check(
    asset=max_wind_cleaned,
    name="valid_daily_weather_key",
    blocking=True,
)
def max_wind_cleaned_valid_daily_weather_key(
    max_wind_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_daily_weather_key(max_wind_cleaned)


@asset_check(
    asset=max_gust_cleaned,
    name="valid_daily_weather_key",
    blocking=True,
)
def max_gust_cleaned_valid_daily_weather_key(
    max_gust_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_daily_weather_key(max_gust_cleaned)


@asset_check(
    asset=pollen_cleaned,
    name="valid_pollen_values",
    blocking=True,
)
def pollen_cleaned_valid_pollen_values(
    pollen_cleaned: pl.DataFrame,
) -> AssetCheckResult:
    return _valid_pollen_values(pollen_cleaned)


CLEANED_KEY_CHECKS = [
    precipitation_cleaned_valid_daily_weather_key,
    max_temperature_cleaned_valid_daily_weather_key,
    min_temperature_cleaned_valid_daily_weather_key,
    max_wind_cleaned_valid_daily_weather_key,
    max_gust_cleaned_valid_daily_weather_key,
]

CLEANED_CHECKS = [*CLEANED_KEY_CHECKS, pollen_cleaned_valid_pollen_values]
