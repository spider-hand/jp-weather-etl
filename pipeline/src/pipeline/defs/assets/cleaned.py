"""Parse raw JMA CSV objects into stable, typed DataFrames."""

from collections.abc import Callable
from datetime import date, datetime
from io import StringIO
import re
from typing import Final

from dagster import AssetExecutionContext, AssetKey, DagsterEventType, asset
import polars as pl

from pipeline.storage import RAW_BUCKET, create_s3_client

COMMON_COLUMN_MAPPING: Final = {
    "観測所番号": "station_id",
    "都道府県": "prefecture",
    "地点": "station_name",
    "国際地点番号": "wmo_station_id",
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
    "prefecture": pl.String,
    "station_name": pl.String,
    "wmo_station_id": pl.String,
    "date": pl.Date,
}
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
CLEANED_SCHEMAS: Final = {
    "precipitation_cleaned": PRECIPITATION_SCHEMA,
    "max_temperature_cleaned": MAX_TEMPERATURE_SCHEMA,
    "min_temperature_cleaned": MIN_TEMPERATURE_SCHEMA,
    "max_wind_cleaned": MAX_WIND_SCHEMA,
    "max_gust_cleaned": MAX_GUST_SCHEMA,
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
    return object_key, datetime.strptime(match.group("date"), "%Y%m%d").date()


def _read_raw_csv(object_key: str) -> pl.DataFrame:
    body = create_s3_client().get_object(Bucket=RAW_BUCKET, Key=object_key)["Body"]
    try:
        payload = body.read()
    finally:
        body.close()
    return pl.read_csv(StringIO(payload.decode("shift_jis")), infer_schema=False)


def _text(source: str, target: str) -> pl.Expr:
    return pl.col(source).str.strip_chars().replace("", None).alias(target)


def _common(observation_date: date) -> list[pl.Expr]:
    return [
        *(
            _text(source, target)
            for source, target in COMMON_COLUMN_MAPPING.items()
        ),
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


CLEANED_ASSETS = [
    precipitation_cleaned,
    max_temperature_cleaned,
    min_temperature_cleaned,
    max_wind_cleaned,
    max_gust_cleaned,
]
