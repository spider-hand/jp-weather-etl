from datetime import date, datetime
from hashlib import sha256
from io import BytesIO
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from pipeline.defs.assets import cleaned, processed, stations, weather
from pipeline.storage import PROCESSED_BUCKET

OBSERVATION_DATE = date(2026, 9, 13)
OBSERVED_AT = datetime(2026, 9, 13, 14, tzinfo=ZoneInfo("Asia/Tokyo"))


def _frame(schema: pl.Schema, rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema)


def _conditions_inputs(station_id="A", observed_at=OBSERVED_AT):
    weather_row = {name: None for name in weather.DAILY_WEATHER_SCHEMA.names()}
    weather_row.update(
        {
            "station_id": station_id,
            "date": OBSERVATION_DATE,
            "observed_at": observed_at,
            "precipitation_mm": 1.5,
            "precipitation_quality": 8,
        }
    )
    daily_weather = _frame(weather.DAILY_WEATHER_SCHEMA, [weather_row])
    active_stations = _frame(
        stations.ACTIVE_STATIONS_SCHEMA,
        [
            {
                "station_id": station_id,
                "station_name": "Alpha",
                "latitude": 35.0,
                "longitude": 139.0,
            }
        ],
    )
    pollen_cleaned = _frame(
        cleaned.POLLEN_SCHEMA,
        [
            {
                "station_id": station_id,
                "date": OBSERVATION_DATE,
                "grass_info": None,
                "tree_info": {
                    "value": 4,
                    "plants": [{"code": "ALDER", "value": 2}],
                },
                "weed_info": {"value": 1, "plants": []},
            }
        ],
    )
    return daily_weather, active_stations, pollen_cleaned


def test_daily_weather_conditions_joins_one_row_per_station():
    result = processed._merge_daily_weather_conditions(*_conditions_inputs())

    assert result.schema == processed.DAILY_WEATHER_CONDITIONS_SCHEMA
    assert result.height == 1
    assert result.select(
        "station_id", "station_name", "latitude", "longitude", "precipitation_mm"
    ).row(0) == ("A", "Alpha", 35.0, 139.0, 1.5)
    assert result["tree_info"].to_list() == [
        {"value": 4, "plants": [{"code": "ALDER", "value": 2}]}
    ]


@pytest.mark.parametrize("input_name", ["active_stations", "pollen_cleaned"])
def test_daily_weather_conditions_rejects_mismatched_stations(input_name):
    daily_weather, active_stations, pollen_cleaned = _conditions_inputs()
    if input_name == "active_stations":
        active_stations = active_stations.with_columns(pl.lit("B").alias("station_id"))
    else:
        pollen_cleaned = pollen_cleaned.with_columns(pl.lit("B").alias("station_id"))

    with pytest.raises(ValueError, match="station IDs or dates do not match"):
        processed._merge_daily_weather_conditions(
            daily_weather, active_stations, pollen_cleaned
        )


def test_daily_weather_conditions_rejects_different_dates():
    daily_weather, active_stations, pollen_cleaned = _conditions_inputs()
    pollen_cleaned = pollen_cleaned.with_columns(
        pl.lit(date(2026, 9, 14)).alias("date")
    )

    with pytest.raises(ValueError, match="station IDs or dates do not match"):
        processed._merge_daily_weather_conditions(
            daily_weather, active_stations, pollen_cleaned
        )


def test_daily_weather_conditions_does_not_upload_mismatched_data(s3_client):
    daily_weather, active_stations, pollen_cleaned = _conditions_inputs()
    pollen_cleaned = pollen_cleaned.with_columns(pl.lit("B").alias("station_id"))

    with pytest.raises(ValueError, match="station IDs or dates do not match"):
        processed.daily_weather_conditions(
            daily_weather, active_stations, pollen_cleaned
        )

    assert "Contents" not in s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)


def test_daily_weather_conditions_stores_verified_parquet(s3_client):
    result = processed.daily_weather_conditions(*_conditions_inputs())

    stored = s3_client.get_object(
        Bucket=PROCESSED_BUCKET,
        Key="20260913/daily_weather_conditions.parquet",
    )
    payload = stored["Body"].read()
    frame = pl.read_parquet(BytesIO(payload))
    assert stored["ContentType"] == processed.PARQUET_CONTENT_TYPE
    assert frame.schema == processed.DAILY_WEATHER_CONDITIONS_SCHEMA
    assert frame["station_id"].to_list() == ["A"]
    assert result.metadata == {
        "destination_bucket": PROCESSED_BUCKET,
        "destination_object_key": "20260913/daily_weather_conditions.parquet",
        "destination_uri": "s3://processed/20260913/daily_weather_conditions.parquet",
        "file_size_bytes": len(payload),
        "sha256": sha256(payload).hexdigest(),
        "observation_date": "2026-09-13",
        "observed_at": "2026-09-13T14:00:00+09:00",
        "row_count": 1,
        "column_count": len(processed.DAILY_WEATHER_CONDITIONS_SCHEMA),
    }


def test_daily_weather_conditions_rejects_empty_weather():
    with pytest.raises(ValueError, match="cannot be empty"):
        processed._merge_daily_weather_conditions(
            _frame(weather.DAILY_WEATHER_SCHEMA, []),
            _frame(stations.ACTIVE_STATIONS_SCHEMA, []),
            _frame(cleaned.POLLEN_SCHEMA, []),
        )


def test_daily_weather_conditions_replaces_same_day_snapshot(s3_client):
    processed.daily_weather_conditions(*_conditions_inputs())
    latest = datetime(2026, 9, 13, 15, tzinfo=ZoneInfo("Asia/Tokyo"))
    processed.daily_weather_conditions(*_conditions_inputs(observed_at=latest))

    stored = s3_client.get_object(
        Bucket=PROCESSED_BUCKET,
        Key="20260913/daily_weather_conditions.parquet",
    )
    frame = pl.read_parquet(BytesIO(stored["Body"].read()))
    assert frame["observed_at"].to_list() == [latest]
