from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from jp_weather_etl.pipeline.defs.assets import cleaned, weather

OBSERVATION_DATE = date(2026, 9, 13)
FOURTEEN_OCLOCK = datetime(2026, 9, 13, 14, tzinfo=ZoneInfo("Asia/Tokyo"))


def _frame(schema: pl.Schema, rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema)


def _row(schema: pl.Schema, station_id: str, **values):
    row = {name: None for name in schema.names()}
    row.update(
        {
            "station_id": station_id,
            "wmo_station_id": f"WMO-{station_id}",
            "date": OBSERVATION_DATE,
            "observed_at": FOURTEEN_OCLOCK,
            **values,
        }
    )
    return row


def _weather_frames(**observed_at_overrides):
    sources = (
        ("precipitation", cleaned.PRECIPITATION_SCHEMA),
        ("max_temperature", cleaned.MAX_TEMPERATURE_SCHEMA),
        ("min_temperature", cleaned.MIN_TEMPERATURE_SCHEMA),
        ("max_wind", cleaned.MAX_WIND_SCHEMA),
        ("max_gust", cleaned.MAX_GUST_SCHEMA),
    )
    return tuple(
        _frame(
            schema,
            [
                _row(
                    schema,
                    "A",
                    observed_at=observed_at_overrides.get(name, FOURTEEN_OCLOCK),
                )
            ],
        )
        for name, schema in sources
    )


def test_daily_weather_full_joins_and_sorts_all_cleaned_keys():
    result = weather._merge_daily_weather(
        _frame(
            cleaned.PRECIPITATION_SCHEMA,
            [
                _row(
                    cleaned.PRECIPITATION_SCHEMA,
                    "B",
                    precipitation_mm=1.5,
                    precipitation_quality=8,
                )
            ],
        ),
        _frame(
            cleaned.MAX_TEMPERATURE_SCHEMA,
            [
                _row(
                    cleaned.MAX_TEMPERATURE_SCHEMA,
                    "A",
                    max_temperature_c=24.0,
                    max_temperature_quality=8,
                )
            ],
        ),
        _frame(
            cleaned.MIN_TEMPERATURE_SCHEMA,
            [_row(cleaned.MIN_TEMPERATURE_SCHEMA, "A")],
        ),
        _frame(cleaned.MAX_WIND_SCHEMA, [_row(cleaned.MAX_WIND_SCHEMA, "A")]),
        _frame(cleaned.MAX_GUST_SCHEMA, [_row(cleaned.MAX_GUST_SCHEMA, "A")]),
    )

    assert result.schema == weather.DAILY_WEATHER_SCHEMA
    assert result["station_id"].to_list() == ["A", "B"]
    assert result["wmo_station_id"].to_list() == ["WMO-A", "WMO-B"]
    assert result["max_temperature_c"].to_list() == [24.0, None]
    assert result["precipitation_mm"].to_list() == [None, 1.5]
    assert result["observed_at"].to_list() == [FOURTEEN_OCLOCK, FOURTEEN_OCLOCK]


def test_daily_weather_filters_rows_without_wmo_station_id():
    frames = list(_weather_frames())
    frames[0] = pl.concat(
        [
            frames[0],
            _frame(
                cleaned.PRECIPITATION_SCHEMA,
                [
                    _row(
                        cleaned.PRECIPITATION_SCHEMA,
                        "B",
                        wmo_station_id=None,
                        precipitation_mm=2.0,
                    )
                ],
            ),
        ]
    )

    result = weather._merge_daily_weather(*frames)

    assert result["station_id"].to_list() == ["A"]


def test_daily_weather_rejects_conflicting_wmo_station_ids():
    frames = list(_weather_frames())
    frames[1] = frames[1].with_columns(pl.lit("WMO-OTHER").alias("wmo_station_id"))

    with pytest.raises(ValueError, match="Conflicting WMO station IDs.*A"):
        weather._merge_daily_weather(*frames)


def test_daily_weather_uses_oldest_source_hour():
    fifteen_oclock = datetime(2026, 9, 13, 15, tzinfo=ZoneInfo("Asia/Tokyo"))
    result = weather._merge_daily_weather(
        *_weather_frames(
            precipitation=fifteen_oclock,
            max_temperature=fifteen_oclock,
            min_temperature=FOURTEEN_OCLOCK,
            max_wind=fifteen_oclock,
            max_gust=fifteen_oclock,
        )
    )

    assert result["observed_at"].to_list() == [FOURTEEN_OCLOCK]


def test_daily_weather_rejects_cross_date_sources():
    frames = list(_weather_frames())
    frames[0] = frames[0].with_columns(pl.lit(date(2026, 9, 14)).alias("date"))

    with pytest.raises(ValueError, match="one shared observation date"):
        weather._merge_daily_weather(*frames)


def test_daily_weather_rejects_empty_source():
    result = cleaned._valid_daily_weather_key(_frame(cleaned.PRECIPITATION_SCHEMA, []))

    assert not result.passed
    assert result.metadata["date_count"].value == 0
    assert result.metadata["observed_at_count"].value == 0


def test_cleaned_check_rejects_multiple_observed_at_values():
    later = datetime(2026, 9, 13, 15, tzinfo=ZoneInfo("Asia/Tokyo"))
    frame = _frame(
        cleaned.PRECIPITATION_SCHEMA,
        [
            _row(cleaned.PRECIPITATION_SCHEMA, "A"),
            _row(cleaned.PRECIPITATION_SCHEMA, "B", observed_at=later),
        ],
    )

    result = cleaned._valid_daily_weather_key(frame)

    assert not result.passed
    assert result.metadata["observed_at_count"].value == 2


@pytest.mark.parametrize(
    ("station_ids", "dates", "passed", "null_count", "duplicate_count"),
    [
        pytest.param(
            ["A", "B"],
            [OBSERVATION_DATE, OBSERVATION_DATE],
            True,
            0,
            0,
            id="valid-keys",
        ),
        pytest.param(
            [None, "B"],
            [OBSERVATION_DATE, None],
            False,
            2,
            0,
            id="null-keys",
        ),
        pytest.param(
            ["A", "A", "B"],
            [OBSERVATION_DATE, OBSERVATION_DATE, OBSERVATION_DATE],
            False,
            0,
            1,
            id="duplicate-key",
        ),
    ],
)
def test_valid_daily_weather_key(
    station_ids, dates, passed, null_count, duplicate_count
):
    frame = pl.DataFrame(
        {"station_id": station_ids, "date": dates},
        schema={"station_id": pl.String, "date": pl.Date},
    )

    result = cleaned._valid_daily_weather_key(frame)

    assert result.passed is passed
    assert result.metadata["null_key_row_count"].value == null_count
    assert result.metadata["duplicate_key_count"].value == duplicate_count
