from datetime import date

import polars as pl
import pytest

from pipeline.defs.assets import cleaned, weather

OBSERVATION_DATE = date(2026, 9, 13)


def _frame(schema: pl.Schema, rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema)


def test_daily_weather_full_joins_and_sorts_all_cleaned_keys():
    result = weather._merge_daily_weather(
        _frame(
            cleaned.PRECIPITATION_SCHEMA,
            [
                {
                    "station_id": "B",
                    "date": OBSERVATION_DATE,
                    "precipitation_mm": 1.5,
                    "precipitation_quality": 8,
                }
            ],
        ),
        _frame(
            cleaned.MAX_TEMPERATURE_SCHEMA,
            [
                {
                    "station_id": "A",
                    "date": OBSERVATION_DATE,
                    "max_temperature_c": 24.0,
                    "max_temperature_quality": 8,
                }
            ],
        ),
        _frame(cleaned.MIN_TEMPERATURE_SCHEMA, []),
        _frame(cleaned.MAX_WIND_SCHEMA, []),
        _frame(cleaned.MAX_GUST_SCHEMA, []),
    )

    assert result.schema == weather.DAILY_WEATHER_SCHEMA
    assert result["station_id"].to_list() == ["A", "B"]
    assert result["max_temperature_c"].to_list() == [24.0, None]
    assert result["precipitation_mm"].to_list() == [None, 1.5]


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
