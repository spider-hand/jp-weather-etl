from datetime import date, datetime
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest

from queries.analytics import connect

OBSERVATION_DATE = date(2026, 9, 17)
OBSERVED_AT = datetime(2026, 9, 17, 12, tzinfo=ZoneInfo("Asia/Tokyo"))


def _write_weather_parquet(tmp_path, **overrides):
    weather = {
        "station_id": ["A", "B", "C", "D"],
        "date": [OBSERVATION_DATE] * 4,
        "observed_at": [OBSERVED_AT] * 4,
        "latitude": [35.0] * 4,
        "longitude": [139.0] * 4,
        "max_temperature_c": [30.0] * 4,
        "min_temperature_c": [20.0] * 4,
        "precipitation_mm": [0.0] * 4,
        "max_gust_speed_ms": [5.0] * 4,
    }
    weather.update(overrides)

    parquet_path = tmp_path / "weather.parquet"
    pl.DataFrame(weather).write_parquet(parquet_path)
    return parquet_path


def _station_ids(parquet_path, query):
    with connect(str(parquet_path)) as connection:
        return [row[0] for row in connection.execute(query).fetchall()]


def test_all_weather_conditions_returns_every_row_for_requested_date(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        date=[
            OBSERVATION_DATE,
            date(2026, 9, 18),
            date(2026, 9, 19),
            date(2026, 9, 20),
        ],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM all_weather_conditions(DATE '2026-09-17')",
    )

    assert result == ["A"]


def test_hottest_returns_hottest_station(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        max_temperature_c=[30.0, 35.0, 32.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM hottest(DATE '2026-09-17')",
    )

    assert result == ["B"]


def test_hottest_returns_all_ties(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        max_temperature_c=[30.0, 35.0, 35.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM hottest(DATE '2026-09-17')",
    )

    assert result == ["B", "C"]


def test_coldest_returns_coldest_station(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        min_temperature_c=[20.0, 15.0, 18.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM coldest(DATE '2026-09-17')",
    )

    assert result == ["B"]


def test_coldest_returns_all_ties(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        min_temperature_c=[20.0, 15.0, 15.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM coldest(DATE '2026-09-17')",
    )

    assert result == ["B", "C"]


def test_highest_precipitation_returns_station_with_highest_precipitation(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        precipitation_mm=[0.0, 10.0, 5.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM highest_precipitation(DATE '2026-09-17')",
    )

    assert result == ["B"]


def test_highest_precipitation_returns_all_ties(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        precipitation_mm=[0.0, 10.0, 10.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM highest_precipitation(DATE '2026-09-17')",
    )

    assert result == ["B", "C"]


def test_strongest_gust_returns_station_with_strongest_gust(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        max_gust_speed_ms=[5.0, 12.0, 8.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM strongest_gust(DATE '2026-09-17')",
    )

    assert result == ["B"]


def test_strongest_gust_returns_all_ties(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        max_gust_speed_ms=[5.0, 12.0, 12.0, None],
    )

    result = _station_ids(
        parquet_path,
        "SELECT station_id FROM strongest_gust(DATE '2026-09-17')",
    )

    assert result == ["B", "C"]


def test_nearest_stations_returns_nearest_station(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        latitude=[35.0, 35.1, 35.2, None],
    )

    with connect(str(parquet_path)) as connection:
        result = connection.execute("""
            SELECT station_id, distance_km
            FROM nearest_stations(DATE '2026-09-17', 35.0, 139.0)
            """).fetchall()

    assert len(result) == 1
    assert result[0][0] == "A"
    assert result[0][1] == pytest.approx(0.0)


def test_daily_summary_returns_daily_aggregates(tmp_path):
    parquet_path = _write_weather_parquet(
        tmp_path,
        max_temperature_c=[30.0, 35.0, 32.0, None],
        min_temperature_c=[20.0, 15.0, 18.0, None],
        precipitation_mm=[0.0, 10.0, 5.0, None],
        max_gust_speed_ms=[5.0, 12.0, 8.0, None],
    )

    with connect(str(parquet_path)) as connection:
        result = connection.execute("SELECT * FROM daily_summary(DATE '2026-09-17')")
        columns = [column[0] for column in result.description]
        summary = result.fetchone()

    assert columns == [
        "date",
        "observed_at",
        "highest_max_temperature_c",
        "lowest_min_temperature_c",
        "average_max_temperature_c",
        "average_min_temperature_c",
        "maximum_precipitation_mm",
        "strongest_gust_speed_ms",
    ]
    assert summary[0] == OBSERVATION_DATE
    assert summary[1] == OBSERVED_AT
    assert summary[2] == 35.0
    assert summary[3] == 15.0
    assert summary[4] == pytest.approx(32.33, rel=0.01)
    assert summary[5] == pytest.approx(17.67, rel=0.01)
    assert summary[6] == 10.0
    assert summary[7] == 12.0


@pytest.mark.parametrize(
    ("latitude", "longitude", "message"),
    [
        (90.1, 139.0, "latitude must be between -90 and 90"),
        (-90.1, 139.0, "latitude must be between -90 and 90"),
        (35.0, 180.1, "longitude must be between -180 and 180"),
        (35.0, -180.1, "longitude must be between -180 and 180"),
    ],
)
def test_nearest_stations_rejects_invalid_coordinates(
    tmp_path, latitude, longitude, message
):
    parquet_path = _write_weather_parquet(tmp_path)

    with (
        connect(str(parquet_path)) as connection,
        pytest.raises(duckdb.InvalidInputException, match=message),
    ):
        connection.execute(
            "SELECT * FROM nearest_stations(DATE '2026-09-17', ?, ?)",
            [latitude, longitude],
        ).fetchall()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM hottest(DATE '2026-09-18')",
        "SELECT * FROM coldest(DATE '2026-09-18')",
        "SELECT * FROM highest_precipitation(DATE '2026-09-18')",
        "SELECT * FROM strongest_gust(DATE '2026-09-18')",
        "SELECT * FROM nearest_stations(DATE '2026-09-18', 35.0, 139.0)",
        "SELECT * FROM all_weather_conditions(DATE '2026-09-18')",
    ],
)
def test_query_returns_no_rows_for_missing_date(tmp_path, query):
    parquet_path = _write_weather_parquet(tmp_path)

    with connect(str(parquet_path)) as connection:
        result = connection.execute(query).fetchall()

    assert result == []
