from datetime import date

import polars as pl
import pytest

from jp_weather_etl.pipeline.defs.assets import stations

MASTER_HEADERS = list(stations.STATION_MASTER_COLUMNS)


def _daily_weather(station_ids, wmo_station_ids=None):
    if wmo_station_ids is None:
        wmo_station_ids = [f"WMO-{value}" for value in station_ids]
    return pl.DataFrame(
        {
            "station_id": station_ids,
            "wmo_station_id": wmo_station_ids,
            "date": [date(2026, 9, day) for day in range(1, len(station_ids) + 1)],
        },
        schema={
            "station_id": pl.String,
            "wmo_station_id": pl.String,
            "date": pl.Date,
        },
    )


def _station_master(tmp_path, rows):
    path = tmp_path / "station_master.csv"
    csv = "\r\n".join([",".join(MASTER_HEADERS), *(",".join(row) for row in rows)])
    path.write_bytes((csv + "\r\n").encode("cp932"))
    return stations._read_station_master(path)


def test_packaged_station_master_is_readable():
    expected_columns = list(stations.STATION_MASTER_COLUMNS.values())

    station_master = stations._read_packaged_station_master()

    assert station_master.height > 0
    assert station_master.columns == expected_columns


def test_wmo_stations(tmp_path):
    master = _station_master(
        tmp_path,
        [
            ["B", "地点B", "35", "30", "139", "45"],
            ["A", "宗谷岬", "45", "31.2", "141", "56.1"],
            ["C", "地点C", "40", "0", "140", "0"],
            ["C", "地点C別地点", "40", "1", "140", "1"],
        ],
    )

    result, duplicate_ids = stations._select_wmo_stations(
        _daily_weather(["B", "A", "A"]), master
    )

    assert duplicate_ids == []
    assert result.schema == stations.WMO_STATIONS_SCHEMA
    assert result["station_id"].to_list() == ["A", "B"]
    assert result["wmo_station_id"].to_list() == ["WMO-A", "WMO-B"]
    assert result["station_name"].to_list() == ["宗谷岬", "地点B"]
    assert result["latitude"].to_list() == pytest.approx([45.52, 35.5])
    assert result["longitude"].to_list() == pytest.approx([141.935, 139.75])


@pytest.mark.parametrize(
    ("daily_ids", "master_rows", "message"),
    [
        pytest.param(
            ["missing"],
            [["A", "地点A", "35", "30", "139", "45"]],
            "Missing station master rows.*missing",
            id="missing-station",
        ),
        pytest.param(
            ["A"],
            [["A", "", "35", "30", "139", "45"]],
            "Invalid required station master fields.*A",
            id="missing-station-name",
        ),
        pytest.param(
            ["A"],
            [["A", "地点A", "invalid", "30", "139", "45"]],
            "Invalid required station master fields.*A",
            id="invalid-coordinate",
        ),
    ],
)
def test_wmo_stations_rejects_invalid_master_rows(
    tmp_path, daily_ids, master_rows, message
):
    master = _station_master(tmp_path, master_rows)

    with pytest.raises(ValueError, match=message):
        stations._select_wmo_stations(_daily_weather(daily_ids), master)


def test_wmo_stations_uses_first_duplicate_station(tmp_path):
    master = _station_master(
        tmp_path,
        [
            ["A", "最初の地点", "35", "30", "139", "45"],
            ["A", "二番目の地点", "36", "0", "140", "0"],
        ],
    )

    result, duplicate_ids = stations._select_wmo_stations(_daily_weather(["A"]), master)

    assert duplicate_ids == ["A"]
    assert result["station_name"].to_list() == ["最初の地点"]
    assert result["latitude"].to_list() == [35.5]
    assert result["longitude"].to_list() == [139.75]


def test_wmo_stations_excludes_null_wmo_station_ids(tmp_path):
    master = _station_master(
        tmp_path,
        [
            ["A", "地点A", "35", "30", "139", "45"],
            ["B", "地点B", "36", "0", "140", "0"],
        ],
    )

    result, _ = stations._select_wmo_stations(
        _daily_weather(["A", "B"], ["WMO-A", None]), master
    )

    assert result["station_id"].to_list() == ["A"]
