from datetime import date

import polars as pl
import pytest

from pipeline.defs.assets import stations

MASTER_HEADERS = list(stations.STATION_MASTER_COLUMNS)


def _daily_weather(station_ids):
    return pl.DataFrame(
        {
            "station_id": station_ids,
            "date": [date(2026, 9, day) for day in range(1, len(station_ids) + 1)],
        },
        schema={"station_id": pl.String, "date": pl.Date},
    )


def _station_master(tmp_path, rows):
    path = tmp_path / "station_master.csv"
    csv = "\r\n".join(
        [",".join(MASTER_HEADERS), *(",".join(row) for row in rows)]
    )
    path.write_bytes((csv + "\r\n").encode("cp932"))
    return stations._read_station_master(path)


def test_active_stations(tmp_path):
    master = _station_master(
        tmp_path,
        [
            ["B", "地点B", "35", "30", "139", "45"],
            ["A", "宗谷岬", "45", "31.2", "141", "56.1"],
            ["C", "地点C", "40", "0", "140", "0"],
            ["C", "地点C別地点", "40", "1", "140", "1"],
        ],
    )

    result, duplicate_ids = stations._select_active_stations(
        _daily_weather(["B", "A", "A"]), master
    )

    assert duplicate_ids == []
    assert result.schema == stations.ACTIVE_STATIONS_SCHEMA
    assert result["station_id"].to_list() == ["A", "B"]
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
def test_active_stations_rejects_invalid_active_master_rows(
    tmp_path, daily_ids, master_rows, message
):
    master = _station_master(tmp_path, master_rows)

    with pytest.raises(ValueError, match=message):
        stations._select_active_stations(_daily_weather(daily_ids), master)


def test_active_stations_uses_first_duplicate_station(tmp_path):
    master = _station_master(
        tmp_path,
        [
            ["A", "最初の地点", "35", "30", "139", "45"],
            ["A", "二番目の地点", "36", "0", "140", "0"],
        ],
    )

    result, duplicate_ids = stations._select_active_stations(
        _daily_weather(["A"]), master
    )

    assert duplicate_ids == ["A"]
    assert result["station_name"].to_list() == ["最初の地点"]
    assert result["latitude"].to_list() == [35.5]
    assert result["longitude"].to_list() == [139.75]
