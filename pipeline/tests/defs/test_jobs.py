from datetime import datetime

import pytest
from dagster import DefaultScheduleStatus, Definitions

from pipeline.defs import jobs, schedules
from pipeline.defs.assets import cleaned, daily, raw, stations
from pipeline.storage import RAW_BUCKET

COMMON_HEADERS = ["観測所番号", "都道府県", "地点", "国際地点番号"]
COMMON_VALUES = ["11001", "北海道", "宗谷岬", ""]


class FrozenDateTime:
    @classmethod
    def now(cls, timezone):
        assert timezone is raw.JST
        return datetime(2026, 9, 13, 23, 30, tzinfo=timezone)


def _payload(headers, values, common_rows=None):
    common_rows = common_rows or [COMMON_VALUES]
    lines = [",".join([*COMMON_HEADERS, *headers])]
    lines.extend(",".join([*common, *values]) for common in common_rows)
    return ("\r\n".join(lines) + "\r\n").encode("shift_jis")


def _raw_payloads():
    wind_headers = [
        "13日の最大値(m/s)",
        "13日の最大値の品質情報",
        "13日の最大値観測時の風向",
        "13日の最大値観測時の風向の品質情報",
        "13日の最大値起時（時）",
        "13日の最大値起時（分）",
        "13日の最大値起時の品質情報",
    ]
    return {
        "precipitation.csv": _payload(
            ["13日の値(mm)", "13日の値の品質情報"], ["0.0", "5"]
        ),
        "max_temperature.csv": _payload(
            [
                "13日の最高気温(℃)",
                "13日の最高気温の品質情報",
                "13日の最高気温起時（時）",
                "13日の最高気温起時（分）",
                "13日の最高気温起時の品質情報",
            ],
            ["26.3", "4", "11", "39", "4"],
        ),
        "min_temperature.csv": _payload(
            [
                "13日の最低気温(℃)",
                "13日の最低気温の品質情報",
                "13日の最低気温起時（時）",
                "13日の最低気温起時（分）",
                "13日の最低気温起時の品質情報",
            ],
            ["12.1", "8", "03", "05", "8"],
        ),
        "max_wind.csv": _payload(
            wind_headers, ["8.1", "4", "南西", "4", "12", "47", "4"]
        ),
        "max_gust.csv": _payload(
            wind_headers, ["16.2", "8", "西北西", "8", "14", "20", "8"]
        ),
    }


def _definitions():
    return Definitions(
        assets=[
            *raw.RAW_ASSETS,
            *cleaned.CLEANED_ASSETS,
            daily.daily_weather,
            stations.active_stations,
        ],
        asset_checks=cleaned.CLEANED_KEY_CHECKS,
        jobs=[jobs.weather_etl_job],
        schedules=[schedules.weather_etl_schedule],
    )


def _mock_inputs(monkeypatch, tmp_path, payloads):
    urls = {url: filename for url, filename in raw.RAW_FILES.values()}
    monkeypatch.setattr(
        raw,
        "_download_bytes",
        lambda url: (payloads[urls[url]], "text/csv"),
    )
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "secret")
    monkeypatch.setattr(
        raw,
        "_download_pollen_response",
        lambda *_args: {
            "regionCode": "JP",
            "dailyInfo": [{"date": {"year": 2026, "month": 9, "day": 13}}],
        },
    )
    station_master_path = tmp_path / "station_master.csv"
    station_master_path.write_bytes(
        (
            "観測所番号,観測所名,緯度(度),緯度(分),経度(度),経度(分)\r\n"
            "11001,宗谷岬,45,31.2,141,56.1\r\n"
        ).encode("cp932")
    )
    monkeypatch.setattr(stations, "STATION_MASTER_PATH", station_master_path)


def test_weather_etl_job(monkeypatch, s3_client, tmp_path):
    payloads = _raw_payloads()
    _mock_inputs(monkeypatch, tmp_path, payloads)
    defs = _definitions()

    Definitions.validate_loadable(defs)
    result = defs.resolve_job_def("weather_etl_job").execute_in_process()

    assert result.success
    assert result.output_for_node("precipitation_cleaned")[
        "precipitation_mm"
    ].to_list() == [0.0]
    daily_result = result.output_for_node("daily_weather")
    assert daily_result.schema == daily.DAILY_WEATHER_SCHEMA
    assert daily_result["precipitation_mm"].to_list() == [0.0]
    active_result = result.output_for_node("active_stations")
    assert active_result.schema == stations.ACTIVE_STATIONS_SCHEMA
    assert active_result["station_name"].to_list() == ["宗谷岬"]

    objects = s3_client.list_objects_v2(Bucket=RAW_BUCKET).get("Contents", [])
    assert {item["Key"] for item in objects} == {
        f"20260913/{filename}" for filename in payloads
    } | {"20260913/pollen.json"}
    events = result.get_asset_materialization_events()
    positions = {
        event.asset_key.to_user_string(): index for index, event in enumerate(events)
    }
    assert set(positions) == set(jobs.WEATHER_ASSET_KEYS)
    for raw_name in raw.RAW_FILES:
        cleaned_name = raw_name.removesuffix("_raw") + "_cleaned"
        assert positions[raw_name] < positions[cleaned_name]
        assert positions[cleaned_name] < positions["daily_weather"]
    assert positions["daily_weather"] < positions["active_stations"]
    assert positions["active_stations"] < positions["pollen_raw"]

    evaluations = result.get_asset_check_evaluations()
    assert {evaluation.asset_key.to_user_string() for evaluation in evaluations} == {
        asset.key.to_user_string() for asset in cleaned.CLEANED_ASSETS
    }
    assert {evaluation.check_name for evaluation in evaluations} == {
        "valid_daily_weather_key"
    }
    assert all(evaluation.passed for evaluation in evaluations)

    precipitation_event = next(
        event
        for event in events
        if event.asset_key.to_user_string() == "precipitation_cleaned"
    )
    metadata = precipitation_event.event_specific_data.materialization.metadata
    assert metadata["source_object_key"].value == "20260913/precipitation.csv"
    assert metadata["observation_date"].value == "2026-09-13"
    assert metadata["row_count"].value == 1
    assert metadata["column_count"].value == 4
    assert metadata["null_count"].value == 0

    daily_event = next(
        event
        for event in events
        if event.asset_key.to_user_string() == "daily_weather"
    )
    daily_metadata = daily_event.event_specific_data.materialization.metadata
    assert daily_metadata["row_count"].value == 1
    assert daily_metadata["column_count"].value == 16

    active_event = next(
        event
        for event in events
        if event.asset_key.to_user_string() == "active_stations"
    )
    active_metadata = active_event.event_specific_data.materialization.metadata
    assert active_metadata["row_count"].value == 1
    assert active_metadata["column_count"].value == 4
    assert active_metadata["duplicate_station_count"].value == 0

    schedule = schedules.weather_etl_schedule
    assert schedule.cron_schedule == "30 23 * * *"
    assert schedule.execution_timezone == "Asia/Tokyo"
    assert schedule.job_name == "weather_etl_job"
    assert schedule.default_status is DefaultScheduleStatus.RUNNING


@pytest.mark.parametrize(
    ("common_rows", "null_key_row_count", "duplicate_key_count"),
    [
        pytest.param(
            [["", "北海道", "宗谷岬", ""]],
            1,
            0,
            id="null-station-id",
        ),
        pytest.param(
            [COMMON_VALUES, COMMON_VALUES],
            0,
            1,
            id="duplicate-key",
        ),
    ],
)
def test_invalid_cleaned_key_blocks_daily_weather(
    monkeypatch,
    s3_client,
    tmp_path,
    common_rows,
    null_key_row_count,
    duplicate_key_count,
):
    payloads = _raw_payloads()
    payloads["precipitation.csv"] = _payload(
        ["13日の値(mm)", "13日の値の品質情報"],
        ["0.0", "5"],
        common_rows,
    )
    _mock_inputs(monkeypatch, tmp_path, payloads)

    result = _definitions().resolve_job_def("weather_etl_job").execute_in_process(
        raise_on_error=False
    )

    evaluation = next(
        evaluation
        for evaluation in result.get_asset_check_evaluations()
        if evaluation.asset_key.to_user_string() == "precipitation_cleaned"
    )
    assert not evaluation.passed
    assert (
        evaluation.metadata["null_key_row_count"].value == null_key_row_count
    )
    assert evaluation.metadata["duplicate_key_count"].value == duplicate_key_count
    materialized_assets = {
        event.asset_key.to_user_string()
        for event in result.get_asset_materialization_events()
    }
    assert "precipitation_cleaned" in materialized_assets
    assert "daily_weather" not in materialized_assets
