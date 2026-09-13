from datetime import date, datetime
from io import StringIO

from dagster import DagsterEventType, DefaultScheduleStatus, Definitions, materialize
import polars as pl
import pytest

from pipeline.defs import jobs, schedules
from pipeline.defs.assets import cleaned, raw
from pipeline.storage import RAW_BUCKET

COMMON_HEADERS = ["観測所番号", "都道府県", "地点", "国際地点番号"]
COMMON_VALUES = ["11001", "北海道", "宗谷岬", ""]


class FrozenDateTime:
    @classmethod
    def now(cls, timezone):
        assert timezone is raw.JST
        return datetime(2026, 9, 13, 23, 30, tzinfo=timezone)


def _frame(headers, values):
    csv = ",".join([*COMMON_HEADERS, *headers]) + "\n" + ",".join(
        [*COMMON_VALUES, *values]
    )
    return pl.read_csv(StringIO(csv), infer_schema=False)


@pytest.mark.parametrize(
    ("transform", "headers", "values", "expected_schema"),
    [
        (
            cleaned._clean_precipitation,
            ["7日の値(mm)", "7日の値の品質情報"],
            ["", "5"],
            cleaned.PRECIPITATION_SCHEMA,
        ),
        (
            cleaned._clean_max_temperature,
            [
                "13日の最高気温(℃)",
                "13日の最高気温の品質情報",
                "13日の最高気温起時（時）",
                "13日の最高気温起時（分）",
                "13日の最高気温起時の品質情報",
            ],
            ["26.3", "4", "11", "39", "4"],
            cleaned.MAX_TEMPERATURE_SCHEMA,
        ),
        (
            cleaned._clean_min_temperature,
            [
                "13日の最低気温(℃)",
                "13日の最低気温の品質情報",
                "13日の最低気温起時（時）",
                "13日の最低気温起時（分）",
                "13日の最低気温起時の品質情報",
            ],
            ["12.1", "8", "03", "05", "8"],
            cleaned.MIN_TEMPERATURE_SCHEMA,
        ),
        (
            cleaned._clean_max_wind,
            [
                "13日の最大値(m/s)",
                "13日の最大値の品質情報",
                "13日の最大値観測時の風向",
                "13日の最大値観測時の風向の品質情報",
                "13日の最大値起時（時）",
                "13日の最大値起時（分）",
                "13日の最大値起時の品質情報",
            ],
            ["8.1", "4", "南西", "4", "12", "47", "4"],
            cleaned.MAX_WIND_SCHEMA,
        ),
        (
            cleaned._clean_max_gust,
            [
                "13日の最大値(m/s)",
                "13日の最大値の品質情報",
                "13日の最大値観測時の風向",
                "13日の最大値観測時の風向の品質情報",
                "13日の最大値起時（時）",
                "13日の最大値起時（分）",
                "13日の最大値起時の品質情報",
            ],
            ["16.2", "8", "西北西", "8", "14", "20", "8"],
            cleaned.MAX_GUST_SCHEMA,
        ),
    ],
)
def test_cleaned_schemas(transform, headers, values, expected_schema):
    result = transform(_frame(headers, values), date(2026, 9, 13))

    assert result.schema == expected_schema
    assert result["wmo_station_id"].to_list() == [None]
    assert result["date"].to_list() == [date(2026, 9, 13)]


def test_missing_measurement_is_null_but_zero_is_preserved():
    frame = pl.concat(
        [
            _frame(["13日の値(mm)", "13日の値の品質情報"], ["", "5"]),
            _frame(["13日の値(mm)", "13日の値の品質情報"], ["0.0", "5"]),
        ]
    )

    result = cleaned._clean_precipitation(frame, date(2026, 9, 13))

    assert result["precipitation_mm"].to_list() == [None, 0.0]
    assert result.height == 2


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        pytest.param(["13日の値(mm)"], "13日の値(mm)", id="one-match"),
        pytest.param(["別の列"], None, id="no-match"),
        pytest.param(
            ["13日の値(mm)", "14日の値(mm)"],
            None,
            id="multiple-matches",
        ),
    ],
)
def test_find_column(columns, expected):
    pattern = r"\d{1,2}日の値\(mm\)"

    if expected:
        assert cleaned.find_column(columns, pattern) == expected
        return

    with pytest.raises(ValueError) as error:
        cleaned.find_column(columns, pattern)
    assert f"Available columns: {columns!r}" in str(error.value)


def _payload(headers, values):
    csv = ",".join([*COMMON_HEADERS, *headers]) + "\r\n" + ",".join(
        [*COMMON_VALUES, *values]
    )
    return (csv + "\r\n").encode("shift_jis")


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


def test_weather_job_runs_raw_then_cleaned_without_writing_cleaned_objects(
    monkeypatch, s3_client
):
    payloads = _raw_payloads()
    urls = {url: filename for url, filename in raw.RAW_FILES.values()}
    monkeypatch.setattr(
        raw,
        "_download_bytes",
        lambda url: (payloads[urls[url]], "text/csv"),
    )
    monkeypatch.setattr(raw, "datetime", FrozenDateTime)
    defs = Definitions(
        assets=[*raw.RAW_ASSETS, *cleaned.CLEANED_ASSETS],
        jobs=[jobs.weather_etl_job],
        schedules=[schedules.weather_etl_schedule],
    )

    Definitions.validate_loadable(defs)
    result = defs.resolve_job_def("weather_etl_job").execute_in_process()

    assert result.success
    assert result.output_for_node("precipitation_cleaned")[
        "precipitation_mm"
    ].to_list() == [0.0]
    objects = s3_client.list_objects_v2(Bucket=RAW_BUCKET).get("Contents", [])
    assert {item["Key"] for item in objects} == {
        f"20260913/{filename}" for filename in payloads
    }
    events = result.get_asset_materialization_events()
    positions = {
        event.asset_key.to_user_string(): index for index, event in enumerate(events)
    }
    for raw_name in raw.RAW_FILES:
        cleaned_name = raw_name.removesuffix("_raw") + "_cleaned"
        assert positions[raw_name] < positions[cleaned_name]

    precipitation_event = next(
        event
        for event in events
        if event.asset_key.to_user_string() == "precipitation_cleaned"
    )
    metadata = precipitation_event.event_specific_data.materialization.metadata
    assert metadata["source_object_key"].value == "20260913/precipitation.csv"
    assert metadata["observation_date"].value == "2026-09-13"
    assert metadata["row_count"].value == 1
    assert metadata["column_count"].value == 7
    assert metadata["null_count"].value == 0

    schedule = schedules.weather_etl_schedule
    assert schedule.cron_schedule == "30 23 * * *"
    assert schedule.execution_timezone == "Asia/Tokyo"
    assert schedule.job_name == "weather_etl_job"
    assert schedule.default_status is DefaultScheduleStatus.RUNNING


def test_cleaned_asset_fails_without_raw_materialization_in_same_run():
    result = materialize([cleaned.precipitation_cleaned], raise_on_error=False)

    assert not result.success
    failure = next(
        event
        for event in result.all_events
        if event.event_type is DagsterEventType.STEP_FAILURE
    )
    assert "Expected exactly one precipitation_raw materialization" in str(
        failure.event_specific_data.error
    )
