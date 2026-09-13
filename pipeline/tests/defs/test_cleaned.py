from datetime import date
from io import StringIO

import polars as pl
import pytest
from dagster import DagsterEventType, materialize

from pipeline.defs.assets import cleaned

COMMON_HEADERS = ["観測所番号", "都道府県", "地点", "国際地点番号"]
COMMON_VALUES = ["11001", "北海道", "宗谷岬", ""]


def _frame(headers, values):
    csv = (
        ",".join([*COMMON_HEADERS, *headers])
        + "\n"
        + ",".join([*COMMON_VALUES, *values])
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
