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


def _raw_pollen_payload(*, pollen_types, plants):
    return {
        "results": [
            {
                "station_id": "11001",
                "response": {
                    "dailyInfo": [
                        {
                            "pollenTypeInfo": pollen_types,
                            "plantInfo": plants,
                        }
                    ]
                },
            }
        ]
    }


def _cleaned_pollen_row(
    *, pollen_code, pollen_value, plant_code=None, plant_value=None
):
    return {
        "station_id": "11001",
        "date": date(2026, 9, 14),
        "pollen_code": pollen_code,
        "pollen_value": pollen_value,
        "plant_code": plant_code,
        "plant_value": plant_value,
    }


def test_clean_pollen_requires_results_list():
    with pytest.raises(TypeError, match="results list"):
        cleaned._clean_pollen({}, date(2026, 9, 14))


@pytest.mark.parametrize(
    "daily_info",
    [
        pytest.param(None, id="missing"),
        pytest.param([], id="empty"),
        pytest.param([{}, {}], id="multiple"),
    ],
)
def test_clean_pollen_requires_exactly_one_daily_record(daily_info):
    payload = _raw_pollen_payload(pollen_types=[], plants=[])
    payload["results"][0]["response"]["dailyInfo"] = daily_info

    with pytest.raises(ValueError, match="exactly one pollen forecast day"):
        cleaned._clean_pollen(payload, date(2026, 9, 14))


@pytest.mark.parametrize(
    ("pollen_types", "plants", "expected"),
    [
        pytest.param(
            [{"code": "TREE", "indexInfo": {"value": 4}}],
            [
                {
                    "code": "ALDER",
                    "indexInfo": {"value": 2},
                    "plantDescription": {"type": "TREE"},
                },
                {
                    "code": "CYPRESS_PINE",
                    "indexInfo": {"value": 4},
                    "plantDescription": {"type": "TREE"},
                },
            ],
            [
                _cleaned_pollen_row(
                    pollen_code="TREE",
                    pollen_value=4,
                    plant_code="ALDER",
                    plant_value=2,
                ),
                _cleaned_pollen_row(
                    pollen_code="TREE",
                    pollen_value=4,
                    plant_code="CYPRESS_PINE",
                    plant_value=4,
                ),
            ],
            id="plant-values",
        ),
        pytest.param(
            [{"code": "TREE"}],
            [{"code": "ALDER", "plantDescription": {"type": "TREE"}}],
            [
                _cleaned_pollen_row(
                    pollen_code="TREE",
                    pollen_value=None,
                    plant_code="ALDER",
                )
            ],
            id="missing-index-info",
        ),
        pytest.param(
            [{"code": "WEED", "indexInfo": {"value": 2}}],
            [],
            [_cleaned_pollen_row(pollen_code="WEED", pollen_value=2)],
            id="pollen-without-plants",
        ),
        pytest.param(
            [
                {
                    "code": "POLLEN_TYPE_UNSPECIFIED",
                    "indexInfo": {"value": 5},
                },
                {"code": "NEW_TYPE", "indexInfo": {"value": 5}},
            ],
            [
                {
                    "code": "PLANT_UNSPECIFIED",
                    "indexInfo": {"value": 3},
                    "plantDescription": {"type": "TREE"},
                },
                {
                    "code": "NEW_PLANT",
                    "indexInfo": {"value": 3},
                    "plantDescription": {"type": "TREE"},
                },
            ],
            [],
            id="unknown-codes",
        ),
        pytest.param(
            [{"code": "TREE", "indexInfo": {"value": 4}}],
            [{"code": "BIRCH", "indexInfo": {"value": 3}}],
            [_cleaned_pollen_row(pollen_code="TREE", pollen_value=4)],
            id="uncategorized-plant",
        ),
        pytest.param([], [], [], id="empty-info"),
    ],
)
def test_clean_pollen_response_patterns(pollen_types, plants, expected):
    result = cleaned._clean_pollen(
        _raw_pollen_payload(pollen_types=pollen_types, plants=plants),
        date(2026, 9, 14),
    )

    assert result.schema == cleaned.POLLEN_SCHEMA
    assert result.to_dicts() == expected


@pytest.mark.parametrize(
    (
        "pollen_value",
        "plant_value",
        "expected_passed",
        "invalid_pollen_count",
        "invalid_plant_count",
    ),
    [
        pytest.param(None, 2, True, 0, 0, id="pollen-null"),
        pytest.param(0, 2, True, 0, 0, id="pollen-min"),
        pytest.param(5, 2, True, 0, 0, id="pollen-max"),
        pytest.param(-1, 2, False, 1, 0, id="pollen-below-min"),
        pytest.param(6, 2, False, 1, 0, id="pollen-above-max"),
        pytest.param(2, None, True, 0, 0, id="plant-null"),
        pytest.param(2, 0, True, 0, 0, id="plant-min"),
        pytest.param(2, 5, True, 0, 0, id="plant-max"),
        pytest.param(2, -1, False, 0, 1, id="plant-below-min"),
        pytest.param(2, 6, False, 0, 1, id="plant-above-max"),
    ],
)
def test_pollen_value_check(
    pollen_value,
    plant_value,
    expected_passed,
    invalid_pollen_count,
    invalid_plant_count,
):
    frame = pl.DataFrame(
        {
            "station_id": ["11001"],
            "date": [date(2026, 9, 14)],
            "pollen_code": ["TREE"],
            "pollen_value": [pollen_value],
            "plant_code": ["ALDER"],
            "plant_value": [plant_value],
        },
        schema=cleaned.POLLEN_SCHEMA,
    )

    result = cleaned._valid_pollen_values(frame)

    assert result.passed is expected_passed
    assert result.metadata["invalid_pollen_value_count"].value == invalid_pollen_count
    assert result.metadata["invalid_plant_value_count"].value == invalid_plant_count


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
