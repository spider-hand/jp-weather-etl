"""Build the WMO station dimension from the JMA station master."""

from pathlib import Path
from typing import Final

import polars as pl
from dagster import AssetExecutionContext, asset

STATION_MASTER_PATH: Final = Path(__file__).parents[3] / "data/station_master.csv"
STATION_MASTER_COLUMNS: Final = {
    "観測所番号": "station_id",
    "観測所名": "station_name",
    "緯度(度)": "latitude_degrees",
    "緯度(分)": "latitude_minutes",
    "経度(度)": "longitude_degrees",
    "経度(分)": "longitude_minutes",
}
WMO_STATIONS_SCHEMA: Final = pl.Schema(
    {
        "station_id": pl.String,
        "wmo_station_id": pl.String,
        "station_name": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }
)


def _read_station_master(path: Path) -> pl.DataFrame:
    # JMA distributes this file in CP932, Microsoft's superset of Shift_JIS.
    frame = pl.read_csv(path, encoding="cp932", infer_schema=False)
    missing_columns = set(STATION_MASTER_COLUMNS) - set(frame.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required station master columns: {sorted(missing_columns)!r}"
        )
    return frame.select(
        *(
            pl.col(source).str.strip_chars().replace("", None).alias(target)
            for source, target in STATION_MASTER_COLUMNS.items()
        )
    )


def _select_wmo_stations(
    daily_weather: pl.DataFrame, station_master: pl.DataFrame
) -> tuple[pl.DataFrame, list[str]]:
    wmo_ids = (
        daily_weather.filter(pl.col("wmo_station_id").is_not_null())
        .select("station_id", "wmo_station_id")
        .unique()
    )
    missing_ids = (
        wmo_ids.select("station_id")
        .join(
            station_master.select("station_id").unique(),
            on="station_id",
            how="anti",
        )["station_id"]
        .to_list()
    )
    if missing_ids:
        raise ValueError(
            f"Missing station master rows for station IDs: "
            f"{sorted(missing_ids, key=str)!r}"
        )

    wmo_master = station_master.with_row_index("source_row").join(
        wmo_ids, on="station_id", how="inner"
    )
    duplicate_ids = (
        wmo_master.group_by("station_id")
        .len()
        .filter(pl.col("len") > 1)["station_id"]
        .sort()
        .to_list()
    )
    wmo_master = wmo_master.sort("source_row").unique(
        subset="station_id", keep="first", maintain_order=True
    )

    result = wmo_master.select(
        "station_id",
        "wmo_station_id",
        "station_name",
        (
            pl.col("latitude_degrees").cast(pl.Float64, strict=False)
            + pl.col("latitude_minutes").cast(pl.Float64, strict=False) / 60
        ).alias("latitude"),
        (
            pl.col("longitude_degrees").cast(pl.Float64, strict=False)
            + pl.col("longitude_minutes").cast(pl.Float64, strict=False) / 60
        ).alias("longitude"),
    )
    invalid_ids = result.filter(
        pl.any_horizontal(
            *(pl.col(column).is_null() for column in WMO_STATIONS_SCHEMA.names())
        )
    )["station_id"].to_list()
    if invalid_ids:
        raise ValueError(
            f"Invalid required station master fields for station IDs: "
            f"{sorted(invalid_ids, key=str)!r}"
        )

    return (
        result.cast(WMO_STATIONS_SCHEMA, strict=True).sort("station_id"),
        duplicate_ids,
    )


@asset(group_name="stations")
def wmo_stations(
    context: AssetExecutionContext, daily_weather: pl.DataFrame
) -> pl.DataFrame:
    result, duplicate_ids = _select_wmo_stations(
        daily_weather,
        _read_station_master(STATION_MASTER_PATH),
    )
    if duplicate_ids:
        context.log.warning(
            "Duplicate station master rows found; using the first row for station "
            f"IDs: {duplicate_ids!r}"
        )
    context.add_output_metadata(
        {
            "row_count": result.height,
            "column_count": result.width,
            "duplicate_station_count": len(duplicate_ids),
        }
    )
    return result
