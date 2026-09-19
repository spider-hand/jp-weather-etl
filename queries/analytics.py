"""Run SQL against the processed weather Parquet files with DuckDB."""

import argparse
import unicodedata
from importlib.resources import files

import duckdb

from storage import PROCESSED_BUCKET, StorageSettings

WEATHER_SOURCE = f"s3://{PROCESSED_BUCKET}/*/daily_weather_conditions.parquet"


def _display_width(value: str) -> int:
    return sum(
        0
        if unicodedata.combining(character)
        else 2
        if unicodedata.east_asian_width(character) in {"F", "W"}
        else 1
        for character in value
    )


def _pad(value: str, width: int) -> str:
    return value + " " * (width - _display_width(value))


def _format_vertical(columns: list[str], rows: list[tuple[object, ...]]) -> str:
    if not rows:
        return ""

    values = [
        ["NULL" if value is None else str(value) for value in row] for row in rows
    ]
    field_width = max(_display_width(column) for column in columns)
    value_width = max(_display_width(value) for row in values for value in row)
    top = f"┌{'─' * (field_width + 2)}┬{'─' * (value_width + 2)}┐"
    divider = f"├{'─' * (field_width + 2)}┼{'─' * (value_width + 2)}┤"
    bottom = f"└{'─' * (field_width + 2)}┴{'─' * (value_width + 2)}┘"
    lines = [top]

    for index, row in enumerate(values):
        if index:
            lines.append(divider)
        lines.extend(
            f"│ {_pad(column, field_width)} │ {_pad(value, value_width)} │"
            for column, value in zip(columns, row, strict=True)
        )

    lines.append(bottom)
    return "\n".join(lines)


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _configure_s3(connection: duckdb.DuckDBPyConnection) -> None:
    settings = StorageSettings.from_env()

    connection.execute(
        f"""
        CREATE OR REPLACE TEMPORARY SECRET weather_storage (
            TYPE s3,
            KEY_ID {_quote(settings.access_key_id)},
            SECRET {_quote(settings.secret_access_key)},
            REGION {_quote(settings.region)},
            ENDPOINT {_quote(settings.endpoint)},
            URL_STYLE 'path',
            USE_SSL {str(settings.use_ssl).lower()},
            SCOPE 's3://{PROCESSED_BUCKET}'
        )
        """
    )


def connect(weather_source: str = WEATHER_SOURCE) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    try:
        connection.install_extension("httpfs")
        connection.load_extension("httpfs")
        connection.install_extension("spatial")
        connection.load_extension("spatial")
        if weather_source.startswith("s3://"):
            _configure_s3(connection)

        connection.execute("SET VARIABLE weather_source = ?", [weather_source])
        sql_root = files("queries")
        connection.execute(
            sql_root.joinpath("weather_conditions.sql").read_text(encoding="utf-8")
        )
        for query in sorted(sql_root.iterdir(), key=lambda item: item.name):
            if query.name.endswith(".sql") and query.name != "weather_conditions.sql":
                connection.execute(query.read_text(encoding="utf-8"))
    except Exception:
        connection.close()
        raise
    return connection


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("sql")
    args = parser.parse_args()

    with connect() as connection:
        relation = connection.sql(args.sql)
        output = _format_vertical(relation.columns, relation.fetchmany(1000))
        if output:
            print(output)


if __name__ == "__main__":
    main()
