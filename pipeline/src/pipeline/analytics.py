"""Run SQL against the processed weather Parquet files with DuckDB."""

import argparse
import os
from importlib.resources import files
from urllib.parse import urlsplit

import duckdb

WEATHER_SOURCE = "s3://processed/*/daily_weather_conditions.parquet"
S3_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_ENDPOINT_URL",
)


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _configure_s3(connection: duckdb.DuckDBPyConnection) -> None:
    missing = [name for name in S3_ENV if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))

    endpoint = urlsplit(os.environ["S3_ENDPOINT_URL"])
    if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
        raise ValueError("S3_ENDPOINT_URL must be an http:// or https:// URL")

    connection.execute(
        f"""
        CREATE OR REPLACE TEMPORARY SECRET weather_storage (
            TYPE s3,
            KEY_ID {_quote(os.environ["AWS_ACCESS_KEY_ID"])},
            SECRET {_quote(os.environ["AWS_SECRET_ACCESS_KEY"])},
            REGION {_quote(os.environ["AWS_DEFAULT_REGION"])},
            ENDPOINT {_quote(endpoint.netloc)},
            URL_STYLE 'path',
            USE_SSL {str(endpoint.scheme == "https").lower()},
            SCOPE 's3://processed'
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
        sql_root = files("pipeline.sql")
        connection.execute(
            sql_root.joinpath("weather_conditions.sql").read_text(encoding="utf-8")
        )
        for query in sorted(
            files("pipeline.sql.queries").iterdir(), key=lambda item: item.name
        ):
            if query.name.endswith(".sql"):
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
        connection.sql(args.sql).show(max_rows=1000)


if __name__ == "__main__":
    main()
