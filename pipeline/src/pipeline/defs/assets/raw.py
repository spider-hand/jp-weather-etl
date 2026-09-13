"""Copy JMA daily observation CSV files to raw storage without modifying them."""

from datetime import datetime
from hashlib import sha256
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from dagster import MaterializeResult, asset

from pipeline.storage import RAW_BUCKET, create_s3_client

JST = ZoneInfo("Asia/Tokyo")
DOWNLOAD_TIMEOUT_SECONDS = 30

RAW_FILES = {
    "precipitation_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/pre_rct/alltable/predaily00_rct.csv",
        "precipitation.csv",
    ),
    "max_temperature_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/tem_rct/alltable/mxtemsadext00_rct.csv",
        "max_temperature.csv",
    ),
    "min_temperature_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/tem_rct/alltable/mntemsadext00_rct.csv",
        "min_temperature.csv",
    ),
    "max_wind_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/wind_rct/alltable/mxwsp00_rct.csv",
        "max_wind.csv",
    ),
    "max_gust_raw": (
        "https://www.data.jma.go.jp/stats/data/mdrr/wind_rct/alltable/gust00_rct.csv",
        "max_gust.csv",
    ),
}


def _download_bytes(source_url: str) -> tuple[bytes, str]:
    with urlopen(source_url, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
        return response.read(), response.headers.get("Content-Type", "")


def _ingest_raw_file(
    source_url: str,
    filename: str,
) -> MaterializeResult:
    payload, content_type = _download_bytes(source_url)
    timestamp = datetime.now(JST)
    object_key = f"{timestamp:%Y%m%d}/{filename}"
    client = create_s3_client()

    client.put_object(
        Bucket=RAW_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType=content_type,
    )

    stored_body = client.get_object(Bucket=RAW_BUCKET, Key=object_key)["Body"]
    try:
        stored = stored_body.read()
    finally:
        stored_body.close()

    if stored != payload:
        raise RuntimeError(f"Uploaded bytes do not match download for s3://{RAW_BUCKET}/{object_key}")

    return MaterializeResult(
        metadata={
            "source_url": source_url,
            "destination_bucket": RAW_BUCKET,
            "destination_object_key": object_key,
            "destination_uri": f"s3://{RAW_BUCKET}/{object_key}",
            "downloaded_at_jst": timestamp.isoformat(),
            "file_size_bytes": len(payload),
            "sha256": sha256(payload).hexdigest(),
            "response_content_type": content_type,
        }
    )


@asset(group_name="raw")
def precipitation_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["precipitation_raw"])


@asset(group_name="raw")
def max_temperature_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_temperature_raw"])


@asset(group_name="raw")
def min_temperature_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["min_temperature_raw"])


@asset(group_name="raw")
def max_wind_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_wind_raw"])


@asset(group_name="raw")
def max_gust_raw() -> MaterializeResult:
    return _ingest_raw_file(*RAW_FILES["max_gust_raw"])


RAW_ASSETS = [
    precipitation_raw,
    max_temperature_raw,
    min_temperature_raw,
    max_wind_raw,
    max_gust_raw,
]
