from dagster import AssetSelection, define_asset_job

WEATHER_ASSET_KEYS = (
    "precipitation_raw",
    "max_temperature_raw",
    "min_temperature_raw",
    "max_wind_raw",
    "max_gust_raw",
    "precipitation_cleaned",
    "max_temperature_cleaned",
    "min_temperature_cleaned",
    "max_wind_cleaned",
    "max_gust_cleaned",
    "daily_weather",
)

weather_etl_job = define_asset_job(
    "weather_etl_job",
    selection=AssetSelection.assets(*WEATHER_ASSET_KEYS),
)
