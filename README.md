# JP Weather ETL

A Dagster-based ETL pipeline for weather data in Japan

Inspired by live weather reports, this pipeline runs on demand instead of on a fixed schedule. Each run creates a snapshot of the observations available so far that day.

## Setup

Set up environment variables:

```sh
cp .env.example .env
```

Start storage and Dagster:

```sh
make dev
```

The Dagster UI is available at http://localhost:3000, the S3 API at
http://localhost:9000, and the RustFS console at http://localhost:9001.

## Architecture

```mermaid
flowchart TD
    jma_weather[/"JMA weather observations"/]
    station_master[/"JMA station master CSV"/]
    pollen_api[/"Google Pollen API"/]

    subgraph dagster["Dagster assets"]
        cleaned_weather(["Cleaned weather"])
        daily_weather(["Daily weather"])
        wmo_stations(["WMO stations"])
        cleaned_pollen(["Cleaned pollen"])
        daily_conditions(["Daily weather conditions"])
    end

    subgraph rustfs["RustFS (S3-compatible storage)"]
        raw_weather[("raw/YYYYMMDD/*.csv")]
        raw_pollen[("raw/YYYYMMDD/pollen.json")]
        processed[("processed/YYYYMMDD/<br/>daily_weather_conditions.parquet")]
    end

    jma_weather -->|ingest| raw_weather
    raw_weather -->|clean| cleaned_weather
    cleaned_weather -->|merge| daily_weather
    daily_weather -->|select matching stations| wmo_stations
    station_master -->|enrich| wmo_stations
    wmo_stations -->|request by coordinates| pollen_api
    pollen_api -->|ingest| raw_pollen
    raw_pollen -->|clean| cleaned_pollen
    daily_weather -->|join| daily_conditions
    wmo_stations -->|join| daily_conditions
    cleaned_pollen -->|join| daily_conditions
    daily_conditions -->|write Parquet| processed
```

## Data Sources

Weather observation data and station master data are provided by the [Japan Meteorological Agency (JMA)](https://www.data.jma.go.jp/stats/data/mdrr/docs/csv_dl_readme.html).

Pollen data is provided by the [Google Maps Platform Pollen API](https://developers.google.com/maps/documentation/pollen).

## License

[MIT](./LICENSE)

`data/station_master.csv` is subject to the JMA's applicable terms of use, as it contains data provided by the JMA.

Copyright (c) 2026-present, Akinori Hoshina
