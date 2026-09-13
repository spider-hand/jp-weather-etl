# JP Weather ETL

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