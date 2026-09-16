CREATE OR REPLACE VIEW weather_conditions AS
SELECT *
FROM read_parquet(getvariable('weather_source'), union_by_name = true);
