CREATE OR REPLACE MACRO daily_summary(p_date DATE) AS TABLE
SELECT
    min(weather_conditions.date) AS date,
    max(observed_at) AS observed_at,
    arg_max(station_name, max_temperature_c ORDER BY station_id)
        AS highest_max_temperature_station_name,
    max(max_temperature_c) AS highest_max_temperature_c,
    arg_min(station_name, min_temperature_c ORDER BY station_id)
        AS lowest_min_temperature_station_name,
    min(min_temperature_c) AS lowest_min_temperature_c,
    arg_max(station_name, precipitation_mm ORDER BY station_id)
        AS maximum_precipitation_station_name,
    max(precipitation_mm) AS maximum_precipitation_mm,
    arg_max(station_name, max_gust_speed_ms ORDER BY station_id)
        AS strongest_gust_station_name,
    max(max_gust_speed_ms) AS strongest_gust_speed_ms
FROM weather_conditions
WHERE date = p_date
HAVING count(*) > 0;
