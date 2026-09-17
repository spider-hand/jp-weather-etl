CREATE OR REPLACE MACRO daily_summary(p_date DATE) AS TABLE
SELECT
    min(weather_conditions.date) AS date,
    max(observed_at) AS observed_at,
    max(max_temperature_c) AS highest_max_temperature_c,
    min(min_temperature_c) AS lowest_min_temperature_c,
    avg(max_temperature_c) AS average_max_temperature_c,
    avg(min_temperature_c) AS average_min_temperature_c,
    max(precipitation_mm) AS maximum_precipitation_mm,
    max(max_gust_speed_ms) AS strongest_gust_speed_ms
FROM weather_conditions
WHERE date = p_date
HAVING count(*) > 0;
