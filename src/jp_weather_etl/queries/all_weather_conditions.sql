CREATE OR REPLACE MACRO all_weather_conditions(p_date DATE) AS TABLE
SELECT *
FROM weather_conditions
WHERE date = p_date
ORDER BY station_id;
