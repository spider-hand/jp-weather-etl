CREATE OR REPLACE MACRO strongest_gust(p_date DATE) AS TABLE
WITH daily AS (
    SELECT *
    FROM weather_conditions
    WHERE date = p_date AND max_gust_speed_ms IS NOT NULL
),
extreme AS (
    SELECT max(max_gust_speed_ms) AS value
    FROM daily
)
SELECT daily.*
FROM daily, extreme
WHERE daily.max_gust_speed_ms = extreme.value
ORDER BY daily.station_id;
