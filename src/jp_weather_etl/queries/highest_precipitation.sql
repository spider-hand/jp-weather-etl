CREATE OR REPLACE MACRO highest_precipitation(p_date DATE) AS TABLE
WITH daily AS (
    SELECT *
    FROM weather_conditions
    WHERE date = p_date AND precipitation_mm IS NOT NULL
),
extreme AS (
    SELECT max(precipitation_mm) AS value
    FROM daily
)
SELECT daily.*
FROM daily, extreme
WHERE daily.precipitation_mm = extreme.value
ORDER BY daily.station_id;
