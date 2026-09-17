CREATE OR REPLACE MACRO hottest(p_date DATE) AS TABLE
WITH daily AS (
    SELECT *
    FROM weather_conditions
    WHERE date = p_date AND max_temperature_c IS NOT NULL
),
extreme AS (
    SELECT max(max_temperature_c) AS value
    FROM daily
)
SELECT daily.*
FROM daily, extreme
WHERE daily.max_temperature_c = extreme.value
ORDER BY daily.station_id;
