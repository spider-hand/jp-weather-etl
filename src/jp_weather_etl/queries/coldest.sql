CREATE OR REPLACE MACRO coldest(p_date DATE) AS TABLE
WITH daily AS (
    SELECT *
    FROM weather_conditions
    WHERE date = p_date AND min_temperature_c IS NOT NULL
),
extreme AS (
    SELECT min(min_temperature_c) AS value
    FROM daily
)
SELECT daily.*
FROM daily, extreme
WHERE daily.min_temperature_c = extreme.value
ORDER BY daily.station_id;
