CREATE OR REPLACE MACRO nearest_stations(
    p_date DATE,
    p_latitude DOUBLE,
    p_longitude DOUBLE
) AS TABLE
WITH arguments AS (
    SELECT
        CASE
            WHEN p_latitude BETWEEN -90 AND 90 THEN p_latitude::DOUBLE
            ELSE error('latitude must be between -90 and 90')
        END AS latitude,
        CASE
            WHEN p_longitude BETWEEN -180 AND 180 THEN p_longitude::DOUBLE
            ELSE error('longitude must be between -180 and 180')
        END AS longitude
),
ranked AS (
    SELECT
        weather_conditions.*,
        ST_Distance_Sphere(
            ST_Point(arguments.latitude, arguments.longitude),
            ST_Point(weather_conditions.latitude, weather_conditions.longitude)
        ) / 1000.0 AS distance_km
    FROM weather_conditions, arguments
    WHERE date = p_date
      AND weather_conditions.latitude IS NOT NULL
      AND weather_conditions.longitude IS NOT NULL
)
SELECT *
FROM ranked
ORDER BY distance_km, station_id
LIMIT 1;
