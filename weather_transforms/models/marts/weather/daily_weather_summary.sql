{{
    config(
        materialized='table',
        schema='public'
    )
}}

-- This model creates a daily summary of weather metrics
WITH daily_metrics AS (
    SELECT 
        city,
        DATE_TRUNC('day', measurement_time) AS day,
        AVG(temperature) AS avg_temperature,
        MIN(temperature) AS min_temperature,
        MAX(temperature) AS max_temperature,
        AVG(humidity) AS avg_humidity,
        AVG(pressure) AS avg_pressure,
        AVG(wind_speed) AS avg_wind_speed,
        COUNT(*) AS measurement_count,
        MODE() WITHIN GROUP (ORDER BY weather_condition) AS most_common_condition
    FROM 
        weather_metrics
    GROUP BY 
        city, DATE_TRUNC('day', measurement_time)
)

SELECT
    day,
    city,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    avg_pressure,
    avg_wind_speed,
    most_common_condition,
    measurement_count,
    NOW() AS generated_at
FROM
    daily_metrics
ORDER BY
    day DESC, city 