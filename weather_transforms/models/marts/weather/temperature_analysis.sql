{{
    config(
        materialized='table',
        schema='public'
    )
}}

WITH weather_data AS (
    SELECT
        id,
        city,
        temperature as temp_celsius,
        measurement_time,
        TO_CHAR(measurement_time, 'YYYY-MM-DD') as measurement_date,
        TO_CHAR(measurement_time, 'HH24') as measurement_hour
    FROM {{ ref('stg_weather_raw') }}
)

SELECT
    city,
    measurement_date,
    measurement_hour,
    temp_celsius,
    {{ round_temp('temp_celsius') }} as temp_celsius_rounded,
    {{ round_temp(celsius_to_fahrenheit('temp_celsius')) }} as temp_fahrenheit,
    
    -- Calculate statistics by city and date
    AVG(temp_celsius) OVER (PARTITION BY city, measurement_date) as daily_avg_temp_celsius,
    {{ round_temp('AVG(temp_celsius) OVER (PARTITION BY city, measurement_date)', 1) }} as daily_avg_temp_celsius_rounded,
    {{ round_temp(celsius_to_fahrenheit('AVG(temp_celsius) OVER (PARTITION BY city, measurement_date)'), 1) }} as daily_avg_temp_fahrenheit,
    
    -- Calculate statistics by city and hour
    AVG(temp_celsius) OVER (PARTITION BY city, measurement_hour) as hourly_avg_temp_celsius,
    {{ round_temp('AVG(temp_celsius) OVER (PARTITION BY city, measurement_hour)', 1) }} as hourly_avg_temp_celsius_rounded,
    {{ round_temp(celsius_to_fahrenheit('AVG(temp_celsius) OVER (PARTITION BY city, measurement_hour)'), 1) }} as hourly_avg_temp_fahrenheit,
    
    -- Add temperature categorizations
    CASE
        WHEN temp_celsius < 0 THEN 'Freezing'
        WHEN temp_celsius < 10 THEN 'Cold'
        WHEN temp_celsius < 20 THEN 'Cool'
        WHEN temp_celsius < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    
    -- Add reference to original data
    id as source_record_id,
    measurement_time
FROM weather_data
ORDER BY city, measurement_time 