-- Test to ensure that temperature metrics make sense
-- Min temp should be less than or equal to average temp which should be less than or equal to max temp
-- This test will return records that violate this rule

WITH test_data AS (
    SELECT
        city,
        day,
        avg_temperature,
        min_temperature,
        max_temperature
    FROM {{ ref('daily_weather_summary') }}
)

SELECT
    city,
    day,
    avg_temperature,
    min_temperature,
    max_temperature
FROM test_data
WHERE 
    -- If any of these conditions are true, the test fails
    min_temperature > avg_temperature
    OR avg_temperature > max_temperature
    OR min_temperature > max_temperature 