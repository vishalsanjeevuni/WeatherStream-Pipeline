{{
    config(
        materialized='view',
        schema='public'
    )
}}

-- Extract structured data from raw JSON
SELECT
    id,
    data->>'name' AS city,
    (data->'main'->>'temp')::NUMERIC AS temperature,
    (data->'main'->>'humidity')::NUMERIC AS humidity,
    (data->'main'->>'pressure')::NUMERIC AS pressure,
    (data->'wind'->>'speed')::NUMERIC AS wind_speed,
    (data->'wind'->>'deg')::NUMERIC AS wind_direction,
    data->'weather'->0->>'main' AS weather_condition,
    data->'weather'->0->>'description' AS weather_description,
    TO_TIMESTAMP((data->>'dt')::NUMERIC) AS measurement_time,
    data->>'ingestion_timestamp' AS ingestion_timestamp,
    (data->'coord'->>'lon')::NUMERIC AS longitude,
    (data->'coord'->>'lat')::NUMERIC AS latitude,
    (data->'sys'->>'country') AS country,
    timestamp AS record_timestamp
FROM {{ source('weather_data', 'weather_raw') }} 