# Weather Data Transformation Project

This dbt project transforms raw weather data into analytical datasets for reporting and visualization.

## Data Flow

```
Raw Data → Staging → Marts
```

### Source Data

The source data comes from OpenWeatherMap API and is stored in two tables:

1. `weather_raw` - Contains the raw JSON responses from the API
2. `weather_metrics` - Contains parsed metrics from the raw data

### Staging Layer

The staging layer transforms the raw data into a structured format:

- `stg_weather_raw` - Extracts and formats data from the raw JSON

### Mart Layer

The mart layer aggregates data for analysis:

- `daily_weather_summary` - Aggregates data by day
- `hourly_weather_summary` - Aggregates data by hour

## Data Model

### Source Tables

| Table | Description |
|-------|-------------|
| weather_raw | Raw JSON responses from the OpenWeatherMap API |
| weather_metrics | Parsed weather metrics in a structured format |

### Staging Tables

| Table | Description |
|-------|-------------|
| stg_weather_raw | Structured data extracted from raw JSON |

### Mart Tables

| Table | Description |
|-------|-------------|
| daily_weather_summary | Daily aggregates of weather metrics |
| hourly_weather_summary | Hourly aggregates of weather metrics |

## Key Metrics

| Metric | Description |
|--------|-------------|
| avg_temperature | Average temperature |
| min_temperature | Minimum temperature |
| max_temperature | Maximum temperature |
| avg_humidity | Average humidity |
| avg_pressure | Average atmospheric pressure |
| avg_wind_speed | Average wind speed |
| most_common_condition | Most common weather condition |
| measurement_count | Number of measurements |

## Data Quality Tests

1. Temperature consistency tests - Ensures min ≤ avg ≤ max temperature
2. Measurement count tests - Ensures we have adequate data coverage 