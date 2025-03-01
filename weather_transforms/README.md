# Weather Data Transformations

This dbt project transforms the raw weather data collected from the OpenWeatherMap API into structured datasets suitable for analysis and visualization.

## Models

### Staging Models
- `stg_weather_raw`: Transforms raw JSON weather data into a structured format

### Mart Models
- `daily_weather_summary`: Aggregates weather data by day
- `hourly_weather_summary`: Aggregates weather data by hour
- `temperature_analysis`: Provides detailed temperature analysis with conversions between units and categorizations

## How to Run

### Prerequisites
- PostgreSQL database with raw weather data
- dbt Core installed with PostgreSQL adapter
- Proper database credentials in profiles.yml

### Running the models
```bash
# Navigate to the project directory
cd weather_transforms

# Run all models
dbt run

# Run specific models
dbt run --select daily_weather_summary
dbt run --select hourly_weather_summary
dbt run --select temperature_analysis

# Generate documentation
dbt docs generate
dbt docs serve
```

## Project Structure
```
weather_transforms/
├── models/
│   ├── marts/
│   │   └── weather/
│   │       ├── daily_weather_summary.sql
│   │       ├── hourly_weather_summary.sql
│   │       └── temperature_analysis.sql
│   ├── staging/
│   │   └── stg_weather_raw.sql
│   ├── schema.yml
│   └── sources.yml
├── macros/
│   └── temperature_conversion.sql
├── seeds/
├── snapshots/
├── tests/
│   ├── test_daily_summary_metrics.sql
│   └── test_hourly_summary_metrics.sql
├── dbt_project.yml
├── profiles.yml
└── README.md
```

## Data Flow
1. Raw weather data is collected from OpenWeatherMap API and stored in PostgreSQL
2. `stg_weather_raw` extracts structured data from the raw JSON
3. `daily_weather_summary` and `hourly_weather_summary` aggregate the data for analysis
4. `temperature_analysis` provides detailed temperature metrics and conversions

Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
