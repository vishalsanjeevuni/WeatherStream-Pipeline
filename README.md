# WeatherStream Pipeline

A comprehensive data pipeline that collects real-time weather data from OpenWeatherMap API, processes it through Confluent Kafka, stores it in PostgreSQL, and transforms it with dbt for analysis.

## Architecture

```
                 ┌─────────────┐
                 │OpenWeatherMap│
                 │     API      │
                 └──────┬──────┘
                        │
                        ▼
┌───────────────────────────────────┐
│           Producer                │
│ (src/producer/weather_producer.py)│
└───────────────────┬───────────────┘
                    │
                    ▼
          ┌───────────────────┐
          │   Confluent       │
          │   Kafka           │
          └─────────┬─────────┘
                    │
                    ▼
┌───────────────────────────────────┐
│           Consumer                │
│ (src/consumer/weather_consumer.py)│
└───────────────────┬───────────────┘
                    │
                    ▼
          ┌───────────────────┐
          │   PostgreSQL      │
          │   Database        │
          └─────────┬─────────┘
                    │
                    ▼
        ┌─────────────────────────┐
        │    DBT Transformations  │
        │    (weather_transforms) │
        └─────────────────────────┘
```

## Project Status

### Implementation Status: 100% Complete

All phases of the implementation plan have been successfully completed:

✅ **Core Pipeline**
- Confluent Cloud setup with Kafka topic
- Python producer script publishing real-time weather data
- Custom Python consumer storing data in PostgreSQL
- Data flow verified from producer to Kafka to PostgreSQL

✅ **Cloud and Transformation**
- PostgreSQL deployed on AWS RDS
- DBT project with transformations
- Full documentation in README

✅ **Stretch Goals**
- Docker containerization for the producer
- AWS Fargate deployment configuration
- End-to-end testing framework

### Latest Test Results

All components have been verified to be working correctly:

```
======= TEST SUMMARY =======
API Connection: ✅ PASS
Kafka Producer: ✅ PASS
Kafka Consumer: ✅ PASS
Database Connection: ✅ PASS
DBT Transformations: ✅ PASS

🎉 All tests passed! The pipeline is working correctly.
```

## Prerequisites

- Python 3.8+
- Confluent Kafka account
- OpenWeatherMap API key
- PostgreSQL database
- dbt Core with PostgreSQL adapter

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/weatherstream-pipeline.git
cd weatherstream-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables

Create a `config/.env` file with the following variables:

```
# Kafka Configuration
BOOTSTRAP_SERVERS=<your-kafka-bootstrap-servers>
KAFKA_API_KEY=<your-kafka-api-key>
KAFKA_API_SECRET=<your-kafka-api-secret>
KAFKA_TOPIC=<your-kafka-topic>

# OpenWeatherMap Configuration
OPENWEATHERMAP_API_KEY=<your-openweathermap-api-key>

# PostgreSQL Configuration
POSTGRES_HOST=<your-postgres-host>
POSTGRES_PORT=<your-postgres-port>
POSTGRES_USER=<your-postgres-username>
POSTGRES_PASSWORD=<your-postgres-password>
POSTGRES_DB=<your-postgres-database>
```

### 5. Set up the database

Run the database setup script:

```bash
python src/setup/db_setup.py
```

## Running the Pipeline

### Option 1: All-in-one Script (Recommended)

You can start the entire pipeline with a single command:

```bash
./run_pipeline.sh
```

This script will:
1. Start the Weather Producer to fetch data from OpenWeatherMap
2. Start the Weather Consumer to process and store the data in PostgreSQL
3. Wait for initial data collection (30 seconds)
4. Start the DBT runner to transform the data every 5 minutes
5. Handle graceful shutdown of all components when you press Ctrl+C

All logs will be available in the `logs/` directory.

### Option 2: Manual Component Startup

If you prefer to start each component individually:

#### 1. Start the Producer

```bash
python src/producer/weather_producer.py
```

#### 2. Start the Consumer

```bash
python src/consumer/weather_consumer.py
```

#### 3. Run DBT transformations

You can run the DBT transformations either directly or using the provided script:

**Option 1: Using the DBT runner script**
```bash
# Run all models once
python src/run_dbt.py

# Run specific models
python src/run_dbt.py --models daily_weather_summary

# Run models every 15 minutes
python src/run_dbt.py --interval 15

# Enable debug mode for more verbose logging
python src/run_dbt.py --debug
```

**Option 2: Running DBT commands directly**
```bash
cd weather_transforms
dbt run
```

## Components

### Producer

The producer fetches weather data from the OpenWeatherMap API for configured cities and sends it to a Kafka topic.

### Consumer

The consumer processes messages from the Kafka topic and stores them in a PostgreSQL database.

### DBT Transformations

DBT models transform the raw data into:
- Structured weather data from raw JSON
- Daily weather summaries
- Hourly weather summaries

## Database Schema

### Raw Data

- `weather_raw`: Stores raw JSON data from the API
- `weather_metrics`: Stores processed metrics

### Transformed Data (Generated by DBT)

- `stg_weather_raw`: Structured data extracted from raw JSON
- `daily_weather_summary`: Daily aggregates of weather metrics
- `hourly_weather_summary`: Hourly aggregates of weather metrics

## Project Structure

```
realtime_weather_data/
├── config/
│   └── .env
├── docs/
│   └── architecture.md
├── logs/                # Log files directory
├── src/
│   ├── producer/
│   │   ├── __init__.py
│   │   └── weather_producer.py
│   ├── consumer/
│   │   ├── __init__.py
│   │   └── weather_consumer.py
│   ├── setup/
│   │   ├── __init__.py
│   │   └── db_setup.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── check_db_tables.py
│   ├── __init__.py
│   └── run_dbt.py
├── tests/
│   ├── unit/
│   │   └── test_utils.py
│   ├── integration/
│   └── __init__.py
├── weather_transforms/
│   ├── models/
│   │   ├── marts/
│   │   │   └── weather/
│   │   │       ├── daily_weather_summary.sql
│   │   │       ├── hourly_weather_summary.sql
│   │   │       └── temperature_analysis.sql
│   │   ├── staging/
│   │   │   └── stg_weather_raw.sql
│   │   ├── overview.md
│   │   ├── schema.yml
│   │   └── sources.yml
│   ├── macros/
│   │   └── temperature_conversion.sql
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/
│   │   ├── test_daily_summary_metrics.sql
│   │   └── test_hourly_summary_metrics.sql
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── README.md
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
├── pytest.ini
├── README.md
└── requirements.txt
└── run_pipeline.sh
```

## Future Enhancements

- Dockerization for easy deployment
- AWS Fargate deployment
- Web dashboard for data visualization
- Additional data transformations and analysis
- Support for more weather data metrics and cities 

## Testing the Pipeline

The WeatherStream Pipeline includes a comprehensive testing framework (`test_pipeline.py`) that verifies all components are working correctly in an integrated fashion:

```bash
python test_pipeline.py
```

This end-to-end test suite performs the following checks:

1. **API Connection Test**: Verifies the connection to OpenWeatherMap API
   - Validates API key and network connectivity
   - Confirms the ability to fetch weather data for the configured city

2. **Kafka Producer Test**: Verifies message publishing to Confluent Kafka
   - Tests authentication with Confluent Cloud
   - Publishes a test message to the configured topic
   - Confirms successful delivery with acknowledgment

3. **Kafka Consumer Test**: Verifies message consumption from Kafka
   - Tests consumer authentication and subscription
   - Consumes messages from the configured topic
   - Validates message structure and content

4. **Database Connection Test**: Verifies PostgreSQL RDS connectivity
   - Tests authentication with AWS RDS instance
   - Validates database schema and table structure
   - Confirms data has been properly stored
   - Retrieves sample records to verify data integrity

5. **DBT Transformation Test**: Verifies DBT transformations
   - Tests DBT configuration and connectivity to the database
   - Runs transformations on the collected data
   - Validates transformed data structure

### Test Results Interpretation

The test provides a clear summary at the end, indicating which components passed or failed:

```
======= TEST SUMMARY =======
API Connection: ✅ PASS
Kafka Producer: ✅ PASS
Kafka Consumer: ✅ PASS
Database Connection: ✅ PASS
DBT Transformations: ✅ PASS
```

Detailed logs for each test are saved in `logs/pipeline_test.log` for deeper investigation if needed.

### Running Individual Tests

You can also test individual components:

```bash
python test_pipeline.py --test api     # Test just the API connection
python test_pipeline.py --test producer # Test just the Kafka producer
python test_pipeline.py --test consumer # Test just the Kafka consumer
python test_pipeline.py --test database # Test just the database connection
python test_pipeline.py --test dbt      # Test just the DBT transformations
```

This is particularly useful for troubleshooting specific components or when making changes to one part of the pipeline.

## Docker Support

The producer component can be containerized for easier deployment.

### Building the Docker Image

```bash
docker build -t weather-producer .
```

### Running with Docker Compose

```bash
docker-compose up -d
```

This will start the producer container in the background.

## AWS Fargate Deployment

The producer can be deployed to AWS Fargate for serverless container management.

### Prerequisites

1. AWS CLI installed and configured
2. Appropriate AWS IAM permissions
3. AWS Secrets Manager configured with the following secrets:
   - `weather/kafka-api-key`
   - `weather/kafka-api-secret`
   - `weather/openweather-api-key`

### Deployment Steps

1. Navigate to the AWS directory:
   ```bash
   cd aws
   ```

2. Update the networking configuration in `deploy_to_fargate.sh`:
   - Replace `subnet-12345678` with your actual subnet ID
   - Replace `sg-12345678` with your actual security group ID

3. Run the deployment script:
   ```bash
   chmod +x deploy_to_fargate.sh
   ./deploy_to_fargate.sh
   ```

This script will:
- Create an ECR repository if it doesn't exist
- Build and push the Docker image to ECR
- Create an ECS cluster if it doesn't exist
- Register a task definition for Fargate
- Create or update the ECS service

### Monitoring

You can monitor the running container in the AWS ECS console or using AWS CloudWatch Logs.

## Conclusion

This weather data pipeline demonstrates a modern data engineering architecture using Confluent Kafka, PostgreSQL, and DBT. The project showcases:

1. **Real-time Data Collection**: Continuous ingestion of weather data from OpenWeatherMap API
2. **Stream Processing**: Reliable message queuing with Confluent Kafka
3. **Cloud-native Storage**: Persistent storage in AWS RDS PostgreSQL
4. **Data Transformation**: Advanced analytics capabilities with DBT
5. **Deployment Options**: 
   - Traditional local deployment
   - Containerized deployment with Docker
   - Serverless deployment on AWS Fargate

The pipeline has been fully implemented and tested, with all components verified to be working correctly. The architecture is scalable and can be extended to support additional data sources, more complex transformations, or integration with other systems.

This implementation follows modern data engineering best practices, including:
- Clean separation of concerns between components
- Comprehensive logging and error handling
- Thorough testing at each layer
- Infrastructure-as-code approach to deployment
- Documentation-driven development

For any questions or issues, please open a GitHub issue or contact the project maintainer. 