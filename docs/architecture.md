# WeatherStream Pipeline Architecture

This document provides a detailed overview of the architecture of the WeatherStream Pipeline.

## System Components

### 1. Producer

The producer component is responsible for:
- Fetching weather data from the OpenWeatherMap API
- Adding metadata to the raw data
- Publishing messages to Kafka

**Key Files:**
- `src/producer/weather_producer.py`: Main producer implementation

**Technology Stack:**
- Python
- Requests library for API calls
- Confluent Kafka Python client

### 2. Message Broker

Confluent Kafka serves as the message broker in the pipeline:
- Handles asynchronous communication between producer and consumer
- Provides fault tolerance and scalability
- Supports exactly-once semantics
- Manages retention of messages

**Configuration:**
- Topic: weather_data
- Partitioning: Based on city name
- Retention: 7 days

### 3. Consumer

The consumer component:
- Subscribes to the Kafka topic
- Processes incoming messages
- Stores data in the PostgreSQL database

**Key Files:**
- `src/consumer/weather_consumer.py`: Main consumer implementation

**Technology Stack:**
- Python
- Confluent Kafka Python client
- psycopg2 for PostgreSQL connectivity

### 4. Database

PostgreSQL database stores:
- Raw weather data as JSON
- Processed metrics in structured format

**Key Tables:**
- `weather_raw`: Stores raw JSON data from the API
- `weather_metrics`: Stores processed metrics

### 5. Transformation Layer

DBT (Data Build Tool) provides:
- Data transformations for analytics
- Documentation of data models
- Testing of data quality

**Key Files:**
- `weather_transforms/models/`: Contains all DBT models
- `weather_transforms/tests/`: Contains data quality tests

**Technology Stack:**
- DBT Core
- PostgreSQL adapter

## Data Flow

1. **Data Collection**:
   - Producer fetches data from OpenWeatherMap API
   - Data is serialized to JSON format
   - Messages are published to Kafka topic

2. **Data Ingestion**:
   - Consumer reads messages from Kafka topic
   - Raw JSON is stored in `weather_raw` table
   - Structured metrics are extracted and stored in `weather_metrics` table

3. **Data Transformation**:
   - DBT models read data from the PostgreSQL tables
   - Transformations are applied to create aggregated views
   - Results are stored as tables/views in the database

4. **Data Consumption**:
   - Transformed data is available for analytics and reporting
   - Data can be queried directly from the PostgreSQL database

## Deployment Architecture

Currently, the application is designed to run on a single machine with all components running in separate processes. Future enhancements could include:

- Docker containerization
- AWS deployment
- Kubernetes orchestration 