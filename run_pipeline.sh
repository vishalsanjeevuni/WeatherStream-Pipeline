#!/bin/bash
# Script to launch the complete weather data pipeline

# Function to handle cleanup on exit
cleanup() {
    echo "Stopping the pipeline components..."
    kill $PRODUCER_PID 2>/dev/null
    kill $CONSUMER_PID 2>/dev/null
    kill $DBT_PID 2>/dev/null
    exit 0
}

# Set up trap to catch Ctrl+C and other termination signals
trap cleanup SIGINT SIGTERM

# Ensure logs directory exists
mkdir -p logs

# Activate the virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Check if environment file exists
if [ ! -f "config/.env" ]; then
    echo "ERROR: config/.env file not found. Please create it first."
    exit 1
fi

echo "Starting the Weather Data Pipeline..."

# Start the producer in the background
echo "Starting Weather Producer..."
python src/producer/weather_producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"

# Wait a bit for the producer to connect to Kafka
sleep 5

# Start the consumer in the background
echo "Starting Weather Consumer..."
python src/consumer/weather_consumer.py > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
echo "Consumer started with PID: $CONSUMER_PID"

# Wait for some data to be collected before running DBT
echo "Waiting for initial data collection (30 seconds)..."
sleep 30

# Run DBT transformations with a 5-minute interval
echo "Starting DBT transformations (running every 5 minutes)..."
python src/run_dbt.py --interval 5 > logs/dbt.log 2>&1 &
DBT_PID=$!
echo "DBT runner started with PID: $DBT_PID"

echo "Pipeline is now running. Press Ctrl+C to stop all components."
echo "Logs are available in the logs/ directory:"
echo " - Producer: logs/producer.log"
echo " - Consumer: logs/consumer.log"
echo " - DBT: logs/dbt.log"

# Keep the script running until Ctrl+C
while true; do
    sleep 1
done 