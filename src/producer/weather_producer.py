#!/usr/bin/env python3
"""
Weather Data Producer
Fetches weather data from OpenWeatherMap API and publishes it to a Kafka topic.
"""
import sys
import os
import json
import time
from datetime import datetime
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

# Explicitly load environment variables from the config/.env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'config', '.env')
print(f"Looking for .env file at: {env_path}")
if os.path.exists(env_path):
    print(f"Found .env file!")
    load_dotenv(env_path)
else:
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import setup_logger

# Set up logging
os.makedirs('logs', exist_ok=True)
logger = setup_logger('weather_producer', 'logs/weather_producer.log')

class WeatherProducer:
    """Producer that fetches weather data and publishes it to Kafka"""
    
    def __init__(self):
        """Initialize the producer with configuration"""
        # Configure Kafka settings directly from environment variables
        kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET')
        }
        
        # Print Kafka configuration (with masked password)
        masked_config = kafka_config.copy()
        if masked_config['sasl.password']:
            masked_config['sasl.password'] = '*' * len(masked_config['sasl.password'])
        print("Kafka configuration:")
        for key, value in masked_config.items():
            print(f"  {key}: {value}")
        
        # Create the Kafka producer
        self.producer = Producer(kafka_config)
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.city = os.getenv('CITY', 'Toronto')
        self.topic = 'weather_data'
        
        logger.info("Weather Producer initialized for city: %s", self.city)
        print(f"Weather Producer initialized for city: {self.city}")
        
    def fetch_weather(self):
        """Fetch current weather data from OpenWeatherMap API"""
        url = f'http://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.api_key}&units=metric'
        
        try:
            logger.debug("Fetching weather data from OpenWeatherMap API")
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # Add timestamp for when we received the data
                data['ingestion_timestamp'] = datetime.now().isoformat()
                logger.info("Successfully fetched weather data")
                return data
            else:
                logger.error("Failed to fetch weather data: HTTP %s - %s", 
                           response.status_code, response.text)
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error("Error fetching weather data: %s", str(e))
            return None
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error('Message delivery failed: %s', err)
        else:
            logger.info('Message delivered to %s [%s] at offset %s',
                      msg.topic(), msg.partition(), msg.offset())
    
    def publish_weather_data(self, weather_data):
        """Publish weather data to Kafka topic"""
        if weather_data:
            try:
                # Convert data to JSON string and encode as bytes
                message = json.dumps(weather_data).encode('utf-8')
                
                # Produce message to topic
                self.producer.produce(
                    self.topic,
                    value=message,
                    callback=self.delivery_report
                )
                
                # Wait for any outstanding messages to be delivered
                self.producer.flush()
                
                logger.info("Weather data published to Kafka topic: %s", self.topic)
                print(f"Weather data published to Kafka topic: {self.topic}")
                return True
                
            except Exception as e:
                logger.error("Failed to publish message: %s", str(e))
                print(f"Error: Failed to publish message: {str(e)}")
                return False
        else:
            logger.warning("No weather data to publish")
            print("Warning: No weather data to publish")
            return False
    
    def run(self, interval=60):
        """Run the producer in a loop, fetching and publishing data at intervals"""
        logger.info("Starting weather data producer with %s second interval", interval)
        print(f"Starting weather data producer with {interval} second interval")
        
        try:
            while True:
                print("\nFetching weather data...")
                weather_data = self.fetch_weather()
                self.publish_weather_data(weather_data)
                
                logger.debug("Sleeping for %s seconds", interval)
                print(f"Sleeping for {interval} seconds...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
            print("\nProducer stopped by user")
        except Exception as e:
            logger.error("Unexpected error: %s", str(e))
            print(f"Unexpected error: {str(e)}")
        finally:
            logger.info("Shutting down producer")
            print("Shutting down producer")

if __name__ == "__main__":
    producer = WeatherProducer()
    
    # If an argument is provided, use it as the interval in seconds
    interval = 60
    if len(sys.argv) > 1:
        try:
            interval = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid interval specified, using default 60 seconds")
    
    producer.run(interval) 