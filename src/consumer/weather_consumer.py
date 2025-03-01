#!/usr/bin/env python3
"""
Weather Data Consumer
Consumes weather data from Kafka topic and processes it
"""
import sys
import os
import json
import signal
from datetime import datetime
import psycopg2
from confluent_kafka import Consumer, KafkaError
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
logger = setup_logger('weather_consumer', 'logs/weather_consumer.log')

class WeatherConsumer:
    """Consumer that reads weather data from Kafka and processes it"""
    
    def __init__(self):
        """Initialize the consumer with configuration"""
        # Configure Kafka consumer
        consumer_conf = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET'),
            'group.id': 'weather_consumers',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        # Print Kafka configuration (with masked password)
        masked_config = consumer_conf.copy()
        if masked_config['sasl.password']:
            masked_config['sasl.password'] = '*' * len(masked_config['sasl.password'])
        print("Kafka configuration:")
        for key, value in masked_config.items():
            print(f"  {key}: {value}")
        
        self.consumer = Consumer(consumer_conf)
        self.topic = 'weather_data'
        self.running = True
        
        # Configure database connection
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_DB', 'weather-db')
        }
        
        # Print DB configuration (with masked password)
        masked_db = self.db_params.copy()
        if masked_db['password']:
            masked_db['password'] = '*' * len(masked_db['password'])
        print("\nDatabase configuration:")
        for key, value in masked_db.items():
            print(f"  {key}: {value}")
        
        logger.info("Weather Consumer initialized")
        print("Weather Consumer initialized")
    
    def connect_to_db(self):
        """Connect to the PostgreSQL database"""
        try:
            return psycopg2.connect(**self.db_params)
        except psycopg2.Error as e:
            logger.error("Failed to connect to database: %s", e)
            print(f"Error: Failed to connect to database: {e}")
            return None
    
    def store_raw_data(self, data):
        """Store raw JSON data in the weather_raw table"""
        conn = self.connect_to_db()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            query = "INSERT INTO weather_raw (data) VALUES (%s) RETURNING id"
            cursor.execute(query, (json.dumps(data),))
            record_id = cursor.fetchone()[0]
            conn.commit()
            logger.info("Stored raw weather data with ID: %s", record_id)
            print(f"Stored raw weather data with ID: {record_id}")
            return True
        except psycopg2.Error as e:
            logger.error("Failed to store raw data: %s", e)
            print(f"Error: Failed to store raw data: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def process_weather_data(self, data):
        """Extract specific metrics from weather data and store in weather_metrics table"""
        try:
            # Extract relevant fields
            city = data.get('name', 'Unknown')
            main = data.get('main', {})
            wind = data.get('wind', {})
            weather = data.get('weather', [{}])[0]
            
            # Get measurement time (from data or current time if not present)
            measurement_time = datetime.fromtimestamp(data.get('dt', datetime.now().timestamp()))
            
            # Extract weather metrics
            metrics = {
                'city': city,
                'temperature': main.get('temp', 0),
                'humidity': main.get('humidity', 0),
                'pressure': main.get('pressure', 0),
                'wind_speed': wind.get('speed', 0),
                'weather_condition': weather.get('main', 'Unknown'),
                'measurement_time': measurement_time
            }
            
            print(f"\nProcessed weather metrics for {city}:")
            print(f"  Temperature: {metrics['temperature']}°C")
            print(f"  Humidity: {metrics['humidity']}%")
            print(f"  Pressure: {metrics['pressure']} hPa")
            print(f"  Wind Speed: {metrics['wind_speed']} m/s")
            print(f"  Condition: {metrics['weather_condition']}")
            
            # Store in database
            conn = self.connect_to_db()
            if not conn:
                return False
            
            try:
                cursor = conn.cursor()
                query = """
                    INSERT INTO weather_metrics 
                    (city, temperature, humidity, pressure, wind_speed, weather_condition, measurement_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cursor.execute(query, (
                    metrics['city'],
                    metrics['temperature'],
                    metrics['humidity'],
                    metrics['pressure'],
                    metrics['wind_speed'],
                    metrics['weather_condition'],
                    metrics['measurement_time']
                ))
                record_id = cursor.fetchone()[0]
                conn.commit()
                logger.info("Processed and stored weather metrics with ID: %s", record_id)
                print(f"Stored processed weather metrics with ID: {record_id}")
                return True
            except psycopg2.Error as e:
                logger.error("Failed to process and store metrics: %s", e)
                print(f"Error: Failed to process and store metrics: {e}")
                conn.rollback()
                return False
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error("Error processing weather data: %s", e)
            print(f"Error processing weather data: {e}")
            return False
    
    def handle_message(self, msg):
        """Process a single message from Kafka"""
        try:
            # Decode JSON message
            message_value = msg.value().decode('utf-8')
            data = json.loads(message_value)
            
            logger.debug("Received weather data for %s", data.get('name', 'Unknown'))
            print(f"\nReceived weather data for {data.get('name', 'Unknown')}")
            
            # Store raw data
            self.store_raw_data(data)
            
            # Process and store metrics
            self.process_weather_data(data)
            
            return True
        except json.JSONDecodeError as e:
            logger.error("Failed to decode JSON message: %s", e)
            print(f"Error: Failed to decode JSON message: {e}")
            return False
        except Exception as e:
            logger.error("Error handling message: %s", e)
            print(f"Error handling message: {e}")
            return False
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        self.running = False
    
    def run(self):
        """Start consuming messages from Kafka topic"""
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        logger.info("Subscribed to topic: %s", self.topic)
        print(f"Subscribed to topic: {self.topic}")
        
        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutdown signal received")
            print("Shutdown signal received")
            self.shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print(f"Waiting for messages on topic '{self.topic}'...\n")
        try:
            # Poll for messages
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug("Reached end of partition")
                    else:
                        logger.error("Consumer error: %s", msg.error())
                        print(f"Consumer error: {msg.error()}")
                else:
                    # Process message
                    self.handle_message(msg)
                    
                    # Manually commit offset
                    self.consumer.commit(msg)
            
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            print(f"Unexpected error: {e}")
        
        finally:
            # Close consumer
            self.consumer.close()
            logger.info("Consumer closed")
            print("Consumer closed")

if __name__ == "__main__":
    consumer = WeatherConsumer()
    consumer.run() 