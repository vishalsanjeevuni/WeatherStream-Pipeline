#!/usr/bin/env python3
"""
End-to-End Pipeline Test
This script tests the entire data pipeline by verifying each component:
1. Producer can connect to OpenWeatherMap API
2. Producer can publish to Confluent Kafka
3. Consumer can read from Kafka
4. Data is correctly stored in PostgreSQL
5. DBT transformations can run on the data
"""
import os
import sys
import json
import time
import argparse
import requests
import subprocess
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))
from utils import setup_logger

# Setup logging
logger = setup_logger('pipeline_test', 'logs/pipeline_test.log')

class PipelineTest:
    """Test the entire data pipeline end-to-end"""

    def __init__(self):
        """Initialize the test with configs from environment variables"""
        # Kafka configuration
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET')
        }
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_DB', 'weather-db')
        }
        
        # OpenWeatherMap configuration
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY')
        self.city = os.getenv('CITY', 'Toronto')
        self.topic = 'weather_data'
        
        # Set up Kafka producer for testing
        self.producer = Producer(self.kafka_config)
        
        logger.info("Pipeline test initialized")

    def test_api_connection(self):
        """Test connection to OpenWeatherMap API"""
        logger.info("Testing OpenWeatherMap API connection")
        print("\n1. Testing OpenWeatherMap API connection...")
        
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.weather_api_key}"
            response = requests.get(url)
            
            if response.status_code == 200:
                logger.info("OpenWeatherMap API connection successful")
                print("✅ OpenWeatherMap API connection successful")
                return True
            else:
                logger.error(f"API connection failed: {response.status_code} - {response.text}")
                print(f"❌ API connection failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error connecting to API: {e}")
            print(f"❌ Error connecting to API: {e}")
            return False

    def test_kafka_producer(self):
        """Test Kafka producer by publishing a test message"""
        logger.info("Testing Kafka producer connection")
        print("\n2. Testing Kafka producer connection...")
        
        try:
            # Create a test message
            test_data = {
                "timestamp": datetime.now().isoformat(),
                "type": "test",
                "source": "pipeline_test",
                "data": {
                    "message": "This is a test message"
                }
            }
            
            # Define a delivery report callback
            def delivery_report(err, msg):
                if err is not None:
                    logger.error(f"Message delivery failed: {err}")
                    print(f"❌ Message delivery failed: {err}")
                else:
                    logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
                    print(f"✅ Message delivered to {msg.topic()} [partition {msg.partition()}]")
            
            # Produce the message
            self.producer.produce(
                self.topic,
                key="test",
                value=json.dumps(test_data).encode('utf-8'),
                callback=delivery_report
            )
            
            # Wait for message to be delivered
            self.producer.flush(timeout=10)
            return True
            
        except Exception as e:
            logger.error(f"Kafka producer test failed: {e}")
            print(f"❌ Kafka producer test failed: {e}")
            return False

    def test_kafka_consumer(self):
        """Test Kafka consumer by consuming messages"""
        logger.info("Testing Kafka consumer connection")
        print("\n3. Testing Kafka consumer connection...")
        
        try:
            # Create a consumer
            consumer_conf = self.kafka_config.copy()
            consumer_conf.update({
                'group.id': 'pipeline_test_consumer',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True
            })
            
            consumer = Consumer(consumer_conf)
            consumer.subscribe([self.topic])
            
            print("Waiting for messages (10 seconds)...")
            messages_received = 0
            start_time = time.time()
            
            # Try to consume for 10 seconds
            while time.time() - start_time < 10:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        print(f"❌ Consumer error: {msg.error()}")
                else:
                    messages_received += 1
                    logger.info(f"Received message: {msg.value().decode('utf-8')[:100]}...")
                    print(f"✅ Message received from topic {msg.topic()} [partition {msg.partition()}]")
            
            consumer.close()
            
            if messages_received > 0:
                logger.info(f"Successfully consumed {messages_received} messages")
                print(f"✅ Successfully consumed {messages_received} messages")
                return True
            else:
                logger.warning("No messages were consumed in the time window")
                print("⚠️ No messages were consumed in the time window")
                return False
                
        except Exception as e:
            logger.error(f"Kafka consumer test failed: {e}")
            print(f"❌ Kafka consumer test failed: {e}")
            return False

    def test_database_connection(self):
        """Test connection to PostgreSQL database"""
        logger.info("Testing PostgreSQL database connection")
        print("\n4. Testing PostgreSQL database connection...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get DB version
            cursor.execute('SELECT version();')
            db_version = cursor.fetchone()
            
            logger.info(f"Database connection successful: {db_version[0]}")
            print(f"✅ Database connection successful")
            print(f"DB version: {db_version[0]}")
            
            # Check if tables exist
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cursor.fetchall()
            
            logger.info(f"Found {len(tables)} tables in the database")
            print(f"Found {len(tables)} tables in the database:")
            for table in tables:
                print(f"  - {table[0]}")
            
            # Check if there's data in the weather_raw table
            try:
                cursor.execute("SELECT COUNT(*) FROM weather_raw")
                count = cursor.fetchone()[0]
                logger.info(f"Found {count} records in weather_raw table")
                print(f"\nFound {count} records in weather_raw table")
                
                if count > 0:
                    # Show a sample record
                    cursor.execute("SELECT * FROM weather_raw LIMIT 1")
                    record = cursor.fetchone()
                    print(f"Sample record (first few fields):")
                    for i, description in enumerate(cursor.description[:5]):
                        print(f"  {description.name}: {record[i]}")
                    print("  ...")
            except Exception as e:
                logger.warning(f"Could not query weather_raw table: {e}")
                print(f"⚠️ Could not query weather_raw table: {e}")
            
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            print(f"❌ Database connection test failed: {e}")
            return False

    def test_dbt_transformations(self):
        """Test DBT transformations"""
        logger.info("Testing DBT transformations")
        print("\n5. Testing DBT transformations...")
        
        try:
            # Change to the weather_transforms directory
            os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'weather_transforms'))
            
            # Run dbt debug to check connections
            result = subprocess.run(['dbt', 'debug'], 
                                   capture_output=True, 
                                   text=True)
            
            if result.returncode != 0:
                logger.error(f"DBT debug failed: {result.stderr}")
                print(f"❌ DBT debug failed: {result.stderr}")
                return False
            
            logger.info("DBT configuration is valid")
            print("✅ DBT configuration is valid")
            
            # Run dbt run to execute transformations
            print("\nRunning DBT transformations...")
            result = subprocess.run(['dbt', 'run'], 
                                   capture_output=True, 
                                   text=True)
            
            if result.returncode != 0:
                logger.error(f"DBT run failed: {result.stderr}")
                print(f"❌ DBT run failed: {result.stderr}")
                return False
            
            logger.info("DBT transformations executed successfully")
            print("✅ DBT transformations executed successfully")
            
            # Return to original directory
            os.chdir(os.path.dirname(os.path.abspath(__file__)))
            
            return True
            
        except Exception as e:
            logger.error(f"DBT transformation test failed: {e}")
            print(f"❌ DBT transformation test failed: {e}")
            # Return to original directory
            os.chdir(os.path.dirname(os.path.abspath(__file__)))
            return False

    def run_all_tests(self):
        """Run all pipeline tests"""
        print("\n======= WEATHER PIPELINE END-TO-END TEST =======\n")
        
        tests = [
            ("API Connection", self.test_api_connection),
            ("Kafka Producer", self.test_kafka_producer),
            ("Kafka Consumer", self.test_kafka_consumer),
            ("Database Connection", self.test_database_connection),
            ("DBT Transformations", self.test_dbt_transformations)
        ]
        
        results = {}
        
        for name, test_func in tests:
            result = test_func()
            results[name] = result
        
        # Summary
        print("\n======= TEST SUMMARY =======")
        all_passed = True
        for name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            if not result:
                all_passed = False
            print(f"{name}: {status}")
        
        if all_passed:
            print("\n🎉 All tests passed! Your pipeline is working correctly.")
        else:
            print("\n⚠️ Some tests failed. Please check the logs for details.")
        
        return all_passed

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the weather data pipeline end-to-end")
    parser.add_argument("--test", choices=["api", "producer", "consumer", "database", "dbt", "all"], 
                        default="all", help="Which component to test")
    args = parser.parse_args()
    
    test = PipelineTest()
    
    if args.test == "api":
        test.test_api_connection()
    elif args.test == "producer":
        test.test_kafka_producer()
    elif args.test == "consumer":
        test.test_kafka_consumer()
    elif args.test == "database":
        test.test_database_connection()
    elif args.test == "dbt":
        test.test_dbt_transformations()
    else:
        test.run_all_tests() 