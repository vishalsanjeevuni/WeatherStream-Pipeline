import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

def load_config():
    """Load configuration from environment variables or .env file"""
    # Load .env file if it exists
    load_dotenv()
    
    config = {
        # Kafka configuration
        'kafka': {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('KAFKA_API_KEY'),
            'sasl.password': os.getenv('KAFKA_API_SECRET')
        },
        # OpenWeatherMap configuration
        'openweather': {
            'api_key': os.getenv('OPENWEATHER_API_KEY'),
            'city': os.getenv('CITY', 'Toronto')
        },
        # PostgreSQL configuration
        'postgres': {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_DB', 'weather_db')
        }
    }
    
    return config

def setup_logger(name, log_file, level=logging.INFO):
    """Set up a logger with file and console handlers"""
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create handlers
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=3
    )
    console_handler = logging.StreamHandler()
    
    # Create formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger 