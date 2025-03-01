#!/usr/bin/env python3
"""
Database Setup Script
Creates the PostgreSQL database and tables for the weather application
"""
import sys
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Set up path for config file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', '.env')
print(f"Looking for .env file at: {env_path}")
if os.path.exists(env_path):
    print(f"Found .env file!")
    load_dotenv(env_path)
else:
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

# Add the parent directory to the path for importing utils
sys.path.append('.')
from src.utils import setup_logger

# Set up logging
logger = setup_logger('db_setup', 'logs/db_setup.log')

def create_database():
    """Create the PostgreSQL database if it doesn't exist"""
    db_name = os.getenv('POSTGRES_DB', 'weather-db')
    params = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': 'postgres'  # Connect to default postgres database first
    }
    
    # Display connection info
    print(f"Connecting to PostgreSQL server at {params['host']} as {params['user']}...")
    
    # Connect to PostgreSQL server
    logger.info(f"Connecting to PostgreSQL server at {params['host']}")
    conn = psycopg2.connect(**params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    try:
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        
        if not exists:
            logger.info(f"Creating database '{db_name}'")
            print(f"Creating database '{db_name}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            logger.info(f"Database '{db_name}' created successfully")
            print(f"✅ Database '{db_name}' created successfully")
        else:
            logger.info(f"Database '{db_name}' already exists")
            print(f"ℹ️ Database '{db_name}' already exists")
    
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating database: {error}")
        print(f"❌ Error creating database: {error}")
    
    finally:
        cursor.close()
        conn.close()

def create_tables():
    """Create the necessary tables in the database"""
    params = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': os.getenv('POSTGRES_DB', 'weather-db')
    }
    
    # Display connection info
    print(f"Connecting to database '{params['database']}' to create tables...")
    
    # Tables to create
    tables = [
        """
        CREATE TABLE IF NOT EXISTS weather_raw (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS weather_metrics (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            temperature NUMERIC NOT NULL,
            humidity NUMERIC NOT NULL,
            pressure NUMERIC NOT NULL,
            wind_speed NUMERIC NOT NULL,
            weather_condition VARCHAR(100),
            measurement_time TIMESTAMP NOT NULL,
            ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]
    
    logger.info(f"Connecting to database '{params['database']}'")
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        # Create each table
        for table_query in tables:
            cursor.execute(table_query)
        
        # Commit the changes
        conn.commit()
        
        logger.info("Tables created successfully")
        print("✅ Tables created successfully")
        
        # List the tables that were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("\nTables in database:")
        for table in tables:
            print(f"  - {table[0]}")
    
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error creating tables: {error}")
        print(f"❌ Error creating tables: {error}")
    
    finally:
        if conn is not None:
            cursor.close()
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    print("\n=== Setting up the database ===\n")
    # Create database and tables
    create_database()
    create_tables()
    
    print("\n✅ Database setup completed!\n")
    logger.info("Database setup completed") 