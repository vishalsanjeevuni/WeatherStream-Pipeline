#!/usr/bin/env python3
"""
Test PostgreSQL Connection
Simple script to verify that the PostgreSQL credentials in the .env file are correct
"""
import sys
import os
import psycopg2
from dotenv import load_dotenv

# Explicitly load the .env file from the config directory
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', '.env')
print(f"Looking for .env file at: {env_path}")
if os.path.exists(env_path):
    print(f"Found .env file!")
    load_dotenv(env_path)
else:
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

def test_connection():
    """Test connection to PostgreSQL using credentials from .env file"""
    # Extract PostgreSQL configuration directly from environment variables
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': 'postgres'  # Connect to default database first
    }
    
    # Display configuration (with masked password)
    masked_config = db_config.copy()
    if masked_config['password']:
        masked_config['password'] = '*' * len(masked_config['password'])
    
    print("\nAttempting to connect with the following configuration:")
    for key, value in masked_config.items():
        print(f"  {key}: {value}")
    
    # Test connection
    try:
        print("\nConnecting to PostgreSQL server...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Get server version
        cursor.execute('SELECT version();')
        db_version = cursor.fetchone()
        
        print(f"✅ Connection successful!")
        print(f"PostgreSQL server version: {db_version[0]}")
        
        # Check if target database exists
        target_db = os.getenv('POSTGRES_DB', 'weather-db')
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
        exists = cursor.fetchone()
        
        if exists:
            print(f"✅ Database '{target_db}' exists")
        else:
            print(f"ℹ️ Database '{target_db}' does not exist yet (will be created by db_setup.py)")
        
        # Close connection
        cursor.close()
        conn.close()
        print("Connection closed.")
        
        print("\n✅ Your PostgreSQL configuration is correct!")
        print("\nYou can now proceed to run the database setup script:")
        print("  python src/db_setup.py")
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Connection failed: {e}")
        print("\nPlease check your PostgreSQL configuration in the .env file.")
        print("Common issues:")
        print("  - Incorrect hostname or port")
        print("  - Invalid username or password")
        print("  - Security group not allowing connections from your IP")
        print("  - Database instance is not publicly accessible")
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_connection() 