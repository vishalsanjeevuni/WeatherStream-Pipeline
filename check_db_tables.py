#!/usr/bin/env python3
"""
Check Database Tables
Script to verify database connection and list existing tables
"""
import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"Error: .env file not found at {env_path}")
    sys.exit(1)

def check_db_connection():
    """Connect to the database and check what databases exist"""
    # Get database configuration
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'database': 'postgres'  # Connect to default postgres database first
    }
    
    print(f"\nConnecting to PostgreSQL server: {db_config['host']}...")
    
    try:
        # Connect to the postgres database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Check server version
        cursor.execute('SELECT version();')
        version = cursor.fetchone()[0]
        print(f"\n✅ Successfully connected to PostgreSQL server")
        print(f"Version: {version}")
        
        # List all databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;")
        databases = cursor.fetchall()
        
        print(f"\nExisting databases:")
        for db in databases:
            print(f"  - {db[0]}")
        
        # Check if our target database exists
        target_db = os.getenv('POSTGRES_DB', 'weather-db')
        db_exists = any(db[0] == target_db for db in databases)
        
        if db_exists:
            print(f"\n✅ Target database '{target_db}' exists")
            print(f"\nNow checking tables in '{target_db}'...")
            
            # Close the current connection
            cursor.close()
            conn.close()
            
            # Connect to the target database
            db_config['database'] = target_db
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            
            tables = cursor.fetchall()
            
            if tables:
                print(f"\nExisting tables in '{target_db}':")
                for table in tables:
                    print(f"  - {table[0]}")
                    
                # For each table, print the number of rows
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"    • {table[0]} has {count} rows")
            else:
                print(f"\nNo tables found in database '{target_db}'.")
                print("You should run the db_setup.py script to create the necessary tables.")
        else:
            print(f"\n⚠️ Target database '{target_db}' does not exist yet")
            print("You should run the db_setup.py script to create the database and tables.")
        
        # Close connection
        cursor.close()
        conn.close()
        print("\nConnection closed.")
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == "__main__":
    check_db_connection() 