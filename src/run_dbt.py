#!/usr/bin/env python
"""
Script to run DBT models for the weather data pipeline.
This script automates the execution of DBT transformations after 
the producer and consumer have collected data.
"""

import os
import sys
import subprocess
import logging
import argparse
import time
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/dbt_runner.log', mode='a')
    ]
)
logger = logging.getLogger('dbt_runner')

# Ensure logs directory exists
Path('logs').mkdir(exist_ok=True)

def get_project_root():
    """Return the project root directory."""
    current_file = Path(__file__).resolve()
    return current_file.parent.parent

def run_dbt_command(command, dbt_project_dir):
    """Run a DBT command and log the output."""
    full_command = f'cd {dbt_project_dir} && {command}'
    logger.info(f"Running command: {full_command}")
    
    try:
        process = subprocess.Popen(
            full_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Stream and log the output in real-time
        for line in process.stdout:
            logger.info(line.strip())
        
        # Wait for the process to complete
        process.wait()
        
        # Check if there were any errors
        if process.returncode != 0:
            for line in process.stderr:
                logger.error(line.strip())
            raise subprocess.CalledProcessError(process.returncode, full_command)
            
        return process.returncode
    except Exception as e:
        logger.error(f"Error running command: {e}")
        raise

def main():
    """Main function to run DBT models."""
    parser = argparse.ArgumentParser(description='Run DBT models for weather data pipeline')
    parser.add_argument(
        '--models', 
        default='all', 
        help='Specify which models to run (e.g., "daily_weather_summary", "hourly_weather_summary")'
    )
    parser.add_argument(
        '--interval', 
        type=int, 
        default=0, 
        help='Run DBT models at the specified interval in minutes (0 means run once and exit)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true', 
        help='Enable debug mode with increased verbosity'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")
    
    project_root = get_project_root()
    dbt_project_dir = os.path.join(project_root, 'weather_transforms')
    
    logger.info(f"Project root: {project_root}")
    logger.info(f"DBT project directory: {dbt_project_dir}")
    
    # Validate the DBT project directory
    if not os.path.exists(os.path.join(dbt_project_dir, 'dbt_project.yml')):
        logger.error(f"DBT project not found at {dbt_project_dir}")
        sys.exit(1)
    
    # Determine which models to run
    if args.models == 'all':
        dbt_command = "dbt run"
    else:
        dbt_command = f"dbt run --select {args.models}"
    
    # If interval is set, run models at the specified interval
    if args.interval > 0:
        logger.info(f"Running DBT models every {args.interval} minutes")
        try:
            while True:
                run_dbt_command(dbt_command, dbt_project_dir)
                logger.info(f"Waiting {args.interval} minutes for next run...")
                time.sleep(args.interval * 60)
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
    else:
        # Run once and exit
        logger.info("Running DBT models once")
        run_dbt_command(dbt_command, dbt_project_dir)
    
    logger.info("DBT run completed successfully")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1) 