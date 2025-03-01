#!/usr/bin/env python3
"""
Environment Setup Script
Helps create a .env file from the .env.example template by prompting for values
"""
import os
import shutil
import sys

def setup_env():
    """Set up the .env file by copying .env.example and prompting for values"""
    
    # Ensure the config directory exists
    if not os.path.exists('config'):
        os.makedirs('config')
    
    # Check if .env.example exists
    example_path = os.path.join('config', '.env.example')
    env_path = os.path.join('config', '.env')
    
    if not os.path.exists(example_path):
        print(f"Error: '{example_path}' not found. Make sure you're in the project root directory.")
        return False
    
    # Check if .env already exists
    if os.path.exists(env_path):
        overwrite = input(f"'{env_path}' already exists. Overwrite? (y/n): ").lower().strip()
        if overwrite != 'y':
            print("Setup cancelled.")
            return False
    
    # Copy .env.example to .env
    shutil.copy2(example_path, env_path)
    print(f"Created '{env_path}' from template.")
    
    # Read the .env file
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Prompt for values
    new_lines = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            new_lines.append(line)
            continue
        
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            
            # Skip if already has a real value (not the example placeholder)
            if not ('your_' in value or value == ''):
                new_value = input(f"Enter value for {key} [{value}]: ").strip()
                if new_value:
                    new_lines.append(f"{key}={new_value}")
                else:
                    new_lines.append(line)
            else:
                new_value = input(f"Enter value for {key}: ").strip()
                if new_value:
                    new_lines.append(f"{key}={new_value}")
                else:
                    new_lines.append(line)
        else:
            new_lines.append(line)
    
    # Write the updated .env file
    with open(env_path, 'w') as f:
        f.write('\n'.join(new_lines))
    
    print(f"Updated '{env_path}' with your values.")
    print("Environment setup complete!")
    return True

if __name__ == "__main__":
    setup_env() 