FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY config/ ./config/
COPY src/ ./src/
COPY logs/ ./logs/

# Make sure the directory for logs exists
RUN mkdir -p logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["python", "src/producer/weather_producer.py"] 