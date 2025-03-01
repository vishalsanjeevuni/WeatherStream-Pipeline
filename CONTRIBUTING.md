# Contributing to the Real-time Weather Data Pipeline

Thank you for considering contributing to this project! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.8+
- PostgreSQL client tools
- Confluent Kafka account
- OpenWeatherMap API key

### Setting Up the Development Environment

1. Clone the repository
   ```bash
   git clone <repository-url>
   cd realtime_weather_data
   ```

2. Create and activate a virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install development dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables
   Create a `config/.env` file with all necessary credentials (see README.md for details).

## Development Workflow

### Code Style

- Follow PEP 8 style guidelines for Python code
- Use meaningful variable and function names
- Add docstrings to all functions and classes
- Format SQL using consistent indentation and capitalization for keywords

### Adding Features

1. Create a feature branch
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Implement your changes
3. Add tests if applicable
4. Update documentation
5. Submit a pull request

### Modifying DBT Models

When modifying or adding DBT models:

1. Update the corresponding schema definitions in `models/schema.yml`
2. Add descriptions for all columns
3. Add tests for data quality
4. Document the changes in `models/overview.md`

## Testing

### Python Code Testing

```bash
# Run tests
pytest

# Check code style
flake8
```

### DBT Model Testing

```bash
cd weather_transforms

# Test all models
dbt test

# Test specific models
dbt test --select daily_weather_summary
```

## Documentation

- Update README.md with any new features or changes
- Update docstrings and comments in the code
- For DBT models, ensure schema.yml is up to date

## Submitting Changes

1. Push your changes to your feature branch
   ```bash
   git push origin feature/your-feature-name
   ```

2. Create a pull request
3. Describe your changes in detail
4. Link to any related issues

## Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [Confluent Kafka Documentation](https://docs.confluent.io/)
- [OpenWeatherMap API Documentation](https://openweathermap.org/api)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) 