# Database Configuration Documentation

This document describes the database configuration system for the MLB Betting Program, including centralized configuration and environment variable fallbacks.

## Overview

The project uses a **centralized configuration system** with **environment variable fallbacks** for maximum flexibility across development, testing, and production environments.

## Configuration Hierarchy

### 1. Primary: Centralized Configuration (`config.toml`)

The main configuration source is `config.toml` in the project root:

```toml
[database]
host = "localhost"
port = 5432
database = "mlb_betting"
user = "your_username"
password = "your_password"
```

**Accessed via**: `src.core.config.get_settings()`

### 2. Fallback: Environment Variables

When centralized configuration is unavailable (e.g., standalone utility scripts), the system falls back to environment variables:

| Environment Variable | Description | Default Value |
|---------------------|-------------|---------------|
| `DB_HOST` | Database host address | `localhost` |
| `DB_PORT` | Database port number | `5432` |
| `DB_NAME` | Database name | `mlb_betting` |
| `DB_USER` | Database username | `samlafell` |
| `DB_PASSWORD` | Database password | `""` (empty) |

## Usage Patterns

### Production Components (Centralized Config)

All main application components use centralized configuration:

```python
from src.core.config import get_settings

# Get centralized settings
settings = get_settings()
db_config = {
    "host": settings.database.host,
    "port": settings.database.port,
    "database": settings.database.database,
    "user": settings.database.user,
    "password": settings.database.password
}
```

**Components using centralized config:**
- `src/data/pipeline/staging_action_network_history_processor.py`
- `src/data/pipeline/staging_action_network_unified_processor.py`
- `src/data/pipeline/staging_action_network_historical_processor.py`
- `src/data/collection/mlb_stats_api_collector.py`
- `src/services/cross_site_game_resolution_service.py`
- `src/interfaces/cli/commands/line_movement.py`

### Utility Scripts (Environment Variable Fallback)

Standalone utility scripts use a fallback pattern:

```python
try:
    # Try centralized configuration first
    from src.core.config import get_settings
    settings = get_settings()
    db_config = {
        "host": settings.database.host,
        "port": settings.database.port,
        "database": settings.database.database,
        "user": settings.database.user,
        "password": settings.database.password
    }
except ImportError:
    # Fallback to environment variables
    import os
    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME", "mlb_betting"),
        "user": os.getenv("DB_USER", "samlafell"),
        "password": os.getenv("DB_PASSWORD", "")
    }
```

**Utility scripts with fallback pattern:**
- `utilities/load_action_network_complete_history.py`

## Environment Setup Examples

### Development (.env file)

Create a `.env` file in the project root:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mlb_betting_dev
DB_USER=developer
DB_PASSWORD=dev_password
```

### Testing Environment

```bash
export DB_HOST=test-host
export DB_PORT=5433
export DB_NAME=mlb_betting_test
export DB_USER=test_user
export DB_PASSWORD=test_password
```

### Production Environment

```bash
export DB_HOST=prod-database.example.com
export DB_PORT=5432
export DB_NAME=mlb_betting_prod
export DB_USER=app_user
export DB_PASSWORD=secure_production_password
```

### Docker Environment

```yaml
# docker-compose.yml
environment:
  - DB_HOST=postgres
  - DB_PORT=5432
  - DB_NAME=mlb_betting
  - DB_USER=postgres
  - DB_PASSWORD=postgres_password
```

## Security Considerations

### Password Management

**✅ Recommended:**
- Use environment variables for passwords in production
- Use secrets management systems (AWS Secrets Manager, etc.)
- Never commit passwords to version control

**❌ Avoid:**
- Hardcoding passwords in source code
- Storing passwords in plain text config files

### Connection Security

**✅ Recommended:**
- Use SSL/TLS connections in production
- Implement connection pooling
- Use principle of least privilege for database users

## Configuration Testing

The project includes comprehensive integration tests that verify configuration patterns:

### Database Configuration Tests

**File**: `tests/integration/test_database_configuration_integration.py`

**Tests include:**
- Centralized configuration usage across all processors
- No hardcoded database values in runtime
- Environment variable fallback compatibility
- Configuration consistency across components

### Collector Configuration Tests

**File**: `tests/integration/test_collector_configuration_integration.py`

**Tests include:**
- CollectorConfig pattern compliance
- Async/sync method handling for different collectors
- Configuration validation and immutability
- CLI compatibility with different collector types

### Running Configuration Tests

```bash
# Run all integration tests
uv run pytest tests/integration/

# Run database configuration tests only
uv run pytest tests/integration/test_database_configuration_integration.py

# Run collector configuration tests only
uv run pytest tests/integration/test_collector_configuration_integration.py
```

## Troubleshooting

### Common Issues

**1. Connection Refused**
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Check connection with specific credentials
psql -h localhost -p 5432 -U your_user -d mlb_betting
```

**2. Permission Denied**
```bash
# Verify user exists and has correct permissions
sudo -u postgres createuser --interactive your_user
sudo -u postgres createdb -O your_user mlb_betting
```

**3. Configuration Not Found**
```python
# Verify config.toml exists and is properly formatted
from src.core.config import get_settings
settings = get_settings()
print(settings.database)
```

### Validation Commands

```bash
# Test database connectivity
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Test collector configuration
uv run -m src.interfaces.cli data test --source vsin --real
uv run -m src.interfaces.cli data test --source sbd --real
```

## Migration Notes

### From Hardcoded Configuration

If migrating from hardcoded database configuration:

1. **Replace hardcoded values** with `get_settings()` calls
2. **Add import** for centralized configuration
3. **Update method signatures** to use centralized config
4. **Test with integration tests** to verify changes

### Example Migration

**Before (hardcoded):**
```python
db_config = {
    "host": "localhost",
    "port": 5432,
    "database": "mlb_betting",
    "user": "samlafell",
    "password": ""
}
```

**After (centralized):**
```python
from src.core.config import get_settings

def _get_db_config(self) -> Dict[str, Any]:
    """Get database configuration from centralized settings."""
    settings = get_settings()
    return {
        "host": settings.database.host,
        "port": settings.database.port,
        "database": settings.database.database,
        "user": settings.database.user,
        "password": settings.database.password
    }
```

## Best Practices

### 1. Configuration Access

**✅ Do:**
- Use centralized `get_settings()` for all production code
- Cache configuration in instance variables when appropriate
- Provide fallback patterns for utility scripts

**❌ Don't:**
- Hardcode database credentials
- Bypass centralized configuration system
- Store passwords in source code

### 2. Environment Variables

**✅ Do:**
- Use descriptive environment variable names
- Provide sensible defaults for development
- Document all environment variables

**❌ Don't:**
- Use environment variables as primary configuration in production code
- Mix environment variable access throughout the codebase

### 3. Testing

**✅ Do:**
- Test configuration patterns with integration tests
- Mock settings in unit tests
- Validate environment variable fallbacks

**❌ Don't:**
- Skip configuration testing
- Test against production databases
- Hardcode test database credentials