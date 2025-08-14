# Environment Configuration Guide

This guide explains how to configure environment variables for different deployment scenarios.

## Quick Setup

1. **Copy the template**:
   ```bash
   cp .env.example .env
   ```

2. **Update required variables** in `.env`:
   - `POSTGRES_USER` and `POSTGRES_PASSWORD`
   - `API_SECRET_KEY` and `JWT_SECRET_KEY`
   - Environment-specific settings

3. **Never commit** your `.env` file (already in `.gitignore`)

## Environment Scenarios

### 🔧 Local Development

For local development with Docker PostgreSQL:

```bash
# Database (using Docker PostgreSQL on port 5433)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=samlafell
POSTGRES_PASSWORD=postgres

# API
API_PORT=8000
DEBUG=false
LOG_LEVEL=INFO

# Redis (local)
REDIS_HOST=localhost
REDIS_PORT=6379
```

### 🏗️ Docker Compose (ML Pipeline)

For running the ML pipeline in Docker containers:

```bash
# Database (host.docker.internal to access host PostgreSQL)
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@host.docker.internal:5432/${POSTGRES_DB}

# Redis (container name)
REDIS_HOST=redis
REDIS_URL=redis://redis:6379/0

# MLflow (container name)
MLFLOW_TRACKING_URI=http://mlflow:5000
```

### 🚀 Staging Environment

For staging deployment:

```bash
ENVIRONMENT=staging
DEBUG=false
LOG_LEVEL=DEBUG

# Use staging hostnames
DATABASE_HOST=your_staging_db_host
REDIS_URL=redis://your_staging_redis_host:6379/0

# Moderate rate limits
RATE_LIMIT_REQUESTS_PER_MINUTE=120
PREDICTION_RATE_LIMIT=20

# SSL preferred
DATABASE_SSL_MODE=prefer
REDIS_SSL=false
```

### 🏭 Production Environment

For production deployment:

```bash
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO

# Production security
DATABASE_SSL_MODE=require
REDIS_SSL=true
SECURITY_HEADERS_ENABLED=true
HSTS_MAX_AGE=31536000

# Strict rate limits
RATE_LIMIT_REQUESTS_PER_MINUTE=60
PREDICTION_RATE_LIMIT=10

# Performance optimization
MAX_WORKERS=4
WORKER_TIMEOUT=120
```

## Required Variables

### Core Requirements (All Environments)
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `API_SECRET_KEY`, `JWT_SECRET_KEY`
- `ENVIRONMENT`

### ML Pipeline Requirements
- `MLFLOW_TRACKING_URI`
- `REDIS_HOST` and `REDIS_PORT`
- `MODEL_SERVING_ENABLED` (if using ML API)

### Production Additional Requirements
- `ALLOWED_ORIGINS` (for CORS)
- `DATABASE_SSL_MODE=require`
- Strong passwords and secrets (>32 characters)

## Security Best Practices

### Password Generation
```bash
# Generate secure API secret
openssl rand -base64 32

# Generate secure JWT secret  
head /dev/urandom | tr -dc A-Za-z0-9 | head -c 32
```

### Environment Validation
```bash
# Test database connection
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Test API startup
uv run -m src.ml.api.main
```

## Migration from Old Templates

If you were using the old template files:

1. **`.env.production.template`** → Use production settings from this guide
2. **`.env.staging.template`** → Use staging settings from this guide  
3. **`.env.ml_template`** → Use Docker/ML settings from this guide
4. **`.env.template`** → Now using comprehensive `.env.example`

## Troubleshooting

### Common Issues

**Database Connection Fails**:
- Check `POSTGRES_HOST` (localhost vs host.docker.internal)
- Verify `POSTGRES_PORT` (5432 vs 5433 for local Docker)
- Confirm PostgreSQL is running: `docker ps | grep postgres`

**Redis Connection Fails**:
- Check `REDIS_HOST` (localhost vs redis for containers)
- Verify Redis is accessible: `redis-cli ping`

**API Keys Invalid**:
- Regenerate using commands above
- Ensure minimum 32-character length
- No special characters that need escaping

### Validation Commands
```bash
# Test full configuration
uv run -m src.interfaces.cli data test --source action_network --real

# Check environment loading
uv run python -c "from src.core.config import get_settings; print(get_settings().model_dump())"
```

## Files Cleaned Up

This guide replaces the following redundant template files:
- `.env.production.template` (removed)
- `.env.staging.template` (removed)  
- `.env.ml_template` (removed)
- `.env.example` (updated to comprehensive template)

The single `.env.example` file now contains all configurations with clear environment-specific guidance.