#!/bin/bash
# Production startup script for MLB Betting FastAPI service
# Optimized for Docker deployment with health checks and graceful shutdown

set -e  # Exit on any error

echo "🚀 Starting MLB Betting FastAPI Production Service"
echo "================================="

# Environment validation
echo "📋 Environment Configuration:"
echo "  - Python Path: ${PYTHONPATH}"
echo "  - UV Environment: ${UV_PROJECT_ENVIRONMENT}"
echo "  - Log Level: ${LOG_LEVEL}"
echo "  - Max Workers: ${MAX_WORKERS:-4}"
echo "  - Environment: ${ENVIRONMENT:-production}"

# Health check function
health_check() {
    local max_attempts=30
    local attempt=1
    
    echo "🔍 Performing startup health check..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s http://localhost:8000/health >/dev/null 2>&1; then
            echo "✅ Health check passed (attempt $attempt/$max_attempts)"
            return 0
        fi
        
        echo "⏳ Health check failed, attempt $attempt/$max_attempts. Retrying in 2s..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "❌ Health check failed after $max_attempts attempts"
    return 1
}

# Pre-flight checks
echo "🔧 Pre-flight checks..."

# Check if virtual environment exists
if [ ! -d "/app/.venv" ]; then
    echo "❌ Virtual environment not found at /app/.venv"
    exit 1
fi

# Check if source code exists
if [ ! -d "/app/src" ]; then
    echo "❌ Source code not found at /app/src"
    exit 1
fi

# Check if config file exists
if [ ! -f "/app/config.toml" ]; then
    echo "❌ Configuration file not found at /app/config.toml"
    exit 1
fi

# Validate Python imports
echo "🐍 Validating Python dependencies..."
uv run --frozen python -c "
import sys
import os
sys.path.insert(0, '/app')

try:
    import fastapi
    import uvicorn
    import asyncpg
    import redis
    import polars
    print('✅ All critical dependencies available')
except ImportError as e:
    print(f'❌ Missing dependency: {e}')
    sys.exit(1)
"

# Database connection check (non-blocking)
echo "🗄️ Testing database connectivity..."
uv run --frozen python -c "
import asyncio
import asyncpg
import os
import sys

async def test_db():
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            print('⚠️  DATABASE_URL not set, skipping database test')
            return
        
        # Quick connection test with timeout
        conn = await asyncio.wait_for(
            asyncpg.connect(db_url), 
            timeout=10.0
        )
        await conn.close()
        print('✅ Database connection successful')
    except asyncio.TimeoutError:
        print('⚠️  Database connection timeout (will retry later)')
    except Exception as e:
        print(f'⚠️  Database connection failed: {e} (will retry later)')

try:
    asyncio.run(test_db())
except Exception as e:
    print(f'⚠️  Database test error: {e} (continuing anyway)')
" || echo "⚠️  Database connectivity test failed (service will retry)"

# Redis connection check (non-blocking)
echo "📦 Testing Redis connectivity..."
uv run --frozen python -c "
import redis
import os

try:
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    redis_password = os.getenv('REDIS_PASSWORD')
    
    r = redis.from_url(
        redis_url, 
        password=redis_password,
        socket_timeout=5,
        socket_connect_timeout=5
    )
    r.ping()
    print('✅ Redis connection successful')
except Exception as e:
    print(f'⚠️  Redis connection failed: {e} (will retry later)')
" || echo "⚠️  Redis connectivity test failed (service will retry)"

echo "✅ Pre-flight checks completed"

# Determine number of workers based on available resources
if [ -z "${MAX_WORKERS}" ]; then
    # Calculate workers: min(4, CPU cores + 1, memory/512MB)
    CPU_CORES=$(nproc 2>/dev/null || echo "1")
    MEMORY_MB=$(awk '/MemAvailable/{printf "%.0f", $2/1024}' /proc/meminfo 2>/dev/null || echo "512")
    
    WORKER_BY_CPU=$((CPU_CORES + 1))
    WORKER_BY_MEM=$((MEMORY_MB / 512))
    
    # Use minimum of calculated values, cap at 4 for container efficiency
    CALCULATED_WORKERS=$((WORKER_BY_CPU < WORKER_BY_MEM ? WORKER_BY_CPU : WORKER_BY_MEM))
    MAX_WORKERS=$((CALCULATED_WORKERS < 4 ? CALCULATED_WORKERS : 4))
    MAX_WORKERS=$((MAX_WORKERS > 1 ? MAX_WORKERS : 1))
    
    echo "🔧 Auto-calculated workers: ${MAX_WORKERS} (CPU: ${CPU_CORES}, Memory: ${MEMORY_MB}MB)"
fi

# Gunicorn configuration for production
GUNICORN_CMD="uv run --frozen gunicorn src.ml.api.main:app"
GUNICORN_ARGS=(
    --worker-class uvicorn.workers.UvicornWorker
    --workers "${MAX_WORKERS:-4}"
    --bind "0.0.0.0:8000"
    --timeout "${WORKER_TIMEOUT:-300}"
    --keepalive "${KEEPALIVE:-2}"
    --max-requests "${MAX_REQUESTS:-1000}"
    --max-requests-jitter "${MAX_REQUESTS_JITTER:-50}"
    --preload
    --access-logfile -
    --error-logfile -
    --log-level "${LOG_LEVEL,,}"
    --capture-output
    --enable-stdio-inheritance
)

# Add graceful timeout for production
GUNICORN_ARGS+=(--graceful-timeout 120)

# Enable worker recycling for memory management
GUNICORN_ARGS+=(--max-requests 1000 --max-requests-jitter 50)

echo "🚀 Starting Gunicorn with configuration:"
echo "  - Workers: ${MAX_WORKERS:-4}"
echo "  - Timeout: ${WORKER_TIMEOUT:-300}s"
echo "  - Keep-alive: ${KEEPALIVE:-2}s"
echo "  - Max requests: ${MAX_REQUESTS:-1000}"

# Trap signals for graceful shutdown
trap 'echo "🛑 Received shutdown signal, stopping gracefully..." && kill -TERM $GUNICORN_PID' TERM INT

# Start Gunicorn in background to allow signal handling
$GUNICORN_CMD "${GUNICORN_ARGS[@]}" &
GUNICORN_PID=$!

echo "✅ FastAPI service started with PID: $GUNICORN_PID"

# Wait for the process to complete
wait $GUNICORN_PID

echo "🏁 FastAPI service stopped"