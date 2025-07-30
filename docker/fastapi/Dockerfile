# FastAPI MLB ML Prediction Service
# Optimized for Mac Mini M2

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY pyproject.toml uv.lock ./

# Install uv and dependencies
RUN pip install uv && \
    uv sync --frozen

# Copy application code
COPY src/ ./src/
COPY config.toml ./

# Create necessary directories
RUN mkdir -p models logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command (can be overridden in docker-compose)
CMD ["uv", "run", "uvicorn", "src.ml.api.main:app", "--host", "0.0.0.0", "--port", "8000"]