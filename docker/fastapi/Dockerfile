# FastAPI MLB ML Prediction Service
# Multi-stage build with security hardening

FROM python:3.11-slim as builder

# Create non-root user early
RUN groupadd -r mlbuser && useradd -r -g mlbuser mlbuser

# Set working directory
WORKDIR /app

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements
COPY --chown=mlbuser:mlbuser pyproject.toml uv.lock ./

# Install uv and dependencies
RUN pip install --no-cache-dir uv && \
    uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim as production

# Security: Create non-root user
RUN groupadd -r mlbuser && useradd -r -g mlbuser mlbuser

# Set working directory
WORKDIR /app

# Copy Python environment from builder
COPY --from=builder --chown=mlbuser:mlbuser /app /app

# Install only runtime dependencies  
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy application code
COPY --chown=mlbuser:mlbuser src/ ./src/
COPY --chown=mlbuser:mlbuser config.toml ./

# Create necessary directories with proper permissions
RUN mkdir -p models logs && \
    chown -R mlbuser:mlbuser models logs && \
    chmod 755 models logs

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    USER=mlbuser

# Switch to non-root user
USER mlbuser

# Health check with improved robustness
HEALTHCHECK --interval=20s --timeout=15s --start-period=90s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command with security considerations
CMD ["uv", "run", "uvicorn", "src.ml.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]