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
    uv sync --frozen --no-dev && \
    echo "Dependencies installed successfully"

# Production stage
FROM python:3.11-slim as production

# Security: Create non-root user
RUN groupadd -r mlbuser && useradd -r -g mlbuser mlbuser

# Set working directory
WORKDIR /app

# Copy Python environment from builder including .venv
COPY --from=builder --chown=mlbuser:mlbuser /app /app

# Install runtime dependencies, UV, and ML libraries
RUN apt-get update && apt-get install -y \
    curl \
    libgomp1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && pip install --no-cache-dir uv

# Copy application code
COPY --chown=mlbuser:mlbuser src/ ./src/
COPY --chown=mlbuser:mlbuser config.toml ./

# Verify polars installation after copying source code
RUN uv run python -c "import polars as pl; print(f'Polars version: {pl.__version__}')" || echo "Polars import failed"

# Create necessary directories with proper permissions
RUN mkdir -p models logs /home/mlbuser/.cache/uv && \
    chown -R mlbuser:mlbuser models logs /home/mlbuser/.cache .venv && \
    chmod 755 models logs

# Set environment variables including UV virtual environment
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    USER=mlbuser \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Switch to non-root user
USER mlbuser

# Health check with improved robustness
HEALTHCHECK --interval=20s --timeout=15s --start-period=90s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command with security considerations and virtual environment activation
CMD ["uv", "run", "--frozen", "uvicorn", "src.ml.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]