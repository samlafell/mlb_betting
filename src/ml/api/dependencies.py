"""
FastAPI dependencies for MLB ML API
"""

import logging
import os
from typing import AsyncGenerator

import redis.asyncio as redis
from fastapi import HTTPException

# Fixed import structure - removed sys.path.append()
try:
    from ...core.config import get_database_config
except ImportError:
    # Fallback for environments where unified config is not available
    get_database_config = None

logger = logging.getLogger(__name__)

# Global clients
_redis_client: redis.Redis = None
_ml_service = None


async def get_redis_client() -> redis.Redis:
    """Get Redis client for feature caching"""
    global _redis_client

    if _redis_client is None:
        try:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            _redis_client = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
            )

            # Test connection
            await _redis_client.ping()
            logger.info("✅ Redis connection established")

        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise HTTPException(status_code=503, detail="Redis connection failed")

    return _redis_client


async def get_ml_service():
    """Get ML prediction service"""
    global _ml_service

    if _ml_service is None:
        try:
            # Import here to avoid circular imports
            from ..services.prediction_service import PredictionService

            _ml_service = PredictionService()
            await _ml_service.initialize()

            logger.info("✅ ML service initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize ML service: {e}")
            raise HTTPException(
                status_code=503, detail="ML service initialization failed"
            )

    return _ml_service


async def get_database_connection():
    """Get database connection for ML operations"""
    try:
        import asyncpg

        # Get database configuration from environment variables
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "mlb_betting")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")

        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        conn = await asyncpg.connect(dsn)
        try:
            yield conn
        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


# Startup/shutdown handlers
async def startup_event():
    """Initialize services on startup"""
    logger.info("🚀 Starting MLB ML API services...")

    # Initialize Redis
    await get_redis_client()

    # Initialize ML service
    await get_ml_service()

    logger.info("✅ All services initialized")


async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("🛑 Shutting down MLB ML API services...")

    global _redis_client, _ml_service

    # Close Redis connection
    if _redis_client:
        await _redis_client.close()
        _redis_client = None

    # Cleanup ML service
    if _ml_service:
        await _ml_service.cleanup()
        _ml_service = None

    logger.info("✅ All services shut down")
