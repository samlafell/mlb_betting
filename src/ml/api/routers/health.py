"""
Health check endpoints for MLB ML API
"""

import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
import redis.asyncio as redis

from ..dependencies import get_redis_client

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health_check(redis_client: redis.Redis = Depends(get_redis_client)) -> Dict[str, Any]:
    """
    Health check endpoint for the ML prediction service
    """
    health_status = {
        "service": "mlb-ml-prediction-api",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "0.1.0",
        "checks": {}
    }
    
    # Check Redis connection
    try:
        await redis_client.ping()
        health_status["checks"]["redis"] = {
            "status": "healthy",
            "message": "Redis connection successful"
        }
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        health_status["checks"]["redis"] = {
            "status": "unhealthy", 
            "message": f"Redis connection failed: {str(e)}"
        }
        health_status["status"] = "degraded"
    
    # Check database connection (basic)
    try:
        # TODO: Add database health check
        health_status["checks"]["database"] = {
            "status": "healthy",
            "message": "Database connection not implemented yet"
        }
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "message": f"Database connection failed: {str(e)}"
        }
        health_status["status"] = "unhealthy"
    
    # Check MLflow connection
    try:
        # TODO: Add MLflow health check
        health_status["checks"]["mlflow"] = {
            "status": "healthy", 
            "message": "MLflow connection not implemented yet"
        }
    except Exception as e:
        logger.error(f"MLflow health check failed: {e}")
        health_status["checks"]["mlflow"] = {
            "status": "unhealthy",
            "message": f"MLflow connection failed: {str(e)}"
        }
        health_status["status"] = "degraded"
    
    # Return appropriate HTTP status
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


@router.get("/health/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check - indicates if service is ready to handle requests
    """
    return {
        "service": "mlb-ml-prediction-api",
        "ready": True,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/health/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check - indicates if service is alive
    """
    return {
        "service": "mlb-ml-prediction-api", 
        "alive": True,
        "timestamp": datetime.utcnow().isoformat()
    }