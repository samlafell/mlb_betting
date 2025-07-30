"""
MLB ML Prediction API
FastAPI application for real-time game outcome predictions
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import uvicorn

from .routers import predictions, models, health
from .dependencies import get_redis_client, get_ml_service
from .security import get_cors_origins, add_security_headers, get_security_config
from ..services.prediction_service import PredictionService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info("Starting MLB ML Prediction API...")
    
    # Initialize services
    app.state.redis_client = await get_redis_client()
    app.state.ml_service = PredictionService()
    
    logger.info("✅ MLB ML API startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down MLB ML Prediction API...")
    
    if hasattr(app.state, 'redis_client'):
        await app.state.redis_client.close()
    
    logger.info("✅ MLB ML API shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="MLB ML Prediction API",
    description="Real-time MLB game outcome predictions with betting recommendations",
    version="0.1.0",
    docs_url="/docs" if os.getenv("DEBUG", "false").lower() == "true" else None,
    redoc_url="/redoc" if os.getenv("DEBUG", "false").lower() == "true" else None,
    lifespan=lifespan
)

# Security configuration
security_config = get_security_config()

# Add security headers middleware
app.middleware("http")(add_security_headers)

# Add trusted host middleware for production
if security_config.environment == "production":
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["yourdomain.com", "*.yourdomain.com"]
    )

# Add CORS middleware with environment-appropriate origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
    expose_headers=["X-RateLimit-Limit", "X-RateLimit-Reset", "Retry-After"]
)


# Include routers
app.include_router(health.router, prefix="", tags=["health"])
app.include_router(predictions.router, prefix="/api/v1", tags=["predictions"])
app.include_router(models.router, prefix="/api/v1", tags=["models"])


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": str(request.state.timestamp) if hasattr(request.state, 'timestamp') else None
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "message": "An unexpected error occurred"
        }
    )


# Root endpoint
@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint with API information"""
    return {
        "name": "MLB ML Prediction API",
        "version": "0.1.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "predictions": "/api/v1/predict",
            "batch_predictions": "/api/v1/predict/batch",
            "model_status": "/api/v1/models/active"
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=os.getenv("RELOAD", "false").lower() == "true"
    )