"""
Model management endpoints for MLB ML API
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from ..dependencies import get_ml_service
from ...services.prediction_service import PredictionService

logger = logging.getLogger(__name__)

router = APIRouter()


class ModelInfo(BaseModel):
    """Model information response"""
    model_name: str
    model_version: str
    model_type: str
    is_active: bool
    created_at: datetime
    
    # Performance metrics
    recent_accuracy: Optional[float] = None
    recent_roi: Optional[float] = None
    total_predictions: Optional[int] = None
    
    # Model metadata
    description: Optional[str] = None
    target_variable: Optional[str] = None
    feature_version: Optional[str] = None


class ModelPerformanceResponse(BaseModel):
    """Model performance metrics response"""
    model_name: str
    model_version: str
    prediction_type: str
    evaluation_period_start: date
    evaluation_period_end: date
    
    # Classification metrics
    accuracy: Optional[float] = None
    precision_score: Optional[float] = None
    recall_score: Optional[float] = None
    f1_score: Optional[float] = None
    roc_auc: Optional[float] = None
    
    # Betting performance
    total_bets_made: int
    winning_bets: int
    hit_rate: Optional[float] = None
    roi_percentage: Optional[float] = None
    
    # Risk metrics
    sharpe_ratio: Optional[float] = None
    max_drawdown_pct: Optional[float] = None


@router.get("/models/active", response_model=List[ModelInfo])
async def get_active_models(
    ml_service: PredictionService = Depends(get_ml_service)
) -> List[ModelInfo]:
    """
    Get list of currently active models
    """
    try:
        logger.info("Request for active models")
        
        models = await ml_service.get_active_models()
        
        return [ModelInfo(**model) for model in models]
        
    except Exception as e:
        logger.error(f"Active models error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve active models"
        )


@router.get("/models/{model_name}", response_model=ModelInfo)
async def get_model_info(
    model_name: str,
    model_version: Optional[str] = Query(None, description="Specific model version"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> ModelInfo:
    """
    Get detailed information about a specific model
    """
    try:
        logger.info(f"Request for model info: {model_name}")
        
        model_info = await ml_service.get_model_info(
            model_name=model_name,
            model_version=model_version
        )
        
        if not model_info:
            raise HTTPException(
                status_code=404,
                detail=f"Model {model_name} not found"
            )
        
        return ModelInfo(**model_info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Model info error for {model_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve model information"
        )


@router.get("/models/{model_name}/performance", response_model=List[ModelPerformanceResponse])
async def get_model_performance(
    model_name: str,
    model_version: Optional[str] = Query(None, description="Specific model version"),
    prediction_type: Optional[str] = Query(None, description="Filter by prediction type"),
    days: Optional[int] = Query(30, ge=1, le=365, description="Performance period in days"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> List[ModelPerformanceResponse]:
    """
    Get performance metrics for a model
    """
    try:
        logger.info(f"Request for model performance: {model_name}")
        
        performance_data = await ml_service.get_model_performance(
            model_name=model_name,
            model_version=model_version,
            prediction_type=prediction_type,
            days=days
        )
        
        return [ModelPerformanceResponse(**perf) for perf in performance_data]
        
    except Exception as e:
        logger.error(f"Model performance error for {model_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve model performance"
        )


@router.get("/models/leaderboard", response_model=List[Dict[str, Any]])
async def get_model_leaderboard(
    metric: str = Query("roi_percentage", description="Metric to rank by"),
    prediction_type: Optional[str] = Query(None, description="Filter by prediction type"),
    days: Optional[int] = Query(30, ge=1, le=365, description="Performance period in days"),
    limit: Optional[int] = Query(10, ge=1, le=50, description="Number of models to return"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> List[Dict[str, Any]]:
    """
    Get model leaderboard ranked by specified metric
    """
    try:
        logger.info(f"Request for model leaderboard by {metric}")
        
        leaderboard = await ml_service.get_model_leaderboard(
            metric=metric,
            prediction_type=prediction_type,
            days=days,
            limit=limit
        )
        
        return leaderboard
        
    except Exception as e:
        logger.error(f"Model leaderboard error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve model leaderboard"
        )


@router.get("/models/{model_name}/recent-predictions")
async def get_model_recent_predictions(
    model_name: str,
    model_version: Optional[str] = Query(None, description="Specific model version"),
    days: Optional[int] = Query(7, ge=1, le=30, description="Number of days to look back"),
    limit: Optional[int] = Query(50, ge=1, le=200, description="Maximum predictions to return"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> List[Dict[str, Any]]:
    """
    Get recent predictions made by a specific model
    """
    try:
        logger.info(f"Request for recent predictions: {model_name}")
        
        predictions = await ml_service.get_model_recent_predictions(
            model_name=model_name,
            model_version=model_version,
            days=days,
            limit=limit
        )
        
        return predictions
        
    except Exception as e:
        logger.error(f"Recent predictions error for {model_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve recent predictions"
        )