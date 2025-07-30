"""
Prediction endpoints for MLB ML API
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from pydantic import BaseModel, Field

from ..dependencies import get_ml_service
from ..security import rate_limit_check, get_current_user
from ...services.prediction_service import PredictionService

logger = logging.getLogger(__name__)

router = APIRouter()


class PredictionRequest(BaseModel):
    """Request model for single game prediction"""
    game_id: str = Field(..., description="Game ID for prediction")
    model_name: Optional[str] = Field(None, description="Specific model to use")
    include_explanation: bool = Field(False, description="Include model explanation")


class BatchPredictionRequest(BaseModel):
    """Request model for batch predictions"""
    game_ids: List[str] = Field(..., max_items=50, description="List of game IDs (max 50)")
    model_name: Optional[str] = Field(None, description="Specific model to use")
    include_explanation: bool = Field(False, description="Include model explanations")


class PredictionResponse(BaseModel):
    """Response model for predictions"""
    game_id: str
    model_name: str
    model_version: str
    prediction_timestamp: datetime
    
    # Predictions
    total_over_probability: Optional[float] = None
    total_over_binary: Optional[int] = None
    total_over_confidence: Optional[float] = None
    
    home_ml_probability: Optional[float] = None
    home_ml_binary: Optional[int] = None
    home_ml_confidence: Optional[float] = None
    
    home_spread_probability: Optional[float] = None
    home_spread_binary: Optional[int] = None
    home_spread_confidence: Optional[float] = None
    
    # Betting recommendations
    betting_recommendations: Optional[Dict[str, Any]] = None
    
    # Model explanation
    explanation: Optional[Dict[str, Any]] = None
    
    # Metadata
    confidence_threshold_met: bool
    risk_level: Optional[str] = None


@router.post("/predict", response_model=PredictionResponse)
async def predict_game(
    request: PredictionRequest,
    ml_service: PredictionService = Depends(get_ml_service),
    _rate_limit: None = Depends(rate_limit_check),
    current_user: dict = Depends(get_current_user)
) -> PredictionResponse:
    """
    Get ML prediction for a single game
    """
    try:
        logger.info(f"Prediction request for game {request.game_id} by user {current_user.get('user_id', 'unknown')}")
        
        # Get prediction from ML service
        prediction = await ml_service.get_prediction(
            game_id=request.game_id,
            model_name=request.model_name,
            include_explanation=request.include_explanation
        )
        
        if not prediction:
            raise HTTPException(
                status_code=404,
                detail=f"No prediction available for game {request.game_id}"
            )
        
        return PredictionResponse(**prediction)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error for game {request.game_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate prediction"
        )


@router.post("/predict/batch", response_model=List[PredictionResponse])
async def predict_games_batch(
    request: BatchPredictionRequest,
    ml_service: PredictionService = Depends(get_ml_service),
    _rate_limit: None = Depends(rate_limit_check),
    current_user: dict = Depends(get_current_user)
) -> List[PredictionResponse]:
    """
    Get ML predictions for multiple games (max 50)
    """
    try:
        logger.info(f"Batch prediction request for {len(request.game_ids)} games by user {current_user.get('user_id', 'unknown')}")
        
        if len(request.game_ids) > 50:
            raise HTTPException(
                status_code=400,
                detail="Maximum 50 games allowed per batch request"
            )
        
        # Get batch predictions from ML service
        predictions = await ml_service.get_batch_predictions(
            game_ids=request.game_ids,
            model_name=request.model_name,
            include_explanation=request.include_explanation
        )
        
        return [PredictionResponse(**pred) for pred in predictions]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate batch predictions"
        )


@router.get("/predict/{game_id}", response_model=PredictionResponse)
async def get_cached_prediction(
    game_id: str,
    model_name: Optional[str] = Query(None, description="Specific model name"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> PredictionResponse:
    """
    Get cached prediction for a game (if available)
    """
    try:
        logger.info(f"Cached prediction request for game {game_id}")
        
        # Get cached prediction
        prediction = await ml_service.get_cached_prediction(
            game_id=game_id,
            model_name=model_name
        )
        
        if not prediction:
            raise HTTPException(
                status_code=404,
                detail=f"No cached prediction found for game {game_id}"
            )
        
        return PredictionResponse(**prediction)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cached prediction error for game {game_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve cached prediction"
        )


@router.get("/predictions/today", response_model=List[PredictionResponse])
async def get_todays_predictions(
    model_name: Optional[str] = Query(None, description="Filter by model name"),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0, description="Minimum confidence threshold"),
    ml_service: PredictionService = Depends(get_ml_service)
) -> List[PredictionResponse]:
    """
    Get all predictions for today's games
    """
    try:
        logger.info("Request for today's predictions")
        
        predictions = await ml_service.get_todays_predictions(
            model_name=model_name,
            min_confidence=min_confidence
        )
        
        return [PredictionResponse(**pred) for pred in predictions]
        
    except Exception as e:
        logger.error(f"Today's predictions error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve today's predictions"
        )