"""
Outcome Metrics Service

Integrates game outcome data with the data quality monitoring system
to provide comprehensive performance tracking and strategy validation.

Reference: CLAUDE.md - Add outcome metrics to data quality monitoring system
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.logging import get_logger, LogComponent
from ...data.database.connection import get_connection
from ...services.game_outcome_service import GameOutcomeService

logger = get_logger(__name__, LogComponent.MONITORING)


class OutcomeMetricType(str, Enum):
    """Types of outcome metrics tracked."""
    STRATEGY_PERFORMANCE = "strategy_performance"
    DATA_QUALITY_IMPACT = "data_quality_impact"
    PREDICTION_ACCURACY = "prediction_accuracy"
    BETTING_LINE_ACCURACY = "betting_line_accuracy"
    SHARP_ACTION_CORRELATION = "sharp_action_correlation"


class OutcomeMetric(BaseModel):
    """Individual outcome metric record."""
    metric_id: str = Field(..., description="Unique metric identifier")
    metric_type: OutcomeMetricType = Field(..., description="Type of metric")
    game_external_id: str = Field(..., description="External game ID")
    strategy_name: Optional[str] = Field(None, description="Associated strategy name")
    sportsbook_name: Optional[str] = Field(None, description="Associated sportsbook")
    bet_type: Optional[str] = Field(None, description="Type of bet (moneyline, spread, total)")
    
    # Metric values
    predicted_value: Optional[Decimal] = Field(None, description="Predicted outcome value")
    actual_value: Optional[Decimal] = Field(None, description="Actual outcome value")
    accuracy_score: Optional[float] = Field(None, description="Accuracy score (0.0-1.0)")
    confidence_level: Optional[float] = Field(None, description="Confidence level (0.0-1.0)")
    
    # Data quality correlation
    data_quality_score: Optional[float] = Field(None, description="Associated data quality score")
    sharp_action_detected: Optional[bool] = Field(None, description="Whether sharp action was detected")
    line_movement_magnitude: Optional[Decimal] = Field(None, description="Line movement magnitude")
    
    # Metadata
    game_date: datetime = Field(..., description="Game date")
    metric_calculated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    outcome_confirmed_at: Optional[datetime] = Field(None, description="When outcome was confirmed")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OutcomeMetricsAggregation(BaseModel):
    """Aggregated outcome metrics for reporting."""
    period_start: datetime
    period_end: datetime
    metric_type: OutcomeMetricType
    
    # Performance metrics
    total_predictions: int = 0
    correct_predictions: int = 0
    accuracy_rate: float = 0.0
    average_confidence: float = 0.0
    
    # Data quality correlation
    high_quality_accuracy: float = 0.0  # Accuracy when data quality > 0.8
    low_quality_accuracy: float = 0.0   # Accuracy when data quality < 0.5
    quality_correlation: float = 0.0    # Correlation between quality and accuracy
    
    # Sharp action correlation
    sharp_action_accuracy: float = 0.0  # Accuracy when sharp action detected
    no_sharp_accuracy: float = 0.0      # Accuracy when no sharp action
    sharp_correlation: float = 0.0      # Correlation between sharp action and outcomes


class OutcomeMetricsService:
    """
    Service for tracking and analyzing outcome metrics in relation to data quality.
    
    Integrates game outcomes with data quality scores to provide insights into
    the relationship between data quality and prediction accuracy.
    """
    
    def __init__(self):
        """Initialize the outcome metrics service."""
        self.settings = get_settings()
        self.outcome_service = GameOutcomeService()
        
    async def record_strategy_performance_metric(
        self,
        game_external_id: str,
        strategy_name: str,
        predicted_outcome: str,
        actual_outcome: str,
        confidence_level: float,
        bet_type: str = "moneyline"
    ) -> str:
        """
        Record a strategy performance metric.
        
        Args:
            game_external_id: External game identifier
            strategy_name: Name of the strategy that made the prediction
            predicted_outcome: Predicted outcome (win/loss, over/under, etc.)
            actual_outcome: Actual game outcome
            confidence_level: Strategy confidence level (0.0-1.0)
            bet_type: Type of bet (moneyline, spread, total)
            
        Returns:
            Metric ID of the recorded metric
        """
        try:
            # Calculate accuracy score
            accuracy_score = 1.0 if predicted_outcome == actual_outcome else 0.0
            
            # Get associated data quality score
            data_quality_score = await self._get_game_data_quality_score(game_external_id)
            
            # Check for sharp action
            sharp_action_detected = await self._check_sharp_action(game_external_id, bet_type)
            
            # Get line movement data
            line_movement = await self._get_line_movement_magnitude(game_external_id, bet_type)
            
            # Create metric record
            metric_id = f"strategy_{strategy_name}_{game_external_id}_{bet_type}_{int(datetime.now().timestamp())}"
            
            metric = OutcomeMetric(
                metric_id=metric_id,
                metric_type=OutcomeMetricType.STRATEGY_PERFORMANCE,
                game_external_id=game_external_id,
                strategy_name=strategy_name,
                bet_type=bet_type,
                predicted_value=1.0 if predicted_outcome == "win" else 0.0,
                actual_value=1.0 if actual_outcome == "win" else 0.0,
                accuracy_score=accuracy_score,
                confidence_level=confidence_level,
                data_quality_score=data_quality_score,
                sharp_action_detected=sharp_action_detected,
                line_movement_magnitude=line_movement,
                game_date=await self._get_game_date(game_external_id),
                outcome_confirmed_at=datetime.now(timezone.utc)
            )
            
            # Store the metric
            await self._store_outcome_metric(metric)
            
            logger.info(
                f"Recorded strategy performance metric",
                metric_id=metric_id,
                strategy=strategy_name,
                accuracy=accuracy_score,
                confidence=confidence_level
            )
            
            return metric_id
            
        except Exception as e:
            logger.error(f"Failed to record strategy performance metric: {e}")
            raise
    
    async def record_betting_line_accuracy_metric(
        self,
        game_external_id: str,
        sportsbook_name: str,
        bet_type: str,
        opening_line: Decimal,
        closing_line: Decimal,
        actual_margin: Decimal
    ) -> str:
        """
        Record a betting line accuracy metric.
        
        Args:
            game_external_id: External game identifier
            sportsbook_name: Name of the sportsbook
            bet_type: Type of bet (spread, total)
            opening_line: Opening betting line
            closing_line: Closing betting line
            actual_margin: Actual game margin/total
            
        Returns:
            Metric ID of the recorded metric
        """
        try:
            # Calculate accuracy based on which line was closer to actual
            opening_accuracy = 1.0 / (1.0 + abs(float(opening_line - actual_margin)))
            closing_accuracy = 1.0 / (1.0 + abs(float(closing_line - actual_margin)))
            
            # Use closing line accuracy as primary metric
            accuracy_score = closing_accuracy
            
            # Get data quality score
            data_quality_score = await self._get_game_data_quality_score(game_external_id)
            
            # Calculate line movement magnitude
            line_movement = abs(closing_line - opening_line)
            
            # Create metric record
            metric_id = f"line_{sportsbook_name}_{game_external_id}_{bet_type}_{int(datetime.now().timestamp())}"
            
            metric = OutcomeMetric(
                metric_id=metric_id,
                metric_type=OutcomeMetricType.BETTING_LINE_ACCURACY,
                game_external_id=game_external_id,
                sportsbook_name=sportsbook_name,
                bet_type=bet_type,
                predicted_value=closing_line,
                actual_value=actual_margin,
                accuracy_score=accuracy_score,
                confidence_level=0.8,  # Default confidence for line accuracy
                data_quality_score=data_quality_score,
                line_movement_magnitude=line_movement,
                game_date=await self._get_game_date(game_external_id),
                outcome_confirmed_at=datetime.now(timezone.utc)
            )
            
            # Store the metric
            await self._store_outcome_metric(metric)
            
            logger.info(
                f"Recorded betting line accuracy metric",
                metric_id=metric_id,
                sportsbook=sportsbook_name,
                accuracy=accuracy_score,
                line_movement=float(line_movement)
            )
            
            return metric_id
            
        except Exception as e:
            logger.error(f"Failed to record betting line accuracy metric: {e}")
            raise
    
    async def get_strategy_performance_summary(
        self,
        strategy_name: str,
        days_back: int = 30
    ) -> Dict[str, Any]:
        """
        Get performance summary for a specific strategy.
        
        Args:
            strategy_name: Name of the strategy
            days_back: Number of days to look back
            
        Returns:
            Performance summary dictionary
        """
        try:
            start_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            async with get_connection() as conn:
                # Get basic performance metrics
                performance_query = """
                    SELECT 
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN accuracy_score > 0.5 THEN 1 ELSE 0 END) as correct_predictions,
                        AVG(accuracy_score) as avg_accuracy,
                        AVG(confidence_level) as avg_confidence,
                        AVG(data_quality_score) as avg_data_quality
                    FROM monitoring.outcome_metrics
                    WHERE strategy_name = $1
                      AND metric_type = 'strategy_performance'
                      AND game_date >= $2
                """
                
                result = await conn.fetchrow(performance_query, strategy_name, start_date)
                
                # Get quality correlation metrics
                quality_query = """
                    SELECT 
                        AVG(CASE WHEN data_quality_score > 0.8 THEN accuracy_score END) as high_quality_accuracy,
                        AVG(CASE WHEN data_quality_score < 0.5 THEN accuracy_score END) as low_quality_accuracy,
                        AVG(CASE WHEN sharp_action_detected = true THEN accuracy_score END) as sharp_action_accuracy,
                        AVG(CASE WHEN sharp_action_detected = false THEN accuracy_score END) as no_sharp_accuracy
                    FROM monitoring.outcome_metrics
                    WHERE strategy_name = $1
                      AND metric_type = 'strategy_performance'
                      AND game_date >= $2
                """
                
                quality_result = await conn.fetchrow(quality_query, strategy_name, start_date)
                
                return {
                    "strategy_name": strategy_name,
                    "period_days": days_back,
                    "total_predictions": result["total_predictions"] or 0,
                    "correct_predictions": result["correct_predictions"] or 0,
                    "accuracy_rate": float(result["avg_accuracy"] or 0.0),
                    "average_confidence": float(result["avg_confidence"] or 0.0),
                    "average_data_quality": float(result["avg_data_quality"] or 0.0),
                    "high_quality_accuracy": float(quality_result["high_quality_accuracy"] or 0.0),
                    "low_quality_accuracy": float(quality_result["low_quality_accuracy"] or 0.0),
                    "sharp_action_accuracy": float(quality_result["sharp_action_accuracy"] or 0.0),
                    "no_sharp_accuracy": float(quality_result["no_sharp_accuracy"] or 0.0),
                    "quality_impact": (
                        float(quality_result["high_quality_accuracy"] or 0.0) -
                        float(quality_result["low_quality_accuracy"] or 0.0)
                    )
                }
                
        except Exception as e:
            logger.error(f"Failed to get strategy performance summary: {e}")
            raise
    
    async def get_data_quality_impact_analysis(
        self,
        days_back: int = 30
    ) -> Dict[str, Any]:
        """
        Analyze the impact of data quality on prediction accuracy.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Data quality impact analysis
        """
        try:
            start_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            async with get_connection() as conn:
                # Get quality vs accuracy correlation
                correlation_query = """
                    SELECT 
                        CASE 
                            WHEN data_quality_score >= 0.9 THEN 'excellent'
                            WHEN data_quality_score >= 0.8 THEN 'good'
                            WHEN data_quality_score >= 0.6 THEN 'fair'
                            ELSE 'poor'
                        END as quality_tier,
                        COUNT(*) as predictions,
                        AVG(accuracy_score) as avg_accuracy,
                        AVG(confidence_level) as avg_confidence
                    FROM monitoring.outcome_metrics
                    WHERE metric_type IN ('strategy_performance', 'betting_line_accuracy')
                      AND game_date >= $1
                      AND data_quality_score IS NOT NULL
                    GROUP BY quality_tier
                    ORDER BY 
                        CASE quality_tier
                            WHEN 'excellent' THEN 1
                            WHEN 'good' THEN 2
                            WHEN 'fair' THEN 3
                            WHEN 'poor' THEN 4
                        END
                """
                
                results = await conn.fetch(correlation_query, start_date)
                
                quality_tiers = {}
                for row in results:
                    quality_tiers[row["quality_tier"]] = {
                        "predictions": row["predictions"],
                        "avg_accuracy": float(row["avg_accuracy"]),
                        "avg_confidence": float(row["avg_confidence"])
                    }
                
                # Calculate overall correlation coefficient
                correlation_coeff = await self._calculate_quality_accuracy_correlation(start_date)
                
                return {
                    "analysis_period_days": days_back,
                    "quality_tiers": quality_tiers,
                    "correlation_coefficient": correlation_coeff,
                    "quality_impact_summary": {
                        "excellent_vs_poor_diff": (
                            quality_tiers.get("excellent", {}).get("avg_accuracy", 0) -
                            quality_tiers.get("poor", {}).get("avg_accuracy", 0)
                        ),
                        "good_vs_fair_diff": (
                            quality_tiers.get("good", {}).get("avg_accuracy", 0) -
                            quality_tiers.get("fair", {}).get("avg_accuracy", 0)
                        )
                    }
                }
                
        except Exception as e:
            logger.error(f"Failed to get data quality impact analysis: {e}")
            raise
    
    async def _store_outcome_metric(self, metric: OutcomeMetric) -> None:
        """Store an outcome metric to the database."""
        try:
            async with get_connection() as conn:
                insert_query = """
                    INSERT INTO monitoring.outcome_metrics (
                        metric_id, metric_type, game_external_id, strategy_name,
                        sportsbook_name, bet_type, predicted_value, actual_value,
                        accuracy_score, confidence_level, data_quality_score,
                        sharp_action_detected, line_movement_magnitude,
                        game_date, metric_calculated_at, outcome_confirmed_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                    ON CONFLICT (metric_id) DO UPDATE SET
                        actual_value = EXCLUDED.actual_value,
                        accuracy_score = EXCLUDED.accuracy_score,
                        outcome_confirmed_at = EXCLUDED.outcome_confirmed_at
                """
                
                await conn.execute(
                    insert_query,
                    metric.metric_id,
                    metric.metric_type.value,
                    metric.game_external_id,
                    metric.strategy_name,
                    metric.sportsbook_name,
                    metric.bet_type,
                    metric.predicted_value,
                    metric.actual_value,
                    metric.accuracy_score,
                    metric.confidence_level,
                    metric.data_quality_score,
                    metric.sharp_action_detected,
                    metric.line_movement_magnitude,
                    metric.game_date,
                    metric.metric_calculated_at,
                    metric.outcome_confirmed_at
                )
                
        except Exception as e:
            logger.error(f"Failed to store outcome metric: {e}")
            raise
    
    async def _get_game_data_quality_score(self, game_external_id: str) -> Optional[float]:
        """Get the data quality score for a game."""
        try:
            async with get_connection() as conn:
                query = """
                    SELECT AVG(data_completeness_score) as avg_quality
                    FROM data_quality.sportsbook_data_monitoring
                    WHERE game_external_id = $1
                """
                result = await conn.fetchrow(query, game_external_id)
                return float(result["avg_quality"]) if result and result["avg_quality"] else None
                
        except Exception as e:
            logger.error(f"Failed to get data quality score: {e}")
            return None
    
    async def _check_sharp_action(self, game_external_id: str, bet_type: str) -> Optional[bool]:
        """Check if sharp action was detected for a game and bet type."""
        try:
            async with get_connection() as conn:
                query = """
                    SELECT sharp_action_detected
                    FROM staging.betting_splits
                    WHERE game_external_id = $1 AND bet_type = $2
                    ORDER BY collected_at DESC
                    LIMIT 1
                """
                result = await conn.fetchrow(query, game_external_id, bet_type)
                return result["sharp_action_detected"] if result else None
                
        except Exception as e:
            logger.error(f"Failed to check sharp action: {e}")
            return None
    
    async def _get_line_movement_magnitude(self, game_external_id: str, bet_type: str) -> Optional[Decimal]:
        """Get the line movement magnitude for a game and bet type."""
        try:
            async with get_connection() as conn:
                query = """
                    SELECT 
                        MAX(line_value) - MIN(line_value) as movement_magnitude
                    FROM staging.betting_lines
                    WHERE game_external_id = $1 AND bet_type = $2
                """
                result = await conn.fetchrow(query, game_external_id, bet_type)
                return Decimal(str(result["movement_magnitude"])) if result and result["movement_magnitude"] else None
                
        except Exception as e:
            logger.error(f"Failed to get line movement magnitude: {e}")
            return None
    
    async def _get_game_date(self, game_external_id: str) -> datetime:
        """Get the game date for a game."""
        try:
            async with get_connection() as conn:
                query = """
                    SELECT game_date
                    FROM staging.games
                    WHERE external_game_id = $1
                    LIMIT 1
                """
                result = await conn.fetchrow(query, game_external_id)
                return result["game_date"] if result else datetime.now(timezone.utc)
                
        except Exception as e:
            logger.error(f"Failed to get game date: {e}")
            return datetime.now(timezone.utc)
    
    async def _calculate_quality_accuracy_correlation(self, start_date: datetime) -> float:
        """Calculate correlation coefficient between data quality and accuracy."""
        try:
            async with get_connection() as conn:
                query = """
                    SELECT 
                        data_quality_score,
                        accuracy_score
                    FROM monitoring.outcome_metrics
                    WHERE game_date >= $1
                      AND data_quality_score IS NOT NULL
                      AND accuracy_score IS NOT NULL
                """
                
                results = await conn.fetch(query, start_date)
                
                if len(results) < 2:
                    return 0.0
                
                # Calculate Pearson correlation coefficient
                quality_scores = [float(r["data_quality_score"]) for r in results]
                accuracy_scores = [float(r["accuracy_score"]) for r in results]
                
                n = len(quality_scores)
                sum_q = sum(quality_scores)
                sum_a = sum(accuracy_scores)
                sum_q2 = sum(q * q for q in quality_scores)
                sum_a2 = sum(a * a for a in accuracy_scores)
                sum_qa = sum(q * a for q, a in zip(quality_scores, accuracy_scores))
                
                numerator = n * sum_qa - sum_q * sum_a
                denominator = ((n * sum_q2 - sum_q * sum_q) * (n * sum_a2 - sum_a * sum_a)) ** 0.5
                
                return numerator / denominator if denominator != 0 else 0.0
                
        except Exception as e:
            logger.error(f"Failed to calculate correlation: {e}")
            return 0.0