"""
Feature Extractor

Extracts features from curated zone data for ML model training.
Integrates with the RAW → STAGING → CURATED data pipeline.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import pandas as pd
import numpy as np
from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_connection

logger = get_logger(__name__, LogComponent.CORE)


class FeatureExtractionConfig(BaseModel):
    """Configuration for feature extraction."""
    
    lookback_days: int = Field(default=30, description="Days of historical data for features")
    min_games_for_features: int = Field(default=10, description="Minimum games required for team features")
    feature_version: str = Field(default="v1.0", description="Feature version for tracking")
    include_temporal_features: bool = Field(default=True, description="Include time-based features")
    include_sharp_action_features: bool = Field(default=True, description="Include sharp action indicators")
    include_market_features: bool = Field(default=True, description="Include market consensus features")


class GameFeatures(BaseModel):
    """Structured game features for ML training."""
    
    game_id: str
    game_date: datetime
    home_team: str
    away_team: str
    
    # Target variables (what we're predicting)
    total_over_target: Optional[float] = None
    home_ml_target: Optional[float] = None  
    home_spread_target: Optional[float] = None
    
    # Temporal features
    days_since_last_game_home: Optional[float] = None
    days_since_last_game_away: Optional[float] = None
    game_time_hour: Optional[int] = None
    is_weekend: Optional[bool] = None
    
    # Sharp action features
    sharp_action_total: Optional[float] = None
    sharp_action_spread: Optional[float] = None
    sharp_action_moneyline: Optional[float] = None
    reverse_line_movement_count: Optional[int] = None
    
    # Market features  
    consensus_total_percentage: Optional[float] = None
    consensus_spread_percentage: Optional[float] = None
    line_movement_total: Optional[float] = None
    line_movement_spread: Optional[float] = None
    
    # Team performance features
    home_team_wins_l10: Optional[int] = None
    away_team_wins_l10: Optional[int] = None
    home_team_runs_avg_l10: Optional[float] = None
    away_team_runs_avg_l10: Optional[float] = None
    
    # Feature metadata
    feature_version: str = "v1.0"
    extraction_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FeatureExtractor:
    """
    Main feature extraction class for MLB betting predictions.
    
    Extracts features from curated zone data for machine learning model training.
    Handles temporal features, sharp action indicators, and market consensus data.
    """
    
    def __init__(self, config: Optional[FeatureExtractionConfig] = None):
        self.config = config or FeatureExtractionConfig()
        self.settings = get_settings()
        logger.info(f"FeatureExtractor initialized with version {self.config.feature_version}")
    
    async def extract_features_for_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        prediction_targets: List[str] = None
    ) -> List[GameFeatures]:
        """
        Extract features for all games in a date range.
        
        Args:
            start_date: Start date for feature extraction
            end_date: End date for feature extraction
            prediction_targets: List of targets to include (total_over, home_ml, home_spread)
            
        Returns:
            List of GameFeatures with extracted features
        """
        try:
            logger.info(f"Extracting features for date range: {start_date.date()} to {end_date.date()}")
            
            # Default to all prediction targets
            if prediction_targets is None:
                prediction_targets = ["total_over", "home_ml", "home_spread"]
            
            # Get games from curated zone
            games_data = await self._get_games_from_curated(start_date, end_date)
            
            if not games_data:
                logger.warning(f"No games found for date range {start_date.date()} to {end_date.date()}")
                return []
            
            features_list = []
            
            for game_data in games_data:
                try:
                    game_features = await self._extract_single_game_features(
                        game_data, prediction_targets
                    )
                    features_list.append(game_features)
                    
                except Exception as e:
                    logger.error(f"Failed to extract features for game {game_data.get('game_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully extracted features for {len(features_list)} games")
            return features_list
            
        except Exception as e:
            logger.error(f"Failed to extract features for date range: {e}")
            raise
    
    async def extract_single_game_features(
        self, 
        game_id: str, 
        prediction_targets: List[str] = None
    ) -> GameFeatures:
        """
        Extract features for a single game.
        
        Args:
            game_id: ID of the game to extract features for
            prediction_targets: List of targets to include
            
        Returns:
            GameFeatures with extracted features
        """
        try:
            # Get game data from curated zone
            game_data = await self._get_single_game_from_curated(game_id)
            
            if not game_data:
                raise ValueError(f"Game {game_id} not found in curated zone")
            
            # Default to all prediction targets
            if prediction_targets is None:
                prediction_targets = ["total_over", "home_ml", "home_spread"]
            
            game_features = await self._extract_single_game_features(game_data, prediction_targets)
            
            logger.info(f"Successfully extracted features for game {game_id}")
            return game_features
            
        except Exception as e:
            logger.error(f"Failed to extract features for game {game_id}: {e}")
            raise
    
    async def _get_games_from_curated(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get games data from curated zone."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    g.game_id,
                    g.game_date,
                    g.home_team,
                    g.away_team,
                    g.home_score,
                    g.away_score,
                    g.total_score,
                    g.game_status
                FROM curated.games g
                WHERE g.game_date >= $1 
                    AND g.game_date <= $2
                    AND g.game_status IN ('final', 'completed')
                ORDER BY g.game_date DESC
            """
            
            rows = await conn.fetch(query, start_date, end_date)
            return [dict(row) for row in rows]
    
    async def _get_single_game_from_curated(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Get single game data from curated zone."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    g.game_id,
                    g.game_date,
                    g.home_team,
                    g.away_team,
                    g.home_score,
                    g.away_score,
                    g.total_score,
                    g.game_status
                FROM curated.games g
                WHERE g.game_id = $1
            """
            
            row = await conn.fetchrow(query, game_id)
            return dict(row) if row else None
    
    async def _extract_single_game_features(
        self, 
        game_data: Dict[str, Any], 
        prediction_targets: List[str]
    ) -> GameFeatures:
        """Extract features for a single game."""
        game_id = game_data["game_id"]
        game_date = game_data["game_date"]
        home_team = game_data["home_team"]
        away_team = game_data["away_team"]
        
        # Initialize features
        features = GameFeatures(
            game_id=game_id,
            game_date=game_date,
            home_team=home_team,
            away_team=away_team,
            feature_version=self.config.feature_version
        )
        
        # Extract target variables if game is completed
        if game_data.get("game_status") in ["final", "completed"]:
            features = await self._extract_target_variables(features, game_data, prediction_targets)
        
        # Extract temporal features
        if self.config.include_temporal_features:
            features = await self._extract_temporal_features(features, game_data)
        
        # Extract sharp action features
        if self.config.include_sharp_action_features:
            features = await self._extract_sharp_action_features(features, game_data)
        
        # Extract market features
        if self.config.include_market_features:
            features = await self._extract_market_features(features, game_data)
        
        # Extract team performance features
        features = await self._extract_team_performance_features(features, game_data)
        
        return features
    
    async def _extract_target_variables(
        self, 
        features: GameFeatures, 
        game_data: Dict[str, Any], 
        prediction_targets: List[str]
    ) -> GameFeatures:
        """Extract target variables from completed games."""
        home_score = game_data.get("home_score", 0) or 0
        away_score = game_data.get("away_score", 0) or 0
        total_score = game_data.get("total_score") or (home_score + away_score)
        
        # Get betting lines from curated zone
        betting_lines = await self._get_game_betting_lines(game_data["game_id"])
        
        if "total_over" in prediction_targets and betting_lines.get("total_line"):
            total_line = betting_lines["total_line"]
            features.total_over_target = 1.0 if total_score > total_line else 0.0
        
        if "home_ml" in prediction_targets:
            features.home_ml_target = 1.0 if home_score > away_score else 0.0
        
        if "home_spread" in prediction_targets and betting_lines.get("spread_line"):
            spread_line = betting_lines["spread_line"]  # Positive means home is favored
            home_spread_result = home_score - away_score + spread_line
            features.home_spread_target = 1.0 if home_spread_result > 0 else 0.0
        
        return features
    
    async def _extract_temporal_features(
        self, 
        features: GameFeatures, 
        game_data: Dict[str, Any]
    ) -> GameFeatures:
        """Extract time-based features."""
        game_date = game_data["game_date"]
        
        # Game time features
        features.game_time_hour = game_date.hour
        features.is_weekend = game_date.weekday() >= 5  # Saturday = 5, Sunday = 6
        
        # Days since last game for each team
        features.days_since_last_game_home = await self._get_days_since_last_game(
            game_data["home_team"], game_date
        )
        features.days_since_last_game_away = await self._get_days_since_last_game(
            game_data["away_team"], game_date
        )
        
        return features
    
    async def _extract_sharp_action_features(
        self, 
        features: GameFeatures, 
        game_data: Dict[str, Any]
    ) -> GameFeatures:
        """Extract sharp action indicators."""
        game_id = game_data["game_id"]
        
        # Get sharp action data from curated zone
        sharp_action_data = await self._get_sharp_action_data(game_id)
        
        if sharp_action_data:
            features.sharp_action_total = sharp_action_data.get("sharp_action_total", 0.0)
            features.sharp_action_spread = sharp_action_data.get("sharp_action_spread", 0.0)
            features.sharp_action_moneyline = sharp_action_data.get("sharp_action_moneyline", 0.0)
            features.reverse_line_movement_count = sharp_action_data.get("rlm_count", 0)
        
        return features
    
    async def _extract_market_features(
        self, 
        features: GameFeatures, 
        game_data: Dict[str, Any]
    ) -> GameFeatures:
        """Extract market consensus and line movement features."""
        game_id = game_data["game_id"]
        
        # Get market data from curated zone
        market_data = await self._get_market_data(game_id)
        
        if market_data:
            features.consensus_total_percentage = market_data.get("consensus_total_pct")
            features.consensus_spread_percentage = market_data.get("consensus_spread_pct")
            features.line_movement_total = market_data.get("total_line_movement")
            features.line_movement_spread = market_data.get("spread_line_movement")
        
        return features
    
    async def _extract_team_performance_features(
        self, 
        features: GameFeatures, 
        game_data: Dict[str, Any]
    ) -> GameFeatures:
        """Extract team performance features."""
        game_date = game_data["game_date"]
        home_team = game_data["home_team"]
        away_team = game_data["away_team"]
        
        # Get last 10 games performance for each team
        home_performance = await self._get_team_recent_performance(home_team, game_date, 10)
        away_performance = await self._get_team_recent_performance(away_team, game_date, 10)
        
        features.home_team_wins_l10 = home_performance.get("wins", 0)
        features.away_team_wins_l10 = away_performance.get("wins", 0)
        features.home_team_runs_avg_l10 = home_performance.get("runs_avg", 0.0)
        features.away_team_runs_avg_l10 = away_performance.get("runs_avg", 0.0)
        
        return features
    
    async def _get_game_betting_lines(self, game_id: str) -> Dict[str, Any]:
        """Get betting lines for a game from curated zone."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    total_line,
                    spread_line,
                    home_ml_odds,
                    away_ml_odds
                FROM curated.betting_lines bl
                WHERE bl.game_id = $1
                ORDER BY bl.updated_at DESC
                LIMIT 1
            """
            
            row = await conn.fetchrow(query, game_id)
            return dict(row) if row else {}
    
    async def _get_days_since_last_game(self, team: str, current_date: datetime) -> Optional[float]:
        """Get days since team's last game."""
        async with get_connection() as conn:
            query = """
                SELECT game_date
                FROM curated.games
                WHERE (home_team = $1 OR away_team = $1)
                    AND game_date < $2
                    AND game_status IN ('final', 'completed')
                ORDER BY game_date DESC
                LIMIT 1
            """
            
            row = await conn.fetchrow(query, team, current_date)
            if row:
                last_game_date = row["game_date"]
                return (current_date - last_game_date).days
            return None
    
    async def _get_sharp_action_data(self, game_id: str) -> Dict[str, Any]:
        """Get sharp action data from curated zone."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    sharp_action_total,
                    sharp_action_spread,
                    sharp_action_moneyline,
                    rlm_count
                FROM curated.betting_analysis ba
                WHERE ba.game_id = $1
                ORDER BY ba.updated_at DESC
                LIMIT 1
            """
            
            row = await conn.fetchrow(query, game_id)
            return dict(row) if row else {}
    
    async def _get_market_data(self, game_id: str) -> Dict[str, Any]:
        """Get market consensus data from curated zone."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    consensus_total_percentage,
                    consensus_spread_percentage,
                    total_line_movement,
                    spread_line_movement
                FROM curated.market_consensus mc
                WHERE mc.game_id = $1
                ORDER BY mc.updated_at DESC
                LIMIT 1
            """
            
            row = await conn.fetchrow(query, game_id)
            return dict(row) if row else {}
    
    async def _get_team_recent_performance(
        self, 
        team: str, 
        current_date: datetime, 
        num_games: int
    ) -> Dict[str, Any]:
        """Get team's recent performance statistics."""
        async with get_connection() as conn:
            query = """
                SELECT 
                    g.game_id,
                    g.home_team,
                    g.away_team,
                    g.home_score,
                    g.away_score
                FROM curated.games g
                WHERE (g.home_team = $1 OR g.away_team = $1)
                    AND g.game_date < $2
                    AND g.game_status IN ('final', 'completed')
                ORDER BY g.game_date DESC
                LIMIT $3
            """
            
            rows = await conn.fetch(query, team, current_date, num_games)
            
            if not rows:
                return {"wins": 0, "runs_avg": 0.0}
            
            wins = 0
            total_runs = 0
            
            for row in rows:
                is_home = row["home_team"] == team
                team_score = row["home_score"] if is_home else row["away_score"]
                opponent_score = row["away_score"] if is_home else row["home_score"]
                
                if team_score > opponent_score:
                    wins += 1
                
                total_runs += team_score or 0
            
            return {
                "wins": wins,
                "runs_avg": total_runs / len(rows) if rows else 0.0
            }
    
    def to_dataframe(self, features_list: List[GameFeatures]) -> pd.DataFrame:
        """Convert list of GameFeatures to pandas DataFrame for ML training."""
        if not features_list:
            return pd.DataFrame()
        
        # Convert to list of dictionaries
        data = []
        for features in features_list:
            feature_dict = features.model_dump()
            # Convert datetime to timestamp
            feature_dict["game_date"] = feature_dict["game_date"].timestamp()
            feature_dict["extraction_timestamp"] = feature_dict["extraction_timestamp"].timestamp()
            data.append(feature_dict)
        
        df = pd.DataFrame(data)
        
        # Sort by game_date for consistency
        df = df.sort_values("game_date").reset_index(drop=True)
        
        logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        return df


# Utility functions for feature extraction
async def extract_features_for_training(
    start_date: datetime,
    end_date: datetime,
    prediction_targets: List[str] = None,
    config: Optional[FeatureExtractionConfig] = None
) -> pd.DataFrame:
    """
    Convenience function to extract features for ML training.
    
    Args:
        start_date: Start date for feature extraction
        end_date: End date for feature extraction  
        prediction_targets: List of targets to include
        config: Feature extraction configuration
        
    Returns:
        DataFrame ready for ML training
    """
    extractor = FeatureExtractor(config)
    features_list = await extractor.extract_features_for_date_range(
        start_date, end_date, prediction_targets
    )
    return extractor.to_dataframe(features_list)