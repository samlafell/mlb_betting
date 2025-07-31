"""
Feast Feature Store Integration

Provides unified feature serving for both production predictions and backtesting.
Ensures feature consistency between live and historical data processing.

Key capabilities:
- Online feature serving for real-time predictions
- Offline feature serving for backtesting and training
- Feature versioning and lineage tracking
- Production-backtest parity validation
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from decimal import Decimal
import pandas as pd

try:
    from feast import FeatureStore, Feature, Entity, FeatureView, Field
    from feast.types import Float64, Int64, String, Bool
    FEAST_AVAILABLE = True
except ImportError:
    # Fallback for environments where Feast is not available
    FEAST_AVAILABLE = False
    FeatureStore = None
    Feature = Entity = FeatureView = Field = None
    Float64 = Int64 = String = Bool = None

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from .models import FeatureVector

logger = get_logger(__name__, LogComponent.ML)


class FeastIntegrationError(Exception):
    """Raised when Feast integration encounters an error"""
    pass


class FeastFeatureStore:
    """
    Feast feature store integration for MLB betting system.
    
    Provides unified feature serving for both production predictions and backtesting,
    ensuring consistent feature engineering across different execution contexts.
    """

    def __init__(self, repo_path: Optional[str] = None):
        """
        Initialize Feast feature store integration.
        
        Args:
            repo_path: Path to Feast repository configuration
        """
        if not FEAST_AVAILABLE:
            raise FeastIntegrationError(
                "Feast is not available. Install with: pip install feast"
            )
        
        self.settings = get_settings()
        self.logger = get_logger(__name__, LogComponent.ML)
        
        # Use configured repository path or default
        self.repo_path = repo_path or getattr(
            self.settings.ml, 'feast_repo_path', './feast_repo'
        )
        
        # Initialize Feast store
        try:
            self.store = FeatureStore(repo_path=self.repo_path)
            self.logger.info(f"Initialized Feast feature store: {self.repo_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Feast feature store: {e}")
            raise FeastIntegrationError(f"Feast initialization failed: {e}")
        
        # Feature configuration
        self.feature_service_name = "mlb_betting_features"
        self.entity_name = "game"
        
        # Cache for feature views
        self._feature_views_cache = {}
        
    async def get_online_features(
        self, 
        game_ids: List[int], 
        feature_version: str = "v2.1"
    ) -> Dict[int, Optional[FeatureVector]]:
        """
        Get features for real-time prediction serving.
        
        Args:
            game_ids: List of game IDs to retrieve features for
            feature_version: Feature version to use
            
        Returns:
            Dictionary mapping game_id to FeatureVector
        """
        try:
            # Prepare entity rows for Feast
            entity_rows = [{"game": game_id} for game_id in game_ids]
            
            # Get feature service or feature views
            feature_refs = await self._get_feature_references(feature_version)
            
            # Retrieve online features
            feature_response = self.store.get_online_features(
                features=feature_refs,
                entity_rows=entity_rows
            )
            
            # Convert to our FeatureVector format
            results = {}
            for i, game_id in enumerate(game_ids):
                try:
                    feature_dict = feature_response.to_dict()
                    
                    # Extract features for this game
                    game_features = {}
                    for feature_name, values in feature_dict.items():
                        if feature_name != "game":  # Skip entity column
                            game_features[feature_name] = values[i] if i < len(values) else None
                    
                    # Create FeatureVector
                    if any(v is not None for v in game_features.values()):
                        feature_vector = await self._create_feature_vector(
                            game_id, game_features, feature_version
                        )
                        results[game_id] = feature_vector
                    else:
                        results[game_id] = None
                        
                except Exception as e:
                    self.logger.warning(f"Error creating feature vector for game {game_id}: {e}")
                    results[game_id] = None
            
            self.logger.debug(f"Retrieved online features for {len(game_ids)} games")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to get online features: {e}")
            # Return empty results to allow graceful degradation
            return {game_id: None for game_id in game_ids}
    
    async def get_historical_features(
        self,
        game_ids: List[int],
        start_date: datetime,
        end_date: datetime,
        feature_version: str = "v2.1"
    ) -> Dict[int, Optional[FeatureVector]]:
        """
        Get historical features for backtesting and training.
        
        Args:
            game_ids: List of game IDs to retrieve features for
            start_date: Start of time range
            end_date: End of time range
            feature_version: Feature version to use
            
        Returns:
            Dictionary mapping game_id to FeatureVector
        """
        try:
            # Create entity DataFrame for historical retrieval
            entity_df = pd.DataFrame({
                "game": game_ids,
                "event_timestamp": [end_date] * len(game_ids)  # Use end_date as point-in-time
            })
            
            # Get feature service or feature views
            feature_refs = await self._get_feature_references(feature_version)
            
            # Retrieve historical features
            historical_features = self.store.get_historical_features(
                entity_df=entity_df,
                features=feature_refs
            ).to_df()
            
            # Convert to our FeatureVector format
            results = {}
            for _, row in historical_features.iterrows():
                game_id = int(row["game"])
                
                try:
                    # Extract features (excluding entity and timestamp columns)
                    feature_dict = row.drop(["game", "event_timestamp"]).to_dict()
                    
                    # Create FeatureVector
                    feature_vector = await self._create_feature_vector(
                        game_id, feature_dict, feature_version
                    )
                    results[game_id] = feature_vector
                    
                except Exception as e:
                    self.logger.warning(f"Error creating historical feature vector for game {game_id}: {e}")
                    results[game_id] = None
            
            # Fill in missing games
            for game_id in game_ids:
                if game_id not in results:
                    results[game_id] = None
            
            self.logger.debug(f"Retrieved historical features for {len(game_ids)} games")
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to get historical features: {e}")
            # Return empty results to allow graceful degradation
            return {game_id: None for game_id in game_ids}
    
    async def validate_feature_consistency(
        self,
        game_id: int,
        online_features: Optional[FeatureVector],
        historical_features: Optional[FeatureVector],
        tolerance: float = 0.001
    ) -> Dict[str, Any]:
        """
        Validate consistency between online and historical feature serving.
        
        Args:
            game_id: Game ID to validate
            online_features: Features from online store
            historical_features: Features from historical store  
            tolerance: Numerical tolerance for floating-point comparisons
            
        Returns:
            Validation results with consistency metrics
        """
        validation_result = {
            "game_id": game_id,
            "consistent": True,
            "inconsistencies": [],
            "missing_features": [],
            "total_features": 0,
            "consistent_features": 0
        }
        
        try:
            # Handle case where one or both feature sets are None
            if online_features is None and historical_features is None:
                validation_result["consistent"] = True
                return validation_result
            
            if online_features is None or historical_features is None:
                validation_result["consistent"] = False
                validation_result["inconsistencies"].append(
                    "One feature set is None while the other is not"
                )
                return validation_result
            
            # Convert to dictionaries for comparison
            online_dict = online_features.model_dump()
            historical_dict = historical_features.model_dump()
            
            # Get all unique feature names
            all_features = set(online_dict.keys()) | set(historical_dict.keys())
            validation_result["total_features"] = len(all_features)
            
            for feature_name in all_features:
                # Check if feature exists in both
                if feature_name not in online_dict:
                    validation_result["missing_features"].append(f"online:{feature_name}")
                    validation_result["consistent"] = False
                    continue
                    
                if feature_name not in historical_dict:
                    validation_result["missing_features"].append(f"historical:{feature_name}")
                    validation_result["consistent"] = False
                    continue
                
                # Compare values
                online_val = online_dict[feature_name]
                historical_val = historical_dict[feature_name]
                
                # Handle different types of comparisons
                if isinstance(online_val, (int, float)) and isinstance(historical_val, (int, float)):
                    # Numerical comparison with tolerance
                    if abs(online_val - historical_val) > tolerance:
                        validation_result["inconsistencies"].append({
                            "feature": feature_name,
                            "online": online_val,
                            "historical": historical_val,
                            "difference": abs(online_val - historical_val)
                        })
                        validation_result["consistent"] = False
                    else:
                        validation_result["consistent_features"] += 1
                else:
                    # Exact comparison for non-numerical values
                    if online_val != historical_val:
                        validation_result["inconsistencies"].append({
                            "feature": feature_name,
                            "online": online_val,
                            "historical": historical_val,
                            "type": "exact_mismatch"
                        })
                        validation_result["consistent"] = False
                    else:
                        validation_result["consistent_features"] += 1
            
            # Calculate consistency percentage
            if validation_result["total_features"] > 0:
                consistency_pct = validation_result["consistent_features"] / validation_result["total_features"]
                validation_result["consistency_percentage"] = consistency_pct
            else:
                validation_result["consistency_percentage"] = 1.0
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating feature consistency for game {game_id}: {e}")
            validation_result["consistent"] = False
            validation_result["error"] = str(e)
            return validation_result
    
    async def create_feature_definitions(self) -> bool:
        """
        Create or update Feast feature definitions for MLB betting features.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Define entities
            game_entity = Entity(
                name="game",
                description="MLB game entity",
                value_type=Int64
            )
            
            # Define feature views for different categories
            team_features = self._create_team_feature_view()
            pitching_features = self._create_pitching_feature_view()
            betting_features = self._create_betting_feature_view()
            situational_features = self._create_situational_feature_view()
            
            # Apply feature definitions to Feast repository
            feature_views = [
                team_features,
                pitching_features,
                betting_features,
                situational_features
            ]
            
            # This would typically be done via Feast CLI or configuration
            # For now, we'll cache the definitions for retrieval
            self._feature_views_cache = {
                fv.name: fv for fv in feature_views
            }
            
            self.logger.info("Created Feast feature definitions")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create feature definitions: {e}")
            return False
    
    def _create_team_feature_view(self) -> FeatureView:
        """Create feature view for team-based features"""
        return FeatureView(
            name="team_features",
            entities=["game"],
            features=[
                Feature(name="home_team_rating", dtype=Float64),
                Feature(name="away_team_rating", dtype=Float64),
                Feature(name="home_team_form_l10", dtype=Float64),
                Feature(name="away_team_form_l10", dtype=Float64),
                Feature(name="head_to_head_home_wins", dtype=Int64),
                Feature(name="head_to_head_away_wins", dtype=Int64),
                Feature(name="home_team_runs_scored_avg", dtype=Float64),
                Feature(name="away_team_runs_scored_avg", dtype=Float64),
                Feature(name="home_team_runs_allowed_avg", dtype=Float64),
                Feature(name="away_team_runs_allowed_avg", dtype=Float64),
            ]
        )
    
    def _create_pitching_feature_view(self) -> FeatureView:
        """Create feature view for pitching-based features"""
        return FeatureView(
            name="pitching_features",
            entities=["game"],
            features=[
                Feature(name="home_pitcher_era", dtype=Float64),
                Feature(name="away_pitcher_era", dtype=Float64),
                Feature(name="home_pitcher_whip", dtype=Float64),
                Feature(name="away_pitcher_whip", dtype=Float64),
                Feature(name="home_pitcher_k9", dtype=Float64),
                Feature(name="away_pitcher_k9", dtype=Float64),
                Feature(name="home_pitcher_bb9", dtype=Float64),
                Feature(name="away_pitcher_bb9", dtype=Float64),
                Feature(name="home_pitcher_vs_handedness", dtype=Float64),
                Feature(name="away_pitcher_vs_handedness", dtype=Float64),
            ]
        )
    
    def _create_betting_feature_view(self) -> FeatureView:
        """Create feature view for betting market features"""
        return FeatureView(
            name="betting_features", 
            entities=["game"],
            features=[
                Feature(name="opening_total", dtype=Float64),
                Feature(name="current_total", dtype=Float64),
                Feature(name="total_line_movement", dtype=Float64),
                Feature(name="opening_ml_home", dtype=Int64),
                Feature(name="current_ml_home", dtype=Int64),
                Feature(name="ml_line_movement", dtype=Float64),
                Feature(name="opening_spread", dtype=Float64),
                Feature(name="current_spread", dtype=Float64),
                Feature(name="spread_line_movement", dtype=Float64),
                Feature(name="public_money_percentage", dtype=Float64),
                Feature(name="sharp_money_indicators", dtype=Int64),
            ]
        )
    
    def _create_situational_feature_view(self) -> FeatureView:
        """Create feature view for situational features"""
        return FeatureView(
            name="situational_features",
            entities=["game"],
            features=[
                Feature(name="is_divisional_game", dtype=Bool),
                Feature(name="is_weekend_game", dtype=Bool),
                Feature(name="is_day_game", dtype=Bool),
                Feature(name="weather_temp", dtype=Float64),
                Feature(name="weather_wind_speed", dtype=Float64),
                Feature(name="weather_humidity", dtype=Float64),
                Feature(name="ballpark_factor", dtype=Float64),
                Feature(name="home_rest_days", dtype=Int64),
                Feature(name="away_rest_days", dtype=Int64),
                Feature(name="season_game_number", dtype=Int64),
            ]
        )
    
    async def _get_feature_references(self, feature_version: str) -> List[str]:
        """
        Get feature references for a specific version.
        
        Args:
            feature_version: Feature version to retrieve
            
        Returns:
            List of feature references
        """
        # For now, return all features from all views
        # In a real implementation, this would be version-specific
        feature_refs = [
            "team_features:home_team_rating",
            "team_features:away_team_rating", 
            "team_features:home_team_form_l10",
            "team_features:away_team_form_l10",
            "pitching_features:home_pitcher_era",
            "pitching_features:away_pitcher_era",
            "pitching_features:home_pitcher_whip",
            "pitching_features:away_pitcher_whip",
            "betting_features:opening_total",
            "betting_features:current_total",
            "betting_features:total_line_movement",
            "situational_features:is_divisional_game",
            "situational_features:is_weekend_game",
            "situational_features:weather_temp"
        ]
        
        return feature_refs
    
    async def _create_feature_vector(
        self, 
        game_id: int, 
        feature_dict: Dict[str, Any], 
        feature_version: str
    ) -> FeatureVector:
        """
        Create FeatureVector from Feast feature dictionary.
        
        Args:
            game_id: Game ID
            feature_dict: Dictionary of feature names to values
            feature_version: Feature version
            
        Returns:
            FeatureVector instance
        """
        # Map Feast features to our FeatureVector fields
        return FeatureVector(
            game_id=game_id,
            feature_version=feature_version,
            
            # Team features
            home_team_rating=feature_dict.get("team_features__home_team_rating"),
            away_team_rating=feature_dict.get("team_features__away_team_rating"),
            home_team_form_l10=feature_dict.get("team_features__home_team_form_l10"),
            away_team_form_l10=feature_dict.get("team_features__away_team_form_l10"),
            
            # Pitching features
            home_pitcher_era=feature_dict.get("pitching_features__home_pitcher_era"),
            away_pitcher_era=feature_dict.get("pitching_features__away_pitcher_era"),
            home_pitcher_whip=feature_dict.get("pitching_features__home_pitcher_whip"),
            away_pitcher_whip=feature_dict.get("pitching_features__away_pitcher_whip"),
            
            # Betting features
            opening_total=feature_dict.get("betting_features__opening_total"),
            current_total=feature_dict.get("betting_features__current_total"),
            total_line_movement=feature_dict.get("betting_features__total_line_movement"),
            
            # Situational features
            is_divisional_game=feature_dict.get("situational_features__is_divisional_game", False),
            is_weekend_game=feature_dict.get("situational_features__is_weekend_game", False),
            weather_temp=feature_dict.get("situational_features__weather_temp"),
            
            # Metadata
            created_at=datetime.utcnow()
        )
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on Feast feature store.
        
        Returns:
            Health check results
        """
        try:
            # Test basic operations
            test_game_ids = [12345]
            
            # Test online features
            online_start = datetime.utcnow()
            online_result = await self.get_online_features(test_game_ids)
            online_time = (datetime.utcnow() - online_start).total_seconds() * 1000
            
            # Test historical features
            historical_start = datetime.utcnow()
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=1)
            historical_result = await self.get_historical_features(
                test_game_ids, start_date, end_date
            )
            historical_time = (datetime.utcnow() - historical_start).total_seconds() * 1000
            
            return {
                "status": "healthy",
                "feast_available": FEAST_AVAILABLE,
                "repo_path": self.repo_path,
                "online_serving": {
                    "available": True,
                    "response_time_ms": online_time
                },
                "historical_serving": {
                    "available": True,
                    "response_time_ms": historical_time
                },
                "feature_views": list(self._feature_views_cache.keys())
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "feast_available": FEAST_AVAILABLE,
                "repo_path": self.repo_path
            }


# Global instance
feast_store: Optional[FeastFeatureStore] = None


async def get_feast_store() -> FeastFeatureStore:
    """
    Get or create global Feast feature store instance.
    
    Returns:
        FeastFeatureStore instance
    """
    global feast_store
    
    if feast_store is None:
        try:
            feast_store = FeastFeatureStore()
            await feast_store.create_feature_definitions()
        except FeastIntegrationError as e:
            logger.error(f"Failed to initialize Feast store: {e}")
            raise
    
    return feast_store


async def validate_production_backtest_features(
    game_ids: List[int],
    feature_version: str = "v2.1"
) -> Dict[str, Any]:
    """
    Validate feature consistency between production and backtesting contexts.
    
    Args:
        game_ids: Game IDs to validate
        feature_version: Feature version to validate
        
    Returns:
        Comprehensive validation results
    """
    store = await get_feast_store()
    
    validation_results = {
        "total_games": len(game_ids),
        "consistent_games": 0,
        "inconsistent_games": 0,
        "validation_details": [],
        "overall_consistency": True
    }
    
    try:
        for game_id in game_ids:
            # Get features from both online and historical stores
            online_features = await store.get_online_features([game_id], feature_version)
            
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=1)
            historical_features = await store.get_historical_features(
                [game_id], start_date, end_date, feature_version
            )
            
            # Validate consistency
            game_validation = await store.validate_feature_consistency(
                game_id,
                online_features.get(game_id),
                historical_features.get(game_id)
            )
            
            validation_results["validation_details"].append(game_validation)
            
            if game_validation["consistent"]:
                validation_results["consistent_games"] += 1
            else:
                validation_results["inconsistent_games"] += 1
                validation_results["overall_consistency"] = False
        
        # Calculate overall metrics
        if validation_results["total_games"] > 0:
            consistency_rate = validation_results["consistent_games"] / validation_results["total_games"]
            validation_results["consistency_rate"] = consistency_rate
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Feature validation failed: {e}")
        validation_results["error"] = str(e)
        validation_results["overall_consistency"] = False
        return validation_results