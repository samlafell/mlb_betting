"""
Feature Engineering Pipeline
Orchestrates all feature extractors and creates consolidated feature vectors
Uses Polars for high-performance data processing and Pydantic V2 for validation
"""

import logging
import hashlib
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal

import polars as pl
import asyncpg

from .models import FeatureVector, BaseFeatureExtractor
from ..database.connection_pool import get_db_transaction
from .temporal_features import TemporalFeatureExtractor
from .market_features import MarketFeatureExtractor
from .team_features import TeamFeatureExtractor
from .betting_splits_features import BettingSplitsFeatureExtractor

from ...core.config import get_settings

logger = logging.getLogger(__name__)


class FeaturePipeline:
    """
    High-performance feature engineering pipeline
    Coordinates all feature extractors and creates consolidated feature vectors
    """

    def __init__(self, feature_version: str = "v2.1"):
        self.feature_version = feature_version
        self.settings = get_settings()

        # Initialize feature extractors
        self.temporal_extractor = TemporalFeatureExtractor(feature_version)
        self.market_extractor = MarketFeatureExtractor(feature_version)
        self.team_extractor = TeamFeatureExtractor(feature_version)
        self.betting_splits_extractor = BettingSplitsFeatureExtractor(feature_version)

        # Performance tracking
        self.extraction_stats = {
            "total_games_processed": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "avg_processing_time_ms": 0.0,
        }

    async def extract_features_for_game(
        self,
        game_id: int,
        cutoff_time: datetime,
        include_derived: bool = True,
        include_interactions: bool = True,
    ) -> Optional[FeatureVector]:
        """
        Extract comprehensive features for a single game

        Args:
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time (must be >= 60min before game)
            include_derived: Whether to compute derived features
            include_interactions: Whether to compute feature interactions

        Returns:
            FeatureVector instance or None if extraction fails
        """
        start_time = datetime.utcnow()

        try:
            logger.info(f"Starting feature extraction for game {game_id}")

            # Load data from database
            data_sources = await self._load_game_data(game_id, cutoff_time)

            if not data_sources:
                logger.warning(f"No data available for game {game_id}")
                return None

            # Extract features from each source
            feature_components = {}
            data_quality_metrics = {"source_count": 0, "completeness_scores": []}

            # Extract temporal features
            if "temporal_data" in data_sources:
                logger.debug(f"Extracting temporal features for game {game_id}")
                temporal_features = await self.temporal_extractor.extract_features(
                    data_sources["temporal_data"], game_id, cutoff_time
                )
                feature_components["temporal_features"] = temporal_features
                data_quality_metrics["source_count"] += 1

            # Extract market features
            if "market_data" in data_sources:
                logger.debug(f"Extracting market features for game {game_id}")
                market_features = await self.market_extractor.extract_features(
                    data_sources["market_data"], game_id, cutoff_time
                )
                feature_components["market_features"] = market_features
                data_quality_metrics["source_count"] += 1

            # Extract team features
            if "team_data" in data_sources:
                logger.debug(f"Extracting team features for game {game_id}")
                team_features = await self.team_extractor.extract_features(
                    data_sources["team_data"], game_id, cutoff_time
                )
                feature_components["team_features"] = team_features
                data_quality_metrics["source_count"] += 1

            # Extract betting splits features
            if "betting_splits_data" in data_sources:
                logger.debug(f"Extracting betting splits features for game {game_id}")
                betting_splits_features = (
                    await self.betting_splits_extractor.extract_features(
                        data_sources["betting_splits_data"], game_id, cutoff_time
                    )
                )
                feature_components["betting_splits_features"] = betting_splits_features
                data_quality_metrics["source_count"] += 1

            # Compute derived features
            derived_features = {}
            if include_derived:
                derived_features = self._compute_derived_features(feature_components)

            # Compute interaction features
            interaction_features = {}
            if include_interactions:
                interaction_features = self._compute_interaction_features(
                    feature_components
                )

            # Calculate data quality metrics
            quality_metrics = self._calculate_quality_metrics(
                feature_components, data_quality_metrics
            )

            # Generate feature hash for caching
            feature_hash = self._generate_feature_hash(
                feature_components, derived_features, interaction_features
            )

            # Create consolidated feature vector
            feature_vector = FeatureVector(
                game_id=game_id,
                feature_cutoff_time=cutoff_time,
                feature_version=self.feature_version,
                feature_hash=feature_hash,
                **feature_components,
                derived_features=derived_features,
                interaction_features=interaction_features,
                **quality_metrics,
            )

            # Update statistics
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats(True, processing_time)

            logger.info(
                f"Successfully extracted features for game {game_id} in {processing_time:.1f}ms"
            )
            return feature_vector

        except Exception as e:
            processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_stats(False, processing_time)
            logger.error(f"Error extracting features for game {game_id}: {e}")
            return None

    async def extract_batch_features(
        self, game_ids: List[int], cutoff_time: datetime, max_concurrent: int = 5
    ) -> List[Tuple[int, Optional[FeatureVector]]]:
        """
        Extract features for multiple games concurrently

        Args:
            game_ids: List of game IDs for feature extraction
            cutoff_time: Feature cutoff time
            max_concurrent: Maximum concurrent extractions

        Returns:
            List of (game_id, feature_vector) tuples
        """
        import asyncio

        logger.info(f"Starting batch feature extraction for {len(game_ids)} games")

        # Process games in batches
        results = []
        semaphore = asyncio.Semaphore(max_concurrent)

        async def extract_single_game(
            game_id: int,
        ) -> Tuple[int, Optional[FeatureVector]]:
            async with semaphore:
                feature_vector = await self.extract_features_for_game(
                    game_id, cutoff_time
                )
                return game_id, feature_vector

        # Create tasks for all games
        tasks = [extract_single_game(game_id) for game_id in game_ids]

        # Execute tasks concurrently
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Batch extraction error: {result}")
                continue
            results.append(result)

        successful_extractions = len([r for r in results if r[1] is not None])
        logger.info(
            f"Batch extraction complete: {successful_extractions}/{len(game_ids)} successful"
        )

        return results

    async def save_feature_vector(
        self, feature_vector: FeatureVector, conn: Optional[asyncpg.Connection] = None
    ) -> bool:
        """
        Save feature vector to database with proper transaction management

        Args:
            feature_vector: FeatureVector to save
            conn: Database connection (optional, if None uses connection pool with transaction)

        Returns:
            True if successful, False otherwise
        """
        try:
            if conn is not None:
                # Use provided connection (assume caller manages transaction)
                return await self._save_feature_vector_with_conn(feature_vector, conn)
            else:
                # Use connection pool with automatic transaction management
                async with get_db_transaction() as transaction_conn:
                    return await self._save_feature_vector_with_conn(
                        feature_vector, transaction_conn
                    )
        except Exception as e:
            logger.error(f"Error saving feature vector: {e}")
            return False

    async def _save_feature_vector_with_conn(
        self, feature_vector: FeatureVector, conn: asyncpg.Connection
    ) -> bool:
        """
        Internal method to save feature vector with given connection
        """
        # Convert feature vector to database format
        db_data = self._feature_vector_to_db_format(feature_vector)

        # Insert into database
        query = """
            INSERT INTO curated.ml_feature_vectors (
                game_id, feature_cutoff_time, feature_version, feature_hash,
                temporal_features, market_features, team_features, betting_splits_features,
                derived_features, interaction_features,
                feature_completeness_score, data_source_coverage, missing_feature_count, total_feature_count,
                action_network_data, vsin_data, sbd_data, mlb_stats_api_data,
                normalization_applied, scaling_method, feature_selection_applied, dimensionality_reduction,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            ON CONFLICT (game_id, feature_version, feature_cutoff_time) 
            DO UPDATE SET
                feature_hash = EXCLUDED.feature_hash,
                temporal_features = EXCLUDED.temporal_features,
                market_features = EXCLUDED.market_features,
                team_features = EXCLUDED.team_features,
                betting_splits_features = EXCLUDED.betting_splits_features,
                derived_features = EXCLUDED.derived_features,
                interaction_features = EXCLUDED.interaction_features,
                feature_completeness_score = EXCLUDED.feature_completeness_score,
                data_source_coverage = EXCLUDED.data_source_coverage,
                missing_feature_count = EXCLUDED.missing_feature_count,
                total_feature_count = EXCLUDED.total_feature_count,
                action_network_data = EXCLUDED.action_network_data,
                vsin_data = EXCLUDED.vsin_data,
                sbd_data = EXCLUDED.sbd_data,
                mlb_stats_api_data = EXCLUDED.mlb_stats_api_data,
                normalization_applied = EXCLUDED.normalization_applied,
                scaling_method = EXCLUDED.scaling_method,
                feature_selection_applied = EXCLUDED.feature_selection_applied,
                dimensionality_reduction = EXCLUDED.dimensionality_reduction
        """

        await conn.execute(query, *db_data)
        logger.debug(f"Saved feature vector for game {feature_vector.game_id}")
        return True

    async def _load_game_data(
        self, game_id: int, cutoff_time: datetime
    ) -> Dict[str, pl.DataFrame]:
        """Load all necessary data for feature extraction"""
        try:
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.username,
                password=self.settings.database.password,
            )

            data_sources = {}

            # Load temporal data (line movements and sharp action)
            temporal_query = """
                SELECT DISTINCT
                    lm.game_id,
                    lm.timestamp,
                    eg.game_datetime as game_start_time,
                    lm.sportsbook_name,
                    'moneyline' as market_type,
                    lm.home_ml_odds,
                    lm.away_ml_odds,
                    lm.home_spread_line,
                    lm.home_spread_odds,
                    lm.total_line,
                    lm.over_odds,
                    lm.under_odds,
                    COALESCE(ba.sharp_action_direction, 'none') as sharp_action_direction,
                    ba.reverse_line_movement
                FROM staging.line_movements lm
                LEFT JOIN curated.enhanced_games eg ON lm.game_id = eg.id
                LEFT JOIN curated.betting_analysis ba ON lm.game_id = ba.game_id
                WHERE lm.game_id = $1 
                    AND lm.timestamp <= $2
                    AND EXTRACT(EPOCH FROM (eg.game_datetime - lm.timestamp)) / 60 >= 60
                ORDER BY lm.timestamp
            """

            temporal_rows = await conn.fetch(temporal_query, game_id, cutoff_time)
            if temporal_rows:
                data_sources["temporal_data"] = pl.DataFrame(
                    [dict(row) for row in temporal_rows]
                )

            # Load market data (cross-sportsbook odds)
            market_query = """
                SELECT DISTINCT
                    lm.game_id,
                    lm.timestamp,
                    lm.sportsbook_name,
                    'odds' as market_type,
                    lm.home_ml_odds,
                    lm.away_ml_odds,
                    lm.home_spread_line,
                    lm.home_spread_odds,
                    lm.away_spread_odds,
                    lm.total_line,
                    lm.over_odds,
                    lm.under_odds,
                    COALESCE(ba.sharp_action_direction, 'none') as sharp_action_direction,
                    COALESCE(ba.sharp_action_strength, 'weak') as sharp_action_strength
                FROM staging.line_movements lm
                LEFT JOIN curated.betting_analysis ba ON lm.game_id = ba.game_id
                WHERE lm.game_id = $1 
                    AND lm.timestamp <= $2
                ORDER BY lm.timestamp
            """

            market_rows = await conn.fetch(market_query, game_id, cutoff_time)
            if market_rows:
                data_sources["market_data"] = pl.DataFrame(
                    [dict(row) for row in market_rows]
                )

            # Load team data (enhanced games with team and venue info)
            team_query = """
                SELECT 
                    eg.id as game_id,
                    eg.home_team,
                    eg.away_team,
                    eg.game_datetime,
                    eg.season,
                    eg.venue_name,
                    eg.venue_city,
                    eg.venue_state,
                    eg.temperature_fahrenheit,
                    eg.wind_speed_mph,
                    eg.wind_direction,
                    eg.humidity_pct,
                    eg.weather_condition,
                    eg.home_pitcher_name,
                    eg.away_pitcher_name,
                    eg.home_pitcher_era,
                    eg.away_pitcher_era,
                    eg.home_pitcher_throws,
                    eg.away_pitcher_throws,
                    eg.home_score,
                    eg.away_score
                FROM curated.enhanced_games eg
                WHERE eg.id = $1
                
                UNION ALL
                
                SELECT 
                    eg2.id as game_id,
                    eg2.home_team,
                    eg2.away_team,
                    eg2.game_datetime,
                    eg2.season,
                    eg2.venue_name,
                    eg2.venue_city,
                    eg2.venue_state,
                    eg2.temperature_fahrenheit,
                    eg2.wind_speed_mph,
                    eg2.wind_direction,
                    eg2.humidity_pct,
                    eg2.weather_condition,
                    eg2.home_pitcher_name,
                    eg2.away_pitcher_name,
                    eg2.home_pitcher_era,
                    eg2.away_pitcher_era,
                    eg2.home_pitcher_throws,
                    eg2.away_pitcher_throws,
                    eg2.home_score,
                    eg2.away_score
                FROM curated.enhanced_games eg2
                WHERE eg2.game_datetime < (SELECT game_datetime FROM curated.enhanced_games WHERE id = $1)
                    AND eg2.game_datetime >= (SELECT game_datetime - INTERVAL '90 days' FROM curated.enhanced_games WHERE id = $1)
                    AND (eg2.home_team = (SELECT home_team FROM curated.enhanced_games WHERE id = $1)
                         OR eg2.away_team = (SELECT home_team FROM curated.enhanced_games WHERE id = $1)
                         OR eg2.home_team = (SELECT away_team FROM curated.enhanced_games WHERE id = $1)
                         OR eg2.away_team = (SELECT away_team FROM curated.enhanced_games WHERE id = $1))
                ORDER BY game_datetime
            """

            team_rows = await conn.fetch(team_query, game_id)
            if team_rows:
                data_sources["team_data"] = pl.DataFrame(
                    [dict(row) for row in team_rows]
                )

            # Load betting splits data
            splits_query = """
                SELECT 
                    ubs.game_id,
                    ubs.data_source,
                    ubs.sportsbook_name,
                    ubs.sportsbook_id,
                    ubs.market_type,
                    ubs.bet_percentage_home,
                    ubs.bet_percentage_away,
                    ubs.money_percentage_home,
                    ubs.money_percentage_away,
                    ubs.bet_percentage_over,
                    ubs.bet_percentage_under,
                    ubs.money_percentage_over,
                    ubs.money_percentage_under,
                    ubs.sharp_action_direction,
                    ubs.sharp_action_strength,
                    ubs.reverse_line_movement,
                    ubs.collected_at,
                    ubs.minutes_before_game
                FROM curated.unified_betting_splits ubs
                WHERE ubs.game_id = $1 
                    AND ubs.collected_at <= $2
                    AND ubs.minutes_before_game >= 60
                ORDER BY ubs.collected_at
            """

            splits_rows = await conn.fetch(splits_query, game_id, cutoff_time)
            if splits_rows:
                data_sources["betting_splits_data"] = pl.DataFrame(
                    [dict(row) for row in splits_rows]
                )

            await conn.close()

            logger.debug(f"Loaded {len(data_sources)} data sources for game {game_id}")
            return data_sources

        except Exception as e:
            logger.error(f"Error loading game data for {game_id}: {e}")
            return {}

    def _compute_derived_features(
        self, feature_components: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute derived features from base feature components"""
        derived = {}

        try:
            # Sharp action intensity score (combines temporal and betting splits)
            if (
                "temporal_features" in feature_components
                and "betting_splits_features" in feature_components
            ):
                temporal = feature_components["temporal_features"]
                splits = feature_components["betting_splits_features"]

                sharp_intensity = 0.0
                if temporal.sharp_action_intensity_60min:
                    sharp_intensity += (
                        float(temporal.sharp_action_intensity_60min) * 0.6
                    )
                if splits.weighted_sharp_action_score:
                    sharp_intensity += float(splits.weighted_sharp_action_score) * 0.4

                derived["combined_sharp_intensity"] = sharp_intensity

            # Market efficiency score (combines market features)
            if "market_features" in feature_components:
                market = feature_components["market_features"]

                efficiency_score = 0.0
                factors = 0

                if market.line_stability_score:
                    efficiency_score += float(market.line_stability_score)
                    factors += 1
                if market.odds_efficiency_score:
                    efficiency_score += float(market.odds_efficiency_score)
                    factors += 1
                if market.sportsbook_consensus_strength:
                    efficiency_score += float(market.sportsbook_consensus_strength)
                    factors += 1

                if factors > 0:
                    derived["market_efficiency_composite"] = efficiency_score / factors

            # Team strength differential
            if "team_features" in feature_components:
                team = feature_components["team_features"]

                if team.home_win_pct and team.away_win_pct:
                    derived["team_strength_differential"] = float(
                        team.home_win_pct
                    ) - float(team.away_win_pct)

                if team.home_recent_form_weighted and team.away_recent_form_weighted:
                    derived["recent_form_differential"] = float(
                        team.home_recent_form_weighted
                    ) - float(team.away_recent_form_weighted)

        except Exception as e:
            logger.error(f"Error computing derived features: {e}")

        return derived

    def _compute_interaction_features(
        self, feature_components: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute feature interactions and combinations"""
        interactions = {}

        try:
            # Sharp action vs public sentiment interaction
            if (
                "temporal_features" in feature_components
                and "betting_splits_features" in feature_components
            ):
                temporal = feature_components["temporal_features"]
                splits = feature_components["betting_splits_features"]

                if (
                    temporal.money_vs_bet_divergence_home
                    and splits.home_money_bet_divergence
                ):
                    interactions["sharp_public_interaction_home"] = float(
                        temporal.money_vs_bet_divergence_home
                    ) * float(splits.home_money_bet_divergence)

            # Market consensus vs team performance
            if (
                "market_features" in feature_components
                and "team_features" in feature_components
            ):
                market = feature_components["market_features"]
                team = feature_components["team_features"]

                if market.sportsbook_consensus_strength and team.home_win_pct:
                    interactions["consensus_team_strength"] = float(
                        market.sportsbook_consensus_strength
                    ) * float(team.home_win_pct)

            # Weather impact on totals vs market movement
            if (
                "team_features" in feature_components
                and "temporal_features" in feature_components
            ):
                team = feature_components["team_features"]
                temporal = feature_components["temporal_features"]

                if team.temperature_impact_total and temporal.opening_to_current_total:
                    interactions["weather_market_interaction"] = float(
                        team.temperature_impact_total
                    ) * abs(float(temporal.opening_to_current_total))

        except Exception as e:
            logger.error(f"Error computing interaction features: {e}")

        return interactions

    def _calculate_quality_metrics(
        self, feature_components: Dict[str, Any], data_quality_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate data quality and completeness metrics"""

        # Count total and missing features
        total_features = 0
        missing_features = 0

        for component_name, component in feature_components.items():
            if component:
                component_dict = (
                    component.model_dump() if hasattr(component, "model_dump") else {}
                )
                for key, value in component_dict.items():
                    total_features += 1
                    if value is None:
                        missing_features += 1

        # Calculate completeness score
        completeness_score = 1.0 - (missing_features / max(total_features, 1))

        # Determine data source coverage
        source_flags = {
            "action_network_data": "temporal_features" in feature_components
            or "market_features" in feature_components,
            "vsin_data": "betting_splits_features" in feature_components,
            "sbd_data": "betting_splits_features" in feature_components,
            "mlb_stats_api_data": "team_features" in feature_components,
        }

        data_source_coverage = sum(source_flags.values())

        return {
            "feature_completeness_score": Decimal(str(completeness_score)),
            "data_source_coverage": data_source_coverage,
            "missing_feature_count": missing_features,
            "total_feature_count": total_features,
            **source_flags,
        }

    def _generate_feature_hash(
        self,
        feature_components: Dict[str, Any],
        derived_features: Dict[str, Any],
        interaction_features: Dict[str, Any],
    ) -> str:
        """Generate SHA-256 hash for feature vector caching and deduplication"""

        # Create hashable representation
        hash_data = {
            "feature_version": self.feature_version,
            "components": {},
            "derived": derived_features,
            "interactions": interaction_features,
        }

        for name, component in feature_components.items():
            if component:
                # Convert to dict and handle Decimal serialization
                component_dict = (
                    component.model_dump() if hasattr(component, "model_dump") else {}
                )
                serializable_dict = {}
                for key, value in component_dict.items():
                    if isinstance(value, Decimal):
                        serializable_dict[key] = float(value)
                    elif isinstance(value, datetime):
                        serializable_dict[key] = value.isoformat()
                    else:
                        serializable_dict[key] = value
                hash_data["components"][name] = serializable_dict

        # Generate hash
        hash_string = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.sha256(hash_string.encode()).hexdigest()

    def _feature_vector_to_db_format(self, feature_vector: FeatureVector) -> Tuple:
        """Convert FeatureVector to database insertion format"""

        # Convert feature components to JSON
        temporal_json = (
            feature_vector.temporal_features.model_dump()
            if feature_vector.temporal_features
            else {}
        )
        market_json = (
            feature_vector.market_features.model_dump()
            if feature_vector.market_features
            else {}
        )
        team_json = (
            feature_vector.team_features.model_dump()
            if feature_vector.team_features
            else {}
        )
        splits_json = (
            feature_vector.betting_splits_features.model_dump()
            if feature_vector.betting_splits_features
            else {}
        )

        return (
            feature_vector.game_id,
            feature_vector.feature_cutoff_time,
            feature_vector.feature_version,
            feature_vector.feature_hash,
            json.dumps(temporal_json, default=str),
            json.dumps(market_json, default=str),
            json.dumps(team_json, default=str),
            json.dumps(splits_json, default=str),
            json.dumps(feature_vector.derived_features, default=str),
            json.dumps(feature_vector.interaction_features, default=str),
            float(feature_vector.feature_completeness_score),
            feature_vector.data_source_coverage,
            feature_vector.missing_feature_count,
            feature_vector.total_feature_count,
            feature_vector.action_network_data,
            feature_vector.vsin_data,
            feature_vector.sbd_data,
            feature_vector.mlb_stats_api_data,
            feature_vector.normalization_applied,
            feature_vector.scaling_method,
            feature_vector.feature_selection_applied,
            feature_vector.dimensionality_reduction,
            feature_vector.created_at,
        )

    def _update_stats(self, success: bool, processing_time_ms: float):
        """Update pipeline performance statistics"""
        self.extraction_stats["total_games_processed"] += 1

        if success:
            self.extraction_stats["successful_extractions"] += 1
        else:
            self.extraction_stats["failed_extractions"] += 1

        # Update rolling average processing time
        current_avg = self.extraction_stats["avg_processing_time_ms"]
        total_processed = self.extraction_stats["total_games_processed"]

        new_avg = (
            (current_avg * (total_processed - 1)) + processing_time_ms
        ) / total_processed
        self.extraction_stats["avg_processing_time_ms"] = new_avg

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline performance statistics"""
        stats = self.extraction_stats.copy()
        if stats["total_games_processed"] > 0:
            stats["success_rate"] = (
                stats["successful_extractions"] / stats["total_games_processed"]
            )
        else:
            stats["success_rate"] = 0.0

        return stats
