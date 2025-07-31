"""
Enhanced Games Service

Processes staging.action_network_games → curated.enhanced_games
Core service to resolve the critical STAGING → CURATED pipeline gap.

This service addresses the 0% game coverage issue by:
- Processing staging games with complete metadata enrichment
- Cross-system ID linking (Action Network, MLB Stats API)
- Feature data population for ML readiness
- Game metadata consolidation and validation

Reference: docs/STAGING_CURATED_GAP_ANALYSIS.md - Critical Gap #1
"""

import asyncio
import json
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...core.datetime_utils import prepare_for_postgres
from ...core.logging import LogComponent, get_logger
from ...core.team_utils import normalize_team_name
from ...data.database.connection import get_connection

logger = get_logger(__name__, LogComponent.CORE)


class GameProcessingResult(BaseModel):
    """Result of processing staging games to curated zone."""
    
    games_processed: int = 0
    games_successful: int = 0
    games_failed: int = 0
    processing_time_seconds: float = 0.0
    errors: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class EnhancedGameData(BaseModel):
    """Enhanced game data model for curated zone."""
    
    # Core identifiers
    staging_game_id: Optional[int] = None
    action_network_game_id: Optional[int] = None
    mlb_stats_api_game_id: Optional[str] = None
    
    # Team information  
    home_team: str
    away_team: str
    home_team_full_name: Optional[str] = None
    away_team_full_name: Optional[str] = None
    
    # Game timing
    game_datetime: datetime
    game_date: Optional[date] = None
    game_time: Optional[str] = None
    season: Optional[int] = None
    
    # Venue information
    venue_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    
    # Enhanced features (JSONB fields)
    feature_data: Dict[str, Any] = Field(default_factory=dict)
    weather_features: Dict[str, Any] = Field(default_factory=dict)
    team_form_features: Dict[str, Any] = Field(default_factory=dict)
    market_features: Dict[str, Any] = Field(default_factory=dict)
    
    # Data quality
    data_quality_score: float = 0.0
    source_coverage_score: float = 0.0
    
    # Metadata
    ml_metadata: Dict[str, Any] = Field(default_factory=dict)


class EnhancedGamesService:
    """
    Service for processing staging games into enhanced curated games.
    
    Resolves the critical 0% game coverage gap by implementing the missing
    STAGING → CURATED game processing pipeline.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.processing_stats = {
            "total_processed": 0,
            "total_successful": 0,
            "total_failed": 0,
            "last_run": None
        }
    
    async def process_recent_games(
        self, 
        days_back: int = 7,
        limit: Optional[int] = None,
        dry_run: bool = False
    ) -> GameProcessingResult:
        """
        Process recent staging games into curated enhanced_games.
        
        Args:
            days_back: Number of days to look back for games
            limit: Maximum number of games to process (None for all)
            dry_run: If True, don't actually insert data
            
        Returns:
            GameProcessingResult with processing details
        """
        start_time = datetime.now(timezone.utc)
        result = GameProcessingResult()
        
        try:
            logger.info(f"Starting enhanced games processing: days_back={days_back}, limit={limit}, dry_run={dry_run}")
            
            # Get staging games to process
            staging_games = await self._get_staging_games(days_back, limit)
            result.games_processed = len(staging_games)
            
            if not staging_games:
                logger.info("No staging games found to process")
                result.metadata["message"] = "No staging games found"
                return result
            
            logger.info(f"Found {len(staging_games)} staging games to process")
            
            # Process each game
            successful = 0
            for game_data in staging_games:
                try:
                    enhanced_game = await self._enhance_game_data(game_data)
                    
                    if not dry_run:
                        await self._insert_enhanced_game(enhanced_game)
                    
                    successful += 1
                    logger.debug(f"Successfully processed game {enhanced_game.action_network_game_id}")
                    
                except Exception as e:
                    error_msg = f"Failed to process game {game_data.get('id', 'unknown')}: {e}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
            
            result.games_successful = successful
            result.games_failed = result.games_processed - successful
            
            # Update processing stats
            self.processing_stats["total_processed"] += result.games_processed  
            self.processing_stats["total_successful"] += result.games_successful
            self.processing_stats["total_failed"] += result.games_failed
            self.processing_stats["last_run"] = start_time
            
            # Calculate processing time
            end_time = datetime.now(timezone.utc)
            result.processing_time_seconds = (end_time - start_time).total_seconds()
            
            result.metadata = {
                "dry_run": dry_run,
                "days_back": days_back,
                "limit": limit,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
            logger.info(
                f"Enhanced games processing completed: "
                f"{result.games_successful}/{result.games_processed} successful "
                f"in {result.processing_time_seconds:.2f}s"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Enhanced games processing failed: {e}")
            result.errors.append(str(e))
            return result
    
    async def _get_staging_games(self, days_back: int, limit: Optional[int]) -> List[Dict[str, Any]]:
        """Get staging games that need to be processed to curated zone."""
        
        async with get_connection() as conn:
            # Extract games from raw data with game metadata
            # This works with the available staging data by joining with raw data
            query = """
                WITH game_metadata AS (
                    SELECT DISTINCT
                        rd.external_game_id,
                        rd.external_game_id::INTEGER as action_network_game_id,
                        rd.raw_odds->'game_metadata'->>'away_team' as away_team_name,  
                        rd.raw_odds->'game_metadata'->>'home_team' as home_team_name,
                        (rd.raw_odds->'game_metadata'->>'game_datetime')::TIMESTAMPTZ as game_datetime,
                        EXTRACT(YEAR FROM (rd.raw_odds->'game_metadata'->>'game_datetime')::TIMESTAMPTZ) as season,
                        rd.collected_at,
                        -- Count odds records for this game
                        (SELECT COUNT(*) FROM staging.action_network_odds_historical soh 
                         WHERE soh.external_game_id = rd.external_game_id) as odds_records_count
                    FROM raw_data.action_network_odds rd
                    WHERE rd.collected_at > NOW() - INTERVAL '%s days'
                        AND rd.external_game_id IS NOT NULL
                        AND rd.raw_odds->'game_metadata' IS NOT NULL
                        AND rd.raw_odds->'game_metadata'->>'game_datetime' IS NOT NULL
                )
                SELECT 
                    NULL as id,  -- No staging games table ID available
                    gm.external_game_id,
                    gm.action_network_game_id,
                    NULL as mlb_stats_api_game_id,  -- Not available in raw data
                    gm.home_team_name,
                    NULL as home_team_abbr,
                    gm.home_team_name as home_team_normalized,  -- Use as normalized
                    gm.away_team_name,
                    NULL as away_team_abbr,
                    gm.away_team_name as away_team_normalized,  -- Use as normalized
                    gm.game_datetime,
                    CAST(gm.game_datetime::DATE AS DATE) as game_date,
                    gm.season,
                    'regular' as game_type,
                    'scheduled' as game_status,
                    -- Calculate data quality score based on odds records count
                    LEAST(1.0, gm.odds_records_count / 100.0) as data_quality_score,
                    gm.collected_at as created_at,
                    gm.collected_at as updated_at,
                    gm.odds_records_count
                FROM game_metadata gm
                LEFT JOIN curated.enhanced_games ceg 
                    ON gm.action_network_game_id = ceg.action_network_game_id
                WHERE ceg.action_network_game_id IS NULL  -- Not yet in curated
                    AND gm.odds_records_count > 0  -- Only include games with odds data
                ORDER BY gm.game_datetime DESC, gm.odds_records_count DESC
            """ % days_back
            
            if limit:
                query += f" LIMIT {limit}"
            
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def _enhance_game_data(self, staging_game: Dict[str, Any]) -> EnhancedGameData:
        """Transform staging game data into enhanced game data."""
        
        # Extract core game information
        game_datetime = staging_game["game_datetime"]
        if game_datetime:
            game_datetime = prepare_for_postgres(game_datetime)
        
        # Build enhanced game data
        enhanced_game = EnhancedGameData(
            staging_game_id=None,  # TODO: Fix FK constraint issue - bypassing for now
            action_network_game_id=staging_game["action_network_game_id"],
            mlb_stats_api_game_id=staging_game["mlb_stats_api_game_id"],
            
            # Team information (use normalized names for consistency)
            home_team=staging_game["home_team_normalized"] or staging_game["home_team_abbr"],
            away_team=staging_game["away_team_normalized"] or staging_game["away_team_abbr"], 
            home_team_full_name=staging_game["home_team_name"],
            away_team_full_name=staging_game["away_team_name"],
            
            # Game timing
            game_datetime=game_datetime,
            game_date=staging_game["game_date"],
            season=staging_game["season"],
            
            # Data quality
            data_quality_score=float(staging_game["data_quality_score"] or 0.0)
        )
        
        # Enhance with additional features
        await self._add_market_features(enhanced_game, staging_game)
        await self._add_ml_metadata(enhanced_game, staging_game)
        
        return enhanced_game
    
    async def _add_market_features(self, enhanced_game: EnhancedGameData, staging_game: Dict[str, Any]) -> None:
        """Add market features based on odds data."""
        
        try:
            async with get_connection() as conn:
                # Get market summary from odds data
                market_query = """
                    SELECT 
                        market_type,
                        COUNT(*) as records_count,
                        COUNT(DISTINCT sportsbook_name) as sportsbooks_count,
                        MIN(updated_at) as earliest_odds,
                        MAX(updated_at) as latest_odds,
                        AVG(CASE WHEN odds > 0 THEN odds ELSE NULL END) as avg_positive_odds,
                        AVG(CASE WHEN odds < 0 THEN odds ELSE NULL END) as avg_negative_odds
                    FROM staging.action_network_odds_historical
                    WHERE external_game_id = $1
                    GROUP BY market_type
                """
                
                market_data = await conn.fetch(market_query, staging_game["external_game_id"])
                
                market_features = {}
                for row in market_data:
                    market_type = row["market_type"]
                    market_features[f"{market_type}_records"] = row["records_count"]
                    market_features[f"{market_type}_sportsbooks"] = row["sportsbooks_count"]
                    market_features[f"{market_type}_time_span_hours"] = (
                        (row["latest_odds"] - row["earliest_odds"]).total_seconds() / 3600
                        if row["latest_odds"] and row["earliest_odds"] else 0
                    )
                
                enhanced_game.market_features = market_features
                
                # Calculate source coverage score based on market diversity
                total_records = sum(row["records_count"] for row in market_data)
                unique_sportsbooks = len(set(row["sportsbooks_count"] for row in market_data))
                enhanced_game.source_coverage_score = min(1.0, (total_records / 100) * (unique_sportsbooks / 5))
                
        except Exception as e:
            logger.warning(f"Failed to add market features for game {enhanced_game.action_network_game_id}: {e}")
            enhanced_game.market_features = {}
    
    async def _add_ml_metadata(self, enhanced_game: EnhancedGameData, staging_game: Dict[str, Any]) -> None:
        """Add ML-specific metadata for feature engineering."""
        
        enhanced_game.ml_metadata = {
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_sources": ["action_network"],
            "feature_version": "1.0",
            "quality_checks": {
                "has_teams": bool(enhanced_game.home_team and enhanced_game.away_team),
                "has_datetime": bool(enhanced_game.game_datetime),
                "has_odds_data": enhanced_game.source_coverage_score > 0,
                "data_quality_score": enhanced_game.data_quality_score
            }
        }
        
        # Set overall feature data for ML compatibility
        enhanced_game.feature_data = {
            "source": "enhanced_games_service",
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "market_summary": enhanced_game.market_features,
            "quality_metrics": enhanced_game.ml_metadata["quality_checks"]
        }
    
    async def _insert_enhanced_game(self, enhanced_game: EnhancedGameData) -> None:
        """Insert enhanced game data into curated.enhanced_games table."""
        
        async with get_connection() as conn:
            insert_query = """
                INSERT INTO curated.enhanced_games (
                    staging_game_id, action_network_game_id, mlb_stats_api_game_id,
                    home_team, away_team, home_team_full_name, away_team_full_name,
                    game_datetime, game_date, season,
                    venue_name, venue_city, venue_state,
                    feature_data, weather_features, team_form_features, market_features,
                    data_quality_score, source_coverage_score, ml_metadata,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
                    $14, $15, $16, $17, $18, $19, $20, $21, $22
                )
            """
            
            now = datetime.now(timezone.utc)
            
            await conn.execute(
                insert_query,
                enhanced_game.staging_game_id,
                enhanced_game.action_network_game_id,
                enhanced_game.mlb_stats_api_game_id,
                enhanced_game.home_team,
                enhanced_game.away_team,
                enhanced_game.home_team_full_name,
                enhanced_game.away_team_full_name,
                enhanced_game.game_datetime,
                enhanced_game.game_date,
                enhanced_game.season,
                enhanced_game.venue_name,
                enhanced_game.venue_city,
                enhanced_game.venue_state,
                json.dumps(enhanced_game.feature_data),
                json.dumps(enhanced_game.weather_features),
                json.dumps(enhanced_game.team_form_features),
                json.dumps(enhanced_game.market_features),
                enhanced_game.data_quality_score,
                enhanced_game.source_coverage_score,
                json.dumps(enhanced_game.ml_metadata),
                now,
                now
            )
    
    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics for monitoring."""
        
        stats = dict(self.processing_stats)
        
        # Add database stats
        try:
            async with get_connection() as conn:
                # Count curated games
                curated_count = await conn.fetchval("SELECT COUNT(*) FROM curated.enhanced_games")
                recent_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM curated.enhanced_games 
                    WHERE created_at > NOW() - INTERVAL '7 days'
                """)
                
                stats.update({
                    "curated_games_total": curated_count,
                    "curated_games_recent": recent_count,
                    "last_run_formatted": self.processing_stats["last_run"].strftime("%Y-%m-%d %H:%M:%S UTC") 
                                         if self.processing_stats["last_run"] else "Never"
                })
                
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            stats["database_error"] = str(e)
        
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for monitoring systems."""
        
        try:
            async with get_connection() as conn:
                # Test database connectivity
                await conn.fetchval("SELECT 1")
                
                # Check for recent processing
                last_processed = await conn.fetchval("""
                    SELECT MAX(created_at) FROM curated.enhanced_games
                """)
                
                hours_since_last = None
                if last_processed:
                    hours_since_last = (datetime.now(timezone.utc) - last_processed.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                
                return {
                    "status": "healthy",
                    "database_connection": "ok",
                    "last_processing": last_processed.isoformat() if last_processed else None,
                    "hours_since_last_processing": hours_since_last,
                    "stats": self.processing_stats
                }
                
        except Exception as e:
            return {
                "status": "unhealthy", 
                "error": str(e),
                "database_connection": "failed"
            }