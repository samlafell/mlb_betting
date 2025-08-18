"""
Games Population Service

Service for populating missing data in curated.games_complete table from multiple sources.
Addresses Issue #70 - Fix games_complete Table Population.

This service provides automated and manual population of critical fields like game scores,
external IDs, venue information, and weather data from available source tables.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from pydantic import BaseModel, Field

from src.core.config import get_settings
from src.core.exceptions import DatabaseError, UnifiedBettingError
from src.core.logging import UnifiedLogger, LogComponent


class PopulationStats(BaseModel):
    """Statistics for games population operation"""
    
    total_games: int = Field(description="Total games in table")
    games_updated: int = Field(description="Number of games updated")
    scores_populated: int = Field(description="Games with scores populated")
    external_ids_populated: int = Field(description="Games with external IDs populated")
    venue_populated: int = Field(description="Games with venue data populated")
    weather_populated: int = Field(description="Games with weather data populated")
    high_quality_games: int = Field(description="Games marked as high quality")
    operation_duration_seconds: float = Field(description="Operation duration in seconds")


class GamesPopulationService:
    """
    Service for populating missing data in curated.games_complete table.
    
    Handles population from multiple data sources:
    - curated.game_outcomes: Game scores and results
    - raw_data.action_network_games: External IDs, venue, and weather data
    
    Provides both automated pipeline integration and manual population capabilities.
    """
    
    def __init__(self):
        self.config = get_settings()
        self.logger = UnifiedLogger("games_population", LogComponent.DATABASE)
        self.connection_pool: Optional[asyncpg.Pool] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()
    
    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.connection_pool = await asyncpg.create_pool(
                host=self.config.database.host,
                port=self.config.database.port,
                user=self.config.database.user,
                password=self.config.database.password,
                database=self.config.database.database,
                min_size=2,
                max_size=10,
                command_timeout=300  # 5 minutes for long-running operations
            )
            self.logger.info("Initialized games population service", 
                           host=self.config.database.host,
                           port=self.config.database.port)
        except Exception as e:
            self.logger.error("Failed to initialize games population service", error=str(e))
            raise UnifiedBettingError(f"Failed to initialize database connection: {e}", 
                                              component="games_population", operation="initialize")
    
    async def cleanup(self):
        """Cleanup database connections"""
        if self.connection_pool:
            await self.connection_pool.close()
            self.logger.info("Closed database connection pool")
    
    async def populate_all_missing_data(self, 
                                      dry_run: bool = False,
                                      max_games: Optional[int] = None) -> PopulationStats:
        """
        Populate all missing data in games_complete table from available sources.
        
        Args:
            dry_run: If True, only analyze what would be updated without making changes
            max_games: Limit number of games to process (for testing)
            
        Returns:
            PopulationStats with operation results
        """
        start_time = datetime.now()
        
        self.logger.info("Starting complete games population", 
                        dry_run=dry_run, max_games=max_games)
        
        try:
            # Get initial state
            initial_stats = await self._get_current_stats()
            
            if dry_run:
                # In dry run, analyze what would be updated
                stats = await self._analyze_population_potential(max_games)
                self.logger.info("Dry run analysis completed", stats=stats.dict())
                return stats
            
            # Execute actual population
            async with self.connection_pool.acquire() as conn:
                async with conn.transaction():
                    # Phase 1: Populate game scores
                    scores_updated = await self._populate_game_scores(conn, max_games)
                    
                    # Phase 2: Populate Action Network IDs
                    ids_updated = await self._populate_action_network_ids(conn, max_games)
                    
                    # Phase 3: Populate venue data
                    venue_updated = await self._populate_venue_data(conn, max_games)
                    
                    # Phase 4: Populate weather data
                    weather_updated = await self._populate_weather_data(conn, max_games)
                    
                    # Phase 5: Update data quality indicators
                    quality_updated = await self._update_data_quality(conn, max_games)
            
            # Get final stats
            final_stats = await self._get_current_stats()
            
            # Calculate operation stats
            operation_duration = (datetime.now() - start_time).total_seconds()
            
            stats = PopulationStats(
                total_games=final_stats['total_games'],
                games_updated=max(scores_updated, ids_updated, venue_updated, weather_updated),
                scores_populated=scores_updated,
                external_ids_populated=ids_updated,
                venue_populated=venue_updated,
                weather_populated=weather_updated,
                high_quality_games=final_stats['high_quality_games'],
                operation_duration_seconds=operation_duration
            )
            
            self.logger.info("Games population completed successfully", 
                           stats=stats.dict())
            
            return stats
            
        except Exception as e:
            self.logger.error("Games population failed", error=str(e))
            raise UnifiedBettingError(f"Games population failed: {e}", 
                                              component="games_population", operation="populate_all")
    
    async def populate_game_scores_only(self, max_games: Optional[int] = None) -> int:
        """
        Populate only game scores from curated.game_outcomes.
        
        Args:
            max_games: Limit number of games to process
            
        Returns:
            Number of games updated
        """
        self.logger.info("Starting game scores population", max_games=max_games)
        
        try:
            async with self.connection_pool.acquire() as conn:
                return await self._populate_game_scores(conn, max_games)
        except Exception as e:
            self.logger.error("Game scores population failed", error=str(e))
            raise UnifiedBettingError(f"Game scores population failed: {e}", 
                                              component="games_population", operation="populate_scores")
    
    async def _populate_game_scores(self, conn: asyncpg.Connection, 
                                  max_games: Optional[int] = None) -> int:
        """Populate game scores from curated.game_outcomes"""
        query = """
        UPDATE curated.games_complete gc
        SET 
            home_score = go.home_score,
            away_score = go.away_score,
            winning_team = CASE 
                WHEN go.home_score > go.away_score THEN gc.home_team
                WHEN go.away_score > go.home_score THEN gc.away_team
                ELSE NULL
            END,
            game_status = 'completed',
            updated_at = NOW()
        FROM curated.game_outcomes go
        WHERE gc.id = go.game_id
          AND gc.home_score IS NULL
        """
        
        if max_games:
            query += f" AND gc.id <= {max_games}"
        
        result = await conn.execute(query)
        updated_count = int(result.split()[-1])
        
        self.logger.info("Populated game scores", updated_count=updated_count)
        return updated_count
    
    async def _populate_action_network_ids(self, conn: asyncpg.Connection,
                                         max_games: Optional[int] = None) -> int:
        """Populate Action Network IDs from raw_data.action_network_games"""
        query = """
        UPDATE curated.games_complete gc
        SET 
            action_network_game_id = ang.external_game_id::integer,
            updated_at = NOW()
        FROM raw_data.action_network_games ang
        WHERE gc.home_team = ang.home_team_abbr 
          AND gc.away_team = ang.away_team_abbr
          AND gc.game_date = ang.game_date
          AND ang.external_game_id IS NOT NULL
          AND ang.external_game_id != ''
          AND gc.action_network_game_id IS NULL
        """
        
        if max_games:
            query += f" AND gc.id <= {max_games}"
        
        result = await conn.execute(query)
        updated_count = int(result.split()[-1])
        
        self.logger.info("Populated Action Network IDs", updated_count=updated_count)
        return updated_count
    
    async def _populate_venue_data(self, conn: asyncpg.Connection,
                                 max_games: Optional[int] = None) -> int:
        """Populate venue data from Action Network raw JSON"""
        query = """
        UPDATE curated.games_complete gc
        SET 
            venue_name = (ang.raw_game_data->>'venue_name'),
            venue_id = CASE 
                WHEN (ang.raw_game_data->>'venue_id') ~ '^[0-9]+$' 
                THEN (ang.raw_game_data->>'venue_id')::integer
                ELSE NULL
            END,
            updated_at = NOW()
        FROM raw_data.action_network_games ang
        WHERE gc.action_network_game_id::text = ang.external_game_id
          AND ang.raw_game_data IS NOT NULL
          AND ang.raw_game_data->>'venue_name' IS NOT NULL
          AND gc.venue_name IS NULL
        """
        
        if max_games:
            query += f" AND gc.id <= {max_games}"
        
        result = await conn.execute(query)
        updated_count = int(result.split()[-1])
        
        self.logger.info("Populated venue data", updated_count=updated_count)
        return updated_count
    
    async def _populate_weather_data(self, conn: asyncpg.Connection,
                                   max_games: Optional[int] = None) -> int:
        """Populate weather data from Action Network raw JSON"""
        query = """
        UPDATE curated.games_complete gc
        SET 
            weather_condition = (ang.raw_game_data->'weather'->>'condition'),
            temperature = CASE 
                WHEN (ang.raw_game_data->'weather'->>'temperature') ~ '^[0-9]+$' 
                THEN (ang.raw_game_data->'weather'->>'temperature')::integer
                ELSE NULL
            END,
            wind_speed = CASE 
                WHEN (ang.raw_game_data->'weather'->>'wind_speed') ~ '^[0-9]+$' 
                THEN (ang.raw_game_data->'weather'->>'wind_speed')::integer
                ELSE NULL
            END,
            wind_direction = (ang.raw_game_data->'weather'->>'wind_direction'),
            humidity = CASE 
                WHEN (ang.raw_game_data->'weather'->>'humidity') ~ '^[0-9]+$' 
                THEN (ang.raw_game_data->'weather'->>'humidity')::integer
                ELSE NULL
            END,
            updated_at = NOW()
        FROM raw_data.action_network_games ang
        WHERE gc.action_network_game_id::text = ang.external_game_id
          AND ang.raw_game_data IS NOT NULL
          AND ang.raw_game_data->'weather' IS NOT NULL
          AND gc.weather_condition IS NULL
        """
        
        if max_games:
            query += f" AND gc.id <= {max_games}"
        
        result = await conn.execute(query)
        updated_count = int(result.split()[-1])
        
        self.logger.info("Populated weather data", updated_count=updated_count)
        return updated_count
    
    async def _update_data_quality(self, conn: asyncpg.Connection,
                                 max_games: Optional[int] = None) -> int:
        """Update data quality indicators based on populated data"""
        query = """
        UPDATE curated.games_complete
        SET 
            data_quality = CASE 
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                 AND venue_name IS NOT NULL 
                THEN 'HIGH'
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                THEN 'MEDIUM'
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                THEN 'LOW'
                ELSE 'MINIMAL'
            END,
            has_mlb_enrichment = CASE 
                WHEN action_network_game_id IS NOT NULL THEN true
                ELSE false
            END,
            mlb_correlation_confidence = CASE 
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                 AND action_network_game_id IS NOT NULL 
                THEN 0.9500
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                THEN 0.8000
                ELSE 0.5000
            END,
            updated_at = NOW()
        WHERE updated_at >= (NOW() - INTERVAL '1 hour')
        """
        
        if max_games:
            query += f" AND id <= {max_games}"
        
        result = await conn.execute(query)
        updated_count = int(result.split()[-1])
        
        self.logger.info("Updated data quality indicators", updated_count=updated_count)
        return updated_count
    
    async def _get_current_stats(self) -> Dict[str, Any]:
        """Get current statistics about games_complete table"""
        query = """
        SELECT 
            COUNT(*) as total_games,
            COUNT(CASE WHEN home_score IS NOT NULL THEN 1 END) as games_with_scores,
            COUNT(CASE WHEN action_network_game_id IS NOT NULL THEN 1 END) as games_with_external_ids,
            COUNT(CASE WHEN venue_name IS NOT NULL THEN 1 END) as games_with_venue,
            COUNT(CASE WHEN weather_condition IS NOT NULL THEN 1 END) as games_with_weather,
            COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_games
        FROM curated.games_complete
        """
        
        async with self.connection_pool.acquire() as conn:
            row = await conn.fetchrow(query)
            return dict(row)
    
    async def _analyze_population_potential(self, max_games: Optional[int] = None) -> PopulationStats:
        """Analyze what would be populated in a dry run"""
        self.logger.info("Analyzing population potential", max_games=max_games)
        
        # This would contain the analysis logic for dry run
        # For now, return current stats as baseline
        stats_dict = await self._get_current_stats()
        
        return PopulationStats(
            total_games=stats_dict['total_games'],
            games_updated=0,  # Would be calculated in full implementation
            scores_populated=stats_dict['games_with_scores'],
            external_ids_populated=stats_dict['games_with_external_ids'],
            venue_populated=stats_dict['games_with_venue'],
            weather_populated=stats_dict['games_with_weather'],
            high_quality_games=stats_dict['high_quality_games'],
            operation_duration_seconds=0.0
        )
    
    async def get_population_status(self) -> Dict[str, Any]:
        """Get current population status and data quality metrics"""
        try:
            # Check if the analytics view exists
            view_check_query = """
            SELECT COUNT(*) FROM information_schema.views 
            WHERE table_schema = 'analytics' 
            AND table_name = 'games_complete_data_quality'
            """
            
            async with self.connection_pool.acquire() as conn:
                view_exists = await conn.fetchval(view_check_query)
                
                if view_exists:
                    # Use the analytics view if available
                    query = "SELECT * FROM analytics.games_complete_data_quality"
                    row = await conn.fetchrow(query)
                    return dict(row)
                else:
                    # Fallback to direct query
                    return await self._get_current_stats()
                    
        except Exception as e:
            self.logger.error("Failed to get population status", error=str(e))
            raise UnifiedBettingError(f"Failed to get population status: {e}", 
                                              component="games_population", operation="get_status")