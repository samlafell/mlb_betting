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
from pydantic import BaseModel, Field, field_validator

from src.core.config import get_settings
from src.core.exceptions import DatabaseError, UnifiedBettingError
from src.core.logging import UnifiedLogger, LogComponent


class PopulationStats(BaseModel):
    """Statistics for games population operation"""
    
    total_games: int = Field(description="Total games in table", ge=0)
    games_updated: int = Field(description="Number of games updated", ge=0)
    scores_populated: int = Field(description="Games with scores populated", ge=0)
    external_ids_populated: int = Field(description="Games with external IDs populated", ge=0)
    venue_populated: int = Field(description="Games with venue data populated", ge=0)
    weather_populated: int = Field(description="Games with weather data populated", ge=0)
    high_quality_games: int = Field(description="Games marked as high quality", ge=0)
    operation_duration_seconds: float = Field(description="Operation duration in seconds", ge=0.0)
    
    @field_validator('*')
    @classmethod
    def validate_non_negative(cls, v):
        if isinstance(v, (int, float)) and v < 0:
            raise ValueError('Values must be non-negative')
        return v


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
                max_size=20,  # Increased for production workloads
                command_timeout=180  # 3 minutes - balanced timeout
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
                                      max_games: Optional[int] = None,
                                      batch_size: Optional[int] = None) -> PopulationStats:
        """
        Populate all missing data in games_complete table from available sources.
        
        Args:
            dry_run: If True, only analyze what would be updated without making changes
            max_games: Limit number of games to process (for testing, must be positive integer)
            batch_size: Number of records to process per batch (for large datasets)
            
        Returns:
            PopulationStats with operation results
            
        Raises:
            ValueError: If max_games or batch_size are not positive integers
            UnifiedBettingError: For database or processing errors
        """
        # Input validation
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        if batch_size is not None:
            if not isinstance(batch_size, int) or batch_size <= 0 or batch_size > 5000:
                raise ValueError(f"batch_size must be a positive integer between 1 and 5000, got: {batch_size}")
        start_time = datetime.now()
        
        self.logger.info("Starting complete games population", 
                        dry_run=dry_run, max_games=max_games)
        
        try:
            # Get initial state
            initial_stats = await self._get_current_stats()
            
            if dry_run:
                # In dry run, analyze what would be updated
                stats = await self._analyze_population_potential(max_games)
                self.logger.info("Dry run analysis completed", stats=stats.model_dump())
                return stats
            
            # Execute actual population with batch processing for large datasets
            if batch_size and max_games and max_games > batch_size:
                # Use batch processing for large datasets
                scores_updated, ids_updated, venue_updated, weather_updated, quality_updated = \
                    await self._batch_populate_all_data(max_games, batch_size)
            else:
                # Single transaction for smaller datasets
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
                           stats=stats.model_dump())
            
            return stats
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error during games population", error=str(e), code=e.sqlstate)
            raise DatabaseError(f"Database error during games population: {e}", 
                              component="games_population", operation="populate_all")
        except ValueError as e:
            self.logger.error("Validation error during games population", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error during games population", error=str(e))
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
        except asyncpg.PostgresError as e:
            self.logger.error("Database error during game scores population", error=str(e), code=e.sqlstate)
            raise DatabaseError(f"Database error during game scores population: {e}", 
                              component="games_population", operation="populate_scores")
        except ValueError as e:
            self.logger.error("Validation error during game scores population", error=str(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error during game scores population", error=str(e))
            raise UnifiedBettingError(f"Game scores population failed: {e}", 
                                    component="games_population", operation="populate_scores")
    
    async def _populate_game_scores(self, conn: asyncpg.Connection, 
                                  max_games: Optional[int] = None) -> int:
        """Populate game scores from curated.game_outcomes
        
        Args:
            conn: Database connection
            max_games: Limit number of games to process (validated)
            
        Returns:
            Number of games updated
            
        Raises:
            ValueError: If max_games is invalid
            asyncpg.PostgresError: For database errors
        """
        # Input validation to prevent SQL injection
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        base_query = """
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
        
        try:
            if max_games:
                query = base_query + " AND gc.id <= $1"
                result = await conn.execute(query, max_games)
            else:
                result = await conn.execute(base_query)
            
            updated_count = int(result.split()[-1])
            
            self.logger.info("Populated game scores", updated_count=updated_count, max_games=max_games)
            return updated_count
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error populating game scores", error=str(e), max_games=max_games)
            raise
        except ValueError as e:
            self.logger.error("Validation error populating game scores", error=str(e), max_games=max_games)
            raise
        except Exception as e:
            self.logger.error("Unexpected error populating game scores", error=str(e), max_games=max_games)
            raise UnifiedBettingError(f"Failed to populate game scores: {e}", 
                                    component="games_population", operation="populate_scores")
    
    async def _populate_action_network_ids(self, conn: asyncpg.Connection,
                                         max_games: Optional[int] = None) -> int:
        """Populate Action Network IDs from raw_data.action_network_games
        
        Args:
            conn: Database connection
            max_games: Limit number of games to process (validated)
            
        Returns:
            Number of games updated
            
        Raises:
            ValueError: If max_games is invalid
            asyncpg.PostgresError: For database errors
        """
        # Input validation to prevent SQL injection
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        base_query = """
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
          AND ang.external_game_id ~ '^[0-9]+$'
          AND gc.action_network_game_id IS NULL
        """
        
        try:
            if max_games:
                query = base_query + " AND gc.id <= $1"
                result = await conn.execute(query, max_games)
            else:
                result = await conn.execute(base_query)
            
            updated_count = int(result.split()[-1])
            
            self.logger.info("Populated Action Network IDs", updated_count=updated_count, max_games=max_games)
            return updated_count
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error populating Action Network IDs", error=str(e), max_games=max_games)
            raise
        except ValueError as e:
            self.logger.error("Validation error populating Action Network IDs", error=str(e), max_games=max_games)
            raise
        except Exception as e:
            self.logger.error("Unexpected error populating Action Network IDs", error=str(e), max_games=max_games)
            raise UnifiedBettingError(f"Failed to populate Action Network IDs: {e}", 
                                    component="games_population", operation="populate_ids")
    
    async def _populate_venue_data(self, conn: asyncpg.Connection,
                                 max_games: Optional[int] = None) -> int:
        """Populate venue data from Action Network raw JSON
        
        Args:
            conn: Database connection
            max_games: Limit number of games to process (validated)
            
        Returns:
            Number of games updated
            
        Raises:
            ValueError: If max_games is invalid
            asyncpg.PostgresError: For database errors
        """
        # Input validation to prevent SQL injection
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        base_query = """
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
        
        try:
            if max_games:
                query = base_query + " AND gc.id <= $1"
                result = await conn.execute(query, max_games)
            else:
                result = await conn.execute(base_query)
            
            updated_count = int(result.split()[-1])
            
            self.logger.info("Populated venue data", updated_count=updated_count, max_games=max_games)
            return updated_count
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error populating venue data", error=str(e), max_games=max_games)
            raise
        except ValueError as e:
            self.logger.error("Validation error populating venue data", error=str(e), max_games=max_games)
            raise
        except Exception as e:
            self.logger.error("Unexpected error populating venue data", error=str(e), max_games=max_games)
            raise UnifiedBettingError(f"Failed to populate venue data: {e}", 
                                    component="games_population", operation="populate_venue")
    
    async def _populate_weather_data(self, conn: asyncpg.Connection,
                                   max_games: Optional[int] = None) -> int:
        """Populate weather data from Action Network raw JSON
        
        Args:
            conn: Database connection
            max_games: Limit number of games to process (validated)
            
        Returns:
            Number of games updated
            
        Raises:
            ValueError: If max_games is invalid
            asyncpg.PostgresError: For database errors
        """
        # Input validation to prevent SQL injection
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        base_query = """
        UPDATE curated.games_complete gc
        SET 
            weather_condition = (ang.raw_game_data->'weather'->>'condition'),
            temperature = CASE 
                WHEN (ang.raw_game_data->'weather'->>'temperature') ~ '^-?[0-9]+(\\.[0-9]+)?$' 
                THEN (ang.raw_game_data->'weather'->>'temperature')::integer
                ELSE NULL
            END,
            wind_speed = CASE 
                WHEN (ang.raw_game_data->'weather'->>'wind_speed') ~ '^[0-9]+(\\.[0-9]+)?$' 
                THEN (ang.raw_game_data->'weather'->>'wind_speed')::integer
                ELSE NULL
            END,
            wind_direction = (ang.raw_game_data->'weather'->>'wind_direction'),
            humidity = CASE 
                WHEN (ang.raw_game_data->'weather'->>'humidity') ~ '^[0-9]+(\\.[0-9]+)?$' 
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
        
        try:
            if max_games:
                query = base_query + " AND gc.id <= $1"
                result = await conn.execute(query, max_games)
            else:
                result = await conn.execute(base_query)
            
            updated_count = int(result.split()[-1])
            
            self.logger.info("Populated weather data", updated_count=updated_count, max_games=max_games)
            return updated_count
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error populating weather data", error=str(e), max_games=max_games)
            raise
        except ValueError as e:
            self.logger.error("Validation error populating weather data", error=str(e), max_games=max_games)
            raise
        except Exception as e:
            self.logger.error("Unexpected error populating weather data", error=str(e), max_games=max_games)
            raise UnifiedBettingError(f"Failed to populate weather data: {e}", 
                                    component="games_population", operation="populate_weather")
    
    async def _update_data_quality(self, conn: asyncpg.Connection,
                                 max_games: Optional[int] = None) -> int:
        """Update data quality indicators based on populated data
        
        Args:
            conn: Database connection
            max_games: Limit number of games to process (validated)
            
        Returns:
            Number of games updated
            
        Raises:
            ValueError: If max_games is invalid
            asyncpg.PostgresError: For database errors
        """
        # Input validation to prevent SQL injection
        if max_games is not None:
            if not isinstance(max_games, int) or max_games <= 0:
                raise ValueError(f"max_games must be a positive integer, got: {max_games}")
        
        # Get configurable confidence scores from settings
        settings = get_settings()
        high_confidence = getattr(settings, 'high_confidence_score', 0.9500)
        medium_confidence = getattr(settings, 'medium_confidence_score', 0.8000)
        low_confidence = getattr(settings, 'low_confidence_score', 0.5000)
        
        base_query = """
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
                THEN $1
                WHEN home_score IS NOT NULL 
                 AND away_score IS NOT NULL 
                THEN $2
                ELSE $3
            END,
            updated_at = NOW()
        WHERE updated_at >= (NOW() - INTERVAL '1 hour')
        """
        
        try:
            if max_games:
                query = base_query + " AND id <= $4"
                result = await conn.execute(query, high_confidence, medium_confidence, low_confidence, max_games)
            else:
                result = await conn.execute(base_query, high_confidence, medium_confidence, low_confidence)
            
            updated_count = int(result.split()[-1])
            
            self.logger.info("Updated data quality indicators", 
                           updated_count=updated_count, 
                           max_games=max_games,
                           high_confidence=high_confidence,
                           medium_confidence=medium_confidence,
                           low_confidence=low_confidence)
            return updated_count
            
        except asyncpg.PostgresError as e:
            self.logger.error("Database error updating data quality", error=str(e), max_games=max_games)
            raise
        except ValueError as e:
            self.logger.error("Validation error updating data quality", error=str(e), max_games=max_games)
            raise
        except Exception as e:
            self.logger.error("Unexpected error updating data quality", error=str(e), max_games=max_games)
            raise UnifiedBettingError(f"Failed to update data quality: {e}", 
                                    component="games_population", operation="update_quality")
    
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
    
    async def _batch_populate_all_data(self, max_games: int, batch_size: int) -> Tuple[int, int, int, int, int]:
        """
        Batch process population for large datasets to prevent memory issues and timeouts.
        
        Args:
            max_games: Maximum number of games to process
            batch_size: Number of records per batch
            
        Returns:
            Tuple of (scores_updated, ids_updated, venue_updated, weather_updated, quality_updated)
        """
        self.logger.info(f"Starting batch population: {max_games} games in batches of {batch_size}")
        
        total_scores = total_ids = total_venue = total_weather = total_quality = 0
        current_offset = 0
        batch_num = 1
        
        while current_offset < max_games:
            # Calculate current batch size (don't exceed max_games)
            current_batch_size = min(batch_size, max_games - current_offset)
            batch_max = current_offset + current_batch_size
            
            self.logger.info(f"Processing batch {batch_num}: records {current_offset+1} to {batch_max}")
            
            try:
                async with self.connection_pool.acquire() as conn:
                    async with conn.transaction():
                        # Process current batch with limited range
                        scores_batch = await self._populate_game_scores_batch(conn, current_offset, current_batch_size)
                        ids_batch = await self._populate_action_network_ids_batch(conn, current_offset, current_batch_size)
                        venue_batch = await self._populate_venue_data_batch(conn, current_offset, current_batch_size)
                        weather_batch = await self._populate_weather_data_batch(conn, current_offset, current_batch_size)
                        quality_batch = await self._update_data_quality_batch(conn, current_offset, current_batch_size)
                        
                        # Accumulate totals
                        total_scores += scores_batch
                        total_ids += ids_batch
                        total_venue += venue_batch
                        total_weather += weather_batch
                        total_quality += quality_batch
                        
                        self.logger.info(f"Batch {batch_num} completed: "
                                       f"scores={scores_batch}, ids={ids_batch}, "
                                       f"venue={venue_batch}, weather={weather_batch}, quality={quality_batch}")
                
                # Update for next iteration
                current_offset += current_batch_size
                batch_num += 1
                
                # Brief pause between batches to prevent overwhelming the database
                await asyncio.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Batch {batch_num} failed", error=str(e), 
                                offset=current_offset, batch_size=current_batch_size)
                # Continue with next batch instead of failing completely
                current_offset += current_batch_size
                batch_num += 1
                continue
        
        self.logger.info(f"Batch population completed: {batch_num-1} batches processed")
        return total_scores, total_ids, total_venue, total_weather, total_quality