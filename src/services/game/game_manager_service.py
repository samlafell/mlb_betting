#!/usr/bin/env python3
"""
Game Manager Service

Migrated and enhanced game management functionality from the legacy module.
Manages game records in the unified database with improved error handling,
validation, and integration with the unified architecture.

Legacy Source: src/mlb_sharp_betting/services/game_manager.py
Enhanced Features:
- Unified database integration
- Improved validation and error handling
- Async operation support
- Enhanced logging and monitoring
- Better type safety and documentation

Part of Phase 5D: Critical Business Logic Migration
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import structlog

from ...core.config import get_settings
from ...core.logging import get_logger
from ...core.exceptions import DatabaseError, ValidationError
from ...data.database.connection import get_connection
from ...data.models.unified.game import UnifiedGameModel

logger = get_logger(__name__)


@dataclass
class GameCreationResult:
    """Result of game creation or update operation."""
    game_id: int
    operation: str  # 'created' or 'updated'
    changes_made: List[str]
    validation_warnings: List[str]


@dataclass
class GameStats:
    """Game statistics summary."""
    total_games: int
    games_last_7_days: int
    games_today: int
    completed_games: int
    scheduled_games: int
    average_games_per_day: float


class GameManagerService:
    """
    Unified Game Manager Service
    
    Manages game records and provides game-related database operations
    with enhanced functionality and unified architecture integration.
    
    Features:
    - Create, update, and retrieve game records
    - Game data validation and normalization
    - Bulk game operations
    - Game statistics and reporting
    - Integration with unified data models
    """
    
    def __init__(self):
        """Initialize the game manager service."""
        self.settings = get_settings()
        self.logger = logger.bind(service="GameManagerService")
        
        # Game field mappings for legacy compatibility
        self.field_mappings = {
            'game_id': 'sportsbookreview_game_id',
            'mlb_game_id': 'mlb_stats_api_game_id',
            'game_datetime': 'game_datetime',
            'game_date': 'game_date'
        }
        
        self.logger.info("GameManagerService initialized")
    
    async def create_or_update_game(self, game_data: Dict[str, Any]) -> GameCreationResult:
        """
        Create or update a game record with enhanced validation and error handling.
        
        Args:
            game_data: Dictionary containing game information
            
        Returns:
            GameCreationResult with operation details
            
        Raises:
            ValidationError: If game data is invalid
            DatabaseError: If database operation fails
        """
        try:
            # Validate and normalize game data
            normalized_data = await self._validate_and_normalize_game_data(game_data)
            
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Check for existing game using multiple identifiers
                    existing_game = await self._find_existing_game(cursor, normalized_data)
                    
                    if existing_game:
                        # Update existing game
                        result = await self._update_existing_game(
                            cursor, existing_game['id'], normalized_data
                        )
                        await conn.commit()
                        
                        self.logger.info("Game updated successfully",
                                       game_id=result.game_id,
                                       operation=result.operation,
                                       changes=len(result.changes_made))
                        
                        return result
                    else:
                        # Create new game
                        result = await self._create_new_game(cursor, normalized_data)
                        await conn.commit()
                        
                        self.logger.info("Game created successfully",
                                       game_id=result.game_id,
                                       operation=result.operation)
                        
                        return result
                        
        except Exception as e:
            self.logger.error("Failed to create/update game", error=str(e), game_data=game_data)
            if isinstance(e, (ValidationError, DatabaseError)):
                raise
            raise DatabaseError(f"Game operation failed: {str(e)}") from e
    
    async def get_game_by_id(self, game_id: Union[str, int], 
                           id_type: str = 'any') -> Optional[Dict[str, Any]]:
        """
        Retrieve a game by its ID with flexible ID type support.
        
        Args:
            game_id: Game identifier
            id_type: Type of ID ('any', 'primary', 'sbr', 'mlb')
            
        Returns:
            Game data dictionary or None if not found
        """
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    if id_type == 'primary':
                        query = "SELECT * FROM core_betting.games WHERE id = %s"
                        params = [game_id]
                    elif id_type == 'sbr':
                        query = "SELECT * FROM core_betting.games WHERE sportsbookreview_game_id = %s"
                        params = [str(game_id)]
                    elif id_type == 'mlb':
                        query = "SELECT * FROM core_betting.games WHERE mlb_stats_api_game_id = %s"
                        params = [str(game_id)]
                    else:  # 'any'
                        query = """
                        SELECT * FROM core_betting.games 
                        WHERE id = %s 
                        OR sportsbookreview_game_id = %s 
                        OR mlb_stats_api_game_id = %s
                        """
                        params = [game_id, str(game_id), str(game_id)]
                    
                    await cursor.execute(query, params)
                    row = await cursor.fetchone()
                    
                    if row:
                        # Convert row to dictionary
                        columns = [desc[0] for desc in cursor.description]
                        return dict(zip(columns, row))
                    
                    return None
                    
        except Exception as e:
            self.logger.error("Failed to retrieve game", game_id=game_id, error=str(e))
            raise DatabaseError(f"Failed to retrieve game: {str(e)}") from e
    
    async def get_recent_games(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get games from the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of game data dictionaries
        """
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    query = """
                    SELECT * FROM core_betting.games 
                    WHERE game_date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY game_datetime DESC
                    """
                    
                    await cursor.execute(query, [days])
                    rows = await cursor.fetchall()
                    
                    # Convert rows to dictionaries
                    columns = [desc[0] for desc in cursor.description]
                    games = [dict(zip(columns, row)) for row in rows]
                    
                    self.logger.debug("Retrieved recent games", count=len(games), days=days)
                    return games
                    
        except Exception as e:
            self.logger.error("Failed to retrieve recent games", days=days, error=str(e))
            raise DatabaseError(f"Failed to retrieve recent games: {str(e)}") from e
    
    async def get_games_by_date_range(self, start_date: datetime, 
                                    end_date: datetime) -> List[Dict[str, Any]]:
        """
        Get games within a specific date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            List of game data dictionaries
        """
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    query = """
                    SELECT * FROM core_betting.games 
                    WHERE game_date BETWEEN %s AND %s
                    ORDER BY game_datetime ASC
                    """
                    
                    await cursor.execute(query, [start_date.date(), end_date.date()])
                    rows = await cursor.fetchall()
                    
                    # Convert rows to dictionaries
                    columns = [desc[0] for desc in cursor.description]
                    games = [dict(zip(columns, row)) for row in rows]
                    
                    self.logger.debug("Retrieved games by date range", 
                                    count=len(games),
                                    start_date=start_date.date(),
                                    end_date=end_date.date())
                    return games
                    
        except Exception as e:
            self.logger.error("Failed to retrieve games by date range", 
                            start_date=start_date, end_date=end_date, error=str(e))
            raise DatabaseError(f"Failed to retrieve games by date range: {str(e)}") from e
    
    async def get_game_stats(self) -> GameStats:
        """
        Get comprehensive game statistics.
        
        Returns:
            GameStats object with current statistics
        """
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Get total games
                    await cursor.execute("SELECT COUNT(*) FROM core_betting.games")
                    total_games = (await cursor.fetchone())[0]
                    
                    # Get games from last 7 days
                    await cursor.execute("""
                        SELECT COUNT(*) FROM core_betting.games 
                        WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
                    """)
                    games_last_7_days = (await cursor.fetchone())[0]
                    
                    # Get games today
                    await cursor.execute("""
                        SELECT COUNT(*) FROM core_betting.games 
                        WHERE game_date = CURRENT_DATE
                    """)
                    games_today = (await cursor.fetchone())[0]
                    
                    # Get completed games
                    await cursor.execute("""
                        SELECT COUNT(*) FROM core_betting.games 
                        WHERE game_status = 'completed'
                    """)
                    completed_games = (await cursor.fetchone())[0]
                    
                    # Get scheduled games
                    await cursor.execute("""
                        SELECT COUNT(*) FROM core_betting.games 
                        WHERE game_status = 'scheduled'
                    """)
                    scheduled_games = (await cursor.fetchone())[0]
                    
                    # Calculate average games per day (last 30 days)
                    await cursor.execute("""
                        SELECT COUNT(*) FROM core_betting.games 
                        WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
                    """)
                    games_last_30_days = (await cursor.fetchone())[0]
                    average_games_per_day = games_last_30_days / 30.0
                    
                    return GameStats(
                        total_games=total_games,
                        games_last_7_days=games_last_7_days,
                        games_today=games_today,
                        completed_games=completed_games,
                        scheduled_games=scheduled_games,
                        average_games_per_day=average_games_per_day
                    )
                    
        except Exception as e:
            self.logger.error("Failed to retrieve game stats", error=str(e))
            raise DatabaseError(f"Failed to retrieve game stats: {str(e)}") from e
    
    async def bulk_create_games(self, games_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple games in a single transaction.
        
        Args:
            games_data: List of game data dictionaries
            
        Returns:
            Dictionary with operation results
        """
        results = {
            'created': 0,
            'updated': 0,
            'errors': 0,
            'error_details': []
        }
        
        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    for game_data in games_data:
                        try:
                            result = await self.create_or_update_game(game_data)
                            if result.operation == 'created':
                                results['created'] += 1
                            else:
                                results['updated'] += 1
                        except Exception as e:
                            results['errors'] += 1
                            results['error_details'].append({
                                'game_data': game_data,
                                'error': str(e)
                            })
                            
                await conn.commit()
                
                self.logger.info("Bulk game creation completed",
                               created=results['created'],
                               updated=results['updated'],
                               errors=results['errors'])
                
                return results
                
        except Exception as e:
            self.logger.error("Bulk game creation failed", error=str(e))
            raise DatabaseError(f"Bulk game creation failed: {str(e)}") from e
    
    # Private helper methods
    
    async def _validate_and_normalize_game_data(self, game_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize game data."""
        normalized = game_data.copy()
        warnings = []
        
        # Required fields validation
        required_fields = ['home_team', 'away_team']
        for field in required_fields:
            if field not in normalized or not normalized[field]:
                raise ValidationError(f"Required field '{field}' is missing or empty")
        
        # Map legacy field names
        for old_field, new_field in self.field_mappings.items():
            if old_field in normalized and old_field != new_field:
                normalized[new_field] = normalized.pop(old_field)
        
        # Set defaults
        if 'game_status' not in normalized:
            normalized['game_status'] = 'scheduled'
        
        if 'created_at' not in normalized:
            normalized['created_at'] = datetime.now()
        
        normalized['updated_at'] = datetime.now()
        
        return normalized
    
    async def _find_existing_game(self, cursor, game_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find existing game using multiple identifiers."""
        # Try to find by various ID fields
        identifiers = [
            ('sportsbookreview_game_id', game_data.get('sportsbookreview_game_id')),
            ('mlb_stats_api_game_id', game_data.get('mlb_stats_api_game_id')),
        ]
        
        for field, value in identifiers:
            if value:
                await cursor.execute(f"SELECT * FROM core_betting.games WHERE {field} = %s", [value])
                row = await cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, row))
        
        # Try to find by team and date combination
        if all(field in game_data for field in ['home_team', 'away_team', 'game_date']):
            await cursor.execute("""
                SELECT * FROM core_betting.games 
                WHERE home_team = %s AND away_team = %s AND game_date = %s
            """, [
                game_data['home_team'],
                game_data['away_team'],
                game_data['game_date']
            ])
            row = await cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
        
        return None
    
    async def _update_existing_game(self, cursor, game_id: int, 
                                  game_data: Dict[str, Any]) -> GameCreationResult:
        """Update an existing game record."""
        # Build update query dynamically
        update_fields = []
        update_values = []
        changes_made = []
        
        for field, value in game_data.items():
            if field != 'id':  # Don't update primary key
                update_fields.append(f"{field} = %s")
                update_values.append(value)
                changes_made.append(field)
        
        if update_fields:
            query = f"""
            UPDATE core_betting.games
            SET {', '.join(update_fields)}
            WHERE id = %s
            """
            update_values.append(game_id)
            
            await cursor.execute(query, update_values)
        
        return GameCreationResult(
            game_id=game_id,
            operation='updated',
            changes_made=changes_made,
            validation_warnings=[]
        )
    
    async def _create_new_game(self, cursor, game_data: Dict[str, Any]) -> GameCreationResult:
        """Create a new game record."""
        # Build insert query dynamically
        fields = list(game_data.keys())
        placeholders = ['%s'] * len(fields)
        values = list(game_data.values())
        
        query = f"""
        INSERT INTO core_betting.games ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        RETURNING id
        """
        
        await cursor.execute(query, values)
        game_id = (await cursor.fetchone())[0]
        
        return GameCreationResult(
            game_id=game_id,
            operation='created',
            changes_made=list(fields),
            validation_warnings=[]
        )


# Service instance for easy importing
game_manager_service = GameManagerService()


# Convenience functions for backward compatibility
async def create_or_update_game(game_data: Dict[str, Any]) -> GameCreationResult:
    """Convenience function to create or update a game."""
    return await game_manager_service.create_or_update_game(game_data)


async def get_game_by_id(game_id: Union[str, int], 
                        id_type: str = 'any') -> Optional[Dict[str, Any]]:
    """Convenience function to get a game by ID."""
    return await game_manager_service.get_game_by_id(game_id, id_type)


async def get_recent_games(days: int = 7) -> List[Dict[str, Any]]:
    """Convenience function to get recent games."""
    return await game_manager_service.get_recent_games(days)


if __name__ == "__main__":
    # Example usage
    async def main():
        # Test game creation
        test_game = {
            'home_team': 'NYY',
            'away_team': 'BOS',
            'game_date': datetime.now().date(),
            'game_datetime': datetime.now(),
            'sportsbookreview_game_id': 'test_123'
        }
        
        result = await create_or_update_game(test_game)
        print(f"Game {result.operation}: ID {result.game_id}")
        
        # Test game retrieval
        game = await get_game_by_id(result.game_id)
        print(f"Retrieved game: {game['home_team']} vs {game['away_team']}")
        
        # Test stats
        stats = await game_manager_service.get_game_stats()
        print(f"Total games: {stats.total_games}")
    
    asyncio.run(main()) 