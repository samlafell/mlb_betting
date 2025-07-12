#!/usr/bin/env python3
"""
Game Management Service

This service manages game records in the consolidated core_betting.games table, ensuring that
all game data is properly stored and accessible across the system.
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging

from ..core.logging import get_logger
from ..db.connection import DatabaseManager, get_db_manager
from ..db.table_registry import get_table_registry
from ..core.exceptions import DatabaseError

class GameManager:
    """Manages game records and provides game-related database operations."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or get_db_manager()
        self.table_registry = get_table_registry()
        self.logger = get_logger(f"{__name__}.GameManager")

    def create_or_update_game(self, game_data: Dict[str, Any]) -> int:
        """
        Create or update a game record in the core_betting.games table.
        
        Args:
            game_data: Dictionary containing game information
            
        Returns:
            int: The game record ID
        """
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                # Check if game already exists using multiple ID columns
                game_identifier = game_data.get('game_id', '')
                
                # Try to find existing game using various ID columns
                cursor.execute(f"""
                    SELECT id FROM {games_table} 
                    WHERE sportsbookreview_game_id = %s 
                    OR mlb_stats_api_game_id = %s
                    OR (home_team = %s AND away_team = %s AND game_date = %s)
                """, (
                    game_identifier,
                    game_identifier,
                    game_data.get('home_team', ''),
                    game_data.get('away_team', ''),
                    game_data.get('game_date', game_data.get('game_datetime', ''))
                ))
                
                existing_game = cursor.fetchone()
                
                if existing_game:
                    # Update existing game
                    game_id = existing_game[0]
                    
                    # Build dynamic update query based on provided data
                    update_fields = []
                    update_values = []
                    
                    # Map game_id to appropriate column
                    for field, value in game_data.items():
                        if field == 'game_id':
                            # Map to sportsbookreview_game_id if not already set
                            update_fields.append("sportsbookreview_game_id = %s")
                            update_values.append(value)
                        elif field != 'id':  # Don't update the primary key
                            update_fields.append(f"{field} = %s")
                            update_values.append(value)
                    
                    if update_fields:
                        update_query = f"""
                        UPDATE {games_table}
                        SET {', '.join(update_fields)}, updated_at = NOW()
                        WHERE id = %s
                        """
                        update_values.append(game_id)
                        
                        cursor.execute(update_query, update_values)
                        self.logger.debug(f"Updated game {game_identifier}")
                    
                    return game_id
                else:
                    # Insert new game - map fields to correct column names
                    mapped_fields = {}
                    for field, value in game_data.items():
                        if field == 'game_id':
                            mapped_fields['sportsbookreview_game_id'] = value
                        else:
                            mapped_fields[field] = value
                    
                    # Ensure required fields have defaults
                    if 'game_status' not in mapped_fields:
                        mapped_fields['game_status'] = 'scheduled'
                    if 'game_date' not in mapped_fields and 'game_datetime' in mapped_fields:
                        mapped_fields['game_date'] = mapped_fields['game_datetime']
                    
                    fields = list(mapped_fields.keys())
                    placeholders = ', '.join(['%s'] * len(fields))
                    values = list(mapped_fields.values())
                    
                    insert_query = f"""
                    INSERT INTO {games_table} (
                        {', '.join(fields)}
                    ) VALUES ({placeholders})
                    RETURNING id
                    """
                    
                    cursor.execute(insert_query, values)
                    game_id = cursor.fetchone()[0]
                    
                    self.logger.debug(f"Created new game {game_identifier} with ID {game_id}")
                    return game_id
                    
        except Exception as e:
            self.logger.error(f"Failed to create/update game {game_data.get('game_id', 'unknown')}: {e}")
            raise DatabaseError(f"Game creation/update failed: {e}")

    def get_game_by_id(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a game by its game_id.
        
        Args:
            game_id: The game identifier
            
        Returns:
            Dict containing game data or None if not found
        """
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(f"""
                    SELECT id, sportsbookreview_game_id, mlb_stats_api_game_id, 
                           home_team, away_team, game_date, game_datetime, game_status
                    FROM {games_table} 
                    WHERE sportsbookreview_game_id = %s 
                    OR mlb_stats_api_game_id = %s
                """, (game_id, game_id))
                
                result = cursor.fetchone()
                
                if result:
                    return {
                        'id': result[0],
                        'sportsbookreview_game_id': result[1],
                        'mlb_stats_api_game_id': result[2],
                        'home_team': result[3],
                        'away_team': result[4],
                        'game_date': result[5],
                        'game_datetime': result[6],
                        'game_status': result[7]
                    }
                else:
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to retrieve game {game_id}: {e}")
            return None

    def get_recent_games(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get games from the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of game dictionaries
        """
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                query = f"""
                SELECT id, sportsbookreview_game_id, home_team, away_team, game_date, game_status
                FROM {games_table}
                WHERE game_date >= %s
                ORDER BY game_date DESC
                """
                
                cutoff_date = datetime.now() - timedelta(days=days)
                cursor.execute(query, (cutoff_date,))
                
                results = cursor.fetchall()
                
                games = []
                for row in results:
                    games.append({
                        'id': row[0],
                        'sportsbookreview_game_id': row[1],
                        'home_team': row[2],
                        'away_team': row[3],
                        'game_date': row[4],
                        'game_status': row[5]
                    })
                
                return games
                
        except Exception as e:
            self.logger.error(f"Failed to get recent games: {e}")
            return []

    def get_game_count(self) -> int:
        """Get total number of games in the database."""
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {games_table}")
                return cursor.fetchone()[0]
                
        except Exception as e:
            self.logger.error(f"Failed to get game count: {e}")
            return 0

    def get_games_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Get games within a specific date range.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            
        Returns:
            List of game dictionaries
        """
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                query = f"""
                SELECT id, sportsbookreview_game_id, home_team, away_team, game_date, game_status
                FROM {games_table}
                WHERE game_date BETWEEN %s AND %s
                ORDER BY game_date
                """
                
                cursor.execute(query, (start_date, end_date))
                results = cursor.fetchall()
                
                games = []
                for row in results:
                    games.append({
                        'id': row[0],
                        'sportsbookreview_game_id': row[1],
                        'home_team': row[2],
                        'away_team': row[3],
                        'game_date': row[4],
                        'game_status': row[5]
                    })
                
                return games
                
        except Exception as e:
            self.logger.error(f"Failed to get games by date range: {e}")
            return []

    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        try:
            # Get table name from registry
            games_table = self.table_registry.get_table('games')
            
            with self.db_manager.get_cursor() as cursor:
                # Total games
                cursor.execute(f"SELECT COUNT(*) FROM {games_table}")
                total_games = cursor.fetchone()[0]
                
                # Games by status
                cursor.execute(f"""
                    SELECT game_status, COUNT(*) 
                    FROM {games_table}
                    GROUP BY game_status
                """)
                status_counts = dict(cursor.fetchall())
                
                # Recent games (last 30 days)
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {games_table}
                    WHERE game_date >= %s
                """, (datetime.now() - timedelta(days=30),))
                recent_games = cursor.fetchone()[0]
                
                # Upcoming games
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {games_table}
                    WHERE game_date > %s
                """, (datetime.now(),))
                upcoming_games = cursor.fetchone()[0]
                
                return {
                    'total_games': total_games,
                    'status_breakdown': status_counts,
                    'recent_games_30_days': recent_games,
                    'upcoming_games': upcoming_games
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get database stats: {e}")
            return {}

    def process_games_from_betting_splits(self, splits_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process games from betting splits data and create/update game records.
        
        Args:
            splits_data: List of betting splits data dictionaries
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            games_processed = 0
            games_created = 0
            games_updated = 0
            errors = 0
            
            # Track unique games from splits
            unique_games = {}
            
            for split in splits_data:
                try:
                    # Extract game information from split
                    game_key = f"{split.get('home_team', '')}_{split.get('away_team', '')}_{split.get('game_datetime', '')}"
                    
                    if game_key not in unique_games:
                        unique_games[game_key] = {
                            'game_id': split.get('game_id', game_key),
                            'home_team': split.get('home_team', ''),
                            'away_team': split.get('away_team', ''),
                            'game_datetime': split.get('game_datetime'),
                            'game_status': 'scheduled'
                        }
                    
                except Exception as e:
                    self.logger.warning(f"Failed to process split for game extraction: {e}")
                    errors += 1
                    continue
            
            # Process each unique game
            for game_data in unique_games.values():
                try:
                    # Check if game already exists
                    existing_game = self.get_game_by_id(game_data['game_id'])
                    
                    if existing_game:
                        # Update existing game
                        self.create_or_update_game(game_data)
                        games_updated += 1
                    else:
                        # Create new game
                        self.create_or_update_game(game_data)
                        games_created += 1
                    
                    games_processed += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to process game {game_data.get('game_id', 'unknown')}: {e}")
                    errors += 1
                    continue
            
            stats = {
                'games_processed': games_processed,
                'games_created': games_created,
                'games_updated': games_updated,
                'unique_games_found': len(unique_games),
                'errors': errors,
                'success_rate': (games_processed / len(unique_games)) * 100 if unique_games else 0
            }
            
            self.logger.info(f"Processed games from betting splits: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to process games from betting splits: {e}")
            return {
                'games_processed': 0,
                'games_created': 0,
                'games_updated': 0,
                'unique_games_found': 0,
                'errors': 1,
                'success_rate': 0,
                'error_message': str(e)
            } 