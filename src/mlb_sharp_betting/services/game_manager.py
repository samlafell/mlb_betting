#!/usr/bin/env python3
"""
Game Management Service

This service manages game records in the splits.games table, ensuring that
all games discovered during data collection are properly stored with
their MLB-StatsAPI game IDs.
"""

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set
import structlog

from ..db.connection import get_db_manager
from ..models.game import Game, Team, GameStatus
from ..core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


class GameManager:
    """Service for managing game records in the database."""
    
    def __init__(self, db_manager=None):
        """Initialize the game manager."""
        self.db_manager = db_manager or get_db_manager()
        self.logger = logger.bind(service="game_manager")
    
    def create_or_update_game(self, game_id: str, home_team: str, away_team: str, 
                            game_datetime: Optional[datetime] = None, **kwargs) -> bool:
        """
        Create or update a game record in the splits.games table.
        
        Args:
            game_id: MLB-StatsAPI game ID (e.g., "777483")
            home_team: Home team abbreviation
            away_team: Away team abbreviation
            game_datetime: Game date/time
            **kwargs: Additional game fields (status, scores, etc.)
            
        Returns:
            True if created/updated successfully
        """
        try:
            # Normalize team names
            home_team_normalized = Team.normalize_team_name(home_team)
            away_team_normalized = Team.normalize_team_name(away_team)
            
            if not home_team_normalized or not away_team_normalized:
                self.logger.warning("Invalid team names", 
                                  home_team=home_team, away_team=away_team)
                return False
            
            with self.db_manager.get_cursor() as cursor:
                # Check if game already exists
                cursor.execute(
                    "SELECT id FROM splits.games WHERE game_id = %s",
                    (game_id,)
                )
                existing_game = cursor.fetchone()
                
                if existing_game:
                    # Update existing game
                    update_fields = []
                    update_values = []
                    
                    # Always update these core fields
                    update_fields.extend(["home_team", "away_team", "updated_at"])
                    update_values.extend([home_team_normalized, away_team_normalized, datetime.now()])
                    
                    # Add optional fields if provided
                    if game_datetime:
                        update_fields.append("game_datetime")
                        update_values.append(game_datetime)
                    
                    for field in ["status", "home_score", "away_score", "venue", "weather_conditions"]:
                        if field in kwargs and kwargs[field] is not None:
                            update_fields.append(field)
                            update_values.append(kwargs[field])
                    
                    # Build update query
                    set_clause = ", ".join([f"{field} = %s" for field in update_fields])
                    update_values.append(game_id)  # For WHERE clause
                    
                    cursor.execute(f"""
                        UPDATE splits.games 
                        SET {set_clause}
                        WHERE game_id = %s
                    """, update_values)
                    
                    self.logger.debug("Updated existing game", game_id=game_id)
                    
                else:
                    # Create new game
                    internal_id = f"game_{game_id}_{uuid.uuid4().hex[:8]}"
                    
                    cursor.execute("""
                        INSERT INTO splits.games (
                            id, game_id, home_team, away_team, game_datetime,
                            status, home_score, away_score, venue, weather_conditions,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        internal_id,
                        game_id,
                        home_team_normalized,
                        away_team_normalized,
                        game_datetime or datetime.now(),
                        kwargs.get("status", GameStatus.SCHEDULED.value),
                        kwargs.get("home_score"),
                        kwargs.get("away_score"),
                        kwargs.get("venue"),
                        kwargs.get("weather_conditions"),
                        datetime.now(),
                        datetime.now()
                    ))
                    
                    self.logger.debug("Created new game", game_id=game_id)
                
                return True
                
        except Exception as e:
            self.logger.error("Failed to create/update game", 
                            game_id=game_id, error=str(e))
            return False
    
    def process_games_from_betting_splits(self, betting_splits: List[Dict]) -> Dict[str, int]:
        """
        Process games discovered from betting splits data.
        
        Args:
            betting_splits: List of betting split records
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {"processed": 0, "created": 0, "updated": 0, "errors": 0}
        
        # Extract unique games from betting splits
        games_found = set()
        game_details = {}
        
        for split in betting_splits:
            game_id = split.get("game_id")
            if not game_id:
                continue
                
            games_found.add(game_id)
            
            # Collect game details from the split
            if game_id not in game_details:
                game_details[game_id] = {
                    "home_team": split.get("home_team"),
                    "away_team": split.get("away_team"),
                    "game_datetime": split.get("game_datetime"),
                }
        
        self.logger.info("Processing games from betting splits", 
                        unique_games=len(games_found))
        
        # Process each unique game
        for game_id in games_found:
            stats["processed"] += 1
            details = game_details[game_id]
            
            if not details["home_team"] or not details["away_team"]:
                stats["errors"] += 1
                continue
            
            # Check if this is a new game or update
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM splits.games WHERE game_id = %s",
                    (game_id,)
                )
                existing = cursor.fetchone()
            
            success = self.create_or_update_game(
                game_id=game_id,
                home_team=details["home_team"],
                away_team=details["away_team"],
                game_datetime=details["game_datetime"]
            )
            
            if success:
                if existing:
                    stats["updated"] += 1
                else:
                    stats["created"] += 1
            else:
                stats["errors"] += 1
        
        self.logger.info("Completed game processing", **stats)
        return stats
    
    def get_games_requiring_updates(self, limit: int = 100) -> List[Dict]:
        """
        Get games that may need updates (e.g., missing scores for completed games).
        
        Args:
            limit: Maximum number of games to return
            
        Returns:
            List of game records that need updates
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT game_id, home_team, away_team, game_datetime, status, home_score, away_score
                    FROM splits.games 
                    WHERE (status IS NULL OR status != 'final') 
                       OR (status = 'final' AND (home_score IS NULL OR away_score IS NULL))
                    ORDER BY game_datetime DESC
                    LIMIT %s
                """, (limit,))
                
                results = cursor.fetchall()
                
                games = []
                for row in results:
                    games.append({
                        "game_id": row[0],
                        "home_team": row[1],
                        "away_team": row[2],
                        "game_datetime": row[3],
                        "status": row[4],
                        "home_score": row[5],
                        "away_score": row[6]
                    })
                
                return games
                
        except Exception as e:
            self.logger.error("Failed to get games requiring updates", error=str(e))
            return []
    
    def update_game_with_mlb_api_data(self, game_id: str, mlb_data: Dict) -> bool:
        """
        Update a game with data from MLB-StatsAPI.
        
        Args:
            game_id: MLB-StatsAPI game ID
            mlb_data: Game data from MLB-StatsAPI
            
        Returns:
            True if updated successfully
        """
        try:
            # Extract relevant data from MLB API response
            game_info = mlb_data.get("gameData", {})
            linescore = mlb_data.get("liveData", {}).get("linescore", {})
            
            teams = game_info.get("teams", {})
            home_team = teams.get("home", {}).get("abbreviation")
            away_team = teams.get("away", {}).get("abbreviation")
            
            game_datetime_str = game_info.get("datetime", {}).get("dateTime")
            game_datetime = None
            if game_datetime_str:
                try:
                    game_datetime = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
                except:
                    pass
            
            venue_name = game_info.get("venue", {}).get("name")
            
            # Game status
            status_data = mlb_data.get("gameData", {}).get("status", {})
            status_code = status_data.get("statusCode")
            
            if status_code == "F":
                status = GameStatus.FINAL.value
            elif status_code in ["I", "P"]:
                status = GameStatus.LIVE.value
            else:
                status = GameStatus.SCHEDULED.value
            
            # Scores
            home_score = linescore.get("teams", {}).get("home", {}).get("runs")
            away_score = linescore.get("teams", {}).get("away", {}).get("runs")
            
            # Update the game
            return self.create_or_update_game(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                status=status,
                home_score=home_score,
                away_score=away_score,
                venue=venue_name
            )
            
        except Exception as e:
            self.logger.error("Failed to update game with MLB API data", 
                            game_id=game_id, error=str(e))
            return False
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get statistics about games in the database.
        
        Returns:
            Dictionary with game statistics
        """
        try:
            with self.db_manager.get_cursor() as cursor:
                stats = {}
                
                # Total games
                cursor.execute("SELECT COUNT(*) FROM splits.games")
                stats["total_games"] = cursor.fetchone()[0]
                
                # Games by status
                cursor.execute("""
                    SELECT status, COUNT(*) 
                    FROM splits.games 
                    WHERE status IS NOT NULL
                    GROUP BY status
                """)
                status_results = cursor.fetchall()
                for status, count in status_results:
                    stats[f"games_{status}"] = count
                
                # Games with scores
                cursor.execute("""
                    SELECT COUNT(*) FROM splits.games 
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                """)
                stats["games_with_scores"] = cursor.fetchone()[0]
                
                # Recent games (last 7 days)
                cursor.execute("""
                    SELECT COUNT(*) FROM splits.games 
                    WHERE game_datetime >= CURRENT_DATE - INTERVAL '7 days'
                """)
                stats["recent_games_7d"] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            self.logger.error("Failed to get game statistics", error=str(e))
            return {}


__all__ = ["GameManager"] 