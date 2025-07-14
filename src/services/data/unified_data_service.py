#!/usr/bin/env python3
"""
Unified Data Service for MLB Sharp Betting System.

This service provides a centralized interface for all data operations
including collection, processing, and storage of betting data from
multiple sources.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

import structlog

from ...data.collection.actionnetwork import ActionNetworkHistoryCollector
from ...data.models.unified.actionnetwork import ActionNetworkHistoricalData
from ...data.database.connection import get_connection
from ...data.database.action_network_repository import ActionNetworkRepository
from ...core.config import get_settings
from ...core.exceptions import DataError

logger = structlog.get_logger(__name__)


class UnifiedDataService:
    """
    Unified data service that provides centralized access to all data operations.
    
    This service consolidates data collection, processing, and storage operations
    from multiple sources into a single, easy-to-use interface.
    """
    
    def __init__(self):
        """Initialize the unified data service."""
        self.settings = get_settings()
        self.logger = logger.bind(service="UnifiedDataService")
        
        # Initialize collectors
        self.action_network_history_collector = ActionNetworkHistoryCollector()
        
        # Initialize database connection and repository
        self.db_connection = None
        self.action_network_repository = None
        
        # Service statistics
        self.stats = {
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "database_saves": 0,
            "database_save_errors": 0,
            "last_operation_time": None
        }
        
        self.logger.info("UnifiedDataService initialized")
    
    async def _get_database_repository(self) -> ActionNetworkRepository:
        """Get or create the Action Network database repository."""
        if not self.action_network_repository:
            if not self.db_connection:
                self.db_connection = get_connection()
                await self.db_connection.connect()
            
            self.action_network_repository = ActionNetworkRepository(self.db_connection)
            self.logger.info("Action Network database repository initialized")
        
        return self.action_network_repository
    
    async def collect_action_network_history(self, game_data: Dict[str, Any]) -> Optional[ActionNetworkHistoricalData]:
        """
        Collect historical line movement data from Action Network for a single game.
        
        Args:
            game_data: Dictionary containing game information including:
                - game_id: Action Network game ID
                - home_team: Home team name
                - away_team: Away team name
                - game_datetime: Game start time
                - history_url: Action Network history URL
                
        Returns:
            ActionNetworkHistoricalData or None if collection fails
        """
        try:
            self.stats["total_operations"] += 1
            
            self.logger.info("Collecting Action Network history for single game",
                           game_id=game_data.get('game_id'),
                           matchup=f"{game_data.get('away_team')} @ {game_data.get('home_team')}")
            
            # Validate required fields
            required_fields = ['game_id', 'home_team', 'away_team', 'game_datetime', 'history_url']
            missing_fields = [field for field in required_fields if field not in game_data]
            
            if missing_fields:
                raise DataError(f"Missing required fields: {missing_fields}")
            
            # Collect historical data
            result = await self.action_network_history_collector.collect_history_data(
                history_url=game_data['history_url'],
                game_id=game_data['game_id'],
                home_team=game_data['home_team'],
                away_team=game_data['away_team'],
                game_datetime=game_data['game_datetime']
            )
            
            if result.success and result.data:
                historical_data = result.data[0]  # First item is the ActionNetworkHistoricalData
                
                # Save to database automatically
                await self._save_to_database(historical_data)
                
                self.stats["successful_operations"] += 1
                self.stats["last_operation_time"] = datetime.now()
                
                self.logger.info("Successfully collected Action Network history",
                               game_id=game_data['game_id'],
                               total_entries=historical_data.total_entries,
                               pregame_entries=historical_data.pregame_entries,
                               live_entries=historical_data.live_entries)
                
                return historical_data
            else:
                self.stats["failed_operations"] += 1
                self.logger.error("Failed to collect Action Network history",
                                game_id=game_data.get('game_id'),
                                errors=result.errors)
                return None
                
        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Exception during Action Network history collection",
                            game_id=game_data.get('game_id'),
                            error=str(e))
            return None
    
    async def collect_multiple_action_network_histories(self, games_data: List[Dict[str, Any]]) -> List[ActionNetworkHistoricalData]:
        """
        Collect historical line movement data from Action Network for multiple games.
        
        Args:
            games_data: List of game data dictionaries
            
        Returns:
            List of ActionNetworkHistoricalData objects
        """
        try:
            self.stats["total_operations"] += 1
            
            self.logger.info("Collecting Action Network histories for multiple games",
                           game_count=len(games_data))
            
            # Collect historical data for all games
            results = await self.action_network_history_collector.collect_multiple_histories(games_data)
            
            # Extract successful results
            historical_data_list = []
            successful_count = 0
            failed_count = 0
            
            for result in results:
                if result.success and result.data:
                    historical_data = result.data[0]
                    historical_data_list.append(historical_data)
                    
                    # Save to database automatically
                    await self._save_to_database(historical_data)
                    
                    successful_count += 1
                else:
                    failed_count += 1
                    self.logger.warning("Failed to collect history for game",
                                      errors=result.errors,
                                      metadata=result.metadata)
            
            self.stats["successful_operations"] += successful_count
            self.stats["failed_operations"] += failed_count
            self.stats["last_operation_time"] = datetime.now()
            
            self.logger.info("Completed multiple Action Network history collection",
                           total_games=len(games_data),
                           successful=successful_count,
                           failed=failed_count)
            
            return historical_data_list
            
        except Exception as e:
            self.stats["failed_operations"] += 1
            self.logger.error("Exception during multiple Action Network history collection",
                            error=str(e))
            return []
    
    async def extract_histories_from_json_file(self, json_file_path: str) -> List[ActionNetworkHistoricalData]:
        """
        Extract historical line movement data from games listed in a JSON file.
        
        This method reads a JSON file containing game data (like the one you provided)
        and extracts historical line movement data for all games with history URLs.
        
        Args:
            json_file_path: Path to the JSON file containing game data
            
        Returns:
            List of ActionNetworkHistoricalData objects
        """
        try:
            self.logger.info("Extracting histories from JSON file", file_path=json_file_path)
            
            # Read JSON file
            json_path = Path(json_file_path)
            if not json_path.exists():
                raise DataError(f"JSON file not found: {json_file_path}")
            
            with open(json_path, 'r') as f:
                json_data = json.load(f)
            
            # Extract games with history URLs
            games_with_history = []
            
            if 'games' in json_data:
                for game in json_data['games']:
                    if 'history_url' in game and game['history_url']:
                        # Convert string datetime to datetime object if needed
                        game_datetime = game.get('start_time')
                        if isinstance(game_datetime, str):
                            game_datetime = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
                        
                        game_data = {
                            'game_id': game.get('game_id'),
                            'home_team': game.get('home_team'),
                            'away_team': game.get('away_team'),
                            'game_datetime': game_datetime,
                            'history_url': game.get('history_url')
                        }
                        games_with_history.append(game_data)
            
            self.logger.info("Found games with history URLs",
                           total_games=len(json_data.get('games', [])),
                           games_with_history=len(games_with_history))
            
            if not games_with_history:
                self.logger.warning("No games with history URLs found in JSON file")
                return []
            
            # Collect historical data for all games (will auto-save to database)
            return await self.collect_multiple_action_network_histories(games_with_history)
            
        except Exception as e:
            self.logger.error("Failed to extract histories from JSON file",
                            file_path=json_file_path,
                            error=str(e))
            return []
    
    async def save_historical_data_to_json(self, historical_data_list: List[ActionNetworkHistoricalData],
                                         output_file_path: str) -> bool:
        """
        Save historical line movement data to a JSON file.
        
        Args:
            historical_data_list: List of ActionNetworkHistoricalData objects
            output_file_path: Path where to save the JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Saving historical data to JSON file",
                           data_count=len(historical_data_list),
                           output_path=output_file_path)
            
            # Convert to serializable format
            serializable_data = {
                "extracted_at": datetime.now().isoformat(),
                "total_games": len(historical_data_list),
                "historical_data": [data.dict() for data in historical_data_list]
            }
            
            # Save to file
            output_path = Path(output_file_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(serializable_data, f, indent=2, default=str)
            
            self.logger.info("Successfully saved historical data to JSON file",
                           output_path=output_file_path,
                           file_size_mb=output_path.stat().st_size / (1024 * 1024))
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to save historical data to JSON file",
                            output_path=output_file_path,
                            error=str(e))
            return False
    
    async def _save_to_database(self, historical_data: ActionNetworkHistoricalData) -> None:
        """
        Save Action Network historical data to database.
        
        Args:
            historical_data: ActionNetworkHistoricalData to save
        """
        try:
            repository = await self._get_database_repository()
            
            # Check if we've already processed this game recently
            last_extraction_time = await repository.get_last_extraction_time(historical_data.game_id)
            
            if last_extraction_time:
                # Check if this is a recent duplicate (within 5 minutes)
                time_diff = datetime.now() - last_extraction_time
                if time_diff.total_seconds() < 300:  # 5 minutes
                    self.logger.info("Skipping database save - recent extraction exists",
                                   game_id=historical_data.game_id,
                                   last_extraction=last_extraction_time,
                                   time_diff_seconds=time_diff.total_seconds())
                    return
            
            # Save to database
            save_result = await repository.save_historical_data(historical_data)
            
            if save_result['success']:
                self.stats["database_saves"] += 1
                self.logger.info("Saved Action Network data to database",
                               game_id=historical_data.game_id,
                               lines_saved=save_result['lines_saved'],
                               duration_seconds=save_result['duration_seconds'])
            else:
                self.stats["database_save_errors"] += 1
                self.logger.error("Failed to save Action Network data to database",
                                game_id=historical_data.game_id,
                                error=save_result['error'])
            
        except Exception as e:
            self.stats["database_save_errors"] += 1
            self.logger.error("Exception during database save",
                            game_id=historical_data.game_id,
                            error=str(e))
    
    async def get_new_lines_since_last_extraction(self, game_id: int, book_id: int, 
                                                market_type: str) -> List[Dict[str, Any]]:
        """
        Get new betting lines since the last extraction for incremental updates.
        
        Args:
            game_id: Action Network game ID
            book_id: Action Network book ID
            market_type: Market type ('moneyline', 'spread', 'total')
            
        Returns:
            List of new betting lines
        """
        try:
            repository = await self._get_database_repository()
            return await repository.get_new_lines_since_last_extraction(
                game_id, book_id, market_type
            )
        except Exception as e:
            self.logger.error("Failed to get new lines since last extraction",
                            game_id=game_id,
                            book_id=book_id,
                            market_type=market_type,
                            error=str(e))
            return []
    
    async def get_line_movement_summary(self, game_id: int, book_id: Optional[int] = None,
                                      market_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get line movement summary for analysis.
        
        Args:
            game_id: Action Network game ID
            book_id: Optional book ID filter
            market_type: Optional market type filter
            
        Returns:
            List of line movement summaries
        """
        try:
            repository = await self._get_database_repository()
            return await repository.get_line_movement_summary(game_id, book_id, market_type)
        except Exception as e:
            self.logger.error("Failed to get line movement summary",
                            game_id=game_id,
                            error=str(e))
            return []
    
    async def check_for_new_movements(self, games_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Check for new line movements across all games and markets.
        
        This method checks each game/book/market combination for new movements
        since the last extraction and returns a summary of what's new.
        
        Args:
            games_data: List of game data dictionaries
            
        Returns:
            Dictionary with new movement summary
        """
        try:
            repository = await self._get_database_repository()
            
            new_movements_summary = {
                "total_games_checked": len(games_data),
                "games_with_new_movements": 0,
                "total_new_lines": 0,
                "by_market": {
                    "moneyline": 0,
                    "spread": 0,
                    "total": 0
                },
                "by_book": {},
                "games_needing_update": []
            }
            
            # Common Action Network book IDs
            book_ids = [15, 30, 68, 69, 71, 75]  # DK, FD, MGM, CZR, PB, CIRCA
            market_types = ['moneyline', 'spread', 'total']
            
            for game_data in games_data:
                game_id = game_data.get('game_id')
                if not game_id:
                    continue
                
                game_has_new_movements = False
                game_new_lines = 0
                
                # Check each book/market combination
                for book_id in book_ids:
                    for market_type in market_types:
                        new_lines = await repository.get_new_lines_since_last_extraction(
                            game_id, book_id, market_type
                        )
                        
                        if new_lines:
                            game_has_new_movements = True
                            game_new_lines += len(new_lines)
                            new_movements_summary["total_new_lines"] += len(new_lines)
                            new_movements_summary["by_market"][market_type] += len(new_lines)
                            
                            # Track by book
                            book_name = f"book_{book_id}"
                            if book_name not in new_movements_summary["by_book"]:
                                new_movements_summary["by_book"][book_name] = 0
                            new_movements_summary["by_book"][book_name] += len(new_lines)
                
                if game_has_new_movements:
                    new_movements_summary["games_with_new_movements"] += 1
                    new_movements_summary["games_needing_update"].append({
                        "game_id": game_id,
                        "home_team": game_data.get('home_team'),
                        "away_team": game_data.get('away_team'),
                        "new_lines_count": game_new_lines
                    })
            
            self.logger.info("Completed new movements check",
                           total_games=new_movements_summary["total_games_checked"],
                           games_with_new_movements=new_movements_summary["games_with_new_movements"],
                           total_new_lines=new_movements_summary["total_new_lines"])
            
            return new_movements_summary
            
        except Exception as e:
            self.logger.error("Failed to check for new movements", error=str(e))
            return {
                "total_games_checked": len(games_data),
                "games_with_new_movements": 0,
                "total_new_lines": 0,
                "error": str(e)
            }
    
    async def cleanup(self) -> None:
        """Clean up database connections and resources."""
        try:
            if self.action_network_history_collector:
                await self.action_network_history_collector.close()
            
            if self.db_connection:
                await self.db_connection.close()
                self.db_connection = None
                self.action_network_repository = None
            
            self.logger.info("UnifiedDataService cleanup completed")
            
        except Exception as e:
            self.logger.error("Error during cleanup", error=str(e))
    
    def get_service_stats(self) -> Dict[str, Any]:
        """
        Get service statistics.
        
        Returns:
            Dictionary containing service statistics
        """
        return {
            **self.stats,
            "success_rate": (
                self.stats["successful_operations"] / self.stats["total_operations"]
                if self.stats["total_operations"] > 0 else 0
            ),
            "last_operation_time_str": (
                self.stats["last_operation_time"].isoformat()
                if self.stats["last_operation_time"] else None
            )
        }
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            # Close any open connections
            await self.action_network_history_collector.close()
            self.logger.info("UnifiedDataService cleanup completed")
        except Exception as e:
            self.logger.error("Error during UnifiedDataService cleanup", error=str(e))


# Convenience function for easy access
_unified_data_service = None

def get_unified_data_service() -> UnifiedDataService:
    """
    Get the unified data service instance (singleton pattern).
    
    Returns:
        UnifiedDataService instance
    """
    global _unified_data_service
    if _unified_data_service is None:
        _unified_data_service = UnifiedDataService()
    return _unified_data_service 