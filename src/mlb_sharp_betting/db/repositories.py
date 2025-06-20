"""
Repository pattern implementation for the MLB Sharp Betting system.

This module provides repository classes for data access with full CRUD operations,
transaction support, and type safety using Pydantic models.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import os
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import structlog
from pydantic import BaseModel

from .connection import DatabaseManager, get_db_manager
from ..core.exceptions import DatabaseError, ValidationError
from ..models.base import IdentifiedModel
from ..models.game import Game, GameStatus, Team
from ..models.splits import BettingSplit, BookType, DataSource, SplitType
from ..models.sharp import SharpAction, SharpSignal, ConfidenceLevel

# Import the new game outcome repository
from .game_outcome_repository import GameOutcomeRepository

logger = structlog.get_logger(__name__)

# Type variable for model types
ModelType = TypeVar('ModelType', bound=BaseModel)


class RepositoryError(Exception):
    """Base exception for repository operations."""
    pass


class NotFoundError(RepositoryError):
    """Raised when a requested entity is not found."""
    pass


class DuplicateError(RepositoryError):
    """Raised when attempting to create a duplicate entity."""
    pass


class BaseRepository(ABC):
    """
    Abstract base repository class with common CRUD operations.
    
    Provides a consistent interface for data access operations
    across different entity types.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        """
        Initialize repository with database manager.
        
        Args:
            db_manager: Database manager instance (uses default if None)
        """
        self.db = db_manager or get_db_manager()
        self._coordinator = None  # Lazy loaded to avoid circular imports
        self.logger = logger.bind(repository=self.__class__.__name__)
    
    def _get_coordinator(self):
        """Lazy load coordinator to avoid circular imports"""
        if self._coordinator is None:
            from ..services.database_coordinator import get_database_coordinator
            self._coordinator = get_database_coordinator()
        return self._coordinator

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Get the table name for this repository."""
        pass

    @property
    @abstractmethod
    def model_class(self) -> Type[ModelType]:
        """Get the model class for this repository."""
        pass

    def _row_to_model(self, row: tuple, columns: List[str]) -> ModelType:
        """
        Convert database row to model instance.
        
        Args:
            row: Database row tuple
            columns: Column names
            
        Returns:
            Model instance
        """
        if not row:
            raise ValueError("Empty row provided")
            
        try:
            data = dict(zip(columns, row))
            return self.model_class(**data)
        except Exception as e:
            self.logger.error("Failed to convert row to model", 
                            error=str(e), row=row, columns=columns)
            raise ValidationError(f"Failed to convert row to model: {e}")

    def _model_to_insert_data(self, model: ModelType) -> Dict[str, Any]:
        """
        Convert model to insert data dictionary.
        
        Args:
            model: Model instance
            
        Returns:
            Dictionary suitable for database insertion
        """
        return model.dict(exclude_none=True, by_alias=False)

    def _build_where_clause(self, filters: Dict[str, Any]) -> tuple[str, tuple]:
        """
        Build WHERE clause from filters dictionary.
        
        Args:
            filters: Dictionary of column:value filters
            
        Returns:
            Tuple of (where_clause, parameters)
        """
        if not filters:
            return "", ()
            
        conditions = []
        parameters = []
        
        for column, value in filters.items():
            if value is None:
                conditions.append(f"{column} IS NULL")
            elif isinstance(value, (list, tuple)):
                placeholders = ",".join("?" * len(value))
                conditions.append(f"{column} IN ({placeholders})")
                parameters.extend(value)
            else:
                conditions.append(f"{column} = ?")
                parameters.append(value)
        
        where_clause = " AND ".join(conditions)
        return f"WHERE {where_clause}", tuple(parameters)

    def exists(self, **filters: Any) -> bool:
        """
        Check if entity exists with given filters.
        
        Args:
            **filters: Filter conditions
            
        Returns:
            True if entity exists, False otherwise
        """
        try:
            where_clause, parameters = self._build_where_clause(filters)
            query = f"SELECT 1 FROM {self.table_name} {where_clause} LIMIT 1"
            
            result = self.db.execute_query(query, parameters)
            return len(result) > 0 if result else False
        except Exception as e:
            self.logger.error("Error checking entity existence", 
                            filters=filters, error=str(e))
            raise DatabaseError(f"Failed to check existence: {e}")

    def count(self, **filters: Any) -> int:
        """
        Count entities matching the given filters.
        
        Args:
            **filters: Filter conditions
            
        Returns:
            Number of matching entities
        """
        try:
            where_clause, parameters = self._build_where_clause(filters)
            query = f"SELECT COUNT(*) FROM {self.table_name} {where_clause}"
            
            result = self.db.execute_query(query, parameters)
            return result[0][0] if result else 0
        except Exception as e:
            self.logger.error("Error counting entities", 
                            filters=filters, error=str(e))
            raise DatabaseError(f"Failed to count entities: {e}")

    def create(self, model: ModelType) -> ModelType:
        """
        Create a new entity.
        
        Args:
            model: Model instance to create
            
        Returns:
            Created model instance
            
        Raises:
            DuplicateError: If entity already exists
            DatabaseError: If creation fails
        """
        try:
            data = self._model_to_insert_data(model)
            
            # Build INSERT query
            columns = list(data.keys())
            placeholders = ",".join("?" * len(columns))
            query = f"INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({placeholders})"
            
            parameters = tuple(data.values())
            
            # Use coordinated database access to prevent conflicts
            self._get_coordinator().execute_write(query, parameters)
                
            self.logger.info("Entity created successfully", 
                           table=self.table_name, model_id=getattr(model, 'id', None))
            
            return model
        except Exception as e:
            self.logger.error("Failed to create entity", 
                            table=self.table_name, error=str(e))
            if "UNIQUE constraint failed" in str(e):
                raise DuplicateError(f"Entity already exists: {e}")
            raise DatabaseError(f"Failed to create entity: {e}")

    def get_by_id(self, entity_id: str) -> Optional[ModelType]:
        """
        Get entity by ID.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Model instance or None if not found
        """
        try:
            query = f"SELECT * FROM {self.table_name} WHERE id = ?"
            result = self.db.execute_query(query, (entity_id,))
            
            if not result:
                return None
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return self._row_to_model(result[0], columns)
        except Exception as e:
            self.logger.error("Failed to get entity by ID", 
                            entity_id=entity_id, error=str(e))
            raise DatabaseError(f"Failed to get entity: {e}")

    def find_all(self, limit: Optional[int] = None, offset: int = 0, **filters: Any) -> List[ModelType]:
        """
        Find all entities matching filters.
        
        Args:
            limit: Maximum number of results
            offset: Number of results to skip
            **filters: Filter conditions
            
        Returns:
            List of model instances
        """
        try:
            where_clause, parameters = self._build_where_clause(filters)
            
            query = f"SELECT * FROM {self.table_name} {where_clause}"
            
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset > 0:
                query += f" OFFSET {offset}"
                
            result = self.db.execute_query(query, parameters)
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find entities", 
                            filters=filters, error=str(e))
            raise DatabaseError(f"Failed to find entities: {e}")

    def find_one(self, **filters: Any) -> Optional[ModelType]:
        """
        Find one entity matching filters.
        
        Args:
            **filters: Filter conditions
            
        Returns:
            Model instance or None if not found
        """
        results = self.find_all(limit=1, **filters)
        return results[0] if results else None

    def update(self, entity_id: str, updates: Dict[str, Any]) -> Optional[ModelType]:
        """
        Update entity by ID.
        
        Args:
            entity_id: Entity ID
            updates: Dictionary of field updates
            
        Returns:
            Updated model instance or None if not found
        """
        try:
            if not updates:
                return self.get_by_id(entity_id)
                
            # Build UPDATE query
            set_clauses = [f"{col} = ?" for col in updates.keys()]
            query = f"UPDATE {self.table_name} SET {','.join(set_clauses)} WHERE id = ?"
            
            parameters = tuple(list(updates.values()) + [entity_id])
            
            # Use coordinated database access to prevent conflicts
            self._get_coordinator().execute_write(query, parameters)
                
            self.logger.info("Entity updated successfully", 
                           entity_id=entity_id, updates=updates)
            
            return self.get_by_id(entity_id)
        except Exception as e:
            self.logger.error("Failed to update entity", 
                            entity_id=entity_id, error=str(e))
            raise DatabaseError(f"Failed to update entity: {e}")

    def delete(self, entity_id: str) -> bool:
        """
        Delete entity by ID.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            True if entity was deleted, False if not found
        """
        try:
            query = f"DELETE FROM {self.table_name} WHERE id = ?"
            
            # Use coordinated database access to prevent conflicts
            self._get_coordinator().execute_write(query, (entity_id,))
                
            self.logger.info("Entity deleted successfully", entity_id=entity_id)
            return True
        except Exception as e:
            self.logger.error("Failed to delete entity", 
                            entity_id=entity_id, error=str(e))
            raise DatabaseError(f"Failed to delete entity: {e}")

    def bulk_create(self, models: List[ModelType], batch_size: int = 1000) -> List[ModelType]:
        """
        Create multiple entities in batches.
        
        Args:
            models: List of model instances
            batch_size: Size of each batch
            
        Returns:
            List of created model instances
        """
        if not models:
            return []
            
        try:
            # Process in batches
            created_models = []
            
            for i in range(0, len(models), batch_size):
                batch = models[i:i + batch_size]
                
                # Build batch insert
                data_list = [self._model_to_insert_data(model) for model in batch]
                
                if not data_list:
                    continue
                    
                # Assume all models have same structure
                columns = list(data_list[0].keys())
                placeholders = ",".join("?" * len(columns))
                query = f"INSERT INTO {self.table_name} ({','.join(columns)}) VALUES ({placeholders})"
                
                parameters_list = [tuple(data.values()) for data in data_list]
                
                # Use coordinated database access to prevent conflicts
                self._get_coordinator().execute_bulk_insert(query, parameters_list)
                    
                created_models.extend(batch)
                
            self.logger.info("Bulk create completed", 
                           count=len(created_models), table=self.table_name)
            
            return created_models
        except Exception as e:
            self.logger.error("Bulk create failed", 
                            count=len(models), error=str(e))
            raise DatabaseError(f"Bulk create failed: {e}")


class GameRepository(BaseRepository):
    """Repository for game data operations."""

    @property
    def table_name(self) -> str:
        return "splits.games"

    @property
    def model_class(self) -> Type[Game]:
        return Game

    def find_by_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        status: Optional[GameStatus] = None
    ) -> List[Game]:
        """
        Find games within a date range.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            status: Optional game status filter
            
        Returns:
            List of games in the date range
        """
        try:
            where_conditions = ["game_datetime BETWEEN ? AND ?"]
            parameters = [start_date.isoformat(), end_date.isoformat()]
            
            if status:
                where_conditions.append("status = ?")
                parameters.append(status.value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            query = f"SELECT * FROM {self.table_name} {where_clause} ORDER BY game_datetime"
            
            result = self.db.execute_query(query, tuple(parameters))
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find games by date range", 
                            start_date=start_date, end_date=end_date, error=str(e))
            raise DatabaseError(f"Failed to find games by date range: {e}")

    def find_by_teams(self, home_team: Team, away_team: Team) -> List[Game]:
        """
        Find games between specific teams.
        
        Args:
            home_team: Home team
            away_team: Away team
            
        Returns:
            List of games between the teams
        """
        return self.find_all(home_team=home_team.value, away_team=away_team.value)

    def find_completed_games(self, limit: Optional[int] = None) -> List[Game]:
        """
        Find completed games.
        
        Args:
            limit: Maximum number of results
            
        Returns:
            List of completed games
        """
        return self.find_all(status=GameStatus.FINAL.value, limit=limit)

    def find_upcoming_games(self, days_ahead: int = 7) -> List[Game]:
        """
        Find upcoming games.
        
        Args:
            days_ahead: Number of days to look ahead
            
        Returns:
            List of upcoming games
        """
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days_ahead)
        
        return self.find_by_date_range(
            start_date, 
            end_date, 
            status=GameStatus.SCHEDULED
        )


class BettingSplitRepository(BaseRepository):
    """Repository for betting split data operations."""

    @property
    def table_name(self) -> str:
        return "splits.raw_mlb_betting_splits"

    @property
    def model_class(self) -> Type[BettingSplit]:
        return BettingSplit

    def find_by_game_id(self, game_id: str) -> List[BettingSplit]:
        """
        Find all betting splits for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of betting splits for the game
        """
        return self.find_all(game_id=game_id)

    def find_by_source_and_book(
        self, 
        source: DataSource, 
        book: Optional[BookType],
        split_type: Optional[SplitType] = None
    ) -> List[BettingSplit]:
        """
        Find betting splits by source and book.
        
        Args:
            source: Data source
            book: Sportsbook (None for aggregated data)
            split_type: Optional split type filter
            
        Returns:
            List of matching betting splits
        """
        filters = {"source": source.value}
        if book is not None:
            filters["book"] = book.value
        else:
            filters["book"] = None  # For aggregated data
            
        if split_type:
            filters["split_type"] = split_type.value
            
        return self.find_all(**filters)

    def find_recent_splits(
        self, 
        hours: int = 24,
        source: Optional[DataSource] = None
    ) -> List[BettingSplit]:
        """
        Find recent betting splits.
        
        Args:
            hours: Number of hours to look back
            source: Optional data source filter
            
        Returns:
            List of recent betting splits
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            where_conditions = ["last_updated >= ?"]
            parameters = [cutoff_time.isoformat()]
            
            if source:
                where_conditions.append("source = ?")
                parameters.append(source.value)
            
            where_clause = "WHERE " + " AND ".join(where_conditions)
            query = f"SELECT * FROM {self.table_name} {where_clause} ORDER BY last_updated DESC"
            
            result = self.db.execute_query(query, tuple(parameters))
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find recent splits", 
                            hours=hours, source=source, error=str(e))
            raise DatabaseError(f"Failed to find recent splits: {e}")

    def find_splits_with_sharp_action(self) -> List[BettingSplit]:
        """
        Find betting splits with detected sharp action.
        
        Returns:
            List of splits with sharp action
        """
        try:
            query = f"SELECT * FROM {self.table_name} WHERE sharp_action IS NOT NULL AND sharp_action != ''"
            result = self.db.execute_query(query)
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find splits with sharp action", error=str(e))
            raise DatabaseError(f"Failed to find splits with sharp action: {e}")


class SharpActionRepository(BaseRepository):
    """Repository for sharp action data operations."""

    @property
    def table_name(self) -> str:
        return "splits.sharp_actions"

    @property
    def model_class(self) -> Type[SharpAction]:
        return SharpAction

    def find_by_game_id(self, game_id: str) -> List[SharpAction]:
        """
        Find sharp actions for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of sharp actions for the game
        """
        return self.find_all(game_id=game_id)

    def find_by_confidence_level(self, confidence: ConfidenceLevel) -> List[SharpAction]:
        """
        Find sharp actions by confidence level.
        
        Args:
            confidence: Confidence level filter
            
        Returns:
            List of sharp actions with matching confidence
        """
        return self.find_all(overall_confidence=confidence.value)

    def find_high_confidence_actions(self) -> List[SharpAction]:
        """
        Find high confidence sharp actions.
        
        Returns:
            List of high and very high confidence sharp actions
        """
        try:
            query = f"""
            SELECT * FROM {self.table_name} 
            WHERE overall_confidence IN ('high', 'very_high') 
            ORDER BY last_updated DESC
            """
            result = self.db.execute_query(query)
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find high confidence actions", error=str(e))
            raise DatabaseError(f"Failed to find high confidence actions: {e}")

    def find_actionable_bets(self, min_signals: int = 2) -> List[SharpAction]:
        """
        Find actionable sharp betting opportunities.
        
        Args:
            min_signals: Minimum number of signals required
            
        Returns:
            List of actionable sharp actions
        """
        try:
            query = f"""
            SELECT * FROM {self.table_name} 
            WHERE total_signals >= ? 
            AND overall_confidence IN ('high', 'very_high')
            AND recommended_bet IS NOT NULL
            ORDER BY high_confidence_signals DESC, last_updated DESC
            """
            result = self.db.execute_query(query, (min_signals,))
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find actionable bets", 
                            min_signals=min_signals, error=str(e))
            raise DatabaseError(f"Failed to find actionable bets: {e}")

    def find_recent_actions(self, hours: int = 24) -> List[SharpAction]:
        """
        Find recently detected sharp actions.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            List of recent sharp actions
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            query = f"""
            SELECT * FROM {self.table_name} 
            WHERE last_updated >= ?
            ORDER BY last_updated DESC
            """
            result = self.db.execute_query(query, (cutoff_time.isoformat(),))
            
            if not result:
                return []
                
            # Get column names
            columns_query = f"PRAGMA table_info({self.table_name})"
            columns_result = self.db.execute_query(columns_query)
            columns = [col[1] for col in columns_result] if columns_result else []
            
            return [self._row_to_model(row, columns) for row in result]
        except Exception as e:
            self.logger.error("Failed to find recent actions", 
                            hours=hours, error=str(e))
            raise DatabaseError(f"Failed to find recent actions: {e}")


# Convenience functions for getting repository instances
def get_game_repository(db_manager: Optional[DatabaseManager] = None) -> GameRepository:
    """Get a GameRepository instance."""
    return GameRepository(db_manager)


def get_betting_split_repository(db_manager: Optional[DatabaseManager] = None) -> BettingSplitRepository:
    """Get a BettingSplitRepository instance."""
    return BettingSplitRepository(db_manager)


def get_sharp_action_repository(db_manager: Optional[DatabaseManager] = None) -> SharpActionRepository:
    """Get a SharpActionRepository instance."""
    return SharpActionRepository(db_manager)


def get_game_outcome_repository(db_manager: Optional[DatabaseManager] = None) -> GameOutcomeRepository:
    """Get a GameOutcomeRepository instance."""
    return GameOutcomeRepository(db_manager)