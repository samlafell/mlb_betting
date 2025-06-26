"""
Data persistence service for the MLB Sharp Betting system.

This service provides high-level operations for storing and retrieving
betting split data with proper validation, error handling, and transaction management.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import structlog
from pydantic import ValidationError

from ..db.connection import DatabaseManager, get_db_manager
from ..db.repositories import (
    BettingSplitRepository,
    GameRepository,
    SharpActionRepository,
    get_betting_split_repository,
    get_game_repository,
    get_sharp_action_repository,
)
from ..db.schema import SchemaManager, setup_database_schema
from ..models.splits import BettingSplit, DataSource, BookType, SplitType
from ..models.game import Game, GameStatus
from ..models.sharp import SharpAction
from ..utils.validators import BettingSplitValidator
from ..core.exceptions import DatabaseError, ValidationError as CustomValidationError

logger = structlog.get_logger(__name__)


class DataPersistenceService:
    """
    High-level service for persisting betting split data with validation and integrity checks.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        """Initialize data persistence service."""
        self.db_manager = db_manager or get_db_manager()
        self.schema_manager = SchemaManager(self.db_manager)
        
        # Initialize repositories
        self.betting_split_repo = get_betting_split_repository(self.db_manager)
        self.game_repo = get_game_repository(self.db_manager)
        self.sharp_action_repo = get_sharp_action_repository(self.db_manager)
        
        # Initialize validator
        self.validator = BettingSplitValidator()
        
        self.logger = logger.bind(service="DataPersistence")
        
        # Ensure schema exists
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Ensure database schema exists."""
        try:
            if not self.schema_manager.verify_schema():
                self.logger.info("Schema verification failed, setting up schema")
                self.schema_manager.setup_complete_schema()
            else:
                self.logger.debug("Schema verification passed")
        except Exception as e:
            self.logger.error("Failed to ensure schema", error=str(e))
            raise DatabaseError(f"Failed to ensure database schema: {e}")

    def store_betting_splits(
        self,
        splits: List[BettingSplit],
        batch_size: int = 100,
        validate: bool = True,
        skip_duplicates: bool = True
    ) -> Dict[str, int]:
        """
        Store betting splits with validation and deduplication.
        
        Args:
            splits: List of BettingSplit objects to store
            batch_size: Number of records to process in each batch
            validate: Whether to validate splits before storing
            skip_duplicates: Whether to skip duplicate records
            
        Returns:
            Dictionary with processing statistics
        """
        if not splits:
            return {"processed": 0, "stored": 0, "skipped": 0, "errors": 0}

        stats = {
            "processed": 0,
            "stored": 0,
            "skipped": 0,
            "errors": 0,
            "validation_errors": 0
        }

        self.logger.info("Starting betting splits storage", total_splits=len(splits))

        try:
            # Process in batches without explicit transaction - let PostgreSQL handle it
            for i in range(0, len(splits), batch_size):
                batch = splits[i:i + batch_size]
                batch_stats = self._process_batch(
                    batch, 
                    validate=validate, 
                    skip_duplicates=skip_duplicates
                )
                
                # Update overall stats
                for key, value in batch_stats.items():
                    stats[key] += value
                
                self.logger.debug(
                    "Processed batch",
                    batch_num=i // batch_size + 1,
                    batch_size=len(batch),
                    batch_stats=batch_stats
                )

            self.logger.info("Completed betting splits storage", stats=stats)
            return stats

        except Exception as e:
            self.logger.error("Failed to store betting splits", error=str(e), stats=stats)
            raise DatabaseError(f"Failed to store betting splits: {e}")

    def _process_batch(
        self,
        batch: List[BettingSplit],
        validate: bool,
        skip_duplicates: bool
    ) -> Dict[str, int]:
        """Process a batch of betting splits."""
        batch_stats = {
            "processed": 0,
            "stored": 0,
            "skipped": 0,
            "errors": 0,
            "validation_errors": 0
        }

        for split in batch:
            batch_stats["processed"] += 1
            
            try:
                # Validate if requested
                if validate:
                    validation_result = self.validator.validate(split)
                    # Handle both dict and object validation results
                    errors = validation_result.get("errors", []) if isinstance(validation_result, dict) else getattr(validation_result, "errors", [])
                    if errors:
                        self.logger.warning(
                            "Validation errors for split",
                            game_id=split.game_id,
                            split_type=split.split_type,
                            errors=errors
                        )
                        batch_stats["validation_errors"] += 1
                        
                        # Skip if there are critical errors
                        if any("required" in error.lower() for error in errors):
                            batch_stats["skipped"] += 1
                            continue

                # Check for duplicates if requested
                if skip_duplicates and self._is_duplicate(split):
                    batch_stats["skipped"] += 1
                    continue

                # Store the split
                stored_split = self.betting_split_repo.create(split)
                if stored_split:
                    batch_stats["stored"] += 1
                    self.logger.debug(
                        "Stored betting split",
                        id=stored_split.id,
                        game_id=split.game_id,
                        split_type=split.split_type
                    )

            except ValidationError as e:
                batch_stats["validation_errors"] += 1
                self.logger.error(
                    "Pydantic validation error",
                    game_id=getattr(split, "game_id", "unknown"),
                    error=str(e)
                )
            except Exception as e:
                batch_stats["errors"] += 1
                self.logger.error(
                    "Error processing split",
                    game_id=getattr(split, "game_id", "unknown"),
                    error=str(e)
                )

        return batch_stats

    def _is_duplicate(self, split: BettingSplit) -> bool:
        """Check if a betting split is a duplicate."""
        try:
            # Handle enum values safely
            split_type_value = split.split_type.value if hasattr(split.split_type, 'value') else str(split.split_type)
            source_value = split.source.value if hasattr(split.source, 'value') else str(split.source)  
            book_value = split.book.value if hasattr(split.book, 'value') else str(split.book)
            
            existing = self.betting_split_repo.find_all(
                game_id=split.game_id,
                split_type=split_type_value,
                source=source_value,
                book=book_value
            )
            
            # Consider it a duplicate if we have a very recent entry
            if existing:
                for existing_split in existing:
                    # Handle timezone aware/naive datetime comparison
                    split_time = split.last_updated
                    existing_time = existing_split.last_updated
                    
                    # Skip comparison if either time is None
                    if split_time is None or existing_time is None:
                        continue
                    
                    # Make both timezone-naive for comparison if one is naive
                    if split_time.tzinfo is not None and existing_time.tzinfo is None:
                        split_time = split_time.replace(tzinfo=None)
                    elif split_time.tzinfo is None and existing_time.tzinfo is not None:
                        existing_time = existing_time.replace(tzinfo=None)
                    
                    time_diff = abs((split_time - existing_time).total_seconds())
                    if time_diff < 300:  # 5 minutes
                        return True
                        
            return False
            
        except Exception as e:
            self.logger.warning("Error checking for duplicates", error=str(e))
            return False

    def store_game_metadata(self, games: List[Game]) -> Dict[str, int]:
        """
        Store game metadata.
        
        Args:
            games: List of Game objects to store
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {"processed": 0, "stored": 0, "updated": 0, "errors": 0}

        try:
            # Process games without explicit transaction - let PostgreSQL handle it
            for game in games:
                stats["processed"] += 1
                
                try:
                    # Check if game exists
                    existing_game = self.game_repo.find_one(game_id=game.game_id)
                    
                    if existing_game:
                        # Update existing game
                        updates = {
                            "status": game.status.value,
                            "home_score": game.home_score,
                            "away_score": game.away_score,
                            "updated_at": datetime.now()
                        }
                        
                        updated_game = self.game_repo.update(existing_game.id, updates)
                        if updated_game:
                            stats["updated"] += 1
                    else:
                        # Create new game
                        stored_game = self.game_repo.create(game)
                        if stored_game:
                            stats["stored"] += 1
                            
                except Exception as e:
                    stats["errors"] += 1
                    self.logger.error(
                        "Error storing game",
                        game_id=game.game_id,
                        error=str(e)
                    )

            self.logger.info("Completed game metadata storage", stats=stats)
            return stats

        except Exception as e:
            self.logger.error("Failed to store game metadata", error=str(e))
            raise DatabaseError(f"Failed to store game metadata: {e}")

    def get_recent_splits(
        self,
        hours: int = 24,
        source: Optional[DataSource] = None,
        split_type: Optional[SplitType] = None
    ) -> List[BettingSplit]:
        """
        Get recently stored betting splits.
        
        Args:
            hours: Number of hours to look back
            source: Optional data source filter
            split_type: Optional split type filter
            
        Returns:
            List of recent betting splits
        """
        try:
            filters = {}
            if source:
                filters["source"] = source.value
            if split_type:
                filters["split_type"] = split_type.value

            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            # Use the repository's find_recent_splits method instead
            return self.betting_split_repo.find_recent_splits(hours=hours, source=source)

        except Exception as e:
            self.logger.error("Failed to get recent splits", error=str(e))
            raise DatabaseError(f"Failed to get recent splits: {e}")

    def get_splits_by_game(self, game_id: str) -> List[BettingSplit]:
        """
        Get all splits for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of betting splits for the game
        """
        try:
            return self.betting_split_repo.find_by_game_id(game_id)
        except Exception as e:
            self.logger.error("Failed to get splits by game", game_id=game_id, error=str(e))
            raise DatabaseError(f"Failed to get splits by game: {e}")

    def cleanup_old_data(self, days_to_keep: int = 30) -> Dict[str, int]:
        """
        Clean up old data beyond the retention period.
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup statistics
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        stats = {"deleted_splits": 0, "deleted_games": 0, "errors": 0}

        try:
            # Delete old betting splits without explicit transaction
            try:
                with self.db_manager.get_cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM splits.raw_mlb_betting_splits WHERE last_updated < %s",
                        (cutoff_date,)
                    )
                    stats["deleted_splits"] = cursor.rowcount
            except Exception as e:
                stats["errors"] += 1
                self.logger.error("Failed to delete old splits", error=str(e))

            # Delete old games
            try:
                with self.db_manager.get_cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM splits.games WHERE game_datetime < %s",
                        (cutoff_date,)
                    )
                    stats["deleted_games"] = cursor.rowcount
            except Exception as e:
                stats["errors"] += 1
                self.logger.error("Failed to delete old games", error=str(e))

            self.logger.info("Completed data cleanup", stats=stats, cutoff_date=cutoff_date)
            return stats

        except Exception as e:
            self.logger.error("Failed to cleanup old data", error=str(e))
            raise DatabaseError(f"Failed to cleanup old data: {e}")

    def get_storage_statistics(self) -> Dict[str, any]:
        """
        Get statistics about stored data.
        
        Returns:
            Dictionary with storage statistics
        """
        try:
            stats = {}
            
            # Betting splits statistics
            with self.db_manager.get_cursor() as cursor:
                # Total splits - use explicit table name for PostgreSQL
                cursor.execute("SELECT COUNT(*) FROM splits.raw_mlb_betting_splits")
                result = cursor.fetchone()
                stats["total_splits"] = result[0] if result else 0
                
                # Splits by source
                cursor.execute("""
                    SELECT source, COUNT(*) 
                    FROM splits.raw_mlb_betting_splits 
                    GROUP BY source
                """)
                result = cursor.fetchall()
                stats["splits_by_source"] = {row[0]: row[1] for row in result}
                
                # Splits by type
                cursor.execute("""
                    SELECT split_type, COUNT(*) 
                    FROM splits.raw_mlb_betting_splits 
                    GROUP BY split_type
                """)
                result = cursor.fetchall()
                stats["splits_by_type"] = {row[0]: row[1] for row in result}
                
                # Recent activity (last 24 hours) - use PostgreSQL syntax
                cutoff = datetime.now() - timedelta(hours=24)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM splits.raw_mlb_betting_splits 
                    WHERE last_updated >= %s
                """, (cutoff,))
                result = cursor.fetchone()
                stats["recent_splits_24h"] = result[0] if result else 0
                
                # Total games
                cursor.execute("SELECT COUNT(*) FROM splits.games")
                result = cursor.fetchone()
                stats["total_games"] = result[0] if result else 0

            stats["last_updated"] = datetime.now().isoformat()
            return stats

        except Exception as e:
            self.logger.error("Failed to get storage statistics", error=str(e))
            raise DatabaseError(f"Failed to get storage statistics: {e}")

    def verify_data_integrity(self) -> Dict[str, any]:
        """
        Verify data integrity and consistency.
        
        Returns:
            Dictionary with integrity check results
        """
        try:
            results = {
                "checks_passed": 0,
                "checks_failed": 0,
                "warnings": [],
                "errors": []
            }

            with self.db_manager.get_cursor() as cursor:
                # Check for orphaned splits (splits without corresponding games)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM splits.raw_mlb_betting_splits bs
                    LEFT JOIN splits.games g ON bs.game_id = g.game_id
                    WHERE g.game_id IS NULL
                """)
                orphan_check = cursor.fetchone()
                
                orphaned_splits = orphan_check[0] if orphan_check else 0
                if orphaned_splits > 0:
                    results["warnings"].append(f"Found {orphaned_splits} orphaned splits")
                    results["checks_failed"] += 1
                else:
                    results["checks_passed"] += 1

                # Check for percentage validation issues
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM splits.raw_mlb_betting_splits
                    WHERE (home_or_over_bets_percentage < 0 OR home_or_over_bets_percentage > 100)
                    OR (away_or_under_bets_percentage < 0 OR away_or_under_bets_percentage > 100)
                """)
                invalid_percentages = cursor.fetchone()
                
                invalid_pct_count = invalid_percentages[0] if invalid_percentages else 0
                if invalid_pct_count > 0:
                    results["errors"].append(f"Found {invalid_pct_count} splits with invalid percentages")
                    results["checks_failed"] += 1
                else:
                    results["checks_passed"] += 1

                # Check for future game dates - use PostgreSQL syntax
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime > %s
                """, (datetime.now() + timedelta(days=30),))
                future_games = cursor.fetchone()
                
                future_count = future_games[0] if future_games else 0
                if future_count > 0:
                    results["warnings"].append(f"Found {future_count} splits with games >30 days in future")

            results["check_time"] = datetime.now().isoformat()
            results["overall_health"] = "healthy" if results["checks_failed"] == 0 else "issues_found"
            
            return results

        except Exception as e:
            self.logger.error("Failed to verify data integrity", error=str(e))
            raise DatabaseError(f"Failed to verify data integrity: {e}")


def get_data_persistence_service(db_manager: Optional[DatabaseManager] = None) -> DataPersistenceService:
    """Get a DataPersistenceService instance."""
    return DataPersistenceService(db_manager) 