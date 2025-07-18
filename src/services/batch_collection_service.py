#!/usr/bin/env python3
"""
Batch Collection Service

Provides comprehensive batch collection capabilities for historical line movement data.
Supports date-range collections, parallel processing, progress tracking, and automatic
retry mechanisms for failed collections.
"""

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

from ..core.config import UnifiedSettings
from ..core.exceptions import DataError
from ..data.collection.base import CollectionStatus as BaseCollectionStatus
from ..data.collection.unified_betting_lines_collector import UnifiedCollectionResult
from .mlb_schedule_service import MLBGame, MLBScheduleService

# Note: This service was originally designed for SBR (SportsbookReview) historical
# line movement collection, but SBR collectors have been removed. The service may
# need refactoring for other data sources like Action Network, VSIN, or SBD.

logger = structlog.get_logger(__name__)


class CollectionStatus(Enum):
    """Collection status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


@dataclass
class BatchCollectionConfig:
    """Configuration for batch collection operations."""
    max_concurrent_collections: int = 5
    retry_attempts: int = 3
    retry_delay_seconds: float = 60.0
    rate_limit_delay: float = 1.0

    # Progress tracking
    enable_progress_tracking: bool = True
    progress_update_interval: int = 10  # games

    # Checkpointing
    enable_checkpointing: bool = True
    checkpoint_interval: int = 50  # games
    checkpoint_directory: str = "checkpoints"

    # Error handling
    fail_fast: bool = False
    skip_failed_games: bool = True

    # Data quality
    minimum_confidence_score: float = 0.7
    enable_data_validation: bool = True

    # Collector selection
    use_enhanced_collector: bool = True


@dataclass
class GameCollectionTask:
    """Individual game collection task."""
    task_id: str
    mlb_game: MLBGame
    # Note: SBR mapping removed with SBR collector cleanup
    # sbr_mapping: Optional[OptimizedSBRGameMapping] = None

    status: CollectionStatus = CollectionStatus.PENDING
    attempts: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None

    # Results
    collection_result: UnifiedCollectionResult | None = None
    records_collected: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def duration(self) -> timedelta | None:
        """Get task duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self.status in [CollectionStatus.COMPLETED, CollectionStatus.FAILED]


@dataclass
class BatchCollectionResult:
    """Result of batch collection operation."""
    batch_id: str
    start_date: datetime
    end_date: datetime

    # Timing
    start_time: datetime
    end_time: datetime | None = None

    # Statistics
    total_games: int = 0
    games_processed: int = 0
    games_successful: int = 0
    games_failed: int = 0
    games_skipped: int = 0

    total_records_collected: int = 0

    # Tasks
    tasks: list[GameCollectionTask] = field(default_factory=list)

    # Errors
    global_errors: list[str] = field(default_factory=list)

    @property
    def duration(self) -> timedelta | None:
        """Get batch duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.games_processed == 0:
            return 0.0
        return self.games_successful / self.games_processed

    @property
    def is_completed(self) -> bool:
        """Check if batch is completed."""
        return self.end_time is not None

    def get_summary(self) -> dict[str, Any]:
        """Get batch summary."""
        return {
            "batch_id": self.batch_id,
            "date_range": f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}",
            "duration": str(self.duration) if self.duration else None,
            "total_games": self.total_games,
            "games_processed": self.games_processed,
            "games_successful": self.games_successful,
            "games_failed": self.games_failed,
            "games_skipped": self.games_skipped,
            "success_rate": f"{self.success_rate:.1%}",
            "total_records": self.total_records_collected,
            "errors": len(self.global_errors)
        }


class BatchCollectionService:
    """
    Batch Collection Service for historical line movement data.
    
    Provides comprehensive batch collection capabilities including:
    - Date-range game discovery
    - SBR game ID resolution
    - Parallel collection processing
    - Progress tracking and checkpointing
    - Error handling and retry mechanisms
    """

    def __init__(self, config: BatchCollectionConfig = None, settings: UnifiedSettings = None):
        self.config = config or BatchCollectionConfig()
        self.settings = settings or UnifiedSettings()
        self.logger = logger.bind(component="BatchCollectionService")

        # Services
        self.schedule_service = MLBScheduleService(self.settings)
        # Note: SBR-specific services removed - game_resolver, sbr_collector, legacy_collector
        # This service needs refactoring for other data sources

        # State
        self.current_batch: BatchCollectionResult | None = None
        self.is_running = False
        self.should_cancel = False

        # Checkpointing
        self.checkpoint_dir = Path(self.config.checkpoint_directory)
        self.checkpoint_dir.mkdir(exist_ok=True)

    async def collect_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        source: str = "sports_betting_report"
    ) -> BatchCollectionResult:
        """
        Collect betting lines for a date range.
        
        Args:
            start_date: Start date for collection
            end_date: End date for collection
            source: Data source (currently only SBR supported)
            
        Returns:
            Batch collection result
        """
        try:
            # Validate date range
            if start_date > end_date:
                raise DataError("Start date must be before end date")

            # Create batch result
            batch_id = str(uuid.uuid4())
            self.current_batch = BatchCollectionResult(
                batch_id=batch_id,
                start_date=start_date,
                end_date=end_date,
                start_time=datetime.now()
            )

            self.is_running = True
            self.should_cancel = False

            self.logger.info(
                "Starting batch collection",
                batch_id=batch_id,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )

            # Phase 1: Discover games
            await self._discover_games(start_date, end_date)

            # Phase 2: Resolve SBR game IDs
            await self._resolve_game_ids()

            # Phase 3: Collect betting lines
            await self._collect_betting_lines()

            # Complete batch
            self.current_batch.end_time = datetime.now()
            self.is_running = False

            self.logger.info(
                "Batch collection completed",
                batch_id=batch_id,
                duration=str(self.current_batch.duration),
                success_rate=f"{self.current_batch.success_rate:.1%}",
                total_records=self.current_batch.total_records_collected
            )

            return self.current_batch

        except Exception as e:
            self.logger.error("Batch collection failed", error=str(e))
            if self.current_batch:
                self.current_batch.global_errors.append(str(e))
                self.current_batch.end_time = datetime.now()
            self.is_running = False
            raise DataError(f"Batch collection failed: {str(e)}")

    async def _discover_games(self, start_date: datetime, end_date: datetime):
        """Discover MLB games in date range."""
        try:
            self.logger.info("Discovering games in date range")

            async with self.schedule_service:
                games = await self.schedule_service.get_games_by_date_range(
                    start_date=start_date,
                    end_date=end_date,
                    season_type="R"  # Regular season
                )

            # Create tasks for each game
            for game in games:
                task = GameCollectionTask(
                    task_id=str(uuid.uuid4()),
                    mlb_game=game
                )
                self.current_batch.tasks.append(task)

            self.current_batch.total_games = len(games)

            self.logger.info(
                f"Discovered {len(games)} games",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )

        except Exception as e:
            self.logger.error("Error discovering games", error=str(e))
            raise DataError(f"Failed to discover games: {str(e)}")

    async def _resolve_game_ids(self):
        """Resolve SBR game IDs for discovered games using optimized bulk resolution."""
        try:
            self.logger.info("Resolving SBR game IDs with optimized resolver")

            async with self.game_resolver:
                # Load existing cache if available
                cache_file = self.checkpoint_dir / "game_id_cache.json"
                if cache_file.exists():
                    await self.game_resolver.load_cache_from_file(str(cache_file))

                # Get all games for bulk resolution
                games = [task.mlb_game for task in self.current_batch.tasks]

                # Use optimized bulk resolution (much faster than individual calls)
                mappings = await self.game_resolver.resolve_multiple_games_optimized(games)

                # Update tasks with mappings
                resolved_count = 0
                for task in self.current_batch.tasks:
                    if task.mlb_game.game_pk in mappings:
                        task.sbr_mapping = mappings[task.mlb_game.game_pk]
                        resolved_count += 1

                # Save cache
                await self.game_resolver.save_cache_to_file(str(cache_file))

            self.logger.info(
                f"Resolved {resolved_count} of {len(self.current_batch.tasks)} games",
                resolution_rate=f"{resolved_count / len(self.current_batch.tasks):.1%}"
            )

        except Exception as e:
            self.logger.error("Error resolving game IDs", error=str(e))
            raise DataError(f"Failed to resolve game IDs: {str(e)}")

    async def _collect_betting_lines(self):
        """Collect betting lines for resolved games."""
        try:
            self.logger.info("Collecting betting lines")

            # Filter tasks with valid mappings
            valid_tasks = [
                task for task in self.current_batch.tasks
                if task.sbr_mapping and task.sbr_mapping.is_high_confidence
            ]

            if not valid_tasks:
                self.logger.warning("No valid tasks to collect")
                return

            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.config.max_concurrent_collections)

            # Process tasks
            tasks = [
                self._collect_single_game(task, semaphore)
                for task in valid_tasks
            ]

            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)

            # Update statistics
            self._update_batch_statistics()

        except Exception as e:
            self.logger.error("Error collecting betting lines", error=str(e))
            raise DataError(f"Failed to collect betting lines: {str(e)}")

    async def _collect_single_game(self, task: GameCollectionTask, semaphore: asyncio.Semaphore):
        """Collect betting lines for a single game."""
        async with semaphore:
            if self.should_cancel:
                return

            task.status = CollectionStatus.RUNNING
            task.start_time = datetime.now()

            for attempt in range(self.config.retry_attempts):
                try:
                    task.attempts = attempt + 1

                    # Collect game lines using configured collector
                    if self.config.use_enhanced_collector and hasattr(self, 'legacy_collector') and self.legacy_collector:
                        # Try enhanced collector with fallback
                        try:
                            self.logger.debug(f"Trying enhanced collector for game {task.sbr_mapping.sbr_game_id}")
                            records_stored = await self.sbr_collector.collect_game_lines_async(task.sbr_mapping.sbr_game_id)
                            self.logger.debug(f"Enhanced collector succeeded: {records_stored} records")
                        except Exception as enhanced_error:
                            # Fallback to legacy collector
                            self.logger.warning(
                                f"Enhanced collector failed for game {task.sbr_mapping.sbr_game_id}, trying legacy collector",
                                error=str(enhanced_error),
                                error_type=type(enhanced_error).__name__
                            )
                            records_stored = await self.legacy_collector.collect_game_lines_async(task.sbr_mapping.sbr_game_id)
                            self.logger.debug(f"Legacy collector result: {records_stored} records")
                    else:
                        # Use primary collector only
                        records_stored = await self.sbr_collector.collect_game_lines_async(task.sbr_mapping.sbr_game_id)

                    if records_stored > 0:
                        # Create successful result
                        task.collection_result = UnifiedCollectionResult(
                            status=BaseCollectionStatus.SUCCESS,
                            records_processed=1,
                            records_stored=records_stored,
                            message="Collection successful"
                        )
                        task.records_collected = records_stored
                        task.status = CollectionStatus.COMPLETED

                        self.logger.debug(
                            f"Collected game {task.mlb_game.game_pk}",
                            sbr_game_id=task.sbr_mapping.sbr_game_id,
                            records=task.records_collected
                        )

                        break
                    else:
                        error_msg = "Collection failed: No records collected"
                        task.errors.append(error_msg)

                        if attempt < self.config.retry_attempts - 1:
                            task.status = CollectionStatus.RETRYING
                            await asyncio.sleep(self.config.retry_delay_seconds)
                        else:
                            task.status = CollectionStatus.FAILED

                except Exception as e:
                    error_msg = f"Attempt {attempt + 1} failed: {str(e)}"
                    task.errors.append(error_msg)

                    if attempt < self.config.retry_attempts - 1:
                        task.status = CollectionStatus.RETRYING
                        await asyncio.sleep(self.config.retry_delay_seconds)
                    else:
                        task.status = CollectionStatus.FAILED

                # Rate limiting
                await asyncio.sleep(self.config.rate_limit_delay)

            task.end_time = datetime.now()

            # Update progress
            self.current_batch.games_processed += 1

            if self.config.enable_progress_tracking:
                if self.current_batch.games_processed % self.config.progress_update_interval == 0:
                    self._log_progress()

            # Checkpointing
            if self.config.enable_checkpointing:
                if self.current_batch.games_processed % self.config.checkpoint_interval == 0:
                    await self._save_checkpoint()

    def _update_batch_statistics(self):
        """Update batch statistics."""
        self.current_batch.games_successful = sum(
            1 for task in self.current_batch.tasks
            if task.status == CollectionStatus.COMPLETED
        )

        self.current_batch.games_failed = sum(
            1 for task in self.current_batch.tasks
            if task.status == CollectionStatus.FAILED
        )

        self.current_batch.games_skipped = sum(
            1 for task in self.current_batch.tasks
            if not task.sbr_mapping or not task.sbr_mapping.is_high_confidence
        )

        self.current_batch.total_records_collected = sum(
            task.records_collected for task in self.current_batch.tasks
        )

    def _log_progress(self):
        """Log collection progress."""
        if not self.current_batch:
            return

        progress = self.current_batch.games_processed / self.current_batch.total_games

        self.logger.info(
            f"Collection progress: {progress:.1%}",
            processed=self.current_batch.games_processed,
            total=self.current_batch.total_games,
            successful=self.current_batch.games_successful,
            failed=self.current_batch.games_failed
        )

    async def _save_checkpoint(self):
        """Save collection checkpoint."""
        try:
            if not self.current_batch:
                return

            checkpoint_file = self.checkpoint_dir / f"batch_{self.current_batch.batch_id}.json"

            # Serialize batch data
            checkpoint_data = {
                "batch_id": self.current_batch.batch_id,
                "start_date": self.current_batch.start_date.isoformat(),
                "end_date": self.current_batch.end_date.isoformat(),
                "start_time": self.current_batch.start_time.isoformat(),
                "games_processed": self.current_batch.games_processed,
                "games_successful": self.current_batch.games_successful,
                "games_failed": self.current_batch.games_failed,
                "total_records_collected": self.current_batch.total_records_collected
            }

            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

            self.logger.debug(f"Checkpoint saved: {checkpoint_file}")

        except Exception as e:
            self.logger.error("Error saving checkpoint", error=str(e))

    async def cancel_collection(self):
        """Cancel running collection."""
        if self.is_running:
            self.should_cancel = True
            self.logger.info("Cancellation requested")

    def get_current_status(self) -> dict[str, Any] | None:
        """Get current collection status."""
        if not self.current_batch:
            return None

        return {
            "batch_id": self.current_batch.batch_id,
            "is_running": self.is_running,
            "total_games": self.current_batch.total_games,
            "progress": self.current_batch.games_processed / self.current_batch.total_games if self.current_batch.total_games > 0 else 0,
            "games_processed": self.current_batch.games_processed,
            "games_successful": self.current_batch.games_successful,
            "games_failed": self.current_batch.games_failed,
            "total_records": self.current_batch.total_records_collected,
            "duration": str(datetime.now() - self.current_batch.start_time)
        }

    async def get_failed_games(self) -> list[GameCollectionTask]:
        """Get list of failed game collections."""
        if not self.current_batch:
            return []

        return [
            task for task in self.current_batch.tasks
            if task.status == CollectionStatus.FAILED
        ]

    async def retry_failed_games(self) -> BatchCollectionResult:
        """Retry failed game collections."""
        if not self.current_batch:
            raise DataError("No batch to retry")

        failed_tasks = await self.get_failed_games()

        if not failed_tasks:
            self.logger.info("No failed games to retry")
            return self.current_batch

        self.logger.info(f"Retrying {len(failed_tasks)} failed games")

        # Reset failed tasks
        for task in failed_tasks:
            task.status = CollectionStatus.PENDING
            task.attempts = 0
            task.errors.clear()

        # Process retry
        semaphore = asyncio.Semaphore(self.config.max_concurrent_collections)
        tasks = [
            self._collect_single_game(task, semaphore)
            for task in failed_tasks
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Update statistics
        self._update_batch_statistics()

        return self.current_batch


# Example usage
if __name__ == "__main__":
    async def main():
        # Configure batch collection
        config = BatchCollectionConfig(
            max_concurrent_collections=3,
            retry_attempts=2,
            enable_progress_tracking=True,
            enable_checkpointing=True
        )

        # Create service
        service = BatchCollectionService(config)

        # Collect data for date range
        start_date = datetime(2025, 3, 15)
        end_date = datetime(2025, 3, 20)

        try:
            result = await service.collect_date_range(start_date, end_date)

            print("Batch Collection Results:")
            print(f"  Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            print(f"  Duration: {result.duration}")
            print(f"  Games Processed: {result.games_processed}")
            print(f"  Success Rate: {result.success_rate:.1%}")
            print(f"  Total Records: {result.total_records_collected}")

        except Exception as e:
            print(f"Collection failed: {e}")

    asyncio.run(main())
