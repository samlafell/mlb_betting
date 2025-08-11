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
from ..data.collection.base import CollectionStatus as BaseCollectionStatus, CollectionRequest, CollectorConfig, DataSource
from ..data.collection.unified_betting_lines_collector import UnifiedCollectionResult
from ..data.collection.registry import get_collector_instance, initialize_all_collectors
from .mlb_schedule_service import MLBGame, MLBScheduleService

# Refactored to work with the centralized collector registry system
# Supports Action Network, VSIN, SBD, and other available collectors

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
    game_date: datetime
    
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
    
    @property
    def has_valid_game_data(self) -> bool:
        """Check if task has valid game data for collection."""
        return self.mlb_game is not None and self.mlb_game.game_pk is not None


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
            "errors": len(self.global_errors),
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

    def __init__(
        self, config: BatchCollectionConfig = None, settings: UnifiedSettings = None
    ):
        self.config = config or BatchCollectionConfig()
        self.settings = settings or UnifiedSettings()
        self.logger = logger.bind(component="BatchCollectionService")

        # Initialize collector registry
        initialize_all_collectors()

        # Services
        self.schedule_service = MLBScheduleService(self.settings)
        
        # Available collectors
        self.available_collectors = {}
        self._initialize_collectors()

        # State
        self.current_batch: BatchCollectionResult | None = None
        self.is_running = False
        self.should_cancel = False

        # Checkpointing
        self.checkpoint_dir = Path(self.config.checkpoint_directory)
        self.checkpoint_dir.mkdir(exist_ok=True)
    
    def _initialize_collectors(self):
        """Initialize available collectors for batch collection."""
        collector_sources = [
            DataSource.ACTION_NETWORK,
            DataSource.VSIN, 
            DataSource.SBD,
            DataSource.SPORTS_BOOK_REVIEW,
            DataSource.MLB_STATS_API
        ]
        
        for source in collector_sources:
            try:
                # Try with settings first, then with None if it fails
                collector = None
                try:
                    collector = get_collector_instance(source, self.settings)
                except Exception as config_error:
                    self.logger.debug(f"Failed with settings for {source.value}: {config_error}")
                    try:
                        # Try with minimal configuration
                        collector = get_collector_instance(source, None)
                    except Exception as fallback_error:
                        self.logger.debug(f"Failed with fallback for {source.value}: {fallback_error}")
                
                if collector:
                    self.available_collectors[source] = collector
                    self.logger.debug(f"Initialized collector for {source.value}")
            except Exception as e:
                self.logger.warning(f"Failed to initialize collector for {source.value}: {e}")
        
        self.logger.info(f"Initialized {len(self.available_collectors)} collectors")

    async def collect_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        source: str = "action_network",
    ) -> BatchCollectionResult:
        """
        Collect betting lines for a date range.

        Args:
            start_date: Start date for collection
            end_date: End date for collection
            source: Data source (action_network, vsin, sbd, sports_book_review, mlb_stats_api)

        Returns:
            Batch collection result
        """
        try:
            # Validate date range
            if start_date > end_date:
                raise DataError("Start date must be before end date")
            
            # Validate data source
            source_key = None
            for ds in DataSource:
                if ds.value == source or ds.name.lower() == source.lower():
                    source_key = ds
                    break
            
            if source_key not in self.available_collectors:
                available_sources = [ds.value for ds in self.available_collectors.keys()]
                raise DataError(f"Data source '{source}' not available. Available sources: {available_sources}")

            # Create batch result
            batch_id = str(uuid.uuid4())
            self.current_batch = BatchCollectionResult(
                batch_id=batch_id,
                start_date=start_date,
                end_date=end_date,
                start_time=datetime.now(),
            )

            self.is_running = True
            self.should_cancel = False

            self.logger.info(
                "Starting batch collection",
                batch_id=batch_id,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                source=source,
            )

            # Phase 1: Discover games
            await self._discover_games(start_date, end_date)

            # Phase 2: Collect betting lines using selected collector
            await self._collect_betting_lines(source_key)

            # Complete batch
            self.current_batch.end_time = datetime.now()
            self.is_running = False

            self.logger.info(
                "Batch collection completed",
                batch_id=batch_id,
                duration=str(self.current_batch.duration),
                success_rate=f"{self.current_batch.success_rate:.1%}",
                total_records=self.current_batch.total_records_collected,
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
                    season_type="R",  # Regular season
                )

            # Create tasks for each game
            for game in games:
                task = GameCollectionTask(
                    task_id=str(uuid.uuid4()), 
                    mlb_game=game,
                    game_date=game.game_date if hasattr(game, 'game_date') else start_date
                )
                self.current_batch.tasks.append(task)

            self.current_batch.total_games = len(games)

            self.logger.info(
                f"Discovered {len(games)} games",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

        except Exception as e:
            self.logger.error("Error discovering games", error=str(e))
            raise DataError(f"Failed to discover games: {str(e)}")

    async def _collect_betting_lines(self, source_key: DataSource):
        """Collect betting lines using the specified collector."""
        try:
            self.logger.info("Collecting betting lines", source=source_key.value)

            # Get the collector for this source
            collector = self.available_collectors[source_key]

            # Filter tasks with valid game data
            valid_tasks = [
                task
                for task in self.current_batch.tasks
                if task.has_valid_game_data
            ]

            if not valid_tasks:
                self.logger.warning("No valid tasks to collect")
                return

            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.config.max_concurrent_collections)

            # Process tasks
            tasks = [self._collect_single_game(task, collector, source_key, semaphore) for task in valid_tasks]

            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)

            # Update statistics
            self._update_batch_statistics()

        except Exception as e:
            self.logger.error("Error collecting betting lines", error=str(e))
            raise DataError(f"Failed to collect betting lines: {str(e)}")

    async def _collect_single_game(
        self, task: GameCollectionTask, collector, source_key: DataSource, semaphore: asyncio.Semaphore
    ):
        """Collect betting lines for a single game."""
        async with semaphore:
            if self.should_cancel:
                return

            task.status = CollectionStatus.RUNNING
            task.start_time = datetime.now()

            for attempt in range(self.config.retry_attempts):
                try:
                    task.attempts = attempt + 1

                    # Create collection request
                    request = CollectionRequest(
                        source=source_key,
                        start_date=task.game_date,
                        end_date=task.game_date,
                        additional_params={
                            "game_pk": task.mlb_game.game_pk,
                            "game_id": task.mlb_game.game_pk,
                        }
                    )

                    # Collect data using the selected collector
                    collected_data = await collector.collect_data(request)

                    if collected_data and len(collected_data) > 0:
                        # Create successful result
                        task.collection_result = UnifiedCollectionResult(
                            status=BaseCollectionStatus.SUCCESS,
                            records_processed=1,
                            records_stored=len(collected_data),
                            message="Collection successful",
                        )
                        task.records_collected = len(collected_data)
                        task.status = CollectionStatus.COMPLETED

                        self.logger.debug(
                            f"Collected game {task.mlb_game.game_pk}",
                            records=task.records_collected,
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
                if (
                    self.current_batch.games_processed
                    % self.config.progress_update_interval
                    == 0
                ):
                    self._log_progress()

            # Checkpointing
            if self.config.enable_checkpointing:
                if (
                    self.current_batch.games_processed % self.config.checkpoint_interval
                    == 0
                ):
                    await self._save_checkpoint()

    def _update_batch_statistics(self):
        """Update batch statistics."""
        self.current_batch.games_successful = sum(
            1
            for task in self.current_batch.tasks
            if task.status == CollectionStatus.COMPLETED
        )

        self.current_batch.games_failed = sum(
            1
            for task in self.current_batch.tasks
            if task.status == CollectionStatus.FAILED
        )

        self.current_batch.games_skipped = sum(
            1
            for task in self.current_batch.tasks
            if not task.has_valid_game_data
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
            failed=self.current_batch.games_failed,
        )

    async def _save_checkpoint(self):
        """Save collection checkpoint."""
        try:
            if not self.current_batch:
                return

            checkpoint_file = (
                self.checkpoint_dir / f"batch_{self.current_batch.batch_id}.json"
            )

            # Serialize batch data
            checkpoint_data = {
                "batch_id": self.current_batch.batch_id,
                "start_date": self.current_batch.start_date.isoformat(),
                "end_date": self.current_batch.end_date.isoformat(),
                "start_time": self.current_batch.start_time.isoformat(),
                "games_processed": self.current_batch.games_processed,
                "games_successful": self.current_batch.games_successful,
                "games_failed": self.current_batch.games_failed,
                "total_records_collected": self.current_batch.total_records_collected,
            }

            with open(checkpoint_file, "w") as f:
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
            "progress": self.current_batch.games_processed
            / self.current_batch.total_games
            if self.current_batch.total_games > 0
            else 0,
            "games_processed": self.current_batch.games_processed,
            "games_successful": self.current_batch.games_successful,
            "games_failed": self.current_batch.games_failed,
            "total_records": self.current_batch.total_records_collected,
            "duration": str(datetime.now() - self.current_batch.start_time),
        }

    async def get_failed_games(self) -> list[GameCollectionTask]:
        """Get list of failed game collections."""
        if not self.current_batch:
            return []

        return [
            task
            for task in self.current_batch.tasks
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
        tasks = [self._collect_single_game(task, semaphore) for task in failed_tasks]

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
            enable_checkpointing=True,
        )

        # Create service
        service = BatchCollectionService(config)

        # Collect data for date range
        start_date = datetime(2025, 3, 15)
        end_date = datetime(2025, 3, 20)

        try:
            result = await service.collect_date_range(start_date, end_date)

            print("Batch Collection Results:")
            print(
                f"  Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )
            print(f"  Duration: {result.duration}")
            print(f"  Games Processed: {result.games_processed}")
            print(f"  Success Rate: {result.success_rate:.1%}")
            print(f"  Total Records: {result.total_records_collected}")

        except Exception as e:
            print(f"Collection failed: {e}")

    asyncio.run(main())
