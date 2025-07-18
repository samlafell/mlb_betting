"""
Data Synchronization Service

Addresses the critical issue: "An odds line from Source A at T:00 and betting 
percentages from Source B at T:03 might lead to incorrect conclusions."

This service ensures data from multiple sources is properly time-aligned before 
analysis, preventing erroneous conclusions from timing mismatches.

Key Features:
- Synchronized collection windows for multiple sources
- Time-alignment validation before analysis
- Timing anomaly detection and reporting
- Quality scoring for synchronization effectiveness
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from ..core.config import get_settings
from ..core.logging import get_logger

logger = get_logger(__name__)

try:
    from ..core.timing import (
        DataSynchronizer,
        SynchronizationWindow,
        TimestampedData,
        TimingMetrics,
        calculate_synchronization_quality,
        get_est_now,
        precise_timestamp,
    )
except ImportError as e:
    logger.critical(
        "Critical timing module dependencies missing",
        error=str(e),
        required_modules=["DataSynchronizer", "TimestampedData", "SynchronizationWindow"]
    )
    raise ImportError(
        "Critical dependency failure: timing module components required for data synchronization. "
        f"Missing: {str(e)}. Ensure src.core.timing module is properly implemented."
    ) from e
from ..data.collection.base import CollectionResult
from ..data.collection.orchestrator import CollectionOrchestrator


@dataclass
class SynchronizedDataSet:
    """Container for synchronized data from multiple sources."""

    sync_id: str
    window_center_est: datetime
    window_seconds: float
    sources: list[str]
    data: dict[str, Any]
    quality_score: float
    timing_anomalies: list[str] = field(default_factory=list)
    created_at_est: datetime = field(default_factory=precise_timestamp)

    @property
    def is_high_quality(self) -> bool:
        """Check if synchronization quality is acceptable for analysis."""
        return self.quality_score >= 0.7 and len(self.timing_anomalies) == 0

    @property
    def time_spread_seconds(self) -> float:
        """Calculate time spread across all data sources."""
        timestamps = []
        for source_data in self.data.values():
            if hasattr(source_data, 'collected_at_est'):
                timestamps.append(source_data.collected_at_est)
            elif hasattr(source_data, 'timestamp'):
                timestamps.append(source_data.timestamp)

        if len(timestamps) < 2:
            return 0.0

        return (max(timestamps) - min(timestamps)).total_seconds()


class DataSynchronizationService:
    """
    Service for coordinating synchronized data collection across multiple sources.
    
    Ensures that analysis models receive time-aligned data to prevent incorrect
    conclusions from timing mismatches between sources.
    """

    def __init__(self, orchestrator: CollectionOrchestrator):
        self.orchestrator = orchestrator
        self.settings = get_settings()
        self.logger = logger.bind(service="DataSynchronizationService")

        # Core synchronizer
        self.synchronizer = DataSynchronizer(
            default_window_seconds=60.0,  # 1-minute default window
            max_skew_seconds=300.0,       # 5-minute max acceptable skew
            require_all_sources=False
        )

        # Timing metrics
        self.timing_metrics = TimingMetrics()

        # Active synchronization windows
        self.active_windows: dict[str, SynchronizationWindow] = {}

        # Synchronized data cache
        self.synchronized_data_cache: dict[str, SynchronizedDataSet] = {}

        self.logger.info("Data synchronization service initialized")

    async def collect_synchronized_data(
        self,
        sources: list[str],
        window_seconds: float = 60.0,
        max_wait_seconds: float = 120.0,
        require_all_sources: bool = False
    ) -> SynchronizedDataSet | None:
        """
        Collect data from multiple sources within a synchronized time window.
        
        This is the core method that addresses timing mismatches between sources.
        
        Args:
            sources: List of data sources to synchronize
            window_seconds: Size of synchronization window
            max_wait_seconds: Maximum time to wait for all sources
            require_all_sources: Whether all sources must provide data
            
        Returns:
            Synchronized data set or None if synchronization fails
        """
        sync_id = str(uuid4())
        center_time = get_est_now()

        self.logger.info(
            "Starting synchronized data collection",
            sync_id=sync_id,
            sources=sources,
            window_seconds=window_seconds,
            center_time=center_time.isoformat()
        )

        # Create synchronization window
        window = SynchronizationWindow(
            center_time_est=center_time,
            window_seconds=window_seconds,
            max_acceptable_skew_seconds=300.0
        )

        self.active_windows[sync_id] = window

        try:
            # Trigger parallel collection from all sources
            collection_tasks = []
            for source in sources:
                task = asyncio.create_task(
                    self._collect_from_source(source, sync_id)
                )
                collection_tasks.append(task)

            # Wait for collections with timeout
            collected_data: dict[str, CollectionResult] = {}

            # Wait for all tasks to complete or timeout
            done, pending = await asyncio.wait(
                collection_tasks,
                timeout=max_wait_seconds,
                return_when=asyncio.ALL_COMPLETED if require_all_sources else asyncio.FIRST_COMPLETED
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()

            # Collect results
            for task in done:
                try:
                    source, result = await task
                    if result and result.success:
                        collected_data[source] = result

                        # Add to synchronizer buffer
                        for item in result.data:
                            self.synchronizer.add_data(
                                data=item,
                                source=source,
                                collected_at_est=result.timestamp,
                                sequence_id=sync_id
                            )
                    else:
                        self.logger.warning(
                            "Collection failed for source",
                            source=source,
                            sync_id=sync_id
                        )
                except Exception as e:
                    self.logger.error(
                        "Error processing collection result",
                        error=str(e),
                        sync_id=sync_id
                    )

            # Check if we have minimum required data
            if require_all_sources and len(collected_data) < len(sources):
                missing_sources = set(sources) - set(collected_data.keys())
                self.logger.warning(
                    "Missing required sources for synchronization",
                    missing_sources=list(missing_sources),
                    sync_id=sync_id
                )
                return None

            if len(collected_data) < 2:
                self.logger.warning(
                    "Insufficient sources for synchronization",
                    collected_sources=list(collected_data.keys()),
                    sync_id=sync_id
                )
                return None

            # Find time-aligned data
            timestamped_data = {}
            for source, result in collected_data.items():
                timestamped_data[source] = [
                    TimestampedData(
                        data=item,
                        collected_at_est=result.timestamp,
                        source=source,
                        source_sequence_id=sync_id
                    )
                    for item in result.data
                ]

            best_alignment = self.synchronizer.find_best_time_alignment(
                timestamped_data,
                max_time_diff_seconds=180.0  # 3-minute max difference
            )

            if not best_alignment:
                self.logger.warning(
                    "Could not find acceptable time alignment",
                    sync_id=sync_id,
                    sources=list(collected_data.keys())
                )
                return None

            # Calculate synchronization quality
            timestamps = [item.collected_at_est for item in best_alignment.values()]
            quality_score = calculate_synchronization_quality(timestamps, window_seconds)

            # Create synchronized data set
            sync_data = SynchronizedDataSet(
                sync_id=sync_id,
                window_center_est=center_time,
                window_seconds=window_seconds,
                sources=list(best_alignment.keys()),
                data={source: item.data for source, item in best_alignment.items()},
                quality_score=quality_score
            )

            # Check for timing anomalies
            time_spread = sync_data.time_spread_seconds
            if time_spread > 180.0:  # 3-minute threshold
                anomaly = f"Large time spread: {time_spread:.1f} seconds"
                sync_data.timing_anomalies.append(anomaly)
                self.timing_metrics.add_timing_anomaly(
                    source="MULTIPLE",
                    description=anomaly,
                    timestamp=center_time
                )

            # Update metrics
            self.timing_metrics.total_data_points += len(best_alignment)
            self.timing_metrics.sources_count = len(best_alignment)
            self.timing_metrics.time_span_seconds = time_spread

            if quality_score >= 0.7:
                self.timing_metrics.synchronization_success_rate += 1

            # Cache the synchronized data
            self.synchronized_data_cache[sync_id] = sync_data

            self.logger.info(
                "Synchronized data collection completed",
                sync_id=sync_id,
                sources=sync_data.sources,
                quality_score=quality_score,
                time_spread_seconds=time_spread,
                is_high_quality=sync_data.is_high_quality
            )

            return sync_data

        except Exception as e:
            self.logger.error(
                "Synchronized collection failed",
                sync_id=sync_id,
                error=str(e),
                exc_info=True
            )
            return None

        finally:
            # Cleanup
            if sync_id in self.active_windows:
                del self.active_windows[sync_id]

    async def _collect_from_source(
        self,
        source: str,
        sync_id: str
    ) -> tuple[str, CollectionResult | None]:
        """Collect data from a single source."""
        try:
            # Use the orchestrator to collect from the specific source
            result = await self.orchestrator.collect_source(source)

            # Add synchronization metadata
            if result and result.success:
                result.set_synchronization_metadata(
                    window_id=sync_id,
                    quality_score=1.0,  # Will be updated after synchronization
                    is_synchronized=True
                )

            return source, result

        except Exception as e:
            self.logger.error(
                "Source collection failed",
                source=source,
                sync_id=sync_id,
                error=str(e)
            )
            return source, None

    def get_synchronized_data_for_analysis(
        self,
        sources: list[str],
        max_age_seconds: float = 300.0
    ) -> dict[str, Any] | None:
        """
        Get the most recent synchronized data for analysis.
        
        This is a convenience method for analysis models to get time-aligned data.
        
        Args:
            sources: Required sources for analysis
            max_age_seconds: Maximum age of data to consider
            
        Returns:
            Synchronized data dictionary or None
        """
        cutoff_time = get_est_now() - timedelta(seconds=max_age_seconds)

        # Find the most recent high-quality synchronized data
        best_data = None
        best_timestamp = None

        for sync_data in self.synchronized_data_cache.values():
            if sync_data.created_at_est < cutoff_time:
                continue

            if not sync_data.is_high_quality:
                continue

            # Check if it has all required sources
            if not all(source in sync_data.sources for source in sources):
                continue

            if best_timestamp is None or sync_data.created_at_est > best_timestamp:
                best_data = sync_data
                best_timestamp = sync_data.created_at_est

        if best_data:
            self.logger.debug(
                "Retrieved synchronized data for analysis",
                sync_id=best_data.sync_id,
                sources=best_data.sources,
                quality_score=best_data.quality_score,
                age_seconds=(get_est_now() - best_data.created_at_est).total_seconds()
            )
            return best_data.data

        self.logger.warning(
            "No suitable synchronized data found for analysis",
            required_sources=sources,
            max_age_seconds=max_age_seconds
        )
        return None

    def get_timing_metrics(self) -> TimingMetrics:
        """Get current timing and synchronization metrics."""
        return self.timing_metrics

    def cleanup_old_data(self, max_age_seconds: float = 3600.0) -> int:
        """Remove old synchronized data from cache."""
        cutoff_time = get_est_now() - timedelta(seconds=max_age_seconds)

        # Remove old entries
        expired_keys = [
            sync_id for sync_id, sync_data in self.synchronized_data_cache.items()
            if sync_data.created_at_est < cutoff_time
        ]

        for sync_id in expired_keys:
            del self.synchronized_data_cache[sync_id]

        # Also cleanup the synchronizer buffer
        removed_from_buffer = self.synchronizer.cleanup_old_data(max_age_seconds)

        removed_count = len(expired_keys)

        if removed_count > 0:
            self.logger.info(
                "Cleaned up old synchronized data",
                removed_cache_entries=removed_count,
                removed_buffer_items=removed_from_buffer,
                remaining_cache_entries=len(self.synchronized_data_cache)
            )

        return removed_count

    async def validate_timing_consistency(
        self,
        data_sources: dict[str, list[Any]],
        max_acceptable_skew_seconds: float = 180.0
    ) -> tuple[bool, list[str]]:
        """
        Validate that data from multiple sources has acceptable timing consistency.
        
        Args:
            data_sources: Dictionary mapping source names to data lists
            max_acceptable_skew_seconds: Maximum acceptable time difference
            
        Returns:
            Tuple of (is_consistent, list_of_timing_issues)
        """
        timing_issues = []

        if len(data_sources) < 2:
            return True, timing_issues

        # Extract timestamps from each source
        source_timestamps = {}
        for source, data_list in data_sources.items():
            timestamps = []
            for item in data_list:
                timestamp = getattr(item, 'collected_at_est', None) or getattr(item, 'timestamp', None)
                if timestamp:
                    timestamps.append(timestamp)

            if timestamps:
                source_timestamps[source] = timestamps

        if len(source_timestamps) < 2:
            timing_issues.append("Insufficient timestamp data for validation")
            return False, timing_issues

        # Compare timestamps between sources
        sources = list(source_timestamps.keys())
        for i, source_a in enumerate(sources):
            for source_b in sources[i+1:]:
                timestamps_a = source_timestamps[source_a]
                timestamps_b = source_timestamps[source_b]

                # Find the minimum time difference between any timestamps
                min_diff = float('inf')
                for ts_a in timestamps_a:
                    for ts_b in timestamps_b:
                        diff = abs((ts_a - ts_b).total_seconds())
                        min_diff = min(min_diff, diff)

                if min_diff > max_acceptable_skew_seconds:
                    issue = (
                        f"Large time skew between {source_a} and {source_b}: "
                        f"{min_diff:.1f} seconds (max: {max_acceptable_skew_seconds})"
                    )
                    timing_issues.append(issue)

        is_consistent = len(timing_issues) == 0

        if not is_consistent:
            self.logger.warning(
                "Timing consistency validation failed",
                issues=timing_issues,
                sources=list(data_sources.keys())
            )

        return is_consistent, timing_issues


# Global service instance
_synchronization_service: DataSynchronizationService | None = None


def get_synchronization_service(orchestrator: CollectionOrchestrator | None = None) -> DataSynchronizationService:
    """Get or create the global data synchronization service."""
    global _synchronization_service

    if _synchronization_service is None:
        if orchestrator is None:
            # Create a default orchestrator if none provided
            from ..data.collection.orchestrator import CollectionOrchestrator
            orchestrator = CollectionOrchestrator()

        _synchronization_service = DataSynchronizationService(orchestrator)

    return _synchronization_service


# Convenience function for analysis models
async def get_time_aligned_data(
    sources: list[str],
    max_age_seconds: float = 300.0,
    window_seconds: float = 60.0
) -> dict[str, Any] | None:
    """
    Convenience function to get time-aligned data for analysis.
    
    This function directly addresses the core issue:
    "An odds line from Source A at T:00 and betting percentages from Source B at T:03
    might lead to incorrect conclusions."
    
    Args:
        sources: List of data sources to synchronize
        max_age_seconds: Maximum age of acceptable data
        window_seconds: Synchronization window size
        
    Returns:
        Time-aligned data dictionary or None
    """
    service = get_synchronization_service()

    # First try to get cached synchronized data
    sync_data = service.get_synchronized_data_for_analysis(sources, max_age_seconds)

    if sync_data:
        return sync_data

    # If no cached data, trigger new synchronized collection
    logger.info(
        "No cached synchronized data available, triggering new collection",
        sources=sources
    )

    new_sync_data = await service.collect_synchronized_data(
        sources=sources,
        window_seconds=window_seconds,
        max_wait_seconds=120.0,
        require_all_sources=False
    )

    if new_sync_data and new_sync_data.is_high_quality:
        return new_sync_data.data

    return None
