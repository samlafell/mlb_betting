"""
Unified Timestamping and Data Synchronization Utilities

Provides precise EST timestamping with data synchronization logic to handle
time mismatches between multiple data sources. Addresses the critical issue
of data from different sources being collected at slightly different times.

Key Features:
- Precise EST timestamping with microsecond accuracy (EST-only per project requirements)
- Data synchronization windows for correlating data across sources
- Time-alignment logic for analysis models
- Consistent EST timezone handling throughout
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import pytz
from pydantic import BaseModel, Field

# Timezone constants - EST only per project requirements
EST = pytz.timezone('US/Eastern')


@dataclass
class TimestampedData:
    """Container for data with precise timing information."""
    
    data: Any
    collected_at_est: datetime
    source: str
    source_sequence_id: Optional[str] = None  # For ordering within source
    
    @property 
    def age_seconds(self) -> float:
        """Get age of data in seconds."""
        return (get_est_now() - self.collected_at_est).total_seconds()


@dataclass
class SynchronizationWindow:
    """Defines a time window for data synchronization."""
    
    center_time_est: datetime
    window_seconds: float = 60.0  # Default 1-minute window
    max_acceptable_skew_seconds: float = 300.0  # 5 minutes max skew
    
    @property
    def start_time_est(self) -> datetime:
        """Window start time."""
        delta = timedelta(seconds=self.window_seconds / 2)
        return self.center_time_est - delta
    
    @property
    def end_time_est(self) -> datetime:
        """Window end time."""
        delta = timedelta(seconds=self.window_seconds / 2)
        return self.center_time_est + delta
    
    def contains(self, timestamp: datetime) -> bool:
        """Check if timestamp falls within this window."""
        return self.start_time_est <= timestamp <= self.end_time_est
    
    def is_acceptable_skew(self, timestamp: datetime) -> bool:
        """Check if timestamp is within acceptable skew range."""
        skew = abs((timestamp - self.center_time_est).total_seconds())
        return skew <= self.max_acceptable_skew_seconds


class DataSynchronizer:
    """
    Handles synchronization of data from multiple sources with different timing.
    
    Key Features:
    - Groups data by time windows
    - Handles timing mismatches between sources
    - Provides synchronized data sets for analysis
    - Detects and reports timing anomalies
    """
    
    def __init__(
        self,
        default_window_seconds: float = 60.0,
        max_skew_seconds: float = 300.0,
        require_all_sources: bool = False
    ):
        self.default_window_seconds = default_window_seconds
        self.max_skew_seconds = max_skew_seconds
        self.require_all_sources = require_all_sources
        self.data_buffer: List[TimestampedData] = []
        
    def add_data(
        self,
        data: Any,
        source: str,
        collected_at_est: Optional[datetime] = None,
        sequence_id: Optional[str] = None
    ) -> TimestampedData:
        """Add timestamped data to synchronization buffer."""
        if collected_at_est is None:
            collected_at_est = get_est_now()
            
        timestamped = TimestampedData(
            data=data,
            collected_at_est=collected_at_est,
            source=source,
            source_sequence_id=sequence_id
        )
        
        self.data_buffer.append(timestamped)
        return timestamped
    
    def get_synchronized_data(
        self,
        center_time: Optional[datetime] = None,
        window_seconds: Optional[float] = None,
        required_sources: Optional[List[str]] = None
    ) -> Dict[str, List[TimestampedData]]:
        """
        Get synchronized data grouped by source within a time window.
        
        Args:
            center_time: Center of synchronization window (default: now)
            window_seconds: Size of window (default: instance default)
            required_sources: Sources that must be present
            
        Returns:
            Dictionary mapping source names to lists of timestamped data
        """
        if center_time is None:
            center_time = get_est_now()
        if window_seconds is None:
            window_seconds = self.default_window_seconds
            
        window = SynchronizationWindow(
            center_time_est=center_time,
            window_seconds=window_seconds,
            max_acceptable_skew_seconds=self.max_skew_seconds
        )
        
        # Group data by source within the window
        synchronized_data: Dict[str, List[TimestampedData]] = {}
        
        for item in self.data_buffer:
            if window.contains(item.collected_at_est):
                source = item.source
                if source not in synchronized_data:
                    synchronized_data[source] = []
                synchronized_data[source].append(item)
        
        # Sort data within each source by timestamp
        for source_data in synchronized_data.values():
            source_data.sort(key=lambda x: x.collected_at_est)
        
        # Check required sources
        if required_sources:
            missing_sources = set(required_sources) - set(synchronized_data.keys())
            if missing_sources:
                if self.require_all_sources:
                    raise ValueError(f"Missing required sources: {missing_sources}")
        
        return synchronized_data
    
    def find_best_time_alignment(
        self,
        data_sets: Dict[str, List[TimestampedData]],
        max_time_diff_seconds: float = 180.0
    ) -> Optional[Dict[str, TimestampedData]]:
        """
        Find the best time-aligned data point from each source.
        
        Args:
            data_sets: Data grouped by source
            max_time_diff_seconds: Maximum acceptable time difference
            
        Returns:
            Dictionary mapping sources to best-aligned data points
        """
        if len(data_sets) < 2:
            return None
            
        sources = list(data_sets.keys())
        best_alignment = None
        min_time_spread = float('inf')
        
        # Try all combinations to find best alignment
        for source_a in sources:
            for item_a in data_sets[source_a]:
                alignment = {source_a: item_a}
                timestamps = [item_a.collected_at_est]
                
                # Find closest item from each other source
                for source_b in sources:
                    if source_b == source_a:
                        continue
                        
                    closest_item = None
                    min_diff = float('inf')
                    
                    for item_b in data_sets[source_b]:
                        diff = abs((item_b.collected_at_est - item_a.collected_at_est).total_seconds())
                        if diff < min_diff:
                            min_diff = diff
                            closest_item = item_b
                    
                    if closest_item and min_diff <= max_time_diff_seconds:
                        alignment[source_b] = closest_item
                        timestamps.append(closest_item.collected_at_est)
                    else:
                        # Can't find acceptable alignment
                        break
                else:
                    # All sources have acceptable alignment
                    time_spread = (max(timestamps) - min(timestamps)).total_seconds()
                    if time_spread < min_time_spread:
                        min_time_spread = time_spread
                        best_alignment = alignment
        
        return best_alignment
    
    def cleanup_old_data(self, max_age_seconds: float = 3600.0) -> int:
        """Remove data older than specified age."""
        cutoff_time = get_est_now() - timedelta(seconds=max_age_seconds)
        
        initial_count = len(self.data_buffer)
        self.data_buffer = [
            item for item in self.data_buffer 
            if item.collected_at_est >= cutoff_time
        ]
        
        removed_count = initial_count - len(self.data_buffer)
        return removed_count


class TimingMetrics(BaseModel):
    """Metrics for timing and synchronization analysis."""
    
    total_data_points: int = 0
    sources_count: int = 0
    time_span_seconds: float = 0.0
    average_source_delay_seconds: float = 0.0
    max_source_delay_seconds: float = 0.0
    synchronization_success_rate: float = 0.0
    timing_anomalies: List[str] = Field(default_factory=list)
    last_updated_est: datetime = Field(default_factory=lambda: get_est_now())
    
    def add_timing_anomaly(self, source: str, description: str, timestamp: datetime) -> None:
        """Add a timing anomaly for tracking."""
        anomaly = f"{format_timestamp_for_logging(timestamp)}: {source} - {description}"
        self.timing_anomalies.append(anomaly)
        
        # Keep only recent anomalies (last 100)
        if len(self.timing_anomalies) > 100:
            self.timing_anomalies = self.timing_anomalies[-100:]
        
        self.last_updated_est = get_est_now()


# Core timing utilities - EST only per project requirements

def get_est_now() -> datetime:
    """Get current EST time with timezone info."""
    return datetime.now(EST)


def to_est(dt: datetime) -> datetime:
    """Convert datetime to EST."""
    if dt.tzinfo is None:
        # Naive datetime - assume EST per project requirements
        dt = EST.localize(dt)
    
    return dt.astimezone(EST)


def precise_timestamp() -> datetime:
    """Get precise EST timestamp for data collection."""
    return get_est_now()


def create_collection_timestamp(
    source: str,
    sequence_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create standardized timestamp metadata for data collection.
    
    Returns:
        Dictionary with timing metadata
    """
    now_est = get_est_now()
    
    return {
        "collected_at_est": now_est,
        "collected_at_unix": now_est.timestamp(),
        "source": source,
        "sequence_id": sequence_id,
        "timezone_info": {
            "est_offset_seconds": now_est.utcoffset().total_seconds() if now_est.utcoffset() else 0,
        }
    }


def calculate_synchronization_quality(
    timestamps: List[datetime],
    expected_interval_seconds: float = 60.0
) -> float:
    """
    Calculate quality score for timestamp synchronization.
    
    Args:
        timestamps: List of timestamps to analyze
        expected_interval_seconds: Expected interval between data points
        
    Returns:
        Quality score from 0.0 (poor) to 1.0 (perfect)
    """
    if len(timestamps) < 2:
        return 1.0
    
    timestamps = sorted(timestamps)
    intervals = []
    
    for i in range(1, len(timestamps)):
        interval = (timestamps[i] - timestamps[i-1]).total_seconds()
        intervals.append(interval)
    
    # Calculate variance from expected interval
    avg_interval = sum(intervals) / len(intervals)
    variance = sum((interval - expected_interval_seconds) ** 2 for interval in intervals) / len(intervals)
    
    # Convert variance to quality score (lower variance = higher quality)
    max_acceptable_variance = (expected_interval_seconds * 0.5) ** 2
    quality = max(0.0, 1.0 - (variance / max_acceptable_variance))
    
    return min(1.0, quality)


async def wait_for_synchronized_collection(
    sources: List[str],
    max_wait_seconds: float = 30.0,
    check_interval_seconds: float = 1.0
) -> bool:
    """
    Wait for all sources to complete collection within a time window.
    
    Args:
        sources: List of source names to wait for
        max_wait_seconds: Maximum time to wait
        check_interval_seconds: How often to check
        
    Returns:
        True if all sources completed within window
    """
    start_time = get_est_now()
    
    while (get_est_now() - start_time).total_seconds() < max_wait_seconds:
        # This would check actual collection status in real implementation
        # For now, simulate with a simple timer
        await asyncio.sleep(check_interval_seconds)
        
        # In real implementation, check if all sources have recent data
        # return all(source_has_recent_data(source) for source in sources)
    
    return False


def format_timestamp_for_logging(dt: datetime) -> str:
    """Format timestamp for consistent logging."""
    if dt.tzinfo is None:
        dt = EST.localize(dt)
    
    est_str = to_est(dt).strftime("%Y-%m-%d %H:%M:%S.%f EST")
    return est_str


# Global synchronizer instance for the application
default_synchronizer = DataSynchronizer(
    default_window_seconds=60.0,
    max_skew_seconds=300.0,
    require_all_sources=False
)
