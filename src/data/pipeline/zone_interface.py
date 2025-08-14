"""
Data Pipeline Zone Interface

Defines the common interface for all pipeline zones (RAW, STAGING, CURATED).
Provides abstraction for zone operations and data flow management.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ZoneType(str, Enum):
    """Pipeline zone types."""

    RAW = "raw"
    STAGING = "staging"
    CURATED = "curated"


class ProcessingStatus(str, Enum):
    """Data processing status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ZoneMetrics:
    """Zone processing metrics."""

    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    processing_time_seconds: float = 0.0
    quality_score: float = 0.0
    error_rate: float = 0.0


class ZoneConfig(BaseModel):
    """Zone configuration settings."""

    zone_type: ZoneType
    schema_name: str
    enabled: bool = True
    quality_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    batch_size: int = Field(default=1000, gt=0)
    retry_attempts: int = Field(default=3, ge=0)
    timeout_seconds: int = Field(default=300, gt=0)
    validation_enabled: bool = True
    auto_promotion: bool = True


class DataRecord(BaseModel):
    """Base data record structure."""

    id: int | None = None
    external_id: str | None = None
    source: str | None = None  # Allow None values, processors will handle fallbacks
    raw_data: dict[str, Any] | None = None
    quality_score: float | None = Field(default=None, ge=0.0, le=1.0)
    validation_status: ProcessingStatus = ProcessingStatus.PENDING
    validation_errors: list[str] | None = None
    collected_at: datetime | None = None
    processed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProcessingResult(BaseModel):
    """Result of zone processing operation."""

    status: ProcessingStatus
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    processing_time: float = 0.0
    quality_metrics: ZoneMetrics | None = None
    errors: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataZone(ABC):
    """
    Abstract base class for all pipeline zones.

    Defines the common interface that all zones (RAW, STAGING, CURATED) must implement.
    Each zone is responsible for:
    - Data ingestion from previous zone or external sources
    - Data processing according to zone-specific logic
    - Data validation and quality control
    - Metrics collection and monitoring
    """

    def __init__(self, config: ZoneConfig):
        self.config = config
        self.zone_type = config.zone_type
        self.schema_name = config.schema_name
        self._metrics = ZoneMetrics()

    @property
    def metrics(self) -> ZoneMetrics:
        """Get current zone metrics."""
        return self._metrics

    @abstractmethod
    async def process_batch(
        self, records: list[DataRecord], **kwargs
    ) -> ProcessingResult:
        """
        Process a batch of records in this zone.

        Args:
            records: List of data records to process
            **kwargs: Additional processing parameters

        Returns:
            ProcessingResult with processing status and metrics
        """
        pass

    @abstractmethod
    async def validate_record(self, record: DataRecord) -> bool:
        """
        Validate a single record according to zone-specific rules.

        Args:
            record: Data record to validate

        Returns:
            True if record passes validation, False otherwise
        """
        pass

    @abstractmethod
    async def get_quality_score(self, record: DataRecord) -> float:
        """
        Calculate quality score for a record.

        Args:
            record: Data record to score

        Returns:
            Quality score between 0.0 and 1.0
        """
        pass

    @abstractmethod
    async def promote_to_next_zone(self, records: list[DataRecord]) -> ProcessingResult:
        """
        Promote validated records to the next pipeline zone.

        Args:
            records: Records to promote

        Returns:
            ProcessingResult with promotion status
        """
        pass

    async def health_check(self) -> dict[str, Any]:
        """
        Perform health check for this zone.

        Returns:
            Dictionary with health status information
        """
        return {
            "zone_type": self.zone_type,
            "schema_name": self.schema_name,
            "enabled": self.config.enabled,
            "metrics": {
                "records_processed": self._metrics.records_processed,
                "records_successful": self._metrics.records_successful,
                "records_failed": self._metrics.records_failed,
                "quality_score": self._metrics.quality_score,
                "error_rate": self._metrics.error_rate,
            },
            "status": "healthy" if self._metrics.error_rate < 0.1 else "degraded",
        }

    def update_metrics(self, result: ProcessingResult) -> None:
        """Update zone metrics with processing result."""
        self._metrics.records_processed += result.records_processed
        self._metrics.records_successful += result.records_successful
        self._metrics.records_failed += result.records_failed
        self._metrics.processing_time_seconds += result.processing_time

        # Update quality score (weighted average)
        if result.quality_metrics and self._metrics.records_processed > 0:
            total_records = self._metrics.records_processed
            current_weight = result.records_processed / total_records
            previous_weight = 1 - current_weight

            self._metrics.quality_score = (
                previous_weight * self._metrics.quality_score
                + current_weight * result.quality_metrics.quality_score
            )

        # Update error rate
        if self._metrics.records_processed > 0:
            self._metrics.error_rate = (
                self._metrics.records_failed / self._metrics.records_processed
            )

    def reset_metrics(self) -> None:
        """Reset zone metrics."""
        self._metrics = ZoneMetrics()


class ZoneFactory:
    """Factory for creating zone instances."""

    _zone_registry: dict[ZoneType, type] = {}

    @classmethod
    def register_zone(cls, zone_type: ZoneType, zone_class: type) -> None:
        """Register a zone implementation."""
        cls._zone_registry[zone_type] = zone_class

    @classmethod
    def create_zone(cls, zone_type: ZoneType, config: ZoneConfig) -> DataZone:
        """Create a zone instance."""
        if zone_type not in cls._zone_registry:
            raise ValueError(f"Unknown zone type: {zone_type}")

        zone_class = cls._zone_registry[zone_type]
        return zone_class(config)

    @classmethod
    def list_registered_zones(cls) -> list[ZoneType]:
        """List all registered zone types."""
        return list(cls._zone_registry.keys())


# Utility functions for zone management
def create_zone_config(zone_type: ZoneType, schema_name: str, **kwargs) -> ZoneConfig:
    """Create a zone configuration."""
    return ZoneConfig(zone_type=zone_type, schema_name=schema_name, **kwargs)


def validate_zone_progression(current_zone: ZoneType, target_zone: ZoneType) -> bool:
    """
    Validate that zone progression is valid (RAW → STAGING → CURATED).

    Args:
        current_zone: Current zone type
        target_zone: Target zone type

    Returns:
        True if progression is valid
    """
    valid_progressions = {
        ZoneType.RAW: [ZoneType.STAGING],
        ZoneType.STAGING: [ZoneType.CURATED],
        ZoneType.CURATED: [],  # Final zone
    }

    return target_zone in valid_progressions.get(current_zone, [])


def get_next_zone(current_zone: ZoneType) -> ZoneType | None:
    """Get the next zone in the pipeline progression."""
    zone_order = {
        ZoneType.RAW: ZoneType.STAGING,
        ZoneType.STAGING: ZoneType.CURATED,
        ZoneType.CURATED: None,  # Final zone
    }

    return zone_order.get(current_zone)
