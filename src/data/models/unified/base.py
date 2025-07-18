"""
Base models for the unified data layer.

Provides common patterns and base classes consolidated from all three legacy modules.
All times are handled in EST as per project requirements.
"""

from datetime import datetime
from typing import Any, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, ConfigDict

# Import precise timing utilities
try:
    from ...core.timing import get_est_now, precise_timestamp, to_est
except ImportError:
    # Fallback for backward compatibility
    import pytz
    EST = pytz.timezone('US/Eastern')
    def get_est_now() -> datetime:
        return datetime.now(EST)
    def to_est(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return EST.localize(dt)
        return dt.astimezone(EST)
    def precise_timestamp() -> datetime:
        return get_est_now()

T = TypeVar("T", bound="UnifiedBaseModel")


class UnifiedBaseModel(BaseModel):
    """
    Base model for all unified data structures.

    Consolidates common patterns from:
    - mlb_sharp_betting.models.base.BaseModel
    - sportsbookreview.models.base.SportsbookReviewBaseModel
    - action models (implicit base patterns)
    """

    model_config = ConfigDict(
        # Validation settings
        validate_assignment=True,
        use_enum_values=True,
        arbitrary_types_allowed=False,
        # Extra fields handling
        extra="forbid"
    )

    def model_dump_json_safe(self) -> dict[str, Any]:
        """
        Dump model to dictionary with safe JSON serialization.

        Returns:
            Dictionary representation safe for JSON serialization
        """
        return self.model_dump(exclude_none=True, by_alias=True)

    def model_copy_with_changes(self: T, **changes: Any) -> T:
        """
        Create a copy of the model with specified changes.

        Args:
            **changes: Fields to update in the copy

        Returns:
            New model instance with changes applied
        """
        return self.model_copy(update=changes, deep=True)

    @classmethod
    def model_validate_json_safe(cls: type[T], json_data: str) -> T:
        """
        Validate JSON data with comprehensive error handling.

        Args:
            json_data: JSON string to validate

        Returns:
            Validated model instance

        Raises:
            ValidationError: If validation fails
        """
        return cls.model_validate_json(json_data)


class TimestampedModel(UnifiedBaseModel):
    """
    Base model for entities with timestamp tracking.

    Consolidates timestamp patterns from all three legacy modules.
    All timestamps are in EST as per project requirements.
    """

    created_at: datetime = Field(
        default_factory=precise_timestamp,
        description="When the record was created (EST with precise timing)",
        frozen=True,
    )

    updated_at: datetime = Field(
        default_factory=precise_timestamp,
        description="When the record was last updated (EST with precise timing)",
    )

    def touch_updated_at(self) -> None:
        """Update the updated_at timestamp to current time with precision."""
        # Note: This requires model_config validate_assignment=True
        self.updated_at = precise_timestamp()


class IdentifiedModel(UnifiedBaseModel):
    """
    Base model for entities with unique identification.

    Consolidates ID patterns from mlb_sharp_betting and other modules.
    """

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the entity",
        frozen=True,
    )


class ValidatedModel(UnifiedBaseModel):
    """
    Base model for entities requiring data quality validation.

    Consolidates validation patterns from sportsbookreview and other modules.
    """

    data_quality: str | None = Field(
        default="MEDIUM",
        description="Data quality assessment (LOW, MEDIUM, HIGH, EXCELLENT)",
        pattern="^(LOW|MEDIUM|HIGH|EXCELLENT)$",
    )

    validation_errors: dict[str, str] | None = Field(
        default=None, description="Validation errors encountered during processing"
    )

    @field_validator("data_quality")
    @classmethod
    def validate_data_quality(cls, v: str | None) -> str | None:
        """Ensure data quality is uppercase."""
        return v.upper() if v else v

    def add_validation_error(self, field: str, error: str) -> None:
        """
        Add a validation error to the model.

        Args:
            field: Field name where error occurred
            error: Error description
        """
        if self.validation_errors is None:
            self.validation_errors = {}
        self.validation_errors[field] = error

        # Downgrade data quality if errors exist
        if self.data_quality in ["HIGH", "EXCELLENT"]:
            self.data_quality = "MEDIUM"

    def has_validation_errors(self) -> bool:
        """Check if the model has any validation errors."""
        return bool(self.validation_errors)


class SourcedModel(UnifiedBaseModel):
    """
    Base model for entities with data source tracking.

    Tracks where data originated from across the three legacy modules.
    """

    source: str = Field(
        ...,
        description="Data source identifier (SBD, VSIN, ACTION_NETWORK, MLB_API, etc.)",
        min_length=1,
        max_length=50,
    )

    source_id: str | None = Field(
        default=None,
        description="Original identifier from the source system",
        max_length=100,
    )

    source_url: str | None = Field(
        default=None, description="URL where data was retrieved from", max_length=500
    )

    source_timestamp: datetime | None = Field(
        default=None, description="When data was retrieved from source (EST with precise timing)"
    )

    collected_at_est: datetime = Field(
        default_factory=precise_timestamp,
        description="Precise collection timestamp in EST for data synchronization"
    )

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        """Ensure source is uppercase."""
        return v.upper()


# Composite base models for common patterns


class UnifiedEntity(IdentifiedModel, TimestampedModel, ValidatedModel, SourcedModel):
    """
    Comprehensive base model combining all common patterns.

    Use this for complex entities that need full tracking and validation.
    """

    pass


class SimpleEntity(TimestampedModel):
    """
    Simple base model for basic entities.

    Use this for simple data structures that only need timestamps.
    """

    pass


class AnalysisEntity(IdentifiedModel, TimestampedModel, ValidatedModel):
    """
    Base model for analysis results and processed data.

    Use this for betting analysis, sharp detection results, etc.
    """

    pass
