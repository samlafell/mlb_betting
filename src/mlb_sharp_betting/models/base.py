"""
Base models for the MLB Sharp Betting system.

This module provides base Pydantic models with common functionality
and validation patterns used throughout the application.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel as PydanticBaseModel, Field, validator


class BaseModel(PydanticBaseModel):
    """
    Base model with common configuration and functionality.
    
    All models in the system should inherit from this base class
    to ensure consistent behavior and configuration.
    """
    
    class Config:
        # Use enum values instead of enum objects in serialization
        use_enum_values = True
        
        # Validate assignment when attributes are changed
        validate_assignment = True
        
        # Allow population by field name or alias
        populate_by_name = True
        
        # Forbid extra fields not defined in the model
        extra = "forbid"
        
        # Use proper JSON encoders for complex types
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }
        
        # Generate schema with examples
        json_schema_extra: Dict[str, Any] = {
            "example": {}
        }
    
    def dict_without_none(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Return dictionary representation excluding None values.
        
        Args:
            **kwargs: Additional arguments passed to dict()
        
        Returns:
            Dictionary with None values excluded
        """
        return {k: v for k, v in self.dict(**kwargs).items() if v is not None}
    
    def dict_with_aliases(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Return dictionary representation using field aliases.
        
        Args:
            **kwargs: Additional arguments passed to dict()
        
        Returns:
            Dictionary using field aliases as keys
        """
        return self.dict(by_alias=True, **kwargs)


class TimestampedModel(BaseModel):
    """
    Base model with automatic timestamp tracking.
    
    Provides created_at and updated_at fields that are automatically
    managed for tracking when records are created and modified.
    """
    
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the record was created"
    )
    
    updated_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp when the record was last updated"
    )
    
    @validator("created_at", "updated_at", pre=True)
    def parse_datetime(cls, v: Any) -> Optional[datetime]:
        """
        Parse datetime values from various formats.
        
        Args:
            v: Input value to parse
            
        Returns:
            Parsed datetime or None
        """
        if v is None:
            return None
            
        if isinstance(v, datetime):
            # Ensure timezone awareness
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v
            
        if isinstance(v, str):
            try:
                # Try parsing ISO format
                parsed = datetime.fromisoformat(v.replace('Z', '+00:00'))
                return parsed
            except ValueError:
                # Try other common formats
                for fmt in [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                ]:
                    try:
                        parsed = datetime.strptime(v, fmt)
                        return parsed.replace(tzinfo=timezone.utc)
                    except ValueError:
                        continue
                        
                raise ValueError(f"Unable to parse datetime: {v}")
        
        raise ValueError(f"Invalid datetime type: {type(v)}")
    
    def touch(self) -> None:
        """Update the updated_at timestamp to current time."""
        self.updated_at = datetime.now(timezone.utc)
    
    @property
    def age_seconds(self) -> float:
        """Get the age of this record in seconds."""
        now = datetime.now(timezone.utc)
        return (now - self.created_at).total_seconds()
    
    @property
    def is_stale(self, max_age_seconds: int = 3600) -> bool:
        """
        Check if the record is stale based on age.
        
        Args:
            max_age_seconds: Maximum age in seconds before considering stale
            
        Returns:
            True if the record is older than max_age_seconds
        """
        return self.age_seconds > max_age_seconds


class IdentifiedModel(TimestampedModel):
    """
    Base model with ID field for entities that need unique identification.
    
    Extends TimestampedModel with an ID field that can be used as
    a primary key or unique identifier.
    """
    
    id: Optional[str] = Field(
        default=None,
        description="Unique identifier for the record",
        max_length=255
    )
    
    @validator("id")
    def validate_id(cls, v: Optional[str]) -> Optional[str]:
        """
        Validate ID field format.
        
        Args:
            v: ID value to validate
            
        Returns:
            Validated ID or None
        """
        if v is None:
            return None
            
        # Remove whitespace
        v = v.strip()
        
        if not v:
            return None
            
        # Basic validation - no control characters
        if any(ord(c) < 32 for c in v):
            raise ValueError("ID cannot contain control characters")
            
        return v


class ValidatedModel(BaseModel):
    """
    Base model with enhanced validation capabilities.
    
    Provides additional validation methods and error handling
    for models that require strict data validation.
    """
    
    def validate_required_fields(self, fields: list[str]) -> None:
        """
        Validate that required fields are present and not None.
        
        Args:
            fields: List of field names to validate
            
        Raises:
            ValueError: If any required field is missing or None
        """
        missing_fields = []
        
        for field in fields:
            if not hasattr(self, field) or getattr(self, field) is None:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Required fields missing: {missing_fields}")
    
    def validate_field_range(
        self, 
        field: str, 
        min_val: Optional[float] = None,
        max_val: Optional[float] = None
    ) -> None:
        """
        Validate that a numeric field is within specified range.
        
        Args:
            field: Field name to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Raises:
            ValueError: If field value is outside the specified range
        """
        if not hasattr(self, field):
            return
        
        value = getattr(self, field)
        if value is None:
            return
        
        if min_val is not None and value < min_val:
            raise ValueError(f"{field} must be >= {min_val}, got {value}")
        
        if max_val is not None and value > max_val:
            raise ValueError(f"{field} must be <= {max_val}, got {value}")
    
    def validate_positive(self, field: str) -> None:
        """
        Validate that a numeric field is positive.
        
        Args:
            field: Field name to validate
            
        Raises:
            ValueError: If field value is not positive
        """
        self.validate_field_range(field, min_val=0.0)
    
    def validate_percentage(self, field: str) -> None:
        """
        Validate that a field contains a valid percentage (0-100).
        
        Args:
            field: Field name to validate
            
        Raises:
            ValueError: If field value is not a valid percentage
        """
        self.validate_field_range(field, min_val=0.0, max_val=100.0) 