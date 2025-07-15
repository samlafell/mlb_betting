"""
Base models for SportsbookReview system.

This module provides base Pydantic models that extend the existing
platform patterns with SportsbookReview-specific functionality.
"""

# Import base models from the main system
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator

# Add the src directory to Python path for imports
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from mlb_sharp_betting.models.base import BaseModel, TimestampedModel, ValidatedModel


class SportsbookReviewBaseModel(BaseModel):
    """
    Base model for SportsbookReview system extending platform patterns.

    Inherits from the main system's BaseModel to ensure consistency
    while adding SportsbookReview-specific functionality.
    """

    class Config:
        # Inherit main system config
        use_enum_values = True
        validate_assignment = True
        validate_by_name = True
        extra = "allow"

        # SportsbookReview-specific encoders
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }

        json_schema_extra: dict[str, Any] = {
            "example": {},
            "source": "sportsbookreview",
        }

    def to_platform_dict(self, **kwargs: Any) -> dict[str, Any]:
        """
        Convert to platform-compatible dictionary format.

        Args:
            **kwargs: Additional arguments passed to dict()

        Returns:
            Dictionary compatible with main platform models
        """
        return self.dict(exclude_none=True, **kwargs)


class SportsBookReviewTimestampedModel(SportsbookReviewBaseModel, TimestampedModel):
    """
    SportsbookReview model with automatic timestamp tracking.

    Combines SportsbookReview base functionality with timestamp management.
    """

    # Additional SportsbookReview-specific metadata
    source_url: str | None = Field(
        default=None, description="Source URL from SportsbookReview.com", max_length=500
    )

    scrape_timestamp: datetime | None = Field(
        default=None, description="When this data was scraped from SportsbookReview"
    )

    @field_validator("scrape_timestamp", mode="before")
    def parse_scrape_timestamp(cls, v: Any) -> datetime | None:
        """
        Parse scrape timestamp from various formats.

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
                parsed = datetime.fromisoformat(v.replace("Z", "+00:00"))
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

                raise ValueError(f"Unable to parse scrape timestamp: {v}")

        raise ValueError(f"Invalid scrape timestamp type: {type(v)}")


class SportsbookReviewValidatedModel(SportsbookReviewBaseModel, ValidatedModel):
    """
    SportsbookReview model with enhanced validation capabilities.

    Extends the main platform's validation with SportsbookReview-specific
    validation methods.
    """

    def validate_odds_format(self, field: str) -> None:
        """
        Validate that a field contains properly formatted American odds.

        Args:
            field: Field name to validate

        Raises:
            ValueError: If odds format is invalid
        """
        if not hasattr(self, field):
            return

        value = getattr(self, field)
        if value is None:
            return

        # Convert to string for validation
        odds_str = str(value)

        # American odds format: +/-XXX
        if odds_str.startswith(("+", "-")):
            try:
                odds_value = int(odds_str)
                # Reasonable bounds for American odds
                if not (-50000 <= odds_value <= 50000):
                    raise ValueError(
                        f"{field} odds value {odds_value} is outside reasonable bounds"
                    )
            except ValueError:
                raise ValueError(
                    f"{field} must be a valid American odds format (+/-XXX)"
                )
        else:
            # Decimal odds format
            try:
                odds_value = float(odds_str)
                if odds_value <= 0:
                    raise ValueError(f"{field} decimal odds must be positive")
            except ValueError:
                raise ValueError(f"{field} must be a valid odds format")

    def validate_spread_format(self, field: str) -> None:
        """
        Validate that a field contains properly formatted spread.

        Args:
            field: Field name to validate

        Raises:
            ValueError: If spread format is invalid
        """
        if not hasattr(self, field):
            return

        value = getattr(self, field)
        if value is None:
            return

        try:
            spread_value = float(value)
            # Reasonable bounds for MLB spreads
            if not (-20.0 <= spread_value <= 20.0):
                raise ValueError(
                    f"{field} spread value {spread_value} is outside reasonable bounds for MLB"
                )
        except (ValueError, TypeError):
            raise ValueError(f"{field} must be a valid spread value")

    def validate_total_format(self, field: str) -> None:
        """
        Validate that a field contains properly formatted total.

        Args:
            field: Field name to validate

        Raises:
            ValueError: If total format is invalid
        """
        if not hasattr(self, field):
            return

        value = getattr(self, field)
        if value is None:
            return

        try:
            total_value = float(value)
            # Reasonable bounds for MLB totals
            if not (2.0 <= total_value <= 30.0):
                raise ValueError(
                    f"{field} total value {total_value} is outside reasonable bounds for MLB"
                )
        except (ValueError, TypeError):
            raise ValueError(f"{field} must be a valid total value")


# Enums for SportsbookReview system
class BetType(str, Enum):
    """Types of bets available in SportsbookReview."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    TEAM_TOTAL = "team_total"


class SportsbookName(str, Enum):
    """Sportsbooks tracked by SportsbookReview."""

    DRAFTKINGS = "draftkings"
    FANDUEL = "fanduel"
    CAESARS = "caesars"
    BETMGM = "betmgm"
    POINTSBET = "pointsbet"
    BARSTOOL = "barstool"
    FOXBET = "foxbet"
    UNIBET = "unibet"
    # Add more as needed


class OddsFormat(str, Enum):
    """Odds display formats."""

    AMERICAN = "american"  # +150, -110
    DECIMAL = "decimal"  # 2.50, 1.91
    FRACTIONAL = "fractional"  # 3/2, 10/11


class DataQuality(str, Enum):
    """Data quality indicators."""

    HIGH = "high"  # Complete data with all expected fields
    MEDIUM = "medium"  # Most data present, some fields missing
    LOW = "low"  # Minimal data, significant gaps
    POOR = "poor"  # Data present but questionable accuracy
