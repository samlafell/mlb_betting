"""
SportsbookMapping model for SportsbookReview system.

This module provides models for sportsbook identification, odds format
normalization, and market mapping across different sportsbooks.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field, computed_field, field_validator

from .base import (
    BetType,
    OddsFormat,
    SportsbookName,
    SportsbookReviewBaseModel,
    SportsBookReviewTimestampedModel,
)


class MarketAvailability(str, Enum):
    """Market availability status for sportsbooks."""

    ALWAYS_AVAILABLE = "always_available"  # Market always offered
    CONDITIONALLY_AVAILABLE = (
        "conditionally_available"  # Offered under certain conditions
    )
    RARELY_AVAILABLE = "rarely_available"  # Rarely offered
    NOT_AVAILABLE = "not_available"  # Never offered


class OddsDisplayPreference(str, Enum):
    """Preferred odds display format for sportsbooks."""

    AMERICAN_ONLY = "american_only"
    DECIMAL_ONLY = "decimal_only"
    FRACTIONAL_ONLY = "fractional_only"
    AMERICAN_PRIMARY = "american_primary"  # American with decimal option
    DECIMAL_PRIMARY = "decimal_primary"  # Decimal with American option
    ALL_FORMATS = "all_formats"


class SportsbookCapabilities(SportsbookReviewBaseModel):
    """
    Model representing the capabilities and characteristics of a sportsbook.
    """

    # Market offerings
    supports_live_betting: bool = Field(
        default=False, description="Whether sportsbook offers live betting"
    )

    supports_props: bool = Field(
        default=False, description="Whether sportsbook offers prop bets"
    )

    supports_parlays: bool = Field(
        default=False, description="Whether sportsbook offers parlay bets"
    )

    supports_early_cash_out: bool = Field(
        default=False, description="Whether sportsbook offers early cash out"
    )

    # Odds characteristics
    typical_juice_percentage: float | None = Field(
        default=None,
        description="Typical juice/vig percentage for standard markets",
        ge=0.0,
        le=50.0,
    )

    odds_update_frequency_minutes: int | None = Field(
        default=None,
        description="How often odds are typically updated (minutes)",
        ge=1,
        le=60,
    )

    # Limits and requirements
    minimum_bet_amount: float | None = Field(
        default=None, description="Minimum bet amount in USD", ge=0.01
    )

    maximum_bet_amount: float | None = Field(
        default=None, description="Maximum bet amount in USD", ge=1.0
    )

    # Geographic availability
    available_states: list[str] | None = Field(
        default=None, description="US states where sportsbook is available"
    )

    available_countries: list[str] | None = Field(
        default=None, description="Countries where sportsbook is available"
    )


class MarketMapping(SportsbookReviewBaseModel):
    """
    Model representing how a specific market is handled by a sportsbook.
    """

    bet_type: BetType = Field(..., description="Type of bet")

    availability: MarketAvailability = Field(
        ..., description="Availability status for this market"
    )

    # Naming conventions
    display_name: str | None = Field(
        default=None, description="How sportsbook displays this market", max_length=100
    )

    internal_name: str | None = Field(
        default=None,
        description="Internal identifier used by sportsbook",
        max_length=100,
    )

    # Market-specific characteristics
    typical_lines: list[float] | None = Field(
        default=None,
        description="Typical line values offered (e.g., [-1.5, -2.5] for spreads)",
    )

    line_increment: float | None = Field(
        default=None,
        description="Typical increment between lines (e.g., 0.5 for spreads)",
        gt=0.0,
    )

    # Juice/vig patterns
    typical_juice_range: dict[str, float] | None = Field(
        default=None, description="Typical juice range for this market (min/max)"
    )

    # Market timing
    opens_hours_before_game: float | None = Field(
        default=None,
        description="Hours before game this market typically opens",
        ge=0.0,
    )

    closes_minutes_before_game: float | None = Field(
        default=None,
        description="Minutes before game this market typically closes",
        ge=0.0,
    )

    @field_validator("typical_juice_range")
    @classmethod
    def validate_juice_range(
        cls, v: dict[str, float] | None
    ) -> dict[str, float] | None:
        """Validate juice range format."""
        if v is None:
            return v

        if "min" not in v or "max" not in v:
            raise ValueError("Juice range must contain 'min' and 'max' keys")

        if v["min"] < 0 or v["max"] < 0:
            raise ValueError("Juice percentages must be non-negative")

        if v["min"] > v["max"]:
            raise ValueError("Minimum juice must be less than or equal to maximum")

        return v


class SportsbookMapping(SportsBookReviewTimestampedModel):
    """
    Comprehensive mapping model for sportsbook identification and normalization.

    Tracks how different sportsbooks handle odds formats, market naming,
    and various betting options to enable consistent data processing.
    """

    # Core identification
    sportsbook_name: SportsbookName = Field(
        ..., description="Standardized sportsbook name"
    )

    # Display information
    display_name: str = Field(
        ..., description="Human-readable sportsbook name", min_length=1, max_length=100
    )

    short_name: str = Field(
        ...,
        description="Short abbreviation for sportsbook",
        min_length=1,
        max_length=10,
    )

    # Website and scraping info
    base_url: str | None = Field(
        default=None, description="Base URL for sportsbook website", max_length=200
    )

    odds_page_pattern: str | None = Field(
        default=None, description="URL pattern for odds pages", max_length=300
    )

    # Technical details
    preferred_odds_format: OddsDisplayPreference = Field(
        default=OddsDisplayPreference.AMERICAN_PRIMARY,
        description="Preferred odds display format",
    )

    supported_odds_formats: list[OddsFormat] = Field(
        default_factory=lambda: [OddsFormat.AMERICAN],
        description="All odds formats supported by sportsbook",
    )

    # Capabilities
    capabilities: SportsbookCapabilities = Field(
        default_factory=SportsbookCapabilities,
        description="Sportsbook capabilities and characteristics",
    )

    # Market mappings
    market_mappings: dict[str, MarketMapping] = Field(
        default_factory=dict, description="Market mappings by bet type"
    )

    # Scraping and parsing metadata
    parsing_rules: dict[str, Any] | None = Field(
        default=None, description="Custom parsing rules for this sportsbook"
    )

    rate_limit_requests_per_minute: int | None = Field(
        default=None, description="Rate limit for scraping requests", ge=1, le=1000
    )

    rate_limit_delay_seconds: float | None = Field(
        default=None,
        description="Minimum delay between requests (seconds)",
        ge=0.1,
        le=60.0,
    )

    # Status and reliability
    is_active: bool = Field(
        default=True, description="Whether sportsbook is actively tracked"
    )

    reliability_score: float | None = Field(
        default=None,
        description="Historical reliability score (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    last_successful_scrape: datetime | None = Field(
        default=None, description="Last successful data scrape"
    )

    consecutive_failures: int = Field(
        default=0, description="Number of consecutive scraping failures", ge=0
    )

    # Data quality tracking
    data_quality_metrics: dict[str, Any] | None = Field(
        default=None, description="Data quality metrics for this sportsbook"
    )

    @field_validator("supported_odds_formats")
    @classmethod
    def validate_odds_formats(cls, v: list[OddsFormat]) -> list[OddsFormat]:
        """Ensure at least one odds format is supported."""
        if not v:
            raise ValueError("At least one odds format must be supported")
        return v

    @field_validator("market_mappings")
    @classmethod
    def validate_market_mappings(
        cls, v: dict[str, MarketMapping]
    ) -> dict[str, MarketMapping]:
        """Validate market mappings structure."""
        valid_keys = {bt.value for bt in BetType}

        for key in v.keys():
            if key not in valid_keys:
                raise ValueError(f"Invalid market mapping key: {key}")

        return v

    @computed_field
    @property
    def supported_bet_types(self) -> set[BetType]:
        """Get set of supported bet types."""
        return {
            BetType(mapping.bet_type)
            for mapping in self.market_mappings.values()
            if mapping.availability
            in [
                MarketAvailability.ALWAYS_AVAILABLE,
                MarketAvailability.CONDITIONALLY_AVAILABLE,
            ]
        }

    @computed_field
    @property
    def average_juice_percentage(self) -> float | None:
        """Calculate average juice percentage across markets."""
        if not self.capabilities.typical_juice_percentage:
            return None

        # Could be enhanced to calculate from market mappings
        return self.capabilities.typical_juice_percentage

    @computed_field
    @property
    def is_reliable(self) -> bool:
        """Check if sportsbook is considered reliable."""
        if not self.is_active:
            return False

        if self.consecutive_failures >= 5:
            return False

        if self.reliability_score is not None and self.reliability_score < 0.7:
            return False

        return True

    def add_market_mapping(self, bet_type: BetType, mapping: MarketMapping) -> None:
        """
        Add or update a market mapping.

        Args:
            bet_type: Type of bet
            mapping: Market mapping configuration
        """
        # Handle both enum and string values
        bet_type_str = bet_type.value if hasattr(bet_type, "value") else str(bet_type)
        self.market_mappings[bet_type_str] = mapping

    def get_market_mapping(self, bet_type: BetType) -> MarketMapping | None:
        """
        Get market mapping for a specific bet type.

        Args:
            bet_type: Type of bet

        Returns:
            Market mapping if available, None otherwise
        """
        # Handle both enum and string values
        bet_type_str = bet_type.value if hasattr(bet_type, "value") else str(bet_type)
        return self.market_mappings.get(bet_type_str)

    def is_market_available(self, bet_type: BetType) -> bool:
        """
        Check if a market is available for this sportsbook.

        Args:
            bet_type: Type of bet

        Returns:
            True if market is available, False otherwise
        """
        mapping = self.get_market_mapping(bet_type)
        if not mapping:
            return False

        return mapping.availability in [
            MarketAvailability.ALWAYS_AVAILABLE,
            MarketAvailability.CONDITIONALLY_AVAILABLE,
        ]

    def record_scrape_success(self) -> None:
        """Record a successful scrape."""
        self.last_successful_scrape = datetime.now()
        self.consecutive_failures = 0

        # Update reliability score
        if self.reliability_score is None:
            self.reliability_score = 0.8
        else:
            # Increase reliability score slightly
            self.reliability_score = min(1.0, self.reliability_score + 0.01)

    def record_scrape_failure(self) -> None:
        """Record a failed scrape."""
        self.consecutive_failures += 1

        # Decrease reliability score
        if self.reliability_score is None:
            self.reliability_score = 0.5
        else:
            # Decrease reliability score based on consecutive failures
            penalty = 0.02 * self.consecutive_failures
            self.reliability_score = max(0.0, self.reliability_score - penalty)

    def get_rate_limit_settings(self) -> dict[str, Any]:
        """
        Get rate limiting settings for this sportsbook.

        Returns:
            Dictionary with rate limit configuration
        """
        return {
            "requests_per_minute": self.rate_limit_requests_per_minute or 30,
            "delay_seconds": self.rate_limit_delay_seconds or 1.0,
            "is_active": self.is_active,
            "reliability_score": self.reliability_score,
        }

    class Config:
        json_schema_extra = {
            "example": {
                "sportsbook_name": "draftkings",
                "display_name": "DraftKings Sportsbook",
                "short_name": "DK",
                "base_url": "https://sportsbook.draftkings.com",
                "preferred_odds_format": "american_primary",
                "supported_odds_formats": ["american", "decimal"],
                "capabilities": {
                    "supports_live_betting": True,
                    "supports_props": True,
                    "supports_parlays": True,
                    "typical_juice_percentage": 4.5,
                    "odds_update_frequency_minutes": 2,
                    "minimum_bet_amount": 0.10,
                    "maximum_bet_amount": 25000.0,
                },
                "market_mappings": {
                    "moneyline": {
                        "bet_type": "moneyline",
                        "availability": "always_available",
                        "display_name": "Moneyline",
                        "opens_hours_before_game": 72.0,
                        "closes_minutes_before_game": 5.0,
                    },
                    "spread": {
                        "bet_type": "spread",
                        "availability": "always_available",
                        "display_name": "Point Spread",
                        "typical_lines": [-1.5, -2.5, +1.5, +2.5],
                        "line_increment": 0.5,
                    },
                },
                "rate_limit_requests_per_minute": 30,
                "rate_limit_delay_seconds": 1.0,
                "is_active": True,
                "reliability_score": 0.95,
            }
        }
