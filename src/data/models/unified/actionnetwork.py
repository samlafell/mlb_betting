"""
Action Network models for the unified MLB Sharp Betting system.

This module provides Pydantic models for Action Network data structures
including historical line movement data and team information.
"""

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, validator

from .base import IdentifiedModel, ValidatedModel


class MarketType(str, Enum):
    """Market types for Action Network data."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"


class LineMovementPeriod(str, Enum):
    """Period types for line movement data."""

    PREGAME = "pregame"
    LIVE = "live"


class ActionNetworkPrice(BaseModel):
    """
    Model representing a price/odds entry from Action Network.

    Contains odds information with decimal and American formats.
    """

    decimal: float | None = Field(default=None, description="Decimal odds format", gt=0)

    american: int | None = Field(
        default=None, description="American odds format (e.g., -110, +150)"
    )

    @validator("decimal")
    def validate_decimal_odds(cls, v):
        """Validate decimal odds are reasonable."""
        if v is not None and (v < 1.0 or v > 50.0):
            raise ValueError("Decimal odds must be between 1.0 and 50.0")
        return v

    @validator("american")
    def validate_american_odds(cls, v):
        """Validate American odds are reasonable."""
        if v is not None and (v < -10000 or v > 10000):
            raise ValueError("American odds must be between -10000 and +10000")
        return v


class ActionNetworkMarketData(BaseModel):
    """
    Model representing market data from Action Network history.

    Contains pricing information for home/away or over/under sides.
    """

    home: ActionNetworkPrice | None = Field(
        default=None, description="Home team or over pricing"
    )

    away: ActionNetworkPrice | None = Field(
        default=None, description="Away team or under pricing"
    )

    line: float | None = Field(
        default=None, description="Line value for spread/total markets"
    )

    timestamp: datetime | None = Field(
        default=None, description="Timestamp when this market data was recorded"
    )


class ActionNetworkHistoricalEntry(BaseModel):
    """
    Model representing a single historical entry from Action Network.

    Contains market data for all bet types at a specific point in time.
    """

    event: dict[str, Any] = Field(
        ..., description="Event data containing market information"
    )

    timestamp: datetime | None = Field(
        default=None, description="Timestamp of this historical entry"
    )

    period: LineMovementPeriod = Field(
        default=LineMovementPeriod.PREGAME,
        description="Whether this is pregame or live data",
    )

    # Market data
    moneyline: ActionNetworkMarketData | None = Field(
        default=None, description="Moneyline market data"
    )

    spread: ActionNetworkMarketData | None = Field(
        default=None, description="Spread market data"
    )

    total: ActionNetworkMarketData | None = Field(
        default=None, description="Total market data"
    )

    @validator("event")
    def validate_event_structure(cls, v):
        """Validate event contains required market data."""
        if not isinstance(v, dict):
            raise ValueError("Event must be a dictionary")
        return v


class ActionNetworkHistoricalData(IdentifiedModel, ValidatedModel):
    """
    Model representing complete historical line movement data for a game.

    Contains all historical entries showing line movement over time.
    """

    # Game identification
    game_id: int = Field(..., description="Action Network game ID", ge=1)

    home_team: str = Field(
        ..., description="Home team name", min_length=1, max_length=100
    )

    away_team: str = Field(
        ..., description="Away team name", min_length=1, max_length=100
    )

    game_datetime: datetime = Field(..., description="Scheduled game start time")

    # Historical data
    historical_entries: list[ActionNetworkHistoricalEntry] = Field(
        default_factory=list, description="List of historical line movement entries"
    )

    # Metadata
    extracted_at: datetime = Field(
        default_factory=datetime.now, description="When this data was extracted"
    )

    history_url: str = Field(
        ...,
        description="URL used to fetch this historical data",
        min_length=1,
        max_length=1000,
    )

    total_entries: int = Field(
        default=0, description="Total number of historical entries", ge=0
    )

    pregame_entries: int = Field(
        default=0, description="Number of pregame entries", ge=0
    )

    live_entries: int = Field(default=0, description="Number of live entries", ge=0)

    @validator("historical_entries")
    def validate_historical_entries(cls, v):
        """Validate historical entries are properly ordered."""
        if len(v) > 1:
            # Check if entries are in chronological order
            timestamps = [entry.timestamp for entry in v if entry.timestamp]
            if len(timestamps) > 1:
                for i in range(1, len(timestamps)):
                    if timestamps[i] < timestamps[i - 1]:
                        # Allow some tolerance for near-simultaneous entries
                        time_diff = (timestamps[i - 1] - timestamps[i]).total_seconds()
                        if time_diff > 60:  # More than 1 minute difference
                            raise ValueError(
                                "Historical entries should be in chronological order"
                            )
        return v

    @property
    def has_pregame_data(self) -> bool:
        """Check if historical data contains pregame entries."""
        return self.pregame_entries > 0

    @property
    def has_live_data(self) -> bool:
        """Check if historical data contains live entries."""
        return self.live_entries > 0

    @property
    def line_movement_summary(self) -> dict[str, Any]:
        """Get summary of line movement for all markets."""
        summary = {
            "moneyline": {"movements": 0, "significant_moves": 0},
            "spread": {"movements": 0, "significant_moves": 0},
            "total": {"movements": 0, "significant_moves": 0},
        }

        if len(self.historical_entries) < 2:
            return summary

        # Analyze line movements
        for market_type in ["moneyline", "spread", "total"]:
            movements = 0
            significant_moves = 0

            prev_entry = None
            for entry in self.historical_entries:
                current_market = getattr(entry, market_type, None)
                if current_market and prev_entry:
                    prev_market = getattr(prev_entry, market_type, None)
                    if prev_market and current_market:
                        # Check for line movement
                        if self._has_line_movement(
                            prev_market, current_market, market_type
                        ):
                            movements += 1
                            if self._is_significant_movement(
                                prev_market, current_market, market_type
                            ):
                                significant_moves += 1

                prev_entry = entry

            summary[market_type]["movements"] = movements
            summary[market_type]["significant_moves"] = significant_moves

        return summary

    def _has_line_movement(
        self,
        prev_market: ActionNetworkMarketData,
        current_market: ActionNetworkMarketData,
        market_type: str,
    ) -> bool:
        """Check if there's any line movement between two market entries."""
        if market_type in ["spread", "total"]:
            return prev_market.line != current_market.line or (
                prev_market.home
                and current_market.home
                and prev_market.home.american != current_market.home.american
            )
        else:  # moneyline
            return (
                prev_market.home
                and current_market.home
                and prev_market.home.american != current_market.home.american
            ) or (
                prev_market.away
                and current_market.away
                and prev_market.away.american != current_market.away.american
            )

    def _is_significant_movement(
        self,
        prev_market: ActionNetworkMarketData,
        current_market: ActionNetworkMarketData,
        market_type: str,
    ) -> bool:
        """Check if line movement is significant (>= 0.5 points for spreads/totals, >= 10 cents for moneylines)."""
        if market_type in ["spread", "total"]:
            if prev_market.line is not None and current_market.line is not None:
                return abs(prev_market.line - current_market.line) >= 0.5
        else:  # moneyline
            if (
                prev_market.home
                and current_market.home
                and prev_market.home.american
                and current_market.home.american
            ):
                return (
                    abs(prev_market.home.american - current_market.home.american) >= 10
                )
        return False


class ActionNetworkTeam(IdentifiedModel, ValidatedModel):
    """
    Model representing an MLB team from Action Network.

    Contains team identification and basic information.
    """

    # Primary identification
    team_id: int = Field(..., description="Action Network team ID", ge=1)

    # Team names and identifiers
    full_name: str = Field(
        ...,
        description="Full team name (e.g., 'New York Yankees')",
        min_length=1,
        max_length=100,
    )

    display_name: str = Field(
        ..., description="Display name (e.g., 'Yankees')", min_length=1, max_length=50
    )

    short_name: str = Field(
        ..., description="Short name (e.g., 'Yankees')", min_length=1, max_length=50
    )

    location: str = Field(
        ...,
        description="Team location/city (e.g., 'New York')",
        min_length=1,
        max_length=50,
    )

    abbr: str = Field(
        ...,
        description="Team abbreviation (e.g., 'NYY')",
        min_length=2,
        max_length=5,
        pattern="^[A-Z]+$",
    )

    # Visual branding
    logo: str = Field(..., description="Team logo URL", min_length=1, max_length=500)

    primary_color: str = Field(
        ...,
        description="Primary team color (hex code without #)",
        min_length=6,
        max_length=6,
        pattern="^[0-9A-Fa-f]{6}$",
    )

    secondary_color: str = Field(
        ...,
        description="Secondary team color (hex code without #)",
        min_length=6,
        max_length=6,
        pattern="^[0-9A-Fa-f]{6}$",
    )

    # League information
    conference_type: str = Field(
        ...,
        description="Conference/league type (e.g., 'AL', 'NL')",
        min_length=1,
        max_length=10,
    )

    division_type: str = Field(
        ...,
        description="Division type (e.g., 'EAST', 'CENTRAL', 'WEST')",
        min_length=1,
        max_length=20,
    )

    url_slug: str = Field(
        ...,
        description="URL slug for team (e.g., 'new-york-yankees')",
        min_length=1,
        max_length=100,
    )

    @validator("primary_color", "secondary_color")
    def validate_color_format(cls, v):
        """Validate color is a valid hex code."""
        if not v.isalnum():
            raise ValueError(
                "Color must be a valid hex code (letters and numbers only)"
            )
        return v.upper()

    @validator("conference_type", "division_type")
    def validate_league_info(cls, v):
        """Validate league information is uppercase."""
        return v.upper()

    @property
    def full_division(self) -> str:
        """Get full division name (e.g., 'AL EAST')."""
        return f"{self.conference_type} {self.division_type}"


class ActionNetworkGame(IdentifiedModel, ValidatedModel):
    """
    Model representing an MLB game from Action Network.

    Contains game identification and basic information for historical analysis.
    """

    # Primary identifiers
    game_id: int = Field(..., description="Action Network game ID", ge=1)

    # Team information
    home_team: ActionNetworkTeam = Field(..., description="Home team information")

    away_team: ActionNetworkTeam = Field(..., description="Away team information")

    # Game timing
    game_datetime: datetime = Field(..., description="Scheduled game start time")

    game_date: date = Field(..., description="Game date (local time)")

    # Game status
    status: str = Field(
        default="scheduled",
        description="Current status of the game",
        pattern="^(scheduled|live|final|postponed|cancelled|suspended|delayed|inprogress)$",
    )

    real_status: str = Field(
        default="scheduled", description="Real-time status of the game"
    )

    status_display: str | None = Field(
        default=None, description="Human-readable status display", max_length=100
    )

    # League information
    league_id: int = Field(..., description="League ID (typically 8 for MLB)", ge=1)

    league_name: str = Field(default="mlb", description="League name")

    season: int = Field(..., description="Season year", ge=1876, le=2030)

    game_type: str = Field(
        default="reg", description="Game type (reg, playoff, etc.)", max_length=20
    )

    # Metadata
    history_url: str | None = Field(
        default=None,
        description="URL for historical line movement data",
        max_length=1000,
    )

    @validator("status", "real_status")
    def validate_status(cls, v):
        """Ensure status is lowercase."""
        return v.lower()

    @validator("league_name")
    def validate_league_name(cls, v):
        """Ensure league name is lowercase."""
        return v.lower()

    @property
    def matchup(self) -> str:
        """Get a string representation of the matchup."""
        return f"{self.away_team.abbr} @ {self.home_team.abbr}"

    @property
    def is_live(self) -> bool:
        """Check if game is currently live."""
        return self.status in ["live", "inprogress"]

    @property
    def is_completed(self) -> bool:
        """Check if game is completed."""
        return self.status == "final"

    @property
    def has_history_url(self) -> bool:
        """Check if game has a history URL available."""
        return self.history_url is not None and len(self.history_url) > 0
