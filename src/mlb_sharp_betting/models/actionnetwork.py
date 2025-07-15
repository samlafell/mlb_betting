"""
ActionNetwork models for the MLB Sharp Betting system.

This module provides Pydantic models for ActionNetwork data structures
including team dimension data and historical line movement data.
"""

from datetime import date, datetime, time
from enum import Enum
from typing import Any

from pydantic import Field, validator

from mlb_sharp_betting.models.base import IdentifiedModel, ValidatedModel


class MarketType(str, Enum):
    """Market types for Action Network data."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"


class LineMovementPeriod(str, Enum):
    """Period types for line movement data."""

    PREGAME = "pregame"
    LIVE = "live"


class ActionNetworkPrice(ValidatedModel):
    """
    Model representing a price/odds entry from Action Network.

    Contains odds information with decimal and American formats.
    """

    decimal: float | None = Field(
        default=None, description="Decimal odds format", gt=0
    )

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


class ActionNetworkMarketData(ValidatedModel):
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


class ActionNetworkHistoricalEntry(ValidatedModel):
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


class TeamDimension(IdentifiedModel, ValidatedModel):
    """
    Model representing an MLB team dimension for the action schema.

    This is a dimension table containing core team attributes
    without game-specific or time-varying data like standings.
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

    # League organization
    conference_type: str = Field(
        ..., description="Conference/League (AL or NL)", pattern="^(AL|NL)$"
    )

    division_type: str = Field(
        ...,
        description="Division (EAST, CENTRAL, or WEST)",
        pattern="^(EAST|CENTRAL|WEST)$",
    )

    # URL reference
    url_slug: str = Field(
        ..., description="URL slug for team pages", min_length=1, max_length=100
    )

    @validator("abbr")
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase."""
        return v.upper()

    @validator("primary_color", "secondary_color")
    def validate_hex_color(cls, v: str) -> str:
        """Ensure color codes are uppercase hex."""
        return v.upper()

    @validator("conference_type")
    def validate_conference(cls, v: str) -> str:
        """Ensure conference is uppercase."""
        return v.upper()

    @validator("division_type")
    def validate_division(cls, v: str) -> str:
        """Ensure division is uppercase."""
        return v.upper()

    @property
    def full_conference_name(self) -> str:
        """Get full conference name."""
        return "American League" if self.conference_type == "AL" else "National League"

    @property
    def full_division_name(self) -> str:
        """Get full division name."""
        return f"{self.full_conference_name} {self.division_type.title()}"

    @property
    def team_colors_css(self) -> str:
        """Get CSS color values for styling."""
        return f"#{self.primary_color}, #{self.secondary_color}"

    class Config:
        json_schema_extra = {
            "example": {
                "team_id": 191,
                "full_name": "New York Yankees",
                "display_name": "Yankees",
                "short_name": "Yankees",
                "location": "New York",
                "abbr": "NYY",
                "logo": "https://static.sprtactn.co/teamlogos/mlb/100/nyyd.png",
                "primary_color": "003087",
                "secondary_color": "E4002B",
                "conference_type": "AL",
                "division_type": "EAST",
                "url_slug": "new-york-yankees",
            }
        }


class GameFact(IdentifiedModel, ValidatedModel):
    """
    Model representing an MLB game fact for the action schema.

    This is a fact table that bridges Action Network games with
    team dimensions and external systems.
    """

    # Primary identifiers
    id_action: int = Field(..., description="Action Network game ID", ge=1)

    id_mlbstatsapi: int | None = Field(
        default=None,
        description="MLB Stats API game identifier for linking to official data",
        ge=1,
    )

    # Team dimensions (foreign keys)
    dim_home_team_actionid: int = Field(
        ..., description="Foreign key to home team in dim_teams", ge=1
    )

    dim_away_team_actionid: int = Field(
        ..., description="Foreign key to away team in dim_teams", ge=1
    )

    # Date/time dimensions
    dim_date: date = Field(..., description="Game date (local time)")

    dim_time: time | None = Field(
        default=None, description="Game start time (local time)"
    )

    dim_datetime: datetime | None = Field(
        default=None, description="Game start timestamp with timezone"
    )

    # Game attributes
    game_status: str = Field(
        default="scheduled",
        description="Current status of the game",
        pattern="^(scheduled|live|final|postponed|cancelled|suspended|delayed)$",
    )

    venue_name: str | None = Field(
        default=None, description="Stadium/venue where game is played", max_length=200
    )

    season: int | None = Field(
        default=None, description="MLB season year", ge=1876, le=2030
    )

    season_type: str = Field(
        default="regular",
        description="Type of season",
        pattern="^(regular|postseason|spring)$",
    )

    game_number: int = Field(
        default=1,
        description="Game number (1 for single games, 1-2 for doubleheaders)",
        ge=1,
        le=2,
    )

    # Weather attributes (optional)
    weather_conditions: str | None = Field(
        default=None, description="Weather conditions at game time"
    )

    temperature: int | None = Field(
        default=None, description="Temperature in Fahrenheit", ge=-20, le=120
    )

    wind_speed: int | None = Field(
        default=None, description="Wind speed in mph", ge=0, le=100
    )

    wind_direction: str | None = Field(
        default=None,
        description="Wind direction (N, NE, E, SE, S, SW, W, NW)",
        max_length=10,
    )

    @validator("dim_home_team_actionid", "dim_away_team_actionid")
    def validate_teams_different(cls, v: int, values: dict) -> int:
        """Ensure home and away teams are different."""
        if "dim_home_team_actionid" in values and "dim_away_team_actionid" in values:
            if values.get("dim_home_team_actionid") == v:
                raise ValueError("Home and away teams must be different")
        return v

    @validator("game_status")
    def validate_game_status(cls, v: str) -> str:
        """Ensure game status is lowercase."""
        return v.lower()

    @validator("season_type")
    def validate_season_type(cls, v: str) -> str:
        """Ensure season type is lowercase."""
        return v.lower()

    @validator("wind_direction")
    def validate_wind_direction(cls, v: str | None) -> str | None:
        """Validate wind direction format."""
        if v is None:
            return v

        v = v.upper()
        valid_directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "CALM", "VAR"]

        if v not in valid_directions:
            raise ValueError(
                f"Wind direction must be one of: {', '.join(valid_directions)}"
            )

        return v

    @property
    def matchup(self) -> str:
        """Get a string representation of the matchup."""
        return f"Game {self.id_action} on {self.dim_date}"

    @property
    def is_doubleheader(self) -> bool:
        """Check if this is part of a doubleheader."""
        return self.game_number > 1

    @property
    def has_weather_data(self) -> bool:
        """Check if weather data is available."""
        return any(
            [
                self.weather_conditions,
                self.temperature is not None,
                self.wind_speed is not None,
                self.wind_direction is not None,
            ]
        )

    class Config:
        json_schema_extra = {
            "example": {
                "id_action": 257324,
                "id_mlbstatsapi": 123456,
                "dim_home_team_actionid": 188,
                "dim_away_team_actionid": 191,
                "dim_date": "2025-07-01",
                "dim_time": "19:07:00",
                "dim_datetime": "2025-07-01T19:07:00-04:00",
                "game_status": "scheduled",
                "venue_name": "Rogers Centre",
                "season": 2025,
                "season_type": "regular",
                "game_number": 1,
                "weather_conditions": "Clear",
                "temperature": 75,
                "wind_speed": 8,
                "wind_direction": "SW",
            }
        }
