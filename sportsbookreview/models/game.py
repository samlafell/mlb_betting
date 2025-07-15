"""
Enhanced Game model for SportsbookReview system.

This model extends the platform's game representation with comprehensive
MLB Stats API integration for complete game context enrichment.
"""

# Import main system models for consistency
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import Field, computed_field, field_validator

src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from mlb_sharp_betting.models.game import GameStatus, Team

from .base import (
    DataQuality,
    SportsbookReviewBaseModel,
    SportsBookReviewTimestampedModel,
    SportsbookReviewValidatedModel,
)


class WeatherCondition(str, Enum):
    """Weather conditions for baseball games."""

    CLEAR = "clear"
    PARTLY_CLOUDY = "partly_cloudy"
    CLOUDY = "cloudy"
    OVERCAST = "overcast"
    LIGHT_RAIN = "light_rain"
    RAIN = "rain"
    DRIZZLE = "drizzle"
    FOG = "fog"
    SNOW = "snow"
    WINDY = "windy"
    DOME = "dome"  # Indoor stadium
    UNKNOWN = "unknown"


class GameType(str, Enum):
    """Types of MLB games."""

    REGULAR = "regular"
    PLAYOFF = "playoff"
    WORLD_SERIES = "world_series"
    ALL_STAR = "all_star"
    EXHIBITION = "exhibition"
    SPRING_TRAINING = "spring_training"


class VenueInfo(SportsbookReviewBaseModel):
    """
    Model representing venue information from MLB Stats API.
    """

    venue_id: int | None = Field(default=None, description="MLB venue ID", ge=1)

    venue_name: str | None = Field(
        default=None, description="Official venue name", max_length=100
    )

    city: str | None = Field(default=None, description="Venue city", max_length=50)

    state: str | None = Field(
        default=None, description="Venue state/province", max_length=50
    )

    timezone: str | None = Field(
        default=None, description="Venue timezone", max_length=50
    )

    capacity: int | None = Field(default=None, description="Stadium capacity", ge=0)

    surface: str | None = Field(
        default=None, description="Playing surface type", max_length=50
    )

    roof_type: str | None = Field(
        default=None, description="Roof type (open, retractable, dome)", max_length=20
    )


class WeatherData(SportsbookReviewBaseModel):
    """
    Model representing weather conditions at game time.
    """

    condition: WeatherCondition | None = Field(
        default=None, description="Weather condition"
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

    humidity: int | None = Field(
        default=None, description="Humidity percentage", ge=0, le=100
    )

    @field_validator("wind_direction")
    @classmethod
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


class PitcherInfo(SportsbookReviewBaseModel):
    """
    Model representing starting pitcher information.
    """

    player_id: int | None = Field(default=None, description="MLB player ID", ge=1)

    full_name: str | None = Field(
        default=None, description="Pitcher full name", max_length=100
    )

    throws: str | None = Field(
        default=None, description="Throwing hand (L/R)", pattern="^[LR]$"
    )

    era: float | None = Field(default=None, description="Current season ERA", ge=0.0)

    wins: int | None = Field(default=None, description="Season wins", ge=0)

    losses: int | None = Field(default=None, description="Season losses", ge=0)


class PitcherMatchup(SportsbookReviewBaseModel):
    """
    Model representing the starting pitcher matchup.
    """

    home_pitcher: PitcherInfo | None = Field(
        default=None, description="Home team starting pitcher"
    )

    away_pitcher: PitcherInfo | None = Field(
        default=None, description="Away team starting pitcher"
    )

    @computed_field
    @property
    def handedness_matchup(self) -> str | None:
        """Get handedness matchup (e.g., 'L vs R')."""
        if (
            self.home_pitcher
            and self.home_pitcher.throws
            and self.away_pitcher
            and self.away_pitcher.throws
        ):
            return f"{self.away_pitcher.throws} vs {self.home_pitcher.throws}"
        return None


class GameContext(SportsbookReviewBaseModel):
    """
    Model representing additional game context from MLB Stats API.
    """

    series_description: str | None = Field(
        default=None, description="Series description", max_length=100
    )

    series_game_number: int | None = Field(
        default=None, description="Game number in series", ge=1
    )

    games_in_series: int | None = Field(
        default=None, description="Total games in series", ge=1
    )

    is_playoff_game: bool = Field(
        default=False, description="Whether this is a playoff game"
    )

    playoff_round: str | None = Field(
        default=None, description="Playoff round if applicable", max_length=50
    )

    attendance: int | None = Field(default=None, description="Game attendance", ge=0)

    game_duration_minutes: int | None = Field(
        default=None, description="Game duration in minutes", ge=0
    )


class EnhancedGame(SportsBookReviewTimestampedModel, SportsbookReviewValidatedModel):
    """
    Enhanced Game model with comprehensive MLB Stats API integration.

    Combines SportsbookReview game data with MLB Stats API enrichment
    for complete game context and analysis capabilities.
    """

    # Core SportsbookReview identification
    sbr_game_id: str = Field(
        ...,
        description="SportsbookReview game identifier",
        min_length=1,
        max_length=100,
    )

    # MLB Stats API integration
    mlb_game_id: str | None = Field(
        default=None, description="MLB Stats API game ID (gamePk)", max_length=20
    )

    # Team information (using existing Team enum)
    home_team: Team = Field(..., description="Home team abbreviation")

    away_team: Team = Field(..., description="Away team abbreviation")

    # Game scheduling
    game_datetime: datetime = Field(..., description="Scheduled game start time (EST)")

    game_type: GameType = Field(default=GameType.REGULAR, description="Type of game")

    game_status: GameStatus = Field(
        default=GameStatus.SCHEDULED, description="Current game status"
    )

    # MLB Stats API enrichment data
    venue_info: VenueInfo | None = Field(
        default=None, description="Venue information from MLB API"
    )

    weather_data: WeatherData | None = Field(
        default=None, description="Weather conditions at game time"
    )

    pitcher_matchup: PitcherMatchup | None = Field(
        default=None, description="Starting pitcher information"
    )

    game_context: GameContext | None = Field(
        default=None, description="Additional game context and metadata"
    )

    # Data quality and correlation
    mlb_correlation_confidence: float | None = Field(
        default=None,
        description="Confidence score for MLB Game ID correlation (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    data_quality: DataQuality = Field(
        default=DataQuality.MEDIUM, description="Overall data quality assessment"
    )

    # Game results (populated after completion)
    home_score: int | None = Field(
        default=None, description="Home team final score", ge=0
    )

    away_score: int | None = Field(
        default=None, description="Away team final score", ge=0
    )

    winning_team: Team | None = Field(
        default=None, description="Winning team (None for ties)"
    )

    @field_validator("home_team", "away_team")
    @classmethod
    def validate_teams_different(cls, v: Team, info) -> Team:
        """Ensure home and away teams are different."""
        if info.data and "home_team" in info.data:
            if info.data["home_team"] == v and info.field_name == "away_team":
                raise ValueError("Home and away teams must be different")
        return v

    @field_validator("game_datetime", mode="before")
    @classmethod
    def parse_game_datetime(cls, v: Any) -> datetime:
        """Parse game date ensuring EST timezone."""
        if isinstance(v, datetime):
            return v

        if isinstance(v, str):
            try:
                # Parse various date formats
                parsed = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return parsed
            except ValueError:
                # Try other formats
                for fmt in [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d",
                ]:
                    try:
                        return datetime.strptime(v, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Unable to parse game date: {v}")

        raise ValueError(f"Invalid game date type: {type(v)}")

    @computed_field
    @property
    def matchup_display(self) -> str:
        """Get formatted matchup display."""
        # Handle both enum and string values
        away_team_str = (
            self.away_team.value
            if hasattr(self.away_team, "value")
            else str(self.away_team)
        )
        home_team_str = (
            self.home_team.value
            if hasattr(self.home_team, "value")
            else str(self.home_team)
        )
        return f"{away_team_str} @ {home_team_str}"

    @computed_field
    @property
    def has_mlb_enrichment(self) -> bool:
        """Check if game has MLB Stats API enrichment data."""
        return bool(
            self.mlb_game_id
            and (
                self.venue_info
                or self.weather_data
                or self.pitcher_matchup
                or self.game_context
            )
        )

    @computed_field
    @property
    def is_completed(self) -> bool:
        """Check if game is completed."""
        return (
            self.game_status == GameStatus.FINAL
            and self.home_score is not None
            and self.away_score is not None
        )

    def get_correlation_key(self) -> str:
        """
        Generate a key for correlating with MLB Stats API.

        For double headers, includes time to differentiate games.

        Returns:
            String key for correlation matching
        """
        # Use full datetime for double header support
        datetime_str = self.game_datetime.strftime("%Y-%m-%d-%H%M")
        # Handle both enum and string values
        away_team_str = (
            self.away_team.value
            if hasattr(self.away_team, "value")
            else str(self.away_team)
        )
        home_team_str = (
            self.home_team.value
            if hasattr(self.home_team, "value")
            else str(self.home_team)
        )
        return f"{datetime_str}-{away_team_str}-{home_team_str}"

    def update_mlb_enrichment(
        self,
        mlb_game_id: str,
        venue_info: VenueInfo | None = None,
        weather_data: WeatherData | None = None,
        pitcher_matchup: PitcherMatchup | None = None,
        game_context: GameContext | None = None,
        correlation_confidence: float = 1.0,
    ) -> None:
        """
        Update game with MLB Stats API enrichment data.

        Args:
            mlb_game_id: MLB Stats API game ID
            venue_info: Venue information
            weather_data: Weather conditions
            pitcher_matchup: Pitcher information
            game_context: Additional context
            correlation_confidence: Confidence in correlation
        """
        self.mlb_game_id = mlb_game_id
        self.mlb_correlation_confidence = correlation_confidence

        if venue_info:
            self.venue_info = venue_info
        if weather_data:
            self.weather_data = weather_data
        if pitcher_matchup:
            self.pitcher_matchup = pitcher_matchup
        if game_context:
            self.game_context = game_context

        # Update data quality based on enrichment
        enrichment_count = sum(
            [
                bool(venue_info),
                bool(weather_data),
                bool(pitcher_matchup),
                bool(game_context),
            ]
        )

        if enrichment_count >= 3:
            self.data_quality = DataQuality.HIGH
        elif enrichment_count >= 2:
            self.data_quality = DataQuality.MEDIUM
        elif enrichment_count >= 1:
            self.data_quality = DataQuality.LOW
        else:
            self.data_quality = DataQuality.POOR

    class Config:
        json_schema_extra = {
            "example": {
                "sbr_game_id": "sbr-2025-04-15-LAD-SF-1",
                "mlb_game_id": "123456",
                "home_team": "SF",
                "away_team": "LAD",
                "game_datetime": "2025-04-15T19:45:00-04:00",
                "game_type": "regular",
                "game_status": "scheduled",
                "venue_info": {
                    "venue_id": 2395,
                    "venue_name": "Oracle Park",
                    "city": "San Francisco",
                    "state": "CA",
                    "capacity": 41915,
                },
                "weather_data": {
                    "condition": "clear",
                    "temperature": 68,
                    "wind_speed": 8,
                    "wind_direction": "W",
                },
                "mlb_correlation_confidence": 0.95,
                "data_quality": "high",
            }
        }
