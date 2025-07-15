"""
Unified Game Model

Consolidates game models from:
- src/mlb_sharp_betting/models/game.py (basic Game model)
- sportsbookreview/models/game.py (EnhancedGame with MLB API integration)
- src/mlb_sharp_betting/models/actionnetwork.py (ActionNetwork dimensional data)

All times are in EST as per project requirements.
"""

from datetime import date, datetime, time
from enum import Enum
from typing import Any, Optional

from pydantic import Field, computed_field, field_validator

from .base import UnifiedEntity, ValidatedModel


class GameStatus(str, Enum):
    """Unified game status enumeration."""

    SCHEDULED = "scheduled"
    LIVE = "live"
    FINAL = "final"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"
    DELAYED = "delayed"


class GameType(str, Enum):
    """Unified game type enumeration."""

    REGULAR = "regular"
    PLAYOFF = "playoff"
    WORLD_SERIES = "world_series"
    ALL_STAR = "all_star"
    EXHIBITION = "exhibition"
    SPRING_TRAINING = "spring_training"


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


class Team(str, Enum):
    """Unified MLB team enumeration with comprehensive team data."""

    # American League East
    BAL = "BAL"  # Baltimore Orioles
    BOS = "BOS"  # Boston Red Sox
    NYY = "NYY"  # New York Yankees
    TB = "TB"  # Tampa Bay Rays
    TOR = "TOR"  # Toronto Blue Jays

    # American League Central
    CWS = "CWS"  # Chicago White Sox
    CLE = "CLE"  # Cleveland Guardians
    DET = "DET"  # Detroit Tigers
    KC = "KC"  # Kansas City Royals
    MIN = "MIN"  # Minnesota Twins

    # American League West
    HOU = "HOU"  # Houston Astros
    LAA = "LAA"  # Los Angeles Angels
    OAK = "OAK"  # Oakland Athletics
    SEA = "SEA"  # Seattle Mariners
    TEX = "TEX"  # Texas Rangers

    # National League East
    ATL = "ATL"  # Atlanta Braves
    MIA = "MIA"  # Miami Marlins
    NYM = "NYM"  # New York Mets
    PHI = "PHI"  # Philadelphia Phillies
    WSH = "WSH"  # Washington Nationals

    # National League Central
    CHC = "CHC"  # Chicago Cubs
    CIN = "CIN"  # Cincinnati Reds
    MIL = "MIL"  # Milwaukee Brewers
    PIT = "PIT"  # Pittsburgh Pirates
    STL = "STL"  # St. Louis Cardinals

    # National League West
    ARI = "ARI"  # Arizona Diamondbacks
    COL = "COL"  # Colorado Rockies
    LAD = "LAD"  # Los Angeles Dodgers
    SD = "SD"  # San Diego Padres
    SF = "SF"  # San Francisco Giants

    @classmethod
    def get_team_info(cls, abbreviation: str) -> dict[str, Any]:
        """
        Get comprehensive team information.

        Args:
            abbreviation: Team abbreviation

        Returns:
            Dictionary with team information
        """
        team_info = {
            # American League East
            "BAL": {
                "full_name": "Baltimore Orioles",
                "display_name": "Orioles",
                "location": "Baltimore",
                "conference": "AL",
                "division": "EAST",
                "primary_color": "DF4601",
                "secondary_color": "000000",
            },
            "BOS": {
                "full_name": "Boston Red Sox",
                "display_name": "Red Sox",
                "location": "Boston",
                "conference": "AL",
                "division": "EAST",
                "primary_color": "BD3039",
                "secondary_color": "0C2340",
            },
            "NYY": {
                "full_name": "New York Yankees",
                "display_name": "Yankees",
                "location": "New York",
                "conference": "AL",
                "division": "EAST",
                "primary_color": "0C2340",
                "secondary_color": "C4CED4",
            },
            "TB": {
                "full_name": "Tampa Bay Rays",
                "display_name": "Rays",
                "location": "Tampa Bay",
                "conference": "AL",
                "division": "EAST",
                "primary_color": "092C5C",
                "secondary_color": "8FBCE6",
            },
            "TOR": {
                "full_name": "Toronto Blue Jays",
                "display_name": "Blue Jays",
                "location": "Toronto",
                "conference": "AL",
                "division": "EAST",
                "primary_color": "134A8E",
                "secondary_color": "1D2D5C",
            },
            # American League Central
            "CWS": {
                "full_name": "Chicago White Sox",
                "display_name": "White Sox",
                "location": "Chicago",
                "conference": "AL",
                "division": "CENTRAL",
                "primary_color": "27251F",
                "secondary_color": "C4CED4",
            },
            "CLE": {
                "full_name": "Cleveland Guardians",
                "display_name": "Guardians",
                "location": "Cleveland",
                "conference": "AL",
                "division": "CENTRAL",
                "primary_color": "E31937",
                "secondary_color": "0C2340",
            },
            "DET": {
                "full_name": "Detroit Tigers",
                "display_name": "Tigers",
                "location": "Detroit",
                "conference": "AL",
                "division": "CENTRAL",
                "primary_color": "0C2340",
                "secondary_color": "FA4616",
            },
            "KC": {
                "full_name": "Kansas City Royals",
                "display_name": "Royals",
                "location": "Kansas City",
                "conference": "AL",
                "division": "CENTRAL",
                "primary_color": "004687",
                "secondary_color": "BD9B60",
            },
            "MIN": {
                "full_name": "Minnesota Twins",
                "display_name": "Twins",
                "location": "Minnesota",
                "conference": "AL",
                "division": "CENTRAL",
                "primary_color": "002B5C",
                "secondary_color": "D31145",
            },
            # American League West
            "HOU": {
                "full_name": "Houston Astros",
                "display_name": "Astros",
                "location": "Houston",
                "conference": "AL",
                "division": "WEST",
                "primary_color": "002D62",
                "secondary_color": "EB6E1F",
            },
            "LAA": {
                "full_name": "Los Angeles Angels",
                "display_name": "Angels",
                "location": "Los Angeles",
                "conference": "AL",
                "division": "WEST",
                "primary_color": "BA0021",
                "secondary_color": "003263",
            },
            "OAK": {
                "full_name": "Oakland Athletics",
                "display_name": "Athletics",
                "location": "Oakland",
                "conference": "AL",
                "division": "WEST",
                "primary_color": "003831",
                "secondary_color": "EFB21E",
            },
            "SEA": {
                "full_name": "Seattle Mariners",
                "display_name": "Mariners",
                "location": "Seattle",
                "conference": "AL",
                "division": "WEST",
                "primary_color": "0C2C56",
                "secondary_color": "005C5C",
            },
            "TEX": {
                "full_name": "Texas Rangers",
                "display_name": "Rangers",
                "location": "Texas",
                "conference": "AL",
                "division": "WEST",
                "primary_color": "003278",
                "secondary_color": "C0111F",
            },
            # National League East
            "ATL": {
                "full_name": "Atlanta Braves",
                "display_name": "Braves",
                "location": "Atlanta",
                "conference": "NL",
                "division": "EAST",
                "primary_color": "CE1141",
                "secondary_color": "13274F",
            },
            "MIA": {
                "full_name": "Miami Marlins",
                "display_name": "Marlins",
                "location": "Miami",
                "conference": "NL",
                "division": "EAST",
                "primary_color": "00A3E0",
                "secondary_color": "EF3340",
            },
            "NYM": {
                "full_name": "New York Mets",
                "display_name": "Mets",
                "location": "New York",
                "conference": "NL",
                "division": "EAST",
                "primary_color": "002D72",
                "secondary_color": "FF5910",
            },
            "PHI": {
                "full_name": "Philadelphia Phillies",
                "display_name": "Phillies",
                "location": "Philadelphia",
                "conference": "NL",
                "division": "EAST",
                "primary_color": "E81828",
                "secondary_color": "002D72",
            },
            "WSH": {
                "full_name": "Washington Nationals",
                "display_name": "Nationals",
                "location": "Washington",
                "conference": "NL",
                "division": "EAST",
                "primary_color": "AB0003",
                "secondary_color": "14225A",
            },
            # National League Central
            "CHC": {
                "full_name": "Chicago Cubs",
                "display_name": "Cubs",
                "location": "Chicago",
                "conference": "NL",
                "division": "CENTRAL",
                "primary_color": "0E3386",
                "secondary_color": "CC3433",
            },
            "CIN": {
                "full_name": "Cincinnati Reds",
                "display_name": "Reds",
                "location": "Cincinnati",
                "conference": "NL",
                "division": "CENTRAL",
                "primary_color": "C6011F",
                "secondary_color": "000000",
            },
            "MIL": {
                "full_name": "Milwaukee Brewers",
                "display_name": "Brewers",
                "location": "Milwaukee",
                "conference": "NL",
                "division": "CENTRAL",
                "primary_color": "FFC52F",
                "secondary_color": "12284B",
            },
            "PIT": {
                "full_name": "Pittsburgh Pirates",
                "display_name": "Pirates",
                "location": "Pittsburgh",
                "conference": "NL",
                "division": "CENTRAL",
                "primary_color": "FDB827",
                "secondary_color": "27251F",
            },
            "STL": {
                "full_name": "St. Louis Cardinals",
                "display_name": "Cardinals",
                "location": "St. Louis",
                "conference": "NL",
                "division": "CENTRAL",
                "primary_color": "C41E3A",
                "secondary_color": "FEDB00",
            },
            # National League West
            "ARI": {
                "full_name": "Arizona Diamondbacks",
                "display_name": "Diamondbacks",
                "location": "Arizona",
                "conference": "NL",
                "division": "WEST",
                "primary_color": "A71930",
                "secondary_color": "E3D4AD",
            },
            "COL": {
                "full_name": "Colorado Rockies",
                "display_name": "Rockies",
                "location": "Colorado",
                "conference": "NL",
                "division": "WEST",
                "primary_color": "33006F",
                "secondary_color": "C4CED4",
            },
            "LAD": {
                "full_name": "Los Angeles Dodgers",
                "display_name": "Dodgers",
                "location": "Los Angeles",
                "conference": "NL",
                "division": "WEST",
                "primary_color": "005A9C",
                "secondary_color": "EF3E42",
            },
            "SD": {
                "full_name": "San Diego Padres",
                "display_name": "Padres",
                "location": "San Diego",
                "conference": "NL",
                "division": "WEST",
                "primary_color": "2F241D",
                "secondary_color": "FFC425",
            },
            "SF": {
                "full_name": "San Francisco Giants",
                "display_name": "Giants",
                "location": "San Francisco",
                "conference": "NL",
                "division": "WEST",
                "primary_color": "FD5A1E",
                "secondary_color": "27251F",
            },
        }

        return team_info.get(
            abbreviation,
            {
                "full_name": abbreviation,
                "display_name": abbreviation,
                "location": abbreviation,
                "conference": "UNKNOWN",
                "division": "UNKNOWN",
                "primary_color": "000000",
                "secondary_color": "FFFFFF",
            },
        )

    @classmethod
    def normalize_team_name(cls, name: str) -> Optional["Team"]:
        """
        Normalize team name to standard Team enum.

        Args:
            name: Team name or abbreviation in various formats

        Returns:
            Team enum value or None if not found
        """
        if not name:
            return None

        name = name.strip().upper()

        # Check if it's already a valid abbreviation
        try:
            return cls(name)
        except ValueError:
            pass

        # Common name mappings
        name_mappings = {
            # Alternative abbreviations
            "WSN": "WSH",
            "WAS": "WSH",
            "CHW": "CWS",
            "ANA": "LAA",
            "TBR": "TB",
            "TBD": "TB",
            "AZ": "ARI",
            "ATH": "OAK",
            # Full names (partial matching)
            "ORIOLES": "BAL",
            "RED SOX": "BOS",
            "YANKEES": "NYY",
            "RAYS": "TB",
            "BLUE JAYS": "TOR",
            "WHITE SOX": "CWS",
            "GUARDIANS": "CLE",
            "TIGERS": "DET",
            "ROYALS": "KC",
            "TWINS": "MIN",
            "ASTROS": "HOU",
            "ANGELS": "LAA",
            "ATHLETICS": "OAK",
            "MARINERS": "SEA",
            "RANGERS": "TEX",
            "BRAVES": "ATL",
            "MARLINS": "MIA",
            "METS": "NYM",
            "PHILLIES": "PHI",
            "NATIONALS": "WSH",
            "CUBS": "CHC",
            "REDS": "CIN",
            "BREWERS": "MIL",
            "PIRATES": "PIT",
            "CARDINALS": "STL",
            "DIAMONDBACKS": "ARI",
            "ROCKIES": "COL",
            "DODGERS": "LAD",
            "PADRES": "SD",
            "GIANTS": "SF",
        }

        # Try direct mapping
        mapped = name_mappings.get(name)
        if mapped:
            try:
                return cls(mapped)
            except ValueError:
                pass

        # Try partial matching for full names
        for key, abbr in name_mappings.items():
            if key in name or name in key:
                try:
                    return cls(abbr)
                except ValueError:
                    continue

        return None


class VenueInfo(ValidatedModel):
    """Unified venue information model."""

    venue_id: int | None = Field(default=None, description="MLB venue ID", ge=1)

    venue_name: str | None = Field(
        default=None, description="Official venue name", max_length=200
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
        default=None,
        description="Roof type (Open, Closed, Retractable, Dome)",
        max_length=20,
    )


class WeatherData(ValidatedModel):
    """Unified weather data model."""

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


class PitcherInfo(ValidatedModel):
    """Unified pitcher information model."""

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


class PitcherMatchup(ValidatedModel):
    """Unified pitcher matchup model."""

    home_pitcher: PitcherInfo | None = Field(
        default=None, description="Home team starting pitcher"
    )

    away_pitcher: PitcherInfo | None = Field(
        default=None, description="Away team starting pitcher"
    )

    @computed_field
    @property
    def handedness_matchup(self) -> str | None:
        """Get handedness matchup description."""
        if not (self.home_pitcher and self.away_pitcher):
            return None

        home_throws = self.home_pitcher.throws or "?"
        away_throws = self.away_pitcher.throws or "?"
        return f"{away_throws} vs {home_throws}"


class GameContext(ValidatedModel):
    """Unified game context model."""

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


class UnifiedGame(UnifiedEntity):
    """
    Unified game model consolidating all game-related data.

    Combines features from:
    - Basic Game model (core game data)
    - EnhancedGame model (MLB API integration)
    - ActionNetwork models (dimensional data)

    All times are in EST as per project requirements.
    """

    # Cross-system identifiers
    game_id: str = Field(
        ..., description="Primary game identifier", min_length=1, max_length=100
    )

    mlb_game_id: str | None = Field(
        default=None, description="MLB Stats API game ID (gamePk)", max_length=20
    )

    sbr_game_id: str | None = Field(
        default=None, description="SportsbookReview game identifier", max_length=100
    )

    action_network_game_id: int | None = Field(
        default=None, description="Action Network game ID", ge=1
    )

    # Core game information
    home_team: Team = Field(..., description="Home team")

    away_team: Team = Field(..., description="Away team")

    # Scheduling (all times in EST)
    game_date: date = Field(..., description="Game date (EST)")

    game_time: time | None = Field(default=None, description="Game start time (EST)")

    game_datetime: datetime = Field(
        ..., description="Complete game start datetime (EST)"
    )

    # Game classification
    game_type: GameType = Field(default=GameType.REGULAR, description="Type of game")

    game_status: GameStatus = Field(
        default=GameStatus.SCHEDULED, description="Current game status"
    )

    season: int = Field(..., description="Season year", ge=1876, le=2030)

    season_type: str = Field(
        default="regular",
        description="Type of season (regular, postseason, spring)",
        pattern="^(regular|postseason|spring)$",
    )

    game_number: int = Field(
        default=1,
        description="Game number (1 for single games, 1-2 for doubleheaders)",
        ge=1,
        le=2,
    )

    # Venue and environment
    venue_info: VenueInfo | None = Field(
        default=None, description="Venue information"
    )

    weather_data: WeatherData | None = Field(
        default=None, description="Weather conditions at game time"
    )

    # Game participants
    pitcher_matchup: PitcherMatchup | None = Field(
        default=None, description="Starting pitcher information"
    )

    # Game context
    game_context: GameContext | None = Field(
        default=None, description="Additional game context and metadata"
    )

    # Game results
    home_score: int | None = Field(
        default=None, description="Home team final score", ge=0
    )

    away_score: int | None = Field(
        default=None, description="Away team final score", ge=0
    )

    winning_team: Team | None = Field(
        default=None, description="Winning team (None for ties)"
    )

    # Data correlation and quality
    mlb_correlation_confidence: float | None = Field(
        default=None,
        description="Confidence score for MLB Game ID correlation (0.0-1.0)",
        ge=0.0,
        le=1.0,
    )

    # Model validation
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
        """Parse game datetime ensuring EST timezone."""
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

    @field_validator("winning_team")
    @classmethod
    def validate_winning_team(cls, v: Team | None, info) -> Team | None:
        """Validate winning team is one of the game participants."""
        if v is None:
            return v

        data = info.data
        if data and "home_team" in data and "away_team" in data:
            if v not in [data["home_team"], data["away_team"]]:
                raise ValueError("Winning team must be either home or away team")

        return v

    # Computed properties
    @computed_field
    @property
    def matchup_display(self) -> str:
        """Get formatted matchup display."""
        return f"{self.away_team.value} @ {self.home_team.value}"

    @computed_field
    @property
    def is_completed(self) -> bool:
        """Check if game is completed."""
        return (
            self.game_status == GameStatus.FINAL
            and self.home_score is not None
            and self.away_score is not None
        )

    @computed_field
    @property
    def is_in_progress(self) -> bool:
        """Check if game is currently in progress."""
        return self.game_status == GameStatus.LIVE

    @computed_field
    @property
    def is_scheduled(self) -> bool:
        """Check if game is scheduled."""
        return self.game_status == GameStatus.SCHEDULED

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
    def is_doubleheader(self) -> bool:
        """Check if this is part of a doubleheader."""
        return self.game_number > 1

    # Utility methods
    def get_team_score(self, team: Team) -> int | None:
        """
        Get score for specified team.

        Args:
            team: Team to get score for

        Returns:
            Team's score or None if game not completed
        """
        if team == self.home_team:
            return self.home_score
        elif team == self.away_team:
            return self.away_score
        else:
            raise ValueError(f"Team {team} is not a participant in this game")

    def set_final_score(self, home_score: int, away_score: int) -> None:
        """
        Set final scores and determine winner.

        Args:
            home_score: Home team final score
            away_score: Away team final score
        """
        self.home_score = home_score
        self.away_score = away_score
        self.game_status = GameStatus.FINAL

        # Determine winner
        if home_score > away_score:
            self.winning_team = self.home_team
        elif away_score > home_score:
            self.winning_team = self.away_team
        # else: tie game, winning_team remains None

        self.touch_updated_at()

    def get_correlation_key(self) -> str:
        """
        Generate a key for correlating with external systems.

        Returns:
            String key for correlation matching
        """
        datetime_str = self.game_datetime.strftime("%Y-%m-%d-%H%M")
        return f"{datetime_str}-{self.away_team.value}-{self.home_team.value}"

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

        # Upgrade data quality if we have good enrichment
        if correlation_confidence >= 0.9 and self.data_quality in ["LOW", "MEDIUM"]:
            self.data_quality = "HIGH"

        self.touch_updated_at()

    class Config:
        json_schema_extra = {
            "example": {
                "game_id": "2025-07-01-NYY-BOS",
                "mlb_game_id": "12345",
                "home_team": "BOS",
                "away_team": "NYY",
                "game_date": "2025-07-01",
                "game_datetime": "2025-07-01T19:10:00",
                "season": 2025,
                "venue_info": {
                    "venue_name": "Fenway Park",
                    "city": "Boston",
                    "state": "MA",
                },
            }
        }
