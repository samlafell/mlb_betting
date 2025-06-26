"""
Game models for the MLB Sharp Betting system.

This module provides models for representing MLB games, teams,
and game-related data structures.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import Field, validator

from mlb_sharp_betting.models.base import IdentifiedModel, ValidatedModel


class GameStatus(str, Enum):
    """Enumeration of possible game statuses."""
    
    SCHEDULED = "scheduled"
    LIVE = "live"
    FINAL = "final"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"
    DELAYED = "delayed"


class Team(str, Enum):
    """Enumeration of MLB teams with their standard abbreviations."""
    
    # American League East
    BAL = "BAL"  # Baltimore Orioles
    BOS = "BOS"  # Boston Red Sox
    NYY = "NYY"  # New York Yankees
    TB = "TB"    # Tampa Bay Rays
    TOR = "TOR"  # Toronto Blue Jays
    
    # American League Central
    CWS = "CWS"  # Chicago White Sox
    CLE = "CLE"  # Cleveland Guardians
    DET = "DET"  # Detroit Tigers
    KC = "KC"    # Kansas City Royals
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
    SD = "SD"    # San Diego Padres
    SF = "SF"    # San Francisco Giants
    
    @classmethod
    def get_team_name(cls, abbreviation: str) -> str:
        """
        Get the full team name from abbreviation.
        
        Args:
            abbreviation: Team abbreviation
            
        Returns:
            Full team name
        """
        team_names = {
            # American League East
            "BAL": "Baltimore Orioles",
            "BOS": "Boston Red Sox", 
            "NYY": "New York Yankees",
            "TB": "Tampa Bay Rays",
            "TOR": "Toronto Blue Jays",
            
            # American League Central
            "CWS": "Chicago White Sox",
            "CLE": "Cleveland Guardians",
            "DET": "Detroit Tigers",
            "KC": "Kansas City Royals",
            "MIN": "Minnesota Twins",
            
            # American League West
            "HOU": "Houston Astros",
            "LAA": "Los Angeles Angels",
            "OAK": "Oakland Athletics",
            "SEA": "Seattle Mariners",
            "TEX": "Texas Rangers",
            
            # National League East
            "ATL": "Atlanta Braves",
            "MIA": "Miami Marlins",
            "NYM": "New York Mets",
            "PHI": "Philadelphia Phillies",
            "WSH": "Washington Nationals",
            
            # National League Central
            "CHC": "Chicago Cubs",
            "CIN": "Cincinnati Reds",
            "MIL": "Milwaukee Brewers",
            "PIT": "Pittsburgh Pirates",
            "STL": "St. Louis Cardinals",
            
            # National League West
            "ARI": "Arizona Diamondbacks",
            "COL": "Colorado Rockies",
            "LAD": "Los Angeles Dodgers",
            "SD": "San Diego Padres",
            "SF": "San Francisco Giants",
        }
        
        return team_names.get(abbreviation, abbreviation)
    
    @classmethod
    def normalize_team_name(cls, name: str) -> Optional[str]:
        """
        Normalize team name to standard abbreviation.
        
        Args:
            name: Team name or abbreviation
            
        Returns:
            Standardized team abbreviation or None if not found
        """
        name = name.strip().upper()
        
        # Check if it's already a valid abbreviation
        try:
            return cls(name).value
        except ValueError:
            pass
        
        # Common name mappings
        name_mappings = {
            # Alternative abbreviations
            "WSN": "WSH",
            "WAS": "WSH", 
            "CWS": "CWS",  # Chicago White Sox
            "CHW": "CWS",
            "LAA": "LAA",  # Los Angeles Angels
            "ANA": "LAA",
            "TBR": "TB",   # Tampa Bay Rays
            "TBD": "TB",
            
            # Full names
            "BALTIMORE ORIOLES": "BAL",
            "BOSTON RED SOX": "BOS",
            "NEW YORK YANKEES": "NYY",
            "TAMPA BAY RAYS": "TB",
            "TORONTO BLUE JAYS": "TOR",
            "CHICAGO WHITE SOX": "CWS",
            "CLEVELAND GUARDIANS": "CLE",
            "DETROIT TIGERS": "DET",
            "KANSAS CITY ROYALS": "KC",
            "MINNESOTA TWINS": "MIN",
            "HOUSTON ASTROS": "HOU",
            "LOS ANGELES ANGELS": "LAA",
            "OAKLAND ATHLETICS": "OAK",
            "SEATTLE MARINERS": "SEA",
            "TEXAS RANGERS": "TEX",
            "ATLANTA BRAVES": "ATL",
            "MIAMI MARLINS": "MIA",
            "NEW YORK METS": "NYM",
            "PHILADELPHIA PHILLIES": "PHI",
            "WASHINGTON NATIONALS": "WSH",
            "CHICAGO CUBS": "CHC",
            "CINCINNATI REDS": "CIN",
            "MILWAUKEE BREWERS": "MIL",
            "PITTSBURGH PIRATES": "PIT",
            "ST. LOUIS CARDINALS": "STL",
            "ARIZONA DIAMONDBACKS": "ARI",
            "COLORADO ROCKIES": "COL",
            "LOS ANGELES DODGERS": "LAD",
            "SAN DIEGO PADRES": "SD",
            "SAN FRANCISCO GIANTS": "SF",
        }
        
        return name_mappings.get(name)


class Game(IdentifiedModel, ValidatedModel):
    """
    Model representing an MLB game.
    
    Contains all relevant information about a game including
    teams, scheduling, and current status.
    """
    
    # Game identification
    game_id: str = Field(
        ...,
        description="Unique identifier for the game",
        min_length=1,
        max_length=100
    )
    
    # Teams
    home_team: Team = Field(
        ...,
        description="Home team abbreviation"
    )
    
    away_team: Team = Field(
        ...,
        description="Away team abbreviation"
    )
    
    # Scheduling
    game_datetime: datetime = Field(
        ...,
        description="Scheduled game start time"
    )
    
    # Status
    status: GameStatus = Field(
        default=GameStatus.SCHEDULED,
        description="Current game status"
    )
    
    # Venue
    venue_name: Optional[str] = Field(
        default=None,
        description="Name of the venue where the game is played",
        max_length=100
    )
    
    # Season information
    season: int = Field(
        ...,
        description="Season year",
        ge=1876,  # First MLB season
        le=2030    # Reasonable future limit
    )
    
    season_type: str = Field(
        default="regular",
        description="Type of season (regular, postseason, spring)",
        pattern="^(regular|postseason|spring)$"
    )
    
    # Game number (for doubleheaders)
    game_number: int = Field(
        default=1,
        description="Game number (1 for single games, 1-2 for doubleheaders)",
        ge=1,
        le=2
    )
    
    # Score information (optional, populated after game completion)
    home_score: Optional[int] = Field(
        default=None,
        description="Home team final score",
        ge=0
    )
    
    away_score: Optional[int] = Field(
        default=None,
        description="Away team final score", 
        ge=0
    )
    
    # Outcome
    winning_team: Optional[Team] = Field(
        default=None,
        description="Winning team (if game is complete)"
    )
    
    @validator("home_team", "away_team")
    def validate_teams_different(cls, v: Team, values: Dict[str, Any]) -> Team:
        """Ensure home and away teams are different."""
        if "home_team" in values and v == values["home_team"]:
            raise ValueError("Home and away teams must be different")
        return v
    
    @validator("winning_team")
    def validate_winning_team(cls, v: Optional[Team], values: Dict[str, Any]) -> Optional[Team]:
        """Ensure winning team is one of the playing teams."""
        if v is None:
            return None
        
        home_team = values.get("home_team")
        away_team = values.get("away_team")
        
        if home_team and away_team and v not in [home_team, away_team]:
            raise ValueError("Winning team must be either home or away team")
        
        return v
    
    @validator("status")
    def validate_status_completion(cls, v: GameStatus, values: Dict[str, Any]) -> GameStatus:
        """Validate status consistency with completion data."""
        if v == GameStatus.FINAL:
            # For final games, we should have score information
            if values.get("home_score") is None or values.get("away_score") is None:
                # This is a warning, not an error, as scores might be populated later
                pass
        
        return v
    
    @property
    def is_complete(self) -> bool:
        """Check if the game is complete."""
        return self.status in [GameStatus.FINAL, GameStatus.CANCELLED]
    
    @property
    def is_in_progress(self) -> bool:
        """Check if the game is currently in progress."""
        return self.status == GameStatus.LIVE
    
    @property
    def is_scheduled(self) -> bool:
        """Check if the game is scheduled but not started."""
        return self.status == GameStatus.SCHEDULED
    
    @property
    def matchup_string(self) -> str:
        """Get a string representation of the matchup."""
        return f"{self.away_team.value} @ {self.home_team.value}"
    
    @property
    def home_team_name(self) -> str:
        """Get the full name of the home team."""
        return Team.get_team_name(self.home_team.value)
    
    @property
    def away_team_name(self) -> str:
        """Get the full name of the away team."""
        return Team.get_team_name(self.away_team.value)
    
    def get_team_score(self, team: Team) -> Optional[int]:
        """
        Get the score for a specific team.
        
        Args:
            team: Team to get score for
            
        Returns:
            Team's score or None if not available
        """
        if team == self.home_team:
            return self.home_score
        elif team == self.away_team:
            return self.away_score
        else:
            raise ValueError(f"Team {team} is not playing in this game")
    
    def set_final_score(self, home_score: int, away_score: int) -> None:
        """
        Set the final score and update game status.
        
        Args:
            home_score: Home team final score
            away_score: Away team final score
        """
        self.home_score = home_score
        self.away_score = away_score
        self.status = GameStatus.FINAL
        
        # Determine winning team
        if home_score > away_score:
            self.winning_team = self.home_team
        elif away_score > home_score:
            self.winning_team = self.away_team
        # Ties are possible in some circumstances, winning_team stays None
        
        self.touch()  # Update timestamp
    
    class Config:
        json_schema_extra = {
            "example": {
                "game_id": "2024-04-15-LAD-SF-1",
                "home_team": "SF",
                "away_team": "LAD",
                "game_datetime": "2024-04-15T22:45:00Z",
                "status": "scheduled",
                "venue_name": "Oracle Park",
                "season": 2024,
                "season_type": "regular",
                "game_number": 1,
                "home_score": None,
                "away_score": None,
                "winning_team": None
            }
        } 