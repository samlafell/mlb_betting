#!/usr/bin/env python3
"""
MLB Schedule Service

Provides comprehensive MLB schedule data retrieval and game discovery for date-range
collections. Integrates with MLB Stats API to fetch game schedules, team information,
and game metadata required for betting line collection.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import aiohttp
import structlog

from ..core.config import UnifiedSettings
from ..core.exceptions import APIError

logger = structlog.get_logger(__name__)


class GameStatus(Enum):
    """MLB game status enumeration."""
    SCHEDULED = "scheduled"
    LIVE = "live"
    FINAL = "final"
    POSTPONED = "postponed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"


@dataclass
class MLBGame:
    """MLB game data model."""
    game_id: str
    game_pk: int
    game_date: datetime
    home_team: str
    away_team: str
    home_team_id: int
    away_team_id: int
    venue: str
    venue_id: int
    status: GameStatus
    season: int
    season_type: str

    # Optional fields for completed games
    home_score: int | None = None
    away_score: int | None = None
    innings: int | None = None

    # SBR integration fields
    sbr_game_id: str | None = None
    sbr_url: str | None = None

    # Timing metadata
    scheduled_time: datetime | None = None
    first_pitch_time: datetime | None = None

    @property
    def is_completed(self) -> bool:
        """Check if game is completed."""
        return self.status in [GameStatus.FINAL, GameStatus.CANCELLED]

    @property
    def game_key(self) -> str:
        """Generate unique game key."""
        return f"{self.season}_{self.game_pk}"

    @property
    def matchup(self) -> str:
        """Get formatted matchup string."""
        return f"{self.away_team} @ {self.home_team}"


@dataclass
class ScheduleRequest:
    """Request parameters for schedule retrieval."""
    start_date: datetime
    end_date: datetime
    season: int | None = None
    season_type: str = "R"  # R=Regular, P=Postseason, S=Spring
    team_ids: list[int] | None = None
    venue_ids: list[int] | None = None

    def to_mlb_params(self) -> dict[str, str]:
        """Convert to MLB API parameters."""
        params = {
            "startDate": self.start_date.strftime("%Y-%m-%d"),
            "endDate": self.end_date.strftime("%Y-%m-%d"),
            "seasonType": self.season_type,
            "sportId": "1",  # MLB
            "hydrate": "team,venue,game(content(media(epg)))",
        }

        if self.season:
            params["season"] = str(self.season)

        if self.team_ids:
            params["teamId"] = ",".join(map(str, self.team_ids))

        if self.venue_ids:
            params["venueIds"] = ",".join(map(str, self.venue_ids))

        return params


class MLBScheduleService:
    """
    MLB Schedule Service for comprehensive game schedule retrieval.
    
    Provides functionality to fetch MLB game schedules for date ranges,
    individual games, and team-specific schedules. Integrates with MLB Stats API
    and provides data formatting for betting line collection systems.
    """

    def __init__(self, settings: UnifiedSettings = None):
        self.settings = settings or UnifiedSettings()
        self.logger = logger.bind(component="MLBScheduleService")
        self.base_url = "https://statsapi.mlb.com/api/v1"
        self.session: aiohttp.ClientSession | None = None

        # Team mapping for common abbreviations
        self.team_mappings = self._initialize_team_mappings()

        # Rate limiting
        self.rate_limit_delay = 0.5  # 500ms between requests

    def _initialize_team_mappings(self) -> dict[str, str]:
        """Initialize team abbreviation mappings."""
        return {
            "ARI": "Arizona Diamondbacks",
            "ATL": "Atlanta Braves",
            "BAL": "Baltimore Orioles",
            "BOS": "Boston Red Sox",
            "CHC": "Chicago Cubs",
            "CWS": "Chicago White Sox",
            "CIN": "Cincinnati Reds",
            "CLE": "Cleveland Guardians",
            "COL": "Colorado Rockies",
            "DET": "Detroit Tigers",
            "HOU": "Houston Astros",
            "KC": "Kansas City Royals",
            "LAA": "Los Angeles Angels",
            "LAD": "Los Angeles Dodgers",
            "MIA": "Miami Marlins",
            "MIL": "Milwaukee Brewers",
            "MIN": "Minnesota Twins",
            "NYM": "New York Mets",
            "NYY": "New York Yankees",
            "OAK": "Oakland Athletics",
            "PHI": "Philadelphia Phillies",
            "PIT": "Pittsburgh Pirates",
            "SD": "San Diego Padres",
            "SF": "San Francisco Giants",
            "SEA": "Seattle Mariners",
            "STL": "St. Louis Cardinals",
            "TB": "Tampa Bay Rays",
            "TEX": "Texas Rangers",
            "TOR": "Toronto Blue Jays",
            "WAS": "Washington Nationals",
        }

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()

    async def _ensure_session(self):
        """Ensure HTTP session is available."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": "MLB-Betting-System/1.0",
                    "Accept": "application/json",
                }
            )

    async def _close_session(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_games_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        season_type: str = "R",
        team_ids: list[int] | None = None
    ) -> list[MLBGame]:
        """
        Get all MLB games for a date range.
        
        Args:
            start_date: Start date for game search
            end_date: End date for game search
            season_type: Season type (R=Regular, P=Postseason, S=Spring)
            team_ids: Optional list of team IDs to filter
            
        Returns:
            List of MLB games
        """
        try:
            await self._ensure_session()

            # Create schedule request
            request = ScheduleRequest(
                start_date=start_date,
                end_date=end_date,
                season_type=season_type,
                team_ids=team_ids
            )

            # Fetch schedule data
            games = await self._fetch_schedule_data(request)

            self.logger.info(
                f"Retrieved {len(games)} games",
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                season_type=season_type
            )

            return games

        except Exception as e:
            self.logger.error("Error fetching games by date range", error=str(e))
            raise APIError(f"Failed to fetch games: {str(e)}", api_name="MLB Stats API")

    async def get_games_by_date(self, date: datetime) -> list[MLBGame]:
        """
        Get all MLB games for a specific date.
        
        Args:
            date: Date to fetch games for
            
        Returns:
            List of MLB games for the date
        """
        return await self.get_games_by_date_range(date, date)

    async def get_game_by_id(self, game_pk: int) -> MLBGame | None:
        """
        Get a specific game by its MLB game ID.
        
        Args:
            game_pk: MLB game primary key
            
        Returns:
            MLB game or None if not found
        """
        try:
            await self._ensure_session()

            url = f"{self.base_url}/game/{game_pk}/feed/live"

            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_game_data(data.get("gameData", {}))
                else:
                    self.logger.warning("Game not found", game_pk=game_pk, status=response.status)
                    return None

        except Exception as e:
            self.logger.error("Error fetching game by ID", game_pk=game_pk, error=str(e))
            return None

    async def _fetch_schedule_data(self, request: ScheduleRequest) -> list[MLBGame]:
        """Fetch schedule data from MLB API."""
        games = []

        # Split large date ranges into chunks to avoid API limits
        current_date = request.start_date
        while current_date <= request.end_date:
            chunk_end = min(current_date + timedelta(days=30), request.end_date)

            chunk_request = ScheduleRequest(
                start_date=current_date,
                end_date=chunk_end,
                season_type=request.season_type,
                team_ids=request.team_ids
            )

            chunk_games = await self._fetch_schedule_chunk(chunk_request)
            games.extend(chunk_games)

            current_date = chunk_end + timedelta(days=1)

            # Rate limiting
            await asyncio.sleep(self.rate_limit_delay)

        return games

    async def _fetch_schedule_chunk(self, request: ScheduleRequest) -> list[MLBGame]:
        """Fetch a chunk of schedule data."""
        try:
            url = f"{self.base_url}/schedule"
            params = request.to_mlb_params()

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_schedule_response(data)
                else:
                    self.logger.warning("Schedule API error", status=response.status)
                    return []

        except Exception as e:
            self.logger.error("Error fetching schedule chunk", error=str(e))
            return []

    def _parse_schedule_response(self, data: dict[str, Any]) -> list[MLBGame]:
        """Parse MLB schedule API response."""
        games = []

        for date_data in data.get("dates", []):
            for game_data in date_data.get("games", []):
                try:
                    game = self._parse_game_data(game_data)
                    if game:
                        games.append(game)
                except Exception as e:
                    self.logger.error("Error parsing game data", error=str(e))
                    continue

        return games

    def _parse_game_data(self, game_data: dict[str, Any]) -> MLBGame | None:
        """Parse individual game data."""
        try:
            # Basic game info
            game_pk = game_data.get("gamePk")
            if not game_pk:
                return None

            # Game timing
            game_date_str = game_data.get("gameDate")
            if not game_date_str:
                return None

            game_date = datetime.fromisoformat(game_date_str.replace("Z", "+00:00"))

            # Teams
            teams = game_data.get("teams", {})
            home_team = teams.get("home", {})
            away_team = teams.get("away", {})

            # Venue
            venue = game_data.get("venue", {})

            # Status
            status_data = game_data.get("status", {})
            status_code = status_data.get("statusCode", "S")

            # Map status codes to enum
            status_mapping = {
                "S": GameStatus.SCHEDULED,
                "P": GameStatus.SCHEDULED,
                "I": GameStatus.LIVE,
                "F": GameStatus.FINAL,
                "D": GameStatus.POSTPONED,
                "C": GameStatus.CANCELLED,
                "U": GameStatus.SUSPENDED,
            }

            status = status_mapping.get(status_code, GameStatus.SCHEDULED)

            # Scores (if available)
            home_score = None
            away_score = None

            if status == GameStatus.FINAL:
                home_score = home_team.get("score")
                away_score = away_team.get("score")

            # Create game object
            game = MLBGame(
                game_id=f"mlb_{game_pk}",
                game_pk=game_pk,
                game_date=game_date,
                home_team=home_team.get("team", {}).get("abbreviation", ""),
                away_team=away_team.get("team", {}).get("abbreviation", ""),
                home_team_id=home_team.get("team", {}).get("id", 0),
                away_team_id=away_team.get("team", {}).get("id", 0),
                venue=venue.get("name", ""),
                venue_id=venue.get("id", 0),
                status=status,
                season=game_data.get("season", datetime.now().year),
                season_type=game_data.get("seasonType", "R"),
                home_score=home_score,
                away_score=away_score,
                scheduled_time=game_date,
            )

            return game

        except Exception as e:
            self.logger.error("Error parsing game data", error=str(e))
            return None

    async def get_team_schedule(
        self,
        team_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> list[MLBGame]:
        """
        Get schedule for specific team.
        
        Args:
            team_id: MLB team ID
            start_date: Start date
            end_date: End date
            
        Returns:
            List of games for the team
        """
        return await self.get_games_by_date_range(
            start_date=start_date,
            end_date=end_date,
            team_ids=[team_id]
        )

    def get_team_id_by_abbr(self, abbreviation: str) -> int | None:
        """
        Get team ID by abbreviation.
        
        Args:
            abbreviation: Team abbreviation (e.g., 'NYY')
            
        Returns:
            Team ID or None if not found
        """
        # This would need to be populated with actual team IDs
        team_ids = {
            "ARI": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
            "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
            "HOU": 117, "KC": 118, "LAA": 108, "LAD": 119, "MIA": 146,
            "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "OAK": 133,
            "PHI": 143, "PIT": 134, "SD": 135, "SF": 137, "SEA": 136,
            "STL": 138, "TB": 139, "TEX": 140, "TOR": 141, "WAS": 120,
        }

        return team_ids.get(abbreviation.upper())

    async def validate_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> tuple[bool, str]:
        """
        Validate date range for data availability.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            Tuple of (is_valid, message)
        """
        # Check date range validity
        if start_date > end_date:
            return False, "Start date must be before end date"

        # Check if date range is too large
        max_days = 365
        if (end_date - start_date).days > max_days:
            return False, f"Date range too large (max {max_days} days)"

        # Check for future dates beyond reasonable limit
        future_limit = datetime.now() + timedelta(days=30)
        if start_date > future_limit:
            return False, "Start date too far in future"

        return True, "Date range is valid"


# Example usage
if __name__ == "__main__":
    async def main():
        async with MLBScheduleService() as service:
            # Get games for a specific date range
            start_date = datetime(2025, 3, 15)
            end_date = datetime(2025, 3, 20)

            games = await service.get_games_by_date_range(start_date, end_date)

            print(f"Found {len(games)} games:")
            for game in games[:5]:  # Show first 5
                print(f"  {game.matchup} - {game.game_date} - {game.status.value}")

    asyncio.run(main())
