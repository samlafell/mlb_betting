"""
MLB Stats API client for fetching game data and results.

This client handles HTTP requests to the MLB Stats API endpoints
to retrieve game information, scores, and other baseball data.
"""

from datetime import date, datetime
from typing import Any

import aiohttp
import structlog

from ..core.exceptions import MLBSharpBettingError
from ..models.game import Team

logger = structlog.get_logger(__name__)


class MLBAPIError(MLBSharpBettingError):
    """Exception for MLB API errors."""

    pass


class MLBStatsAPIClient:
    """
    Client for MLB Stats API.

    Provides methods to fetch game data, schedules, and results
    from the official MLB Stats API.
    """

    BASE_URL = "https://statsapi.mlb.com/api"

    def __init__(self, timeout: int = 30):
        """
        Initialize the MLB API client.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session: aiohttp.ClientSession | None = None

        # Team ID mappings (MLB API uses numeric IDs)
        self.team_id_map = {
            Team.LAA: 108,
            Team.HOU: 117,
            Team.OAK: 133,
            Team.SEA: 136,
            Team.TEX: 140,
            Team.NYY: 147,
            Team.BOS: 111,
            Team.BAL: 110,
            Team.TB: 139,
            Team.TOR: 141,
            Team.CWS: 145,
            Team.CLE: 114,
            Team.DET: 116,
            Team.KC: 118,
            Team.MIN: 142,
            Team.ATL: 144,
            Team.MIA: 146,
            Team.NYM: 121,
            Team.PHI: 143,
            Team.WSH: 120,
            Team.CHC: 112,
            Team.CIN: 113,
            Team.MIL: 158,
            Team.PIT: 134,
            Team.STL: 138,
            Team.ARI: 109,
            Team.COL: 115,
            Team.LAD: 119,
            Team.SD: 135,
            Team.SF: 137,
        }

        # Reverse mapping for API responses
        self.id_team_map = {v: k for k, v in self.team_id_map.items()}

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def _get_json(
        self, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """
        Make GET request and return JSON response.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            JSON response data or None if error
        """
        if not self.session:
            raise MLBAPIError(
                "Client session not initialized. Use async context manager."
            )

        try:
            logger.debug("Making MLB API request", url=url, params=params)

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logger.warning(
                        "MLB API request failed", url=url, status=response.status
                    )
                    return None

        except Exception as e:
            logger.error("MLB API request error", url=url, error=str(e))
            return None

    async def get_schedule(
        self, date_obj: date | datetime | str, sport_id: int = 1
    ) -> dict[str, Any] | None:
        """
        Get game schedule for a specific date.

        Args:
            date_obj: Date to get schedule for
            sport_id: Sport ID (1 for MLB)

        Returns:
            Schedule data with games
        """
        if isinstance(date_obj, (date, datetime)):
            date_str = date_obj.strftime("%Y-%m-%d")
        else:
            date_str = str(date_obj)

        url = f"{self.BASE_URL}/v1/schedule"
        params = {"sportId": sport_id, "date": date_str, "hydrate": "team,linescore"}

        return await self._get_json(url, params)

    async def get_game_live_feed(self, game_pk: str) -> dict[str, Any] | None:
        """
        Get live feed data for a specific game.

        Args:
            game_pk: Game primary key (ID)

        Returns:
            Live game data including score, status, etc.
        """
        url = f"{self.BASE_URL}/v1.1/game/{game_pk}/feed/live"
        return await self._get_json(url)

    async def get_game_boxscore(self, game_pk: str) -> dict[str, Any] | None:
        """
        Get boxscore data for a specific game.

        Args:
            game_pk: Game primary key (ID)

        Returns:
            Boxscore data with detailed stats
        """
        url = f"{self.BASE_URL}/v1/game/{game_pk}/boxscore"
        return await self._get_json(url)

    async def get_game_linescore(self, game_pk: str) -> dict[str, Any] | None:
        """
        Get linescore data for a specific game.

        Args:
            game_pk: Game primary key (ID)

        Returns:
            Linescore data with inning-by-inning scoring
        """
        url = f"{self.BASE_URL}/v1/game/{game_pk}/linescore"
        return await self._get_json(url)

    async def get_completed_games(
        self,
        start_date: date | datetime | str,
        end_date: date | datetime | str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get completed games within a date range.

        Args:
            start_date: Start date for search
            end_date: End date for search (defaults to start_date)

        Returns:
            List of completed games with basic info
        """
        if isinstance(start_date, (date, datetime)):
            start_str = start_date.strftime("%Y-%m-%d")
        else:
            start_str = str(start_date)

        if end_date:
            if isinstance(end_date, (date, datetime)):
                end_str = end_date.strftime("%Y-%m-%d")
            else:
                end_str = str(end_date)
        else:
            end_str = start_str

        url = f"{self.BASE_URL}/v1/schedule"
        params = {
            "sportId": 1,
            "startDate": start_str,
            "endDate": end_str,
            "hydrate": "team,linescore",
            "gameType": "R",  # Regular season
        }

        schedule_data = await self._get_json(url, params)
        if not schedule_data or "dates" not in schedule_data:
            return []

        completed_games = []
        for date_entry in schedule_data["dates"]:
            for game in date_entry.get("games", []):
                # Include completed games (F = Final, FR = Final: Rain, etc.)
                status_code = game.get("status", {}).get("statusCode")
                if status_code in [
                    "F",
                    "FR",
                    "FG",
                ]:  # F=Final, FR=Final:Rain, FG=Final:Game Over
                    completed_games.append(game)

        return completed_games

    def extract_game_result(self, game_data: dict[str, Any]) -> dict[str, Any] | None:
        """
        Extract essential game result information from API response.

        Args:
            game_data: Raw game data from API

        Returns:
            Processed game result data
        """
        try:
            # Extract basic game info
            game_pk = str(game_data.get("gamePk", ""))
            if not game_pk:
                return None

            # Extract teams
            teams = game_data.get("teams", {})
            home_team_data = teams.get("home", {}).get("team", {})
            away_team_data = teams.get("away", {}).get("team", {})

            home_team_id = home_team_data.get("id")
            away_team_id = away_team_data.get("id")

            if not home_team_id or not away_team_id:
                return None

            # Convert to our Team enum
            home_team = self.id_team_map.get(home_team_id)
            away_team = self.id_team_map.get(away_team_id)

            if not home_team or not away_team:
                logger.warning(
                    "Unknown team IDs", home_id=home_team_id, away_id=away_team_id
                )
                return None

            # Extract scores from linescore
            linescore = game_data.get("linescore", {})
            home_score = linescore.get("teams", {}).get("home", {}).get("runs", 0)
            away_score = linescore.get("teams", {}).get("away", {}).get("runs", 0)

            # Extract game date
            game_date_str = game_data.get("gameDate")
            game_date = None
            if game_date_str:
                try:
                    game_date = datetime.fromisoformat(
                        game_date_str.replace("Z", "+00:00")
                    )
                except:
                    pass

            return {
                "game_id": game_pk,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "game_date": game_date,
                "home_win": home_score > away_score,
            }

        except Exception as e:
            logger.error(
                "Failed to extract game result", error=str(e), game_data=game_data
            )
            return None


__all__ = ["MLBStatsAPIClient", "MLBAPIError"]
