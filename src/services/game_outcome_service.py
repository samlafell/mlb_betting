#!/usr/bin/env python3
"""
Game Outcome Service

This service automatically checks for completed games and updates the
core_betting.game_outcomes table with final scores and betting outcomes.
Integrates with the Action Network flow to ensure game outcomes are
updated whenever the Action Network pipeline runs.

Features:
- Fetches completed games from MLB-StatsAPI
- Calculates betting outcomes (home_win, over/under, spread cover)
- Updates core_betting.game_outcomes table
- Handles time zone conversions (UTC -> EST)
- Provides comprehensive logging and error handling
- Integrates seamlessly with existing Action Network workflow

General Balls
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import structlog

from ..core.config import get_settings
from ..core.exceptions import DataError
from ..data.database.connection import get_connection

logger = structlog.get_logger(__name__)


@dataclass
class GameOutcome:
    """Represents a completed game's outcome and betting results."""

    game_id: int  # core_betting.games.id
    mlb_stats_api_game_id: str  # MLB API game PK
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    game_datetime: datetime

    # Betting outcomes
    home_win: bool
    over: bool
    home_cover_spread: bool | None = None

    # Context for calculations
    total_line: float | None = None
    home_spread_line: float | None = None

    # Metadata
    source: str = "mlb_stats_api"
    data_quality: str = "HIGH"


class MLBStatsAPIClient:
    """Client for fetching game data from MLB Stats API."""

    BASE_URL = "https://statsapi.mlb.com/api/v1"

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session: aiohttp.ClientSession | None = None

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

    async def get_games_for_date(self, date_str: str) -> list[dict[str, Any]]:
        """
        Get all games for a specific date.

        Args:
            date_str: Date in YYYY-MM-DD format

        Returns:
            List of game data dictionaries
        """
        if not self.session:
            raise DataError("Client session not initialized")

        url = f"{self.BASE_URL}/schedule"
        params = {
            "sportId": 1,  # MLB
            "date": date_str,
            "hydrate": "team,linescore,decisions",
        }

        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("dates", [{}])[0].get("games", [])
                else:
                    logger.warning(
                        "MLB API request failed", status=response.status, date=date_str
                    )
                    return []
        except Exception as e:
            logger.error("MLB API request error", error=str(e), date=date_str)
            return []

    async def get_game_details(self, game_pk: str) -> dict[str, Any] | None:
        """
        Get detailed information for a specific game.

        Args:
            game_pk: MLB Stats API game PK

        Returns:
            Game details dictionary or None if error
        """
        if not self.session:
            raise DataError("Client session not initialized")

        url = f"{self.BASE_URL}/game/{game_pk}/feed/live"

        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(
                        "Game details request failed",
                        status=response.status,
                        game_pk=game_pk,
                    )
                    return None
        except Exception as e:
            logger.error("Game details request error", error=str(e), game_pk=game_pk)
            return None


class GameOutcomeService:
    """
    Service for checking and updating game outcomes.

    This service integrates with the Action Network flow to automatically
    check for completed games and update the core_betting.game_outcomes table.
    """

    def __init__(self):
        """Initialize the game outcome service."""
        self.settings = get_settings()
        self.logger = logger.bind(service="GameOutcomeService")

        # Team abbreviation mappings
        self.team_mappings = {
            # MLB Stats API team names to our abbreviations
            "Los Angeles Angels": "LAA",
            "Houston Astros": "HOU",
            "Oakland Athletics": "OAK",
            "Seattle Mariners": "SEA",
            "Texas Rangers": "TEX",
            "New York Yankees": "NYY",
            "Boston Red Sox": "BOS",
            "Baltimore Orioles": "BAL",
            "Tampa Bay Rays": "TB",
            "Toronto Blue Jays": "TOR",
            "Chicago White Sox": "CWS",
            "Cleveland Guardians": "CLE",
            "Detroit Tigers": "DET",
            "Kansas City Royals": "KC",
            "Minnesota Twins": "MIN",
            "Atlanta Braves": "ATL",
            "Miami Marlins": "MIA",
            "New York Mets": "NYM",
            "Philadelphia Phillies": "PHI",
            "Washington Nationals": "WSH",
            "Chicago Cubs": "CHC",
            "Cincinnati Reds": "CIN",
            "Milwaukee Brewers": "MIL",
            "Pittsburgh Pirates": "PIT",
            "St. Louis Cardinals": "STL",
            "Arizona Diamondbacks": "ARI",
            "Colorado Rockies": "COL",
            "Los Angeles Dodgers": "LAD",
            "San Diego Padres": "SD",
            "San Francisco Giants": "SF",
        }

        self.logger.info("GameOutcomeService initialized")

    async def check_and_update_game_outcomes(
        self, date_range: tuple[str, str] | None = None, force_update: bool = False
    ) -> dict[str, Any]:
        """
        Check for completed games and update outcomes.

        Args:
            date_range: Optional tuple of (start_date, end_date) in YYYY-MM-DD format
            force_update: If True, update even if outcome already exists

        Returns:
            Dictionary with update results and statistics
        """
        self.logger.info(
            "Starting game outcome check",
            date_range=date_range,
            force_update=force_update,
        )

        # Default to checking last 3 days if no range specified
        if not date_range:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=3)
            date_range = (
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )

        results = {
            "processed_games": 0,
            "completed_games": 0,
            "updated_outcomes": 0,
            "skipped_games": 0,
            "errors": [],
            "date_range": date_range,
        }

        try:
            # Get all games in date range that need outcome checking
            games_to_check = await self._get_games_needing_outcomes(
                date_range, force_update
            )

            self.logger.info("Found games to check", count=len(games_to_check))

            # Process each game
            async with MLBStatsAPIClient() as mlb_client:
                for game_info in games_to_check:
                    results["processed_games"] += 1

                    try:
                        outcome = await self._check_game_outcome(mlb_client, game_info)

                        if outcome:
                            await self._update_game_outcome(outcome)
                            results["updated_outcomes"] += 1
                            results["completed_games"] += 1

                            self.logger.info(
                                "Updated game outcome",
                                game_id=outcome.game_id,
                                home_team=outcome.home_team,
                                away_team=outcome.away_team,
                                home_score=outcome.home_score,
                                away_score=outcome.away_score,
                            )
                        else:
                            results["skipped_games"] += 1

                    except Exception as e:
                        error_msg = f"Error processing game {game_info.get('id', 'unknown')}: {str(e)}"
                        results["errors"].append(error_msg)
                        self.logger.error(
                            "Game processing error",
                            game_id=game_info.get("id"),
                            error=str(e),
                        )

            self.logger.info("Game outcome check completed", results=results)
            return results

        except Exception as e:
            self.logger.error("Game outcome check failed", error=str(e))
            results["errors"].append(f"Service error: {str(e)}")
            return results

    async def _get_games_needing_outcomes(
        self, date_range: tuple[str, str], force_update: bool
    ) -> list[dict[str, Any]]:
        """
        Get games that need outcome checking from the database.

        Args:
            date_range: Tuple of (start_date, end_date)
            force_update: Whether to include games that already have outcomes

        Returns:
            List of game dictionaries from core_betting.games
        """
        start_date, end_date = date_range

        # SQL query to get games needing outcome updates
        if force_update:
            # Get all games in date range
            query = """
            SELECT g.id, g.mlb_stats_api_game_id, g.home_team, g.away_team,
                   g.game_datetime, g.game_status
            FROM core_betting.games g
            WHERE g.game_date BETWEEN %s AND %s
              AND g.mlb_stats_api_game_id IS NOT NULL
            ORDER BY g.game_datetime DESC
            """
            params = [start_date, end_date]
        else:
            # Only get games without outcomes
            query = """
            SELECT g.id, g.mlb_stats_api_game_id, g.home_team, g.away_team,
                   g.game_datetime, g.game_status
            FROM core_betting.games g
            LEFT JOIN core_betting.game_outcomes go ON g.id = go.game_id
            WHERE g.game_date BETWEEN %s AND %s
              AND g.mlb_stats_api_game_id IS NOT NULL
              AND go.game_id IS NULL
            ORDER BY g.game_datetime DESC
            """
            params = [start_date, end_date]

        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    rows = await cursor.fetchall()

                    # Convert to list of dictionaries
                    games = []
                    for row in rows:
                        games.append(
                            {
                                "id": row[0],
                                "mlb_stats_api_game_id": row[1],
                                "home_team": row[2],
                                "away_team": row[3],
                                "game_datetime": row[4],
                                "game_status": row[5],
                            }
                        )

                    return games

        except Exception as e:
            self.logger.error("Database query error", error=str(e))
            return []

    async def _check_game_outcome(
        self, mlb_client: MLBStatsAPIClient, game_info: dict[str, Any]
    ) -> GameOutcome | None:
        """
        Check if a game is completed and get the outcome.

        Args:
            mlb_client: MLB Stats API client
            game_info: Game information from database

        Returns:
            GameOutcome object if game is completed, None otherwise
        """
        mlb_game_id = game_info["mlb_stats_api_game_id"]
        if not mlb_game_id:
            return None

        # Get game details from MLB API
        game_data = await mlb_client.get_game_details(mlb_game_id)
        if not game_data:
            return None

        # Store raw response before processing
        await self._store_raw_response(mlb_game_id, game_data)

        # Check if game is completed
        game_state = game_data.get("gameData", {}).get("status", {})
        status_code = game_state.get("statusCode", "")

        # Game is completed if status is 'F' (Final) or 'O' (Official)
        if status_code not in ["F", "O"]:
            self.logger.debug(
                "Game not completed", game_id=mlb_game_id, status=status_code
            )
            return None

        # Extract game details
        live_data = game_data.get("liveData", {})
        linescore = live_data.get("linescore", {})

        # Get final scores
        home_score = linescore.get("teams", {}).get("home", {}).get("runs", 0)
        away_score = linescore.get("teams", {}).get("away", {}).get("runs", 0)

        # Get team information
        game_teams = game_data.get("gameData", {}).get("teams", {})
        home_team_name = game_teams.get("home", {}).get("name", "")
        away_team_name = game_teams.get("away", {}).get("name", "")

        # Map team names to abbreviations
        home_team_abbr = self.team_mappings.get(home_team_name, game_info["home_team"])
        away_team_abbr = self.team_mappings.get(away_team_name, game_info["away_team"])

        # Calculate betting outcomes
        home_win = home_score > away_score
        total_score = home_score + away_score

        # Get betting lines for this game to calculate over/under and spread
        betting_lines = await self._get_betting_lines(game_info["id"])

        # Calculate over/under
        over = None
        total_line = None
        if betting_lines.get("total_line"):
            total_line = float(betting_lines["total_line"])
            over = total_score > total_line

        # Calculate spread cover
        home_cover_spread = None
        home_spread_line = None
        if betting_lines.get("home_spread_line"):
            home_spread_line = float(betting_lines["home_spread_line"])
            # If home spread is negative, they're favored
            # If positive, they're getting points
            home_cover_spread = (home_score + home_spread_line) > away_score

        # Get game datetime in EST
        game_datetime_str = (
            game_data.get("gameData", {}).get("datetime", {}).get("dateTime", "")
        )
        if game_datetime_str:
            # Parse UTC datetime and convert to EST
            game_datetime = datetime.fromisoformat(
                game_datetime_str.replace("Z", "+00:00")
            )
            est_timezone = timezone(timedelta(hours=-5))  # EST is UTC-5
            game_datetime = game_datetime.astimezone(est_timezone)
        else:
            game_datetime = game_info["game_datetime"]

        return GameOutcome(
            game_id=game_info["id"],
            mlb_stats_api_game_id=mlb_game_id,
            home_team=home_team_abbr,
            away_team=away_team_abbr,
            home_score=home_score,
            away_score=away_score,
            game_datetime=game_datetime,
            home_win=home_win,
            over=over,
            home_cover_spread=home_cover_spread,
            total_line=total_line,
            home_spread_line=home_spread_line,
        )

    async def _store_raw_response(self, mlb_game_id: str, game_data: dict[str, Any]) -> None:
        """
        Store raw MLB Stats API response in raw_data.mlb_game_outcomes table.

        Args:
            mlb_game_id: MLB Stats API game PK
            game_data: Complete JSON response from MLB Stats API
        """
        try:
            # Extract basic info from response for quick access
            game_state = game_data.get("gameData", {}).get("status", {})
            game_status = game_state.get("abstractGameState", "")
            
            # Extract team info
            teams = game_data.get("gameData", {}).get("teams", {})
            home_team = teams.get("home", {}).get("name", "")
            away_team = teams.get("away", {}).get("name", "")
            
            # Extract game date
            game_datetime_str = game_data.get("gameData", {}).get("datetime", {}).get("dateTime", "")
            game_date = None
            if game_datetime_str:
                try:
                    game_datetime = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
                    game_date = game_datetime.date()
                except ValueError:
                    pass
            
            # Extract scores if available
            linescore = game_data.get("liveData", {}).get("linescore", {})
            home_score = None
            away_score = None
            is_final = False
            
            if linescore:
                home_runs = linescore.get("teams", {}).get("home", {}).get("runs")
                away_runs = linescore.get("teams", {}).get("away", {}).get("runs")
                if home_runs is not None and away_runs is not None:
                    home_score = int(home_runs)
                    away_score = int(away_runs)
                    
            # Check if game is final
            status_code = game_state.get("statusCode", "")
            is_final = status_code in ["F", "O"]  # Final or Official

            # Insert raw response
            query = """
            INSERT INTO raw_data.mlb_game_outcomes (
                mlb_game_pk, mlb_stats_api_game_id, api_endpoint, 
                raw_response, game_date, home_team, away_team, 
                game_status, home_score, away_score, is_final_game
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (mlb_game_pk, request_timestamp) DO UPDATE SET
                raw_response = EXCLUDED.raw_response,
                game_status = EXCLUDED.game_status,
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                is_final_game = EXCLUDED.is_final_game,
                updated_at = NOW()
            """

            params = [
                mlb_game_id,  # mlb_game_pk
                mlb_game_id,  # mlb_stats_api_game_id (same value)
                f"/api/v1/game/{mlb_game_id}/feed/live",  # api_endpoint
                json.dumps(game_data),  # raw_response
                game_date,  # game_date
                home_team,  # home_team
                away_team,  # away_team
                game_status,  # game_status
                home_score,  # home_score
                away_score,  # away_score
                is_final,  # is_final_game
            ]

            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    await conn.commit()

            self.logger.debug(
                "Stored raw MLB response",
                game_id=mlb_game_id,
                game_status=game_status,
                is_final=is_final,
            )

        except Exception as e:
            self.logger.error(
                "Error storing raw MLB response",
                game_id=mlb_game_id,
                error=str(e),
            )
            # Don't raise - raw storage failure shouldn't block outcome processing

    async def _get_betting_lines(self, game_id: int) -> dict[str, Any]:
        """
        Get the latest betting lines for a game.

        Args:
            game_id: Game ID from core_betting.games

        Returns:
            Dictionary with betting line information
        """
        query = """
        SELECT 
            t.total_line,
            s.home_spread
        FROM core_betting.games g
        LEFT JOIN (
            SELECT game_id, total_line
            FROM core_betting.betting_lines_totals
            WHERE game_id = %s
            ORDER BY odds_timestamp DESC
            LIMIT 1
        ) t ON g.id = t.game_id
        LEFT JOIN (
            SELECT game_id, home_spread
            FROM core_betting.betting_lines_spreads
            WHERE game_id = %s
            ORDER BY odds_timestamp DESC
            LIMIT 1
        ) s ON g.id = s.game_id
        WHERE g.id = %s
        """

        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, [game_id, game_id, game_id])
                    row = await cursor.fetchone()

                    if row:
                        return {"total_line": row[0], "home_spread_line": row[1]}
                    else:
                        return {}

        except Exception as e:
            self.logger.error(
                "Error getting betting lines", game_id=game_id, error=str(e)
            )
            return {}

    async def _update_game_outcome(self, outcome: GameOutcome) -> None:
        """
        Update the game outcome in the database.

        Args:
            outcome: GameOutcome object to insert/update
        """
        query = """
        INSERT INTO core_betting.game_outcomes (
            game_id, home_team, away_team, home_score, away_score,
            home_win, over, home_cover_spread, total_line, home_spread_line,
            game_date, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (game_id) DO UPDATE SET
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_win = EXCLUDED.home_win,
            over = EXCLUDED.over,
            home_cover_spread = EXCLUDED.home_cover_spread,
            total_line = EXCLUDED.total_line,
            home_spread_line = EXCLUDED.home_spread_line,
            game_date = EXCLUDED.game_date,
            updated_at = NOW()
        """

        params = [
            outcome.game_id,
            outcome.home_team,
            outcome.away_team,
            outcome.home_score,
            outcome.away_score,
            outcome.home_win,
            outcome.over,
            outcome.home_cover_spread,
            outcome.total_line,
            outcome.home_spread_line,
            outcome.game_datetime,
        ]

        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    await conn.commit()

        except Exception as e:
            self.logger.error(
                "Error updating game outcome", game_id=outcome.game_id, error=str(e)
            )
            raise

    async def get_recent_outcomes(self, days: int = 7) -> list[dict[str, Any]]:
        """
        Get recent game outcomes for reporting.

        Args:
            days: Number of days to look back

        Returns:
            List of recent game outcomes
        """
        query = """
        SELECT 
            go.game_id,
            go.home_team,
            go.away_team,
            go.home_score,
            go.away_score,
            go.home_win,
            go.over,
            go.home_cover_spread,
            go.total_line,
            go.home_spread_line,
            go.game_date,
            g.game_status
        FROM core_betting.game_outcomes go
        JOIN core_betting.games g ON go.game_id = g.id
        WHERE go.game_date >= NOW() - INTERVAL '%s days'
        ORDER BY go.game_date DESC
        """

        try:
            async with get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, [days])
                    rows = await cursor.fetchall()

                    outcomes = []
                    for row in rows:
                        outcomes.append(
                            {
                                "game_id": row[0],
                                "home_team": row[1],
                                "away_team": row[2],
                                "home_score": row[3],
                                "away_score": row[4],
                                "home_win": row[5],
                                "over": row[6],
                                "home_cover_spread": row[7],
                                "total_line": row[8],
                                "home_spread_line": row[9],
                                "game_date": row[10],
                                "game_status": row[11],
                            }
                        )

                    return outcomes

        except Exception as e:
            self.logger.error("Error getting recent outcomes", error=str(e))
            return []


# Service instance for easy importing
game_outcome_service = GameOutcomeService()


async def check_game_outcomes(
    date_range: tuple[str, str] | None = None, force_update: bool = False
) -> dict[str, Any]:
    """
    Convenience function to check game outcomes.

    Args:
        date_range: Optional tuple of (start_date, end_date) in YYYY-MM-DD format
        force_update: If True, update even if outcome already exists

    Returns:
        Dictionary with update results and statistics
    """
    return await game_outcome_service.check_and_update_game_outcomes(
        date_range, force_update
    )


if __name__ == "__main__":
    # Example usage
    async def main():
        # Check outcomes for the last 3 days
        results = await check_game_outcomes()
        print(json.dumps(results, indent=2, default=str))

        # Get recent outcomes
        recent = await game_outcome_service.get_recent_outcomes(days=7)
        print(f"\nFound {len(recent)} recent outcomes")

    asyncio.run(main())
