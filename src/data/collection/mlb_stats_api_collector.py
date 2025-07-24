#!/usr/bin/env python3
"""
MLB Stats API Collector

This collector fetches authoritative game data from the official MLB Stats API.
It serves as the canonical source for game identification and cross-reference mapping.
"""

import asyncio
import json
from datetime import datetime
from typing import Any

import aiohttp
import asyncpg
import structlog

from ...core.config import get_settings
from ...core.datetime_utils import (
    now_est,
    safe_game_datetime_parse,
)
from ...core.team_utils import normalize_team_name
from .base import BaseCollector, CollectionRequest, CollectorConfig

logger = structlog.get_logger(__name__)


class MLBStatsAPIClient:
    """HTTP client for MLB Stats API calls."""

    def __init__(self, db_config: dict):
        self.api_base = "https://statsapi.mlb.com/api/v1"
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "MLB Betting Program/1.0",
        }
        self.session = None
        self.db_config = db_config

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def fetch_schedule(
        self, date: str, season: int = None
    ) -> list[dict[str, Any]]:
        """Fetch games schedule from MLB Stats API."""
        session = await self._get_session()

        # Use current year if season not specified
        if season is None:
            season = datetime.now().year

        url = f"{self.api_base}/schedule"
        params = {
            "sportId": "1",  # MLB
            "date": date,
            "season": str(season),
            "gameType": "R,P,S,E,A,I,W",  # All game types
            "hydrate": "team,venue,game(content(editorial(recap)))",
        }

        logger.info("Fetching MLB schedule", url=url, date=date, season=season)

        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                games = []
                for date_entry in data.get("dates", []):
                    games.extend(date_entry.get("games", []))
                return games
            else:
                logger.error(
                    "MLB Stats API request failed", status=response.status, url=url
                )
                return []

    async def fetch_game_detail(self, game_pk: int) -> dict[str, Any]:
        """Fetch detailed game data."""
        session = await self._get_session()

        url = f"{self.api_base}/game/{game_pk}/feed/live"

        logger.info("Fetching game detail", game_pk=game_pk, url=url)

        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(
                    "Game detail API request failed",
                    status=response.status,
                    game_pk=game_pk,
                )
                return {}

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None


class MLBStatsAPICollector(BaseCollector):
    """
    MLB Stats API collector for authoritative game data.

    This collector fetches official game data from MLB and serves as the
    canonical source for game identification across all other data sources.
    """

    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        # Use centralized database configuration
        settings = get_settings()
        self.db_config = {
            "host": settings.database.host,
            "port": settings.database.port,
            "database": settings.database.database,
            "user": settings.database.user,
            "password": settings.database.password,
        }
        self.client = MLBStatsAPIClient(self.db_config)

        # Statistics tracking
        self.stats = {
            "games_found": 0,
            "games_processed": 0,
            "games_stored": 0,
            "errors": 0,
        }

    async def collect_data(self, request: CollectionRequest) -> list[dict[str, Any]]:
        """Main collection method for MLB Stats API data."""
        try:
            target_date = request.start_date if request.start_date else datetime.now()
            date_str = target_date.strftime("%Y-%m-%d")
            season = target_date.year

            logger.info(
                "Starting MLB Stats API collection", date=date_str, season=season
            )

            # Fetch schedule for the date
            games = await self.client.fetch_schedule(date_str, season)

            if not games:
                logger.warning(
                    "No games found for MLB Stats API collection", date=date_str
                )
                return []

            self.stats["games_found"] = len(games)
            logger.info("Found games from MLB Stats API", count=len(games))

            # Store raw game data
            await self._store_raw_game_data(games, date_str)

            return games

        except Exception as e:
            logger.error("MLB Stats API collection failed", error=str(e))
            self.stats["errors"] += 1
            return []

        finally:
            await self.client.close()

    async def _store_raw_game_data(
        self, games: list[dict[str, Any]], date_str: str
    ) -> None:
        """Store raw game data to raw_data.mlb_stats_api_games table."""
        if not games:
            return

        try:
            conn = await asyncpg.connect(**self.db_config)

            for game in games:
                try:
                    # Extract key fields
                    game_pk = game.get("gamePk")
                    if not game_pk:
                        continue

                    external_game_id = str(game_pk)

                    # Extract team information
                    teams = game.get("teams", {})
                    home_team_data = teams.get("home", {}).get("team", {})
                    away_team_data = teams.get("away", {}).get("team", {})

                    home_team = normalize_team_name(home_team_data.get("name", ""))
                    away_team = normalize_team_name(away_team_data.get("name", ""))

                    # Extract game details
                    game_date_str = game.get("gameDate")
                    game_datetime = (
                        safe_game_datetime_parse(game_date_str)
                        if game_date_str
                        else None
                    )

                    season = game.get("season")
                    season_type = game.get("gameType")

                    # Venue information
                    venue = game.get("venue", {})
                    venue_id = venue.get("id")
                    venue_name = venue.get("name")

                    # Game status
                    status = game.get("status", {})
                    game_status = status.get("statusCode")

                    # Store raw game data
                    await conn.execute(
                        """
                        INSERT INTO raw_data.mlb_stats_api_games (
                            external_game_id, game_pk, raw_response, endpoint_url,
                            response_status, game_date, season, season_type,
                            home_team, away_team, game_datetime, venue_id, venue_name,
                            game_status, collected_at, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                        ON CONFLICT (external_game_id) DO UPDATE SET
                            raw_response = EXCLUDED.raw_response,
                            game_status = EXCLUDED.game_status,
                            collected_at = EXCLUDED.collected_at,
                            updated_at = NOW()
                    """,
                        external_game_id,
                        game_pk,
                        json.dumps(game),  # Store complete raw response
                        f"https://statsapi.mlb.com/api/v1/schedule?date={date_str}",
                        200,
                        game_datetime.date()
                        if game_datetime
                        else datetime.strptime(date_str, "%Y-%m-%d").date(),
                        int(season) if season else None,
                        season_type,
                        home_team,
                        away_team,
                        game_datetime,
                        venue_id,
                        venue_name,
                        game_status,
                        now_est(),
                        now_est(),
                    )

                    self.stats["games_stored"] += 1
                    self.stats["games_processed"] += 1

                    logger.debug(
                        "Stored MLB Stats API game",
                        game_pk=game_pk,
                        home_team=home_team,
                        away_team=away_team,
                    )

                except Exception as e:
                    logger.error(
                        "Error storing individual game",
                        game_pk=game.get("gamePk"),
                        error=str(e),
                    )
                    self.stats["errors"] += 1

            await conn.close()
            logger.info(
                f"Stored {self.stats['games_stored']} games to raw_data.mlb_stats_api_games"
            )

        except Exception as e:
            logger.error("Error storing MLB Stats API game data", error=str(e))
            self.stats["errors"] += 1

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Validate MLB Stats API game record."""
        required_fields = ["gamePk", "teams", "gameDate", "status"]
        return all(field in record for field in required_fields)

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Add collection metadata to record."""
        record["source"] = "MLB_STATS_API"
        record["collected_at"] = datetime.now()
        return record

    def get_stats(self) -> dict[str, Any]:
        """Get collection statistics."""
        return {
            **self.stats,
            "success_rate": self.stats["games_processed"]
            / max(self.stats["games_found"], 1)
            * 100,
        }


# Convenience function for MLB Stats API data collection
async def collect_mlb_stats_api_data(date: str = None) -> dict[str, Any]:
    """Convenience function for MLB Stats API data collection."""
    config = CollectorConfig(name="MLBStatsAPI", enabled=True)
    collector = MLBStatsAPICollector(config)

    try:
        request = CollectionRequest(
            start_date=datetime.strptime(date, "%Y-%m-%d") if date else datetime.now()
        )

        await collector.collect_data(request)

        return collector.get_stats()

    finally:
        await collector.client.close()


# Example usage
if __name__ == "__main__":

    async def main():
        # Test collection for today
        stats = await collect_mlb_stats_api_data()
        print(f"Collection completed: {stats}")

    asyncio.run(main())
