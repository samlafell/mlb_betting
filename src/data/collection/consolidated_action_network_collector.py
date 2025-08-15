#!/usr/bin/env python3
"""
Consolidated Action Network Collector

This collector combines all Action Network data collection capabilities into a single,
unified interface. It replaces the multiple scattered files with a clean, maintainable
solution that supports all collection modes.
"""

import asyncio
import json
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp
import asyncpg
import structlog

from ...core.config import get_settings
from ...core.datetime_utils import (
    now_est,
    safe_game_datetime_parse,
)
from ...core.sportsbook_utils import SportsbookResolver
from ...core.team_utils import normalize_team_name
from ..models.unified.actionnetwork import (
    ActionNetworkBettingInfo,
    ActionNetworkHistoricalData,
    ActionNetworkHistoricalEntry,
    ActionNetworkMarketData,
    ActionNetworkPrice,
    LineMovementPeriod,
)
from .base import BaseCollector, CollectionRequest, CollectorConfig
from .smart_line_movement_filter import SmartLineMovementFilter

logger = structlog.get_logger(__name__)


class CollectionMode(Enum):
    """Collection modes for Action Network data."""

    CURRENT = "current"
    HISTORICAL = "historical"
    COMPREHENSIVE = "comprehensive"


class ActionNetworkClient:
    """HTTP client for Action Network API calls."""

    def __init__(self, db_config: dict):
        self.api_base = "https://api.actionnetwork.com"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.session = None
        self.sportsbook_resolver = SportsbookResolver(db_config)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def fetch_games(self, date: str) -> list[dict[str, Any]]:
        """Fetch games data from Action Network API."""
        session = await self._get_session()

        url = f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb"
        params = {
            "bookIds": "15,30,75,123,69,68,972,71,247,79",
            "date": date,
            "periods": "event",
        }

        logger.info("Fetching games from Action Network", url=url, date=date)

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    games = data.get("games", [])
                    logger.info(f"Successfully fetched {len(games)} games", status=response.status)
                    return games
                elif response.status == 429:
                    logger.warning("Rate limited by API", status=response.status, retry_after=response.headers.get('Retry-After'))
                    return []
                elif response.status >= 500:
                    logger.error("Server error from API", status=response.status, url=url, recovery_suggestion="Retry after delay")
                    return []
                else:
                    logger.error("API request failed", status=response.status, url=url, recovery_suggestion="Check API endpoint and parameters")
                    return []
        except aiohttp.ClientTimeout as e:
            logger.warning("API request timeout", url=url, error=str(e), recovery_suggestion="Retry with longer timeout")
            return []
        except aiohttp.ClientError as e:
            logger.error("Network error during API request", url=url, error=str(e), error_type=type(e).__name__)
            return []

    async def fetch_game_history(self, game_id: int) -> dict[str, Any]:
        """Fetch game history data from Action Network API."""
        session = await self._get_session()

        url = f"{self.api_base}/web/v2/markets/event/{game_id}/history"

        # Add rate limiting - wait before history requests
        await asyncio.sleep(0.5)

        logger.info("Fetching game history", game_id=game_id, url=url)

        # Add additional headers specifically for history endpoint
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"https://www.actionnetwork.com/mlb/game/{game_id}",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }

        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Successfully fetched history for game {game_id}", status=response.status)
                    return data
                elif response.status == 429:
                    logger.warning("Rate limited on history API", game_id=game_id, retry_after=response.headers.get('Retry-After'))
                    return {}
                elif response.status >= 500:
                    logger.warning("Server error on history API", game_id=game_id, status=response.status)
                    return {}
                else:
                    logger.warning(
                        "History API request failed",
                        status=response.status,
                        game_id=game_id,
                        recovery_suggestion="History data not available for this game"
                    )
                    return {}
        except aiohttp.ClientTimeout as e:
            logger.warning("History API timeout", game_id=game_id, error=str(e))
            return {}
        except aiohttp.ClientError as e:
            logger.warning("Network error during history request", game_id=game_id, error=str(e), error_type=type(e).__name__)
            return {}

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None


class ActionNetworkHistoryParser:
    """
    Parser for Action Network historical line movement data.

    Processes raw JSON responses from Action Network history APIs
    to extract structured line movement information.
    """

    def __init__(self):
        """Initialize the Action Network history parser."""
        self.logger = logger.bind(parser="ActionNetworkHistory")

    def parse_history_response(
        self,
        response_data: dict[str, Any],
        game_id: int,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        history_url: str,
    ) -> ActionNetworkHistoricalData | None:
        """
        Parse the raw response data from Action Network history API.

        Args:
            response_data: Raw JSON response from API
            game_id: Action Network game ID
            home_team: Home team name
            away_team: Away team name
            game_datetime: Scheduled game start time
            history_url: History URL used for extraction

        Returns:
            Processed ActionNetworkHistoricalData or None if parsing fails
        """
        try:
            # Log the raw response for debugging
            self.logger.info(
                "Raw API response received",
                response_type=type(response_data).__name__,
                response_keys=list(response_data.keys())
                if isinstance(response_data, dict)
                else "N/A",
                response_length=len(response_data)
                if isinstance(response_data, list | dict)
                else "N/A",
            )

            # Handle both list and dict response formats
            if isinstance(response_data, dict):
                # If it's a dict, try to extract the list from common keys
                if "data" in response_data:
                    response_data = response_data["data"]
                elif "history" in response_data:
                    response_data = response_data["history"]
                elif "results" in response_data:
                    response_data = response_data["results"]
                else:
                    # Check if the dict has numeric keys (sportsbook IDs)
                    # This is the new format where keys are sportsbook IDs
                    if all(key.isdigit() for key in response_data.keys()):
                        self.logger.info(
                            "Detected sportsbook ID format",
                            sportsbook_ids=list(response_data.keys()),
                        )
                        # Extract historical data from each sportsbook
                        historical_entries = []
                        for _sportsbook_id, sportsbook_data in response_data.items():
                            if (
                                isinstance(sportsbook_data, dict)
                                and "event" in sportsbook_data
                            ):
                                historical_entries.append(sportsbook_data)
                        response_data = historical_entries
                    else:
                        # If it's a single dict entry, wrap it in a list
                        response_data = [response_data]

            # Validate response structure
            if not isinstance(response_data, list):
                self.logger.error(
                    "Expected list response from history API",
                    response_type=type(response_data),
                )
                return None

            if len(response_data) == 0:
                self.logger.warning("Empty response from history API")
                return None

            historical_entries = []
            pregame_count = 0
            live_count = 0

            # Process each entry in the response
            for i, entry_data in enumerate(response_data):
                if not isinstance(entry_data, dict):
                    self.logger.warning(
                        f"Skipping invalid entry at index {i}",
                        entry_type=type(entry_data),
                    )
                    continue

                # Extract event data
                event_data = entry_data.get("event", {})
                if not event_data:
                    self.logger.warning(f"No event data in entry {i}")
                    continue

                # Determine if this is pregame or live data
                # Based on your description: indices 0-1 are pregame, 2+ are live
                period = (
                    LineMovementPeriod.PREGAME if i < 2 else LineMovementPeriod.LIVE
                )

                # Extract market data
                moneyline_data = self._extract_market_data(event_data, "moneyline")
                spread_data = self._extract_market_data(event_data, "spread")
                total_data = self._extract_market_data(event_data, "total")

                # Create historical entry
                historical_entry = ActionNetworkHistoricalEntry(
                    event=event_data,
                    period=period,
                    moneyline=moneyline_data,
                    spread=spread_data,
                    total=total_data,
                )

                historical_entries.append(historical_entry)

                if period == LineMovementPeriod.PREGAME:
                    pregame_count += 1
                else:
                    live_count += 1

            # Create the complete historical data object
            historical_data = ActionNetworkHistoricalData(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                historical_entries=historical_entries,
                history_url=history_url,
                total_entries=len(historical_entries),
                pregame_entries=pregame_count,
                live_entries=live_count,
            )

            self.logger.info(
                "Successfully parsed Action Network history",
                game_id=game_id,
                total_entries=len(historical_entries),
                pregame_entries=pregame_count,
                live_entries=live_count,
            )

            return historical_data

        except Exception as e:
            self.logger.error(
                "Failed to parse historical response", error=str(e), game_id=game_id
            )
            return None

    def _extract_market_data(
        self, event_data: dict[str, Any], market_type: str
    ) -> ActionNetworkMarketData | None:
        """
        Extract market data for a specific market type from event data.

        Args:
            event_data: Event data from the API response
            market_type: Type of market ('moneyline', 'spread', 'total')

        Returns:
            ActionNetworkMarketData or None if not found
        """
        try:
            market_data = event_data.get(market_type)
            if not market_data:
                return None

            # Handle both dict and list formats
            if isinstance(market_data, list):
                # New format: market_data is a list of betting lines
                # Find home and away sides
                home_line = None
                away_line = None

                for line in market_data:
                    if line.get("side") == "home":
                        home_line = line
                    elif line.get("side") == "away":
                        away_line = line
                    elif line.get("side") == "over" and market_type == "total":
                        home_line = line  # Treat "over" as home for totals
                    elif line.get("side") == "under" and market_type == "total":
                        away_line = line  # Treat "under" as away for totals

                # Extract pricing data
                home_price = None
                away_price = None
                line_value = None
                home_bet_info = None
                away_bet_info = None

                if home_line:
                    home_price = ActionNetworkPrice(
                        decimal=None,  # Not provided in this format
                        american=home_line.get("odds"),
                    )
                    if market_type in ["spread", "total"]:
                        line_value = home_line.get("value")

                    # Extract betting info for home/over side
                    bet_info_data = home_line.get("bet_info", {})
                    if bet_info_data:
                        home_bet_info = ActionNetworkBettingInfo(
                            tickets=bet_info_data.get("tickets"),
                            money=bet_info_data.get("money"),
                        )

                if away_line:
                    away_price = ActionNetworkPrice(
                        decimal=None,  # Not provided in this format
                        american=away_line.get("odds"),
                    )
                    if market_type in ["spread", "total"] and not line_value:
                        line_value = away_line.get("value")

                    # Extract betting info for away/under side
                    bet_info_data = away_line.get("bet_info", {})
                    if bet_info_data:
                        away_bet_info = ActionNetworkBettingInfo(
                            tickets=bet_info_data.get("tickets"),
                            money=bet_info_data.get("money"),
                        )

            else:
                # Legacy format: market_data is a dict
                home_price = None
                away_price = None
                line_value = None
                home_bet_info = None
                away_bet_info = None

                if market_type == "moneyline":
                    # Moneyline has home and away prices
                    home_odds = market_data.get("home", {})
                    away_odds = market_data.get("away", {})

                    if home_odds:
                        home_price = ActionNetworkPrice(
                            decimal=home_odds.get("decimal"),
                            american=home_odds.get("american"),
                        )

                    if away_odds:
                        away_price = ActionNetworkPrice(
                            decimal=away_odds.get("decimal"),
                            american=away_odds.get("american"),
                        )

                elif market_type in ["spread", "total"]:
                    # Spread and total have line values and pricing
                    line_value = market_data.get("line") or market_data.get("value")

                    # Extract pricing (may be in different formats)
                    home_odds = market_data.get("home", {}) or market_data.get(
                        "over", {}
                    )
                    away_odds = market_data.get("away", {}) or market_data.get(
                        "under", {}
                    )

                    if home_odds:
                        home_price = ActionNetworkPrice(
                            decimal=home_odds.get("decimal"),
                            american=home_odds.get("american"),
                        )

                    if away_odds:
                        away_price = ActionNetworkPrice(
                            decimal=away_odds.get("decimal"),
                            american=away_odds.get("american"),
                        )

            # Create market data object
            return ActionNetworkMarketData(
                home=home_price,
                away=away_price,
                line=line_value,
                home_bet_info=home_bet_info,
                away_bet_info=away_bet_info,
            )

        except Exception as e:
            self.logger.error(
                "Error extracting market data",
                market_type=market_type,
                error=str(e),
            )
            return None


class ActionNetworkCollector(BaseCollector):
    """
    Consolidated Action Network collector with all capabilities.

    Supports multiple collection modes:
    - CURRENT: Current lines only
    - HISTORICAL: Historical line movements
    - COMPREHENSIVE: Combined current + historical with smart filtering
    """

    def __init__(
        self,
        config: CollectorConfig,
        mode: CollectionMode = CollectionMode.COMPREHENSIVE,
    ):
        super().__init__(config)
        self.mode = mode
        # Use proper settings configuration for database connection
        settings = get_settings()
        self.db_config = {
            "host": settings.database.host,
            "port": settings.database.port,
            "database": settings.database.database,
            "user": settings.database.user,
            "password": settings.database.password,
        }
        self.client = ActionNetworkClient(self.db_config)
        self.filter = SmartLineMovementFilter()
        self.history_parser = ActionNetworkHistoryParser()

        # Statistics tracking
        self.stats = {
            "games_found": 0,
            "games_processed": 0,
            "current_lines": 0,
            "history_points": 0,
            "filtered_movements": 0,
            "moneyline_inserted": 0,
            "spread_inserted": 0,
            "totals_inserted": 0,
            "total_inserted": 0,
        }

    async def collect_data(self, request: CollectionRequest) -> list[dict[str, Any]]:
        """Main collection method supporting all modes."""
        try:
            date_str = (
                request.start_date.strftime("%Y%m%d")
                if request.start_date
                else datetime.now().strftime("%Y%m%d")
            )

            logger.info(
                "Starting Action Network collection",
                mode=self.mode.value,
                date=date_str,
            )

            if self.mode == CollectionMode.CURRENT:
                return await self._collect_current_lines(date_str)
            elif self.mode == CollectionMode.HISTORICAL:
                return await self._collect_historical_data(date_str)
            else:  # COMPREHENSIVE
                return await self._collect_comprehensive(date_str)

        except aiohttp.ClientError as e:
            logger.error(
                "Network error during collection", 
                error=str(e), 
                mode=self.mode.value,
                error_type=type(e).__name__,
                recovery_suggestion="Check network connectivity and retry"
            )
            return []
        except asyncpg.PostgresError as e:
            logger.error(
                "Database error during collection", 
                error=str(e), 
                mode=self.mode.value,
                error_code=getattr(e, 'sqlstate', 'unknown'),
                recovery_suggestion="Check database connectivity and schema"
            )
            return []
        except Exception as e:
            logger.error(
                "Unexpected error during collection", 
                error=str(e), 
                mode=self.mode.value,
                error_type=type(e).__name__,
                recovery_suggestion="Check configuration and system resources"
            )
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            return []

        finally:
            await self.client.close()

    async def _collect_current_lines(self, date: str) -> list[dict[str, Any]]:
        """Collect current betting lines only."""
        games = await self.client.fetch_games(date)

        if not games:
            logger.warning("No games found for current lines collection", date=date)
            return []

        self.stats["games_found"] = len(games)
        logger.info("Found games for current lines", count=len(games))

        # Store raw data first (RAW layer)
        await self._store_raw_game_data(
            games, "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
        )

        # Process current lines (CURATED layer)
        await self._process_current_lines(games)

        return games

    async def _collect_historical_data(self, date: str) -> list[dict[str, Any]]:
        """Collect historical line movements."""
        games = await self.client.fetch_games(date)

        if not games:
            logger.warning("No games found for historical collection", date=date)
            return []

        self.stats["games_found"] = len(games)
        logger.info("Found games for historical collection", count=len(games))

        # Process historical data
        await self._process_historical_data(games)

        return games

    async def _collect_comprehensive(self, date: str) -> list[dict[str, Any]]:
        """Collect comprehensive data with smart filtering."""
        games = await self.client.fetch_games(date)

        if not games:
            logger.warning("No games found for comprehensive collection", date=date)
            return []

        self.stats["games_found"] = len(games)
        logger.info("Found games for comprehensive collection", count=len(games))

        # Store raw game data first (RAW layer)
        await self._store_raw_game_data(
            games, "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
        )

        # Store raw odds data from current games (RAW layer)
        await self._store_raw_current_odds(games)

        # Fetch and store historical data for each game (RAW layer)
        await self._fetch_and_store_historical_data(games)

        # Get game mappings from raw data (no CURATED layer writes)
        game_mappings = await self._get_game_mappings(games)

        # Store processed odds to raw_data.action_network_odds (RAW layer only)
        await self._process_comprehensive_data(games, game_mappings)

        return games

    async def _get_game_mappings(self, games: list[dict[str, Any]]) -> dict[str, str]:
        """Get game mappings from raw data - no longer creates legacy games."""
        game_mappings = {}

        try:
            conn = await asyncpg.connect(**self.db_config)

            for game in games:
                game_id = game.get("id")
                if not game_id:
                    continue

                # Check if raw game data exists
                existing_raw = await conn.fetchval(
                    """
                    SELECT id FROM raw_data.action_network_games WHERE external_game_id = $1
                """,
                    str(game_id),
                )

                if existing_raw:
                    game_mappings[str(game_id)] = str(game_id)
                    logger.debug(f"Found existing raw game data for game {game_id}")

            await conn.close()

        except Exception as e:
            logger.error("Error getting game mappings", error=str(e))

        return game_mappings

    async def _store_raw_game_data(
        self, games: list[dict[str, Any]], endpoint_url: str = None
    ) -> None:
        """Store raw game data to raw_data.action_network_games table with extracted readable info."""
        if not games:
            return

        try:
            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )

            for game in games:
                game_id = game.get("id")
                if not game_id:
                    continue

                # Extract readable game information
                teams = game.get("teams", [])
                home_team = None
                away_team = None
                home_team_abbr = None
                away_team_abbr = None

                if len(teams) >= 2:
                    # Use home_team_id and away_team_id to correctly map teams
                    home_team_id = game.get("home_team_id")
                    away_team_id = game.get("away_team_id")

                    # Find home and away teams by matching IDs
                    for team in teams:
                        team_id = team.get("id")
                        if team_id == home_team_id:
                            home_team = team.get(
                                "full_name", team.get("display_name", "Unknown")
                            )
                            home_team_abbr = team.get("abbr", team.get("abbreviation"))
                        elif team_id == away_team_id:
                            away_team = team.get(
                                "full_name", team.get("display_name", "Unknown")
                            )
                            away_team_abbr = team.get("abbr", team.get("abbreviation"))

                game_status = game.get("status", "unknown")
                start_time = safe_game_datetime_parse(game.get("start_time"))
                game_date = start_time.date() if start_time else now_est().date()

                # Store raw game data with extracted readable fields
                # Ensure we have valid JSON data before inserting
                game_json = json.dumps(game) if game else None
                if not game_json or game_json == 'null':
                    logger.warning(f"Skipping game {game_id} due to empty game data")
                    continue
                    
                await conn.execute(
                    """
                    INSERT INTO raw_data.action_network_games (
                        external_game_id, raw_response, raw_game_data, endpoint_url, response_status,
                        game_date, home_team, away_team, home_team_abbr, away_team_abbr,
                        game_status, start_time, collected_at, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (external_game_id) DO UPDATE SET
                        raw_response = EXCLUDED.raw_response,
                        raw_game_data = EXCLUDED.raw_game_data,
                        home_team = EXCLUDED.home_team,
                        away_team = EXCLUDED.away_team,
                        home_team_abbr = EXCLUDED.home_team_abbr,
                        away_team_abbr = EXCLUDED.away_team_abbr,
                        game_status = EXCLUDED.game_status,
                        start_time = EXCLUDED.start_time,
                        collected_at = EXCLUDED.collected_at
                """,
                    str(game_id),
                    game_json,  # raw_response: Full raw data as JSON string
                    game_json,  # raw_game_data: Also use JSON string for JSONB column (asyncpg will handle conversion)
                    endpoint_url
                    or "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb",
                    200,
                    game_date,
                    home_team,
                    away_team,
                    home_team_abbr,
                    away_team_abbr,
                    game_status,
                    start_time,
                    now_est(),
                    now_est(),
                )

            await conn.close()
            logger.info(
                f"Stored {len(games)} games to raw_data.action_network_games with readable info"
            )

        except Exception as e:
            logger.error(
                "Error storing raw game data", 
                error=str(e), 
                games_attempted=len(games),
                error_type=type(e).__name__,
                recovery_suggestion="Check database connection and table schema"
            )
            # Continue processing - don't let one game failure stop the entire collection
            pass

    async def _store_raw_odds_data(
        self, game_id: str, odds_data: dict[str, Any], sportsbook_key: str = None
    ) -> None:
        """Store raw odds data to raw_data.action_network_odds table."""
        try:
            logger.info(
                f"Attempting to store odds data for game {game_id}, sportsbook {sportsbook_key}"
            )

            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )

            logger.info(f"Database connection established for game {game_id}")

            # Prepare data for insertion
            collected_at = now_est()
            created_at = now_est()
            json_data = json.dumps(odds_data)

            logger.info(
                f"Prepared data: game_id={game_id}, sportsbook_key={sportsbook_key}, collected_at={collected_at}"
            )

            await conn.execute(
                """
                INSERT INTO raw_data.action_network_odds (
                    external_game_id, sportsbook_key, raw_odds, collected_at, created_at
                ) VALUES ($1, $2, $3, $4, $5)
            """,
                str(game_id),
                sportsbook_key or "unknown",
                json_data,  # Convert dict to JSON string for JSONB storage
                collected_at,
                created_at,
            )

            await conn.close()
            logger.info(
                f"✅ Successfully stored odds data for game {game_id}, sportsbook {sportsbook_key}"
            )

        except asyncpg.ConnectionDoesNotExistError as e:
            logger.warning(
                f"⚠️ Database connection lost for game {game_id}, sportsbook {sportsbook_key}, retrying...",
                error=str(e)
            )
            # Retry once for connection issues
            try:
                await asyncio.sleep(1)  # Brief delay before retry
                await self._store_raw_odds_data(game_id, odds_data, sportsbook_key)
            except Exception as retry_e:
                logger.error(
                    f"❌ Retry failed for game {game_id}, sportsbook {sportsbook_key}",
                    original_error=str(e),
                    retry_error=str(retry_e),
                    recovery_suggestion="Check database connectivity and restart collection"
                )
        except asyncpg.PostgresError as e:
            logger.error(
                f"❌ Database error storing odds data for game {game_id}, sportsbook {sportsbook_key}",
                error=str(e),
                error_code=getattr(e, 'sqlstate', 'unknown'),
                recovery_suggestion="Check database schema and constraints"
            )
        except Exception as e:
            logger.error(
                f"❌ Unexpected error storing raw odds data for game {game_id}, sportsbook {sportsbook_key}",
                error=str(e),
                error_type=type(e).__name__,
                recovery_suggestion="Check data format and database schema"
            )
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")

    async def _store_raw_current_odds(self, games: list[dict[str, Any]]) -> None:
        """Extract and store raw odds data from current games to raw_data.action_network_odds table."""
        try:
            for game in games:
                game_id = game.get("id")
                if not game_id:
                    continue

                markets = game.get("markets", {})
                for book_id_str, book_data in markets.items():
                    event_markets = book_data.get("event", {})
                    if event_markets:
                        await self._store_raw_odds_data(
                            str(game_id), event_markets, book_id_str
                        )

            logger.info(f"Processed current odds for {len(games)} games")

        except Exception as e:
            logger.error("Error storing raw current odds", error=str(e))

    async def _fetch_and_store_historical_data(
        self, games: list[dict[str, Any]]
    ) -> None:
        """Fetch historical line movement data for each game and store in RAW layer."""
        try:
            for game in games:
                game_id = game.get("id")
                if not game_id:
                    continue

                # Fetch historical data from history endpoint
                history_data = await self.client.fetch_game_history(int(game_id))

                if history_data:
                    # Store raw historical data
                    await self._store_raw_historical_data(str(game_id), history_data)
                    self.stats["history_points"] += len(history_data)

            logger.info(f"Fetched historical data for {len(games)} games")

        except Exception as e:
            logger.error("Error fetching and storing historical data", error=str(e))

    async def _store_raw_historical_data(
        self, game_id: str, history_data: dict[str, Any]
    ) -> None:
        """Store raw historical data to raw_data.action_network_history table."""
        try:
            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
            )

            # Create table if it doesn't exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_data.action_network_history (
                    id BIGSERIAL PRIMARY KEY,
                    external_game_id VARCHAR(255),
                    raw_history JSONB NOT NULL,
                    endpoint_url TEXT,
                    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(external_game_id)
                )
            """)

            await conn.execute(
                """
                INSERT INTO raw_data.action_network_history (
                    external_game_id, raw_history, endpoint_url, collected_at, created_at
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (external_game_id) DO UPDATE SET
                    raw_history = EXCLUDED.raw_history,
                    collected_at = EXCLUDED.collected_at
            """,
                str(game_id),
                json.dumps(history_data),
                f"https://api.actionnetwork.com/web/v2/markets/event/{game_id}/history",
                now_est(),
                now_est(),
            )

            await conn.close()
            logger.debug(f"Stored historical data for game {game_id}")

        except Exception as e:
            logger.error(
                "Error storing raw historical data", error=str(e), game_id=game_id
            )

    async def _process_current_lines(self, games: list[dict[str, Any]]) -> None:
        """Process current betting lines - stores to RAW layer only."""
        for game in games:
            try:
                game_id = game.get("id")
                markets = game.get("markets", {})

                for book_id_str, book_data in markets.items():
                    event_markets = book_data.get("event", {})

                    # Store raw odds data to RAW layer only
                    await self._store_raw_odds_data(
                        str(game_id), event_markets, book_id_str
                    )

                    # Update statistics
                    for market_type in ["moneyline", "spread", "total"]:
                        market_data = event_markets.get(market_type, [])
                        if market_data:
                            if market_type == "moneyline":
                                self.stats["moneyline_inserted"] += len(market_data)
                            elif market_type == "spread":
                                self.stats["spread_inserted"] += len(market_data)
                            elif market_type == "total":
                                self.stats["totals_inserted"] += len(market_data)
                            self.stats["total_inserted"] += len(market_data)
                            self.stats["current_lines"] += 1

                self.stats["games_processed"] += 1

            except Exception as e:
                logger.error(
                    "Error processing current lines",
                    game_id=game.get("id"),
                    error=str(e),
                )

    async def _process_historical_data(self, games: list[dict[str, Any]]) -> None:
        """Process historical line movements."""
        for game in games:
            try:
                game_id = game.get("id")
                if not game_id:
                    continue

                # Fetch historical data
                history_data = await self.client.fetch_game_history(int(game_id))

                if history_data:
                    await self._process_game_history(game, history_data)

                self.stats["games_processed"] += 1

            except Exception as e:
                logger.error(
                    "Error processing historical data",
                    game_id=game.get("id"),
                    error=str(e),
                )

    async def _process_comprehensive_data(
        self, games: list[dict[str, Any]], game_mappings: dict[str, str]
    ) -> None:
        """Process comprehensive data - stores only to raw_data.action_network_odds."""
        processed_count = 0
        failed_count = 0
        
        for i, game in enumerate(games):
            try:
                game_id = str(game.get("id"))
                logger.debug(f"Processing game {i+1}/{len(games)}: {game_id}")
                
                if game_id not in game_mappings:
                    logger.debug(f"Skipping game {game_id} - no raw data mapping")
                    continue

                teams = game.get("teams", [])

                if len(teams) < 2:
                    logger.warning(f"Skipping game {game_id} - insufficient team data (found {len(teams)} teams)")
                    continue

                away_team = normalize_team_name(teams[0].get("full_name", ""))
                home_team = normalize_team_name(teams[1].get("full_name", ""))
                game_datetime = safe_game_datetime_parse(game.get("start_time"))

                markets = game.get("markets", {})
                sportsbook_count = len(markets)
                sportsbook_success = 0

                # Process each sportsbook - store to RAW layer only
                for book_id_str, book_data in markets.items():
                    try:
                        book_id = int(book_id_str)
                        event_markets = book_data.get("event", {})

                        # Store all odds data to raw_data.action_network_odds
                        await self._store_comprehensive_odds_data(
                            game_id,
                            book_id_str,
                            event_markets,
                            home_team,
                            away_team,
                            game_datetime,
                        )
                        sportsbook_success += 1
                        
                    except Exception as book_e:
                        logger.warning(
                            f"Failed to process sportsbook {book_id_str} for game {game_id}",
                            error=str(book_e),
                            error_type=type(book_e).__name__
                        )

                logger.debug(f"Game {game_id}: processed {sportsbook_success}/{sportsbook_count} sportsbooks")
                self.stats["games_processed"] += 1
                processed_count += 1

            except Exception as e:
                failed_count += 1
                logger.error(
                    "Error processing comprehensive data",
                    game_id=game.get("id"),
                    game_number=f"{i+1}/{len(games)}",
                    error=str(e),
                    error_type=type(e).__name__,
                    recovery_suggestion="Continue processing remaining games"
                )
                
        logger.info(
            f"Comprehensive data processing complete: {processed_count} successful, {failed_count} failed"
        )

    async def _process_current_market(self, *args, **kwargs) -> None:
        """DEPRECATED: Use raw data storage methods instead."""
        logger.warning(
            "_process_current_market is deprecated. Use raw data storage methods."
        )
        pass

    async def _process_game_history(
        self, game: dict[str, Any], history_data: dict[str, Any]
    ) -> None:
        """Process game history data - stores to RAW layer only."""
        game_id = game.get("id")
        if not game_id or not history_data:
            return

        # Store raw historical data
        await self._store_raw_historical_data(str(game_id), history_data)

        logger.debug("Stored game history to raw data", game_id=game_id)
        self.stats["history_points"] += len(history_data)

    async def _store_comprehensive_odds_data(
        self,
        game_id: str,
        sportsbook_key: str,
        event_markets: dict[str, Any],
        home_team: str,
        away_team: str,
        game_datetime: datetime,
    ) -> None:
        """Store comprehensive odds data to raw_data.action_network_odds."""
        try:
            # Add metadata to the odds data
            enhanced_odds_data = {
                "event_markets": event_markets,
                "game_metadata": {
                    "home_team": home_team,
                    "away_team": away_team,
                    "game_datetime": game_datetime.isoformat(),
                },
                "collection_info": {
                    "collection_mode": "comprehensive",
                    "smart_filtering_applied": True,
                    "timestamp": now_est().isoformat(),
                },
            }

            # Store to raw_data.action_network_odds
            await self._store_raw_odds_data(game_id, enhanced_odds_data, sportsbook_key)

            # Update statistics
            for market_type in ["moneyline", "spread", "total"]:
                market_data = event_markets.get(market_type, [])
                if market_data:
                    if market_type == "moneyline":
                        self.stats["moneyline_inserted"] += len(market_data)
                    elif market_type == "spread":
                        self.stats["spread_inserted"] += len(market_data)
                    elif market_type == "total":
                        self.stats["totals_inserted"] += len(market_data)
                    self.stats["total_inserted"] += len(market_data)

        except Exception as e:
            logger.error(
                "Error storing comprehensive odds data",
                game_id=game_id,
                sportsbook_key=sportsbook_key,
                error=str(e),
            )

    async def _process_moneyline_markets(
        self,
        moneyline_data: list[dict],
        game_id: str,
        sportsbook_key: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
    ) -> None:
        """Process moneyline markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning(
            "_process_moneyline_markets is deprecated. Use raw data storage methods."
        )
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass

    async def _process_spread_markets(
        self,
        spread_data: list[dict],
        game_id: str,
        sportsbook_key: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
    ) -> None:
        """Process spread markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning(
            "_process_spread_markets is deprecated. Use raw data storage methods."
        )
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass

    async def _process_totals_markets(
        self,
        totals_data: list[dict],
        game_id: str,
        sportsbook_key: str,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
    ) -> None:
        """Process totals markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning(
            "_process_totals_markets is deprecated. Use raw data storage methods."
        )
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass

    async def _insert_moneyline_history(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning(
            "_insert_moneyline_history is deprecated. All odds data should be stored in raw_data schema."
        )
        pass

    async def _insert_single_moneyline(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning(
            "_insert_single_moneyline is deprecated. All odds data should be stored in raw_data schema."
        )
        pass

    async def _insert_single_spread(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning(
            "_insert_single_spread is deprecated. All odds data should be stored in raw_data schema."
        )
        pass

    async def _insert_single_total(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning(
            "_insert_single_total is deprecated. All odds data should be stored in raw_data schema."
        )
        pass

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Validate Action Network game record."""
        required_fields = ["id", "teams", "start_time", "markets"]
        return all(field in record for field in required_fields)

    def normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Add collection metadata to record."""
        normalized = record.copy()

        # Add standardized metadata
        normalized["source"] = self.source.value
        normalized["collected_at_est"] = datetime.now().isoformat()
        normalized["collector_version"] = "action_network_consolidated_v2"
        normalized["collection_mode"] = self.mode.value

        # Add data quality indicators
        normalized["has_teams"] = bool(normalized.get("teams"))
        normalized["has_markets"] = bool(normalized.get("markets"))
        normalized["has_public_betting"] = bool(normalized.get("public_betting"))

        return normalized

    def get_stats(self) -> dict[str, Any]:
        """Get collection statistics."""
        return {
            **self.stats,
            "collection_mode": self.mode.value,
            "success_rate": self.stats["games_processed"]
            / max(self.stats["games_found"], 1)
            * 100,
        }


# Convenience functions for backward compatibility
async def collect_action_network_data(
    date: str = None, mode: str = "comprehensive"
) -> dict[str, Any]:
    """Convenience function for Action Network data collection."""
    collection_mode = CollectionMode(mode)
    config = CollectorConfig(name="ActionNetwork", enabled=True)

    collector = ActionNetworkCollector(config, collection_mode)

    try:
        request = CollectionRequest(
            start_date=datetime.strptime(date, "%Y%m%d") if date else datetime.now()
        )

        await collector.collect_data(request)

        return collector.get_stats()

    finally:
        await collector.client.close()


# Example usage
if __name__ == "__main__":

    async def main():
        # Test comprehensive collection
        stats = await collect_action_network_data("20250718", "comprehensive")
        print(f"Collection completed: {stats}")

    asyncio.run(main())
