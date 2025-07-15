"""
Action Network parser for historical line movement data.

This parser processes historical betting data from Action Network
history URLs to extract line movement information over time.
"""

import asyncio
from datetime import datetime
from typing import Any

import structlog

from ..collection.base import CollectionResult
from ..models.unified.actionnetwork import (
    ActionNetworkHistoricalData,
    ActionNetworkHistoricalEntry,
    ActionNetworkMarketData,
    ActionNetworkPrice,
    LineMovementPeriod,
)

logger = structlog.get_logger(__name__)


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
                if isinstance(response_data, (list, dict))
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
                        for sportsbook_id, sportsbook_data in response_data.items():
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

                if home_line:
                    home_price = ActionNetworkPrice(
                        decimal=None,  # Not provided in this format
                        american=home_line.get("odds"),
                    )
                    if market_type in ["spread", "total"]:
                        line_value = home_line.get("value")

                if away_line:
                    away_price = ActionNetworkPrice(
                        decimal=None,  # Not provided in this format
                        american=away_line.get("odds"),
                    )
                    if market_type in ["spread", "total"] and not line_value:
                        line_value = away_line.get("value")

            else:
                # Legacy format: market_data is a dict
                home_price = None
                away_price = None
                line_value = None

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
                home=home_price, away=away_price, line=line_value
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to extract {market_type} market data", error=str(e)
            )
            return None

    def parse_multiple_histories(
        self, response_data_list: list[tuple[dict[str, Any], dict[str, Any]]]
    ) -> list[ActionNetworkHistoricalData]:
        """
        Parse historical data for multiple games.

        Args:
            response_data_list: List of tuples containing (response_data, game_metadata)

        Returns:
            List of ActionNetworkHistoricalData objects
        """
        parsed_histories = []

        for response_data, game_metadata in response_data_list:
            try:
                historical_data = self.parse_history_response(
                    response_data=response_data,
                    game_id=game_metadata["game_id"],
                    home_team=game_metadata["home_team"],
                    away_team=game_metadata["away_team"],
                    game_datetime=game_metadata["game_datetime"],
                    history_url=game_metadata["history_url"],
                )

                if historical_data:
                    parsed_histories.append(historical_data)

            except Exception as e:
                self.logger.error(
                    "Failed to parse game history",
                    game_id=game_metadata.get("game_id"),
                    error=str(e),
                )

        self.logger.info(
            "Completed parsing multiple histories",
            total_games=len(response_data_list),
            successful_parses=len(parsed_histories),
        )

        return parsed_histories


class ActionNetworkHistoryCollector:
    """
    Collector for Action Network historical line movement data.

    Integrates with the unified data collection system to fetch
    and parse historical betting data from Action Network.
    """

    def __init__(self):
        """Initialize the Action Network history collector."""
        self.source_name = "ActionNetworkHistory"
        self.parser = ActionNetworkHistoryParser()
        self.logger = logger.bind(source=self.source_name)
        self.session = None

        # Action Network specific headers
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

    async def _get_session(self):
        """Get or create an HTTP session."""
        if self.session is None:
            import aiohttp

            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def _fetch_json(self, url: str) -> dict[str, Any] | None:
        """Fetch JSON data from a URL."""
        try:
            session = await self._get_session()

            self.logger.info("Fetching data from URL", url=url)

            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    self.logger.info(
                        "Successfully fetched data", url=url, status=response.status
                    )
                    return data
                else:
                    self.logger.warning("HTTP error", url=url, status=response.status)
                    return None

        except Exception as e:
            self.logger.error("Failed to fetch data", url=url, error=str(e))
            return None

    async def close(self):
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def collect_history_data(
        self,
        history_url: str,
        game_id: int,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
    ) -> CollectionResult:
        """
        Collect historical line movement data from Action Network history URL.

        Args:
            history_url: The Action Network history API URL
            game_id: Action Network game ID
            home_team: Home team name
            away_team: Away team name
            game_datetime: Scheduled game start time

        Returns:
            CollectionResult with historical line movement data
        """
        start_time = datetime.now()

        try:
            self.logger.info(
                "Collecting Action Network history",
                game_id=game_id,
                matchup=f"{away_team} @ {home_team}",
                url=history_url,
            )

            # Fetch historical data from API
            response_data = await self._fetch_json(history_url)

            if not response_data:
                return CollectionResult(
                    success=False,
                    data=[],
                    source=self.source_name,
                    timestamp=datetime.now(),
                    errors=[f"No data received from history URL: {history_url}"],
                    metadata={
                        "game_id": game_id,
                        "history_url": history_url,
                        "matchup": f"{away_team} @ {home_team}",
                    },
                )

            # Parse historical data
            historical_data = self.parser.parse_history_response(
                response_data, game_id, home_team, away_team, game_datetime, history_url
            )

            if not historical_data:
                return CollectionResult(
                    success=False,
                    data=[],
                    source=self.source_name,
                    timestamp=datetime.now(),
                    errors=["Failed to parse historical data"],
                    metadata={
                        "game_id": game_id,
                        "history_url": history_url,
                        "matchup": f"{away_team} @ {home_team}",
                    },
                )

            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds() * 1000

            return CollectionResult(
                success=True,
                data=[historical_data],
                source=self.source_name,
                timestamp=datetime.now(),
                errors=[],
                metadata={
                    "game_id": game_id,
                    "history_url": history_url,
                    "matchup": f"{away_team} @ {home_team}",
                    "total_entries": historical_data.total_entries,
                    "pregame_entries": historical_data.pregame_entries,
                    "live_entries": historical_data.live_entries,
                    "response_time_ms": response_time,
                },
                request_count=1,
                response_time_ms=response_time,
            )

        except Exception as e:
            self.logger.error(
                "Action Network history collection failed",
                game_id=game_id,
                url=history_url,
                error=str(e),
            )

            return CollectionResult(
                success=False,
                data=[],
                source=self.source_name,
                timestamp=datetime.now(),
                errors=[f"Collection failed: {str(e)}"],
                metadata={
                    "game_id": game_id,
                    "history_url": history_url,
                    "matchup": f"{away_team} @ {home_team}",
                },
            )

    async def collect_multiple_histories(
        self, game_data_list: list[dict[str, Any]]
    ) -> list[CollectionResult]:
        """
        Collect historical data for multiple games concurrently.

        Args:
            game_data_list: List of game data dictionaries with history URLs

        Returns:
            List of CollectionResult objects
        """
        if not game_data_list:
            return []

        self.logger.info(
            "Collecting multiple Action Network histories",
            game_count=len(game_data_list),
        )

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent requests

        async def collect_single_game(game_data: dict[str, Any]) -> CollectionResult:
            async with semaphore:
                return await self.collect_history_data(
                    history_url=game_data["history_url"],
                    game_id=game_data["game_id"],
                    home_team=game_data["home_team"],
                    away_team=game_data["away_team"],
                    game_datetime=game_data["game_datetime"],
                )

        # Execute all collection tasks
        tasks = [collect_single_game(game_data) for game_data in game_data_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        collection_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(
                    "Failed to collect game history",
                    game_id=game_data_list[i].get("game_id"),
                    error=str(result),
                )
                # Create failed result
                collection_results.append(
                    CollectionResult(
                        success=False,
                        data=[],
                        source=self.source_name,
                        timestamp=datetime.now(),
                        errors=[f"Exception during collection: {str(result)}"],
                        metadata=game_data_list[i],
                    )
                )
            else:
                collection_results.append(result)

        successful_collections = sum(1 for r in collection_results if r.success)
        self.logger.info(
            "Completed multiple history collection",
            total_games=len(game_data_list),
            successful=successful_collections,
            failed=len(game_data_list) - successful_collections,
        )

        return collection_results

    async def collect(self, **kwargs: Any) -> CollectionResult:
        """
        Main collect method for compatibility with base class.

        This method is required by the base class but Action Network
        historical collection is typically done via collect_history_data.
        """
        return CollectionResult(
            success=False,
            data=[],
            source=self.source_name,
            timestamp=datetime.now(),
            errors=[
                "Use collect_history_data method for Action Network historical data"
            ],
            metadata=kwargs,
        )
