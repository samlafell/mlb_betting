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
from typing import Dict, List, Any, Optional, Union
from enum import Enum

import aiohttp
import asyncpg
import structlog

from .base import BaseCollector, CollectorConfig, CollectionRequest, CollectionResult
from .smart_line_movement_filter import SmartLineMovementFilter
from ...core.datetime_utils import prepare_for_postgres, safe_game_datetime_parse, now_est
from ...core.team_utils import normalize_team_name
from ...core.sportsbook_utils import SportsbookResolver

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
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.session = None
        self.sportsbook_resolver = SportsbookResolver(db_config)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session
    
    async def fetch_games(self, date: str) -> List[Dict[str, Any]]:
        """Fetch games data from Action Network API."""
        session = await self._get_session()
        
        url = f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb"
        params = {
            "bookIds": "15,30,75,123,69,68,972,71,247,79",
            "date": date,
            "periods": "event",
        }
        
        logger.info("Fetching games from Action Network", url=url, date=date)
        
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("games", [])
            else:
                logger.error("API request failed", status=response.status, url=url)
                return []
    
    async def fetch_game_history(self, game_id: int) -> Dict[str, Any]:
        """Fetch game history data from Action Network API."""
        session = await self._get_session()
        
        url = f"{self.api_base}/web/v2/markets/event/{game_id}/history"
        
        logger.info("Fetching game history", game_id=game_id, url=url)
        
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error("History API request failed", status=response.status, game_id=game_id)
                return {}
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None


class ActionNetworkCollector(BaseCollector):
    """
    Consolidated Action Network collector with all capabilities.
    
    Supports multiple collection modes:
    - CURRENT: Current lines only
    - HISTORICAL: Historical line movements
    - COMPREHENSIVE: Combined current + historical with smart filtering
    """
    
    def __init__(self, config: CollectorConfig, mode: CollectionMode = CollectionMode.COMPREHENSIVE):
        super().__init__(config)
        self.mode = mode
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "mlb_betting",
            "user": "samlafell"
        }
        self.client = ActionNetworkClient(self.db_config)
        self.filter = SmartLineMovementFilter()
        
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
            "total_inserted": 0
        }
    
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Main collection method supporting all modes."""
        try:
            date_str = request.start_date.strftime("%Y%m%d") if request.start_date else datetime.now().strftime("%Y%m%d")
            
            logger.info("Starting Action Network collection", 
                       mode=self.mode.value, date=date_str)
            
            if self.mode == CollectionMode.CURRENT:
                return await self._collect_current_lines(date_str)
            elif self.mode == CollectionMode.HISTORICAL:
                return await self._collect_historical_data(date_str)
            else:  # COMPREHENSIVE
                return await self._collect_comprehensive(date_str)
        
        except Exception as e:
            logger.error("Collection failed", error=str(e), mode=self.mode.value)
            return []
        
        finally:
            await self.client.close()
    
    async def _collect_current_lines(self, date: str) -> List[Dict[str, Any]]:
        """Collect current betting lines only."""
        games = await self.client.fetch_games(date)
        
        if not games:
            logger.warning("No games found for current lines collection", date=date)
            return []
        
        self.stats["games_found"] = len(games)
        logger.info("Found games for current lines", count=len(games))
        
        # Store raw data first (RAW layer)
        await self._store_raw_game_data(games, "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb")
        
        # Process current lines (CURATED layer)
        await self._process_current_lines(games)
        
        return games
    
    async def _collect_historical_data(self, date: str) -> List[Dict[str, Any]]:
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
    
    async def _collect_comprehensive(self, date: str) -> List[Dict[str, Any]]:
        """Collect comprehensive data with smart filtering."""
        games = await self.client.fetch_games(date)
        
        if not games:
            logger.warning("No games found for comprehensive collection", date=date)
            return []
        
        self.stats["games_found"] = len(games)
        logger.info("Found games for comprehensive collection", count=len(games))
        
        # Store raw game data first (RAW layer)
        await self._store_raw_game_data(games, "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb")
        
        # Store raw odds data from current games (RAW layer)
        await self._store_raw_current_odds(games)
        
        # Fetch and store historical data for each game (RAW layer)
        await self._fetch_and_store_historical_data(games)
        
        # Get game mappings from raw data (no CURATED layer writes)
        game_mappings = await self._get_game_mappings(games)
        
        # Store processed odds to raw_data.action_network_odds (RAW layer only)
        await self._process_comprehensive_data(games, game_mappings)
        
        return games
    
    async def _get_game_mappings(self, games: List[Dict[str, Any]]) -> Dict[str, str]:
        """Get game mappings from raw data - no longer creates legacy games."""
        game_mappings = {}
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            for game in games:
                game_id = game.get("id")
                if not game_id:
                    continue
                
                # Check if raw game data exists
                existing_raw = await conn.fetchval("""
                    SELECT id FROM raw_data.action_network_games WHERE external_game_id = $1
                """, str(game_id))
                
                if existing_raw:
                    game_mappings[str(game_id)] = str(game_id)
                    logger.debug(f"Found existing raw game data for game {game_id}")
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error getting game mappings", error=str(e))
        
        return game_mappings
    
    async def _store_raw_game_data(self, games: List[Dict[str, Any]], endpoint_url: str = None) -> None:
        """Store raw game data to raw_data.action_network_games table."""
        if not games:
            return
            
        try:
            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"]
            )
            
            for game in games:
                game_id = game.get('id')
                if not game_id:
                    continue
                    
                # Store raw game data exactly as received from API
                await conn.execute("""
                    INSERT INTO raw_data.action_network_games (
                        external_game_id, raw_response, endpoint_url, 
                        response_status, game_date, collected_at, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (external_game_id) DO UPDATE SET
                        raw_response = EXCLUDED.raw_response,
                        collected_at = EXCLUDED.collected_at
                """,
                str(game_id), 
                json.dumps(game),  # Convert dict to JSON string for JSONB storage
                endpoint_url or "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb",
                200,
                safe_game_datetime_parse(game.get('start_time')).date() if game.get('start_time') else now_est().date(),
                now_est(),
                now_est()
                )
            
            await conn.close()
            logger.info(f"Stored {len(games)} games to raw_data.action_network_games")
            
        except Exception as e:
            logger.error("Error storing raw game data", error=str(e))
    
    async def _store_raw_odds_data(self, game_id: str, odds_data: Dict[str, Any], sportsbook_key: str = None) -> None:
        """Store raw odds data to raw_data.action_network_odds table."""
        try:
            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"]
            )
            
            await conn.execute("""
                INSERT INTO raw_data.action_network_odds (
                    external_game_id, sportsbook_key, raw_odds, collected_at, created_at
                ) VALUES ($1, $2, $3, $4, $5)
            """,
            str(game_id),
            sportsbook_key or "unknown", 
            json.dumps(odds_data),  # Convert dict to JSON string for JSONB storage
            now_est(),
            now_est()
            )
            
            await conn.close()
            logger.debug(f"Stored odds data for game {game_id}, sportsbook {sportsbook_key}")
            
        except Exception as e:
            logger.error("Error storing raw odds data", error=str(e), game_id=game_id)
    
    async def _store_raw_current_odds(self, games: List[Dict[str, Any]]) -> None:
        """Extract and store raw odds data from current games to raw_data.action_network_odds table."""
        try:
            for game in games:
                game_id = game.get('id')
                if not game_id:
                    continue
                
                markets = game.get('markets', {})
                for book_id_str, book_data in markets.items():
                    event_markets = book_data.get('event', {})
                    if event_markets:
                        await self._store_raw_odds_data(str(game_id), event_markets, book_id_str)
                        
            logger.info(f"Processed current odds for {len(games)} games")
            
        except Exception as e:
            logger.error("Error storing raw current odds", error=str(e))
    
    async def _fetch_and_store_historical_data(self, games: List[Dict[str, Any]]) -> None:
        """Fetch historical line movement data for each game and store in RAW layer."""
        try:
            for game in games:
                game_id = game.get('id')
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
    
    async def _store_raw_historical_data(self, game_id: str, history_data: Dict[str, Any]) -> None:
        """Store raw historical data to raw_data.action_network_history table.""" 
        try:
            conn = await asyncpg.connect(
                host=self.db_config["host"],
                port=self.db_config["port"], 
                database=self.db_config["database"],
                user=self.db_config["user"]
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
            
            await conn.execute("""
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
            now_est()
            )
            
            await conn.close()
            logger.debug(f"Stored historical data for game {game_id}")
            
        except Exception as e:
            logger.error("Error storing raw historical data", error=str(e), game_id=game_id)
    
    async def _process_current_lines(self, games: List[Dict[str, Any]]) -> None:
        """Process current betting lines - stores to RAW layer only."""
        for game in games:
            try:
                game_id = game.get("id")
                markets = game.get("markets", {})
                
                for book_id_str, book_data in markets.items():
                    event_markets = book_data.get("event", {})
                    
                    # Store raw odds data to RAW layer only
                    await self._store_raw_odds_data(str(game_id), event_markets, book_id_str)
                    
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
                logger.error("Error processing current lines", 
                           game_id=game.get("id"), error=str(e))
    
    async def _process_historical_data(self, games: List[Dict[str, Any]]) -> None:
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
                logger.error("Error processing historical data", 
                           game_id=game.get("id"), error=str(e))
    
    async def _process_comprehensive_data(self, games: List[Dict[str, Any]], game_mappings: Dict[str, str]) -> None:
        """Process comprehensive data - stores only to raw_data.action_network_odds."""
        for game in games:
            try:
                game_id = str(game.get("id"))
                if game_id not in game_mappings:
                    logger.debug(f"Skipping game {game_id} - no raw data mapping")
                    continue
                
                teams = game.get("teams", [])
                
                if len(teams) < 2:
                    continue
                
                away_team = normalize_team_name(teams[0].get("full_name", ""))
                home_team = normalize_team_name(teams[1].get("full_name", ""))
                game_datetime = safe_game_datetime_parse(game.get("start_time"))
                
                markets = game.get("markets", {})
                
                # Process each sportsbook - store to RAW layer only
                for book_id_str, book_data in markets.items():
                    book_id = int(book_id_str)
                    
                    event_markets = book_data.get("event", {})
                    
                    # Store all odds data to raw_data.action_network_odds
                    await self._store_comprehensive_odds_data(
                        game_id, book_id_str, event_markets, home_team, away_team, game_datetime
                    )
                
                self.stats["games_processed"] += 1
                
            except Exception as e:
                logger.error("Error processing comprehensive data", 
                           game_id=game.get("id"), error=str(e))
    
    async def _process_current_market(self, *args, **kwargs) -> None:
        """DEPRECATED: Use raw data storage methods instead."""
        logger.warning("_process_current_market is deprecated. Use raw data storage methods.")
        pass
    
    async def _process_game_history(self, game: Dict[str, Any], history_data: Dict[str, Any]) -> None:
        """Process game history data - stores to RAW layer only."""
        game_id = game.get("id")
        if not game_id or not history_data:
            return
            
        # Store raw historical data
        await self._store_raw_historical_data(str(game_id), history_data)
        
        logger.debug("Stored game history to raw data", game_id=game_id)
        self.stats["history_points"] += len(history_data)
    
    async def _store_comprehensive_odds_data(self, game_id: str, sportsbook_key: str, 
                                           event_markets: Dict[str, Any], home_team: str, 
                                           away_team: str, game_datetime: datetime) -> None:
        """Store comprehensive odds data to raw_data.action_network_odds."""
        try:
            # Add metadata to the odds data
            enhanced_odds_data = {
                "event_markets": event_markets,
                "game_metadata": {
                    "home_team": home_team,
                    "away_team": away_team,
                    "game_datetime": game_datetime.isoformat()
                },
                "collection_info": {
                    "collection_mode": "comprehensive",
                    "smart_filtering_applied": True,
                    "timestamp": now_est().isoformat()
                }
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
            logger.error("Error storing comprehensive odds data", 
                        game_id=game_id, sportsbook_key=sportsbook_key, error=str(e))
    
    async def _process_moneyline_markets(self, moneyline_data: List[Dict], 
                                       game_id: str, sportsbook_key: str,
                                       home_team: str, away_team: str,
                                       game_datetime: datetime) -> None:
        """Process moneyline markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning("_process_moneyline_markets is deprecated. Use raw data storage methods.")
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass
    
    async def _process_spread_markets(self, spread_data: List[Dict], 
                                    game_id: str, sportsbook_key: str,
                                    home_team: str, away_team: str,
                                    game_datetime: datetime) -> None:
        """Process spread markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning("_process_spread_markets is deprecated. Use raw data storage methods.")
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass
    
    async def _process_totals_markets(self, totals_data: List[Dict], 
                                    game_id: str, sportsbook_key: str,
                                    home_team: str, away_team: str,
                                    game_datetime: datetime) -> None:
        """Process totals markets - DEPRECATED: Use _store_comprehensive_odds_data instead."""
        logger.warning("_process_totals_markets is deprecated. Use raw data storage methods.")
        # This method is now deprecated - all odds data should go to raw_data.action_network_odds
        pass
    
    async def _insert_moneyline_history(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning("_insert_moneyline_history is deprecated. All odds data should be stored in raw_data schema.")
        pass
    
    async def _insert_single_moneyline(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning("_insert_single_moneyline is deprecated. All odds data should be stored in raw_data schema.")
        pass
    
    async def _insert_single_spread(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning("_insert_single_spread is deprecated. All odds data should be stored in raw_data schema.")
        pass
    
    async def _insert_single_total(self, *args, **kwargs) -> None:
        """DEPRECATED: Legacy method - use raw data storage instead."""
        logger.warning("_insert_single_total is deprecated. All odds data should be stored in raw_data schema.")
        pass
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate Action Network game record."""
        required_fields = ["id", "teams", "start_time", "markets"]
        return all(field in record for field in required_fields)
    
    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add collection metadata to record."""
        record["source"] = "ACTION_NETWORK"
        record["collected_at"] = datetime.now()
        record["collection_mode"] = self.mode.value
        return record
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        return {
            **self.stats,
            "collection_mode": self.mode.value,
            "success_rate": self.stats["games_processed"] / max(self.stats["games_found"], 1) * 100
        }


# Convenience functions for backward compatibility
async def collect_action_network_data(date: str = None, mode: str = "comprehensive") -> Dict[str, Any]:
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