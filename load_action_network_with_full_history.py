#!/usr/bin/env python3
"""
Load Action Network data with COMPLETE line movement history for July 18th, 2025

This script processes Action Network's 'history' arrays and inserts each historical
point as a separate record in our main betting tables with proper timestamps.

Instead of creating a separate line_movement_history table, we store all movements
in the existing betting_lines_* tables with different updated_at timestamps.
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

import aiohttp
import asyncpg
import structlog

# Import utilities
from src.core.datetime_utils import (
    prepare_for_postgres,
    safe_game_datetime_parse,
    now_est
)
from src.core.team_utils import (
    normalize_team_name,
    create_external_source_id
)
from src.core.sportsbook_utils import SportsbookResolver

# Configure logging
structlog.configure(
    processors=[structlog.dev.ConsoleRenderer()],
    wrapper_class=structlog.make_filtering_bound_logger(20),
    logger_factory=structlog.WriteLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class ActionNetworkFullHistoryLoader:
    """Load Action Network data with complete line movement history."""
    
    def __init__(self):
        self.api_base = "https://api.actionnetwork.com"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "mlb_betting",
            "user": "samlafell"
        }
        
        self.sportsbook_resolver = SportsbookResolver(self.db_config)
        self.stats = {
            "games_processed": 0,
            "total_history_records": 0,
            "moneyline_inserted": 0,
            "spread_inserted": 0,
            "totals_inserted": 0,
            "errors": 0
        }
    
    async def collect_and_store_full_history(self) -> Dict[str, Any]:
        """Collect Action Network data and store complete line movement history."""
        
        print("🚀 Action Network COMPLETE Line Movement Collection for July 18th, 2025")
        print("=" * 80)
        print("🎯 Strategy: Store ALL history points in main betting tables")
        print("📊 Each history point = separate record with different updated_at")
        print("🗄️ Tables: betting_lines_moneyline, betting_lines_spread, betting_lines_totals")
        print()
        
        # Step 1: Fetch raw data
        print("📡 Step 1: Fetching Action Network data with history...")
        games_data = await self._fetch_games_data()
        
        if not games_data:
            print("❌ No games found")
            return self.stats
        
        print(f"   ✅ Found {len(games_data)} games")
        
        # Step 2: Create game records
        print("🎮 Step 2: Creating/updating game records...")
        game_mappings = await self._ensure_games_exist(games_data)
        print(f"   ✅ Processed {len(game_mappings)} games")
        
        # Step 3: Process history for each game
        print("📊 Step 3: Processing complete line movement history...")
        for game in games_data:
            await self._process_game_history(game, game_mappings)
        
        print(f"   ✅ Processed {self.stats['games_processed']} games")
        print(f"   📈 Total history records: {self.stats['total_history_records']}")
        
        return self.stats
    
    async def _fetch_games_data(self) -> List[Dict[str, Any]]:
        """Fetch games data from Action Network."""
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb"
                params = {
                    "bookIds": "15,30,75,123,69,68,972,71,247,79",
                    "date": "20250718",  # July 18th, 2025
                    "periods": "event",
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("games", [])
                    else:
                        logger.error("API request failed", status=response.status)
                        return []
        
        except Exception as e:
            logger.error("Failed to fetch Action Network data", error=str(e))
            return []
    
    async def _ensure_games_exist(self, games: List[Dict[str, Any]]) -> Dict[str, int]:
        """Ensure all games exist and return game ID mappings."""
        
        game_mappings = {}
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            for game in games:
                game_id = game.get("id")
                teams = game.get("teams", [])
                start_time = game.get("start_time")
                
                if len(teams) < 2 or not game_id or not start_time:
                    continue
                
                # Check if exists
                existing_id = await conn.fetchval("""
                    SELECT id FROM core_betting.games WHERE action_network_game_id = $1
                """, int(game_id))
                
                if existing_id:
                    game_mappings[str(game_id)] = existing_id
                    continue
                
                # Create new game
                away_team = normalize_team_name(teams[0].get("full_name", ""))
                home_team = normalize_team_name(teams[1].get("full_name", ""))
                game_datetime = safe_game_datetime_parse(start_time)
                
                new_id = await conn.fetchval("""
                    INSERT INTO core_betting.games (
                        action_network_game_id, home_team, away_team, game_date, 
                        game_datetime, season, season_type, game_type, data_quality,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                """, 
                int(game_id), home_team, away_team, game_datetime.date(), game_datetime,
                2025, "REG", "R", "HIGH", now_est(), now_est()
                )
                
                game_mappings[str(game_id)] = new_id
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error ensuring games exist", error=str(e))
        
        return game_mappings
    
    async def _process_game_history(self, game: Dict[str, Any], game_mappings: Dict[str, int]) -> None:
        """Process complete line movement history for a single game."""
        
        game_id = str(game.get("id"))
        if game_id not in game_mappings:
            return
        
        internal_game_id = game_mappings[game_id]
        teams = game.get("teams", [])
        start_time = game.get("start_time")
        
        if len(teams) < 2:
            return
        
        away_team = normalize_team_name(teams[0].get("full_name", ""))
        home_team = normalize_team_name(teams[1].get("full_name", ""))
        game_datetime = safe_game_datetime_parse(start_time)
        
        markets = game.get("markets", {})
        
        # Process each sportsbook's historical data
        for book_id_str, book_data in markets.items():
            book_id = int(book_id_str)
            
            # Resolve sportsbook ID
            sportsbook_mapping = await self.sportsbook_resolver.resolve_action_network_id(book_id)
            if not sportsbook_mapping:
                continue
            
            sportsbook_id, sportsbook_name = sportsbook_mapping
            
            event_markets = book_data.get("event", {})
            
            # Process moneyline history
            await self._process_moneyline_history(
                event_markets.get("moneyline", []), internal_game_id, 
                sportsbook_id, sportsbook_name, home_team, away_team, 
                game_datetime, int(game_id), book_id
            )
            
            # Process spread history
            await self._process_spread_history(
                event_markets.get("spread", []), internal_game_id,
                sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, int(game_id), book_id
            )
            
            # Process totals history
            await self._process_totals_history(
                event_markets.get("total", []), internal_game_id,
                sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, int(game_id), book_id
            )
        
        self.stats["games_processed"] += 1
    
    async def _process_moneyline_history(
        self, moneyline_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str, 
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process moneyline history and insert all historical points."""
        
        if len(moneyline_data) < 2:
            return
        
        home_entry = next((entry for entry in moneyline_data if entry.get("side") == "home"), None)
        away_entry = next((entry for entry in moneyline_data if entry.get("side") == "away"), None)
        
        if not home_entry or not away_entry:
            return
        
        home_history = home_entry.get("history", [])
        away_history = away_entry.get("history", [])
        
        # If no history, insert current lines only
        if not home_history and not away_history:
            await self._insert_current_moneyline(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                home_entry.get("odds"), away_entry.get("odds")
            )
            return
        
        # Process historical points
        # Match history points by timestamp (assuming they align)
        await self._insert_moneyline_history_points(
            game_id, sportsbook_id, sportsbook_name, home_team, away_team,
            game_datetime, action_network_game_id, action_network_book_id,
            home_history, away_history, home_entry.get("odds"), away_entry.get("odds")
        )
    
    async def _insert_moneyline_history_points(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        home_history: List[Dict], away_history: List[Dict],
        current_home_odds: int, current_away_odds: int
    ) -> None:
        """Insert all moneyline history points as separate records."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            # Create lookup for away odds by timestamp
            away_odds_by_time = {}
            for away_point in away_history:
                timestamp = away_point.get("updated_at")
                if timestamp:
                    away_odds_by_time[timestamp] = away_point.get("odds")
            
            # Process each home history point and match with away
            for home_point in home_history:
                home_odds = home_point.get("odds")
                timestamp = home_point.get("updated_at")
                
                if not home_odds or not timestamp:
                    continue
                
                # Find matching away odds at same timestamp
                away_odds = away_odds_by_time.get(timestamp)
                if not away_odds:
                    # Use current away odds if no matching timestamp
                    away_odds = current_away_odds
                
                # Parse timestamp
                odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                
                # Insert historical record
                external_source_id = create_external_source_id(
                    "ACTION_NETWORK", str(action_network_game_id), 
                    str(action_network_book_id), f"ml_{timestamp}"
                )
                
                await conn.execute("""
                    INSERT INTO core_betting.betting_lines_moneyline (
                        game_id, external_source_id, sportsbook_id, sportsbook,
                        home_team, away_team, home_ml, away_ml, odds_timestamp,
                        collection_method, source, game_datetime, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (external_source_id) DO NOTHING
                """,
                game_id, external_source_id, sportsbook_id, sportsbook_name,
                home_team, away_team, home_odds, away_odds, 
                prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
                prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
                )
                
                self.stats["moneyline_inserted"] += 1
                self.stats["total_history_records"] += 1
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error inserting moneyline history", error=str(e))
            self.stats["errors"] += 1
    
    async def _insert_current_moneyline(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        home_odds: int, away_odds: int
    ) -> None:
        """Insert current moneyline if no history available."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            external_source_id = create_external_source_id(
                "ACTION_NETWORK", str(action_network_game_id), 
                str(action_network_book_id), "ml_current"
            )
            
            await conn.execute("""
                INSERT INTO core_betting.betting_lines_moneyline (
                    game_id, external_source_id, sportsbook_id, sportsbook,
                    home_team, away_team, home_ml, away_ml, odds_timestamp,
                    collection_method, source, game_datetime, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (external_source_id) DO NOTHING
            """,
            game_id, external_source_id, sportsbook_id, sportsbook_name,
            home_team, away_team, home_odds, away_odds, now_est(),
            "API", "ACTION_NETWORK", prepare_for_postgres(game_datetime), 
            now_est(), now_est()
            )
            
            self.stats["moneyline_inserted"] += 1
            self.stats["total_history_records"] += 1
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error inserting current moneyline", error=str(e))
            self.stats["errors"] += 1
    
    async def _process_spread_history(
        self, spread_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str,
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process spread history and insert all historical points."""
        
        if len(spread_data) < 2:
            return
        
        home_entry = next((entry for entry in spread_data if entry.get("side") == "home"), None)
        away_entry = next((entry for entry in spread_data if entry.get("side") == "away"), None)
        
        if not home_entry or not away_entry:
            return
        
        home_history = home_entry.get("history", [])
        
        # Process each historical point
        for i, home_point in enumerate(home_history):
            try:
                conn = await asyncpg.connect(**self.db_config)
                
                spread_line = home_point.get("value")
                home_price = home_point.get("odds")
                away_price = away_entry.get("odds", -110)  # Default away price
                timestamp = home_point.get("updated_at")
                
                if not all([spread_line, home_price, timestamp]):
                    continue
                
                odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                external_source_id = create_external_source_id(
                    "ACTION_NETWORK", str(action_network_game_id),
                    str(action_network_book_id), f"spread_{timestamp}"
                )
                
                await conn.execute("""
                    INSERT INTO core_betting.betting_lines_spread (
                        game_id, external_source_id, sportsbook_id, sportsbook,
                        home_team, away_team, spread_line, home_spread_price, away_spread_price,
                        odds_timestamp, collection_method, source, game_datetime, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (external_source_id) DO NOTHING
                """,
                game_id, external_source_id, sportsbook_id, sportsbook_name,
                home_team, away_team, spread_line, home_price, away_price,
                prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
                prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
                )
                
                self.stats["spread_inserted"] += 1
                self.stats["total_history_records"] += 1
                
                await conn.close()
                
            except Exception as e:
                logger.error("Error inserting spread history point", error=str(e))
                self.stats["errors"] += 1
    
    async def _process_totals_history(
        self, totals_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str,
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process totals history and insert all historical points."""
        
        if len(totals_data) < 2:
            return
        
        over_entry = next((entry for entry in totals_data if entry.get("side") == "over"), None)
        under_entry = next((entry for entry in totals_data if entry.get("side") == "under"), None)
        
        if not over_entry or not under_entry:
            return
        
        over_history = over_entry.get("history", [])
        
        # Process each historical point
        for over_point in over_history:
            try:
                conn = await asyncpg.connect(**self.db_config)
                
                total_line = over_point.get("value")
                over_price = over_point.get("odds")
                under_price = under_entry.get("odds", -110)  # Default under price
                timestamp = over_point.get("updated_at")
                
                if not all([total_line, over_price, timestamp]):
                    continue
                
                odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                external_source_id = create_external_source_id(
                    "ACTION_NETWORK", str(action_network_game_id),
                    str(action_network_book_id), f"total_{timestamp}"
                )
                
                await conn.execute("""
                    INSERT INTO core_betting.betting_lines_totals (
                        game_id, external_source_id, sportsbook_id, sportsbook,
                        home_team, away_team, total_line, over_price, under_price,
                        odds_timestamp, collection_method, source, game_datetime, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (external_source_id) DO NOTHING
                """,
                game_id, external_source_id, sportsbook_id, sportsbook_name,
                home_team, away_team, total_line, over_price, under_price,
                prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
                prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
                )
                
                self.stats["totals_inserted"] += 1
                self.stats["total_history_records"] += 1
                
                await conn.close()
                
            except Exception as e:
                logger.error("Error inserting totals history point", error=str(e))
                self.stats["errors"] += 1


async def main():
    """Run the complete line movement collection."""
    
    loader = ActionNetworkFullHistoryLoader()
    results = await loader.collect_and_store_full_history()
    
    print("\n" + "=" * 80)
    print("🎯 COMPLETE LINE MOVEMENT COLLECTION RESULTS")
    print("=" * 80)
    
    print(f"📊 Summary:")
    print(f"   • Games processed: {results['games_processed']}")
    print(f"   • Total history records: {results['total_history_records']}")
    print(f"   • Moneyline records: {results['moneyline_inserted']}")
    print(f"   • Spread records: {results['spread_inserted']}")
    print(f"   • Totals records: {results['totals_inserted']}")
    print(f"   • Errors: {results['errors']}")
    
    # Verify results in database
    print(f"\n🔍 Verification Query Example:")
    print(f"   SELECT * FROM core_betting.betting_lines_moneyline")
    print(f"   WHERE home_team = 'CHC' AND date(game_datetime) = '2025-07-18'")
    print(f"   ORDER BY updated_at;")
    
    if results['total_history_records'] > 0:
        print(f"\n🎉 SUCCESS: Complete line movement history captured!")
        print(f"   Each history point stored as separate record with proper timestamp")
    else:
        print(f"\n⚠️  NOTE: No historical data available in Action Network API")
        print(f"   This is expected for future dates or games without line movement")


if __name__ == "__main__":
    asyncio.run(main())